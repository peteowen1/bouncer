# 3-Way ELO Rating System
#
# A unified ELO system where Batter, Bowler, and Venue all participate in
# rating updates based on delivery outcomes.
#
# Two dimensions:
#   - Run ELO: Expected runs vs actual runs (continuous scale 0-1)
#   - Wicket ELO: Expected wicket probability vs actual wicket (binary 0/1)
#
# Key features:
#   - Dual venue component (permanent + session)
#   - Dynamic K-factors with inactivity decay
#   - Situational modifiers (knockout, tier, phase, high chase)
#   - Post-match normalization

# ============================================================================
# BASELINE LOOKUP FUNCTIONS (Pre-computed Agnostic Predictions)
# ============================================================================
# The agnostic model predictions are pre-computed and stored in database tables:
#   - agnostic_predictions_t20
#   - agnostic_predictions_odi
#   - agnostic_predictions_test
#
# These provide delivery-level baselines that account for:
#   - Match context (over, phase, wickets, score pressure)
#   - League characteristics (historical scoring patterns)
#
# See: data-raw/models/ball-outcome/02_save_agnostic_predictions.R

#' Get Agnostic Baselines for Deliveries
#'
#' Retrieves pre-computed agnostic model predictions for a set of deliveries.
#' These serve as the baseline expected values before ELO adjustments.
#'
#' @param conn DBI connection to the database.
#' @param delivery_ids Character vector. The delivery IDs to look up.
#' @param format Character. Match format: "t20", "odi", or "test".
#'
#' @return Data frame with columns: delivery_id, agnostic_expected_runs,
#'   agnostic_expected_wicket. Missing deliveries get format defaults.
#'
#' @export
get_agnostic_baselines <- function(conn, delivery_ids, format = "t20") {
  format <- tolower(format)
  table_name <- sprintf("agnostic_predictions_%s", format)

  # Get format defaults for fallback
  default_runs <- switch(format,
    "t20" = EXPECTED_RUNS_T20,
    "odi" = EXPECTED_RUNS_ODI,
    "test" = EXPECTED_RUNS_TEST,
    EXPECTED_RUNS_T20)
  default_wicket <- switch(format,
    "t20" = EXPECTED_WICKET_T20,
    "odi" = EXPECTED_WICKET_ODI,
    "test" = EXPECTED_WICKET_TEST,
    EXPECTED_WICKET_T20)

  # Check if table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Table {table_name} not found. Using format defaults.")
    return(data.frame(
      delivery_id = delivery_ids,
      agnostic_expected_runs = default_runs,
      agnostic_expected_wicket = default_wicket
    ))
  }

  # Query in batches to avoid SQL length limits
  batch_size <- 10000
  n_batches <- ceiling(length(delivery_ids) / batch_size)
  results <- vector("list", n_batches)

  for (i in seq_len(n_batches)) {
    start_idx <- (i - 1) * batch_size + 1
    end_idx <- min(i * batch_size, length(delivery_ids))
    batch_ids <- delivery_ids[start_idx:end_idx]

    # Quote and escape IDs (prevent SQL injection)
    ids_sql <- paste(sprintf("'%s'", escape_sql_strings(batch_ids)), collapse = ", ")

    query <- sprintf("
      SELECT delivery_id, agnostic_expected_runs, agnostic_expected_wicket
      FROM %s
      WHERE delivery_id IN (%s)
    ", table_name, ids_sql)

    results[[i]] <- DBI::dbGetQuery(conn, query)
  }

  result <- fast_rbind(results)

  # Add defaults for missing deliveries
  missing_ids <- setdiff(delivery_ids, result$delivery_id)
  if (length(missing_ids) > 0) {
    missing_df <- data.frame(
      delivery_id = missing_ids,
      agnostic_expected_runs = default_runs,
      agnostic_expected_wicket = default_wicket
    )
    result <- fast_rbind(list(result, missing_df))
  }

  # Ensure same order as input
  result[match(delivery_ids, result$delivery_id), ]
}


#' Get Single Delivery Baseline
#'
#' Fast lookup for a single delivery's agnostic baseline.
#' Uses caching environment for repeated lookups within a session.
#'
#' @param delivery_id Character. The delivery ID.
#' @param format Character. Match format.
#' @param baseline_cache Environment. Cache of pre-loaded baselines.
#'   If NULL, returns format defaults.
#'
#' @return Named list with `runs` and `wicket` baseline values.
#' @keywords internal
get_single_baseline <- function(delivery_id, format = "t20",
                                 baseline_cache = NULL) {
  format <- tolower(format)

  # If cache provided and contains this delivery, use cached value
  if (!is.null(baseline_cache) && exists(delivery_id, baseline_cache)) {
    cached <- get(delivery_id, baseline_cache)
    return(list(runs = cached$runs, wicket = cached$wicket))
  }

  # Return format defaults
  default_runs <- switch(format,
    "t20" = EXPECTED_RUNS_T20,
    "odi" = EXPECTED_RUNS_ODI,
    "test" = EXPECTED_RUNS_TEST,
    EXPECTED_RUNS_T20)
  default_wicket <- switch(format,
    "t20" = EXPECTED_WICKET_T20,
    "odi" = EXPECTED_WICKET_ODI,
    "test" = EXPECTED_WICKET_TEST,
    EXPECTED_WICKET_T20)

  list(runs = default_runs, wicket = default_wicket)
}


#' Build Baseline Cache from Database
#'
#' Pre-loads agnostic baselines into an environment for fast lookup
#' during ELO calculation loops.
#'
#' @param conn DBI connection.
#' @param delivery_ids Character vector. Deliveries to cache.
#' @param format Character. Match format.
#'
#' @return Environment with delivery_id -> list(runs, wicket) mappings.
#' @keywords internal
build_baseline_cache <- function(conn, delivery_ids, format = "t20") {
  baselines <- get_agnostic_baselines(conn, delivery_ids, format)

  cache <- new.env(hash = TRUE, size = nrow(baselines))
  for (i in seq_len(nrow(baselines))) {
    assign(
      baselines$delivery_id[i],
      list(
        runs = baselines$agnostic_expected_runs[i],
        wicket = baselines$agnostic_expected_wicket[i]
      ),
      cache
    )
  }

  cache
}


# ============================================================================
# EXPECTED VALUE CALCULATIONS
# ============================================================================

#' Calculate Combined ELO Difference
#'
#' Computes the weighted combined ELO difference for batter, bowler, and venue.
#' This is the core ELO signal before conversion to expected runs.
#'
#' Formula:
#'   combined_diff = w_batter * (batter - start) +
#'                   w_bowler * (start - bowler) +  # inverted for bowler
#'                   w_venue_perm * (venue_perm - start) +
#'                   w_venue_session * (venue_session - start)
#'
#' @param batter_run_elo Numeric. Batter's run ELO.
#' @param bowler_run_elo Numeric. Bowler's run ELO.
#' @param venue_perm_run_elo Numeric. Venue's permanent run ELO.
#' @param venue_session_run_elo Numeric. Venue's session run ELO.
#' @param format Character. Match format.
#' @param gender Character. Gender.
#'
#' @return Numeric. Combined weighted ELO difference.
#' @keywords internal
calculate_combined_elo_diff <- function(batter_run_elo,
                                         bowler_run_elo,
                                         venue_perm_run_elo,
                                         venue_session_run_elo,
                                         format = "t20",
                                         gender = "male") {
  weights <- get_run_elo_weights(format, gender)

  # Batter: higher ELO = more runs (positive contribution)
  batter_diff <- batter_run_elo - THREE_WAY_ELO_START

  # Bowler: higher ELO = FEWER runs (inverted)
  bowler_diff <- THREE_WAY_ELO_START - bowler_run_elo

  # Venue: higher ELO = more runs
  venue_perm_diff <- venue_perm_run_elo - THREE_WAY_ELO_START
  venue_session_diff <- venue_session_run_elo - THREE_WAY_ELO_START

  # Apply attribution weights
  weights$w_batter * batter_diff +
    weights$w_bowler * bowler_diff +
    weights$w_venue_perm * venue_perm_diff +
    weights$w_venue_session * venue_session_diff
}


#' Calculate Logistic ELO Multiplier
#'
#' Converts an ELO difference to a multiplier using logistic (sigmoid) function.
#' This naturally bounds the output regardless of how extreme the ELO difference is.
#'
#' Uses tanh for a symmetric multiplier centered at 1.0:
#'   multiplier = 1 + (max_mult - 1) * tanh(elo_diff / sensitivity)
#'
#' At elo_diff = 0: multiplier = 1.0 (no change)
#' At elo_diff = +Inf: multiplier approaches max_mult
#' At elo_diff = -Inf: multiplier approaches 2 - max_mult (e.g., 0.5 for max_mult=1.5)
#'
#' @param elo_diff Numeric. Combined weighted ELO difference.
#' @param sensitivity Numeric. ELO points for ~76% of max effect.
#'   Default uses THREE_WAY_LOGISTIC_SENSITIVITY.
#' @param max_mult Numeric. Maximum multiplier at extreme positive ELO.
#'   Default uses THREE_WAY_LOGISTIC_MAX_MULTIPLIER.
#'
#' @return Numeric. Multiplier to apply to baseline (symmetric around 1.0).
#' @keywords internal
calculate_logistic_multiplier <- function(elo_diff,
                                           sensitivity = THREE_WAY_LOGISTIC_SENSITIVITY,
                                           max_mult = THREE_WAY_LOGISTIC_MAX_MULTIPLIER) {
  # tanh-based formula: symmetric around 1.0
  # tanh(0) = 0, tanh(inf) = 1, tanh(-inf) = -1
  # So: at 0 -> 1.0, at +inf -> max_mult, at -inf -> 2 - max_mult
  amplitude <- max_mult - 1
  1 + amplitude * tanh(elo_diff / sensitivity)
}


#' Calculate 3-Way Expected Runs
#'
#' Uses agnostic model as baseline with ELO-based adjustments from
#' batter, bowler, and venue ratings.
#'
#' The `agnostic_runs` parameter should come from pre-computed per-delivery
#' baselines (see `get_agnostic_baselines()`) which account for match context
#' and league characteristics. This enables league-specific expected values.
#'
#' Supports two conversion modes (controlled by THREE_WAY_USE_LOGISTIC_CONVERSION):
#' - Linear (default): expected = baseline + weighted_elo_diff * runs_per_elo
#' - Logistic: expected = baseline * logistic_multiplier(weighted_elo_diff)
#'
#' @param agnostic_runs Numeric. Base expected runs from agnostic model.
#'   Use per-delivery values from agnostic_predictions table for best results.
#' @param batter_run_elo Numeric. Batter's run ELO.
#' @param bowler_run_elo Numeric. Bowler's run ELO.
#' @param venue_perm_run_elo Numeric. Venue's permanent run ELO.
#' @param venue_session_run_elo Numeric. Venue's session run ELO.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param gender Character. Gender: "male" or "female". Default "male".
#'
#' @return Numeric. Adjusted expected runs.
#' @keywords internal
calculate_3way_expected_runs <- function(agnostic_runs,
                                          batter_run_elo,
                                          bowler_run_elo,
                                          venue_perm_run_elo,
                                          venue_session_run_elo,
                                          format = "t20",
                                          gender = "male") {
  format <- tolower(format)

  # Calculate combined weighted ELO difference
  combined_elo_diff <- calculate_combined_elo_diff(
    batter_run_elo, bowler_run_elo,
    venue_perm_run_elo, venue_session_run_elo,
    format, gender
  )

  # Choose conversion method
  if (THREE_WAY_USE_LOGISTIC_CONVERSION) {
    # Logistic conversion: naturally bounded multiplier
    multiplier <- calculate_logistic_multiplier(combined_elo_diff)
    expected_runs <- agnostic_runs * multiplier
  } else {
    # Linear conversion (original method)
    runs_per_100_elo <- get_runs_per_100_elo(format, gender)
    runs_per_elo <- runs_per_100_elo / 100
    expected_runs <- agnostic_runs + combined_elo_diff * runs_per_elo
  }

  # Bound to reasonable range (0 to 6 runs)
  max(0, min(6, expected_runs))
}


#' Calculate 3-Way Expected Wicket Probability
#'
#' Uses agnostic model as baseline with ELO-based adjustments on log-odds scale.
#'
#' The `agnostic_wicket` parameter should come from pre-computed per-delivery
#' baselines (see `get_agnostic_baselines()`) which account for match context
#' and league characteristics. This enables league-specific expected values.
#'
#' @param agnostic_wicket Numeric. Base wicket probability from agnostic model.
#'   Use per-delivery values from agnostic_predictions table for best results.
#' @param batter_wicket_elo Numeric. Batter's wicket ELO (higher = survives better).
#' @param bowler_wicket_elo Numeric. Bowler's wicket ELO (higher = takes more).
#' @param venue_perm_wicket_elo Numeric. Venue permanent wicket ELO.
#' @param venue_session_wicket_elo Numeric. Venue session wicket ELO.
#' @param format Character. Match format: "t20", "odi", or "test". Default "t20".
#' @param gender Character. Gender: "male" or "female". Default "male".
#'
#' @return Numeric. Adjusted expected wicket probability.
#' @keywords internal
calculate_3way_expected_wicket <- function(agnostic_wicket,
                                            batter_wicket_elo,
                                            bowler_wicket_elo,
                                            venue_perm_wicket_elo,
                                            venue_session_wicket_elo,
                                            format = "t20",
                                            gender = "male") {
  # Get format-gender-specific parameters
  weights <- get_wicket_elo_weights(format, gender)
  divisor <- get_wicket_elo_divisor(format, gender)

  # Prevent log(0) or log(Inf)
  agnostic_wicket <- max(0.001, min(0.999, agnostic_wicket))

  # Convert to log-odds
  base_logit <- log(agnostic_wicket / (1 - agnostic_wicket))

  # Adjust log-odds based on ELO differences using format-gender-specific weights
  # Batter: higher ELO = lower wicket probability (subtract)
  # Bowler: higher ELO = higher wicket probability (add)
  # Venue: higher ELO = more wickets fall (add)
  adjusted_logit <- base_logit -
    weights$w_batter * (batter_wicket_elo - THREE_WAY_ELO_START) / divisor +
    weights$w_bowler * (bowler_wicket_elo - THREE_WAY_ELO_START) / divisor +
    weights$w_venue_perm * (venue_perm_wicket_elo - THREE_WAY_ELO_START) / divisor +
    weights$w_venue_session * (venue_session_wicket_elo - THREE_WAY_ELO_START) / divisor

  # Convert back to probability
  expected_wicket <- 1 / (1 + exp(-adjusted_logit))

  # Bound to reasonable range
  max(0.001, min(0.5, expected_wicket))
}


# ============================================================================
# K-FACTOR CALCULATIONS
# ============================================================================

#' Get 3-Way Player K-Factor
#'
#' Calculates dynamic K-factor for a player based on experience and context.
#' Includes calibration-based scaling where uncalibrated players (low anchor
#' exposure) get higher K to learn faster from calibrated opponents.
#'
#' Now also supports centrality-based K-factor modulation (Option A):
#' - High centrality opponents → higher K (learn more from elite players)
#' - Low centrality opponents → lower K (learn less from weak players)
#'
#' @param deliveries Integer. Player's total deliveries faced/bowled.
#' @param days_inactive Integer. Days since player's last match.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param elo_type Character. "run" or "wicket".
#' @param phase Character. Match phase: "powerplay", "middle", or "death".
#' @param is_knockout Logical. Whether this is a knockout match.
#' @param event_tier Integer. Event tier (1-4).
#' @param is_high_chase Logical. Whether this is a high-pressure chase.
#' @param calibration_score Numeric. Player's calibration score (0-100).
#'   NULL uses no calibration adjustment (backward compatible).
#' @param use_player_tier_k Logical. Use player-level tier K multipliers
#'   (more aggressive) instead of standard tier multipliers.
#' @param opponent_centrality_percentile Numeric. Opponent's centrality percentile (0-100).
#'   If provided, applies sigmoid-based K-factor modulation where:
#'   - 99th percentile → K × 1.5 (learn a lot from elite)
#'   - 50th percentile → K × 1.0 (neutral)
#'   - 10th percentile → K × 0.5 (learn little from weak)
#'   NULL or NA uses no centrality adjustment (backward compatible).
#' @param gender Character. Gender: "male" or "female". Default "male".
#'
#' @return Numeric. The final adjusted K-factor.
#' @keywords internal
get_3way_player_k <- function(deliveries,
                               days_inactive = 0,
                               format = "t20",
                               elo_type = "run",
                               phase = "middle",
                               is_knockout = FALSE,
                               event_tier = 2,
                               is_high_chase = FALSE,
                               calibration_score = NULL,
                               use_player_tier_k = FALSE,
                               opponent_centrality_percentile = NULL,
                               gender = "male") {
  format <- tolower(format)

  # Get format-gender-specific K parameters using helper functions
  if (elo_type == "wicket") {
    params <- get_wicket_k_factors(format, gender)
  } else {
    params <- get_run_k_factors(format, gender)
  }

  # Base K from experience decay
  k <- params$k_min + (params$k_max - params$k_min) * exp(-deliveries / params$halflife)

  # Inactivity boost (more uncertainty after absence)
  if (days_inactive > THREE_WAY_INACTIVITY_THRESHOLD_DAYS * 2) {
    decay_factor <- exp(-days_inactive / THREE_WAY_INACTIVITY_HALFLIFE)
    k <- k * (1 + THREE_WAY_INACTIVITY_K_BOOST_FACTOR * (1 - decay_factor))
  }

  # Phase multiplier
  phase_mult <- switch(tolower(phase),
    "powerplay" = THREE_WAY_PHASE_POWERPLAY_MULT,
    "death" = THREE_WAY_PHASE_DEATH_MULT,
    THREE_WAY_PHASE_MIDDLE_MULT
  )
  k <- k * phase_mult

  # Event tier multiplier
  # Use player-level tier K multipliers if requested (more aggressive for calibration)
  if (use_player_tier_k) {
    tier_mult <- get_player_tier_k_multiplier(event_tier)
  } else {
    tier_mult <- switch(as.character(event_tier),
      "1" = THREE_WAY_TIER_1_MULT,
      "2" = THREE_WAY_TIER_2_MULT,
      "3" = THREE_WAY_TIER_3_MULT,
      "4" = THREE_WAY_TIER_4_MULT,
      THREE_WAY_TIER_2_MULT
    )
  }
  k <- k * tier_mult

  # Knockout multiplier
  if (is_knockout) {
    k <- k * THREE_WAY_KNOCKOUT_MULT
  }

  # High chase adjustment (handled separately for run/wicket K)
  if (is_high_chase) {
    if (elo_type == "wicket") {
      k <- k * THREE_WAY_HIGH_CHASE_WICKET_MULT
    } else {
      k <- k * THREE_WAY_HIGH_CHASE_RUN_MULT
    }
  }

  # Calibration-based K-factor adjustment
  # Uncalibrated players (low anchor exposure) get higher K to learn faster
  if (!is.null(calibration_score)) {
    k <- k * get_calibration_k_multiplier(calibration_score)
  }

  # Centrality-based K-factor modulation (Option A: Preventive)
  # Learn more from elite opponents, less from weak ones
  if (!is.null(opponent_centrality_percentile) && !is.na(opponent_centrality_percentile)) {
    k <- k * get_centrality_k_multiplier(opponent_centrality_percentile)
  }

  k
}


#' Get 3-Way Venue Permanent K-Factor
#'
#' Calculates K-factor for permanent venue ELO (slow-learning).
#' Uses format-gender-specific parameters optimized via Poisson loss (Feb 2026).
#'
#' Applies THREE_WAY_VENUE_PERM_K_MULTIPLIER to reduce venue K-factors and
#' prevent extreme venue ELO drift. Set multiplier to 1.0 for original values.
#'
#' @param venue_total_balls Integer. Total balls at this venue.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param gender Character. Gender: "male" or "female". Default "male".
#'
#' @return Numeric. The K-factor for permanent venue ELO.
#' @keywords internal
get_3way_venue_perm_k <- function(venue_total_balls, format = "t20", gender = "male") {
  format <- tolower(format)

  # Get format-gender-specific venue K parameters
  params <- get_venue_k_factors(format, gender)

  base_k <- params$perm_min + (params$perm_max - params$perm_min) *
    exp(-venue_total_balls / params$perm_halflife)

  # Apply global venue K reduction multiplier (set in constants_3way.R)
  base_k * THREE_WAY_VENUE_PERM_K_MULTIPLIER
}


#' Get 3-Way Venue Session K-Factor
#'
#' Calculates K-factor for session venue ELO (fast-learning within match).
#' Resets each match to capture current conditions.
#' Uses format-gender-specific parameters optimized via Poisson loss (Feb 2026).
#'
#' @param balls_in_match Integer. Balls bowled in current match.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param gender Character. Gender: "male" or "female". Default "male".
#'
#' @return Numeric. The K-factor for session venue ELO.
#' @keywords internal
get_3way_venue_session_k <- function(balls_in_match, format = "t20", gender = "male") {
  format <- tolower(format)

  # Get format-gender-specific venue K parameters
  params <- get_venue_k_factors(format, gender)

  k <- params$session_max * exp(-balls_in_match / params$session_halflife)
  max(params$session_min, k)
}


# ============================================================================
# ELO UPDATE FUNCTIONS
# ============================================================================

#' Update 3-Way Run ELOs
#'
#' Calculates ELO updates for run dimension for all three entities.
#'
#' @param actual_run_score Numeric. Actual run outcome score (0-1 scale).
#' @param expected_run_score Numeric. Expected run score.
#' @param k_batter Numeric. Batter's K-factor.
#' @param k_bowler Numeric. Bowler's K-factor.
#' @param k_venue_perm Numeric. Venue permanent K-factor.
#' @param k_venue_session Numeric. Venue session K-factor.
#'
#' @return List with delta_batter, delta_bowler, delta_venue_perm, delta_venue_session.
#' @keywords internal
update_3way_run_elos <- function(actual_run_score,
                                  expected_run_score,
                                  k_batter,
                                  k_bowler,
                                  k_venue_perm,
                                  k_venue_session,
                                  update_rule = THREE_WAY_UPDATE_RULE) {
  raw_delta <- actual_run_score - expected_run_score


  # Apply update rule transformation
  # sqrt compresses large errors (sixes), improving MAE by ~8%
  if (update_rule == "sqrt") {
    delta <- sign(raw_delta) * sqrt(abs(raw_delta))
  } else {
    delta <- raw_delta
  }

  list(
    # Batter: positive delta = scored well = ELO increase
    delta_batter = k_batter * delta,
    # Bowler: inverted - if batter scored well, bowler ELO decreases
    delta_bowler = k_bowler * (-delta),
    # Venue: high-scoring venue = higher run ELO
    delta_venue_perm = k_venue_perm * delta,
    delta_venue_session = k_venue_session * delta
  )
}


#' Update 3-Way Wicket ELOs
#'
#' Calculates ELO updates for wicket dimension for all three entities.
#'
#' @param actual_wicket Integer. 1 if wicket fell, 0 otherwise.
#' @param expected_wicket Numeric. Expected wicket probability.
#' @param k_batter Numeric. Batter's K-factor.
#' @param k_bowler Numeric. Bowler's K-factor.
#' @param k_venue_perm Numeric. Venue permanent K-factor.
#' @param k_venue_session Numeric. Venue session K-factor.
#'
#' @return List with delta_batter, delta_bowler, delta_venue_perm, delta_venue_session.
#' @keywords internal
update_3way_wicket_elos <- function(actual_wicket,
                                     expected_wicket,
                                     k_batter,
                                     k_bowler,
                                     k_venue_perm,
                                     k_venue_session) {
  list(
    # Batter: survival = good, so batter gains if survived (expected - actual)
    delta_batter = k_batter * (expected_wicket - actual_wicket),
    # Bowler: wicket = good, so bowler gains if got wicket (actual - expected)
    delta_bowler = k_bowler * (actual_wicket - expected_wicket),
    # Venue: wicket-friendly venue = higher wicket ELO (actual - expected)
    delta_venue_perm = k_venue_perm * (actual_wicket - expected_wicket),
    delta_venue_session = k_venue_session * (actual_wicket - expected_wicket)
  )
}


# ============================================================================
# ELO BOUNDING FUNCTIONS
# ============================================================================

#' Apply Bounds to Player ELO
#'
#' Ensures player ELO stays within defined bounds to prevent extreme drift.
#' Uses THREE_WAY_PLAYER_ELO_MIN and THREE_WAY_PLAYER_ELO_MAX constants.
#'
#' @param elo Numeric. Current or updated ELO rating.
#'
#' @return Numeric. Bounded ELO rating.
#' @export
bound_player_elo <- function(elo) {
  if (!THREE_WAY_APPLY_BOUNDS) {
    return(elo)
  }
  pmax(THREE_WAY_PLAYER_ELO_MIN, pmin(THREE_WAY_PLAYER_ELO_MAX, elo))
}


#' Apply Bounds to Venue ELO
#'
#' Ensures venue ELO stays within defined bounds to prevent extreme drift.
#' Venue bounds are tighter than player bounds since venue characteristics
#' should be more stable over time.
#'
#' Uses THREE_WAY_VENUE_ELO_MIN and THREE_WAY_VENUE_ELO_MAX constants.
#'
#' @param elo Numeric. Current or updated venue ELO rating.
#'
#' @return Numeric. Bounded venue ELO rating.
#' @export
bound_venue_elo <- function(elo) {
  if (!THREE_WAY_APPLY_BOUNDS) {
    return(elo)
  }
  pmax(THREE_WAY_VENUE_ELO_MIN, pmin(THREE_WAY_VENUE_ELO_MAX, elo))
}


#' Apply Bounds to All ELO Updates
#'
#' Convenience wrapper to apply appropriate bounds to a set of ELO updates.
#' Returns a list with bounded values for batter, bowler, and venue ELOs.
#'
#' @param batter_elo Numeric. Updated batter ELO.
#' @param bowler_elo Numeric. Updated bowler ELO.
#' @param venue_perm_elo Numeric. Updated venue permanent ELO.
#' @param venue_session_elo Numeric. Updated venue session ELO.
#'
#' @return Named list with bounded ELO values.
#' @keywords internal
bound_all_elos <- function(batter_elo, bowler_elo, venue_perm_elo, venue_session_elo) {
  list(
    batter = bound_player_elo(batter_elo),
    bowler = bound_player_elo(bowler_elo),
    venue_perm = bound_venue_elo(venue_perm_elo),
    venue_session = bound_venue_elo(venue_session_elo)
  )
}


# ============================================================================
# INACTIVITY DECAY
# ============================================================================

#' Apply Inactivity Decay to Player ELO
#'
#' Decays player ELO towards centrality-implied replacement level based on days inactive.
#' Players regress toward a replacement level appropriate for their league quality,
#' not a fixed global average.
#'
#' @param current_elo Numeric. Current ELO rating.
#' @param days_inactive Integer. Days since last match.
#' @param centrality_percentile Numeric (0-100). Player's or league's centrality percentile.
#'   If NULL, uses global replacement level (1500).
#'
#' @return Numeric. Decayed ELO rating.
#' @keywords internal
apply_inactivity_decay <- function(current_elo, days_inactive, centrality_percentile = NULL) {
  if (days_inactive <= THREE_WAY_INACTIVITY_THRESHOLD_DAYS) {
    return(current_elo)
  }

  # Calculate centrality-implied replacement level
 # Players from weaker leagues (low centrality) regress to lower replacement level
  # Players from stronger leagues (high centrality) regress to higher replacement level
  if (!is.null(centrality_percentile) && !is.na(centrality_percentile)) {
    replacement_level <- THREE_WAY_ELO_START +
      (centrality_percentile - 50) * CENTRALITY_ELO_PER_PERCENTILE
  } else {
    # Fallback to global replacement level if no centrality available
    replacement_level <- THREE_WAY_REPLACEMENT_LEVEL
  }

  # Exponential decay towards centrality-implied replacement level
  decay_factor <- exp(-days_inactive / THREE_WAY_INACTIVITY_HALFLIFE)

  replacement_level + (current_elo - replacement_level) * decay_factor
}


# ============================================================================
# NORMALIZATION
# ============================================================================

#' Normalize 3-Way ELOs After Match
#'
#' Applies post-match normalization to keep mean ELO at target.
#' Should be applied to each entity type and dimension separately.
#'
#' @param elos Numeric vector. Current ELO ratings for an entity type.
#' @param target_mean Numeric. Target mean ELO (default 1500).
#'
#' @return Numeric vector. Normalized ELO ratings.
#' @keywords internal
normalize_3way_elos <- function(elos, target_mean = THREE_WAY_ELO_TARGET_MEAN) {
  drift <- mean(elos, na.rm = TRUE) - target_mean

  if (abs(drift) <= THREE_WAY_NORMALIZATION_MIN_DRIFT) {
    return(elos)
  }

  # Correct portion of drift
  adjustment <- -drift * THREE_WAY_NORMALIZATION_CORRECTION_RATE
  elos + adjustment
}


# ============================================================================
# VENUE SESSION RESET
# ============================================================================

#' Reset Venue Session ELO for New Match
#'
#' Resets session ELO to neutral at the start of each match.
#'
#' @return Numeric. Starting session ELO (1500).
#' @keywords internal
reset_venue_session_elo <- function() {
  THREE_WAY_ELO_START
}


# ============================================================================
# SAMPLE-SIZE BAYESIAN BLENDING
# ============================================================================

#' Calculate Reliability Score
#'
#' Computes reliability (confidence) in a player's rating based on sample size.
#' Uses a hyperbolic formula where reliability approaches 1 asymptotically.
#'
#' Formula: reliability = balls / (balls + halflife)
#'
#' At halflife balls, reliability = 0.5 (50% weight on raw ELO).
#' This is mathematically equivalent to a Bayesian prior centered at replacement level.
#'
#' @param balls Integer. Number of balls faced/bowled.
#' @param halflife Numeric. Balls until 50% reliability.
#'   Default uses package constant THREE_WAY_RELIABILITY_HALFLIFE (500).
#'
#' @return Numeric. Reliability score between 0 and 1.
#' @keywords internal
calculate_reliability <- function(balls,
                                   halflife = THREE_WAY_RELIABILITY_HALFLIFE) {
  if (is.na(balls) || balls <= 0) {
    return(0)
  }
  balls / (balls + halflife)
}


#' Blend ELO with Replacement Level
#'
#' Blends a player's raw ELO toward replacement level based on sample size.
#' This Bayesian approach prevents extreme ELOs for small-sample players.
#'
#' Formula:
#'   effective_elo = raw_elo * reliability + replacement_level * (1 - reliability)
#'
#' Examples:
#'   - 0 balls: effective_elo = replacement_level (no data = prior)
#'   - 500 balls: effective_elo = (raw_elo + replacement_level) / 2
#'   - 2000 balls: effective_elo ≈ 0.8 * raw_elo + 0.2 * replacement_level
#'
#' @param raw_elo Numeric. Raw ELO rating from calculations.
#' @param balls Integer. Number of balls faced/bowled.
#' @param halflife Numeric. Balls until 50% weight on raw ELO.
#'   Default uses package constant THREE_WAY_RELIABILITY_HALFLIFE.
#' @param replacement_level Numeric. Prior/baseline ELO rating.
#'   Default uses package constant THREE_WAY_REPLACEMENT_LEVEL.
#'
#' @return Numeric. Blended effective ELO rating.
#' @keywords internal
blend_elo_with_replacement <- function(raw_elo,
                                        balls,
                                        halflife = THREE_WAY_RELIABILITY_HALFLIFE,
                                        replacement_level = THREE_WAY_REPLACEMENT_LEVEL) {
  if (is.na(raw_elo)) {
    return(replacement_level)
  }

  reliability <- calculate_reliability(balls, halflife)
  raw_elo * reliability + replacement_level * (1 - reliability)
}


#' Calculate Calibration K-Factor Multiplier
#'
#' Adjusts K-factor based on player calibration score.
#' Uncalibrated players (low score) get higher K to learn faster from
#' calibrated opponents. Calibrated players keep normal K.
#'
#' Formula:
#'   calibration_mult = 1 + (1 - calibration_score / 100) * k_boost
#'
#' Examples:
#'   - calibration_score = 0: K multiplier = 1 + k_boost (max boost)
#'   - calibration_score = 50: K multiplier = 1 + 0.5 * k_boost
#'   - calibration_score = 100: K multiplier = 1 (no boost)
#'
#' @param calibration_score Numeric. Player's calibration score (0-100).
#'   Higher = more exposure to anchor leagues.
#' @param k_boost Numeric. Maximum K boost for completely uncalibrated players.
#'   Default uses package constant THREE_WAY_CALIBRATION_K_BOOST.
#'
#' @return Numeric. K-factor multiplier (1.0 to 1 + k_boost).
#' @keywords internal
get_calibration_k_multiplier <- function(calibration_score,
                                          k_boost = THREE_WAY_CALIBRATION_K_BOOST) {
  # Handle NA/NULL

  if (is.null(calibration_score) || is.na(calibration_score)) {
    calibration_score <- 0  # Uncalibrated = max boost

  }

  # Clamp to 0-100 range
  calibration_score <- max(0, min(100, calibration_score))

  # Higher calibration = lower multiplier (closer to 1)
  1 + (1 - calibration_score / 100) * k_boost
}


#' Get Player-Level Tier K Multiplier
#'
#' Returns K-factor multiplier based on event tier for player ratings.
#' Lower K in weak leagues = ratings change slowly = less inflation.
#'
#' Uses THREE_WAY_PLAYER_TIER_K_MULT_* constants which are more aggressive
#' than the standard tier multipliers to combat isolated ecosystem inflation.
#'
#' @param event_tier Integer. Event tier (1-4).
#'
#' @return Numeric. K-factor multiplier.
#' @keywords internal
get_player_tier_k_multiplier <- function(event_tier) {
  switch(as.character(event_tier),
    "1" = THREE_WAY_PLAYER_TIER_K_MULT_1,
    "2" = THREE_WAY_PLAYER_TIER_K_MULT_2,
    "3" = THREE_WAY_PLAYER_TIER_K_MULT_3,
    "4" = THREE_WAY_PLAYER_TIER_K_MULT_4,
    THREE_WAY_PLAYER_TIER_K_MULT_2  # Default to tier 2
  )
}


# ============================================================================
# CENTRALITY CONTINUOUS REGRESSION (Stronger Bayesian Prior)
# ============================================================================

#' Apply Centrality Regression to ELO
#'
#' Applies a continuous "gravity" pull toward the player's centrality-implied ELO.
#' This creates a stronger Bayesian prior that prevents low-centrality players from
#' accumulating inflated ratings by dominating weak opponents in isolated ecosystems.
#'
#' The regression is LINEAR and proportional to the gap between current ELO
#' and centrality-implied ELO, providing consistent pull strength.
#'
#' Formula:
#'   implied_elo = ELO_START + (centrality_percentile - 50) * ELO_PER_PERCENTILE
#'   correction = REGRESSION_STRENGTH * (implied_elo - raw_elo)
#'   corrected_elo = raw_elo + correction
#'
#' Effect examples (with REGRESSION_STRENGTH = 0.002):
#'   - Player at 2400 ELO with 5% centrality (implied = 1220):
#'     correction = 0.002 * (1220 - 2400) = -2.36 per delivery
#'     Over 300 balls: ~700 point regression toward implied level
#'
#'   - Player at 1800 ELO with 90% centrality (implied = 1560):
#'     correction = 0.002 * (1560 - 1800) = -0.48 per delivery
#'     (Much smaller since high centrality justifies higher ELO)
#'
#' @param raw_elo Numeric. The player's current ELO after standard update.
#' @param centrality_percentile Numeric. Player's centrality percentile (0-100).
#'   Use get_centrality_as_of() or get_cold_start_percentile() to obtain.
#' @param regression_strength Numeric. Pull strength per delivery.
#'   Default uses CENTRALITY_REGRESSION_STRENGTH constant (0.002).
#' @param elo_per_percentile Numeric. ELO points per percentile point.
#'   Default uses CENTRALITY_ELO_PER_PERCENTILE constant (4).
#'
#' @return Numeric. The corrected ELO rating.
#'
#' @examples
#' # Low centrality player with inflated ELO
#' apply_centrality_correction(2400, 5)   # 2397.64 (-2.36 per delivery)
#'
#' # Elite player with high ELO (justified by centrality)
#' apply_centrality_correction(2000, 95)  # 1999.28 (small correction)
#'
#' # Average player at average ELO
#' apply_centrality_correction(1400, 50)  # 1400 (no correction needed)
#'
#' @export
apply_centrality_correction <- function(raw_elo,
                                         centrality_percentile,
                                         regression_strength = CENTRALITY_REGRESSION_STRENGTH,
                                         elo_per_percentile = CENTRALITY_ELO_PER_PERCENTILE) {
  # Handle NULL/NA centrality - no correction
  if (is.null(centrality_percentile) || is.na(centrality_percentile)) {
    return(raw_elo)
  }

  # Calculate centrality-implied ELO
  # 50th percentile → ELO_START, each percentile point = elo_per_percentile ELO
  cent_implied_elo <- THREE_WAY_ELO_START + (centrality_percentile - 50) * elo_per_percentile

  # Apply LINEAR regression toward implied ELO (stronger Bayesian prior)
  # Positive correction if below implied, negative if above
  correction <- regression_strength * (cent_implied_elo - raw_elo)

  # Return corrected ELO
  raw_elo + correction
}


#' Get Centrality-Implied ELO
#'
#' Calculates the ELO rating implied by a player's centrality percentile.
#' This is the "target" ELO that periodic correction pulls toward.
#'
#' Formula: ELO_START + (percentile - 50) * ELO_PER_PERCENTILE
#'
#' @param centrality_percentile Numeric. Player's centrality percentile (0-100).
#' @param elo_per_percentile Numeric. ELO points per percentile.
#'   Default uses CENTRALITY_ELO_PER_PERCENTILE constant.
#'
#' @return Numeric. The centrality-implied ELO rating.
#'
#' @examples
#' \dontrun{
#' get_centrality_implied_elo(50)   # 1400 (50th percentile = start)
#' get_centrality_implied_elo(100)  # 1600 (100th percentile = +200)
#' get_centrality_implied_elo(0)    # 1200 (0th percentile = -200)
#' }
#'
#' @keywords internal
get_centrality_implied_elo <- function(centrality_percentile,
                                        elo_per_percentile = CENTRALITY_ELO_PER_PERCENTILE) {
  if (is.null(centrality_percentile) || is.na(centrality_percentile)) {
    return(THREE_WAY_ELO_START)
  }

  THREE_WAY_ELO_START + (centrality_percentile - 50) * elo_per_percentile
}


# ============================================================================
# LEAGUE-ADJUSTED BASELINE (Environment-Aware Expected Runs)
# ============================================================================

#' Calculate League-Adjusted Expected Runs Baseline
#'
#' Blends the league's historical average runs per delivery with the global
#' format average based on sample size. This prevents ELO inflation in
#' high-scoring leagues and unfair penalties in low-scoring leagues.
#'
#' Formula: baseline = w * league_avg + (1-w) * global_avg
#' Where:   w = league_deliveries / (league_deliveries + HALFLIFE)
#'
#' @param league_avg_runs Numeric. The league's historical average runs per delivery.
#'   If NULL or NA, returns the global average.
#' @param league_deliveries Integer. Number of deliveries in this league so far.
#' @param global_avg_runs Numeric. The global format average runs per delivery.
#' @param blend_halflife Integer. Deliveries until 50% weight on league average.
#'   Default uses LEAGUE_BASELINE_BLEND_HALFLIFE constant (5000).
#'
#' @return Numeric. The blended expected runs baseline.
#'
#' @examples
#' \dontrun{
#' # New league (500 deliveries, avg 1.35) with global 1.28
#' calculate_league_baseline(1.35, 500, 1.28)   # ~1.286 (9% league weight)
#'
#' # Established IPL (20000 deliveries, avg 1.35)
#' calculate_league_baseline(1.35, 20000, 1.28) # ~1.336 (80% league weight)
#'
#' # No league data yet
#' calculate_league_baseline(NA, 0, 1.28)       # 1.28 (global average)
#' }
#'
#' @keywords internal
calculate_league_baseline <- function(league_avg_runs,
                                       league_deliveries,
                                       global_avg_runs,
                                       blend_halflife = LEAGUE_BASELINE_BLEND_HALFLIFE) {
  # Handle missing league data - use global average
 if (is.null(league_avg_runs) || is.na(league_avg_runs) ||
      is.null(league_deliveries) || league_deliveries <= 0) {
    return(global_avg_runs)
  }

  # Calculate blend weight (Bayesian-style based on sample size)
  blend_weight <- league_deliveries / (league_deliveries + blend_halflife)

  # Blend league and global averages
  blend_weight * league_avg_runs + (1 - blend_weight) * global_avg_runs
}


#' Build League Running Averages Lookup
#'
#' Pre-computes running average runs per delivery for each league, organized
#' by match date. This allows efficient lookup of "league average as of date X"
#' without data leakage.
#'
#' @param conn DBI connection to the database.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param gender Character. Gender: "male" or "female".
#'
#' @return Data frame with columns: event_name, match_date, cumulative_runs,
#'   cumulative_deliveries, running_avg_runs.
#'
#' @keywords internal
build_league_running_averages <- function(conn, format, gender) {
  format <- tolower(format)

  # Determine match types for this format using central helper
  match_types <- get_match_types_for_format(format)
  match_types_sql <- paste0("'", escape_sql_strings(match_types), "'", collapse = ", ")
  gender_escaped <- escape_sql_value(gender)

  query <- sprintf("
    WITH match_totals AS (
      SELECT
        m.event_name,
        m.match_date,
        m.match_id,
        SUM(d.runs_batter + d.runs_extras) as match_runs,
        COUNT(*) as match_deliveries
      FROM deliveries d
      JOIN matches m ON d.match_id = m.match_id
      WHERE m.match_type IN (%s)
        AND m.gender = '%s'
        AND m.event_name IS NOT NULL
      GROUP BY m.event_name, m.match_date, m.match_id
    ),
    ordered_matches AS (
      SELECT
        event_name,
        match_date,
        match_runs,
        match_deliveries,
        ROW_NUMBER() OVER (PARTITION BY event_name ORDER BY match_date, match_id) as match_num
      FROM match_totals
    )
    SELECT
      event_name,
      match_date,
      SUM(match_runs) OVER (PARTITION BY event_name ORDER BY match_num) as cumulative_runs,
      SUM(match_deliveries) OVER (PARTITION BY event_name ORDER BY match_num) as cumulative_deliveries
    FROM ordered_matches
    ORDER BY event_name, match_date
  ", match_types_sql, gender_escaped)

  result <- DBI::dbGetQuery(conn, query)

  if (nrow(result) > 0) {
    result$running_avg_runs <- ifelse(
      result$cumulative_deliveries > 0,
      result$cumulative_runs / result$cumulative_deliveries,
      0
    )
  }

  result
}


#' Get League Baseline As Of Date
#'
#' Looks up the league's running average and calculates the blended baseline
#' for a specific match date. Uses the most recent data BEFORE the match date
#' to prevent data leakage.
#'
#' @param event_name Character. The event/league name.
#' @param match_date Date or character. The match date.
#' @param league_lookup Data frame from build_league_running_averages().
#' @param global_avg_runs Numeric. The global format average.
#' @param blend_halflife Integer. Deliveries until 50% league weight.
#'
#' @return Numeric. The blended expected runs baseline for this league/date.
#'
#' @export
get_league_baseline_as_of <- function(event_name,
                                       match_date,
                                       league_lookup,
                                       global_avg_runs,
                                       blend_halflife = LEAGUE_BASELINE_BLEND_HALFLIFE) {
  # Handle missing event name
  if (is.null(event_name) || is.na(event_name) || event_name == "") {
    return(global_avg_runs)
  }

  # Filter to this league and dates BEFORE this match
  league_data <- league_lookup[league_lookup$event_name == event_name &
                                league_lookup$match_date < as.character(match_date), ]

  if (nrow(league_data) == 0) {
    # No prior data for this league - use global average
    return(global_avg_runs)
  }

  # Get most recent row (highest cumulative values)
  latest <- league_data[nrow(league_data), ]

  # Calculate blended baseline
  calculate_league_baseline(
    league_avg_runs = latest$running_avg_runs,
    league_deliveries = latest$cumulative_deliveries,
    global_avg_runs = global_avg_runs,
    blend_halflife = blend_halflife
  )
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

#' Get League-Specific Starting ELO
#'
#' Returns the starting ELO for a new player based on their debut league.
#' Players debuting in elite leagues (IPL, internationals) start higher than
#' players debuting in development leagues, providing a better prior.
#'
#' @param event_name Character. The event/league name (e.g., "Indian Premier League").
#' @param match_type Character. The match type (e.g., "T20", "IT20", "Test").
#'
#' @return Numeric. The starting ELO for players debuting in this event.
#' @export
get_league_starting_elo <- function(event_name, match_type = NULL) {
  # Handle NULL/NA
 if (is.null(event_name) || is.na(event_name) || event_name == "") {
    # Check if international match type
    if (!is.null(match_type) && toupper(match_type) %in% c("T20I", "IT20", "ODI", "TEST")) {
      return(THREE_WAY_LEAGUE_START_TIER_1)
    }
    return(THREE_WAY_ELO_START)  # Default fallback
  }

  event_lower <- tolower(event_name)

  # Check Tier 1 events
  for (t1 in THREE_WAY_TIER_1_EVENTS) {
    if (grepl(tolower(t1), event_lower, fixed = TRUE)) {
      return(THREE_WAY_LEAGUE_START_TIER_1)
    }
  }

  # Check Tier 2 events
  for (t2 in THREE_WAY_TIER_2_EVENTS) {
    if (grepl(tolower(t2), event_lower, fixed = TRUE)) {
      return(THREE_WAY_LEAGUE_START_TIER_2)
    }
  }

  # Check Tier 3 events
  for (t3 in THREE_WAY_TIER_3_EVENTS) {
    if (grepl(tolower(t3), event_lower, fixed = TRUE)) {
      return(THREE_WAY_LEAGUE_START_TIER_3)
    }
  }

  # Default to Tier 4 for unknown events
  THREE_WAY_LEAGUE_START_TIER_4
}


#' Determine Match Phase
#'
#' Determines the phase of the match based on over number and format.
#'
#' @param over Integer. Current over number (0-indexed).
#' @param format Character. Match format: "t20", "odi", or "test".
#'
#' @return Character. Phase: "powerplay", "middle", or "death".
#' @keywords internal
get_match_phase <- function(over, format = "t20") {
  format <- tolower(format)

  if (format == "t20") {
    if (over < 6) return("powerplay")
    if (over >= 16) return("death")
    return("middle")
  } else if (format == "odi") {
    if (over < 10) return("powerplay")
    if (over >= 40) return("death")
    return("middle")
  } else {
    # Test matches don't have phases in the same way
    return("middle")
  }
}


#' Check if High Chase Situation
#'
#' Determines if the current situation is a high-pressure chase.
#'
#' @param innings Integer. Current innings number.
#' @param runs_required Integer. Runs still needed to win.
#' @param balls_remaining Integer. Balls remaining in innings.
#'
#' @return Logical. TRUE if high chase situation.
#' @keywords internal
is_high_chase_situation <- function(innings, runs_required, balls_remaining) {
  if (innings != 2) return(FALSE)
  if (is.na(runs_required) || is.na(balls_remaining)) return(FALSE)
  if (balls_remaining <= 0) return(FALSE)

  required_run_rate <- runs_required / (balls_remaining / 6)
  required_run_rate > 10
}


#' Build 3-Way ELO Parameters for Format and Gender
#'
#' Constructs a parameter list for 3-way ELO calculation using
#' format-gender-specific optimized parameters.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param gender Character. Gender: "male" or "female". Default "male".
#'
#' @return List with all parameters needed for 3-way ELO calculation.
#' @keywords internal
build_3way_elo_params <- function(format = "t20", gender = "male") {
  format <- tolower(format)

  # Get format-gender-specific parameters using helper functions
  run_k <- get_run_k_factors(format, gender)
  wicket_k <- get_wicket_k_factors(format, gender)
  venue_k <- get_venue_k_factors(format, gender)
  run_weights <- get_run_elo_weights(format, gender)
  wicket_weights <- get_wicket_elo_weights(format, gender)

  # Player K-factor parameters
  player_params <- list(
    k_run_max = run_k$k_max,
    k_run_min = run_k$k_min,
    k_run_halflife = run_k$halflife,
    k_wicket_max = wicket_k$k_max,
    k_wicket_min = wicket_k$k_min,
    k_wicket_halflife = wicket_k$halflife,
    runs_per_100_elo = get_runs_per_100_elo(format, gender)
  )

  # Venue K-factor parameters
  venue_params <- list(
    k_venue_perm_max = venue_k$perm_max,
    k_venue_perm_min = venue_k$perm_min,
    k_venue_perm_halflife = venue_k$perm_halflife,
    k_venue_session_max = venue_k$session_max,
    k_venue_session_min = venue_k$session_min,
    k_venue_session_halflife = venue_k$session_halflife
  )

  # Run attribution weights (format-gender-specific)
  weight_params <- list(
    w_batter = run_weights$w_batter,
    w_bowler = run_weights$w_bowler,
    w_venue_perm = run_weights$w_venue_perm,
    w_venue_session = run_weights$w_venue_session,
    # Also include wicket weights
    w_batter_wicket = wicket_weights$w_batter,
    w_bowler_wicket = wicket_weights$w_bowler,
    w_venue_perm_wicket = wicket_weights$w_venue_perm,
    w_venue_session_wicket = wicket_weights$w_venue_session
  )

  # Inactivity parameters
  inactivity_params <- list(
    inactivity_threshold = THREE_WAY_INACTIVITY_THRESHOLD_DAYS,
    inactivity_halflife = THREE_WAY_INACTIVITY_HALFLIFE,
    inactivity_k_boost = THREE_WAY_INACTIVITY_K_BOOST_FACTOR,
    replacement_level = THREE_WAY_REPLACEMENT_LEVEL
  )

  # Situational modifiers
  situational_params <- list(
    high_chase_wicket_mult = THREE_WAY_HIGH_CHASE_WICKET_MULT,
    high_chase_run_mult = THREE_WAY_HIGH_CHASE_RUN_MULT,
    phase_powerplay_mult = THREE_WAY_PHASE_POWERPLAY_MULT,
    phase_middle_mult = THREE_WAY_PHASE_MIDDLE_MULT,
    phase_death_mult = THREE_WAY_PHASE_DEATH_MULT,
    knockout_mult = THREE_WAY_KNOCKOUT_MULT,
    tier_1_mult = THREE_WAY_TIER_1_MULT,
    tier_2_mult = THREE_WAY_TIER_2_MULT,
    tier_3_mult = THREE_WAY_TIER_3_MULT,
    tier_4_mult = THREE_WAY_TIER_4_MULT
  )

  # Common parameters
  common_params <- list(
    format = format,
    gender = gender,
    elo_start = THREE_WAY_ELO_START,
    elo_target_mean = THREE_WAY_ELO_TARGET_MEAN,
    elo_divisor = THREE_WAY_ELO_DIVISOR,
    wicket_elo_divisor = get_wicket_elo_divisor(format, gender),
    normalize_after_match = THREE_WAY_NORMALIZE_AFTER_MATCH,
    normalization_rate = THREE_WAY_NORMALIZATION_CORRECTION_RATE,
    normalization_min_drift = THREE_WAY_NORMALIZATION_MIN_DRIFT
  )

  c(player_params, venue_params, weight_params, inactivity_params,
    situational_params, common_params)
}


#' Get Stored 3-Way ELO Parameters
#'
#' Retrieves stored parameters from database.
#'
#' @param format Character. Format identifier.
#' @param conn A DuckDB connection object.
#'
#' @return List with parameters or NULL if not found.
#' @keywords internal
get_stored_3way_elo_params <- function(format, conn) {
  # Check if table exists
  if (!"three_way_elo_params" %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  result <- DBI::dbGetQuery(conn,
    "SELECT * FROM three_way_elo_params WHERE format = ?",
    params = list(format))

  if (nrow(result) == 0) return(NULL)

  as.list(result)
}


#' Store 3-Way ELO Parameters
#'
#' Stores calculation parameters in database.
#'
#' @param params List. Parameter values.
#' @param last_delivery_id Character. Last processed delivery ID.
#' @param last_match_date Date. Last processed match date.
#' @param total_deliveries Integer. Total deliveries processed.
#' @param conn A DuckDB connection object.
#'
#' @return Invisibly returns TRUE.
#' @keywords internal
store_3way_elo_params <- function(params, last_delivery_id, last_match_date,
                                   total_deliveries, conn) {
  # Ensure table exists
  ensure_3way_params_table(conn)

  # Delete existing params for this format
  DBI::dbExecute(conn,
    "DELETE FROM three_way_elo_params WHERE format = ?",
    params = list(params$format))

  # Insert new params
  DBI::dbExecute(conn, sprintf("
    INSERT INTO three_way_elo_params (
      format, k_run_max, k_run_min, k_run_halflife,
      k_wicket_max, k_wicket_min, k_wicket_halflife,
      k_venue_perm_max, k_venue_perm_min, k_venue_perm_halflife,
      k_venue_session_max, k_venue_session_min, k_venue_session_halflife,
      w_batter, w_bowler, w_venue_perm, w_venue_session,
      runs_per_100_elo, inactivity_halflife, replacement_level,
      last_delivery_id, last_match_date, total_deliveries, calculated_at
    ) VALUES (
      '%s', %f, %f, %f,
      %f, %f, %f,
      %f, %f, %f,
      %f, %f, %f,
      %f, %f, %f, %f,
      %f, %f, %f,
      '%s', '%s', %d, CURRENT_TIMESTAMP
    )
  ",
    params$format,
    params$k_run_max, params$k_run_min, params$k_run_halflife,
    params$k_wicket_max, params$k_wicket_min, params$k_wicket_halflife,
    params$k_venue_perm_max, params$k_venue_perm_min, params$k_venue_perm_halflife,
    params$k_venue_session_max, params$k_venue_session_min, params$k_venue_session_halflife,
    params$w_batter, params$w_bowler, params$w_venue_perm, params$w_venue_session,
    params$runs_per_100_elo, params$inactivity_halflife, params$replacement_level,
    last_delivery_id, last_match_date, total_deliveries
  ))

  invisible(TRUE)
}


#' Log 3-Way ELO Drift Metrics
#'
#' Logs drift metrics for monitoring.
#'
#' @param format Character. Format identifier.
#' @param dimension Character. "run" or "wicket".
#' @param entity_type Character. "batter", "bowler", or "venue".
#' @param mean_elo Numeric. Mean ELO for this entity type.
#' @param std_elo Numeric. Standard deviation of ELO.
#' @param conn A DuckDB connection object.
#'
#' @return Invisibly returns TRUE.
#' @keywords internal
log_3way_drift_metrics <- function(format, dimension, entity_type,
                                    mean_elo, std_elo, conn) {
  # Ensure table exists
  ensure_3way_drift_table(conn)

  DBI::dbExecute(conn, sprintf("
    INSERT OR REPLACE INTO three_way_elo_drift_metrics (
      metric_date, format, dimension, entity_type, mean_elo, std_elo
    ) VALUES (
      CURRENT_DATE, '%s', '%s', '%s', %f, %f
    )
  ", format, dimension, entity_type, mean_elo, std_elo))

  invisible(TRUE)
}


#' Ensure 3-Way ELO Drift Metrics Table Exists
#'
#' Creates the drift metrics table if it doesn't exist.
#'
#' @param conn A DuckDB connection object.
#'
#' @return Invisibly returns TRUE.
#' @keywords internal
ensure_3way_drift_table <- function(conn) {
  if (!"three_way_elo_drift_metrics" %in% DBI::dbListTables(conn)) {
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS three_way_elo_drift_metrics (
        metric_date DATE,
        format VARCHAR,
        dimension VARCHAR,
        entity_type VARCHAR,
        mean_elo DOUBLE,
        std_elo DOUBLE,
        PRIMARY KEY (metric_date, format, dimension, entity_type)
      )
    ")
  }
  invisible(TRUE)
}


#' Ensure 3-Way ELO Params Table Exists
#'
#' Creates the params table if it doesn't exist.
#'
#' @param conn A DuckDB connection object.
#'
#' @return Invisibly returns TRUE.
#' @keywords internal
ensure_3way_params_table <- function(conn) {
  if (!"three_way_elo_params" %in% DBI::dbListTables(conn)) {
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS three_way_elo_params (
        format VARCHAR PRIMARY KEY,
        k_run_max DOUBLE,
        k_run_min DOUBLE,
        k_run_halflife DOUBLE,
        k_wicket_max DOUBLE,
        k_wicket_min DOUBLE,
        k_wicket_halflife DOUBLE,
        k_venue_perm_max DOUBLE,
        k_venue_perm_min DOUBLE,
        k_venue_perm_halflife DOUBLE,
        k_venue_session_max DOUBLE,
        k_venue_session_min DOUBLE,
        k_venue_session_halflife DOUBLE,
        w_batter DOUBLE,
        w_bowler DOUBLE,
        w_venue_perm DOUBLE,
        w_venue_session DOUBLE,
        runs_per_100_elo DOUBLE,
        inactivity_halflife DOUBLE,
        replacement_level DOUBLE,
        last_delivery_id VARCHAR,
        last_match_date DATE,
        total_deliveries INTEGER,
        calculated_at TIMESTAMP
      )
    ")
  }
  invisible(TRUE)
}


NULL
