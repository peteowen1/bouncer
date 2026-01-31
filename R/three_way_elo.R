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
# EXPECTED VALUE CALCULATIONS
# ============================================================================

#' Calculate 3-Way Expected Runs
#'
#' Uses agnostic model as baseline with ELO-based adjustments from
#' batter, bowler, and venue ratings.
#'
#' @param agnostic_runs Numeric. Base expected runs from agnostic model.
#' @param batter_run_elo Numeric. Batter's run ELO.
#' @param bowler_run_elo Numeric. Bowler's run ELO.
#' @param venue_perm_run_elo Numeric. Venue's permanent run ELO.
#' @param venue_session_run_elo Numeric. Venue's session run ELO.
#' @param format Character. Match format: "t20", "odi", or "test".
#'
#' @return Numeric. Adjusted expected runs.
#' @keywords internal
calculate_3way_expected_runs <- function(agnostic_runs,
                                          batter_run_elo,
                                          bowler_run_elo,
                                          venue_perm_run_elo,
                                          venue_session_run_elo,
                                          format = "t20") {
  format <- tolower(format)

  # Get runs per ELO point for format
  runs_per_100_elo <- switch(format,
    "t20" = THREE_WAY_RUNS_PER_100_ELO_POINTS_T20,
    "odi" = THREE_WAY_RUNS_PER_100_ELO_POINTS_ODI,
    "test" = THREE_WAY_RUNS_PER_100_ELO_POINTS_TEST,
    THREE_WAY_RUNS_PER_100_ELO_POINTS_T20
  )

  # Convert to runs per ELO point (divide by 100)
  runs_per_elo <- runs_per_100_elo / 100

  # Calculate ELO contributions
  # Batter: higher = more runs
  batter_contrib <- (batter_run_elo - THREE_WAY_ELO_START) * runs_per_elo

  # Bowler: higher = FEWER runs (inverted)
  bowler_contrib <- (THREE_WAY_ELO_START - bowler_run_elo) * runs_per_elo

  # Venue: separate permanent and session contributions
  venue_perm_contrib <- (venue_perm_run_elo - THREE_WAY_ELO_START) * runs_per_elo
  venue_session_contrib <- (venue_session_run_elo - THREE_WAY_ELO_START) * runs_per_elo

  # Apply attribution weights (batter + bowler + venue_perm + venue_session = 1.0)
  expected_runs <- agnostic_runs +
    THREE_WAY_W_BATTER * batter_contrib +
    THREE_WAY_W_BOWLER * bowler_contrib +
    THREE_WAY_W_VENUE_PERM * venue_perm_contrib +
    THREE_WAY_W_VENUE_SESSION * venue_session_contrib

  # Bound to reasonable range (0 to 6 runs)
  max(0, min(6, expected_runs))
}


#' Calculate 3-Way Expected Wicket Probability
#'
#' Uses agnostic model as baseline with ELO-based adjustments on log-odds scale.
#'
#' @param agnostic_wicket Numeric. Base wicket probability from agnostic model.
#' @param batter_wicket_elo Numeric. Batter's wicket ELO (higher = survives better).
#' @param bowler_wicket_elo Numeric. Bowler's wicket ELO (higher = takes more).
#' @param venue_perm_wicket_elo Numeric. Venue permanent wicket ELO.
#' @param venue_session_wicket_elo Numeric. Venue session wicket ELO.
#'
#' @return Numeric. Adjusted expected wicket probability.
#' @keywords internal
calculate_3way_expected_wicket <- function(agnostic_wicket,
                                            batter_wicket_elo,
                                            bowler_wicket_elo,
                                            venue_perm_wicket_elo,
                                            venue_session_wicket_elo) {
  # Prevent log(0) or log(Inf)
  agnostic_wicket <- max(0.001, min(0.999, agnostic_wicket))

  # Convert to log-odds
  base_logit <- log(agnostic_wicket / (1 - agnostic_wicket))

  # Adjust log-odds based on ELO differences
  # Batter: higher ELO = lower wicket probability (subtract)
  # Bowler: higher ELO = higher wicket probability (add)
  # Venue: higher ELO = more wickets fall (add)
  adjusted_logit <- base_logit -
    THREE_WAY_W_BATTER * (batter_wicket_elo - THREE_WAY_ELO_START) / THREE_WAY_ELO_DIVISOR +
    THREE_WAY_W_BOWLER * (bowler_wicket_elo - THREE_WAY_ELO_START) / THREE_WAY_ELO_DIVISOR +
    THREE_WAY_W_VENUE_PERM * (venue_perm_wicket_elo - THREE_WAY_ELO_START) / THREE_WAY_ELO_DIVISOR +
    THREE_WAY_W_VENUE_SESSION * (venue_session_wicket_elo - THREE_WAY_ELO_START) / THREE_WAY_ELO_DIVISOR

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
                               use_player_tier_k = FALSE) {
  format <- tolower(format)

  # Get base K parameters
  if (elo_type == "wicket") {
    params <- switch(format,
      "t20" = list(
        k_max = THREE_WAY_K_WICKET_MAX_T20,
        k_min = THREE_WAY_K_WICKET_MIN_T20,
        halflife = THREE_WAY_K_WICKET_HALFLIFE_T20
      ),
      "odi" = list(
        k_max = THREE_WAY_K_WICKET_MAX_ODI,
        k_min = THREE_WAY_K_WICKET_MIN_ODI,
        halflife = THREE_WAY_K_WICKET_HALFLIFE_ODI
      ),
      "test" = list(
        k_max = THREE_WAY_K_WICKET_MAX_TEST,
        k_min = THREE_WAY_K_WICKET_MIN_TEST,
        halflife = THREE_WAY_K_WICKET_HALFLIFE_TEST
      ),
      list(
        k_max = THREE_WAY_K_WICKET_MAX_T20,
        k_min = THREE_WAY_K_WICKET_MIN_T20,
        halflife = THREE_WAY_K_WICKET_HALFLIFE_T20
      )
    )
  } else {
    params <- switch(format,
      "t20" = list(
        k_max = THREE_WAY_K_RUN_MAX_T20,
        k_min = THREE_WAY_K_RUN_MIN_T20,
        halflife = THREE_WAY_K_RUN_HALFLIFE_T20
      ),
      "odi" = list(
        k_max = THREE_WAY_K_RUN_MAX_ODI,
        k_min = THREE_WAY_K_RUN_MIN_ODI,
        halflife = THREE_WAY_K_RUN_HALFLIFE_ODI
      ),
      "test" = list(
        k_max = THREE_WAY_K_RUN_MAX_TEST,
        k_min = THREE_WAY_K_RUN_MIN_TEST,
        halflife = THREE_WAY_K_RUN_HALFLIFE_TEST
      ),
      list(
        k_max = THREE_WAY_K_RUN_MAX_T20,
        k_min = THREE_WAY_K_RUN_MIN_T20,
        halflife = THREE_WAY_K_RUN_HALFLIFE_T20
      )
    )
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

  k
}


#' Get 3-Way Venue Permanent K-Factor
#'
#' Calculates K-factor for permanent venue ELO (slow-learning).
#' Uses format-specific parameters optimized via Poisson grid search (Jan 2026).
#'
#' @param venue_total_balls Integer. Total balls at this venue.
#' @param format Character. Match format: "t20", "odi", or "test".
#'
#' @return Numeric. The K-factor for permanent venue ELO.
#' @keywords internal
get_3way_venue_perm_k <- function(venue_total_balls, format = "t20") {
  format <- tolower(format)

  params <- switch(format,
    "t20" = list(
      k_max = THREE_WAY_K_VENUE_PERM_MAX_T20,
      k_min = THREE_WAY_K_VENUE_PERM_MIN_T20,
      halflife = THREE_WAY_K_VENUE_PERM_HALFLIFE_T20
    ),
    "odi" = list(
      k_max = THREE_WAY_K_VENUE_PERM_MAX_ODI,
      k_min = THREE_WAY_K_VENUE_PERM_MIN_ODI,
      halflife = THREE_WAY_K_VENUE_PERM_HALFLIFE_ODI
    ),
    "test" = list(
      k_max = THREE_WAY_K_VENUE_PERM_MAX_TEST,
      k_min = THREE_WAY_K_VENUE_PERM_MIN_TEST,
      halflife = THREE_WAY_K_VENUE_PERM_HALFLIFE_TEST
    ),
    list(
      k_max = THREE_WAY_K_VENUE_PERM_MAX_T20,
      k_min = THREE_WAY_K_VENUE_PERM_MIN_T20,
      halflife = THREE_WAY_K_VENUE_PERM_HALFLIFE_T20
    )
  )

  params$k_min + (params$k_max - params$k_min) * exp(-venue_total_balls / params$halflife)
}


#' Get 3-Way Venue Session K-Factor
#'
#' Calculates K-factor for session venue ELO (fast-learning within match).
#' Resets each match to capture current conditions.
#' Uses format-specific parameters optimized via Poisson grid search (Jan 2026).
#'
#' @param balls_in_match Integer. Balls bowled in current match.
#' @param format Character. Match format: "t20", "odi", or "test".
#'
#' @return Numeric. The K-factor for session venue ELO.
#' @keywords internal
get_3way_venue_session_k <- function(balls_in_match, format = "t20") {
  format <- tolower(format)

  params <- switch(format,
    "t20" = list(
      k_max = THREE_WAY_K_VENUE_SESSION_MAX_T20,
      k_min = THREE_WAY_K_VENUE_SESSION_MIN_T20,
      halflife = THREE_WAY_K_VENUE_SESSION_HALFLIFE_T20
    ),
    "odi" = list(
      k_max = THREE_WAY_K_VENUE_SESSION_MAX_ODI,
      k_min = THREE_WAY_K_VENUE_SESSION_MIN_ODI,
      halflife = THREE_WAY_K_VENUE_SESSION_HALFLIFE_ODI
    ),
    "test" = list(
      k_max = THREE_WAY_K_VENUE_SESSION_MAX_TEST,
      k_min = THREE_WAY_K_VENUE_SESSION_MIN_TEST,
      halflife = THREE_WAY_K_VENUE_SESSION_HALFLIFE_TEST
    ),
    list(
      k_max = THREE_WAY_K_VENUE_SESSION_MAX_T20,
      k_min = THREE_WAY_K_VENUE_SESSION_MIN_T20,
      halflife = THREE_WAY_K_VENUE_SESSION_HALFLIFE_T20
    )
  )

  k <- params$k_max * exp(-balls_in_match / params$halflife)
  max(params$k_min, k)
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
# INACTIVITY DECAY
# ============================================================================

#' Apply Inactivity Decay to Player ELO
#'
#' Decays player ELO towards replacement level based on days inactive.
#'
#' @param current_elo Numeric. Current ELO rating.
#' @param days_inactive Integer. Days since last match.
#'
#' @return Numeric. Decayed ELO rating.
#' @keywords internal
apply_inactivity_decay <- function(current_elo, days_inactive) {
  if (days_inactive <= THREE_WAY_INACTIVITY_THRESHOLD_DAYS) {
    return(current_elo)
  }

  # Exponential decay towards replacement level
  decay_factor <- exp(-days_inactive / THREE_WAY_INACTIVITY_HALFLIFE)

  THREE_WAY_REPLACEMENT_LEVEL + (current_elo - THREE_WAY_REPLACEMENT_LEVEL) * decay_factor
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
# HELPER FUNCTIONS
# ============================================================================

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


#' Build 3-Way ELO Parameters for Format
#'
#' Constructs a parameter list for 3-way ELO calculation.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#'
#' @return List with all parameters needed for 3-way ELO calculation.
#' @keywords internal
build_3way_elo_params <- function(format = "t20") {
  format <- tolower(format)

  # Player K-factor parameters
  player_params <- switch(format,
    "t20" = list(
      k_run_max = THREE_WAY_K_RUN_MAX_T20,
      k_run_min = THREE_WAY_K_RUN_MIN_T20,
      k_run_halflife = THREE_WAY_K_RUN_HALFLIFE_T20,
      k_wicket_max = THREE_WAY_K_WICKET_MAX_T20,
      k_wicket_min = THREE_WAY_K_WICKET_MIN_T20,
      k_wicket_halflife = THREE_WAY_K_WICKET_HALFLIFE_T20,
      runs_per_100_elo = THREE_WAY_RUNS_PER_100_ELO_POINTS_T20
    ),
    "odi" = list(
      k_run_max = THREE_WAY_K_RUN_MAX_ODI,
      k_run_min = THREE_WAY_K_RUN_MIN_ODI,
      k_run_halflife = THREE_WAY_K_RUN_HALFLIFE_ODI,
      k_wicket_max = THREE_WAY_K_WICKET_MAX_ODI,
      k_wicket_min = THREE_WAY_K_WICKET_MIN_ODI,
      k_wicket_halflife = THREE_WAY_K_WICKET_HALFLIFE_ODI,
      runs_per_100_elo = THREE_WAY_RUNS_PER_100_ELO_POINTS_ODI
    ),
    "test" = list(
      k_run_max = THREE_WAY_K_RUN_MAX_TEST,
      k_run_min = THREE_WAY_K_RUN_MIN_TEST,
      k_run_halflife = THREE_WAY_K_RUN_HALFLIFE_TEST,
      k_wicket_max = THREE_WAY_K_WICKET_MAX_TEST,
      k_wicket_min = THREE_WAY_K_WICKET_MIN_TEST,
      k_wicket_halflife = THREE_WAY_K_WICKET_HALFLIFE_TEST,
      runs_per_100_elo = THREE_WAY_RUNS_PER_100_ELO_POINTS_TEST
    ),
    list(
      k_run_max = THREE_WAY_K_RUN_MAX_T20,
      k_run_min = THREE_WAY_K_RUN_MIN_T20,
      k_run_halflife = THREE_WAY_K_RUN_HALFLIFE_T20,
      k_wicket_max = THREE_WAY_K_WICKET_MAX_T20,
      k_wicket_min = THREE_WAY_K_WICKET_MIN_T20,
      k_wicket_halflife = THREE_WAY_K_WICKET_HALFLIFE_T20,
      runs_per_100_elo = THREE_WAY_RUNS_PER_100_ELO_POINTS_T20
    )
  )

  # Venue K-factor parameters (format-specific, optimized Jan 2026)
  venue_params <- switch(format,
    "t20" = list(
      k_venue_perm_max = THREE_WAY_K_VENUE_PERM_MAX_T20,
      k_venue_perm_min = THREE_WAY_K_VENUE_PERM_MIN_T20,
      k_venue_perm_halflife = THREE_WAY_K_VENUE_PERM_HALFLIFE_T20,
      k_venue_session_max = THREE_WAY_K_VENUE_SESSION_MAX_T20,
      k_venue_session_min = THREE_WAY_K_VENUE_SESSION_MIN_T20,
      k_venue_session_halflife = THREE_WAY_K_VENUE_SESSION_HALFLIFE_T20
    ),
    "odi" = list(
      k_venue_perm_max = THREE_WAY_K_VENUE_PERM_MAX_ODI,
      k_venue_perm_min = THREE_WAY_K_VENUE_PERM_MIN_ODI,
      k_venue_perm_halflife = THREE_WAY_K_VENUE_PERM_HALFLIFE_ODI,
      k_venue_session_max = THREE_WAY_K_VENUE_SESSION_MAX_ODI,
      k_venue_session_min = THREE_WAY_K_VENUE_SESSION_MIN_ODI,
      k_venue_session_halflife = THREE_WAY_K_VENUE_SESSION_HALFLIFE_ODI
    ),
    "test" = list(
      k_venue_perm_max = THREE_WAY_K_VENUE_PERM_MAX_TEST,
      k_venue_perm_min = THREE_WAY_K_VENUE_PERM_MIN_TEST,
      k_venue_perm_halflife = THREE_WAY_K_VENUE_PERM_HALFLIFE_TEST,
      k_venue_session_max = THREE_WAY_K_VENUE_SESSION_MAX_TEST,
      k_venue_session_min = THREE_WAY_K_VENUE_SESSION_MIN_TEST,
      k_venue_session_halflife = THREE_WAY_K_VENUE_SESSION_HALFLIFE_TEST
    ),
    list(
      k_venue_perm_max = THREE_WAY_K_VENUE_PERM_MAX_T20,
      k_venue_perm_min = THREE_WAY_K_VENUE_PERM_MIN_T20,
      k_venue_perm_halflife = THREE_WAY_K_VENUE_PERM_HALFLIFE_T20,
      k_venue_session_max = THREE_WAY_K_VENUE_SESSION_MAX_T20,
      k_venue_session_min = THREE_WAY_K_VENUE_SESSION_MIN_T20,
      k_venue_session_halflife = THREE_WAY_K_VENUE_SESSION_HALFLIFE_T20
    )
  )

  # Attribution weights (optimized Jan 2026: 70% player, 30% venue)
  weight_params <- list(
    w_batter = THREE_WAY_W_BATTER,
    w_bowler = THREE_WAY_W_BOWLER,
    w_venue_perm = THREE_WAY_W_VENUE_PERM,
    w_venue_session = THREE_WAY_W_VENUE_SESSION
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
    elo_start = THREE_WAY_ELO_START,
    elo_target_mean = THREE_WAY_ELO_TARGET_MEAN,
    elo_divisor = THREE_WAY_ELO_DIVISOR,
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

  result <- DBI::dbGetQuery(conn, sprintf("
    SELECT * FROM three_way_elo_params WHERE format = '%s'
  ", format))

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
  DBI::dbExecute(conn, sprintf("
    DELETE FROM three_way_elo_params WHERE format = '%s'
  ", params$format))

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
