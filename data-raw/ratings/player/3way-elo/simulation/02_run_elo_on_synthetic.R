# Run ELO on Synthetic Data ----
#
# Adapts the core 3-Way ELO calculation loop to work on in-memory synthetic data.
# This is a simplified version of 01_calculate_3way_elo.R focused on essential
# ELO mechanics without database I/O or advanced features like centrality.
#
# Key simplifications:
#   - No database operations (in-memory only)
#   - No centrality correction (just core ELO updates)
#   - No inactivity decay (synthetic data has no gaps)
#   - No normalization (focus on raw skill recovery)
#   - Single format (T20 parameters)

# Load dependencies
library(data.table)

# Load configuration and package
source(file.path(dirname(sys.frame(1)$ofile), "00_simulation_config.R"))

# ============================================================================
# MAIN ELO CALCULATION FUNCTION
# ============================================================================

#' Run 3-Way ELO on Synthetic Deliveries
#'
#' Processes deliveries chronologically, updating ELOs for batters, bowlers,
#' and venues based on run outcomes.
#'
#' @param deliveries data.table. Synthetic deliveries from generate_synthetic_deliveries().
#' @param params List. ELO parameters. If NULL, uses package defaults.
#' @param verbose Logical. Print progress messages.
#'
#' @return List with:
#'   - final_elos: List with batter_run, batter_wicket, bowler_run, bowler_wicket,
#'                 venue_perm_run, venue_perm_wicket vectors
#'   - predictions: data.table with delivery-level predictions
#'   - params: Parameters used
run_elo_on_synthetic <- function(deliveries,
                                  params = NULL,
                                  verbose = TRUE) {
  # Use package constants if no params provided
  if (is.null(params)) {
    params <- list(
      # K-factors (T20)
      k_run_max = THREE_WAY_K_RUN_MAX_MENS_T20,
      k_run_min = THREE_WAY_K_RUN_MIN_MENS_T20,
      k_run_halflife = THREE_WAY_K_RUN_HALFLIFE_MENS_T20,

      k_wicket_max = THREE_WAY_K_WICKET_MAX_MENS_T20,
      k_wicket_min = THREE_WAY_K_WICKET_MIN_MENS_T20,
      k_wicket_halflife = THREE_WAY_K_WICKET_HALFLIFE_MENS_T20,

      k_venue_perm_max = THREE_WAY_K_VENUE_PERM_MAX_MENS_T20,
      k_venue_perm_min = THREE_WAY_K_VENUE_PERM_MIN_MENS_T20,
      k_venue_perm_halflife = THREE_WAY_K_VENUE_PERM_HALFLIFE_MENS_T20,

      k_venue_session_max = THREE_WAY_K_VENUE_SESSION_MAX_MENS_T20,
      k_venue_session_min = THREE_WAY_K_VENUE_SESSION_MIN_MENS_T20,
      k_venue_session_halflife = THREE_WAY_K_VENUE_SESSION_HALFLIFE_MENS_T20,

      # Attribution weights
      w_batter = THREE_WAY_W_BATTER_MENS_T20,
      w_bowler = THREE_WAY_W_BOWLER_MENS_T20,
      w_venue_perm = THREE_WAY_W_VENUE_PERM_MENS_T20,
      w_venue_session = THREE_WAY_W_VENUE_SESSION_MENS_T20,

      w_batter_wicket = THREE_WAY_W_BATTER_WICKET_MENS_T20,
      w_bowler_wicket = THREE_WAY_W_BOWLER_WICKET_MENS_T20,
      w_venue_perm_wicket = THREE_WAY_W_VENUE_PERM_WICKET_MENS_T20,
      w_venue_session_wicket = THREE_WAY_W_VENUE_SESSION_WICKET_MENS_T20,

      # ELO conversion
      runs_per_100_elo = THREE_WAY_RUNS_PER_100_ELO_POINTS_MENS_T20,
      wicket_elo_divisor = THREE_WAY_WICKET_ELO_DIVISOR_MENS_T20,

      # Starting values
      elo_start = THREE_WAY_ELO_START
    )
  }

  runs_per_elo <- params$runs_per_100_elo / 100

  n <- nrow(deliveries)
  if (verbose) cat(sprintf("Processing %s deliveries...\n", format(n, big.mark = ",")))

  # Get unique entities
  all_batters <- unique(deliveries$batter_id)
  all_bowlers <- unique(deliveries$bowler_id)
  all_venues <- unique(deliveries$venue)

  if (verbose) {
    cat(sprintf("  Entities: %d batters, %d bowlers, %d venues\n",
                length(all_batters), length(all_bowlers), length(all_venues)))
  }

  # Initialize ELO environments (fast hash lookup)
  batter_run_elo <- new.env(hash = TRUE, size = length(all_batters))
  batter_wicket_elo <- new.env(hash = TRUE, size = length(all_batters))
  bowler_run_elo <- new.env(hash = TRUE, size = length(all_bowlers))
  bowler_wicket_elo <- new.env(hash = TRUE, size = length(all_bowlers))
  venue_perm_run_elo <- new.env(hash = TRUE, size = length(all_venues))
  venue_perm_wicket_elo <- new.env(hash = TRUE, size = length(all_venues))

  # Experience counters
  batter_balls <- new.env(hash = TRUE, size = length(all_batters))
  bowler_balls <- new.env(hash = TRUE, size = length(all_bowlers))
  venue_balls <- new.env(hash = TRUE, size = length(all_venues))

  # Initialize all entities at start ELO
  for (b in all_batters) {
    batter_run_elo[[b]] <- params$elo_start
    batter_wicket_elo[[b]] <- params$elo_start
    batter_balls[[b]] <- 0L
  }
  for (b in all_bowlers) {
    bowler_run_elo[[b]] <- params$elo_start
    bowler_wicket_elo[[b]] <- params$elo_start
    bowler_balls[[b]] <- 0L
  }
  for (v in all_venues) {
    venue_perm_run_elo[[v]] <- params$elo_start
    venue_perm_wicket_elo[[v]] <- params$elo_start
    venue_balls[[v]] <- 0L
  }

  # Pre-extract columns for speed
  delivery_ids <- deliveries$delivery_id
  match_ids <- deliveries$match_id
  batter_ids <- deliveries$batter_id
  bowler_ids <- deliveries$bowler_id
  venues_vec <- deliveries$venue
  actual_runs_vec <- deliveries$actual_runs
  is_wicket_vec <- deliveries$is_wicket
  baseline_runs_vec <- deliveries$baseline_runs
  baseline_wicket_vec <- deliveries$baseline_wicket

  # Pre-allocate prediction storage
  pred_runs <- numeric(n)
  pred_wicket <- numeric(n)
  batter_run_elo_vec <- numeric(n)
  bowler_run_elo_vec <- numeric(n)
  venue_perm_run_elo_vec <- numeric(n)
  batter_wicket_elo_vec <- numeric(n)
  bowler_wicket_elo_vec <- numeric(n)
  venue_perm_wicket_elo_vec <- numeric(n)

  # Track session ELO (resets per match)
  current_match <- ""
  venue_session_run <- params$elo_start
  venue_session_wicket <- params$elo_start
  match_balls <- 0L

  # Main loop
  for (i in seq_len(n)) {
    batter_id <- batter_ids[i]
    bowler_id <- bowler_ids[i]
    venue <- venues_vec[i]
    match_id <- match_ids[i]

    # Reset session ELO on new match
    if (match_id != current_match) {
      current_match <- match_id
      venue_session_run <- params$elo_start
      venue_session_wicket <- params$elo_start
      match_balls <- 0L
    }

    # Get current ELOs (before update)
    bat_run <- batter_run_elo[[batter_id]]
    bat_wicket <- batter_wicket_elo[[batter_id]]
    bowl_run <- bowler_run_elo[[bowler_id]]
    bowl_wicket <- bowler_wicket_elo[[bowler_id]]
    venue_perm_run <- venue_perm_run_elo[[venue]]
    venue_perm_wicket <- venue_perm_wicket_elo[[venue]]

    # Get experience counts
    bat_exp <- batter_balls[[batter_id]]
    bowl_exp <- bowler_balls[[bowler_id]]
    venue_exp <- venue_balls[[venue]]

    # Store ELOs for output
    batter_run_elo_vec[i] <- bat_run
    bowler_run_elo_vec[i] <- bowl_run
    venue_perm_run_elo_vec[i] <- venue_perm_run
    batter_wicket_elo_vec[i] <- bat_wicket
    bowler_wicket_elo_vec[i] <- bowl_wicket
    venue_perm_wicket_elo_vec[i] <- venue_perm_wicket

    # Calculate expected runs
    baseline <- baseline_runs_vec[i]
    combined_elo_diff <-
      params$w_batter * (bat_run - params$elo_start) +
      params$w_bowler * (params$elo_start - bowl_run) +
      params$w_venue_perm * (venue_perm_run - params$elo_start) +
      params$w_venue_session * (venue_session_run - params$elo_start)

    expected_runs <- baseline + combined_elo_diff * runs_per_elo
    expected_runs <- max(0.01, min(6, expected_runs))
    pred_runs[i] <- expected_runs

    # Calculate expected wicket (log-odds approach)
    baseline_wicket <- baseline_wicket_vec[i]
    baseline_wicket <- max(0.001, min(0.999, baseline_wicket))
    base_logit <- log(baseline_wicket / (1 - baseline_wicket))

    adjusted_logit <- base_logit -
      params$w_batter_wicket * (bat_wicket - params$elo_start) / params$wicket_elo_divisor +
      params$w_bowler_wicket * (bowl_wicket - params$elo_start) / params$wicket_elo_divisor +
      params$w_venue_perm_wicket * (venue_perm_wicket - params$elo_start) / params$wicket_elo_divisor +
      params$w_venue_session_wicket * (venue_session_wicket - params$elo_start) / params$wicket_elo_divisor

    expected_wicket <- 1 / (1 + exp(-adjusted_logit))
    expected_wicket <- max(0.001, min(0.5, expected_wicket))
    pred_wicket[i] <- expected_wicket

    # Get actual outcomes
    actual_runs <- actual_runs_vec[i]
    is_wicket <- is_wicket_vec[i]

    # Calculate K-factors based on experience
    k_batter_run <- params$k_run_min +
      (params$k_run_max - params$k_run_min) * exp(-bat_exp / params$k_run_halflife)
    k_bowler_run <- params$k_run_min +
      (params$k_run_max - params$k_run_min) * exp(-bowl_exp / params$k_run_halflife)
    k_venue_perm_run <- params$k_venue_perm_min +
      (params$k_venue_perm_max - params$k_venue_perm_min) * exp(-venue_exp / params$k_venue_perm_halflife)
    k_venue_session_run <- max(params$k_venue_session_min,
      params$k_venue_session_max * exp(-match_balls / params$k_venue_session_halflife))

    k_batter_wicket <- params$k_wicket_min +
      (params$k_wicket_max - params$k_wicket_min) * exp(-bat_exp / params$k_wicket_halflife)
    k_bowler_wicket <- params$k_wicket_min +
      (params$k_wicket_max - params$k_wicket_min) * exp(-bowl_exp / params$k_wicket_halflife)
    k_venue_perm_wicket <- params$k_venue_perm_min +
      (params$k_venue_perm_max - params$k_venue_perm_min) * exp(-venue_exp / params$k_venue_perm_halflife)
    k_venue_session_wicket <- max(params$k_venue_session_min,
      params$k_venue_session_max * exp(-match_balls / params$k_venue_session_halflife))

    # Calculate run residual and updates
    run_delta <- actual_runs - expected_runs

    batter_run_elo[[batter_id]] <- bat_run + k_batter_run * run_delta
    bowler_run_elo[[bowler_id]] <- bowl_run - k_bowler_run * run_delta  # Inverted
    venue_perm_run_elo[[venue]] <- venue_perm_run + k_venue_perm_run * run_delta
    venue_session_run <- venue_session_run + k_venue_session_run * run_delta

    # Calculate wicket updates
    wicket_val <- if (is_wicket) 1 else 0

    batter_wicket_elo[[batter_id]] <- bat_wicket + k_batter_wicket * (expected_wicket - wicket_val)
    bowler_wicket_elo[[bowler_id]] <- bowl_wicket + k_bowler_wicket * (wicket_val - expected_wicket)
    venue_perm_wicket_elo[[venue]] <- venue_perm_wicket + k_venue_perm_wicket * (wicket_val - expected_wicket)
    venue_session_wicket <- venue_session_wicket + k_venue_session_wicket * (wicket_val - expected_wicket)

    # Update experience counters
    batter_balls[[batter_id]] <- bat_exp + 1L
    bowler_balls[[bowler_id]] <- bowl_exp + 1L
    venue_balls[[venue]] <- venue_exp + 1L
    match_balls <- match_balls + 1L

    # Progress
    if (verbose && i %% 25000 == 0) {
      cat(sprintf("  Processed %s deliveries...\n", format(i, big.mark = ",")))
    }
  }

  if (verbose) cat("  Done!\n")

  # Extract final ELOs
  final_batter_run <- sapply(all_batters, function(b) batter_run_elo[[b]])
  final_batter_wicket <- sapply(all_batters, function(b) batter_wicket_elo[[b]])
  final_bowler_run <- sapply(all_bowlers, function(b) bowler_run_elo[[b]])
  final_bowler_wicket <- sapply(all_bowlers, function(b) bowler_wicket_elo[[b]])
  final_venue_perm_run <- sapply(all_venues, function(v) venue_perm_run_elo[[v]])
  final_venue_perm_wicket <- sapply(all_venues, function(v) venue_perm_wicket_elo[[v]])

  names(final_batter_run) <- all_batters
  names(final_batter_wicket) <- all_batters
  names(final_bowler_run) <- all_bowlers
  names(final_bowler_wicket) <- all_bowlers
  names(final_venue_perm_run) <- all_venues
  names(final_venue_perm_wicket) <- all_venues

  # Create predictions data.table
  predictions <- data.table::data.table(
    delivery_id = delivery_ids,
    batter_id = batter_ids,
    bowler_id = bowler_ids,
    venue = venues_vec,
    predicted_runs = pred_runs,
    actual_runs = actual_runs_vec,
    predicted_wicket = pred_wicket,
    actual_wicket = is_wicket_vec,
    batter_run_elo = batter_run_elo_vec,
    bowler_run_elo = bowler_run_elo_vec,
    venue_perm_run_elo = venue_perm_run_elo_vec,
    batter_wicket_elo = batter_wicket_elo_vec,
    bowler_wicket_elo = bowler_wicket_elo_vec,
    venue_perm_wicket_elo = venue_perm_wicket_elo_vec
  )

  list(
    final_elos = list(
      batter_run = final_batter_run,
      batter_wicket = final_batter_wicket,
      bowler_run = final_bowler_run,
      bowler_wicket = final_bowler_wicket,
      venue_perm_run = final_venue_perm_run,
      venue_perm_wicket = final_venue_perm_wicket
    ),
    predictions = predictions,
    params = params
  )
}


# ============================================================================
# LOSS FUNCTIONS
# ============================================================================

#' Calculate Poisson Loss on Run Predictions
#'
#' Poisson deviance: mean(pred - actual * log(pred))
#'
#' @param predicted Numeric vector. Predicted runs.
#' @param actual Numeric vector. Actual runs.
#'
#' @return Numeric. Poisson loss value.
calculate_poisson_loss <- function(predicted, actual) {
  # Ensure positive predictions for log
  predicted <- pmax(0.01, predicted)
  mean(predicted - actual * log(predicted))
}


#' Calculate Log-Loss on Wicket Predictions
#'
#' Binary cross-entropy: -mean(actual * log(pred) + (1-actual) * log(1-pred))
#'
#' @param predicted Numeric vector. Predicted wicket probabilities.
#' @param actual Logical/Integer vector. Actual wickets (0/1).
#'
#' @return Numeric. Log-loss value.
calculate_wicket_logloss <- function(predicted, actual) {
  # Bound predictions away from 0 and 1
  predicted <- pmax(0.001, pmin(0.999, predicted))
  actual_num <- as.numeric(actual)
  -mean(actual_num * log(predicted) + (1 - actual_num) * log(1 - predicted))
}


#' Calculate Mean Absolute Error
#'
#' @param predicted Numeric vector. Predicted values.
#' @param actual Numeric vector. Actual values.
#'
#' @return Numeric. MAE.
calculate_mae <- function(predicted, actual) {
  mean(abs(predicted - actual))
}


# ============================================================================
# EVALUATION FUNCTION
# ============================================================================

#' Evaluate ELO Model on Test Set
#'
#' Uses the final ELOs from training to predict on test data,
#' then calculates loss metrics.
#'
#' @param elo_results List from run_elo_on_synthetic().
#' @param test_deliveries data.table. Test deliveries.
#' @param verbose Logical. Print results.
#'
#' @return List with loss metrics.
evaluate_on_test_set <- function(elo_results, test_deliveries, verbose = TRUE) {
  params <- elo_results$params
  final_elos <- elo_results$final_elos

  n <- nrow(test_deliveries)

  # Pre-extract columns
  batter_ids <- test_deliveries$batter_id
  bowler_ids <- test_deliveries$bowler_id
  venues <- test_deliveries$venue
  baseline_runs <- test_deliveries$baseline_runs
  baseline_wicket <- test_deliveries$baseline_wicket
  actual_runs <- test_deliveries$actual_runs
  actual_wicket <- test_deliveries$is_wicket

  runs_per_elo <- params$runs_per_100_elo / 100

  # Calculate predictions using final ELOs (no updates)
  pred_runs <- numeric(n)
  pred_wicket <- numeric(n)

  for (i in seq_len(n)) {
    batter <- batter_ids[i]
    bowler <- bowler_ids[i]
    venue <- venues[i]

    # Get final ELOs (use start if entity not seen in training)
    bat_run <- if (batter %in% names(final_elos$batter_run)) {
      final_elos$batter_run[batter]
    } else {
      params$elo_start
    }
    bowl_run <- if (bowler %in% names(final_elos$bowler_run)) {
      final_elos$bowler_run[bowler]
    } else {
      params$elo_start
    }
    venue_run <- if (venue %in% names(final_elos$venue_perm_run)) {
      final_elos$venue_perm_run[venue]
    } else {
      params$elo_start
    }

    bat_wicket <- if (batter %in% names(final_elos$batter_wicket)) {
      final_elos$batter_wicket[batter]
    } else {
      params$elo_start
    }
    bowl_wicket <- if (bowler %in% names(final_elos$bowler_wicket)) {
      final_elos$bowler_wicket[bowler]
    } else {
      params$elo_start
    }
    venue_wicket <- if (venue %in% names(final_elos$venue_perm_wicket)) {
      final_elos$venue_perm_wicket[venue]
    } else {
      params$elo_start
    }

    # Calculate expected runs
    combined_diff <-
      params$w_batter * (bat_run - params$elo_start) +
      params$w_bowler * (params$elo_start - bowl_run) +
      params$w_venue_perm * (venue_run - params$elo_start)

    pred_runs[i] <- max(0.01, min(6, baseline_runs[i] + combined_diff * runs_per_elo))

    # Calculate expected wicket
    base_wkt <- max(0.001, min(0.999, baseline_wicket[i]))
    base_logit <- log(base_wkt / (1 - base_wkt))

    adj_logit <- base_logit -
      params$w_batter_wicket * (bat_wicket - params$elo_start) / params$wicket_elo_divisor +
      params$w_bowler_wicket * (bowl_wicket - params$elo_start) / params$wicket_elo_divisor +
      params$w_venue_perm_wicket * (venue_wicket - params$elo_start) / params$wicket_elo_divisor

    pred_wicket[i] <- max(0.001, min(0.5, 1 / (1 + exp(-adj_logit))))
  }

  # Calculate metrics
  run_poisson <- calculate_poisson_loss(pred_runs, actual_runs)
  run_mae <- calculate_mae(pred_runs, actual_runs)
  wicket_logloss <- calculate_wicket_logloss(pred_wicket, actual_wicket)

  # Null model comparison
  null_run_poisson <- calculate_poisson_loss(rep(mean(baseline_runs), n), actual_runs)
  null_wicket_logloss <- calculate_wicket_logloss(rep(mean(baseline_wicket), n), actual_wicket)

  if (verbose) {
    cat("\n=== Test Set Evaluation ===\n")
    cat(sprintf("Run Prediction:\n"))
    cat(sprintf("  Poisson Loss: %.4f (null: %.4f, improvement: %.2f%%)\n",
                run_poisson, null_run_poisson,
                100 * (null_run_poisson - run_poisson) / null_run_poisson))
    cat(sprintf("  MAE: %.4f\n", run_mae))
    cat(sprintf("\nWicket Prediction:\n"))
    cat(sprintf("  Log-Loss: %.4f (null: %.4f, improvement: %.2f%%)\n",
                wicket_logloss, null_wicket_logloss,
                100 * (null_wicket_logloss - wicket_logloss) / null_wicket_logloss))
  }

  list(
    run_poisson_loss = run_poisson,
    run_mae = run_mae,
    wicket_logloss = wicket_logloss,
    null_run_poisson = null_run_poisson,
    null_wicket_logloss = null_wicket_logloss,
    run_improvement_pct = 100 * (null_run_poisson - run_poisson) / null_run_poisson,
    wicket_improvement_pct = 100 * (null_wicket_logloss - wicket_logloss) / null_wicket_logloss
  )
}

NULL
