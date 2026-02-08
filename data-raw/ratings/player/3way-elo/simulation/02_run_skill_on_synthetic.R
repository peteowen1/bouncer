# Run Skill Indices on Synthetic Data ----
#
# Adapts the core Skill Index calculation loop to work on in-memory synthetic data.
# This is the skill index equivalent of 02_run_elo_on_synthetic.R.
#
# The key difference from ELO:
#   - Skills start at 0 (neutral) instead of 1400
#   - Skills are directly interpretable (runs/ball deviation)
#   - Continuous decay toward 0 on every delivery
#   - Simple additive formula: expected = baseline + weighted skill sum
#
# Key simplifications (same as ELO version):
#   - No database operations (in-memory only)
#   - No inactivity decay (synthetic data has no gaps)
#   - Single format (T20 parameters)

# Load dependencies
library(data.table)

# Load configuration and package
source(file.path(dirname(sys.frame(1)$ofile), "00_simulation_config.R"))

# ============================================================================
# MAIN SKILL INDEX CALCULATION FUNCTION
# ============================================================================

#' Run Skill Indices on Synthetic Deliveries
#'
#' Processes deliveries chronologically, updating skill indices for batters,
#' bowlers, and venues based on run outcomes.
#'
#' @param deliveries data.table. Synthetic deliveries from generate_synthetic_deliveries().
#' @param params List. Skill index parameters. If NULL, uses package defaults.
#' @param verbose Logical. Print progress messages.
#'
#' @return List with:
#'   - final_skills: List with batter_run, batter_wicket, bowler_run, bowler_wicket,
#'                   venue_perm_run, venue_perm_wicket vectors
#'   - predictions: data.table with delivery-level predictions
#'   - params: Parameters used
run_skill_on_synthetic <- function(deliveries,
                                    params = NULL,
                                    verbose = TRUE) {
  # Use package constants if no params provided
  if (is.null(params)) {
    params <- list(
      # Alpha (learning rate) parameters - Men's T20
      alpha_run_max = SKILL_ALPHA_RUN_MAX_MENS_T20,
      alpha_run_min = SKILL_ALPHA_RUN_MIN_MENS_T20,
      alpha_run_halflife = SKILL_ALPHA_RUN_HALFLIFE_MENS_T20,

      alpha_wicket_max = SKILL_ALPHA_WICKET_MAX_MENS_T20,
      alpha_wicket_min = SKILL_ALPHA_WICKET_MIN_MENS_T20,
      alpha_wicket_halflife = SKILL_ALPHA_WICKET_HALFLIFE_MENS_T20,

      # Venue alpha
      venue_alpha_perm = SKILL_VENUE_ALPHA_PERM_T20,
      venue_alpha_session_max = SKILL_VENUE_ALPHA_SESSION_MAX,
      venue_alpha_session_min = SKILL_VENUE_ALPHA_SESSION_MIN,
      venue_alpha_session_halflife = SKILL_VENUE_ALPHA_SESSION_HALFLIFE,

      # Attribution weights - Run
      w_batter_run = SKILL_W_BATTER_RUN_MENS_T20,
      w_bowler_run = SKILL_W_BOWLER_RUN_MENS_T20,
      w_venue_perm_run = SKILL_W_VENUE_PERM_RUN_MENS_T20,
      w_venue_session_run = SKILL_W_VENUE_SESSION_RUN_MENS_T20,

      # Attribution weights - Wicket
      w_batter_wicket = SKILL_W_BATTER_WICKET_MENS_T20,
      w_bowler_wicket = SKILL_W_BOWLER_WICKET_MENS_T20,
      w_venue_perm_wicket = SKILL_W_VENUE_PERM_WICKET_MENS_T20,
      w_venue_session_wicket = SKILL_W_VENUE_SESSION_WICKET_MENS_T20,

      # Decay rates
      decay_rate = SKILL_DECAY_T20,
      venue_perm_decay = SKILL_VENUE_DECAY_PERM,
      venue_session_decay = SKILL_VENUE_DECAY_SESSION,

      # Bounds
      skill_start = SKILL_INDEX_START,
      run_max = SKILL_INDEX_RUN_MAX,
      run_min = SKILL_INDEX_RUN_MIN,
      wicket_max = SKILL_INDEX_WICKET_MAX,
      wicket_min = SKILL_INDEX_WICKET_MIN,
      venue_run_max = SKILL_VENUE_RUN_MAX,
      venue_run_min = SKILL_VENUE_RUN_MIN,
      venue_wicket_max = SKILL_VENUE_WICKET_MAX,
      venue_wicket_min = SKILL_VENUE_WICKET_MIN
    )
  }

  n <- nrow(deliveries)
  if (verbose) cat(sprintf("Processing %s deliveries with skill indices...\n",
                            format(n, big.mark = ",")))

  # Get unique entities
  all_batters <- unique(deliveries$batter_id)
  all_bowlers <- unique(deliveries$bowler_id)
  all_venues <- unique(deliveries$venue)

  if (verbose) {
    cat(sprintf("  Entities: %d batters, %d bowlers, %d venues\n",
                length(all_batters), length(all_bowlers), length(all_venues)))
  }

  # Initialize skill environments (fast hash lookup)
  # All start at 0 (neutral) - this is a key difference from ELO (1400)
  batter_run_skill <- new.env(hash = TRUE, size = length(all_batters))
  batter_wicket_skill <- new.env(hash = TRUE, size = length(all_batters))
  bowler_run_skill <- new.env(hash = TRUE, size = length(all_bowlers))
  bowler_wicket_skill <- new.env(hash = TRUE, size = length(all_bowlers))
  venue_perm_run_skill <- new.env(hash = TRUE, size = length(all_venues))
  venue_perm_wicket_skill <- new.env(hash = TRUE, size = length(all_venues))

  # Experience counters
  batter_balls <- new.env(hash = TRUE, size = length(all_batters))
  bowler_balls <- new.env(hash = TRUE, size = length(all_bowlers))
  venue_balls <- new.env(hash = TRUE, size = length(all_venues))

  # Initialize all entities at 0 (neutral skill)
  for (b in all_batters) {
    batter_run_skill[[b]] <- params$skill_start
    batter_wicket_skill[[b]] <- params$skill_start
    batter_balls[[b]] <- 0L
  }
  for (b in all_bowlers) {
    bowler_run_skill[[b]] <- params$skill_start
    bowler_wicket_skill[[b]] <- params$skill_start
    bowler_balls[[b]] <- 0L
  }
  for (v in all_venues) {
    venue_perm_run_skill[[v]] <- params$skill_start
    venue_perm_wicket_skill[[v]] <- params$skill_start
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
  batter_run_skill_vec <- numeric(n)
  bowler_run_skill_vec <- numeric(n)
  venue_perm_run_skill_vec <- numeric(n)
  batter_wicket_skill_vec <- numeric(n)
  bowler_wicket_skill_vec <- numeric(n)
  venue_perm_wicket_skill_vec <- numeric(n)

  # Track session skill (resets per match)
  current_match <- ""
  venue_session_run <- params$skill_start
  venue_session_wicket <- params$skill_start
  match_balls <- 0L

  # Main loop
  for (i in seq_len(n)) {
    batter_id <- batter_ids[i]
    bowler_id <- bowler_ids[i]
    venue <- venues_vec[i]
    match_id <- match_ids[i]

    # Reset session skill on new match
    if (match_id != current_match) {
      current_match <- match_id
      venue_session_run <- params$skill_start
      venue_session_wicket <- params$skill_start
      match_balls <- 0L
    }

    # Get current skills (before update)
    bat_run <- batter_run_skill[[batter_id]]
    bat_wicket <- batter_wicket_skill[[batter_id]]
    bowl_run <- bowler_run_skill[[bowler_id]]
    bowl_wicket <- bowler_wicket_skill[[bowler_id]]
    venue_perm_run <- venue_perm_run_skill[[venue]]
    venue_perm_wicket <- venue_perm_wicket_skill[[venue]]

    # Get experience counts
    bat_exp <- batter_balls[[batter_id]]
    bowl_exp <- bowler_balls[[bowler_id]]
    venue_exp <- venue_balls[[venue]]

    # Store skills for output
    batter_run_skill_vec[i] <- bat_run
    bowler_run_skill_vec[i] <- bowl_run
    venue_perm_run_skill_vec[i] <- venue_perm_run
    batter_wicket_skill_vec[i] <- bat_wicket
    bowler_wicket_skill_vec[i] <- bowl_wicket
    venue_perm_wicket_skill_vec[i] <- venue_perm_wicket

    # ========================================================================
    # CALCULATE EXPECTED RUNS (Skill Index Formula)
    # ========================================================================
    # Simple additive: expected = baseline + weighted skill sum
    # Key difference from ELO: no conversion factor needed
    baseline <- baseline_runs_vec[i]

    # Combined skill effect on runs
    # Batter: positive skill = more runs
    # Bowler: INVERTED - positive bowler skill = restricts runs (subtract)
    # Venue: positive skill = more runs
    skill_effect_run <-
      params$w_batter_run * bat_run -
      params$w_bowler_run * bowl_run +  # Inverted
      params$w_venue_perm_run * venue_perm_run +
      params$w_venue_session_run * venue_session_run

    expected_runs <- baseline + skill_effect_run
    expected_runs <- max(0.01, min(6, expected_runs))
    pred_runs[i] <- expected_runs

    # ========================================================================
    # CALCULATE EXPECTED WICKET (Skill Index Formula)
    # ========================================================================
    # Direct additive on probability (bounded to valid range)
    # NO WEIGHTS in prediction - each skill IS the direct probability effect
    # Weights are only for distributing UPDATE responsibility
    baseline_wicket <- baseline_wicket_vec[i]

    # Combined skill effect on wicket probability (DIRECT, no weighting)
    # Batter wicket skill positive = gets out more (bad for batter)
    # Bowler wicket skill positive = takes more wickets (good for bowler)
    skill_effect_wicket <- bat_wicket + bowl_wicket + venue_perm_wicket + venue_session_wicket

    expected_wicket <- baseline_wicket + skill_effect_wicket
    expected_wicket <- max(0.001, min(0.5, expected_wicket))
    pred_wicket[i] <- expected_wicket

    # Get actual outcomes
    actual_runs <- actual_runs_vec[i]
    is_wicket <- is_wicket_vec[i]

    # ========================================================================
    # CALCULATE ALPHA (Learning Rate) BASED ON EXPERIENCE
    # ========================================================================
    alpha_batter_run <- params$alpha_run_min +
      (params$alpha_run_max - params$alpha_run_min) * exp(-bat_exp / params$alpha_run_halflife)
    alpha_bowler_run <- params$alpha_run_min +
      (params$alpha_run_max - params$alpha_run_min) * exp(-bowl_exp / params$alpha_run_halflife)
    alpha_venue_perm_run <- params$venue_alpha_perm * exp(-venue_exp / 10000)
    alpha_venue_perm_run <- max(params$venue_alpha_perm * 0.2, alpha_venue_perm_run)
    alpha_venue_session_run <- max(params$venue_alpha_session_min,
      params$venue_alpha_session_max * exp(-match_balls / params$venue_alpha_session_halflife))

    alpha_batter_wicket <- params$alpha_wicket_min +
      (params$alpha_wicket_max - params$alpha_wicket_min) * exp(-bat_exp / params$alpha_wicket_halflife)
    alpha_bowler_wicket <- params$alpha_wicket_min +
      (params$alpha_wicket_max - params$alpha_wicket_min) * exp(-bowl_exp / params$alpha_wicket_halflife)
    alpha_venue_perm_wicket <- params$venue_alpha_perm * exp(-venue_exp / 10000)
    alpha_venue_perm_wicket <- max(params$venue_alpha_perm * 0.2, alpha_venue_perm_wicket)
    alpha_venue_session_wicket <- max(params$venue_alpha_session_min,
      params$venue_alpha_session_max * exp(-match_balls / params$venue_alpha_session_halflife))

    # ========================================================================
    # UPDATE RUN SKILLS
    # ========================================================================
    # Formula: new_skill = (1 - decay) * old_skill + alpha * residual * weight
    run_residual <- actual_runs - expected_runs

    # Apply decay first, then update
    # Batter: positive residual = scored well = skill increases
    new_bat_run <- (1 - params$decay_rate) * bat_run +
      alpha_batter_run * run_residual * params$w_batter_run

    # Bowler: INVERTED - if batter scored well, bowler skill decreases
    new_bowl_run <- (1 - params$decay_rate) * bowl_run -
      alpha_bowler_run * run_residual * params$w_bowler_run

    # Venues
    new_venue_perm_run <- (1 - params$venue_perm_decay) * venue_perm_run +
      alpha_venue_perm_run * run_residual * params$w_venue_perm_run

    new_venue_session_run <- (1 - params$venue_session_decay) * venue_session_run +
      alpha_venue_session_run * run_residual * params$w_venue_session_run

    # Apply bounds
    batter_run_skill[[batter_id]] <- max(params$run_min, min(params$run_max, new_bat_run))
    bowler_run_skill[[bowler_id]] <- max(params$run_min, min(params$run_max, new_bowl_run))
    venue_perm_run_skill[[venue]] <- max(params$venue_run_min, min(params$venue_run_max, new_venue_perm_run))
    venue_session_run <- max(params$venue_run_min, min(params$venue_run_max, new_venue_session_run))

    # ========================================================================
    # UPDATE WICKET SKILLS
    # ========================================================================
    wicket_val <- if (is_wicket) 1 else 0
    wicket_residual <- wicket_val - expected_wicket

    # Batter: got out = positive residual = wicket skill increases (gets out more)
    new_bat_wicket <- (1 - params$decay_rate) * bat_wicket +
      alpha_batter_wicket * wicket_residual * params$w_batter_wicket

    # Bowler: took wicket = positive residual = wicket skill increases (takes more)
    new_bowl_wicket <- (1 - params$decay_rate) * bowl_wicket +
      alpha_bowler_wicket * wicket_residual * params$w_bowler_wicket

    # Venues
    new_venue_perm_wicket <- (1 - params$venue_perm_decay) * venue_perm_wicket +
      alpha_venue_perm_wicket * wicket_residual * params$w_venue_perm_wicket

    new_venue_session_wicket <- (1 - params$venue_session_decay) * venue_session_wicket +
      alpha_venue_session_wicket * wicket_residual * params$w_venue_session_wicket

    # Apply bounds
    batter_wicket_skill[[batter_id]] <- max(params$wicket_min, min(params$wicket_max, new_bat_wicket))
    bowler_wicket_skill[[bowler_id]] <- max(params$wicket_min, min(params$wicket_max, new_bowl_wicket))
    venue_perm_wicket_skill[[venue]] <- max(params$venue_wicket_min, min(params$venue_wicket_max, new_venue_perm_wicket))
    venue_session_wicket <- max(params$venue_wicket_min, min(params$venue_wicket_max, new_venue_session_wicket))

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

  # Extract final skills
  final_batter_run <- sapply(all_batters, function(b) batter_run_skill[[b]])
  final_batter_wicket <- sapply(all_batters, function(b) batter_wicket_skill[[b]])
  final_bowler_run <- sapply(all_bowlers, function(b) bowler_run_skill[[b]])
  final_bowler_wicket <- sapply(all_bowlers, function(b) bowler_wicket_skill[[b]])
  final_venue_perm_run <- sapply(all_venues, function(v) venue_perm_run_skill[[v]])
  final_venue_perm_wicket <- sapply(all_venues, function(v) venue_perm_wicket_skill[[v]])

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
    batter_run_skill = batter_run_skill_vec,
    bowler_run_skill = bowler_run_skill_vec,
    venue_perm_run_skill = venue_perm_run_skill_vec,
    batter_wicket_skill = batter_wicket_skill_vec,
    bowler_wicket_skill = bowler_wicket_skill_vec,
    venue_perm_wicket_skill = venue_perm_wicket_skill_vec
  )

  list(
    final_skills = list(
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
# LOSS FUNCTIONS (Same as ELO version)
# ============================================================================

#' Calculate Poisson Loss on Run Predictions
calculate_poisson_loss <- function(predicted, actual) {
  predicted <- pmax(0.01, predicted)
  mean(predicted - actual * log(predicted))
}

#' Calculate Log-Loss on Wicket Predictions
calculate_wicket_logloss <- function(predicted, actual) {
  predicted <- pmax(0.001, pmin(0.999, predicted))
  actual_num <- as.numeric(actual)
  -mean(actual_num * log(predicted) + (1 - actual_num) * log(1 - predicted))
}

#' Calculate Mean Absolute Error
calculate_mae <- function(predicted, actual) {
  mean(abs(predicted - actual))
}


# ============================================================================
# EVALUATION FUNCTION
# ============================================================================

#' Evaluate Skill Model on Test Set
#'
#' Uses the final skills from training to predict on test data.
#'
#' @param skill_results List from run_skill_on_synthetic().
#' @param test_deliveries data.table. Test deliveries.
#' @param verbose Logical. Print results.
#'
#' @return List with loss metrics.
evaluate_skill_on_test_set <- function(skill_results, test_deliveries, verbose = TRUE) {
  params <- skill_results$params
  final_skills <- skill_results$final_skills

  n <- nrow(test_deliveries)

  # Pre-extract columns
  batter_ids <- test_deliveries$batter_id
  bowler_ids <- test_deliveries$bowler_id
  venues <- test_deliveries$venue
  baseline_runs <- test_deliveries$baseline_runs
  baseline_wicket <- test_deliveries$baseline_wicket
  actual_runs <- test_deliveries$actual_runs
  actual_wicket <- test_deliveries$is_wicket

  # Calculate predictions using final skills (no updates)
  pred_runs <- numeric(n)
  pred_wicket <- numeric(n)

  for (i in seq_len(n)) {
    batter <- batter_ids[i]
    bowler <- bowler_ids[i]
    venue <- venues[i]

    # Get final skills (use 0 if entity not seen in training)
    bat_run <- if (batter %in% names(final_skills$batter_run)) {
      final_skills$batter_run[batter]
    } else {
      params$skill_start
    }
    bowl_run <- if (bowler %in% names(final_skills$bowler_run)) {
      final_skills$bowler_run[bowler]
    } else {
      params$skill_start
    }
    venue_run <- if (venue %in% names(final_skills$venue_perm_run)) {
      final_skills$venue_perm_run[venue]
    } else {
      params$skill_start
    }

    bat_wicket <- if (batter %in% names(final_skills$batter_wicket)) {
      final_skills$batter_wicket[batter]
    } else {
      params$skill_start
    }
    bowl_wicket <- if (bowler %in% names(final_skills$bowler_wicket)) {
      final_skills$bowler_wicket[bowler]
    } else {
      params$skill_start
    }
    venue_wicket <- if (venue %in% names(final_skills$venue_perm_wicket)) {
      final_skills$venue_perm_wicket[venue]
    } else {
      params$skill_start
    }

    # Calculate expected runs (additive formula)
    skill_effect <-
      params$w_batter_run * bat_run -
      params$w_bowler_run * bowl_run +
      params$w_venue_perm_run * venue_run

    pred_runs[i] <- max(0.01, min(6, baseline_runs[i] + skill_effect))

    # Calculate expected wicket (DIRECT, no weighting)
    skill_effect_wicket <- bat_wicket + bowl_wicket + venue_wicket

    pred_wicket[i] <- max(0.001, min(0.5, baseline_wicket[i] + skill_effect_wicket))
  }

  # Calculate metrics
  run_poisson <- calculate_poisson_loss(pred_runs, actual_runs)
  run_mae <- calculate_mae(pred_runs, actual_runs)
  wicket_logloss <- calculate_wicket_logloss(pred_wicket, actual_wicket)

  # Null model comparison
  null_run_poisson <- calculate_poisson_loss(rep(mean(baseline_runs), n), actual_runs)
  null_wicket_logloss <- calculate_wicket_logloss(rep(mean(baseline_wicket), n), actual_wicket)

  if (verbose) {
    cat("\n=== Test Set Evaluation (Skill Indices) ===\n")
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


# ============================================================================
# SKILL RECOVERY VALIDATION
# ============================================================================

#' Validate Skill Recovery Against True Values
#'
#' Compares recovered skill indices against true skills from synthetic data.
#' This is the key validation that the skill system works correctly.
#'
#' For batter run skill, we need to convert from "runs per ball" (true_run_skill)
#' to "deviation from baseline" (recovered skill).
#'
#' @param skill_results List from run_skill_on_synthetic().
#' @param dataset List from generate_synthetic_dataset().
#' @param verbose Logical. Print results.
#'
#' @return List with correlation metrics for each entity type.
validate_skill_recovery <- function(skill_results, dataset, verbose = TRUE) {

  final_skills <- skill_results$final_skills
  batters <- dataset$players$batters
  bowlers <- dataset$players$bowlers
  venues <- dataset$venues
  baseline <- SIM_BASELINE_RUNS

  # ========================================================================
  # BATTER RUN SKILL RECOVERY
  # ========================================================================
  # True skill: runs per ball (e.g., 1.2)
  # Recovered skill: deviation from baseline (e.g., +0.08)
  # Need to convert true skill to deviation for comparison
  batter_true_deviation <- batters$true_run_skill - baseline
  batter_recovered <- final_skills$batter_run[batters$batter_id]

  batter_run_cor <- cor(batter_true_deviation, batter_recovered, use = "complete.obs")

  # ========================================================================
  # BOWLER RUN SKILL RECOVERY
  # ========================================================================
  # True skill (economy): runs conceded per ball (e.g., 1.0 = economical)
  # Recovered skill: positive = restricts runs (good for bowler)
  # So if bowler has low economy (good), they should have positive skill
  # Mapping: true_economy = baseline means skill = 0
  #          true_economy < baseline means skill > 0 (restricts runs)
  bowler_true_deviation <- baseline - bowlers$true_economy  # Inverted
  bowler_recovered <- final_skills$bowler_run[bowlers$bowler_id]

  bowler_run_cor <- cor(bowler_true_deviation, bowler_recovered, use = "complete.obs")

  # ========================================================================
  # VENUE RUN EFFECT RECOVERY
  # ========================================================================
  # True effect: direct additive (e.g., +0.1 = high-scoring venue)
  # Recovered skill: same interpretation
  venue_true_effect <- venues$true_run_effect
  venue_recovered <- final_skills$venue_perm_run[venues$venue]

  venue_run_cor <- cor(venue_true_effect, venue_recovered, use = "complete.obs")

  # ========================================================================
  # WICKET SKILL RECOVERY
  # ========================================================================
  # Batter wicket: higher true_wicket_rate = gets out more = higher recovered skill
  batter_wicket_true <- batters$true_wicket_rate - SIM_BASELINE_WICKET
  batter_wicket_recovered <- final_skills$batter_wicket[batters$batter_id]
  batter_wicket_cor <- cor(batter_wicket_true, batter_wicket_recovered, use = "complete.obs")

  # Bowler wicket: higher true_wicket_rate = takes more wickets = higher recovered skill
  bowler_wicket_true <- bowlers$true_wicket_rate - SIM_BASELINE_WICKET
  bowler_wicket_recovered <- final_skills$bowler_wicket[bowlers$bowler_id]
  bowler_wicket_cor <- cor(bowler_wicket_true, bowler_wicket_recovered, use = "complete.obs")

  # Venue wicket
  venue_wicket_true <- venues$true_wicket_effect
  venue_wicket_recovered <- final_skills$venue_perm_wicket[venues$venue]
  venue_wicket_cor <- cor(venue_wicket_true, venue_wicket_recovered, use = "complete.obs")

  # ========================================================================
  # REPORT RESULTS
  # ========================================================================
  if (verbose) {
    cat("\n=== Skill Recovery Validation ===\n")
    cat("\nRun Skill Correlations (true vs recovered):\n")
    cat(sprintf("  Batter:  %.3f %s\n", batter_run_cor,
                if (batter_run_cor >= SIM_MIN_CORRELATION) "[PASS]" else "[FAIL]"))
    cat(sprintf("  Bowler:  %.3f %s\n", bowler_run_cor,
                if (bowler_run_cor >= SIM_MIN_CORRELATION) "[PASS]" else "[FAIL]"))
    cat(sprintf("  Venue:   %.3f %s\n", venue_run_cor,
                if (venue_run_cor >= SIM_MIN_VENUE_CORRELATION) "[PASS]" else "[FAIL]"))

    cat("\nWicket Skill Correlations (true vs recovered):\n")
    cat(sprintf("  Batter:  %.3f %s\n", batter_wicket_cor,
                if (batter_wicket_cor >= SIM_MIN_CORRELATION) "[PASS]" else "[FAIL]"))
    cat(sprintf("  Bowler:  %.3f %s\n", bowler_wicket_cor,
                if (bowler_wicket_cor >= SIM_MIN_CORRELATION) "[PASS]" else "[FAIL]"))
    cat(sprintf("  Venue:   %.3f %s\n", venue_wicket_cor,
                if (venue_wicket_cor >= SIM_MIN_VENUE_CORRELATION) "[PASS]" else "[FAIL]"))

    # Summary
    all_pass <-
      batter_run_cor >= SIM_MIN_CORRELATION &&
      bowler_run_cor >= SIM_MIN_CORRELATION &&
      venue_run_cor >= SIM_MIN_VENUE_CORRELATION &&
      batter_wicket_cor >= SIM_MIN_CORRELATION &&
      bowler_wicket_cor >= SIM_MIN_CORRELATION &&
      venue_wicket_cor >= SIM_MIN_VENUE_CORRELATION

    cat(sprintf("\n%s All correlations above threshold\n",
                if (all_pass) "[SUCCESS]" else "[WARNING]"))
  }

  list(
    batter_run = list(
      correlation = batter_run_cor,
      true = batter_true_deviation,
      recovered = batter_recovered
    ),
    bowler_run = list(
      correlation = bowler_run_cor,
      true = bowler_true_deviation,
      recovered = bowler_recovered
    ),
    venue_run = list(
      correlation = venue_run_cor,
      true = venue_true_effect,
      recovered = venue_recovered
    ),
    batter_wicket = list(
      correlation = batter_wicket_cor,
      true = batter_wicket_true,
      recovered = batter_wicket_recovered
    ),
    bowler_wicket = list(
      correlation = bowler_wicket_cor,
      true = bowler_wicket_true,
      recovered = bowler_wicket_recovered
    ),
    venue_wicket = list(
      correlation = venue_wicket_cor,
      true = venue_wicket_true,
      recovered = venue_wicket_recovered
    )
  )
}


# ============================================================================
# COMPARISON WITH ELO (Convert for comparison)
# ============================================================================

#' Compare Skill Index Results with ELO Results
#'
#' Compares the skill index recovery with ELO recovery on the same data.
#' This helps validate that the skill system performs at least as well as ELO.
#'
#' @param skill_recovery List from validate_skill_recovery().
#' @param elo_recovery List from validate_elo_recovery() (from 03_validate_recovery.R).
#' @param verbose Logical. Print comparison.
#'
#' @return Data frame with side-by-side comparison.
compare_skill_vs_elo <- function(skill_recovery, elo_recovery, verbose = TRUE) {
  comparison <- data.frame(
    Entity = c("Batter Run", "Bowler Run", "Venue Run",
               "Batter Wicket", "Bowler Wicket", "Venue Wicket"),
    Skill_Correlation = c(
      skill_recovery$batter_run$correlation,
      skill_recovery$bowler_run$correlation,
      skill_recovery$venue_run$correlation,
      skill_recovery$batter_wicket$correlation,
      skill_recovery$bowler_wicket$correlation,
      skill_recovery$venue_wicket$correlation
    ),
    ELO_Correlation = c(
      elo_recovery$batter_run$correlation,
      elo_recovery$bowler_run$correlation,
      elo_recovery$venue_run$correlation,
      elo_recovery$batter_wicket$correlation,
      elo_recovery$bowler_wicket$correlation,
      elo_recovery$venue_wicket$correlation
    )
  )

  comparison$Difference <- comparison$Skill_Correlation - comparison$ELO_Correlation

  if (verbose) {
    cat("\n=== Skill Index vs ELO Comparison ===\n")
    print(comparison)
    cat(sprintf("\nMean difference: %.3f (positive = skill better)\n",
                mean(comparison$Difference)))
  }

  comparison
}


NULL
