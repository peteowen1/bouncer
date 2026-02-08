# Run Full Simulation ----
#
# Master orchestration script that runs the complete simulation validation:
#   1. Generate synthetic players, venues, and deliveries with known true skills
#   2. Run the 3-Way ELO calculation on synthetic data
#   3. Validate whether ELO recovers the true skills
#   4. Evaluate on held-out test set
#
# This is the main entry point for running the simulation validation.
#
# Usage:
#   source("data-raw/ratings/player/3way-elo/simulation/04_run_full_simulation.R")
#   results <- run_full_simulation()

# ============================================================================
# SETUP
# ============================================================================

library(data.table)

# Get script directory for sourcing
script_dir <- if (exists("sys.frame")) {
  tryCatch({
    dirname(sys.frame(1)$ofile)
  }, error = function(e) {
    "."
  })
} else {
  "."
}

# Source dependencies (with error handling for different contexts)
source_script <- function(filename) {
  paths <- c(
    file.path(script_dir, filename),
    file.path("data-raw/ratings/player/3way-elo/simulation", filename),
    filename
  )
  for (path in paths) {
    if (file.exists(path)) {
      source(path)
      return(invisible(TRUE))
    }
  }
  stop("Could not find: ", filename)
}

source_script("00_simulation_config.R")
source_script("01_generate_synthetic_data.R")
source_script("02_run_elo_on_synthetic.R")
source_script("03_validate_recovery.R")

# Load bouncer package for ELO constants
tryCatch({
  devtools::load_all()
}, error = function(e) {
  # Try from parent directories
  for (path in c(".", "..", "../..", "../../..", "../../../..")) {
    desc_path <- file.path(path, "DESCRIPTION")
    if (file.exists(desc_path)) {
      devtools::load_all(path)
      break
    }
  }
})

# ============================================================================
# MAIN SIMULATION FUNCTION
# ============================================================================

#' Run Full Simulation Validation
#'
#' Orchestrates the complete simulation pipeline:
#'   1. Generate synthetic data with known true skills
#'   2. Run ELO calculation on synthetic deliveries
#'   3. Validate skill recovery
#'   4. Evaluate on test set
#'
#' @param n_simulations Integer. Number of simulation runs (for stability testing).
#'   Default 1 for single run.
#' @param config List. Custom configuration. If NULL, uses defaults.
#' @param seed Integer. Random seed for reproducibility.
#' @param verbose Logical. Print progress and results.
#' @param save_results Logical. Save results to file.
#'
#' @return List with:
#'   - dataset: Synthetic data generated
#'   - elo_results: ELO calculation results
#'   - validation: Skill recovery validation
#'   - test_metrics: Test set evaluation metrics
#'   - success: Overall success indicator
run_full_simulation <- function(n_simulations = 1,
                                 config = NULL,
                                 seed = SIM_SEED,
                                 verbose = TRUE,
                                 save_results = FALSE) {

  cat("\n")
  cat("╔════════════════════════════════════════════════════════════════╗\n")
  cat("║     3-WAY ELO SIMULATION VALIDATION FRAMEWORK                  ║\n")
  cat("║     Testing skill recovery on synthetic data                   ║\n")
  cat("╚════════════════════════════════════════════════════════════════╝\n")
  cat("\n")

  # Use default config if not provided
  if (is.null(config)) {
    config <- get_simulation_config()
  }

  # Store results from all simulations
  all_results <- vector("list", n_simulations)

  for (sim in seq_len(n_simulations)) {
    if (n_simulations > 1) {
      cat(sprintf("\n=== Simulation %d of %d ===\n", sim, n_simulations))
    }

    # Use different seed for each simulation
    current_seed <- seed + (sim - 1) * 1000

    # =========================================================================
    # STEP 1: Generate Synthetic Data
    # =========================================================================
    cat("\n─── Step 1: Generating Synthetic Data ───\n")

    dataset <- generate_synthetic_dataset(config, seed = current_seed)

    if (verbose) {
      print_synthetic_summary(dataset)
    }

    # =========================================================================
    # STEP 2: Run ELO Calculation
    # =========================================================================
    cat("\n─── Step 2: Running ELO Calculation ───\n")

    elo_results <- run_elo_on_synthetic(
      deliveries = dataset$train_deliveries,
      params = NULL,  # Use package defaults
      verbose = verbose
    )

    if (verbose) {
      # Print some ELO distribution stats
      cat("\nFinal ELO distributions:\n")
      cat(sprintf("  Batter Run ELO: mean=%.1f, sd=%.1f, range=[%.1f, %.1f]\n",
                  mean(elo_results$final_elos$batter_run),
                  sd(elo_results$final_elos$batter_run),
                  min(elo_results$final_elos$batter_run),
                  max(elo_results$final_elos$batter_run)))
      cat(sprintf("  Bowler Run ELO: mean=%.1f, sd=%.1f, range=[%.1f, %.1f]\n",
                  mean(elo_results$final_elos$bowler_run),
                  sd(elo_results$final_elos$bowler_run),
                  min(elo_results$final_elos$bowler_run),
                  max(elo_results$final_elos$bowler_run)))
      cat(sprintf("  Venue Perm Run ELO: mean=%.1f, sd=%.1f, range=[%.1f, %.1f]\n",
                  mean(elo_results$final_elos$venue_perm_run),
                  sd(elo_results$final_elos$venue_perm_run),
                  min(elo_results$final_elos$venue_perm_run),
                  max(elo_results$final_elos$venue_perm_run)))
    }

    # =========================================================================
    # STEP 3: Validate Skill Recovery
    # =========================================================================
    cat("\n─── Step 3: Validating Skill Recovery ───\n")

    validation <- validate_elo_recovery(
      elo_results = elo_results,
      players = dataset$players,
      venues = dataset$venues
    )

    if (verbose) {
      generate_validation_report(validation)
    }

    # =========================================================================
    # STEP 4: Test Set Evaluation
    # =========================================================================
    cat("\n─── Step 4: Test Set Evaluation ───\n")

    test_metrics <- evaluate_on_test_set(
      elo_results = elo_results,
      test_deliveries = dataset$test_deliveries,
      verbose = verbose
    )

    # =========================================================================
    # Compile Results
    # =========================================================================
    all_results[[sim]] <- list(
      dataset = dataset,
      elo_results = elo_results,
      validation = validation,
      test_metrics = test_metrics,
      success = validation$success,
      seed = current_seed
    )
  }

  # =========================================================================
  # Summary (for multiple simulations)
  # =========================================================================
  if (n_simulations > 1) {
    cat("\n=== Multi-Simulation Summary ===\n")
    successes <- sum(sapply(all_results, function(r) r$success))
    cat(sprintf("Success rate: %d/%d (%.1f%%)\n",
                successes, n_simulations, 100 * successes / n_simulations))

    # Average correlations
    avg_cors <- list(
      batter_run = mean(sapply(all_results, function(r) r$validation$correlations$batter_run)),
      bowler_run = mean(sapply(all_results, function(r) r$validation$correlations$bowler_run)),
      venue_run = mean(sapply(all_results, function(r) r$validation$correlations$venue_run))
    )
    cat(sprintf("Average correlations: batter=%.3f, bowler=%.3f, venue=%.3f\n",
                avg_cors$batter_run, avg_cors$bowler_run, avg_cors$venue_run))
  }

  # =========================================================================
  # Final Status
  # =========================================================================
  cat("\n")
  cat("╔════════════════════════════════════════════════════════════════╗\n")

  if (n_simulations == 1) {
    final_result <- all_results[[1]]
    if (final_result$success) {
      cat("║     SIMULATION VALIDATION: SUCCESS                             ║\n")
      cat("║     ELO system correctly recovers true skills                  ║\n")
    } else {
      cat("║     SIMULATION VALIDATION: FAILED                              ║\n")
      cat("║     ELO system did NOT correctly recover true skills           ║\n")
      cat("║     See validation report above for diagnostics                ║\n")
    }
  } else {
    successes <- sum(sapply(all_results, function(r) r$success))
    if (successes == n_simulations) {
      cat("║     SIMULATION VALIDATION: ALL PASSED                          ║\n")
    } else {
      cat(sprintf("║     SIMULATION VALIDATION: %d/%d PASSED                        ║\n",
                  successes, n_simulations))
    }
  }
  cat("╚════════════════════════════════════════════════════════════════╝\n")
  cat("\n")

  # Return results
  if (n_simulations == 1) {
    result <- all_results[[1]]

    # Save results if requested
    if (save_results) {
      results_dir <- file.path(find_bouncerdata_dir(), "simulation_results")
      dir.create(results_dir, showWarnings = FALSE, recursive = TRUE)
      results_file <- file.path(results_dir,
                                 sprintf("sim_validation_%s.rds", format(Sys.time(), "%Y%m%d_%H%M%S")))
      saveRDS(result, results_file)
      cat(sprintf("Results saved to: %s\n", results_file))
    }

    return(result)
  } else {
    return(all_results)
  }
}


# ============================================================================
# QUICK VALIDATION FUNCTION
# ============================================================================

#' Quick Validation Check
#'
#' Runs a smaller simulation for quick sanity checking.
#'
#' @param seed Integer. Random seed.
#'
#' @return Logical. TRUE if validation passed.
quick_validation <- function(seed = 123) {
  # Use smaller config for speed
  quick_config <- list(
    n_batters = 20,
    n_bowlers = 20,
    n_venues = 10,
    n_train_deliveries = 30000,
    n_test_deliveries = 10000,
    deliveries_per_match = 240,
    batter_sr_mean = SIM_BATTER_SR_MEAN,
    batter_sr_sd = SIM_BATTER_SR_SD,
    batter_wicket_mean = SIM_BATTER_WICKET_MEAN,
    batter_wicket_sd = SIM_BATTER_WICKET_SD,
    bowler_econ_mean = SIM_BOWLER_ECON_MEAN,
    bowler_econ_sd = SIM_BOWLER_ECON_SD,
    bowler_wicket_mean = SIM_BOWLER_WICKET_MEAN,
    bowler_wicket_sd = SIM_BOWLER_WICKET_SD,
    venue_run_effect_sd = SIM_VENUE_RUN_EFFECT_SD,
    venue_wicket_effect_sd = SIM_VENUE_WICKET_EFFECT_SD,
    baseline_runs = SIM_BASELINE_RUNS,
    baseline_wicket = SIM_BASELINE_WICKET,
    w_batter = SIM_W_BATTER,
    w_bowler = SIM_W_BOWLER,
    w_venue_session = SIM_W_VENUE_SESSION,
    w_venue_perm = SIM_W_VENUE_PERM,
    min_correlation = 0.4,  # Lower threshold for quick check
    min_venue_correlation = 0.2,
    seed = seed
  )

  cat("Running quick validation (smaller dataset)...\n")
  result <- run_full_simulation(config = quick_config, seed = seed, verbose = FALSE)
  result$success
}


# ============================================================================
# DEBUGGING HELPERS
# ============================================================================

#' Debug Specific Entity
#'
#' Shows detailed information about a specific entity's ELO trajectory.
#'
#' @param entity_id Character. Entity ID (batter_id, bowler_id, or venue).
#' @param elo_results List from run_elo_on_synthetic().
#' @param entity_type Character. One of "batter", "bowler", "venue".
debug_entity <- function(entity_id, elo_results, entity_type = "batter") {
  predictions <- elo_results$predictions

  if (entity_type == "batter") {
    entity_data <- predictions[batter_id == entity_id]
    elo_col <- "batter_run_elo"
  } else if (entity_type == "bowler") {
    entity_data <- predictions[bowler_id == entity_id]
    elo_col <- "bowler_run_elo"
  } else {
    entity_data <- predictions[venue == entity_id]
    elo_col <- "venue_perm_run_elo"
  }

  cat(sprintf("\n=== Debug: %s '%s' ===\n", entity_type, entity_id))
  cat(sprintf("Total deliveries: %d\n", nrow(entity_data)))
  cat(sprintf("ELO trajectory: %.1f -> %.1f\n",
              entity_data[[elo_col]][1],
              entity_data[[elo_col]][nrow(entity_data)]))
  cat(sprintf("Mean predicted runs: %.3f\n", mean(entity_data$predicted_runs)))
  cat(sprintf("Mean actual runs: %.3f\n", mean(entity_data$actual_runs)))
  cat(sprintf("Residual: %.3f\n", mean(entity_data$actual_runs - entity_data$predicted_runs)))

  invisible(entity_data)
}


# ============================================================================
# RUN IF EXECUTED DIRECTLY
# ============================================================================

# Check if this script is being run directly (not sourced)
if (sys.nframe() == 0 || identical(environment(), globalenv())) {
  # Run the full simulation
  results <- run_full_simulation(
    n_simulations = 1,
    verbose = TRUE,
    save_results = FALSE
  )

  # Run additional diagnostics if failed
  if (!results$success) {
    cat("\n=== Additional Diagnostics ===\n")
    diagnose_issues(results$validation, results$elo_results)
    analyze_entity_rankings(results$validation)
  }
}
