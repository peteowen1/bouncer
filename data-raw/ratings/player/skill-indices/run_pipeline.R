# Run Player Skill Index Pipeline ----
#
# This script runs the complete Skill Index rating pipeline end-to-end.
#
# Skill indices are the RECOMMENDED rating system for new development.
# They are drift-proof by design (EMA-based) and directly interpretable
# (e.g., BSI of 1.8 = 1.8 runs/ball average).
#
# Pipeline steps:
#   1. Calculate skill indices (03_calculate_skill_indices.R)
#   2. Validate results (06_validate_skill_indices.R)
#
# Note: No calibration or normalization needed for skill indices!
#
# Configuration:
#   Edit the settings below or modify the individual scripts.
#
# Usage:
#   source("data-raw/ratings/player/run_skill_index_pipeline.R")

# 1. Configuration ----

# Set working directory to bouncer package root if needed
if (!file.exists("DESCRIPTION")) {
  if (file.exists("bouncer/DESCRIPTION")) {
    setwd("bouncer")
  } else {
    stop("Please run from the bouncer package root directory")
  }
}

# Pipeline options
RUN_CALCULATION <- TRUE   # Run main skill index calculation
RUN_VALIDATION <- TRUE    # Run validation checks

# Timing
start_time <- Sys.time()
step_times <- list()

cat("\n")
cli::cli_h1("Player Skill Index Pipeline")
cli::cli_alert_info("Started at: {format(start_time, '%Y-%m-%d %H:%M:%S')}")
cli::cli_alert_info("Skill indices are drift-proof - no normalization needed!")
cat("\n")

# 2. Run Skill Index Calculation ----
if (RUN_CALCULATION) {
  cli::cli_rule("Step 1: Skill Index Calculation")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/ratings/player/03_calculate_skill_indices.R", local = new.env())
    step_times$calculation <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Skill index calculation complete ({round(step_times$calculation, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Skill index calculation failed: {e$message}")
    stop(e)
  })

  cat("\n")
}

# 3. Run Validation ----
if (RUN_VALIDATION) {
  cli::cli_rule("Step 2: Validation")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/ratings/player/06_validate_skill_indices.R", local = new.env())
    step_times$validation <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Validation complete ({round(step_times$validation, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Validation failed: {e$message}")
    stop(e)
  })

  cat("\n")
}

# 4. Summary ----
end_time <- Sys.time()
total_time <- difftime(end_time, start_time, units = "mins")

cli::cli_rule("Pipeline Complete")
cat("\n")
cli::cli_alert_success("Skill Index Pipeline finished successfully!")
cli::cli_alert_info("Total time: {round(total_time, 1)} minutes")
cat("\n")

if (length(step_times) > 0) {
  cli::cli_h3("Step Timings")
  for (step_name in names(step_times)) {
    cli::cli_alert_info("{step_name}: {round(step_times[[step_name]], 1)} mins")
  }
}

cat("\n")
cli::cli_h3("Output Tables")
cli::cli_bullets(c(
  "*" = "t20_player_skill - T20 player skill indices",
  "*" = "odi_player_skill - ODI player skill indices",
  "*" = "test_player_skill - Test player skill indices"
))

cat("\n")
cli::cli_h3("Skill Index Interpretation")
cli::cli_bullets(c(
  "i" = "Batter Scoring Index (BSI): runs per ball faced (higher = better)",
  "i" = "Batter Survival Rate (BSR): probability of not getting out (higher = better)",
  "i" = "Bowler Economy Index (BEI): runs conceded per ball (lower = better)",
  "i" = "Bowler Strike Rate (BoSR): wicket probability per ball (higher = better)"
))
cat("\n")
