# Run Player Dual ELO Pipeline ----
#
# This script runs the complete Dual ELO rating pipeline end-to-end.
#
# Pipeline steps:
#   1. Calibrate expected values (01_calibrate_expected_values.R)
#   2. Calculate dual ELOs (02_calculate_dual_elos.R)
#   3. Apply normalization if needed (04_apply_elo_normalization.R)
#   4. Validate results (05_validate_dual_elos.R)
#
# Configuration:
#   Edit the settings below or modify the individual scripts.
#
# Usage:
#   source("data-raw/ratings/player/run_dual_elo_pipeline.R")

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
RUN_CALIBRATION <- TRUE      # Run calibration step (recommended before first ELO calc)
RUN_ELO_CALCULATION <- TRUE  # Run main ELO calculation
RUN_NORMALIZATION <- TRUE    # Check and apply normalization if needed
RUN_VALIDATION <- TRUE       # Run validation checks

# Timing
start_time <- Sys.time()
step_times <- list()

cat("\n")
cli::cli_h1("Player Dual ELO Pipeline")
cli::cli_alert_info("Started at: {format(start_time, '%Y-%m-%d %H:%M:%S')}")
cat("\n")

# 2. Run Calibration ----
if (RUN_CALIBRATION) {
  cli::cli_rule("Step 1: Calibration")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/ratings/player/01_calibrate_expected_values.R", local = new.env())
    step_times$calibration <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Calibration complete ({round(step_times$calibration, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Calibration failed: {e$message}")
    stop(e)
  })

  cat("\n")
}

# 3. Run ELO Calculation ----
if (RUN_ELO_CALCULATION) {
  cli::cli_rule("Step 2: Dual ELO Calculation")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/ratings/player/02_calculate_dual_elos.R", local = new.env())
    step_times$elo_calculation <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("ELO calculation complete ({round(step_times$elo_calculation, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("ELO calculation failed: {e$message}")
    stop(e)
  })

  cat("\n")
}

# 4. Run Normalization ----
if (RUN_NORMALIZATION) {
  cli::cli_rule("Step 3: Normalization Check")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/ratings/player/04_apply_elo_normalization.R", local = new.env())
    step_times$normalization <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Normalization check complete ({round(step_times$normalization, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Normalization failed: {e$message}")
    stop(e)
  })

  cat("\n")
}

# 5. Run Validation ----
if (RUN_VALIDATION) {
  cli::cli_rule("Step 4: Validation")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/ratings/player/05_validate_dual_elos.R", local = new.env())
    step_times$validation <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Validation complete ({round(step_times$validation, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Validation failed: {e$message}")
    stop(e)
  })

  cat("\n")
}

# 6. Summary ----
end_time <- Sys.time()
total_time <- difftime(end_time, start_time, units = "mins")

cli::cli_rule("Pipeline Complete")
cat("\n")
cli::cli_alert_success("Dual ELO Pipeline finished successfully!")
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
  "*" = "mens_t20_player_elo - Men's T20 player ELOs",
  "*" = "womens_t20_player_elo - Women's T20 player ELOs",
  "*" = "(and other format/gender combinations as configured)"
))
cat("\n")
