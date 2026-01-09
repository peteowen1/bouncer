# Run Ball-Outcome Models - Test Format ----
#
# This script runs the ball-outcome model pipeline for Test cricket.
#
# Test cricket uses the "longform" model which differs from T20/ODI:
#   - Phase is defined as new_ball/middle/old_ball (not overs-based)
#   - No overs_left feature (Tests have unlimited length)
#   - Different run distribution (more dots, fewer boundaries)
#
# Model Types:
#   - XGBoost (default): Fast, accurate, good feature importance
#   - BAM: Interpretable smooth terms, slower training
#
# IMPORTANT: XGBoost longform DEPENDS on BAM longform (reuses data splits)
#            If running XGBoost only, BAM data must exist first.
#
# Pipeline steps:
#   1. Train BAM model (creates data splits)
#   2. Train XGBoost model (reuses BAM data splits)
#   3. Compare models
#   4. Visualize comparison
#   5. Add predictions to deliveries table
#
# Usage:
#   source("data-raw/models/ball-outcome/run_outcome_models_test.R")

# 1. Configuration ----

# Set working directory to bouncer package root if needed
if (!file.exists("DESCRIPTION")) {
  if (file.exists("bouncer/DESCRIPTION")) {
    setwd("bouncer")
  } else {
    stop("Please run from the bouncer package root directory")
  }
}

# Model selection
MODEL_TYPE <- "xgboost"  # "xgboost" (default), "bam", or "both"

# Pipeline options
RUN_COMPARISON <- TRUE       # Run model comparison (only if both models exist)
RUN_VISUALIZATION <- TRUE    # Run visualization scripts
ADD_PREDICTIONS <- TRUE      # Add predictions to deliveries table

# Timing
start_time <- Sys.time()
step_times <- list()

cat("\n")
cli::cli_h1("Ball-Outcome Models: Test (Long-Form)")
cli::cli_alert_info("Started at: {format(start_time, '%Y-%m-%d %H:%M:%S')}")
cli::cli_alert_info("Model type: {MODEL_TYPE}")
cat("\n")

# 2. Check for BAM data dependency ----

# XGBoost longform depends on BAM longform data splits
data_splits_path <- file.path("..", "bouncerdata", "models", "model_data_splits_longform.rds")
bam_data_exists <- file.exists(data_splits_path)

if (MODEL_TYPE == "xgboost" && !bam_data_exists) {
  cli::cli_alert_warning("XGBoost longform requires BAM data splits")
  cli::cli_alert_info("Running BAM first to create data splits...")
  MODEL_TYPE <- "both"  # Force BAM to run first
}

# 3. Train Models ----

# BAM must run first for longform (creates data splits)
if (MODEL_TYPE %in% c("bam", "both")) {
  cli::cli_rule("Training BAM Model (Long-Form)")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/ball-outcome/model_bam_outcome_longform.R", local = new.env())
    step_times$bam <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("BAM training complete ({round(step_times$bam, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("BAM training failed: {e$message}")
    step_times$bam <- NA
  })

  cat("\n")
}

if (MODEL_TYPE %in% c("xgboost", "both")) {
  cli::cli_rule("Training XGBoost Model (Long-Form)")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/ball-outcome/model_xgb_outcome_longform.R", local = new.env())
    step_times$xgboost <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("XGBoost training complete ({round(step_times$xgboost, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("XGBoost training failed: {e$message}")
    step_times$xgboost <- NA
  })

  cat("\n")
}

# 4. Compare Models ----

if (RUN_COMPARISON && MODEL_TYPE == "both") {
  cli::cli_rule("Comparing Models")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/ball-outcome/compare_models.R", local = new.env())
    step_times$comparison <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Model comparison complete ({round(step_times$comparison, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Model comparison failed: {e$message}")
    step_times$comparison <- NA
  })

  cat("\n")
}

# 5. Visualize ----

if (RUN_VISUALIZATION && MODEL_TYPE == "both") {
  cli::cli_rule("Generating Visualizations")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/ball-outcome/visualize_comparison.R", local = new.env())
    step_times$visualization <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Visualization complete ({round(step_times$visualization, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Visualization failed: {e$message}")
    step_times$visualization <- NA
  })

  cat("\n")
}

# 6. Add Predictions ----

if (ADD_PREDICTIONS) {
  cli::cli_rule("Adding Predictions to Database")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/ball-outcome/add_all_predictions.R", local = new.env())
    step_times$predictions <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Predictions added ({round(step_times$predictions, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Adding predictions failed: {e$message}")
    step_times$predictions <- NA
  })

  cat("\n")
}

# 7. Summary ----

end_time <- Sys.time()
total_time <- difftime(end_time, start_time, units = "mins")

cli::cli_rule("Pipeline Complete")
cat("\n")

cli::cli_h3("Step Timings")
for (step_name in names(step_times)) {
  time_val <- step_times[[step_name]]
  if (is.na(time_val)) {
    cli::cli_alert_danger("{step_name}: FAILED")
  } else {
    cli::cli_alert_success("{step_name}: {round(time_val, 1)} mins")
  }
}

cat("\n")
cli::cli_alert_info("Total time: {round(total_time, 1)} minutes")

cli::cli_h3("Output Files")
cli::cli_bullets(c(
  "*" = "bouncerdata/models/xgb_outcome_longform.ubj (XGBoost model)",
  "*" = "bouncerdata/models/model_bam_outcome_longform.rds (BAM model)",
  "*" = "bouncerdata/models/model_data_splits_longform.rds (train/test splits)"
))

cat("\n")
cli::cli_h3("Test Cricket Phases")
cli::cli_bullets(c(
  "i" = "new_ball: First ~20 overs (ball swinging/seaming)",
  "i" = "middle: ~20-60 overs (ball softer, spin dominant)",
  "i" = "old_ball: 60+ overs (reverse swing, wear)"
))
cat("\n")
