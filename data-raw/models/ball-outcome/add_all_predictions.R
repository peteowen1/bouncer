# Add All Model Predictions to Deliveries Table ----
#
# This script adds prediction columns from all trained models to the deliveries table.
# Run this after training all models (BAM and XGBoost for both short/long form).
#
# Adds 28 total columns (7 probabilities × 4 models):
#   - pred_bam_sf_* : BAM short-form (T20/ODI)
#   - pred_xgb_sf_* : XGBoost short-form (T20/ODI)
#   - pred_bam_lf_* : BAM long-form (Test)
#   - pred_xgb_lf_* : XGBoost long-form (Test)

library(dplyr)
devtools::load_all()

cat("\n=== Adding Model Predictions to Deliveries Table ===\n\n")

# Configuration ----
MATCH_LIMIT <- NULL  # NULL = all matches, or set to number for testing

# Change to data-raw directory (where models are saved)
if (basename(getwd()) != "data-raw") {
  if (dir.exists("bouncer/data-raw")) {
    setwd("bouncer/data-raw")
  } else if (dir.exists("data-raw")) {
    setwd("data-raw")
  } else {
    stop("Please run this script from the project root or data-raw directory")
  }
}

cli::cli_alert_info("Working directory: {.file {getwd()}}")

# 1. BAM Short-Form (T20/ODI) ----
if (file.exists("model_bam_outcome_shortform.rds")) {
  cli::cli_h1("BAM Short-Form Predictions")
  add_predictions_to_deliveries(
    model_path = "model_bam_outcome_shortform.rds",
    model_type = "bam",
    format = "shortform",
    column_prefix = "pred_bam_sf",
    match_limit = MATCH_LIMIT
  )
} else {
  cli::cli_alert_warning("Skipping BAM short-form: model file not found")
}

# 2. XGBoost Short-Form (T20/ODI) ----
if (file.exists("model_xgb_outcome_shortform.json")) {
  cli::cli_h1("XGBoost Short-Form Predictions")
  add_predictions_to_deliveries(
    model_path = "model_xgb_outcome_shortform.json",
    model_type = "xgb",
    format = "shortform",
    column_prefix = "pred_xgb_sf",
    match_limit = MATCH_LIMIT
  )
} else {
  cli::cli_alert_warning("Skipping XGBoost short-form: model file not found")
}

# 3. BAM Long-Form (Test) ----
if (file.exists("model_bam_outcome_longform.rds")) {
  cli::cli_h1("BAM Long-Form Predictions")
  add_predictions_to_deliveries(
    model_path = "model_bam_outcome_longform.rds",
    model_type = "bam",
    format = "longform",
    column_prefix = "pred_bam_lf",
    match_limit = MATCH_LIMIT
  )
} else {
  cli::cli_alert_warning("Skipping BAM long-form: model file not found")
}

# 4. XGBoost Long-Form (Test) ----
if (file.exists("model_xgb_outcome_longform.json")) {
  cli::cli_h1("XGBoost Long-Form Predictions")
  add_predictions_to_deliveries(
    model_path = "model_xgb_outcome_longform.json",
    model_type = "xgb",
    format = "longform",
    column_prefix = "pred_xgb_lf",
    match_limit = MATCH_LIMIT
  )
} else {
  cli::cli_alert_warning("Skipping XGBoost long-form: model file not found")
}

# Done ----
cat("\n")
cli::cli_alert_success("All predictions added to deliveries table!")
cat("\n")
cat("You can now query predictions like:\n")
cat("  SELECT delivery_id, pred_bam_sf_wicket, pred_xgb_sf_wicket FROM deliveries LIMIT 10;\n")
cat("\n")
cat("Column naming convention:\n")
cat("  pred_{model}_{format}_{outcome}\n")
cat("    model:   bam or xgb\n")
cat("    format:  sf (short-form: T20/ODI) or lf (long-form: Test)\n")
cat("    outcome: wicket, 0, 1, 2, 3, 4, 6\n")
cat("\n")
