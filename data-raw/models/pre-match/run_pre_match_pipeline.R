# Run Pre-Match Prediction Pipeline ----
#
# This script runs the complete pre-match prediction pipeline.
#
# Purpose: Predict match winner BEFORE the game starts using:
#   - Team ELO ratings (result-based and roster-based)
#   - Player skill aggregates (batting/bowling indices)
#   - Form metrics (last 5 matches)
#   - Head-to-head historical record
#   - Venue characteristics
#   - Toss information
#
# Supports: T20, ODI, Test (configurable via FORMAT_FILTER)
#
# CRITICAL DEPENDENCIES:
#   - team_elo table must be populated (run team ratings first)
#   - {format}_player_skill tables must exist (run player ratings first)
#
# Pipeline steps:
#   1. 00_diagnose_features.R - Data quality checks (optional)
#   2. 02_calculate_pre_match_features.R - Feature calculation
#   3. 03_train_prediction_model.R - XGBoost model training
#   4. 04_evaluate_model.R - Model evaluation
#   5. 05_generate_predictions.R - Generate predictions for database
#   6. 06_visualize_predictions.R - Visualization (optional)
#
# Usage:
#   source("data-raw/models/pre-match/run_pre_match_pipeline.R")

# 1. Configuration ----

# Set working directory to bouncer package root if needed
if (!file.exists("DESCRIPTION")) {
  if (file.exists("bouncer/DESCRIPTION")) {
    setwd("bouncer")
  } else {
    stop("Please run from the bouncer package root directory")
  }
}

# Format configuration
FORMAT_FILTER <- NULL  # NULL = all formats, or "t20", "odi", "test"

# Pipeline options
RUN_DIAGNOSTICS <- TRUE       # Run data quality diagnostics
RUN_FEATURE_CALC <- TRUE      # Run feature calculation
RUN_TRAINING <- TRUE          # Run model training
RUN_EVALUATION <- TRUE        # Run model evaluation
RUN_PREDICTIONS <- TRUE       # Generate predictions for database
RUN_VISUALIZATION <- TRUE     # Run visualization scripts

# Export configuration to environment for child scripts
if (!is.null(FORMAT_FILTER)) {
  Sys.setenv(BOUNCER_FORMAT = FORMAT_FILTER)
}

# Timing
start_time <- Sys.time()
step_times <- list()

# Determine formats to process
if (is.null(FORMAT_FILTER)) {
  formats_to_process <- c("t20", "odi", "test")
  format_display <- "ALL"
} else {
  formats_to_process <- FORMAT_FILTER
  format_display <- toupper(FORMAT_FILTER)
}

cat("\n")
cli::cli_h1("Pre-Match Prediction Pipeline")
cli::cli_alert_info("Started at: {format(start_time, '%Y-%m-%d %H:%M:%S')}")
cli::cli_alert_info("Format(s): {format_display}")
cat("\n")

# 2. Check Prerequisites ----

cli::cli_rule("Checking Prerequisites")

devtools::load_all()
conn <- get_db_connection(read_only = TRUE)

# Check team_elo table
tables <- DBI::dbListTables(conn)
if (!"team_elo" %in% tables) {
  DBI::dbDisconnect(conn, shutdown = TRUE)
  cli::cli_abort("team_elo table not found - run ratings/team/run_team_elo_pipeline.R first")
}

team_elo_count <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM team_elo")$n
cli::cli_alert_success("team_elo table found ({format(team_elo_count, big.mark = ',')} records)")

# Check player skill tables for each format
for (fmt in formats_to_process) {
  skill_table <- paste0(fmt, "_player_skill")
  if (!skill_table %in% tables) {
    DBI::dbDisconnect(conn, shutdown = TRUE)
    cli::cli_abort("{skill_table} not found - run ratings/player/run_skill_index_pipeline.R first")
  }
  skill_count <- DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) as n FROM %s", skill_table))$n
  cli::cli_alert_success("{skill_table} found ({format(skill_count, big.mark = ',')} records)")
}

DBI::dbDisconnect(conn, shutdown = TRUE)
cat("\n")

# 3. Diagnostics ----

if (RUN_DIAGNOSTICS) {
  cli::cli_rule("Step 1: Data Quality Diagnostics")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/pre-match/00_diagnose_features.R", local = new.env())
    step_times$diagnostics <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Diagnostics complete ({round(step_times$diagnostics, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_warning("Diagnostics failed: {e$message}")
    cli::cli_alert_info("Continuing with pipeline...")
    step_times$diagnostics <- NA
  })

  cat("\n")
}

# 4. Feature Calculation ----

if (RUN_FEATURE_CALC) {
  cli::cli_rule("Step 2: Feature Calculation")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/pre-match/02_calculate_pre_match_features.R", local = new.env())
    step_times$feature_calc <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Feature calculation complete ({round(step_times$feature_calc, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Feature calculation failed: {e$message}")
    cli::cli_alert_danger("Cannot continue - features are required for training")
    stop(e)
  })

  cat("\n")
}

# 5. Model Training ----

if (RUN_TRAINING) {
  cli::cli_rule("Step 3: Model Training")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/pre-match/03_train_prediction_model.R", local = new.env())
    step_times$training <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Model training complete ({round(step_times$training, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Model training failed: {e$message}")
    step_times$training <- NA
  })

  cat("\n")
}

# 6. Evaluation ----

if (RUN_EVALUATION) {
  cli::cli_rule("Step 4: Model Evaluation")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/pre-match/04_evaluate_model.R", local = new.env())
    step_times$evaluation <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Evaluation complete ({round(step_times$evaluation, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Evaluation failed: {e$message}")
    step_times$evaluation <- NA
  })

  cat("\n")
}

# 7. Generate Predictions ----

if (RUN_PREDICTIONS) {
  cli::cli_rule("Step 5: Generate Predictions")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/pre-match/05_generate_predictions.R", local = new.env())
    step_times$predictions <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Predictions generated ({round(step_times$predictions, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Prediction generation failed: {e$message}")
    step_times$predictions <- NA
  })

  cat("\n")
}

# 8. Visualization ----

if (RUN_VISUALIZATION) {
  cli::cli_rule("Step 6: Visualization")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/pre-match/06_visualize_predictions.R", local = new.env())
    step_times$visualization <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Visualization complete ({round(step_times$visualization, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_warning("Visualization failed: {e$message}")
    cli::cli_alert_info("This may require ggplot2 package")
    step_times$visualization <- NA
  })

  cat("\n")
}

# 9. Summary ----

end_time <- Sys.time()
total_time <- difftime(end_time, start_time, units = "mins")

# Clean up environment variables
Sys.unsetenv("BOUNCER_FORMAT")

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
  "*" = "bouncerdata/models/{format}_prediction_model.ubj (XGBoost model)",
  "*" = "bouncerdata/models/{format}_prediction_features.rds (train/test data)",
  "*" = "bouncerdata/models/{format}_prediction_training.rds (results)",
  "*" = "bouncerdata/models/{format}_prediction_evaluation.rds (metrics)"
))

cat("\n")
cli::cli_h3("Database Tables")
cli::cli_bullets(c(
  "i" = "pre_match_features: 31 features per match",
  "i" = "pre_match_predictions: Model predictions per match"
))

cat("\n")
cli::cli_h3("Feature Categories (31 total)")
cli::cli_bullets(c(
  "i" = "Team ELO: elo_result, elo_roster_combined, elo_diff",
  "i" = "Player Skills: batting/bowling aggregates per team",
  "i" = "Form: Last 5 match results",
  "i" = "Head-to-Head: Historical win rate",
  "i" = "Venue: Home/away, ground characteristics",
  "i" = "Toss: Winner, decision (bat/bowl)"
))
cat("\n")
