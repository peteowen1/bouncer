# Run All Model Pipelines ----
#
# Master script to run all predictive model training pipelines.
#
# This runs all model categories in the recommended order:
#   1. Ball-outcome models (per-delivery prediction)
#   2. Pre-match models (match winner prediction)
#   3. In-match models (live win probability)
#
# PREREQUISITES:
#   Rating calculations must be complete before running models.
#   Run data-raw/ratings/run_all_ratings.R first.
#
# Configuration:
#   Set the options below to control which pipelines run.
#
# Usage:
#   source("data-raw/models/run_all_models.R")

# 1. Configuration ----

# Set working directory to bouncer package root if needed
if (!file.exists("DESCRIPTION")) {
  if (file.exists("bouncer/DESCRIPTION")) {
    setwd("bouncer")
  } else {
    stop("Please run from the bouncer package root directory")
  }
}

# Choose which pipelines to run
RUN_BALL_OUTCOME <- TRUE      # Per-delivery outcome prediction
RUN_PRE_MATCH <- TRUE         # Pre-game match prediction
RUN_IN_MATCH <- TRUE          # Live win probability

# Ball-outcome settings
OUTCOME_FORMATS <- c("t20")   # Which formats: "t20", "odi", "test" (or combination)
OUTCOME_MODEL_TYPE <- "xgboost"  # "xgboost" (default), "bam", or "both"

# Pre-match settings
PRE_MATCH_FORMATS <- NULL     # NULL = all formats, or specific format

# In-match settings
IN_MATCH_EVENT <- "Indian Premier League"  # Event for in-match models
IN_MATCH_FORMAT <- "t20"      # "t20" or "odi" (Test not supported)

# Timing
start_time <- Sys.time()
pipeline_times <- list()

cat("\n")
cli::cli_h1("Complete Model Training Pipeline")
cli::cli_alert_info("Started at: {format(start_time, '%Y-%m-%d %H:%M:%S')}")
cat("\n")

# Summary of what will run
cli::cli_h3("Pipelines to Run")
cli::cli_bullets(c(
  if (RUN_BALL_OUTCOME) "v" else "x" = "Ball-Outcome Models ({paste(OUTCOME_FORMATS, collapse = ', ')})",
  if (RUN_PRE_MATCH) "v" else "x" = "Pre-Match Models",
  if (RUN_IN_MATCH) "v" else "x" = "In-Match Models ({IN_MATCH_EVENT})"
))
cat("\n")

# 2. Check Prerequisites ----

cli::cli_rule("Checking Prerequisites")

devtools::load_all()
conn <- get_db_connection(read_only = TRUE)
tables <- DBI::dbListTables(conn)

# Check for required rating tables
missing_tables <- c()

if (RUN_PRE_MATCH && !"team_elo" %in% tables) {
  missing_tables <- c(missing_tables, "team_elo")
}

for (fmt in unique(c(OUTCOME_FORMATS, IN_MATCH_FORMAT))) {
  skill_table <- paste0(fmt, "_player_skill")
  if (!skill_table %in% tables) {
    missing_tables <- c(missing_tables, skill_table)
  }
}

DBI::dbDisconnect(conn, shutdown = TRUE)

if (length(missing_tables) > 0) {
  cli::cli_alert_danger("Missing required rating tables:")
  for (tbl in missing_tables) {
    cli::cli_alert_danger("  - {tbl}")
  }
  cli::cli_alert_info("Run data-raw/ratings/run_all_ratings.R first")
  stop("Prerequisites not met")
}

cli::cli_alert_success("All required rating tables found")
cat("\n")

# 3. Ball-Outcome Models ----

if (RUN_BALL_OUTCOME) {
  cli::cli_rule("Ball-Outcome Models")

  for (fmt in OUTCOME_FORMATS) {
    cli::cli_h2("Format: {toupper(fmt)}")
    pipeline_start <- Sys.time()

    # Set model type for child script
    MODEL_TYPE <- OUTCOME_MODEL_TYPE

    runner_script <- sprintf("data-raw/models/ball-outcome/run_outcome_models_%s.R", fmt)

    if (!file.exists(runner_script)) {
      cli::cli_alert_warning("Runner script not found: {runner_script}")
      next
    }

    tryCatch({
      # Create environment with MODEL_TYPE set
      env <- new.env()
      env$MODEL_TYPE <- OUTCOME_MODEL_TYPE
      env$RUN_COMPARISON <- (OUTCOME_MODEL_TYPE == "both")
      env$RUN_VISUALIZATION <- (OUTCOME_MODEL_TYPE == "both")
      env$ADD_PREDICTIONS <- TRUE

      source(runner_script, local = env)

      pipeline_times[[paste0("ball_outcome_", fmt)]] <- difftime(Sys.time(), pipeline_start, units = "mins")
      cli::cli_alert_success("{toupper(fmt)} ball-outcome complete ({round(pipeline_times[[paste0('ball_outcome_', fmt)]], 1)} mins)")
    }, error = function(e) {
      cli::cli_alert_danger("{toupper(fmt)} ball-outcome failed: {e$message}")
      pipeline_times[[paste0("ball_outcome_", fmt)]] <- NA
    })

    cat("\n")
  }
}

# 4. Pre-Match Models ----

if (RUN_PRE_MATCH) {
  cli::cli_rule("Pre-Match Models")
  pipeline_start <- Sys.time()

  tryCatch({
    # Create environment with FORMAT_FILTER set
    env <- new.env()
    env$FORMAT_FILTER <- PRE_MATCH_FORMATS
    env$RUN_DIAGNOSTICS <- TRUE
    env$RUN_FEATURE_CALC <- TRUE
    env$RUN_TRAINING <- TRUE
    env$RUN_EVALUATION <- TRUE
    env$RUN_PREDICTIONS <- TRUE
    env$RUN_VISUALIZATION <- TRUE

    source("data-raw/models/pre-match/run_pre_match_pipeline.R", local = env)

    pipeline_times$pre_match <- difftime(Sys.time(), pipeline_start, units = "mins")
    cli::cli_alert_success("Pre-match models complete ({round(pipeline_times$pre_match, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Pre-match models failed: {e$message}")
    pipeline_times$pre_match <- NA
  })

  cat("\n")
}

# 5. In-Match Models ----

if (RUN_IN_MATCH) {
  cli::cli_rule("In-Match Models")
  pipeline_start <- Sys.time()

  tryCatch({
    # Create environment with event/format set
    env <- new.env()
    env$EVENT_FILTER <- IN_MATCH_EVENT
    env$FORMAT <- IN_MATCH_FORMAT
    env$TEST_SEASONS <- c("2024", "2024/25")
    env$RUN_PREPARATION <- TRUE
    env$RUN_BASELINE <- TRUE
    env$RUN_STAGE1 <- TRUE
    env$RUN_WIN_PROB_INN1 <- TRUE
    env$RUN_WIN_PROB_INN2 <- TRUE
    env$RUN_EVALUATION <- TRUE
    env$RUN_WPA_ANALYSIS <- TRUE

    source("data-raw/models/in-match/run_in_match_pipeline.R", local = env)

    pipeline_times$in_match <- difftime(Sys.time(), pipeline_start, units = "mins")
    cli::cli_alert_success("In-match models complete ({round(pipeline_times$in_match, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("In-match models failed: {e$message}")
    pipeline_times$in_match <- NA
  })

  cat("\n")
}

# 6. Final Summary ----

end_time <- Sys.time()
total_time <- difftime(end_time, start_time, units = "mins")

cli::cli_rule("All Pipelines Complete")
cat("\n")

# Pipeline timing summary
cli::cli_h3("Pipeline Timings")
for (pipeline_name in names(pipeline_times)) {
  time_val <- pipeline_times[[pipeline_name]]
  if (is.na(time_val)) {
    cli::cli_alert_danger("{pipeline_name}: FAILED")
  } else {
    cli::cli_alert_success("{pipeline_name}: {round(time_val, 1)} mins")
  }
}
cat("\n")

# Overall stats
successful <- sum(!sapply(pipeline_times, is.na))
total_pipelines <- length(pipeline_times)

cli::cli_alert_info("Pipelines completed: {successful}/{total_pipelines}")
cli::cli_alert_info("Total time: {round(total_time, 1)} minutes")

if (successful == total_pipelines) {
  cli::cli_alert_success("All model pipelines completed successfully!")
} else {
  cli::cli_alert_warning("Some pipelines failed - check output above")
}

cat("\n")
cli::cli_h3("Model Outputs")
cli::cli_bullets(c(
  "i" = "Ball-Outcome: 7-class delivery predictions (wicket, 0-6 runs)",
  "i" = "Pre-Match: Match winner probability before game",
  "i" = "In-Match: Win probability at each delivery during play"
))

cat("\n")
cli::cli_h3("Output Directory")
cli::cli_alert_info("All models saved to: bouncerdata/models/")
cat("\n")
