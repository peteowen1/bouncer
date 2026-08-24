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
OUTCOME_FORMATS <- c("t20", "odi", "test")  # Which formats: "t20", "odi", "test"

# The full model is OFF by default because it cannot currently train: its 3-way
# ELO inputs are empty (t20 0%, odi 16.9%, test 0.8%) and the ELO rebuild is
# still open. Turning this on today produces a failure, not a model.
# Tracked as #63 (rebuild the ELO) and #65 (retrain the full model).
RUN_FULL_OUTCOME <- FALSE

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

# Player skills are required by the FULL outcome model and the in-match models,
# not by the agnostic model — it is context-only by construction, which is the
# whole point of it. Demanding them for an agnostic-only run blocks a run that
# would have worked.
skill_formats <- unique(c(
  if (RUN_BALL_OUTCOME && RUN_FULL_OUTCOME) OUTCOME_FORMATS,
  if (RUN_IN_MATCH) IN_MATCH_FORMAT
))
for (fmt in skill_formats) {
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

  # Both trainers loop over formats internally, so they are sourced once each
  # rather than per format. Order matters: the agnostic model is the baseline
  # every residual rating is measured against, and the full model builds on it.
  outcome_steps <- list(
    agnostic = "data-raw/models/ball-outcome/01_train_agnostic_model.R"
  )
  if (RUN_FULL_OUTCOME) {
    outcome_steps$full <- "data-raw/models/ball-outcome/02_train_full_model.R"
  } else {
    cli::cli_alert_info("Full outcome model SKIPPED (RUN_FULL_OUTCOME = FALSE; see #65)")
  }

  for (step in names(outcome_steps)) {
    script <- outcome_steps[[step]]
    cli::cli_h2("Ball-outcome: {step} ({paste(OUTCOME_FORMATS, collapse = ', ')})")
    pipeline_start <- Sys.time()

    if (!file.exists(script)) {
      cli::cli_alert_warning("Trainer script not found: {script}")
      next
    }

    tryCatch({
      env <- new.env()
      env$FORMATS_TO_TRAIN <- OUTCOME_FORMATS

      source(script, local = env)

      pipeline_times[[paste0("ball_outcome_", step)]] <- difftime(Sys.time(), pipeline_start, units = "mins")
      cli::cli_alert_success("Ball-outcome {step} complete ({round(pipeline_times[[paste0('ball_outcome_', step)]], 1)} mins)")
    }, error = function(e) {
      cli::cli_alert_danger("Ball-outcome {step} failed: {e$message}")
      pipeline_times[[paste0("ball_outcome_", step)]] <- NA
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
