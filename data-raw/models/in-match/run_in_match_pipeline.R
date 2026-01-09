# Run In-Match Win Probability Pipeline ----
#
# This script runs the complete in-match prediction pipeline for any event.
#
# Pipeline Overview:
#   Stage 1: Projected Score Model (what will batting team score?)
#   Stage 2: Win Probability Model (who will win from here?)
#
# The pipeline supports any limited-overs event with chase scenarios:
#   - T20 leagues: IPL, BBL, PSL, CPL, etc.
#   - ODI events: World Cup, Champions Trophy, etc.
#
# NOT APPLICABLE for Test cricket (no fixed target/chase dynamic).
#
# Pipeline steps (STRICT SEQUENTIAL - each depends on previous):
#   1. 01_prepare_data.R - Data preparation and feature engineering
#   2. 02_baseline_projected_score.R - Venue/context baseline model
#   3. 03_projected_score_model.R - Stage 1: Predict final score
#   4. 04_win_probability_innings1.R - Win prob during 1st innings
#   5. 05_win_probability_innings2.R - Win prob during chase
#   6. 06_evaluate_predictions.R - Model evaluation
#   7. 07_wpa_era_analysis.R - WPA/ERA player impact metrics
#
# Usage:
#   source("data-raw/models/in-match/run_in_match_pipeline.R")

# 1. Configuration ----

# Set working directory to bouncer package root if needed
if (!file.exists("DESCRIPTION")) {
  if (file.exists("bouncer/DESCRIPTION")) {
    setwd("bouncer")
  } else {
    stop("Please run from the bouncer package root directory")
  }
}

# Event configuration
EVENT_FILTER <- "Indian Premier League"  # Event name (use LIKE pattern matching)
FORMAT <- "t20"  # "t20" or "odi" (Test not supported - no chase scenario)

# Holdout seasons for testing (model won't see these during training)
TEST_SEASONS <- c("2024", "2024/25")

# Pipeline options
RUN_PREPARATION <- TRUE       # Run data preparation
RUN_BASELINE <- TRUE          # Run baseline model
RUN_STAGE1 <- TRUE            # Run projected score model
RUN_WIN_PROB_INN1 <- TRUE     # Run 1st innings win probability
RUN_WIN_PROB_INN2 <- TRUE     # Run 2nd innings win probability
RUN_EVALUATION <- TRUE        # Run model evaluation
RUN_WPA_ANALYSIS <- TRUE      # Run WPA/ERA analysis

# Export configuration to environment for child scripts
Sys.setenv(
  BOUNCER_EVENT_FILTER = EVENT_FILTER,
  BOUNCER_FORMAT = FORMAT,
  BOUNCER_TEST_SEASONS = paste(TEST_SEASONS, collapse = ",")
)

# Timing
start_time <- Sys.time()
step_times <- list()

cat("\n")
cli::cli_h1("In-Match Win Probability Pipeline")
cli::cli_alert_info("Started at: {format(start_time, '%Y-%m-%d %H:%M:%S')}")
cli::cli_alert_info("Event: {EVENT_FILTER}")
cli::cli_alert_info("Format: {toupper(FORMAT)}")
cli::cli_alert_info("Test seasons (holdout): {paste(TEST_SEASONS, collapse = ', ')}")
cat("\n")

# 2. Validate Configuration ----

if (FORMAT == "test") {
  cli::cli_abort("Test cricket is not supported for in-match models (no chase scenario)")
}

# 3. Data Preparation ----

if (RUN_PREPARATION) {
  cli::cli_rule("Step 1: Data Preparation")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/in-match/01_prepare_data.R", local = new.env())
    step_times$preparation <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Data preparation complete ({round(step_times$preparation, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Data preparation failed: {e$message}")
    cli::cli_alert_danger("Cannot continue - data preparation is required")
    stop(e)
  })

  cat("\n")
}

# 4. Baseline Model ----

if (RUN_BASELINE) {
  cli::cli_rule("Step 2: Baseline Projected Score Model")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/in-match/02_baseline_projected_score.R", local = new.env())
    step_times$baseline <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Baseline model complete ({round(step_times$baseline, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Baseline model failed: {e$message}")
    step_times$baseline <- NA
  })

  cat("\n")
}

# 5. Stage 1: Projected Score Model ----

if (RUN_STAGE1) {
  cli::cli_rule("Step 3: Stage 1 - Projected Score Model")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/in-match/03_projected_score_model.R", local = new.env())
    step_times$stage1 <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Stage 1 model complete ({round(step_times$stage1, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Stage 1 model failed: {e$message}")
    cli::cli_alert_danger("Cannot continue - Stage 1 is required for Stage 2")
    stop(e)
  })

  cat("\n")
}

# 6. Win Probability - 1st Innings ----

if (RUN_WIN_PROB_INN1) {
  cli::cli_rule("Step 4: Win Probability (1st Innings)")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/in-match/04_win_probability_innings1.R", local = new.env())
    step_times$win_prob_inn1 <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("1st innings win prob complete ({round(step_times$win_prob_inn1, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("1st innings win prob failed: {e$message}")
    step_times$win_prob_inn1 <- NA
  })

  cat("\n")
}

# 7. Win Probability - 2nd Innings (Chase) ----

if (RUN_WIN_PROB_INN2) {
  cli::cli_rule("Step 5: Win Probability (2nd Innings / Chase)")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/in-match/05_win_probability_innings2.R", local = new.env())
    step_times$win_prob_inn2 <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("2nd innings win prob complete ({round(step_times$win_prob_inn2, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("2nd innings win prob failed: {e$message}")
    step_times$win_prob_inn2 <- NA
  })

  cat("\n")
}

# 8. Evaluation ----

if (RUN_EVALUATION) {
  cli::cli_rule("Step 6: Model Evaluation")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/in-match/06_evaluate_predictions.R", local = new.env())
    step_times$evaluation <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("Evaluation complete ({round(step_times$evaluation, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Evaluation failed: {e$message}")
    step_times$evaluation <- NA
  })

  cat("\n")
}

# 9. WPA/ERA Analysis ----

if (RUN_WPA_ANALYSIS) {
  cli::cli_rule("Step 7: WPA/ERA Analysis")
  step_start <- Sys.time()

  tryCatch({
    source("data-raw/models/in-match/07_wpa_era_analysis.R", local = new.env())
    step_times$wpa_analysis <- difftime(Sys.time(), step_start, units = "mins")
    cli::cli_alert_success("WPA/ERA analysis complete ({round(step_times$wpa_analysis, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("WPA/ERA analysis failed: {e$message}")
    step_times$wpa_analysis <- NA
  })

  cat("\n")
}

# 10. Summary ----

end_time <- Sys.time()
total_time <- difftime(end_time, start_time, units = "mins")

# Clean up environment variables
Sys.unsetenv(c("BOUNCER_EVENT_FILTER", "BOUNCER_FORMAT", "BOUNCER_TEST_SEASONS"))

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

# Derive event name prefix for file naming
event_prefix <- tolower(gsub(" ", "_", EVENT_FILTER))
event_prefix <- gsub("[^a-z0-9_]", "", event_prefix)

cli::cli_h3("Output Files")
cli::cli_bullets(c(
  "*" = "bouncerdata/models/{event_prefix}_stage1_data.rds",
  "*" = "bouncerdata/models/{event_prefix}_stage2_data.rds",
  "*" = "bouncerdata/models/{event_prefix}_stage1_results.rds",
  "*" = "bouncerdata/models/{event_prefix}_stage2_results.rds",
  "*" = "bouncerdata/models/{event_prefix}_venue_stats.rds"
))

cat("\n")
cli::cli_h3("Model Outputs")
cli::cli_bullets(c(
  "i" = "Projected Score: Predicts final innings total at any point",
  "i" = "Win Probability: P(batting team wins) at each delivery",
  "i" = "WPA: Win Probability Added per delivery/player",
  "i" = "ERA: Expected Runs Added per delivery/player"
))

cat("\n")
cli::cli_h3("Supported Events")
cli::cli_bullets(c(
  "i" = "T20: Indian Premier League, Big Bash League, PSL, CPL, etc.",
  "i" = "ODI: ICC Cricket World Cup, Champions Trophy, bilateral series",
  "i" = "NOT supported: Test cricket (no chase scenario)"
))
cat("\n")
