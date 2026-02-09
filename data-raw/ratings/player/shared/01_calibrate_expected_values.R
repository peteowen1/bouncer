# 01 Calibrate Expected Values ----
#
# This script calculates actual wicket rates and run distributions from the
# deliveries data. These calibration metrics are used to anchor ELO expected
# values to actual outcomes, preventing drift.
#
# Output:
#   - elo_calibration_metrics table populated in DuckDB
#
# Run this script before calculating ELOs, or periodically to update calibration.

# 1. Setup ----
library(DBI)
devtools::load_all()

cat("\n")
cli::cli_h1("ELO Calibration")
cat("\n")

# 2. Configuration ----
FORMAT_GROUPS <- list(
  t20 = c("T20", "IT20"),
  odi = c("ODI", "ODM"),
  test = c("Test", "MDM")
)

FORMAT_FILTER <- NULL  # NULL = all formats, or "t20", "odi", "test" for single format

# Determine formats to process
if (is.null(FORMAT_FILTER)) {
  formats_to_process <- names(FORMAT_GROUPS)
} else {
  formats_to_process <- FORMAT_FILTER
}
cli::cli_alert_info("Formats to calibrate: {paste(toupper(formats_to_process), collapse = ', ')}")

# 3. Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cli::cli_alert_success("Connected to database")

# 4. Ensure Calibration Table Exists ----
existing_tables <- DBI::dbListTables(conn)
if (!"elo_calibration_metrics" %in% existing_tables) {
  cli::cli_alert_info("Creating elo_calibration_metrics table...")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS elo_calibration_metrics (
      format VARCHAR,
      metric_type VARCHAR,
      metric_key VARCHAR,
      metric_value DOUBLE,
      sample_size INTEGER,
      calculated_date DATE,
      PRIMARY KEY (format, metric_type, metric_key)
    )
  ")
  cli::cli_alert_success("Created elo_calibration_metrics table")
}

# 5. Calculate Calibration for Each Format ----
cli::cli_h2("Calculating calibration metrics")

for (format in formats_to_process) {
  cli::cli_h3("Format: {toupper(format)}")

  # Calculate metrics
  calibration <- calculate_calibration_metrics(format, conn)

  if (is.null(calibration)) {
    cli::cli_alert_warning("No data found for {format}")
    next
  }

  # Display results
  cli::cli_alert_info("Total deliveries: {format(calibration$total_balls, big.mark = ',')}")
  cli::cli_alert_info("Wicket rate: {round(calibration$wicket_rate * 100, 2)}% ({round(1/calibration$wicket_rate, 1)} balls per wicket)")
  cli::cli_alert_info("Mean runs per ball: {round(calibration$mean_runs_per_ball, 3)}")
  cli::cli_alert_info("Mean outcome score: {round(calibration$mean_outcome_score, 4)} (for zero-sum ELO)")

  cat("\nRun distribution:\n")
  for (i in seq_len(nrow(calibration$run_distribution))) {
    cat(sprintf("  %d runs: %.1f%% (%s deliveries)\n",
                calibration$run_distribution$runs_batter[i],
                calibration$run_distribution$proportion[i] * 100,
                format(calibration$run_distribution$count[i], big.mark = ",")))
  }

  # Store in database
  store_calibration_metrics(calibration, conn)
}

# 6. Verify Stored Calibration ----
cat("\n")
cli::cli_h2("Stored Calibration Data")

for (format in formats_to_process) {
  cal_data <- get_calibration_data(format, conn)

  if (!is.null(cal_data)) {
    cli::cli_alert_success("{toupper(format)}: wicket_rate={round(cal_data$wicket_rate * 100, 2)}%, mean_runs={round(cal_data$mean_runs, 3)}, mean_outcome_score={round(cal_data$mean_outcome_score, 4)}")
  }
}

# 7. Summary ----
cat("\n")
cli::cli_alert_success("Calibration complete!")
cat("\n")

cli::cli_h3("Expected Values at ELO Parity (1500 vs 1500)")
for (format in formats_to_process) {
  cal_data <- get_calibration_data(format, conn)
  if (!is.null(cal_data)) {
    exp_runs <- calculate_expected_runs_calibrated(1500, 1500, cal_data)
    exp_wicket <- calculate_expected_wicket_calibrated(1500, 1500, cal_data)
    cat(sprintf("  %s: Expected outcome score = %.4f (base=%.4f), Expected wicket prob = %.3f%%\n",
                toupper(format), exp_runs, cal_data$mean_outcome_score, exp_wicket * 100))
  }
}

cli::cli_h3("Expected Values at +200 ELO Advantage")
for (format in formats_to_process) {
  cal_data <- get_calibration_data(format, conn)
  if (!is.null(cal_data)) {
    exp_runs <- calculate_expected_runs_calibrated(1700, 1500, cal_data)
    exp_wicket <- calculate_expected_wicket_calibrated(1500, 1700, cal_data)  # Bowler advantage
    cat(sprintf("  %s: Batter +200: exp_runs=%.3f | Bowler +200: exp_wicket=%.3f%%\n",
                toupper(format), exp_runs, exp_wicket * 100))
  }
}

# 8. Next Steps ----
cat("\n")
cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "Run 02_calculate_dual_elos.R to calculate player ELOs",
  "i" = "Calibration data stored in elo_calibration_metrics table"
))
cat("\n")
