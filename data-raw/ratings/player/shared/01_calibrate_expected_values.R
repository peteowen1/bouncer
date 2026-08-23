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
# One declaration of this table lives in create_elo_calibration_metrics_table();
# this step used to carry its own copy. See that function for why.
if (!"elo_calibration_metrics" %in% DBI::dbListTables(conn)) {
  cli::cli_alert_info("Creating elo_calibration_metrics table...")
  create_elo_calibration_metrics_table(conn)
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

# The two "Expected Values" demo blocks that stood here are removed
# (bouncerverse#63). They called calculate_expected_runs_calibrated() and its
# wicket sibling, which the 2026-02-09 sweep deleted along with the rest of
# this script's dependencies, and which are built on DUAL_ELO_DIVISOR from the
# deprecated dual-ELO engine. They printed an illustration; they fed nothing.
# Restoring a deprecated engine to print two lines is the wrong trade, so the
# lines go instead. The calibration this script exists to compute and store is
# unaffected.


# 8. Next Steps ----
cat("\n")
cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  # Was "Run 02_calculate_dual_elos.R" -- the dual-ELO engine is deprecated
  # and archived in data-raw/_deprecated/. The live consumer is the 3-way ELO.
  "i" = "Run ratings/player/3way-elo/01_calculate_3way_elo.R to calculate player ELOs",
  "i" = "Calibration data stored in elo_calibration_metrics table"
))
cat("\n")
