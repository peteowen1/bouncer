# Validate Data Quality
#
# Checks for common data quality issues across key tables.
# Run regularly to catch data corruption or ingestion issues.

library(DBI)
devtools::load_all()

validate_data_quality <- function() {
  cli::cli_h1("Data Quality Validation")

  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  all_passed <- TRUE
  issues <- list()

  # ============================================================================
  # Deliveries Table Checks
  # ============================================================================
  cli::cli_h2("Deliveries Table")

  # Check for NULL values in required columns
  null_checks <- list(
    match_id = "SELECT COUNT(*) AS n FROM deliveries WHERE match_id IS NULL",
    innings = "SELECT COUNT(*) AS n FROM deliveries WHERE innings IS NULL",
    over_number = "SELECT COUNT(*) AS n FROM deliveries WHERE over_number IS NULL",
    batter_id = "SELECT COUNT(*) AS n FROM deliveries WHERE batter_id IS NULL"
  )

  for (col in names(null_checks)) {
    null_count <- DBI::dbGetQuery(conn, null_checks[[col]])$n
    if (null_count == 0) {
      cli::cli_alert_success("No NULL {col} values")
    } else {
      cli::cli_alert_danger("{format(null_count, big.mark=',')} NULL {col} values")
      issues <- c(issues, paste("NULL", col, "in deliveries"))
      all_passed <- FALSE
    }
  }

  # Check ball values in range 1-6
  invalid_balls <- DBI::dbGetQuery(conn, "
    SELECT COUNT(*) AS n FROM deliveries
    WHERE ball_number < 1 OR ball_number > 6
  ")$n
  if (invalid_balls == 0) {
    cli::cli_alert_success("All ball numbers in valid range (1-6)")
  } else {
    cli::cli_alert_warning("{format(invalid_balls, big.mark=',')} deliveries with ball outside 1-6")
  }

  # Check wickets in range 0-10
  invalid_wickets <- DBI::dbGetQuery(conn, "
    SELECT COUNT(*) AS n FROM deliveries
    WHERE is_wicket < 0 OR is_wicket > 1
  ")$n
  if (invalid_wickets == 0) {
    cli::cli_alert_success("All is_wicket values valid (0 or 1)")
  } else {
    cli::cli_alert_danger("{format(invalid_wickets, big.mark=',')} invalid is_wicket values")
    all_passed <- FALSE
  }

  # ============================================================================
  # Matches Table Checks
  # ============================================================================
  cli::cli_h2("Matches Table")

  # Check for future dates (data quality issue)
  future_matches <- DBI::dbGetQuery(conn, "
    SELECT COUNT(*) AS n FROM matches
    WHERE match_date > CURRENT_DATE
  ")$n
  if (future_matches == 0) {
    cli::cli_alert_success("No matches with future dates")
  } else {
    cli::cli_alert_warning("{future_matches} matches have future dates")
  }

  # Check match_type values
  match_types <- DBI::dbGetQuery(conn, "
    SELECT DISTINCT match_type FROM matches ORDER BY match_type
  ")$match_type
  valid_types <- c("T20", "IT20", "ODI", "ODM", "Test", "MDM")
  invalid_types <- setdiff(match_types, valid_types)
  if (length(invalid_types) == 0) {
    cli::cli_alert_success("All match_type values valid")
  } else {
    cli::cli_alert_warning("Unexpected match_type values: {paste(invalid_types, collapse=', ')}")
  }

  # ============================================================================
  # Duplicate Checks
  # ============================================================================
  cli::cli_h2("Duplicate Checks")

  # Duplicate delivery_ids
  dup_deliveries <- DBI::dbGetQuery(conn, "
    SELECT COUNT(*) - COUNT(DISTINCT delivery_id) AS n FROM deliveries
  ")$n
  if (dup_deliveries == 0) {
    cli::cli_alert_success("No duplicate delivery_ids")
  } else {
    cli::cli_alert_danger("{format(dup_deliveries, big.mark=',')} duplicate delivery_ids")
    all_passed <- FALSE
  }

  # Duplicate match_ids
  dup_matches <- DBI::dbGetQuery(conn, "
    SELECT COUNT(*) - COUNT(DISTINCT match_id) AS n FROM matches
  ")$n
  if (dup_matches == 0) {
    cli::cli_alert_success("No duplicate match_ids")
  } else {
    cli::cli_alert_danger("{dup_matches} duplicate match_ids")
    all_passed <- FALSE
  }

  # ============================================================================
  # Summary
  # ============================================================================
  cli::cli_rule()
  if (all_passed) {
    cli::cli_alert_success("All data quality checks PASSED")
  } else {
    cli::cli_alert_danger("Some data quality checks FAILED")
    cli::cli_alert_info("Issues found: {paste(issues, collapse='; ')}")
  }

  invisible(all_passed)
}

# Run if executed directly
if (sys.nframe() == 0) {
  validate_data_quality()
}
