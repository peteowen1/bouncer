# Pipeline State Management
#
# Functions for tracking pipeline execution state to enable smart caching
# and skipping of steps when data hasn't changed significantly.


#' Create Pipeline State Table
#'
#' Creates the pipeline_state table for tracking step execution history.
#'
#' @param conn DBI connection
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
create_pipeline_state_table <- function(conn) {
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS pipeline_state (
      step_name VARCHAR PRIMARY KEY,
      last_run_at TIMESTAMP,
      last_match_date DATE,
      last_match_count INTEGER,
      last_delivery_count INTEGER,
      status VARCHAR
    )
  ")
  invisible(TRUE)
}


#' Get Current Data Statistics
#'
#' Returns current counts of matches and deliveries in the database.
#'
#' @param conn DBI connection
#'
#' @return Named list with match_count, delivery_count, last_match_date
#' @keywords internal
get_data_stats <- function(conn) {
  stats <- DBI::dbGetQuery(conn, "
    SELECT
      (SELECT COUNT(*) FROM matches) as match_count,
      (SELECT COUNT(*) FROM deliveries) as delivery_count,
      (SELECT MAX(match_date) FROM matches) as last_match_date
  ")

  list(
    match_count = stats$match_count,
    delivery_count = stats$delivery_count,
    last_match_date = as.Date(stats$last_match_date)
  )
}


#' Get Pipeline State for a Step
#'
#' Retrieves the last execution state for a pipeline step.
#'
#' @param step_name Character. Name of the pipeline step
#' @param conn DBI connection
#'
#' @return Data frame with state info, or NULL if step never run
#' @keywords internal
get_pipeline_state <- function(step_name, conn) {
  # Ensure table exists
  if (!"pipeline_state" %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  result <- DBI::dbGetQuery(conn, sprintf("
    SELECT *
    FROM pipeline_state
    WHERE step_name = '%s'
  ", step_name))

  if (nrow(result) == 0) {
    return(NULL)
  }

  result
}


#' Update Pipeline State
#'
#' Records the execution state after a pipeline step completes.
#'
#' @param step_name Character. Name of the pipeline step
#' @param conn DBI connection
#' @param status Character. "success", "failed", or "skipped"
#'
#' @return Invisibly returns TRUE
#' @keywords internal
update_pipeline_state <- function(step_name, conn, status = "success") {
  # Ensure table exists
  create_pipeline_state_table(conn)

  # Get current data stats
  stats <- get_data_stats(conn)

  # Generate timestamp in R (avoids DuckDB CURRENT_TIMESTAMP issues)
  current_time <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

  # Upsert state
  DBI::dbExecute(conn, sprintf("
    INSERT INTO pipeline_state (step_name, last_run_at, last_match_date, last_match_count, last_delivery_count, status)
    VALUES ('%s', '%s', '%s', %d, %d, '%s')
    ON CONFLICT (step_name) DO UPDATE SET
      last_run_at = '%s',
      last_match_date = EXCLUDED.last_match_date,
      last_match_count = EXCLUDED.last_match_count,
      last_delivery_count = EXCLUDED.last_delivery_count,
      status = EXCLUDED.status
  ", step_name,
     current_time,
     as.character(stats$last_match_date),
     stats$match_count,
     stats$delivery_count,
     status,
     current_time))

  invisible(TRUE)
}


#' Get New Data Count Since Step Last Run
#'
#' Returns counts of new matches and deliveries since a step was last run.
#'
#' @param step_name Character. Name of the pipeline step
#' @param conn DBI connection
#'
#' @return Named list with new_matches and new_deliveries
#' @keywords internal
get_new_data_since <- function(step_name, conn) {
  state <- get_pipeline_state(step_name, conn)
  current <- get_data_stats(conn)

  if (is.null(state)) {
    # Never run - all data is "new"
    return(list(
      new_matches = current$match_count,
      new_deliveries = current$delivery_count,
      never_run = TRUE
    ))
  }

  list(
    new_matches = current$match_count - state$last_match_count,
    new_deliveries = current$delivery_count - state$last_delivery_count,
    never_run = FALSE
  )
}


#' Check If Step Should Be Skipped
#'
#' Determines if a pipeline step can be skipped based on data changes.
#'
#' @param step_name Character. Name of the pipeline step
#' @param conn DBI connection
#' @param delivery_threshold Integer. Min new deliveries to require running (default 0)
#' @param match_threshold Integer. Min new matches to require running (default 0)
#'
#' @return Named list with should_skip (logical), reason (character), and new_data counts
#' @keywords internal
should_skip_step <- function(step_name, conn,
                              delivery_threshold = 0,
                              match_threshold = 0) {
  new_data <- get_new_data_since(step_name, conn)

  # Never run before - must run

  if (new_data$never_run) {
    return(list(
      should_skip = FALSE,
      reason = "Never run before",
      new_matches = new_data$new_matches,
      new_deliveries = new_data$new_deliveries
    ))
  }

  # Check thresholds
  if (delivery_threshold > 0 && new_data$new_deliveries < delivery_threshold) {
    return(list(
      should_skip = TRUE,
      reason = sprintf("Only %s new deliveries (threshold: %s)",
                       format(new_data$new_deliveries, big.mark = ","),
                       format(delivery_threshold, big.mark = ",")),
      new_matches = new_data$new_matches,
      new_deliveries = new_data$new_deliveries
    ))
  }

  if (match_threshold > 0 && new_data$new_matches < match_threshold) {
    return(list(
      should_skip = TRUE,
      reason = sprintf("Only %d new matches (threshold: %d)",
                       new_data$new_matches, match_threshold),
      new_matches = new_data$new_matches,
      new_deliveries = new_data$new_deliveries
    ))
  }

  # No new data at all
  if (new_data$new_deliveries == 0 && new_data$new_matches == 0) {
    return(list(
      should_skip = TRUE,
      reason = "No new data",
      new_matches = 0,
      new_deliveries = 0
    ))
  }

  # Has new data above thresholds - should run
  list(
    should_skip = FALSE,
    reason = sprintf("%s new deliveries, %d new matches",
                     format(new_data$new_deliveries, big.mark = ","),
                     new_data$new_matches),
    new_matches = new_data$new_matches,
    new_deliveries = new_data$new_deliveries
  )
}


#' Print Skip Decision
#'
#' Helper to print formatted skip/run decision for a step.
#'
#' @param step_name Character. Display name for the step
#' @param decision List. Result from should_skip_step()
#'
#' @return Invisibly returns the decision
#' @keywords internal
print_skip_decision <- function(step_name, decision) {
  if (decision$should_skip) {
    cli::cli_alert_info("{step_name}: Skipped ({decision$reason})")
  } else {
    cli::cli_alert_success("{step_name}: Running ({decision$reason})")
  }
  invisible(decision)
}


#' Get Pipeline Summary
#'
#' Returns summary of all pipeline steps and their last run status.
#'
#' @param conn DBI connection
#'
#' @return Data frame with all step states
#' @keywords internal
get_pipeline_summary <- function(conn) {
  if (!"pipeline_state" %in% DBI::dbListTables(conn)) {
    return(data.frame())
  }

  DBI::dbGetQuery(conn, "
    SELECT
      step_name,
      last_run_at,
      last_match_count,
      last_delivery_count,
      status
    FROM pipeline_state
    ORDER BY step_name
  ")
}


# Pipeline Helper Functions --------------------------------------------------
#
# Internal functions used by run_full_pipeline.R


#' Check if a pipeline step should run
#'
#' @param step Step number to check
#' @param steps_to_run Vector of steps to run, or NULL for all
#' @return TRUE if step should run
#' @keywords internal
pipeline_should_run <- function(step, steps_to_run = NULL) {
  is.null(steps_to_run) || step %in% steps_to_run
}


#' Print step completion with timing
#'
#' @param step_name Name of the step
#' @param step_time difftime object with step duration
#' @keywords internal
pipeline_step_complete <- function(step_name, step_time) {
  mins <- round(as.numeric(step_time), 1)
  cat("\n")
  cli::cli_rule()
  cli::cli_alert_success("{step_name} COMPLETE - Duration: {mins} minutes")
  cli::cli_rule()
  cat("\n")
}
