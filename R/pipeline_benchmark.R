# Pipeline Benchmark Tracking
#
# Persistent tracking of model performance metrics across pipeline runs.
# Enables comparison of model versions and regression detection.


#' Create Benchmark Results Table
#'
#' Creates the benchmark_results table for persistent metric tracking.
#'
#' @param conn DBI connection (read-write)
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
create_benchmark_table <- function(conn) {
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS benchmark_results (
      run_id VARCHAR,
      step_name VARCHAR,
      model_name VARCHAR,
      format VARCHAR,
      gender VARCHAR DEFAULT 'all',
      metric_name VARCHAR,
      metric_value DOUBLE,
      baseline_value DOUBLE,
      improvement_pct DOUBLE,
      n_train INTEGER,
      n_test INTEGER,
      date_range_start DATE,
      date_range_end DATE,
      run_timestamp TIMESTAMP,
      notes VARCHAR
    )
  ")
  invisible(TRUE)
}


#' Record a Benchmark Result
#'
#' Stores a single metric from a model training or evaluation run.
#'
#' @param conn DBI connection (read-write)
#' @param step_name Character. Pipeline step (e.g., "agnostic_model", "3way_elo")
#' @param model_name Character. Model identifier (e.g., "agnostic_outcome_t20")
#' @param format Character. Cricket format ("t20", "odi", "test")
#' @param metric_name Character. Metric name (e.g., "mlogloss", "brier_score", "poisson_loss")
#' @param metric_value Numeric. The metric value
#' @param baseline_value Numeric. Baseline comparison value (e.g., naive average)
#' @param n_train Integer. Number of training observations
#' @param n_test Integer. Number of test observations
#' @param date_range Date vector of length 2 (start, end of test data)
#' @param gender Character. Gender filter used (default "all")
#' @param notes Character. Optional notes about this run
#' @param run_id Character. Optional run ID for grouping metrics from the same run
#'
#' @return Invisibly returns TRUE
#' @keywords internal
record_benchmark <- function(conn, step_name, model_name, format,
                             metric_name, metric_value, baseline_value = NA,
                             n_train = NA, n_test = NA,
                             date_range = c(NA, NA),
                             gender = "all", notes = NULL,
                             run_id = NULL) {

  create_benchmark_table(conn)

  if (is.null(run_id)) {
    run_id <- base::format(Sys.time(), "%Y%m%d_%H%M%S")
  }

  improvement <- if (!is.na(baseline_value) && baseline_value != 0) {
    (baseline_value - metric_value) / baseline_value * 100
  } else {
    NA_real_
  }

  current_time <- base::format(Sys.time(), "%Y-%m-%d %H:%M:%S")

  DBI::dbExecute(conn, "
    INSERT INTO benchmark_results
      (run_id, step_name, model_name, format, gender, metric_name,
       metric_value, baseline_value, improvement_pct,
       n_train, n_test, date_range_start, date_range_end,
       run_timestamp, notes)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ", params = list(
    run_id, step_name, model_name, format, gender, metric_name,
    metric_value,
    if (is.na(baseline_value)) NA_real_ else baseline_value,
    if (is.na(improvement)) NA_real_ else improvement,
    if (is.na(n_train)) NA_integer_ else as.integer(n_train),
    if (is.na(n_test)) NA_integer_ else as.integer(n_test),
    if (is.na(date_range[1])) NA_character_ else as.character(date_range[1]),
    if (is.na(date_range[2])) NA_character_ else as.character(date_range[2]),
    current_time,
    if (is.null(notes)) NA_character_ else notes
  ))

  invisible(TRUE)
}


#' Record Multiple Benchmark Results
#'
#' Convenience function for recording several metrics from one model run.
#'
#' @param conn DBI connection (read-write)
#' @param step_name Character. Pipeline step name
#' @param model_name Character. Model identifier
#' @param format Character. Cricket format
#' @param metrics Named list of metric values (e.g., list(mlogloss = 1.23, accuracy = 0.45))
#' @param baselines Named list of baseline values (same names as metrics)
#' @param n_train Integer. Training set size
#' @param n_test Integer. Test set size
#' @param date_range Date vector of length 2
#' @param gender Character. Gender filter
#' @param notes Character. Optional notes
#'
#' @return Invisibly returns the run_id used
#' @export
record_benchmarks <- function(conn, step_name, model_name, format,
                              metrics, baselines = list(),
                              n_train = NA, n_test = NA,
                              date_range = c(NA, NA),
                              gender = "all", notes = NULL) {

  run_id <- base::format(Sys.time(), "%Y%m%d_%H%M%S")

  for (metric_name in names(metrics)) {
    baseline_val <- baselines[[metric_name]] %||% NA_real_

    record_benchmark(
      conn = conn,
      step_name = step_name,
      model_name = model_name,
      format = format,
      metric_name = metric_name,
      metric_value = metrics[[metric_name]],
      baseline_value = baseline_val,
      n_train = n_train,
      n_test = n_test,
      date_range = date_range,
      gender = gender,
      notes = notes,
      run_id = run_id
    )
  }

  invisible(run_id)
}


#' Get Latest Benchmark for a Model
#'
#' Retrieves the most recent benchmark results for a given model/step.
#'
#' @param conn DBI connection
#' @param step_name Character. Pipeline step name
#' @param format Character. Cricket format
#' @param metric_name Character. Optional - filter to specific metric
#'
#' @return Data frame with latest benchmark results
#' @export
get_latest_benchmark <- function(conn, step_name, format,
                                 metric_name = NULL) {
  if (!table_exists(conn, "benchmark_results")) {
    return(data.frame())
  }

  query <- "
    SELECT *
    FROM benchmark_results
    WHERE step_name = ?
      AND format = ?
      AND run_timestamp = (
        SELECT MAX(run_timestamp)
        FROM benchmark_results
        WHERE step_name = ?
          AND format = ?
      )
  "
  params <- list(step_name, format, step_name, format)

  if (!is.null(metric_name)) {
    query <- paste0(query, " AND metric_name = ?")
    params <- c(params, list(metric_name))
  }

  DBI::dbGetQuery(conn, query, params = params)
}


#' Compare Current Run Against Previous
#'
#' Checks if current metrics represent a regression from the last run.
#'
#' @param conn DBI connection
#' @param step_name Character. Pipeline step name
#' @param format Character. Cricket format
#' @param current_metrics Named list of current metric values
#' @param lower_is_better Character vector of metric names where lower = better
#'   (default: common loss metrics)
#' @param regression_threshold Numeric. Percentage threshold to flag regression
#'   (default 2 = 2% worse triggers warning)
#'
#' @return List with is_regression (logical), details (data.frame), messages (character vector)
#' @export
check_benchmark_regression <- function(conn, step_name, format,
                                       current_metrics,
                                       lower_is_better = c(
                                         "mlogloss", "logloss", "log_loss",
                                         "brier_score", "poisson_loss",
                                         "rmse", "mae"
                                       ),
                                       regression_threshold = 2) {
  previous <- get_latest_benchmark(conn, step_name, format)

  if (nrow(previous) == 0) {
    return(list(
      is_regression = FALSE,
      details = data.frame(),
      messages = "No previous benchmark - this is the first run"
    ))
  }

  messages <- character()
  is_regression <- FALSE
  comparisons <- list()

  for (metric_name in names(current_metrics)) {
    prev_row <- previous[previous$metric_name == metric_name, ]
    if (nrow(prev_row) == 0) next

    current_val <- current_metrics[[metric_name]]
    prev_val <- prev_row$metric_value[1]

    if (metric_name %in% lower_is_better) {
      pct_change <- (current_val - prev_val) / abs(prev_val) * 100
      got_worse <- pct_change > regression_threshold
    } else {
      pct_change <- (prev_val - current_val) / abs(prev_val) * 100
      got_worse <- pct_change > regression_threshold
    }

    comparisons[[length(comparisons) + 1]] <- data.frame(
      metric = metric_name,
      previous = prev_val,
      current = current_val,
      change_pct = round(pct_change, 2),
      status = if (got_worse) "REGRESSION" else "OK",
      stringsAsFactors = FALSE
    )

    if (got_worse) {
      is_regression <- TRUE
      messages <- c(messages, sprintf(
        "%s REGRESSED: %.4f -> %.4f (%+.1f%%)",
        metric_name, prev_val, current_val, pct_change
      ))
    }
  }

  details <- if (length(comparisons) > 0) {
    do.call(rbind, comparisons)
  } else {
    data.frame()
  }

  list(
    is_regression = is_regression,
    details = details,
    messages = if (length(messages) == 0) "All metrics stable or improved" else messages
  )
}


#' Get Benchmark History
#'
#' Returns full history of benchmark results for analysis and plotting.
#'
#' @param conn DBI connection
#' @param step_name Character. Optional filter by step
#' @param format Character. Optional filter by format
#' @param last_n Integer. Return only last N runs per model (default NULL = all)
#'
#' @return Data frame with benchmark history
#' @export
get_benchmark_history <- function(conn, step_name = NULL, format = NULL,
                                  last_n = NULL) {
  if (!table_exists(conn, "benchmark_results")) {
    return(data.frame())
  }

  query <- "SELECT * FROM benchmark_results WHERE 1=1"
  params <- list()

  if (!is.null(step_name)) {
    query <- paste0(query, " AND step_name = ?")
    params <- c(params, list(step_name))
  }

  if (!is.null(format)) {
    query <- paste0(query, " AND format = ?")
    params <- c(params, list(format))
  }

  query <- paste0(query, " ORDER BY run_timestamp DESC")

  if (!is.null(last_n)) {
    query <- paste0(query, " LIMIT ?")
    params <- c(params, list(as.integer(last_n * 10)))
  }

  DBI::dbGetQuery(conn, query, params = params)
}


#' Print Benchmark Summary
#'
#' Prints a formatted comparison of model performance across the pipeline.
#'
#' @param conn DBI connection
#' @param format Character. Cricket format to summarise
#'
#' @return Invisibly returns the summary data frame
#' @export
print_benchmark_summary <- function(conn, format = "t20") {
  if (!table_exists(conn, "benchmark_results")) {
    cli::cli_alert_warning("No benchmark data found. Run the pipeline to generate benchmarks.")
    return(invisible(data.frame()))
  }

  # Get latest results for each step
  summary <- DBI::dbGetQuery(conn, "
    WITH latest_runs AS (
      SELECT step_name, MAX(run_timestamp) as latest_run
      FROM benchmark_results
      WHERE format = ?
      GROUP BY step_name
    )
    SELECT
      b.step_name,
      b.model_name,
      b.metric_name,
      ROUND(b.metric_value, 6) as value,
      ROUND(b.baseline_value, 6) as baseline,
      ROUND(b.improvement_pct, 2) as improvement_pct,
      b.n_test,
      b.run_timestamp
    FROM benchmark_results b
    JOIN latest_runs lr ON b.step_name = lr.step_name
      AND b.run_timestamp = lr.latest_run
    WHERE b.format = ?
    ORDER BY b.step_name, b.metric_name
  ", params = list(format, format))

  if (nrow(summary) == 0) {
    cli::cli_alert_info("No benchmarks recorded for {toupper(format)} format")
    return(invisible(data.frame()))
  }

  cli::cli_h2("Benchmark Summary: {toupper(format)}")

  current_step <- ""
  for (i in seq_len(nrow(summary))) {
    row <- summary[i, ]

    if (row$step_name != current_step) {
      current_step <- row$step_name
      cat("\n")
      cli::cli_h3("{current_step}")
    }

    improvement_str <- if (!is.na(row$improvement_pct)) {
      sprintf(" (%+.1f%% vs baseline)", row$improvement_pct)
    } else {
      ""
    }

    cli::cli_alert_info("{row$metric_name}: {row$value}{improvement_str}")
  }

  cat("\n")
  invisible(summary)
}
