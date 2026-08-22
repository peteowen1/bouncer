# Computing and storing the ELO calibration.
#
# RESTORED 2026-08-20 (bouncerverse#63) from 442f6ae^. The dead-code sweep that
# removed get_calibration_data() also removed these, and
# `data-raw/ratings/player/shared/01_calibrate_expected_values.R` -- the
# documented prerequisite for the 3-way ELO rebuild -- calls them. So the
# calibration could not be RECOMPUTED either.
#
# The rebuild runs today only because the `elo_calibration_metrics` table these
# write was never dropped, so the stored values date from before 2026-02-09.
# They are global per-format means over millions of deliveries, so seven more
# months of data moves them very little -- but they were unrefreshable.
#
# NOT restored: calculate_expected_runs_calibrated() and its wicket sibling.
# They are called only from a printed summary at the end of that script and
# depend on DUAL_ELO_DIVISOR, from the deprecated dual-ELO system. Resurrecting
# a deprecated engine to print a demo line is the wrong trade; the demo block
# is removed from the script instead.
#
# Found by data-raw/validation/pipeline_call_audit.R, which exists because the
# first instance of this went unnoticed for six months.

# Outcome scores per delivery result. Recovered from 442f6ae^:R/constants_elo.R,
# where the sweep also deleted them; values match the inline comments in
# calculate_run_outcome_score() below.
RUN_SCORE_WICKET <- 0.0    # Worst outcome
RUN_SCORE_DOT    <- 0.15   # Slight credit for survival
RUN_SCORE_SINGLE <- 0.35
RUN_SCORE_DOUBLE <- 0.45
RUN_SCORE_THREE  <- 0.55
RUN_SCORE_FOUR   <- 0.75
RUN_SCORE_SIX    <- 1.0    # Best outcome

#' Calculate Run Outcome Score for ELO Update
#'
#' Converts batter runs into a 0-1 score for ELO update calculation.
#' Uses the scoring weights from constants.R.
#'
#' @param runs Integer. Runs scored by batter on this delivery
#' @param is_wicket Logical. Whether batter was dismissed
#' @param is_boundary Logical. Whether runs came from boundary (optional)
#'
#' @return Numeric. Score between 0 and 1
#' @keywords internal
calculate_run_outcome_score <- function(runs, is_wicket, is_boundary = FALSE) {

  if (is_wicket) {
    return(RUN_SCORE_WICKET)  # 0.0
  }

  score <- switch(as.character(runs),
    "0" = RUN_SCORE_DOT,      # 0.15
    "1" = RUN_SCORE_SINGLE,   # 0.35
    "2" = RUN_SCORE_DOUBLE,   # 0.45
    "3" = RUN_SCORE_THREE,    # 0.55
    "4" = RUN_SCORE_FOUR,     # 0.75
    "5" = 0.85,               # Rare - interpolate
    "6" = RUN_SCORE_SIX,      # 1.0
    min(1.0, 0.15 + runs * 0.15)  # Default for higher values
  )

  return(score)
}

#' Calculate Calibration Metrics from Data
#'
#' Analyzes deliveries table to compute actual wicket rates and run distributions
#' for a specific format. Used to calibrate expected values in ELO calculations.
#'
#' @param format Character. Match format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return List with calibration metrics:
#'   - format: The format analyzed
#'   - total_balls: Total deliveries analyzed
#'   - wicket_rate: Proportion of deliveries resulting in wicket
#'   - mean_runs_per_ball: Average batter runs per ball
#'   - mean_outcome_score: Average outcome score (using scoring weights)
#'   - run_distribution: Data frame with run value frequencies
#' @keywords internal
calculate_calibration_metrics <- function(format = "t20", conn) {

  # Query actual outcomes
  stats <- DBI::dbGetQuery(conn, "
    SELECT
      COUNT(*) as total_balls,
      SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as total_wickets,
      SUM(runs_batter) as total_runs,
      AVG(CAST(runs_batter AS DOUBLE)) as mean_runs_per_ball
    FROM cricsheet.deliveries
    WHERE LOWER(match_type) IN (?, ?, ?)
      AND is_wicket IS NOT NULL
  ", params = list(
    tolower(format),
    paste0("i", tolower(format)),  # IT20, etc.
    toupper(format)
  ))

  if (stats$total_balls == 0) {
    cli::cli_alert_warning("No deliveries found for format: {format}")
    return(NULL)
  }

  # Run distribution (for scoring weights validation)
  run_dist <- DBI::dbGetQuery(conn, "
    SELECT
      runs_batter,
      COUNT(*) as count,
      COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () as proportion
    FROM cricsheet.deliveries
    WHERE LOWER(match_type) IN (?, ?, ?)
      AND is_wicket IS NOT NULL
      AND NOT is_wicket
    GROUP BY runs_batter
    ORDER BY runs_batter
  ", params = list(
    tolower(format),
    paste0("i", tolower(format)),
    # was toupper(format) -- compared against LOWER(match_type), so it could
    # never match. Dead weight that read as if it caught a third variant.
    paste0(tolower(format), "s")
  ))

  # Calculate mean outcome score using the scoring weights
  # This is the key calibration metric for zero-sum ELO
  wicket_rate <- stats$total_wickets / stats$total_balls

  # Calculate weighted average of run outcome scores (excluding wickets)
  run_outcome_sum <- 0
  for (i in seq_len(nrow(run_dist))) {
    runs <- run_dist$runs_batter[i]
    prop <- run_dist$proportion[i]
    score <- calculate_run_outcome_score(runs, is_wicket = FALSE, is_boundary = FALSE)
    run_outcome_sum <- run_outcome_sum + prop * score
  }

  # Mean outcome score = P(wicket) * 0 + P(not wicket) * E[score | not wicket]
  mean_outcome_score <- wicket_rate * RUN_SCORE_WICKET + (1 - wicket_rate) * run_outcome_sum

  list(
    format = format,
    total_balls = stats$total_balls,
    wicket_rate = wicket_rate,
    mean_runs_per_ball = stats$mean_runs_per_ball,
    mean_outcome_score = mean_outcome_score,
    run_distribution = run_dist
  )
}

#' Store Calibration Metrics in Database
#'
#' Stores calculated calibration metrics in the elo_calibration_metrics table.
#'
#' @param calibration List. Output from calculate_calibration_metrics()
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns number of rows inserted
#' @keywords internal
store_calibration_metrics <- function(calibration, conn) {

  if (is.null(calibration)) {
    return(invisible(0))
  }

  format <- calibration$format
  calc_date <- Sys.Date()

  # The refresh must work on a database that has not had the full schema
  # applied -- otherwise recalibrating is gated on a step that has nothing to
  # do with calibration.
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS elo_calibration_metrics (
      format VARCHAR, metric_type VARCHAR, metric_key VARCHAR,
      metric_value DOUBLE, sample_size INTEGER, calculated_date VARCHAR)")

  rows <- 0
  # DELETE-then-INSERT in ONE transaction. Unwrapped, an interruption between
  # the two leaves the format with NO calibration, and the ELO rebuild that
  # reads it then falls back to defaults without saying so -- the same shape as
  # the whole-table drops in #45 and #70.
  rows <- .in_transaction(conn, function() {
    DBI::dbExecute(conn, "DELETE FROM elo_calibration_metrics WHERE format = ?",
                   params = list(format))

  # Store wicket rate
  DBI::dbExecute(conn, "
    INSERT INTO elo_calibration_metrics (format, metric_type, metric_key, metric_value, sample_size, calculated_date)
    VALUES (?, 'wicket_rate', 'overall', ?, ?, ?)
  ", params = list(format, calibration$wicket_rate, calibration$total_balls, as.character(calc_date)))
  rows <- rows + 1

  # Store mean runs
  DBI::dbExecute(conn, "
    INSERT INTO elo_calibration_metrics (format, metric_type, metric_key, metric_value, sample_size, calculated_date)
    VALUES (?, 'mean_runs', 'overall', ?, ?, ?)
  ", params = list(format, calibration$mean_runs_per_ball, calibration$total_balls, as.character(calc_date)))
  rows <- rows + 1

  # Store mean outcome score (critical for zero-sum ELO)
  DBI::dbExecute(conn, "
    INSERT INTO elo_calibration_metrics (format, metric_type, metric_key, metric_value, sample_size, calculated_date)
    VALUES (?, 'mean_outcome_score', 'overall', ?, ?, ?)
  ", params = list(format, calibration$mean_outcome_score, calibration$total_balls, as.character(calc_date)))
  rows <- rows + 1

  # Store run distribution
  for (i in seq_len(nrow(calibration$run_distribution))) {
    DBI::dbExecute(conn, "
      INSERT INTO elo_calibration_metrics (format, metric_type, metric_key, metric_value, sample_size, calculated_date)
      VALUES (?, 'run_distribution', ?, ?, ?, ?)
    ", params = list(
      format,
      paste0("runs_", calibration$run_distribution$runs_batter[i]),
      calibration$run_distribution$proportion[i],
      calibration$run_distribution$count[i],
      as.character(calc_date)
    ))
    rows <- rows + 1
  }

    rows
  })

  cli::cli_alert_success("Stored {rows} calibration metrics for {format}")
  invisible(rows)
}
