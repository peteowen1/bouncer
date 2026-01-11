# ELO Calibration Functions
#
# Functions to calculate calibrated expected values from actual data.
# Prevents ELO drift by anchoring expected values to actual outcome rates.


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
    FROM deliveries
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
    FROM deliveries
    WHERE LOWER(match_type) IN (?, ?, ?)
      AND is_wicket IS NOT NULL
      AND NOT is_wicket
    GROUP BY runs_batter
    ORDER BY runs_batter
  ", params = list(
    tolower(format),
    paste0("i", tolower(format)),
    toupper(format)
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

  # Delete existing metrics for this format

DBI::dbExecute(conn, "
    DELETE FROM elo_calibration_metrics WHERE format = ?
  ", params = list(format))

  rows <- 0

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

  cli::cli_alert_success("Stored {rows} calibration metrics for {format}")
  invisible(rows)
}


#' Get Calibration Data from Database
#'
#' Retrieves stored calibration metrics for a format.
#'
#' @param format Character. Match format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return List with wicket_rate, mean_runs, run_distribution or NULL if not found
#' @keywords internal
get_calibration_data <- function(format = "t20", conn) {

  metrics <- DBI::dbGetQuery(conn, "
    SELECT metric_type, metric_key, metric_value, sample_size
    FROM elo_calibration_metrics
    WHERE format = ?
  ", params = list(format))

  if (nrow(metrics) == 0) {
    cli::cli_alert_warning("No calibration data found for format: {format}")
    cli::cli_alert_info("Run 01_calibrate_expected_values.R first")
    return(NULL)
  }

  # Extract key metrics
  wicket_row <- metrics[metrics$metric_type == "wicket_rate" & metrics$metric_key == "overall", ]
  runs_row <- metrics[metrics$metric_type == "mean_runs" & metrics$metric_key == "overall", ]
  outcome_row <- metrics[metrics$metric_type == "mean_outcome_score" & metrics$metric_key == "overall", ]

  list(
    format = format,
    wicket_rate = if (nrow(wicket_row) > 0) wicket_row$metric_value else BASE_WICKET_PROB_T20,
    mean_runs = if (nrow(runs_row) > 0) runs_row$metric_value else 1.3,
    mean_outcome_score = if (nrow(outcome_row) > 0) outcome_row$metric_value else 0.25,
    sample_size = if (nrow(wicket_row) > 0) wicket_row$sample_size else 0
  )
}


#' Calculate Calibrated Expected Runs
#'
#' Calculates expected outcome score using ELO difference adjusted by actual data
#' calibration. Uses the mean_outcome_score from calibration data to ensure
#' zero-sum ELO updates (what batters gain, bowlers lose).
#'
#' @param batter_run_elo Numeric. Batter's run ELO rating
#' @param bowler_run_elo Numeric. Bowler's run ELO rating
#' @param calibration List. Calibration data from get_calibration_data()
#' @param divisor Numeric. ELO divisor (default 400)
#'
#' @return Numeric. Calibrated expected outcome score (0-1 scale for ELO update)
#' @keywords internal
calculate_expected_runs_calibrated <- function(batter_run_elo,
                                                bowler_run_elo,
                                                calibration,
                                                divisor = DUAL_ELO_DIVISOR) {

  # Standard ELO expected value formula
  elo_diff <- batter_run_elo - bowler_run_elo
  elo_factor <- 1 / (1 + 10^(-elo_diff / divisor))

 # Base expected outcome score from calibration (NOT mean_runs/6!)
  # This is the average actual outcome score given the scoring weights
  # Using this ensures zero-sum: E[batter_update + bowler_update] = 0
  base_expected <- if (!is.null(calibration) && !is.null(calibration$mean_outcome_score)) {
    calibration$mean_outcome_score
  } else {
    0.25  # Fallback (approximate)
  }

  # Adjust based on ELO advantage
  # At ELO parity (elo_factor = 0.5), expected = base_expected
  # At +400 ELO (elo_factor ≈ 0.91), expected ≈ base_expected * 1.25
  # At -400 ELO (elo_factor ≈ 0.09), expected ≈ base_expected * 0.75
  adjustment <- (elo_factor - 0.5) * 0.5  # Range: -0.25 to +0.25

  expected <- base_expected * (1 + adjustment)

  # Clamp to valid range
  max(0.05, min(0.95, expected))
}


#' Calculate Calibrated Wicket Probability
#'
#' Calculates expected wicket probability using ELO difference adjusted by actual
#' wicket rates from data.
#'
#' @param batter_wicket_elo Numeric. Batter's wicket survival ELO
#' @param bowler_wicket_elo Numeric. Bowler's wicket strike ELO
#' @param calibration List. Calibration data from get_calibration_data()
#' @param divisor Numeric. ELO divisor (default 400)
#'
#' @return Numeric. Calibrated expected wicket probability (0-1)
#' @keywords internal
calculate_expected_wicket_calibrated <- function(batter_wicket_elo,
                                                  bowler_wicket_elo,
                                                  calibration,
                                                  divisor = DUAL_ELO_DIVISOR) {

  # ELO advantage from bowler perspective (higher = more likely to get wicket)
  elo_diff <- bowler_wicket_elo - batter_wicket_elo
  elo_factor <- 1 / (1 + 10^(-elo_diff / divisor))

  # Base wicket probability from calibration (or default ~2.5% for T20)
  base_prob <- if (!is.null(calibration)) calibration$wicket_rate else 0.025

  # Adjust based on ELO advantage
  # Wicket probability can range from ~0.5x to ~2x base rate
  multiplier <- 0.5 + elo_factor  # Range: 0.5 to 1.5

  expected <- base_prob * multiplier

  # Clamp to reasonable range
  max(0.005, min(0.15, expected))
}


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
