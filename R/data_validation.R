# Data Validation Functions

#' Validate Match Data
#'
#' Checks that parsed match data has all required fields.
#'
#' @param match_data List returned from \code{parse_cricsheet_json}
#'
#' @return Logical TRUE if valid, stops with error if invalid
#' @keywords internal
validate_match_data <- function(match_data) {
  # Check match_info
  if (is.null(match_data$match_info) || nrow(match_data$match_info) == 0) {
    cli::cli_abort("Match info is missing or empty")
  }

  # Check required fields in match_info
  required_fields <- c("match_id", "match_type", "team1", "team2")
  missing_fields <- setdiff(required_fields, names(match_data$match_info))

  if (length(missing_fields) > 0) {
    cli::cli_abort("Missing required fields in match_info: {paste(missing_fields, collapse = ', ')}")
  }

  # Check deliveries
  if (is.null(match_data$deliveries) || nrow(match_data$deliveries) == 0) {
    cli::cli_warn("No deliveries found in match data")
  }

  TRUE
}


#' Validate Deliveries Data
#'
#' Checks that deliveries data is valid.
#'
#' @param deliveries Data frame with delivery data
#'
#' @return Logical TRUE if valid
#' @keywords internal
validate_deliveries <- function(deliveries) {
  if (nrow(deliveries) == 0) {
    cli::cli_warn("No deliveries to validate")
    return(TRUE)
  }

  # Check for required columns
  required_cols <- c("delivery_id", "match_id", "batter_id", "bowler_id",
                     "runs_total", "is_wicket")
  missing_cols <- setdiff(required_cols, names(deliveries))

  if (length(missing_cols) > 0) {
    cli::cli_abort("Missing required columns: {paste(missing_cols, collapse = ', ')}")
  }

  # Check for NA in critical fields
  if (any(is.na(deliveries$delivery_id))) {
    cli::cli_warn("Some delivery_ids are NA")
  }

  if (any(is.na(deliveries$batter_id))) {
    cli::cli_warn("Some batter_ids are NA")
  }

  TRUE
}


#' Check Data Quality
#'
#' Reports on data quality metrics.
#'
#' @param match_data List returned from \code{parse_cricsheet_json}
#'
#' @return Invisibly returns a list with quality metrics
#' @keywords internal
check_data_quality <- function(match_data) {
  metrics <- list()

  # Count deliveries
  metrics$n_deliveries <- nrow(match_data$deliveries)

  # Count wickets
  metrics$n_wickets <- sum(match_data$deliveries$is_wicket, na.rm = TRUE)

  # Count boundaries
  metrics$n_boundaries <- sum(match_data$deliveries$is_boundary, na.rm = TRUE)

  # Check for missing player IDs
  metrics$missing_batter <- sum(is.na(match_data$deliveries$batter_id))
  metrics$missing_bowler <- sum(is.na(match_data$deliveries$bowler_id))

  # Calculate completeness
  metrics$completeness <- 1 - (metrics$missing_batter + metrics$missing_bowler) / (2 * metrics$n_deliveries)

  cli::cli_h3("Data Quality Metrics")
  cli::cli_alert_info("Deliveries: {metrics$n_deliveries}")
  cli::cli_alert_info("Wickets: {metrics$n_wickets}")
  cli::cli_alert_info("Boundaries: {metrics$n_boundaries}")
  cli::cli_alert_info("Completeness: {round(metrics$completeness * 100, 1)}%")

  invisible(metrics)
}
