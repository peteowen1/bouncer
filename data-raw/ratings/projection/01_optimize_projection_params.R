# Optimize Score Projection Parameters
#
# This script optimizes the projection parameters (a, b, z, y) for each
# segment (format x gender x team_type) by minimizing RMSE against actual
# final innings totals.
#
# Formula:
#   projected = cs + a * eis * resource_remaining + b * cs * resource_remaining / resource_used
#
# Where:
#   resource_remaining = (balls_remaining / max_balls)^z * (wickets_remaining / 10)^y
#   resource_used = 1 - resource_remaining
#
# Usage:
#   source("data-raw/ratings/projection/01_optimize_projection_params.R")

library(dplyr)
library(tidyr)
library(cli)
library(DBI)

devtools::load_all()

# ============================================================================
# CONFIGURATION
# ============================================================================

DB_PATH <- "../bouncerdata/bouncer.duckdb"
OUTPUT_DIR <- "../bouncerdata/models"
VALIDATION_SPLIT <- 0.2  # 20% of innings for validation

# Grid search ranges for initial parameter exploration
PARAM_GRID <- list(
  a = seq(0.5, 1.0, by = 0.1),
  b = seq(0.0, 0.5, by = 0.1),
  z = seq(0.7, 1.1, by = 0.1),
  y = seq(0.8, 1.4, by = 0.1)
)

# Segments to optimize (format x gender x team_type)
SEGMENTS <- expand.grid(
  format = c("t20", "odi", "test"),
  gender = c("male", "female"),
  team_type = c("international", "club"),
  stringsAsFactors = FALSE
)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

#' Load Training Data for a Segment
#'
#' Loads all deliveries with final innings totals for a specific segment.
#'
#' @param conn DBI connection
#' @param format Character. Format: "t20", "odi", "test"
#' @param gender Character. "male" or "female"
#' @param team_type Character. "international" or "club"
#' @param sample_frac Numeric. Fraction of data to sample (for speed)
#'
#' @return Data frame with columns: current_score, balls_remaining, wickets_remaining,
#'         final_innings_total, match_id, innings
load_segment_data <- function(conn, format, gender, team_type, sample_frac = 1.0) {

  # Build match type filter
  match_types <- switch(format,
    "t20" = c("T20", "IT20"),
    "odi" = c("ODI", "ODM"),
    "test" = c("Test", "MDM")
  )

  # Build team type filter
  if (team_type == "international") {
    team_type_filter <- "team_type = 'international'"
  } else {
    team_type_filter <- "team_type != 'international'"
  }

  match_types_sql <- paste0("'", match_types, "'", collapse = ", ")

  # Get max balls for format
  max_balls <- switch(format,
    "t20" = 120,
    "odi" = 300,
    "test" = 540
  )

  # Query to get deliveries with final innings totals
  query <- sprintf("
    WITH innings_totals AS (
      SELECT
        match_id,
        innings,
        MAX(total_runs) as final_innings_total
      FROM deliveries
      WHERE LOWER(match_type) IN (%s)
        AND LOWER(gender) = '%s'
      GROUP BY match_id, innings
    )
    SELECT
      d.match_id,
      d.innings,
      d.total_runs as current_score,
      -- Calculate balls bowled so far
      ((d.over * 6) + d.ball) as balls_bowled,
      %d - ((d.over * 6) + d.ball) as balls_remaining,
      (10 - d.wickets_fallen) as wickets_remaining,
      it.final_innings_total
    FROM deliveries d
    JOIN innings_totals it ON d.match_id = it.match_id AND d.innings = it.innings
    JOIN matches m ON d.match_id = m.match_id
    WHERE LOWER(d.match_type) IN (%s)
      AND LOWER(d.gender) = '%s'
      AND %s
      AND it.final_innings_total IS NOT NULL
    ORDER BY RANDOM()
  ", match_types_sql, gender, max_balls, match_types_sql, gender, team_type_filter)

  data <- DBI::dbGetQuery(conn, query)

  # Sample if requested
  if (sample_frac < 1.0 && nrow(data) > 1000) {
    n_sample <- ceiling(nrow(data) * sample_frac)
    data <- data[sample(nrow(data), n_sample), ]
  }

  # Ensure valid values
  data$balls_remaining <- pmax(0, data$balls_remaining)
  data$wickets_remaining <- pmax(0, pmin(10, data$wickets_remaining))

  return(data)
}


#' Calculate RMSE for Given Parameters
#'
#' @param params Numeric vector c(a, b, z, y)
#' @param data Data frame with columns: current_score, balls_remaining, wickets_remaining, final_innings_total
#' @param eis Numeric. Expected initial score
#' @param max_balls Integer. Maximum balls in format
#'
#' @return Numeric. RMSE
calculate_rmse <- function(params, data, eis, max_balls) {
  a <- params[1]
  b <- params[2]
  z <- params[3]
  y <- params[4]

  # Calculate projections
  projected <- calculate_projected_scores_vectorized(
    current_score = data$current_score,
    balls_remaining = data$balls_remaining,
    wickets_remaining = data$wickets_remaining,
    expected_initial_score = rep(eis, nrow(data)),
    a = a, b = b, z = z, y = y,
    max_balls = max_balls
  )

  # Calculate RMSE
  errors <- projected - data$final_innings_total
  rmse <- sqrt(mean(errors^2, na.rm = TRUE))

  return(rmse)
}


#' Grid Search for Initial Parameters
#'
#' @param data Data frame with training data
#' @param eis Numeric. Expected initial score
#' @param max_balls Integer. Maximum balls in format
#' @param grid List with parameter ranges
#'
#' @return Named list with best parameters and RMSE
grid_search <- function(data, eis, max_balls, grid = PARAM_GRID) {

  # Generate all combinations
  combinations <- expand.grid(grid)

  best_rmse <- Inf
  best_params <- NULL

  cli::cli_progress_bar("Grid search", total = nrow(combinations))

  for (i in seq_len(nrow(combinations))) {
    params <- as.numeric(combinations[i, ])

    rmse <- calculate_rmse(params, data, eis, max_balls)

    if (rmse < best_rmse) {
      best_rmse <- rmse
      best_params <- params
    }

    cli::cli_progress_update()
  }

  cli::cli_progress_done()

  list(
    a = best_params[1],
    b = best_params[2],
    z = best_params[3],
    y = best_params[4],
    rmse = best_rmse
  )
}


#' Refine Parameters with Nelder-Mead
#'
#' @param data Data frame with training data
#' @param eis Numeric. Expected initial score
#' @param max_balls Integer. Maximum balls in format
#' @param initial Named list with starting parameters
#'
#' @return Named list with optimized parameters and RMSE
refine_params <- function(data, eis, max_balls, initial) {

  # Objective function for optim
  objective <- function(params) {
    # Enforce constraints
    if (any(params <= 0)) return(1e10)
    if (params[1] + params[2] > 2) return(1e10)

    calculate_rmse(params, data, eis, max_balls)
  }

  # Initial values
  start <- c(initial$a, initial$b, initial$z, initial$y)

  # Optimize
  result <- optim(
    par = start,
    fn = objective,
    method = "Nelder-Mead",
    control = list(maxit = 500, trace = 0)
  )

  list(
    a = result$par[1],
    b = result$par[2],
    z = result$par[3],
    y = result$par[4],
    rmse = result$value,
    convergence = result$convergence
  )
}


#' Calculate Actual EIS from Data
#'
#' Computes the actual average first innings score for a segment.
#'
#' @param conn DBI connection
#' @param format Character
#' @param gender Character
#' @param team_type Character
#'
#' @return Numeric. Average first innings total
calculate_actual_eis <- function(conn, format, gender, team_type) {

  match_types <- switch(format,
    "t20" = c("T20", "IT20"),
    "odi" = c("ODI", "ODM"),
    "test" = c("Test", "MDM")
  )

  if (team_type == "international") {
    team_type_filter <- "m.team_type = 'international'"
  } else {
    team_type_filter <- "m.team_type != 'international'"
  }

  match_types_sql <- paste0("'", match_types, "'", collapse = ", ")

  query <- sprintf("
    SELECT AVG(mi.total_runs) as avg_score
    FROM match_innings mi
    JOIN matches m ON mi.match_id = m.match_id
    WHERE mi.innings = 1
      AND LOWER(m.match_type) IN (%s)
      AND LOWER(m.gender) = '%s'
      AND %s
      AND mi.total_runs IS NOT NULL
  ", match_types_sql, gender, team_type_filter)

  result <- DBI::dbGetQuery(conn, query)

  if (nrow(result) == 0 || is.na(result$avg_score[1])) {
    # Fall back to default
    return(get_agnostic_expected_score(format, gender, team_type))
  }

  return(result$avg_score[1])
}


# ============================================================================
# MAIN OPTIMIZATION LOOP
# ============================================================================

optimize_all_segments <- function(segments = SEGMENTS, db_path = DB_PATH,
                                  output_dir = OUTPUT_DIR, sample_frac = 0.5) {

  # Connect to database
  cli::cli_h1("Score Projection Parameter Optimization")
  cli::cli_alert_info("Connecting to database: {.file {db_path}}")

  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Ensure output directory exists
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  results <- list()

  for (i in seq_len(nrow(segments))) {
    segment <- segments[i, ]
    segment_id <- paste(segment$format, segment$gender, segment$team_type, sep = "_")

    cli::cli_h2("Optimizing: {segment_id}")

    # Load data
    cli::cli_alert_info("Loading data...")
    data <- tryCatch(
      load_segment_data(conn, segment$format, segment$gender, segment$team_type,
                        sample_frac = sample_frac),
      error = function(e) {
        cli::cli_alert_warning("Error loading data: {e$message}")
        NULL
      }
    )

    if (is.null(data) || nrow(data) < 100) {
      cli::cli_alert_warning("Insufficient data for {segment_id}, skipping...")
      next
    }

    cli::cli_alert_success("Loaded {nrow(data)} deliveries")

    # Split train/validation by match
    unique_matches <- unique(data$match_id)
    n_val <- ceiling(length(unique_matches) * VALIDATION_SPLIT)
    val_matches <- sample(unique_matches, n_val)

    train_data <- data[!data$match_id %in% val_matches, ]
    val_data <- data[data$match_id %in% val_matches, ]

    cli::cli_alert_info("Train: {nrow(train_data)} rows, Validation: {nrow(val_data)} rows")

    # Calculate actual EIS from data
    eis <- calculate_actual_eis(conn, segment$format, segment$gender, segment$team_type)
    cli::cli_alert_info("Calculated EIS: {round(eis, 1)}")

    # Get max balls
    max_balls <- switch(segment$format,
      "t20" = 120,
      "odi" = 300,
      "test" = 540
    )

    # Grid search
    cli::cli_alert_info("Running grid search...")
    grid_result <- grid_search(train_data, eis, max_balls)
    cli::cli_alert_success("Grid search RMSE: {round(grid_result$rmse, 2)}")

    # Refine with Nelder-Mead
    cli::cli_alert_info("Refining with Nelder-Mead...")
    refined <- refine_params(train_data, eis, max_balls, grid_result)
    cli::cli_alert_success("Refined RMSE: {round(refined$rmse, 2)}")

    # Validate
    val_rmse <- calculate_rmse(
      c(refined$a, refined$b, refined$z, refined$y),
      val_data, eis, max_balls
    )
    cli::cli_alert_info("Validation RMSE: {round(val_rmse, 2)}")

    # Save parameters
    params <- list(
      a = refined$a,
      b = refined$b,
      z = refined$z,
      y = refined$y,
      eis_agnostic = eis,
      segment_id = segment_id,
      format = segment$format,
      gender = segment$gender,
      team_type = segment$team_type,
      train_rmse = refined$rmse,
      validation_rmse = val_rmse,
      n_innings = length(unique_matches),
      optimized_at = Sys.time()
    )

    save_projection_params(
      params,
      segment$format,
      segment$gender,
      segment$team_type,
      params_dir = output_dir,
      metrics = list(
        train_rmse = refined$rmse,
        validation_rmse = val_rmse,
        n_innings = length(unique_matches)
      )
    )

    results[[segment_id]] <- params

    cli::cli_alert_success("Saved parameters for {segment_id}")
    cli::cli_alert_info("  a={round(refined$a, 3)}, b={round(refined$b, 3)}, z={round(refined$z, 3)}, y={round(refined$y, 3)}")
  }

  # Summary
  cli::cli_h1("Optimization Complete")

  summary_df <- do.call(rbind, lapply(results, function(r) {
    data.frame(
      segment = r$segment_id,
      a = round(r$a, 3),
      b = round(r$b, 3),
      z = round(r$z, 3),
      y = round(r$y, 3),
      eis = round(r$eis_agnostic, 1),
      train_rmse = round(r$train_rmse, 2),
      val_rmse = round(r$validation_rmse, 2)
    )
  }))

  print(summary_df)

  invisible(results)
}


# ============================================================================
# RUN OPTIMIZATION
# ============================================================================

if (interactive()) {
  cli::cli_alert_info("Run optimize_all_segments() to start optimization")
  cli::cli_alert_info("Example: results <- optimize_all_segments(sample_frac = 0.3)")
} else {
  # Run with sampling for speed
  results <- optimize_all_segments(sample_frac = 0.5)
}
