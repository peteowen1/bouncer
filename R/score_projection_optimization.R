# Score Projection Parameter Optimization and Batch Calculation
#
# Functions for optimizing projection parameters and calculating projections
# for all deliveries in the database. These are separate from core projection
# functions because they:
# 1. Require database connections (not pure functions)
# 2. Are run infrequently (initial calibration or periodic updates)
# 3. Have different dependencies (training vs. prediction)
#
# Core projection functions are in score_projection.R.

# ============================================================================
# PARAMETER OPTIMIZATION FUNCTIONS
# ============================================================================

#' Load Training Data for Projection Optimization
#'
#' Loads deliveries with final innings totals for a specific segment.
#'
#' @param conn DBI connection to database.
#' @param format Character. Format: "t20", "odi", "test".
#' @param gender Character. "male" or "female".
#' @param team_type Character. "international" or "club".
#' @param sample_frac Numeric. Fraction of data to sample (for speed).
#'
#' @return Data frame with columns: current_score, balls_remaining, wickets_remaining,
#'   final_innings_total, match_id, innings.
#' @keywords internal
load_projection_training_data <- function(conn, format, gender, team_type,
                                          sample_frac = 1.0) {

  format_lower <- normalize_format(format)
  gender_lower <- escape_sql_quotes(tolower(gender))

  # Match types lowercase for LOWER() comparison
  match_types <- tolower(get_match_types_for_format(format_lower))

  # Team type filter (handle NULL for club matches)
  if (team_type == "international") {
    team_type_filter <- "m.team_type = 'international'"
  } else {
    team_type_filter <- "(m.team_type IS NULL OR m.team_type != 'international')"
  }

  match_types_sql <- paste0("'", escape_sql_quotes(match_types), "'", collapse = ", ")

  # Get max balls for format using central helper
  max_balls <- get_max_balls(format)

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
  ", match_types_sql, gender_lower, max_balls, match_types_sql, gender_lower, team_type_filter)

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


#' Calculate RMSE for Projection Parameters
#'
#' Objective function for parameter optimization.
#'
#' @param params Numeric vector c(a, b, z, y).
#' @param data Data frame with columns: current_score, balls_remaining,
#'   wickets_remaining, final_innings_total.
#' @param eis Numeric. Expected initial score.
#' @param max_balls Integer. Maximum balls in format.
#'
#' @return Numeric. RMSE.
#' @keywords internal
calculate_projection_rmse <- function(params, data, eis, max_balls) {
  a <- params[1]
  b <- params[2]
  z <- params[3]
  y <- params[4]

  # Calculate projections
  projected <- calculate_projected_scores_vectorized(
    current_score = data$current_score,
    wickets_remaining = data$wickets_remaining,
    balls_remaining = data$balls_remaining,
    expected_initial_score = rep(eis, nrow(data)),
    a = a, b = b, z = z, y = y,
    max_balls = max_balls
  )

  # Calculate RMSE
  errors <- projected - data$final_innings_total
  rmse <- sqrt(mean(errors^2, na.rm = TRUE))

  return(rmse)
}


#' Grid Search for Projection Parameters
#'
#' Searches parameter space for good starting values.
#'
#' @param data Data frame with training data.
#' @param eis Numeric. Expected initial score.
#' @param max_balls Integer. Maximum balls in format.
#' @param grid Named list with parameter ranges (a, b, z, y).
#'
#' @return Named list with best parameters and RMSE.
#' @keywords internal
grid_search_projection_params <- function(data, eis, max_balls, grid = NULL) {

  # Default grid
  if (is.null(grid)) {
    grid <- list(
      a = seq(0.5, 1.0, by = 0.1),
      b = seq(0.0, 0.5, by = 0.1),
      z = seq(0.7, 1.1, by = 0.1),
      y = seq(0.8, 1.4, by = 0.1)
    )
  }

  # Generate all combinations
  combinations <- expand.grid(grid)

  best_rmse <- Inf
  best_params <- NULL

  cli::cli_progress_bar("Grid search", total = nrow(combinations))

  for (i in seq_len(nrow(combinations))) {
    params <- as.numeric(combinations[i, ])

    rmse <- calculate_projection_rmse(params, data, eis, max_balls)

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


#' Refine Projection Parameters with Nelder-Mead
#'
#' Fine-tunes parameters using optimization.
#'
#' @param data Data frame with training data.
#' @param eis Numeric. Expected initial score.
#' @param max_balls Integer. Maximum balls in format.
#' @param initial Named list with starting parameters.
#'
#' @return Named list with optimized parameters and RMSE.
#' @keywords internal
refine_projection_params <- function(data, eis, max_balls, initial) {

  # Objective function for optim
  objective <- function(params) {
    # Enforce constraints
    if (any(params <= 0)) return(1e10)
    if (params[1] + params[2] > 2) return(1e10)

    calculate_projection_rmse(params, data, eis, max_balls)
  }

  # Initial values
  start <- c(initial$a, initial$b, initial$z, initial$y)

  # Optimize
  result <- stats::optim(
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


#' Calculate Actual EIS from Database
#'
#' Computes the actual average first innings score for a segment.
#'
#' @param conn DBI connection.
#' @param format Character. Format: "t20", "odi", "test".
#' @param gender Character. "male" or "female".
#' @param team_type Character. "international" or "club".
#'
#' @return Numeric. Average first innings total.
#' @keywords internal
calculate_actual_eis <- function(conn, format, gender, team_type) {

  gender_lower <- tolower(gender)

  # Match types lowercase for LOWER() comparison
  match_types <- tolower(get_match_types_for_format(format))

  # Team type filter (handle NULL for club matches)
  if (team_type == "international") {
    team_type_filter <- "m.team_type = 'international'"
  } else {
    team_type_filter <- "(m.team_type IS NULL OR m.team_type != 'international')"
  }

  match_types_sql <- paste0("'", escape_sql_quotes(match_types), "'", collapse = ", ")

  query <- sprintf("
    SELECT AVG(mi.total_runs) as avg_score
    FROM match_innings mi
    JOIN matches m ON mi.match_id = m.match_id
    WHERE mi.innings = 1
      AND LOWER(m.match_type) IN (%s)
      AND LOWER(m.gender) = '%s'
      AND %s
      AND mi.total_runs IS NOT NULL
  ", match_types_sql, gender_lower, team_type_filter)

  result <- DBI::dbGetQuery(conn, query)

  if (nrow(result) == 0 || is.na(result$avg_score[1])) {
    # Fall back to default
    return(get_agnostic_expected_score(format, gender, team_type))
  }

  return(result$avg_score[1])
}


#' Optimize Projection Parameters for One Segment
#'
#' Runs full optimization (grid search + refinement) for a single segment.
#'
#' @param conn DBI connection.
#' @param format Character. Format: "t20", "odi", "test".
#' @param gender Character. "male" or "female".
#' @param team_type Character. "international" or "club".
#' @param sample_frac Numeric. Fraction of data to sample.
#' @param validation_split Numeric. Fraction for validation.
#' @param output_dir Character. Directory to save parameters.
#'
#' @return Named list with optimized parameters and metrics, or NULL if insufficient data.
#' @keywords internal
optimize_projection_segment <- function(conn, format, gender, team_type,
                                        sample_frac = 0.5,
                                        validation_split = 0.2,
                                        output_dir = "../bouncerdata/models") {

  segment_id <- get_projection_segment_id(format, gender, team_type)

  cli::cli_h2("Optimizing: {segment_id}")

  # Load data
  cli::cli_alert_info("Loading data...")
  data <- tryCatch(
    load_projection_training_data(conn, format, gender, team_type,
                                  sample_frac = sample_frac),
    error = function(e) {
      cli::cli_alert_warning("Error loading data: {e$message}")
      NULL
    }
  )

  if (is.null(data) || nrow(data) < 100) {
    cli::cli_alert_warning("Insufficient data for {segment_id}, skipping...")
    return(NULL)
  }

  cli::cli_alert_success("Loaded {nrow(data)} deliveries")

  # Split train/validation by match
  unique_matches <- unique(data$match_id)
  n_val <- ceiling(length(unique_matches) * validation_split)
  val_matches <- sample(unique_matches, n_val)

  train_data <- data[!data$match_id %in% val_matches, ]
  val_data <- data[data$match_id %in% val_matches, ]

  cli::cli_alert_info("Train: {nrow(train_data)} rows, Validation: {nrow(val_data)} rows")

  # Calculate actual EIS from data
  eis <- calculate_actual_eis(conn, format, gender, team_type)
  cli::cli_alert_info("Calculated EIS: {round(eis, 1)}")

  # Get max balls using central helper
  max_balls <- get_max_balls(format)

  # Grid search
  cli::cli_alert_info("Running grid search...")
  grid_result <- grid_search_projection_params(train_data, eis, max_balls)
  cli::cli_alert_success("Grid search RMSE: {round(grid_result$rmse, 2)}")

  # Refine with Nelder-Mead
  cli::cli_alert_info("Refining with Nelder-Mead...")
  refined <- refine_projection_params(train_data, eis, max_balls, grid_result)
  cli::cli_alert_success("Refined RMSE: {round(refined$rmse, 2)}")

  # Validate
  val_rmse <- calculate_projection_rmse(
    c(refined$a, refined$b, refined$z, refined$y),
    val_data, eis, max_balls
  )
  cli::cli_alert_info("Validation RMSE: {round(val_rmse, 2)}")

  # Build result
  params <- list(
    a = refined$a,
    b = refined$b,
    z = refined$z,
    y = refined$y,
    eis_agnostic = eis
  )

  # Save parameters
  save_projection_params(
    params,
    format, gender, team_type,
    params_dir = output_dir,
    metrics = list(
      train_rmse = refined$rmse,
      validation_rmse = val_rmse,
      n_innings = length(unique_matches)
    )
  )

  # Return full result
  params$segment_id <- segment_id
  params$format <- format
  params$gender <- gender
  params$team_type <- team_type
  params$train_rmse <- refined$rmse
  params$validation_rmse <- val_rmse
  params$n_innings <- length(unique_matches)

  cli::cli_alert_info("  a={round(refined$a, 3)}, b={round(refined$b, 3)}, z={round(refined$z, 3)}, y={round(refined$y, 3)}")

  return(params)
}


#' Optimize All Projection Segments
#'
#' Runs optimization for all format x gender x team_type combinations.
#'
#' @param db_path Character. Path to database.
#' @param output_dir Character. Directory to save parameters.
#' @param sample_frac Numeric. Fraction of data to sample.
#' @param formats Character vector. Formats to optimize (default: all).
#' @param genders Character vector. Genders to optimize (default: all).
#' @param team_types Character vector. Team types to optimize (default: all).
#'
#' @return Named list of results for each segment.
#' @keywords internal
optimize_all_projection_segments <- function(db_path = "../bouncerdata/bouncer.duckdb",
                                             output_dir = "../bouncerdata/models",
                                             sample_frac = 0.5,
                                             formats = c("t20", "odi", "test"),
                                             genders = c("male", "female"),
                                             team_types = c("international", "club")) {

  # Generate segments
  segments <- expand.grid(
    format = formats,
    gender = genders,
    team_type = team_types,
    stringsAsFactors = FALSE
  )

  # Connect to database
  cli::cli_h1("Score Projection Parameter Optimization")
  cli::cli_alert_info("Connecting to database: {.file {db_path}}")

  check_duckdb_available()
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Ensure output directory exists
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  results <- list()

  for (i in seq_len(nrow(segments))) {
    segment <- segments[i, ]
    segment_id <- get_projection_segment_id(segment$format, segment$gender, segment$team_type)

    result <- optimize_projection_segment(
      conn = conn,
      format = segment$format,
      gender = segment$gender,
      team_type = segment$team_type,
      sample_frac = sample_frac,
      output_dir = output_dir
    )

    if (!is.null(result)) {
      results[[segment_id]] <- result
    }
  }

  # Summary
  cli::cli_h1("Optimization Complete")

  if (length(results) > 0) {
    summary_df <- fast_rbind(lapply(results, function(r) {
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
  }

  invisible(results)
}


# ============================================================================
# PER-DELIVERY PROJECTION CALCULATION
# ============================================================================

#' Calculate Projections for All Deliveries
#'
#' Calculates and stores per-delivery score projections for a format.
#' Both agnostic (format average EIS) and full (team/venue adjusted) projections.
#'
#' @param conn DBI connection to database (write access required).
#' @param format Character. Format: "t20", "odi", or "test".
#' @param batch_size Integer. Number of deliveries to process per batch.
#' @param params_dir Character. Directory with parameter files.
#'
#' @return Invisible NULL.
#' @keywords internal
calculate_all_delivery_projections <- function(conn, format,
                                               batch_size = 50000,
                                               params_dir = "../bouncerdata/models") {

  format_lower <- normalize_format(format)

  # Match types for this format (lowercase for SQL LOWER() comparison)
  match_types <- tolower(get_match_types_for_format(format_lower))
  match_types_sql <- paste0("'", escape_sql_quotes(match_types), "'", collapse = ", ")

  # Get max balls using central helper
  max_balls <- get_max_balls(format)

  # Table name
  table_name <- paste0(format_lower, "_score_projection")

  cli::cli_h2("Calculating {format_lower} projections")

 # Create table if it doesn't exist
  create_table_sql <- sprintf("
    CREATE TABLE IF NOT EXISTS %s (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      innings INTEGER,
      batting_team_id VARCHAR,
      current_score INTEGER,
      balls_remaining INTEGER,
      wickets_remaining INTEGER,
      resource_remaining DOUBLE,
      resource_used DOUBLE,
      eis_agnostic DOUBLE,
      eis_full DOUBLE,
      projected_agnostic DOUBLE,
      projected_full DOUBLE,
      final_innings_total INTEGER,
      projection_change_agnostic DOUBLE,
      projection_change_full DOUBLE
    )
  ", table_name)

  DBI::dbExecute(conn, create_table_sql)

  # Count deliveries
  count_query <- sprintf("
    SELECT COUNT(*) as n
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    WHERE LOWER(d.match_type) IN (%s)
  ", match_types_sql)

  total_count <- DBI::dbGetQuery(conn, count_query)$n

  cli::cli_alert_info("Total deliveries to process: {total_count}")

  if (total_count == 0) {
    cli::cli_alert_warning("No deliveries found for {format_lower}")
    return(invisible(NULL))
  }

  # Clear existing projections
  cli::cli_alert_info("Clearing existing projections...")
  DBI::dbExecute(conn, sprintf("DELETE FROM %s", table_name))

  # Pre-load parameters for all segments
  cli::cli_alert_info("Loading optimized parameters...")
  params_cache <- list()
  for (gender in c("male", "female")) {
    for (team_type in c("international", "club")) {
      key <- paste(gender, team_type, sep = "_")
      params_cache[[key]] <- load_projection_params(
        format, gender, team_type, params_dir = params_dir
      )
      cli::cli_alert_success("  {key}: a={round(params_cache[[key]]$a, 3)}, b={round(params_cache[[key]]$b, 3)}, EIS={round(params_cache[[key]]$eis_agnostic, 1)}")
    }
  }

  # Process in batches
  n_batches <- ceiling(total_count / batch_size)
  rows_inserted <- 0
  start_time <- Sys.time()

  cli::cli_alert_info("Processing {n_batches} batches of {batch_size} deliveries each")

  for (batch_idx in seq_len(n_batches)) {
    offset <- (batch_idx - 1) * batch_size

    if (batch_idx <= 3 || batch_idx %% 10 == 0) {
      cli::cli_alert("Batch {batch_idx}/{n_batches}: querying rows {offset + 1} to {min(offset + batch_size, total_count)}...")
    }

    # Query batch of deliveries
    query <- sprintf("
      WITH innings_totals AS (
        SELECT
          match_id,
          innings,
          MAX(total_runs) as final_innings_total
        FROM deliveries
        WHERE LOWER(match_type) IN (%s)
        GROUP BY match_id, innings
      )
      SELECT
        d.delivery_id,
        d.match_id,
        m.match_date,
        d.innings,
        d.batting_team as batting_team_id,
        d.total_runs as current_score,
        %d - ((d.over * 6) + d.ball) as balls_remaining,
        (10 - d.wickets_fallen) as wickets_remaining,
        LOWER(m.gender) as gender,
        CASE WHEN m.team_type = 'international' THEN 'international' ELSE 'club' END as team_type,
        it.final_innings_total
      FROM deliveries d
      JOIN matches m ON d.match_id = m.match_id
      JOIN innings_totals it ON d.match_id = it.match_id AND d.innings = it.innings
      WHERE LOWER(d.match_type) IN (%s)
      ORDER BY m.match_date, d.match_id, d.delivery_id
      LIMIT %d OFFSET %d
    ", match_types_sql, max_balls, match_types_sql, batch_size, offset)

    batch_data <- DBI::dbGetQuery(conn, query)

    if (nrow(batch_data) == 0) {
      cli::cli_alert_warning("Batch {batch_idx} returned 0 rows, stopping")
      break
    }

    if (batch_idx <= 3) {
      cli::cli_alert_info("  Got {nrow(batch_data)} rows, calculating projections...")
    }

    # Calculate projections for each segment
    batch_data$projected_agnostic <- NA_real_
    batch_data$projected_full <- NA_real_
    batch_data$eis_agnostic <- NA_real_
    batch_data$resource_remaining <- NA_real_
    batch_data$resource_used <- NA_real_

    for (gender in c("male", "female")) {
      for (team_type in c("international", "club")) {
        key <- paste(gender, team_type, sep = "_")
        params <- params_cache[[key]]

        mask <- batch_data$gender == gender & batch_data$team_type == team_type

        if (sum(mask) > 0) {
          # Get EIS
          eis <- params$eis_agnostic %||%
            get_agnostic_expected_score(format, gender, team_type)

          # Calculate resource remaining
          rr <- calculate_projection_resource(
            wickets_remaining = batch_data$wickets_remaining[mask],
            balls_remaining = batch_data$balls_remaining[mask],
            format = format,
            z = params$z,
            y = params$y
          )

          ru <- pmax(1 - rr, PROJ_MIN_RESOURCE_USED)

          # Calculate agnostic projection
          proj_agnostic <- calculate_projected_scores_vectorized(
            current_score = batch_data$current_score[mask],
            wickets_remaining = batch_data$wickets_remaining[mask],
            balls_remaining = batch_data$balls_remaining[mask],
            expected_initial_score = rep(eis, sum(mask)),
            a = params$a, b = params$b, z = params$z, y = params$y,
            max_balls = max_balls
          )

          batch_data$projected_agnostic[mask] <- proj_agnostic
          batch_data$eis_agnostic[mask] <- eis
          batch_data$resource_remaining[mask] <- rr
          batch_data$resource_used[mask] <- ru

          # For full projection, would need team/venue skills
          # For now, just use agnostic
          batch_data$projected_full[mask] <- proj_agnostic
          batch_data$eis_full[mask] <- eis
        }
      }
    }

    # Calculate projection changes (requires ordering within innings)
    # Use vectorized dplyr operations instead of nested loops (O(n) vs O(n²))
    batch_data <- batch_data %>%
      dplyr::arrange(match_id, innings, delivery_id) %>%
      dplyr::group_by(match_id, innings) %>%
      dplyr::mutate(
        projection_change_agnostic = c(NA_real_, diff(projected_agnostic)),
        projection_change_full = c(NA_real_, diff(projected_full))
      ) %>%
      dplyr::ungroup()

    # Prepare for insertion
    insert_data <- data.frame(
      delivery_id = batch_data$delivery_id,
      match_id = batch_data$match_id,
      match_date = batch_data$match_date,
      innings = batch_data$innings,
      batting_team_id = batch_data$batting_team_id,
      current_score = batch_data$current_score,
      balls_remaining = batch_data$balls_remaining,
      wickets_remaining = batch_data$wickets_remaining,
      resource_remaining = round(batch_data$resource_remaining, 4),
      resource_used = round(batch_data$resource_used, 4),
      eis_agnostic = round(batch_data$eis_agnostic, 2),
      eis_full = round(batch_data$eis_full, 2),
      projected_agnostic = round(batch_data$projected_agnostic, 2),
      projected_full = round(batch_data$projected_full, 2),
      final_innings_total = batch_data$final_innings_total,
      projection_change_agnostic = round(batch_data$projection_change_agnostic, 3),
      projection_change_full = round(batch_data$projection_change_full, 3)
    )

    # Write to database
    DBI::dbWriteTable(conn, table_name, insert_data, append = TRUE, row.names = FALSE)
    rows_inserted <- rows_inserted + nrow(insert_data)

    if (batch_idx <= 3) {
      cli::cli_alert_success("  Batch {batch_idx} complete: inserted {nrow(insert_data)} rows")
    }

    # Show progress every 10 batches or on last batch
    if (batch_idx %% 10 == 0 || batch_idx == n_batches) {
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
      rate <- rows_inserted / elapsed
      remaining <- (total_count - rows_inserted) / rate
      cli::cli_alert_info("Progress: {rows_inserted}/{total_count} rows ({round(100 * rows_inserted/total_count, 1)}%) - {round(elapsed, 1)} min elapsed, ~{round(remaining, 1)} min remaining")
    }
  }

  # Get final count
  final_count <- DBI::dbGetQuery(
    conn,
    sprintf("SELECT COUNT(*) as n FROM %s", table_name)
  )$n

  elapsed_total <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
  cli::cli_alert_success("Stored {final_count} projections in {table_name} ({round(elapsed_total, 1)} minutes)")

  invisible(NULL)
}


#' Calculate Projections for All Formats
#'
#' Convenience function to calculate projections for T20, ODI, and Test formats.
#'
#' @param db_path Character. Path to database.
#' @param formats Character vector. Formats to process.
#' @param batch_size Integer. Deliveries per batch.
#' @param params_dir Character. Directory with parameter files.
#'
#' @return Invisible NULL.
#' @keywords internal
calculate_all_format_projections <- function(db_path = "../bouncerdata/bouncer.duckdb",
                                             formats = c("t20", "odi", "test"),
                                             batch_size = 50000,
                                             params_dir = "../bouncerdata/models") {

  cli::cli_h1("Score Projection Calculation")
  cli::cli_alert_info("Connecting to database: {.file {db_path}}")

  check_duckdb_available()
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  for (format in formats) {
    calculate_all_delivery_projections(
      conn = conn,
      format = format,
      batch_size = batch_size,
      params_dir = params_dir
    )
  }

  cli::cli_h1("Projection Calculation Complete")

  invisible(NULL)
}
