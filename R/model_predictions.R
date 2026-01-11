#' Add Model Predictions to Deliveries Table
#'
#' Adds prediction columns to the deliveries table for a trained outcome model.
#' Creates 7 probability columns (one per outcome category) with customizable prefix.
#'
#' @param model_path Character. Path to the saved model file (.rds for BAM, .json for XGBoost)
#' @param model_type Character. Either "bam" or "xgb"
#' @param format Character. Either "shortform" (T20/ODI) or "longform" (Test)
#' @param column_prefix Character. Prefix for prediction columns (default: "pred_bam" or "pred_xgb")
#' @param db_path Character. Path to DuckDB database (default: use package default)
#' @param match_limit Integer. Limit number of matches to process (default: NULL = all)
#' @param batch_size Integer. Number of deliveries to process per batch (default: 10000)
#'
#' @return Invisible NULL. Modifies database in place.
#'
#' @details
#' For each delivery, adds 7 columns with prediction probabilities:
#' - {prefix}_wicket: P(wicket)
#' - {prefix}_0: P(0 runs)
#' - {prefix}_1: P(1 run)
#' - {prefix}_2: P(2 runs)
#' - {prefix}_3: P(3 runs)
#' - {prefix}_4: P(4 runs)
#' - {prefix}_6: P(6 runs)
#'
#' @keywords internal
add_predictions_to_deliveries <- function(model_path,
                                          model_type = c("bam", "xgb"),
                                          format = c("shortform", "longform"),
                                          column_prefix = NULL,
                                          db_path = NULL,
                                          match_limit = NULL,
                                          batch_size = 10000) {

  # Validate inputs
  model_type <- match.arg(model_type)
  format <- match.arg(format)

  # Set default column prefix
  if (is.null(column_prefix)) {
    column_prefix <- paste0("pred_", model_type)
  }

  # Load model
  cli::cli_h2("Loading model")
  if (model_type == "bam") {
    if (!requireNamespace("mgcv", quietly = TRUE)) {
      stop("Package 'mgcv' is required for BAM models. Please install it.")
    }
    model <- readRDS(model_path)
    cli::cli_alert_success("Loaded BAM model from {.file {model_path}}")
  } else {
    if (!requireNamespace("xgboost", quietly = TRUE)) {
      stop("Package 'xgboost' is required for XGBoost models. Please install it.")
    }
    model <- xgboost::xgb.load(model_path)
    cli::cli_alert_success("Loaded XGBoost model from {.file {model_path}}")
  }

  # Get database connection
  cli::cli_h2("Connecting to database")
  if (is.null(db_path)) {
    db_path <- get_db_path()
  }
  conn <- get_db_connection(path = db_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  cli::cli_alert_success("Connected to database")

  # Add prediction columns to schema if they don't exist
  cli::cli_h2("Adding prediction columns to schema")
  outcome_labels <- c("wicket", "0", "1", "2", "3", "4", "6")
  for (label in outcome_labels) {
    col_name <- paste0(column_prefix, "_", label)

    # Try to add column (ignore if exists)
    tryCatch({
      DBI::dbExecute(conn, sprintf("ALTER TABLE deliveries ADD COLUMN %s DOUBLE", col_name))
      cli::cli_alert_success("Added column: {.field {col_name}}")
    }, error = function(e) {
      if (grepl("already exists", e$message, ignore.case = TRUE)) {
        cli::cli_alert_info("Column {.field {col_name}} already exists, will overwrite")
      } else {
        stop(e)
      }
    })
  }

  # Query deliveries with feature engineering
  cli::cli_h2("Querying deliveries for prediction")

  # Build format-specific query
  if (format == "shortform") {
    format_filter <- "LOWER(match_type) IN ('t20', 'odi')"
  } else {
    format_filter <- "LOWER(match_type) = 'test'"
  }

  query <- sprintf("
    WITH innings_totals AS (
      SELECT
        match_id,
        innings,
        batting_team,
        MAX(total_runs) AS innings_total
      FROM deliveries
      WHERE %s
      GROUP BY match_id, innings, batting_team
    ),
    cumulative_scores AS (
      SELECT
        d.*,
        d.total_runs AS batting_score,
        COALESCE(
          (SELECT SUM(it.innings_total)
           FROM innings_totals it
           WHERE it.match_id = d.match_id
             AND it.batting_team = d.bowling_team
             AND it.innings < d.innings),
          0
        ) AS bowling_score
      FROM deliveries d
      WHERE %s
    )
    SELECT
      delivery_id,
      match_id,
      match_type,
      innings,
      over,
      ball,
      over_ball,
      venue,
      gender,
      wickets_fallen,
      (batting_score - bowling_score) AS runs_difference
    FROM cumulative_scores
    %s
    ORDER BY match_id, innings, over, ball
  ", format_filter, format_filter,
     if (!is.null(match_limit)) sprintf("LIMIT %d", match_limit * 1000) else "")

  cli::cli_alert_info("Executing query...")
  delivery_data <- DBI::dbGetQuery(conn, query)
  cli::cli_alert_success("Loaded {.val {nrow(delivery_data)}} deliveries")

  # Feature engineering (same as training scripts)
  cli::cli_h2("Engineering features")

  if (format == "shortform") {
    # Short-form features
    delivery_data <- delivery_data %>%
      dplyr::mutate(
        # Overs left
        overs_left = dplyr::case_when(
          tolower(match_type) == "t20" ~ pmax(0, 20 - over_ball),
          tolower(match_type) == "odi" ~ pmax(0, 50 - over_ball),
          TRUE ~ NA_real_
        ),

        # Phase
        phase = dplyr::case_when(
          tolower(match_type) == "t20" & over < 6 ~ "powerplay",
          tolower(match_type) == "t20" & over < 16 ~ "middle",
          tolower(match_type) == "t20" ~ "death",
          tolower(match_type) == "odi" & over < 10 ~ "powerplay",
          tolower(match_type) == "odi" & over < 40 ~ "middle",
          tolower(match_type) == "odi" ~ "death",
          TRUE ~ "middle"
        ),
        phase = factor(phase, levels = c("powerplay", "middle", "death")),

        # Categoricals
        match_type = factor(tolower(match_type)),
        innings = factor(innings),
        venue = factor(venue),
        gender = factor(gender)
      )
  } else {
    # Long-form features
    delivery_data <- delivery_data %>%
      dplyr::mutate(
        # Phase (ball age)
        phase = dplyr::case_when(
          over < 20 ~ "new_ball",
          over < 80 ~ "middle",
          TRUE ~ "old_ball"
        ),
        phase = factor(phase, levels = c("new_ball", "middle", "old_ball")),

        # Categoricals
        match_type = factor(tolower(match_type)),
        innings = factor(innings),
        venue = factor(venue),
        gender = factor(gender)
      )
  }

  cli::cli_alert_success("Features engineered")

  # Generate predictions in batches
  cli::cli_h2("Generating predictions")

  n_deliveries <- nrow(delivery_data)
  n_batches <- ceiling(n_deliveries / batch_size)

  cli::cli_progress_bar("Processing batches", total = n_batches)

  for (batch_num in 1:n_batches) {
    # Get batch indices
    start_idx <- (batch_num - 1) * batch_size + 1
    end_idx <- min(batch_num * batch_size, n_deliveries)
    batch_data <- delivery_data[start_idx:end_idx, ]

    # Generate predictions based on model type
    if (model_type == "bam") {
      # BAM predictions
      pred_probs <- predict(model, newdata = batch_data, type = "response")

    } else {
      # XGBoost predictions - need to prepare features
      if (format == "shortform") {
        batch_features <- batch_data %>%
          dplyr::mutate(
            match_type_t20 = as.integer(match_type == "t20"),
            match_type_odi = as.integer(match_type == "odi"),
            phase_powerplay = as.integer(phase == "powerplay"),
            phase_middle = as.integer(phase == "middle"),
            phase_death = as.integer(phase == "death"),
            gender_male = as.integer(gender == "male"),
            innings_num = as.integer(as.character(innings))
          ) %>%
          dplyr::select(
            match_type_t20, match_type_odi,
            innings_num, over, ball,
            wickets_fallen, runs_difference, overs_left,
            phase_powerplay, phase_middle, phase_death,
            gender_male
          )
      } else {
        batch_features <- batch_data %>%
          dplyr::mutate(
            phase_new_ball = as.integer(phase == "new_ball"),
            phase_middle = as.integer(phase == "middle"),
            phase_old_ball = as.integer(phase == "old_ball"),
            gender_male = as.integer(gender == "male"),
            innings_num = as.integer(as.character(innings))
          ) %>%
          dplyr::select(
            innings_num, over, ball,
            wickets_fallen, runs_difference,
            phase_new_ball, phase_middle, phase_old_ball,
            gender_male
          )
      }

      # Create DMatrix and predict
      dmat <- xgboost::xgb.DMatrix(data = as.matrix(batch_features))
      pred_probs <- predict(model, dmat)
    }

    # Update database with predictions for this batch
    for (i in 1:nrow(batch_data)) {
      delivery_id <- batch_data$delivery_id[i]

      # Build UPDATE statement
      updates <- paste0(
        column_prefix, "_", outcome_labels, " = ", pred_probs[i, ],
        collapse = ", "
      )

      update_sql <- sprintf(
        "UPDATE deliveries SET %s WHERE delivery_id = '%s'",
        updates, delivery_id
      )

      DBI::dbExecute(conn, update_sql)
    }

    cli::cli_progress_update()
  }

  cli::cli_progress_done()
  cli::cli_alert_success("Added predictions to {.val {n_deliveries}} deliveries")

  # Summary
  cli::cli_h2("Summary")
  cli::cli_alert_info("Model type: {.field {model_type}}")
  cli::cli_alert_info("Format: {.field {format}}")
  cli::cli_alert_info("Column prefix: {.field {column_prefix}}")
  cli::cli_alert_info("Deliveries updated: {.val {n_deliveries}}")

  cat("\nPrediction columns added:\n")
  for (label in outcome_labels) {
    cat(sprintf("  - %s_%s\n", column_prefix, label))
  }

  invisible(NULL)
}
