# Match Prediction Functions
#
# This module provides functions for generating and storing pre-match predictions.


#' Predict Match Outcome
#'
#' Generates a prediction for a match using a trained model and pre-match features.
#'
#' @param match_id Character. Match identifier
#' @param model Model object. Trained prediction model (XGBoost or similar)
#' @param conn DBI connection. Database connection
#' @param model_version Character. Version identifier for the model
#' @param model_type Character. Type of model ("xgboost", "logistic", "ensemble")
#'
#' @return List with prediction details
#' @export
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection()
#' model <- xgboost::xgb.load("path/to/model.ubj")
#' prediction <- predict_match_outcome("1234567", model, conn)
#' }
predict_match_outcome <- function(match_id, model, conn,
                                   model_version = "v1.0",
                                   model_type = "xgboost") {

  # Get or calculate features
  features <- get_pre_match_features(match_id = match_id, conn = conn)

  if (nrow(features) == 0) {
    # Calculate features if not in database
    features <- calculate_pre_match_features(match_id, conn)
    if (is.null(features)) {
      cli::cli_alert_warning("Could not calculate features for match {match_id}")
      return(NULL)
    }
  }

  # Prepare feature matrix for model
  feature_cols <- c(
    "team1_elo_result", "team1_elo_roster", "team1_form_last5",
    "team2_elo_result", "team2_elo_roster", "team2_form_last5",
    "venue_avg_score", "venue_chase_success_rate", "venue_matches",
    "is_knockout"
  )

  # Handle derived features
  feature_matrix <- data.frame(
    team1_elo_result = features$team1_elo_result[1],
    team1_elo_roster = features$team1_elo_roster[1],
    team1_form_last5 = features$team1_form_last5[1],
    team2_elo_result = features$team2_elo_result[1],
    team2_elo_roster = features$team2_elo_roster[1],
    team2_form_last5 = features$team2_form_last5[1],
    venue_avg_score = features$venue_avg_score[1],
    venue_chase_success_rate = features$venue_chase_success_rate[1],
    venue_matches = features$venue_matches[1],
    is_knockout = as.integer(features$is_knockout[1]),

    # Derived features
    elo_diff_result = features$team1_elo_result[1] - features$team2_elo_result[1],
    elo_diff_roster = features$team1_elo_roster[1] - features$team2_elo_roster[1],
    form_diff = (features$team1_form_last5[1] %||% 0.5) - (features$team2_form_last5[1] %||% 0.5),
    h2h_advantage = ifelse(
      features$team1_h2h_total[1] > 0,
      features$team1_h2h_wins[1] / features$team1_h2h_total[1] - 0.5,
      0
    )
  )

  # Handle NA values - replace with defaults
  feature_matrix$team1_form_last5[is.na(feature_matrix$team1_form_last5)] <- 0.5
  feature_matrix$team2_form_last5[is.na(feature_matrix$team2_form_last5)] <- 0.5
  feature_matrix$venue_avg_score[is.na(feature_matrix$venue_avg_score)] <- 160  # T20 default
  feature_matrix$venue_chase_success_rate[is.na(feature_matrix$venue_chase_success_rate)] <- 0.5
  feature_matrix$venue_matches[is.na(feature_matrix$venue_matches)] <- 0

  # Make prediction based on model type
  if (model_type == "xgboost") {
    if (requireNamespace("xgboost", quietly = TRUE)) {
      dmatrix <- xgboost::xgb.DMatrix(data = as.matrix(feature_matrix))
      team1_win_prob <- predict(model, dmatrix)[1]
    } else {
      cli::cli_alert_warning("xgboost package not available, using ELO-based prediction")
      team1_win_prob <- predict_from_elo(features)
    }
  } else if (model_type == "logistic") {
    team1_win_prob <- predict(model, feature_matrix, type = "response")[1]
  } else {
    # Default to ELO-based prediction
    team1_win_prob <- predict_from_elo(features)
  }

  # Ensure probability is in valid range
  team1_win_prob <- max(MIN_WIN_PROBABILITY, min(MAX_WIN_PROBABILITY, team1_win_prob))
  team2_win_prob <- 1 - team1_win_prob

  # Determine predicted winner and confidence
  if (team1_win_prob > 0.5) {
    predicted_winner <- features$team1[1]
    confidence <- team1_win_prob
  } else {
    predicted_winner <- features$team2[1]
    confidence <- team2_win_prob
  }

  prediction <- list(
    prediction_id = paste0(match_id, "_", model_version),
    match_id = match_id,
    model_version = model_version,
    model_type = model_type,
    prediction_date = Sys.time(),
    team1 = features$team1[1],
    team2 = features$team2[1],
    team1_win_prob = team1_win_prob,
    team2_win_prob = team2_win_prob,
    predicted_winner = predicted_winner,
    confidence = confidence,
    actual_winner = NA_character_,
    prediction_correct = NA
  )

  return(prediction)
}


#' Predict from ELO
#'
#' Simple prediction based on ELO difference when model is not available.
#'
#' @param features Data frame. Pre-match features
#'
#' @return Numeric. Team 1 win probability
#' @keywords internal
predict_from_elo <- function(features) {
  # Use combined ELO (average of result and roster)
  team1_elo <- (features$team1_elo_result[1] + features$team1_elo_roster[1]) / 2
  team2_elo <- (features$team2_elo_result[1] + features$team2_elo_roster[1]) / 2

  # Standard ELO expected outcome
  prob <- calculate_expected_outcome(team1_elo, team2_elo)

  return(prob)
}


#' Store Prediction
#'
#' Stores a prediction in the database.
#'
#' @param prediction List. Prediction from predict_match_outcome
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
store_prediction <- function(prediction, conn) {

  DBI::dbExecute(conn, "
    INSERT INTO pre_match_predictions (
      prediction_id, match_id, model_version, model_type, prediction_date,
      team1_win_prob, team2_win_prob, predicted_winner, confidence,
      actual_winner, prediction_correct
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (prediction_id) DO UPDATE SET
      team1_win_prob = EXCLUDED.team1_win_prob,
      team2_win_prob = EXCLUDED.team2_win_prob,
      predicted_winner = EXCLUDED.predicted_winner,
      confidence = EXCLUDED.confidence,
      prediction_date = EXCLUDED.prediction_date
  ", params = list(
    prediction$prediction_id,
    prediction$match_id,
    prediction$model_version,
    prediction$model_type,
    prediction$prediction_date,
    prediction$team1_win_prob,
    prediction$team2_win_prob,
    prediction$predicted_winner,
    prediction$confidence,
    prediction$actual_winner,
    prediction$prediction_correct
  ))

  invisible(TRUE)
}


#' Update Prediction Outcome
#'
#' Updates a prediction with the actual match outcome.
#'
#' @param match_id Character. Match identifier
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns number of predictions updated
#' @keywords internal
update_prediction_outcome <- function(match_id, conn) {

  # Get actual outcome
  outcome <- DBI::dbGetQuery(conn, "
    SELECT outcome_winner FROM cricsheet.matches WHERE match_id = ?
  ", params = list(match_id))

  if (nrow(outcome) == 0 || is.na(outcome$outcome_winner[1]) ||
      outcome$outcome_winner[1] == "") {
    return(invisible(0L))
  }

  actual_winner <- outcome$outcome_winner[1]

  # Update predictions
  n_updated <- DBI::dbExecute(conn, "
    UPDATE pre_match_predictions
    SET actual_winner = ?,
        prediction_correct = (predicted_winner = ?)
    WHERE match_id = ?
  ", params = list(actual_winner, actual_winner, match_id))

  invisible(n_updated)
}


#' Batch Update Prediction Outcomes
#'
#' Updates outcomes for all predictions where the match has been completed.
#'
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns number of predictions updated
#' @keywords internal
batch_update_prediction_outcomes <- function(conn) {

  # Find predictions without outcomes but where match is complete
  n_updated <- DBI::dbExecute(conn, "
    UPDATE pre_match_predictions p
    SET actual_winner = (
      SELECT outcome_winner FROM cricsheet.matches m WHERE m.match_id = p.match_id
    ),
    prediction_correct = (
      p.predicted_winner = (
        SELECT outcome_winner FROM cricsheet.matches m WHERE m.match_id = p.match_id
      )
    )
    WHERE p.actual_winner IS NULL
      AND EXISTS (
        SELECT 1 FROM cricsheet.matches m
        WHERE m.match_id = p.match_id
          AND m.outcome_winner IS NOT NULL
          AND m.outcome_winner != ''
      )
  ")

  if (n_updated > 0) {
    cli::cli_alert_success("Updated {n_updated} prediction outcomes")
  }

  invisible(n_updated)
}


#' Get Predictions
#'
#' Retrieves predictions from the database.
#'
#' @param match_id Character. Match identifier (optional)
#' @param model_version Character. Filter by model version (optional)
#' @param include_outcomes Logical. Only include predictions with outcomes (default FALSE)
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with predictions
#' @keywords internal
get_predictions <- function(match_id = NULL, model_version = NULL,
                             include_outcomes = FALSE, conn) {

  query <- "SELECT * FROM pre_match_predictions WHERE 1=1"
  params <- list()

  if (!is.null(match_id)) {
    query <- paste0(query, " AND match_id = ?")
    params <- c(params, match_id)
  }

  if (!is.null(model_version)) {
    query <- paste0(query, " AND model_version = ?")
    params <- c(params, model_version)
  }

  if (include_outcomes) {
    query <- paste0(query, " AND actual_winner IS NOT NULL")
  }

  query <- paste0(query, " ORDER BY prediction_date")

  if (length(params) > 0) {
    DBI::dbGetQuery(conn, query, params = params)
  } else {
    DBI::dbGetQuery(conn, query)
  }
}


#' Calculate Prediction Accuracy
#'
#' Calculates accuracy metrics for predictions.
#'
#' @param predictions Data frame. Predictions with outcomes
#'
#' @return List with accuracy metrics
#' @keywords internal
calculate_prediction_accuracy <- function(predictions) {

  if (nrow(predictions) == 0 || all(is.na(predictions$prediction_correct))) {
    return(list(
      n_predictions = 0L,
      accuracy = NA_real_,
      log_loss = NA_real_,
      brier_score = NA_real_
    ))
  }

  # Filter to predictions with outcomes
  with_outcomes <- predictions[!is.na(predictions$prediction_correct), ]
  n <- nrow(with_outcomes)

  if (n == 0) {
    return(list(
      n_predictions = 0L,
      accuracy = NA_real_,
      log_loss = NA_real_,
      brier_score = NA_real_
    ))
  }

  # Accuracy
  accuracy <- mean(with_outcomes$prediction_correct)

  # Log loss
  # Clip probabilities to avoid log(0)
  probs <- pmax(MIN_WIN_PROBABILITY, pmin(MAX_WIN_PROBABILITY, with_outcomes$confidence))
  actuals <- as.integer(with_outcomes$prediction_correct)

  log_loss <- -mean(actuals * log(probs) + (1 - actuals) * log(1 - probs))

  # Brier score
  brier_score <- mean((probs - actuals)^2)

  return(list(
    n_predictions = as.integer(n),
    accuracy = accuracy,
    log_loss = log_loss,
    brier_score = brier_score
  ))
}


#' Generate Prediction Report
#'
#' Creates a formatted prediction report for a match.
#'
#' @param prediction List. Prediction from predict_match_outcome
#'
#' @return Character string with formatted report
#' @keywords internal
generate_prediction_report <- function(prediction) {

  report <- sprintf(
    "Match Prediction Report\n%s\n\n%s vs %s\n\nPredicted Winner: %s (%.1f%% confidence)\n\n%s: %.1f%%\n%s: %.1f%%\n\nModel: %s (%s)\nGenerated: %s",
    paste(rep("=", 40), collapse = ""),
    prediction$team1,
    prediction$team2,
    prediction$predicted_winner,
    prediction$confidence * 100,
    prediction$team1,
    prediction$team1_win_prob * 100,
    prediction$team2,
    prediction$team2_win_prob * 100,
    prediction$model_type,
    prediction$model_version,
    format(prediction$prediction_date, "%Y-%m-%d %H:%M")
  )

  return(report)
}
