# Generate Match Predictions ----
#
# This script generates predictions for upcoming or historical matches
# and stores them in the pre_match_predictions table.
# Supports all formats (T20, ODI, Test) - set FORMAT below.
#
# Output:
#   - pre_match_predictions table updated in DuckDB
#   - Console output with predictions

# 1. Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

if (!requireNamespace("xgboost", quietly = TRUE)) {
  stop("xgboost package required. Install with: install.packages('xgboost')")
}
library(xgboost)

# 2. Configuration ----
FORMAT <- "t20"  # Options: "t20", "odi", "test"

# Format match type mappings
FORMAT_GROUPS <- list(
  t20 = c("t20", "it20"),
  odi = c("odi", "odm"),
  test = c("test", "mdm")
)

MODEL_VERSION <- "v1.0"
MODEL_TYPE <- "xgboost"

# Set to NULL to generate for all matches without predictions
# Or set to specific match_ids
TARGET_MATCH_IDS <- NULL

# Set to a date to only predict matches after this date
MIN_DATE <- NULL  # e.g., as.Date("2024-01-01")

# Filter by event name (NULL for all matches of this format)
TARGET_EVENT <- NULL  # e.g., "Indian Premier League"

# Output directory
bouncerdata_root <- find_bouncerdata_dir(create = FALSE)
if (is.null(bouncerdata_root)) {
  stop("Cannot locate bouncerdata/ directory. Run from within the bouncer/ workspace with bouncerdata/ as sibling.")
}
output_dir <- file.path(bouncerdata_root, "models")

cat("\n")
cli::cli_h1("Generate {toupper(FORMAT)} Match Predictions")
cat("\n")

# 3. Load Model ----
cli::cli_h2("Loading model")

model_path <- file.path(output_dir, paste0(FORMAT, "_prediction_model.ubj"))
if (!file.exists(model_path)) {
  stop("Model not found at ", model_path, ". Run 03_train_prediction_model.R first.")
}
model <- xgb.load(model_path)
cli::cli_alert_success("Loaded model from {model_path}")

training_results <- readRDS(file.path(output_dir, paste0(FORMAT, "_prediction_training.rds")))
feature_cols <- training_results$feature_cols

# 4. Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cli::cli_alert_success("Connected to database")

# 5. Find Matches to Predict ----
cli::cli_h2("Finding matches to predict")

if (!is.null(TARGET_MATCH_IDS)) {
  # Specific matches
  placeholders <- paste(rep("?", length(TARGET_MATCH_IDS)), collapse = ", ")
  query <- sprintf("
    SELECT match_id, match_date, team1, team2, event_name, outcome_winner
    FROM cricsheet.matches
    WHERE match_id IN (%s)
    ORDER BY match_date
  ", placeholders)
  matches <- DBI::dbGetQuery(conn, query, params = as.list(TARGET_MATCH_IDS))
} else {
  # Find matches of this format without predictions
  format_types <- FORMAT_GROUPS[[FORMAT]]
  type_placeholders <- paste(sprintf("'%s'", format_types), collapse = ", ")

  query <- sprintf("
    SELECT m.match_id, m.match_date, m.team1, m.team2, m.event_name, m.outcome_winner
    FROM cricsheet.matches m
    WHERE LOWER(m.match_type) IN (%s)
      AND NOT EXISTS (
        SELECT 1 FROM pre_match_predictions p
        WHERE p.match_id = m.match_id
          AND p.model_version = ?
      )
  ", type_placeholders)
  params <- list(MODEL_VERSION)

  if (!is.null(TARGET_EVENT)) {
    query <- paste0(query, " AND m.event_name LIKE ?")
    params <- c(params, paste0("%", TARGET_EVENT, "%"))
  }

  if (!is.null(MIN_DATE)) {
    query <- paste0(query, " AND m.match_date >= ?")
    params <- c(params, MIN_DATE)
  }

  query <- paste0(query, " ORDER BY m.match_date")
  matches <- DBI::dbGetQuery(conn, query, params = params)
}

cli::cli_alert_info("Found {nrow(matches)} matches to predict")

if (nrow(matches) == 0) {
  cli::cli_alert_success("No new matches to predict")
  quit(save = "no")
}

# 6. Generate Predictions ----
cli::cli_h2("Generating predictions")

cli::cli_progress_bar("Predicting", total = nrow(matches))

predictions_list <- vector("list", nrow(matches))

for (i in seq_len(nrow(matches))) {
  match_id <- matches$match_id[i]

  prediction <- tryCatch({
    predict_match_outcome(
      match_id = match_id,
      model = model,
      conn = conn,
      model_version = MODEL_VERSION,
      model_type = MODEL_TYPE
    )
  }, error = function(e) {
    cli::cli_alert_warning("Error predicting {match_id}: {e$message}")
    NULL
  })

  if (!is.null(prediction)) {
    predictions_list[[i]] <- prediction
  }

  cli::cli_progress_update()
}

cli::cli_progress_done()

# Filter out failed predictions
predictions_list <- predictions_list[!sapply(predictions_list, is.null)]
cli::cli_alert_success("Generated {length(predictions_list)} predictions")

# 7. Store Predictions ----
cli::cli_h2("Storing predictions")

stored_count <- 0
for (prediction in predictions_list) {
  tryCatch({
    store_prediction(prediction, conn)
    stored_count <- stored_count + 1
  }, error = function(e) {
    cli::cli_alert_warning("Error storing prediction for {prediction$match_id}: {e$message}")
  })
}

cli::cli_alert_success("Stored {stored_count} predictions in database")

# 8. Update Outcomes ----
cli::cli_h2("Updating prediction outcomes")

n_updated <- batch_update_prediction_outcomes(conn)

# 9. Display Predictions ----
cli::cli_h2("Prediction Summary")

# Show recent predictions
recent_preds <- DBI::dbGetQuery(conn, "
  SELECT
    p.match_id,
    m.match_date,
    m.team1,
    m.team2,
    p.team1_win_prob,
    p.predicted_winner,
    p.confidence,
    p.actual_winner,
    p.prediction_correct
  FROM pre_match_predictions p
  JOIN cricsheet.matches m ON p.match_id = m.match_id
  WHERE p.model_version = ?
  ORDER BY m.match_date DESC
  LIMIT 20
", params = list(MODEL_VERSION))

cat("\nRecent Predictions:\n")
cat(paste(rep("-", 90), collapse = ""), "\n")
cat(sprintf("%-12s %-25s %-25s %8s %10s\n",
            "Date", "Team 1", "Team 2", "Team1 %", "Correct"))
cat(paste(rep("-", 90), collapse = ""), "\n")

for (i in seq_len(nrow(recent_preds))) {
  p <- recent_preds[i, ]
  correct_str <- if (is.na(p$prediction_correct)) {
    "Pending"
  } else if (p$prediction_correct) {
    "Yes"
  } else {
    "No"
  }

  cat(sprintf("%-12s %-25s %-25s %7.1f%% %10s\n",
              p$match_date,
              substr(p$team1, 1, 24),
              substr(p$team2, 1, 24),
              p$team1_win_prob * 100,
              correct_str))
}

# Overall accuracy for this model version
accuracy_query <- DBI::dbGetQuery(conn, "
  SELECT
    COUNT(*) as total,
    SUM(CASE WHEN prediction_correct THEN 1 ELSE 0 END) as correct
  FROM pre_match_predictions
  WHERE model_version = ?
    AND prediction_correct IS NOT NULL
", params = list(MODEL_VERSION))

if (accuracy_query$total > 0) {
  accuracy <- accuracy_query$correct / accuracy_query$total
  cli::cli_alert_success("Model {MODEL_VERSION} accuracy: {round(accuracy * 100, 1)}% ({accuracy_query$correct}/{accuracy_query$total})")
}

# 10. Summary ----
cat("\n")
cli::cli_alert_success("Prediction generation complete!")
cat("\n")

cli::cli_h3("Summary")
cli::cli_bullets(c(
  "i" = "Predictions generated: {length(predictions_list)}",
  "i" = "Predictions stored: {stored_count}",
  "i" = "Outcomes updated: {n_updated}",
  "i" = "Model version: {MODEL_VERSION}"
))
cat("\n")
