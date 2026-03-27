# track_prediction_accuracy.R
# Compare past predictions against actual match results.
# Downloads predictions from predictions-latest release,
# matches against completed results from cricsheet/cricinfo,
# and reports accuracy metrics.
#
# Usage: Rscript data-raw/release/track_prediction_accuracy.R

library(DBI)
library(duckdb)
library(piggyback)
library(cli)

cli_h1("Prediction Accuracy Tracking")

REPO <- "peteowen1/bouncerdata"

# 1. Download predictions ----
cli_h2("Loading predictions")

pred_dir <- file.path(tempdir(), "pred_accuracy")
dir.create(pred_dir, showWarnings = FALSE, recursive = TRUE)

tryCatch({
  pb_download(file = "predictions.parquet", repo = REPO, tag = "predictions-latest",
              dest = pred_dir)
  cli_alert_success("Downloaded predictions")
}, error = function(e) {
  cli_abort("Could not download predictions: {e$message}")
})

# Load with DuckDB (no arrow dependency)
conn <- dbConnect(duckdb(), ":memory:")
on.exit(dbDisconnect(conn, shutdown = TRUE))

pred_path <- gsub("\\\\", "/", file.path(pred_dir, "predictions.parquet"))
predictions <- dbGetQuery(conn, sprintf("SELECT * FROM read_parquet('%s')", pred_path))
cli_alert_info("Loaded {nrow(predictions)} predictions")

# 2. Load actual results ----
cli_h2("Loading actual results")

# Try to get results from the real DuckDB
db_path <- tryCatch({
  find_path <- function() {
    candidates <- c(
      "../bouncerdata/bouncer.duckdb",
      "C:/Users/peteo/OneDrive/Documents/bouncerverse/bouncerdata/bouncer.duckdb"
    )
    for (d in candidates) if (file.exists(d)) return(normalizePath(d, winslash = "/"))
    NULL
  }
  find_path()
}, error = function(e) NULL)

if (!is.null(db_path)) {
  results_conn <- dbConnect(duckdb(), db_path, read_only = TRUE)
  results <- dbGetQuery(results_conn, "
    SELECT match_id, team1, team2, outcome_winner, match_date, match_type
    FROM cricsheet.matches
    WHERE outcome_winner IS NOT NULL AND outcome_winner != ''
  ")
  dbDisconnect(results_conn, shutdown = TRUE)
  cli_alert_success("Loaded {nrow(results)} completed match results from DuckDB")
} else {
  # Fall back to cricsheet release
  tryCatch({
    pb_download(file = "matches.parquet", repo = REPO, tag = "cricsheet",
                dest = pred_dir)
    results <- dbGetQuery(conn, sprintf(
      "SELECT match_id, team1, team2, outcome_winner, match_date, match_type
       FROM read_parquet('%s')
       WHERE outcome_winner IS NOT NULL",
      gsub("\\\\", "/", file.path(pred_dir, "matches.parquet"))
    ))
    cli_alert_success("Loaded {nrow(results)} results from cricsheet release")
  }, error = function(e) {
    cli_abort("Could not load match results: {e$message}")
  })
}

# 3. Match predictions to results ----
cli_h2("Matching predictions to results")

# Join on match_id
if (!"match_id" %in% names(predictions)) {
  cli_alert_warning("No match_id column in predictions — cannot match to results")
  quit(save = "no")
}

matched <- merge(predictions, results, by = "match_id", all.x = FALSE, suffixes = c(".pred", ".result"))

if (nrow(matched) == 0) {
  cli_alert_info("No predictions have completed matches yet (predictions may be for future fixtures)")
  quit(save = "no")
}

# 4. Calculate accuracy ----
cli_h2("Accuracy Metrics ({nrow(matched)} matched predictions)")

# Determine if prediction was correct
matched$predicted_correct <- matched$predicted_winner == matched$outcome_winner

accuracy <- mean(matched$predicted_correct, na.rm = TRUE)

# Log loss (for probabilistic calibration)
matched$actual_prob <- ifelse(
  matched$outcome_winner == matched$team1,
  matched$team1_win_prob,
  1 - matched$team1_win_prob
)
matched$actual_prob <- pmax(0.01, pmin(0.99, matched$actual_prob))
log_loss <- -mean(log(matched$actual_prob))

# Brier score
matched$brier <- (matched$confidence - as.numeric(matched$predicted_correct))^2
brier_score <- mean(matched$brier, na.rm = TRUE)

cli_alert_success("Accuracy: {round(accuracy * 100, 1)}% ({sum(matched$predicted_correct)}/{nrow(matched)})")
cli_alert_info("Log loss: {round(log_loss, 3)}")
cli_alert_info("Brier score: {round(brier_score, 3)}")

# 5. Breakdown by confidence ----
cli_h2("Accuracy by Confidence Level")

matched$conf_bucket <- cut(matched$confidence,
                            breaks = c(0, 0.55, 0.65, 0.75, 0.85, 1),
                            labels = c("50-55%", "55-65%", "65-75%", "75-85%", "85-100%"))

if (any(!is.na(matched$conf_bucket))) {
  acc_by_conf <- aggregate(
    predicted_correct ~ conf_bucket,
    data = matched,
    FUN = function(x) paste0(round(mean(x) * 100, 1), "% (n=", length(x), ")")
  )
  for (i in seq_len(nrow(acc_by_conf))) {
    cli_alert_info("  {acc_by_conf$conf_bucket[i]}: {acc_by_conf$predicted_correct[i]}")
  }
}

cli_h1("Done")
