# Dated, append-only history of published match predictions (issue #88).
#
# Every publish writes one new file, predictions_<YYYYMMDDTHHMMSSZ>.parquet, to
# the predictions-history release. Files are never rewritten: a correction is a
# new publish with a new timestamp. predictions-latest is still overwritten on
# every run; this history is the record of what was published, and when.
#
# Plain comments, no roxygen: these are internal helpers used by
# data-raw/models/pre-match/run_predictions_gha.R, which calls devtools::load_all().

# File name for one publish, from its UTC time to the second.
prediction_history_filename <- function(published_utc) {
  if (!inherits(published_utc, "POSIXt")) {
    cli::cli_abort("{.arg published_utc} must be a POSIXct time.")
  }
  paste0("predictions_", format(published_utc, "%Y%m%dT%H%M%SZ", tz = "UTC"), ".parquet")
}

# Long-format rows for one publish: three per fixture (team1, Draw, team2), so
# each row is a single outcome with its own probability. Binary-model fixtures
# carry draw_prob = 0, so their Draw row has probability 0, as the model says.
build_prediction_history <- function(pred_df, published_utc) {
  required <- c("match_id", "format", "team1", "team2",
                "team1_win_prob", "draw_prob", "team2_win_prob", "model_version")
  missing_cols <- setdiff(required, names(pred_df))
  if (length(missing_cols) > 0) {
    cli::cli_abort("Prediction table is missing: {paste(missing_cols, collapse = ', ')}")
  }

  probs <- c(pred_df$team1_win_prob, pred_df$draw_prob, pred_df$team2_win_prob)
  if (anyNA(probs) || any(probs < 0 | probs > 1)) {
    cli::cli_abort("Refusing to publish history: probabilities must be finite and between 0 and 1")
  }

  n <- nrow(pred_df)
  published <- as.POSIXct(published_utc, tz = "UTC")
  data.frame(
    verse = "bouncer",
    event = rep(as.character(pred_df$match_id), times = 3),
    market = "match_winner",
    selection = c(as.character(pred_df$team1), rep("Draw", n), as.character(pred_df$team2)),
    selection_side = rep(c("team1", "draw", "team2"), each = n),
    probability = as.numeric(probs),
    published_utc = rep(published, times = 3 * n),
    model_version = rep(as.character(pred_df$model_version), times = 3),
    format = rep(as.character(pred_df$format), times = 3),
    team1 = rep(as.character(pred_df$team1), times = 3),
    team2 = rep(as.character(pred_df$team2), times = 3),
    stringsAsFactors = FALSE
  )
}

# Write one publish's history to dir as a new file and return its path. A second
# write with the same timestamp is an error, never a replacement.
write_prediction_history <- function(history, dir, published_utc) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    cli::cli_abort("Writing the prediction history needs the arrow package.")
  }
  path <- file.path(dir, prediction_history_filename(published_utc))
  if (file.exists(path)) {
    cli::cli_abort("{.file {basename(path)}} already exists. History files are never overwritten.")
  }
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(history, path, compression = "zstd")
  path
}
