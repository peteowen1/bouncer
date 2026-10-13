# run_predictions_gha.R
# Lightweight prediction script for GitHub Actions.
#
# Data source hierarchy (mirrors pannaverse's Opta/FBref pattern):
#   - Cricinfo = PRIMARY (fixtures, recent results, most up-to-date)
#   - Cricsheet = SECONDARY (historical coverage for form/H2H/venue stats)
#   - Prediction caches = RATINGS (player skills, ELOs from heavy pipeline)
#
# Downloads prediction caches (parquets) from GitHub releases,
# loads them into a temporary DuckDB so existing SQL-based
# feature functions work unchanged, then generates predictions
# for upcoming fixtures.
#
# This script does NOT need the full 18GB DuckDB — it works with
# ~50MB of cached aggregates from the predictions-cache release.
#
# Prerequisites: Run upload_prediction_caches.R locally after
# the heavy pipeline to populate the predictions-cache release.

# 1. Setup ----

library(DBI)
library(duckdb)
library(arrow)
library(piggyback)
library(cli)
library(xgboost)

devtools::load_all()

cli_h1("Bouncer Predictions (GHA)")

REPO <- "peteowen1/bouncerdata"
FORMATS <- c("t20", "odi", "test")

# Where to download caches
cache_dir <- Sys.getenv("CACHE_DIR", file.path(tempdir(), "prediction_caches"))
dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)

# 2. Download prediction caches ----
cli_h2("Downloading prediction caches")

download_cache <- function(filename) {
  dest <- file.path(cache_dir, filename)
  if (file.exists(dest)) {
    cli_alert_info("Already have {filename}")
    return(TRUE)
  }
  tryCatch({
    pb_download(file = filename, repo = REPO, tag = "predictions-cache",
                dest = cache_dir)
    if (file.exists(dest)) {
      cli_alert_success("Downloaded {filename}")
      return(TRUE)
    }
    cli_alert_warning("File not found: {filename}")
    return(FALSE)
  }, error = function(e) {
    cli_alert_warning("Failed to download {filename}: {e$message}")
    return(FALSE)
  })
}

# Core files
download_cache("matches.parquet")
download_cache("match_innings.parquet")
download_cache("team_elo.parquet")
download_cache("recent_lineups.parquet")

# Per-format files
for (fmt in FORMATS) {
  download_cache(paste0(fmt, "_batter_skill_latest.parquet"))
  download_cache(paste0(fmt, "_bowler_skill_latest.parquet"))
  download_cache(paste0(fmt, "_batter_elo_latest.parquet"))
  download_cache(paste0(fmt, "_bowler_elo_latest.parquet"))
  download_cache(paste0(fmt, "_prediction_model.ubj"))
  download_cache(paste0(fmt, "_margin_model.ubj"))
  download_cache(paste0(fmt, "_team_skill_latest.parquet"))
  download_cache(paste0(fmt, "_venue_skill_latest.parquet"))
  download_cache(paste0(fmt, "_prediction_features.rds"))
}

# Download Cricinfo data (primary source — most up-to-date fixtures + results)
cli_h2("Downloading Cricinfo data (primary source)")
cricinfo_dir <- file.path(cache_dir, "cricinfo")
dir.create(cricinfo_dir, showWarnings = FALSE, recursive = TRUE)

for (f in c("fixtures.parquet", "matches.parquet", "innings.parquet")) {
  tryCatch({
    pb_download(file = f, repo = REPO, tag = "cricinfo", dest = cricinfo_dir)
    cli_alert_success("Downloaded cricinfo/{f}")
  }, error = function(e) {
    cli_alert_warning("Cricinfo {f} not available: {e$message}")
  })
}

# Download Cricsheet matches (secondary source — broader historical coverage)
cli_h2("Downloading Cricsheet data (secondary source)")
cricsheet_dir <- file.path(cache_dir, "cricsheet")
dir.create(cricsheet_dir, showWarnings = FALSE, recursive = TRUE)

tryCatch({
  pb_download(file = "matches.parquet", repo = REPO, tag = "cricsheet",
              dest = cricsheet_dir)
  cli_alert_success("Downloaded cricsheet matches (historical coverage)")
}, error = function(e) {
  cli_alert_warning("Could not download cricsheet matches: {e$message}")
})

# 3. Build temporary DuckDB ----
cli_h2("Building temporary DuckDB from caches")

db_path <- file.path(tempdir(), "bouncer_predictions.duckdb")
if (file.exists(db_path)) file.remove(db_path)

conn <- dbConnect(duckdb(), db_path)
on.exit(dbDisconnect(conn, shutdown = TRUE))

# Create schemas
dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS cricsheet")
dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS cricinfo")
dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS main")

# Load Cricinfo fixtures (primary source for upcoming + recent results)
cricinfo_fixtures_path <- file.path(cache_dir, "cricinfo", "fixtures.parquet")
if (file.exists(cricinfo_fixtures_path)) {
  dbExecute(conn, sprintf(
    "CREATE TABLE cricinfo.fixtures AS SELECT * FROM read_parquet('%s')",
    gsub("\\\\", "/", cricinfo_fixtures_path)
  ))
  fixture_count <- dbGetQuery(conn, "SELECT COUNT(*) as n FROM cricinfo.fixtures")$n
  cli_alert_success("Loaded cricinfo fixtures (primary): {format(fixture_count, big.mark=',')} rows")
}

# Load Cricinfo match metadata
cricinfo_matches_path <- file.path(cache_dir, "cricinfo", "matches.parquet")
if (file.exists(cricinfo_matches_path)) {
  dbExecute(conn, sprintf(
    "CREATE TABLE cricinfo.matches AS SELECT * FROM read_parquet('%s')",
    gsub("\\\\", "/", cricinfo_matches_path)
  ))
  cli_alert_success("Loaded cricinfo matches")
}

# Load Cricinfo innings
cricinfo_innings_path <- file.path(cache_dir, "cricinfo", "innings.parquet")
if (file.exists(cricinfo_innings_path)) {
  dbExecute(conn, sprintf(
    "CREATE TABLE cricinfo.innings AS SELECT * FROM read_parquet('%s')",
    gsub("\\\\", "/", cricinfo_innings_path)
  ))
  cli_alert_success("Loaded cricinfo innings")
}

# Load Cricsheet matches (secondary — broader historical coverage for form/H2H/venue)
cricsheet_matches_path <- file.path(cache_dir, "cricsheet", "matches.parquet")
cached_matches_path <- file.path(cache_dir, "matches.parquet")

# DISTINCT ON: the cricsheet release's matches.parquet held 1,219 matches
# repeated up to 86 times (2026-10-13), which inflated form and h2h.
if (file.exists(cricsheet_matches_path)) {
  dbExecute(conn, sprintf(
    "CREATE TABLE cricsheet.matches AS SELECT DISTINCT ON (match_id) * FROM read_parquet('%s')",
    gsub("\\\\", "/", cricsheet_matches_path)
  ))
  cli_alert_success("Loaded cricsheet matches (secondary, historical)")
} else if (file.exists(cached_matches_path)) {
  dbExecute(conn, sprintf(
    "CREATE TABLE cricsheet.matches AS SELECT DISTINCT ON (match_id) * FROM read_parquet('%s')",
    gsub("\\\\", "/", cached_matches_path)
  ))
  cli_alert_success("Loaded matches from prediction cache")
} else {
  cli_abort("No matches data available!")
}

match_count <- dbGetQuery(conn, "SELECT COUNT(*) as n FROM cricsheet.matches")$n
cli_alert_info("Cricsheet matches loaded: {format(match_count, big.mark=',')} (historical coverage)")

# Load match innings (from prediction cache — exported from DuckDB)
innings_path <- file.path(cache_dir, "match_innings.parquet")
if (file.exists(innings_path)) {
  dbExecute(conn, sprintf(
    "CREATE TABLE cricsheet.match_innings AS SELECT * FROM read_parquet('%s')",
    gsub("\\\\", "/", innings_path)
  ))
  cli_alert_success("Loaded match innings")
}

# Load team ELO
team_elo_path <- file.path(cache_dir, "team_elo.parquet")
if (file.exists(team_elo_path)) {
  dbExecute(conn, sprintf(
    "CREATE TABLE main.team_elo AS SELECT * FROM read_parquet('%s')",
    gsub("\\\\", "/", team_elo_path)
  ))
  cli_alert_success("Loaded team ELO")
}

# Load player skills and ELO into format-specific tables
for (fmt in FORMATS) {
  # Batter skills
  batter_skill_path <- file.path(cache_dir, paste0(fmt, "_batter_skill_latest.parquet"))
  bowler_skill_path <- file.path(cache_dir, paste0(fmt, "_bowler_skill_latest.parquet"))

  if (file.exists(batter_skill_path) && file.exists(bowler_skill_path)) {
    # Create unified skill table with explicit VARCHAR types to avoid NULL type inference
    dbExecute(conn, sprintf(
      "CREATE TABLE main.%s_player_skill AS
       SELECT b.batter_id::VARCHAR AS batter_id, b.match_date, b.batter_scoring_index, b.batter_survival_rate, b.batter_balls_faced,
              CAST(NULL AS VARCHAR) AS bowler_id, CAST(NULL AS DOUBLE) AS bowler_economy_index,
              CAST(NULL AS DOUBLE) AS bowler_strike_rate, CAST(NULL AS INTEGER) AS bowler_balls_bowled,
              CAST(NULL AS VARCHAR) AS delivery_id
       FROM read_parquet('%s') b",
      fmt, gsub("\\\\", "/", batter_skill_path)
    ))
    dbExecute(conn, sprintf(
      "INSERT INTO main.%s_player_skill
       SELECT CAST(NULL AS VARCHAR), w.match_date, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS INTEGER),
              w.bowler_id::VARCHAR, w.bowler_economy_index, w.bowler_strike_rate, w.bowler_balls_bowled,
              CAST(NULL AS VARCHAR)
       FROM read_parquet('%s') w",
      fmt, gsub("\\\\", "/", bowler_skill_path)
    ))
    cli_alert_success("Loaded {fmt} player skills")
  }

  # Player ELO
  batter_elo_path <- file.path(cache_dir, paste0(fmt, "_batter_elo_latest.parquet"))
  bowler_elo_path <- file.path(cache_dir, paste0(fmt, "_bowler_elo_latest.parquet"))

  if (file.exists(batter_elo_path) && file.exists(bowler_elo_path)) {
    dbExecute(conn, sprintf(
      "CREATE TABLE main.%s_3way_elo AS
       SELECT b.batter_id::VARCHAR AS batter_id, b.match_date, b.batter_run_elo_after, b.batter_wicket_elo_after,
              CAST(NULL AS VARCHAR) AS bowler_id, CAST(NULL AS DOUBLE) AS bowler_run_elo_after,
              CAST(NULL AS DOUBLE) AS bowler_wicket_elo_after, CAST(NULL AS VARCHAR) AS delivery_id
       FROM read_parquet('%s') b",
      fmt, gsub("\\\\", "/", batter_elo_path)
    ))
    dbExecute(conn, sprintf(
      "INSERT INTO main.%s_3way_elo
       SELECT CAST(NULL AS VARCHAR), w.match_date, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),
              w.bowler_id::VARCHAR, w.bowler_run_elo_after, w.bowler_wicket_elo_after,
              CAST(NULL AS VARCHAR)
       FROM read_parquet('%s') w",
      fmt, gsub("\\\\", "/", bowler_elo_path)
    ))
    cli_alert_success("Loaded {fmt} player 3-Way ELO")
  }
}

# Team and venue per-delivery skills (latest per team / venue), read by
# calc_match_features(). load_pre_match_inputs(strict = TRUE) aborts if absent.
for (fmt in FORMATS) {
  for (kind in c("team", "venue")) {
    path <- file.path(cache_dir, paste0(fmt, "_", kind, "_skill_latest.parquet"))
    if (file.exists(path)) {
      dbExecute(conn, sprintf("CREATE TABLE main.%s_%s_skill AS SELECT * FROM read_parquet('%s')",
                              fmt, kind, gsub("\\\\", "/", path)))
      cli_alert_success("Loaded {fmt} {kind} skills")
    }
  }
}

# Load recent lineups into deliveries table (for XI inference)
lineups_path <- file.path(cache_dir, "recent_lineups.parquet")
if (file.exists(lineups_path)) {
  dbExecute(conn, sprintf(
    "CREATE TABLE cricsheet.deliveries AS
     SELECT match_id, match_date, match_type, gender, team AS batting_team, team AS bowling_team,
            player_id AS batter_id, player_id AS bowler_id
     FROM read_parquet('%s')",
    gsub("\\\\", "/", lineups_path)
  ))
  cli_alert_success("Loaded recent lineups for XI inference")
}

# 4. Get upcoming fixtures ----
cli_h2("Loading upcoming fixtures")

upcoming <- tryCatch(
  get_upcoming_matches(source = "remote",
                       days_ahead = as.integer(Sys.getenv("DAYS_AHEAD", "14"))),
  error = function(e) {
    cli_alert_warning("Could not load fixtures from Cricinfo: {e$message}")
    data.frame()
  }
)

if (nrow(upcoming) == 0) {
  days <- as.integer(Sys.getenv("DAYS_AHEAD", "14"))
  cli_alert_warning("No upcoming matches found in next {days} days")
  quit(save = "no", status = 0)  # on.exit handles disconnect
}

cli_alert_success("Found {nrow(upcoming)} upcoming matches")

# 5. Generate predictions ----
cli_h2("Generating predictions")

# Build home lookups once
all_matches <- dbGetQuery(conn,
  "SELECT team1, team2, venue, team_type, gender, match_type FROM cricsheet.matches")
home_lookups <- build_home_lookups(all_matches)

# Features come from calc_match_features(), the builder the models were
# trained on (via load_pre_match_inputs() / build_fixture_features()). The old
# single-match path, calculate_pre_match_features(), had drifted from it and
# gave every fixture the same default features (2026-10-13).
upcoming$fmt <- vapply(seq_len(nrow(upcoming)), function(i)
  normalize_format(upcoming$match_type[i] %||% upcoming$format[i] %||% "t20"), character(1))
upcoming$match_date <- as.Date(substr(as.character(upcoming$start_date), 1, 10))

predictions <- list()
for (fmt in intersect(FORMATS, unique(upcoming$fmt))) {
  fx <- upcoming[upcoming$fmt == fmt, , drop = FALSE]
  model_path <- file.path(cache_dir, paste0(fmt, "_prediction_model.ubj"))
  if (!file.exists(model_path)) {
    cli_alert_warning("No {fmt} model in the cache - skipping {nrow(fx)} fixture{?s}")
    next
  }
  model <- xgb.load(model_path)
  margin_model_path <- file.path(cache_dir, paste0(fmt, "_margin_model.ubj"))
  if (!file.exists(margin_model_path)) {
    cli_abort("No {fmt} margin model in the cache. The win-probability model takes predicted_margin as an input, so predicting without it gives wrong numbers. Re-run upload_prediction_caches.R.")
  }
  margin_model <- xgb.load(margin_model_path)

  inputs <- load_pre_match_inputs(conn, fmt, strict = TRUE)
  fixtures <- data.frame(
    match_id = as.character(fx$match_id), match_date = fx$match_date, match_type = fmt,
    team1 = fx$team1, team2 = fx$team2, gender = fx$gender,
    venue = fx$venue %||% NA_character_, event_name = fx$series_name %||% NA_character_,
    stringsAsFactors = FALSE)
  feats <- build_fixture_features(fixtures, inputs, home_lookups)
  probs <- predict_fixture_probs(feats, model, margin_model)

  out <- data.frame(
    prediction_id = paste0(probs$match_id, "_v1.0"), match_id = probs$match_id,
    format = fmt, model_version = "v1.0", model_type = "xgboost", prediction_date = Sys.time(),
    team1 = fixtures$team1, team2 = fixtures$team2, team_type = feats$team_type_resolved,
    team1_win_prob = probs$team1_win_prob, draw_prob = probs$draw_prob,
    team2_win_prob = probs$team2_win_prob, stringsAsFactors = FALSE)
  out$predicted_winner <- ifelse(out$team1_win_prob >= out$team2_win_prob, out$team1, out$team2)
  out$confidence <- pmax(out$team1_win_prob, out$team2_win_prob)
  out$actual_winner <- NA_character_
  out$prediction_correct <- NA
  out$team1_elo_found <- feats$team1_elo_found
  out$team2_elo_found <- feats$team2_elo_found
  out$team1_in_history <- feats$team1_in_history
  out$team2_in_history <- feats$team2_in_history
  predictions[[fmt]] <- out
  cli_alert_success(paste0(
    "{toupper(fmt)}: {nrow(out)} fixture{?s}, team1 win prob ",
    "{round(min(out$team1_win_prob), 3)}-{round(max(out$team1_win_prob), 3)}, ",
    "{length(unique(round(out$team1_win_prob, 4)))} distinct"))
}

# Fail loudly rather than publish predictions built on default features
# (2026-10-13: every fixture defaulted for months while the run stayed green).
skipped <- upcoming[!upcoming$fmt %in% FORMATS, , drop = FALSE]
if (nrow(skipped) > 0) {
  cli_alert_warning("{nrow(skipped)} fixture{?s} in formats with no model skipped: {paste(unique(skipped$fmt), collapse = ', ')}")
}
pred_all <- do.call(rbind, predictions)
n_expected <- sum(upcoming$fmt %in% FORMATS)
if (is.null(pred_all) || nrow(pred_all) != n_expected) {
  cli_abort("Predicted {NROW(pred_all)} of {n_expected} fixtures in supported formats")
}
check_fixture_coverage(pred_all)

# Cleanup DB (on.exit handles disconnect, just remove temp file)
if (file.exists(db_path)) file.remove(db_path)

# 6. Export predictions ----
cli_h2("Exporting predictions")

if (length(predictions) > 0) {
  pred_df <- pred_all

  output_dir <- Sys.getenv("OUTPUT_DIR", file.path(tempdir(), "predictions_output"))
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  pred_path <- file.path(output_dir, "predictions.parquet")
  write_parquet(pred_df, pred_path, compression = "zstd")
  cli_alert_success("Saved {nrow(pred_df)} predictions to {pred_path}")

  pred_csv_path <- file.path(output_dir, "predictions.csv")
  write.csv(pred_df, pred_csv_path, row.names = FALSE)
  cli_alert_success("Saved predictions CSV")

  # Upload to predictions-latest release
  upload_predictions <- as.logical(Sys.getenv("UPLOAD_PREDICTIONS", "true"))
  if (upload_predictions) {
    cli_h2("Uploading predictions")
    tryCatch(
      pb_release_create(repo = REPO, tag = "predictions-latest", name = "Match Predictions"),
      error = function(e) cli_alert_info("Release already exists")
    )
    pb_upload(pred_path, repo = REPO, tag = "predictions-latest", overwrite = TRUE)
    pb_upload(pred_csv_path, repo = REPO, tag = "predictions-latest", overwrite = TRUE)
    cli_alert_success("Uploaded to predictions-latest release")
  }

  # Write output for GHA summary
  output_file <- Sys.getenv("GITHUB_OUTPUT", "")
  if (nzchar(output_file)) {
    cat(sprintf("prediction_count=%d\n", nrow(pred_df)), file = output_file, append = TRUE)
    cat(sprintf("formats=%s\n", paste(unique(pred_df$model_type), collapse = ",")),
        file = output_file, append = TRUE)
  }
} else {
  cli_alert_warning("No predictions generated")
}

cli_h1("Predictions Complete!")
