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

if (file.exists(cricsheet_matches_path)) {
  dbExecute(conn, sprintf(
    "CREATE TABLE cricsheet.matches AS SELECT * FROM read_parquet('%s')",
    gsub("\\\\", "/", cricsheet_matches_path)
  ))
  cli_alert_success("Loaded cricsheet matches (secondary, historical)")
} else if (file.exists(cached_matches_path)) {
  dbExecute(conn, sprintf(
    "CREATE TABLE cricsheet.matches AS SELECT * FROM read_parquet('%s')",
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
    # Create a unified skill table matching the schema expected by existing functions
    dbExecute(conn, sprintf(
      "CREATE TABLE main.%s_player_skill AS
       SELECT b.batter_id, b.match_date, b.batter_scoring_index, b.batter_survival_rate, b.batter_balls_faced,
              NULL AS bowler_id, NULL AS bowler_economy_index, NULL AS bowler_strike_rate, NULL AS bowler_balls_bowled,
              NULL AS delivery_id
       FROM read_parquet('%s') b",
      fmt, gsub("\\\\", "/", batter_skill_path)
    ))
    # Add bowler rows
    dbExecute(conn, sprintf(
      "INSERT INTO main.%s_player_skill
       SELECT NULL, w.match_date, NULL, NULL, NULL,
              w.bowler_id, w.bowler_economy_index, w.bowler_strike_rate, w.bowler_balls_bowled,
              NULL
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
       SELECT b.batter_id, b.match_date, b.batter_run_elo_after, b.batter_wicket_elo_after,
              NULL AS bowler_id, NULL AS bowler_run_elo_after, NULL AS bowler_wicket_elo_after,
              NULL AS delivery_id
       FROM read_parquet('%s') b",
      fmt, gsub("\\\\", "/", batter_elo_path)
    ))
    dbExecute(conn, sprintf(
      "INSERT INTO main.%s_3way_elo
       SELECT NULL, w.match_date, NULL, NULL,
              w.bowler_id, w.bowler_run_elo_after, w.bowler_wicket_elo_after,
              NULL
       FROM read_parquet('%s') w",
      fmt, gsub("\\\\", "/", bowler_elo_path)
    ))
    cli_alert_success("Loaded {fmt} player 3-Way ELO")
  }
}

# Load recent lineups into deliveries table (for XI inference)
lineups_path <- file.path(cache_dir, "recent_lineups.parquet")
if (file.exists(lineups_path)) {
  dbExecute(conn, sprintf(
    "CREATE TABLE cricsheet.deliveries AS
     SELECT match_id, match_date, team AS batting_team, team AS bowling_team,
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

predictions <- list()

for (i in seq_len(nrow(upcoming))) {
  fixture <- upcoming[i, ]
  fmt <- normalize_format(fixture$match_type %||% fixture$format %||% "t20")

  # Load model for this format
  model_path <- file.path(cache_dir, paste0(fmt, "_prediction_model.ubj"))
  if (!file.exists(model_path)) {
    cli_alert_warning("No model for format {fmt}, using ELO-based prediction")
    model <- NULL
  } else {
    model <- xgb.load(model_path)
  }

  # Try to calculate features and predict
  tryCatch({
    match_id <- fixture$match_id %||% fixture$id %||% paste0("upcoming_", i)

    # Check if match exists in DB (for feature calculation)
    exists_in_db <- dbGetQuery(conn, sprintf(
      "SELECT COUNT(*) as n FROM cricsheet.matches WHERE match_id = %s",
      dbQuoteLiteral(conn, as.character(match_id))
    ))$n > 0

    if (!exists_in_db) {
      # Insert fixture into matches table for feature calculation
      dbExecute(conn, sprintf(
        "INSERT INTO cricsheet.matches (match_id, match_date, match_type, venue, team1, team2,
         team_type, gender, event_name)
         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        dbQuoteLiteral(conn, as.character(match_id)),
        dbQuoteLiteral(conn, as.character(fixture$match_date %||% fixture$date %||% Sys.Date())),
        dbQuoteLiteral(conn, fixture$match_type %||% fixture$format %||% "T20"),
        dbQuoteLiteral(conn, fixture$venue %||% "Unknown"),
        dbQuoteLiteral(conn, fixture$team1 %||% fixture$home_team %||% "TBD"),
        dbQuoteLiteral(conn, fixture$team2 %||% fixture$away_team %||% "TBD"),
        dbQuoteLiteral(conn, fixture$team_type %||% "international"),
        dbQuoteLiteral(conn, fixture$gender %||% "male"),
        dbQuoteLiteral(conn, fixture$event_name %||% fixture$series %||% NA_character_)
      ))
    }

    pred <- predict_match_outcome(
      match_id = as.character(match_id),
      model = model,
      conn = conn,
      model_type = if (!is.null(model)) "xgboost" else "elo"
    )

    if (!is.null(pred)) {
      predictions[[length(predictions) + 1]] <- as.data.frame(pred, stringsAsFactors = FALSE)
      cli_alert_success("{pred$team1} vs {pred$team2}: {pred$predicted_winner} ({round(pred$confidence * 100, 1)}%)")
    }
  }, error = function(e) {
    cli_alert_warning("Failed to predict fixture {i}: {e$message}")
  })
}

# Cleanup DB (on.exit handles disconnect, just remove temp file)
if (file.exists(db_path)) file.remove(db_path)

# 6. Export predictions ----
cli_h2("Exporting predictions")

if (length(predictions) > 0) {
  pred_df <- do.call(rbind, predictions)

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
