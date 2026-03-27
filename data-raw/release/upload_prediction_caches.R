# upload_prediction_caches.R
# Export prediction-relevant data from DuckDB to parquets and upload
# to the predictions-cache release on peteowen1/bouncerdata.
#
# Run this locally after the heavy pipeline (steps 1-9) completes.
# The GHA predictions workflow downloads these caches instead of
# needing the full 18GB DuckDB.
#
# Usage:
#   Rscript data-raw/release/upload_prediction_caches.R

library(DBI)
library(duckdb)
library(piggyback)
library(cli)

# NOTE: Do NOT load arrow here — it invalidates DuckDB connections on Windows.
# Instead, use DuckDB's built-in COPY TO for parquet export (no arrow needed).

# Setup ----

cli_h1("Export Prediction Caches")

# Find bouncerdata directory (walk up from bouncer/)
find_bouncerdata <- function() {
  candidates <- c(
    file.path(getwd(), "..", "bouncerdata"),
    file.path(getwd(), "bouncerdata"),
    "C:/Users/peteo/OneDrive/Documents/bouncerverse/bouncerdata"
  )
  for (d in candidates) {
    db <- file.path(d, "bouncer.duckdb")
    if (file.exists(db)) return(normalizePath(d, winslash = "/"))
  }
  cli_abort("Could not find bouncerdata directory")
}

bouncerdata_dir <- find_bouncerdata()
db_path <- file.path(bouncerdata_dir, "bouncer.duckdb")
cli_alert_info("Database: {db_path}")

conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

# Helper: export query results to parquet using DuckDB's built-in COPY TO
export_query_to_parquet <- function(conn, query, output_path) {
  # Use DuckDB's native parquet writer (no arrow dependency)
  escaped_path <- gsub("\\\\", "/", output_path)
  copy_sql <- sprintf("COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)", query, escaped_path)
  DBI::dbExecute(conn, copy_sql)
  size_mb <- file.size(output_path) / 1024 / 1024
  # Count rows from the file
  count <- DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) as n FROM read_parquet('%s')", escaped_path))$n
  cli_alert_success("{basename(output_path)}: {format(count, big.mark=',')} rows, {round(size_mb, 1)} MB")
}

cache_dir <- file.path(tempdir(), "prediction_caches")
dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)

REPO <- "peteowen1/bouncerdata"
TAG <- "predictions-cache"
FORMATS <- c("t20", "odi", "test")

# Ensure release exists
tryCatch(

  pb_release_create(repo = REPO, tag = TAG, name = "Prediction Caches"),
  error = function(e) cli_alert_info("Release '{TAG}' already exists")
)

# 1. Match results (for form, H2H, venue stats) ----
cli_h2("Exporting match results")

export_query_to_parquet(conn, "
  SELECT match_id, match_date, match_type, venue, team1, team2,
         event_name, event_match_number, event_group,
         team_type, gender, toss_winner, toss_decision,
         outcome_winner, outcome_type, outcome_method
  FROM cricsheet.matches
  WHERE outcome_winner IS NOT NULL OR outcome_type IS NOT NULL
  ORDER BY match_date
", file.path(cache_dir, "matches.parquet"))

# 2. Match innings (for venue avg scores) ----
cli_h2("Exporting match innings")

export_query_to_parquet(conn, "
  SELECT mi.match_id, mi.innings, mi.total_runs, mi.batting_team
  FROM cricsheet.match_innings mi
", file.path(cache_dir, "match_innings.parquet"))

# 3. Team ELO ----
cli_h2("Exporting team ELO")

export_query_to_parquet(conn, "
  SELECT * FROM main.team_elo ORDER BY match_date
", file.path(cache_dir, "team_elo.parquet"))

# 4. Player skill indices (latest per player per format) ----
cli_h2("Exporting player skill indices (latest per player)")

for (fmt in FORMATS) {
  skill_table <- paste0(fmt, "_player_skill")

  export_query_to_parquet(conn, sprintf("
    WITH ranked AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM main.%s WHERE batter_scoring_index IS NOT NULL
    )
    SELECT batter_id, match_date, batter_scoring_index, batter_survival_rate, batter_balls_faced
    FROM ranked WHERE rn = 1
  ", skill_table), file.path(cache_dir, paste0(fmt, "_batter_skill_latest.parquet")))

  export_query_to_parquet(conn, sprintf("
    WITH ranked AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM main.%s WHERE bowler_economy_index IS NOT NULL
    )
    SELECT bowler_id, match_date, bowler_economy_index, bowler_strike_rate, bowler_balls_bowled
    FROM ranked WHERE rn = 1
  ", skill_table), file.path(cache_dir, paste0(fmt, "_bowler_skill_latest.parquet")))
}

# 5. Player 3-Way ELO (latest per player per format) ----
cli_h2("Exporting player 3-Way ELO (latest per player)")

for (fmt in FORMATS) {
  # Try gender-prefixed table names first (mens_t20_3way_elo), then fallback
  elo_candidates <- c(
    paste0("mens_", fmt, "_3way_elo"),
    paste0(fmt, "_3way_elo")
  )
  elo_table <- NULL
  for (candidate in elo_candidates) {
    exists_check <- tryCatch(
      { DBI::dbGetQuery(conn, sprintf("SELECT 1 FROM main.%s LIMIT 1", candidate)); TRUE },
      error = function(e) FALSE
    )
    if (exists_check) { elo_table <- candidate; break }
  }
  if (is.null(elo_table)) {
    cli_alert_warning("No 3-Way ELO table found for {fmt} (tried: {paste(elo_candidates, collapse=', ')})")
    next
  }
  cli_alert_info("Using table: {elo_table}")

  export_query_to_parquet(conn, sprintf("
    WITH ranked AS (
      SELECT batter_id, match_date, batter_run_elo_after, batter_wicket_elo_after,
             ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM main.%s WHERE batter_run_elo_after IS NOT NULL
    )
    SELECT batter_id, match_date, batter_run_elo_after, batter_wicket_elo_after
    FROM ranked WHERE rn = 1
  ", elo_table), file.path(cache_dir, paste0(fmt, "_batter_elo_latest.parquet")))

  export_query_to_parquet(conn, sprintf("
    WITH ranked AS (
      SELECT bowler_id, match_date, bowler_run_elo_after, bowler_wicket_elo_after,
             ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM main.%s WHERE bowler_run_elo_after IS NOT NULL
    )
    SELECT bowler_id, match_date, bowler_run_elo_after, bowler_wicket_elo_after
    FROM ranked WHERE rn = 1
  ", elo_table), file.path(cache_dir, paste0(fmt, "_bowler_elo_latest.parquet")))
}

# 6. Prediction models ----
cli_h2("Copying prediction models")

models_dir <- file.path(bouncerdata_dir, "models")

for (fmt in FORMATS) {
  model_file <- file.path(models_dir, paste0(fmt, "_prediction_model.ubj"))
  if (file.exists(model_file)) {
    file.copy(model_file, file.path(cache_dir, basename(model_file)), overwrite = TRUE)
    cli_alert_success("Copied {basename(model_file)}")
  } else {
    cli_alert_warning("Model not found: {basename(model_file)}")
  }

  features_file <- file.path(models_dir, paste0(fmt, "_prediction_features.rds"))
  if (file.exists(features_file)) {
    file.copy(features_file, file.path(cache_dir, basename(features_file)), overwrite = TRUE)
    cli_alert_success("Copied {basename(features_file)}")
  }
}

# 7. Recent team lineups (for XI inference) ----
cli_h2("Exporting recent team lineups")

export_query_to_parquet(conn, "
  WITH recent_matches AS (
    SELECT match_id, match_type, team1, team2, match_date, gender, team_type
    FROM cricsheet.matches
    WHERE match_date >= CURRENT_DATE - INTERVAL '6 months'
  ),
  player_appearances AS (
    SELECT d.match_id, m.match_type, m.match_date, m.gender, m.team_type,
           d.batting_team AS team, d.batter_id AS player_id
    FROM cricsheet.deliveries d
    JOIN recent_matches m ON d.match_id = m.match_id
    UNION ALL
    SELECT d.match_id, m.match_type, m.match_date, m.gender, m.team_type,
           d.bowling_team AS team, d.bowler_id AS player_id
    FROM cricsheet.deliveries d
    JOIN recent_matches m ON d.match_id = m.match_id
  )
  SELECT DISTINCT match_id, match_type, match_date, gender, team_type, team, player_id
  FROM player_appearances
  ORDER BY match_date DESC
", file.path(cache_dir, "recent_lineups.parquet"))

# Upload all files ----
cli_h2("Uploading to GitHub release")

cache_files <- list.files(cache_dir, full.names = TRUE)
cli_alert_info("Uploading {length(cache_files)} files to {REPO} tag '{TAG}'")

for (f in cache_files) {
  tryCatch({
    pb_upload(f, repo = REPO, tag = TAG, overwrite = TRUE)
    cli_alert_success("Uploaded {basename(f)}")
  }, error = function(e) {
    cli_alert_danger("Failed to upload {basename(f)}: {e$message}")
  })
}

# Summary ----
cli_h1("Upload Complete")

total_size <- sum(file.size(cache_files)) / 1024 / 1024
cli_alert_info("Total files: {length(cache_files)}")
cli_alert_info("Total size: {round(total_size, 1)} MB")
cli_alert_info("Release: {REPO} tag '{TAG}'")
cli_alert_info("")
cli_alert_info("Next: Run predictions workflow on GHA:")
cli_alert_info("  gh workflow run predictions-pipeline.yml --repo peteowen1/bouncer --ref dev")
