#!/usr/bin/env Rscript
# upload_ratings_release.R - Upload player ratings data to GitHub release
#
# Exports player_game_data and stat_ratings from DuckDB to parquet files
# and uploads to the "ratings" GitHub release on peteowen1/bouncerdata.
#
# Files uploaded:
#   - {format}_player_game_data.parquet (one per format: t20, odi, test)
#   - {format}_stat_ratings.parquet (one per format)
#
# Prerequisites:
#   - Pipeline steps 13-14 run (player game data + stat ratings computed)
#   - GITHUB_PAT environment variable set
#
# Usage:
#   source("data-raw/release/upload_ratings_release.R")

# Setup
library(DBI)
library(arrow)
library(piggyback)
library(cli)
devtools::load_all()

# Configuration
REPO <- "peteowen1/bouncerdata"
TAG <- "ratings"
FORMATS <- c("t20", "odi", "test")

cli::cli_h1("Uploading Ratings to GitHub Release")
cli::cli_alert_info("Repo: {REPO}, Tag: {TAG}")

# Create temp directory for exports
export_dir <- tempdir()

# Open DB connection
conn <- get_db_connection(read_only = TRUE)

files_to_upload <- character(0)

for (fmt in FORMATS) {
  cli::cli_h2(toupper(fmt))

  # Export player_game_data
  pgd_table <- paste0(fmt, "_player_game_data")
  if (pgd_table %in% DBI::dbListTables(conn)) {
    pgd <- DBI::dbGetQuery(conn, sprintf("SELECT * FROM %s", pgd_table))
    if (nrow(pgd) > 0) {
      pgd_file <- file.path(export_dir, sprintf("%s_player_game_data.parquet", fmt))
      arrow::write_parquet(pgd, pgd_file)
      files_to_upload <- c(files_to_upload, pgd_file)
      cli::cli_alert_success("Exported {pgd_table}: {nrow(pgd)} rows")
    }
  }

  # Export stat_ratings
  sr_table <- paste0(fmt, "_stat_ratings")
  if (sr_table %in% DBI::dbListTables(conn)) {
    sr <- DBI::dbGetQuery(conn, sprintf("SELECT * FROM %s", sr_table))
    if (nrow(sr) > 0) {
      sr_file <- file.path(export_dir, sprintf("%s_stat_ratings.parquet", fmt))
      arrow::write_parquet(sr, sr_file)
      files_to_upload <- c(files_to_upload, sr_file)
      cli::cli_alert_success("Exported {sr_table}: {nrow(sr)} rows")
    }
  }
}

DBI::dbDisconnect(conn, shutdown = TRUE)

# Ensure release exists
cli::cli_h2("Uploading to GitHub")
tryCatch({
  piggyback::pb_release_create(repo = REPO, tag = TAG, .token = Sys.getenv("GITHUB_PAT"))
  cli::cli_alert_success("Created release '{TAG}'")
}, error = function(e) {
  cli::cli_alert_info("Release '{TAG}' already exists (or error: {e$message})")
})

# Upload files
for (f in files_to_upload) {
  fname <- basename(f)
  cli::cli_alert_info("Uploading {fname}...")
  tryCatch({
    piggyback::pb_upload(f, repo = REPO, tag = TAG, .token = Sys.getenv("GITHUB_PAT"),
                          overwrite = TRUE)
    cli::cli_alert_success("Uploaded {fname}")
  }, error = function(e) {
    cli::cli_alert_danger("Failed to upload {fname}: {e$message}")
  })
}

cli::cli_h1("Upload Complete")
cat(sprintf("Files uploaded: %d\n", length(files_to_upload)))
