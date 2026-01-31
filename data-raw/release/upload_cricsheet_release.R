#!/usr/bin/env Rscript
# upload_cricsheet_release.R - Upload Cricsheet data to GitHub release
#
# This script exports raw Cricsheet data from the local DuckDB database to
# parquet files and uploads them individually (plus ZIP) to the "cricsheet"
# GitHub release.
#
# Key features:
#   - Partitions by match_type × gender (12 partitions max)
#   - Uploads individual parquet files for remote loading
#   - Generates manifest.json for quick match_id lookup
#   - Includes ZIP for convenience bulk download
#
# Prerequisites:
#   - Database populated via install_all_bouncer_data()
#   - GITHUB_PAT environment variable set (for piggyback)
#   - Required packages: devtools, arrow, piggyback, cli, DBI, duckdb, jsonlite
#
# Usage:
#   source("data-raw/upload_cricsheet_release.R")
#   # OR: Rscript data-raw/upload_cricsheet_release.R

# =============================================================================
# Setup
# =============================================================================

library(DBI)
library(arrow)
library(piggyback)
library(cli)
library(jsonlite)

# Load bouncer package functions
devtools::load_all()

# =============================================================================
# Configuration
# =============================================================================

REPO <- "peteowen1/bouncerdata"
TAG <- "cricsheet"
ZIP_NAME <- "cricsheet-all.zip"

# Database path (uses bouncer's path resolution)
DB_PATH <- get_db_path()

# Partition dimensions (match_type × gender = 12 partitions max)
MATCH_TYPES <- c("Test", "ODI", "T20", "IT20", "MDM", "ODM")
GENDERS <- c("male", "female")

# =============================================================================
# Helper Functions
# =============================================================================

#' Export a table to parquet with zstd compression
export_parquet <- function(data, filepath) {
  arrow::write_parquet(data, filepath, compression = "zstd")
  cli::cli_alert_success("Exported {.file {basename(filepath)}} ({format(nrow(data), big.mark=',')} rows)")
}

#' Upload file to GitHub release with retry logic
upload_with_retry <- function(file, repo, tag, max_attempts = 3) {
  for (attempt in seq_len(max_attempts)) {
    success <- tryCatch({
      pb_upload(
        file = file,
        repo = repo,
        tag = tag,
        overwrite = TRUE
      )
      TRUE
    }, error = function(e) {
      cli::cli_alert_warning("Attempt {attempt}/{max_attempts} failed: {e$message}")
      FALSE
    })

    if (success) {
      cli::cli_alert_success("Uploaded {.file {basename(file)}}")
      return(TRUE)
    }

    if (attempt < max_attempts) {
      Sys.sleep(2)  # Brief pause before retry
    }
  }

  cli::cli_abort("Upload failed after {max_attempts} attempts: {.file {basename(file)}}")
}

#' Generate manifest.json with match_ids and partition metadata
generate_manifest <- function(conn, parquet_dir) {
  cli::cli_alert_info("Generating manifest.json...")

  # Get all match_ids
  match_ids <- DBI::dbGetQuery(conn, "SELECT match_id FROM matches ORDER BY match_id")$match_id

  # Get partition metadata
  partition_query <- "
    SELECT
      m.match_type,
      m.gender,
      COUNT(DISTINCT m.match_id) as match_count
    FROM matches m
    GROUP BY m.match_type, m.gender
  "
  partition_stats <- DBI::dbGetQuery(conn, partition_query)

  # Build partition info with file sizes
  partitions <- list()
  for (i in seq_len(nrow(partition_stats))) {
    mt <- partition_stats$match_type[i]
    gender <- partition_stats$gender[i]
    key <- paste(mt, gender, sep = "_")
    filepath <- file.path(parquet_dir, paste0("deliveries_", key, ".parquet"))

    file_size <- if (file.exists(filepath)) file.size(filepath) else 0

    partitions[[key]] <- list(
      match_count = partition_stats$match_count[i],
      file_size_bytes = file_size
    )
  }

  manifest <- list(
    updated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    match_count = length(match_ids),
    match_ids = match_ids,
    partitions = partitions
  )

  manifest_path <- file.path(parquet_dir, "manifest.json")
  write_json(manifest, manifest_path, auto_unbox = TRUE, pretty = TRUE)
  cli::cli_alert_success("Generated manifest.json ({length(match_ids)} match_ids)")

  manifest_path
}

# =============================================================================
# Main Script
# =============================================================================

cli::cli_h1("Cricsheet Data Upload")
cli::cli_alert_info("Partition scheme: match_type × gender (up to 12 partitions)")

# -----------------------------------------------------------------------------
# Step 1: Connect to database
# -----------------------------------------------------------------------------

cli::cli_h2("Connecting to database")
cli::cli_alert_info("Database: {.file {DB_PATH}}")

conn <- dbConnect(duckdb::duckdb(), dbdir = DB_PATH, read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

cli::cli_alert_success("Connected to DuckDB")

# -----------------------------------------------------------------------------
# Step 2: Create temp directory for parquet files
# -----------------------------------------------------------------------------

temp_dir <- tempfile("cricsheet_export_")
dir.create(temp_dir, recursive = TRUE)
cli::cli_alert_info("Temp directory: {.file {temp_dir}}")

# Track exported files for ZIP creation and upload
exported_files <- character()

# -----------------------------------------------------------------------------
# Step 3: Export matches table (unified, no partitioning)
# -----------------------------------------------------------------------------

cli::cli_h2("Exporting core tables")

matches <- dbGetQuery(conn, "SELECT * FROM matches")
matches_file <- file.path(temp_dir, "matches.parquet")
export_parquet(matches, matches_file)
exported_files <- c(exported_files, matches_file)
rm(matches)
gc()

# -----------------------------------------------------------------------------
# Step 4: Export players table (unified)
# -----------------------------------------------------------------------------

players <- dbGetQuery(conn, "SELECT * FROM players")
players_file <- file.path(temp_dir, "players.parquet")
export_parquet(players, players_file)
exported_files <- c(exported_files, players_file)
rm(players)
gc()

# -----------------------------------------------------------------------------
# Step 5: Export match_innings table (unified)
# -----------------------------------------------------------------------------

match_innings <- dbGetQuery(conn, "SELECT * FROM match_innings")
match_innings_file <- file.path(temp_dir, "match_innings.parquet")
export_parquet(match_innings, match_innings_file)
exported_files <- c(exported_files, match_innings_file)
rm(match_innings)
gc()

# -----------------------------------------------------------------------------
# Step 6: Export innings_powerplays table (unified)
# -----------------------------------------------------------------------------

innings_powerplays <- dbGetQuery(conn, "SELECT * FROM innings_powerplays")
powerplays_file <- file.path(temp_dir, "innings_powerplays.parquet")
export_parquet(innings_powerplays, powerplays_file)
exported_files <- c(exported_files, powerplays_file)
rm(innings_powerplays)
gc()

# -----------------------------------------------------------------------------
# Step 7: Export partitioned deliveries (match_type × gender)
# -----------------------------------------------------------------------------

cli::cli_h2("Exporting deliveries (partitioned by match_type × gender)")

partition_count <- 0
empty_count <- 0

for (match_type in MATCH_TYPES) {
  for (gender in GENDERS) {
    # Query for this partition (no team_type filter)
    query <- sprintf("
      SELECT d.*
      FROM deliveries d
      JOIN matches m ON d.match_id = m.match_id
      WHERE m.match_type = '%s'
        AND m.gender = '%s'
    ", match_type, gender)

    deliveries <- dbGetQuery(conn, query)

    # Skip empty partitions
    if (nrow(deliveries) == 0) {
      empty_count <- empty_count + 1
      next
    }

    # Export partition
    filename <- sprintf("deliveries_%s_%s.parquet", match_type, gender)
    filepath <- file.path(temp_dir, filename)
    export_parquet(deliveries, filepath)
    exported_files <- c(exported_files, filepath)

    partition_count <- partition_count + 1
    rm(deliveries)
    gc()
  }
}

cli::cli_alert_info("Exported {partition_count} delivery partitions ({empty_count} empty skipped)")

# -----------------------------------------------------------------------------
# Step 8: Generate manifest.json
# -----------------------------------------------------------------------------

cli::cli_h2("Generating manifest")
manifest_file <- generate_manifest(conn, temp_dir)
exported_files <- c(exported_files, manifest_file)

# -----------------------------------------------------------------------------
# Step 9: Create ZIP file for convenience download
# -----------------------------------------------------------------------------

cli::cli_h2("Creating ZIP archive")

zip_path <- file.path(temp_dir, ZIP_NAME)

# Get just the filenames for ZIP (not full paths)
files_to_zip <- basename(exported_files)

# Create ZIP from within temp directory
old_wd <- getwd()
setwd(temp_dir)
zip(ZIP_NAME, files = files_to_zip)
setwd(old_wd)

zip_size <- file.size(zip_path) / (1024^2)  # MB
cli::cli_alert_success("Created {.file {ZIP_NAME}} ({round(zip_size, 1)} MB)")

# Add ZIP to upload list
exported_files <- c(exported_files, zip_path)

# -----------------------------------------------------------------------------
# Step 10: Ensure release exists and upload all files
# -----------------------------------------------------------------------------

cli::cli_h2("Uploading to GitHub")

cli::cli_alert_info("Repository: {.val {REPO}}")
cli::cli_alert_info("Release tag: {.val {TAG}}")
cli::cli_alert_info("Files to upload: {length(exported_files)}")

# Create release if it doesn't exist
tryCatch({
  pb_release_create(repo = REPO, tag = TAG, name = "Cricsheet Raw Data")
  cli::cli_alert_success("Created release '{TAG}'")
}, error = function(e) {
  if (grepl("already exists", e$message, ignore.case = TRUE)) {
    cli::cli_alert_info("Release '{TAG}' already exists")
  } else {
    cli::cli_warn("Release check: {e$message}")
  }
})

# Upload all individual files
cli::cli_progress_bar("Uploading files", total = length(exported_files))

for (file_path in exported_files) {
  upload_with_retry(file_path, REPO, TAG)
  cli::cli_progress_update()
}

cli::cli_progress_done()

# -----------------------------------------------------------------------------
# Step 11: Cleanup
# -----------------------------------------------------------------------------

cli::cli_h2("Cleanup")

unlink(temp_dir, recursive = TRUE)
cli::cli_alert_success("Removed temp directory")

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------

cli::cli_h1("Upload Complete")
cli::cli_alert_success("Uploaded {length(exported_files)} files to {.val {REPO}} release '{TAG}'")
cli::cli_alert_info("Files: manifest.json, matches.parquet, players.parquet, match_innings.parquet,")
cli::cli_alert_info("       innings_powerplays.parquet, {partition_count} deliveries_*.parquet, {ZIP_NAME}")
cli::cli_alert_info("")
cli::cli_alert_info("Verify with: {.code piggyback::pb_list(repo = \"{REPO}\", tag = \"{TAG}\")}")
