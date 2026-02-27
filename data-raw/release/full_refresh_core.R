#!/usr/bin/env Rscript
# Full Refresh Core Release ----
#
# This script:
#   1. Downloads ALL Cricsheet data (not just last 7 days)
#   2. Builds the DuckDB database
#   3. Exports to partitioned parquet files (match_type x gender x team_type)
#   4. Uploads to GitHub "core" release
#
# WARNING: This downloads ~500MB+ and takes 30-60 minutes!
#
# Usage:
#   Rscript full_refresh_core.R
#
# Requirements:
#   - bouncer package installed
#   - GitHub PAT with repo access
#   - ~2GB free disk space
#   - Good internet connection

devtools::load_all()
library(arrow)
library(dplyr)
library(piggyback)
library(cli)
library(DBI)

# Configuration ----

REPO <- "peteowen1/bouncerdata"

# Data directories
# Note: This script lives in bouncer/data-raw/, so bouncerdata is at ../../bouncerdata/
DATA_DIR <- normalizePath("../../bouncerdata/", mustWork = FALSE)
PARQUET_DIR <- file.path(DATA_DIR, "parquet")

# Partition configuration (match_type x gender x team_type)
MATCH_TYPES <- c("Test", "ODI", "T20", "IT20", "MDM", "ODM")
GENDERS <- c("male", "female")
TEAM_TYPES <- c("international", "club")

# Step 1: Download Cricsheet Data ----

cli_h1("Full Refresh of Core Release")
cli_alert_warning("This will download ALL Cricsheet data and rebuild from scratch!")
cli_alert_info("Estimated time: 30-60 minutes")

cli_h2("Step 1: Download Cricsheet data")
cli_alert_info("This downloads ALL historical match data...")

# Use bouncer's built-in function to download and build database
install_all_bouncer_data(fresh = TRUE)

cli_alert_success("Database built successfully!")

# Step 2: Export to Parquet ----

cli_h2("Step 2: Export to partitioned parquet files")

dir.create(PARQUET_DIR, showWarnings = FALSE, recursive = TRUE)

# Connect to database (use bouncer's path resolution)
db_path <- get_db_path()
conn <- dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

## Export matches ----
cli_alert_info("Exporting matches...")
matches <- dbGetQuery(conn, "SELECT * FROM cricsheet.matches")
write_parquet(matches, file.path(PARQUET_DIR, "matches.parquet"), compression = "zstd")
cli_alert_success("  matches.parquet: {nrow(matches)} rows")
rm(matches)
gc()

## Export players ----
cli_alert_info("Exporting players...")
players <- dbGetQuery(conn, "SELECT * FROM cricsheet.players")
write_parquet(players, file.path(PARQUET_DIR, "players.parquet"), compression = "zstd")
cli_alert_success("  players.parquet: {nrow(players)} rows")
rm(players)
gc()

## Export deliveries by partition ----
cli_alert_info("Exporting deliveries by partition...")

# Get match metadata for partitioning
match_lookup <- dbGetQuery(conn, "
  SELECT match_id, match_type, gender, team_type
  FROM cricsheet.matches
")

# Process each partition
partition_counts <- list()

for (mt in MATCH_TYPES) {
  for (gender in GENDERS) {
    for (team_type in TEAM_TYPES) {
      partition_key <- paste(mt, gender, team_type, sep = "_")

      # Get match_ids for this partition
      match_ids <- match_lookup$match_id[
        match_lookup$match_type == mt &
        match_lookup$gender == gender &
        match_lookup$team_type == team_type
      ]

      if (length(match_ids) == 0) next

      # Query deliveries for these matches
      # Use parameterized query for safety
      placeholders <- paste(rep("?", length(match_ids)), collapse = ", ")
      query <- sprintf("SELECT * FROM cricsheet.deliveries WHERE match_id IN (%s)", placeholders)

      deliveries <- dbGetQuery(conn, query, params = as.list(match_ids))

      if (nrow(deliveries) == 0) next

      # Write partition file
      filename <- paste0("deliveries_", partition_key, ".parquet")
      write_parquet(deliveries, file.path(PARQUET_DIR, filename), compression = "zstd")

      partition_counts[[partition_key]] <- nrow(deliveries)
      cli_alert_success("  {partition_key}: {nrow(deliveries)} rows")

      rm(deliveries)
      gc()
    }
  }
}

rm(match_lookup)
gc()

# Summary of partitions
cli_alert_info("Created {length(partition_counts)} partition files")
total_deliveries <- sum(unlist(partition_counts))
cli_alert_success("Total deliveries: {format(total_deliveries, big.mark = ',')}")

# Step 3: Upload to GitHub ----

cli_h2("Step 3: Upload to GitHub")

parquet_files <- list.files(PARQUET_DIR, pattern = "\\.parquet$", full.names = TRUE)
total_size <- sum(file.size(parquet_files)) / 1024 / 1024
cli_alert_info("Found {length(parquet_files)} parquet files ({round(total_size, 1)} MB)")

# Create ZIP
zip_file <- tempfile(fileext = ".zip")
old_wd <- getwd()
setwd(PARQUET_DIR)
zip(zip_file, files = basename(parquet_files), flags = "-rq")
setwd(old_wd)

zip_size <- file.size(zip_file) / 1024 / 1024
cli_alert_success("Created ZIP: {round(zip_size, 1)} MB")

# Ensure release exists
tryCatch({
  pb_release_create(repo = REPO, tag = "core")
  cli_alert_info("Created 'core' release")
  Sys.sleep(2)
}, error = function(e) {
  cli_alert_info("Release 'core' already exists")
})

# Upload with retry
cli_alert_info("Uploading to GitHub...")
success <- FALSE
for (attempt in 1:3) {
  success <- tryCatch({
    pb_upload(
      file = zip_file,
      repo = REPO,
      tag = "core",
      name = "bouncerdata-parquet.zip",
      overwrite = TRUE
    )
    TRUE
  }, error = function(e) {
    if (attempt < 3) {
      cli_alert_warning("Attempt {attempt} failed, retrying...")
      Sys.sleep(5)
    }
    FALSE
  })
  if (success) break
}

file.remove(zip_file)

# Summary ----

cli_h1("Complete!")

if (success) {
  cli_alert_success("Full refresh completed successfully!")
  cli_alert_info("Release: https://github.com/{REPO}/releases/tag/core")
  cli_alert_info("Partitions: {length(partition_counts)}")
  cli_alert_info("Total deliveries: {format(total_deliveries, big.mark = ',')}")
  cli_alert_info("ZIP size: {round(zip_size, 1)} MB")
} else {
  cli_alert_danger("Upload failed - parquet files are still in {PARQUET_DIR}")
  cli_alert_info("You can manually upload using upload_core_release.R")
}
