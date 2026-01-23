#!/usr/bin/env Rscript
# daily_scrape.R - Daily Cricsheet sync and parquet upload for bouncerdata
#
# This script runs on Oracle Cloud VM to:
# 1. Check Cricsheet for new data (ETag caching)
# 2. Download existing parquets (as ZIP, like panna)
# 3. Parse new JSON files
# 4. Merge with existing data (partitioned by team_type for memory efficiency)
# 5. Upload combined parquets to GitHub Release
#
# Key design: Deliveries are partitioned by match_type (Test, ODI, T20, etc.)
# Each partition is ~10-15MB instead of one 58MB file - fits in VM memory.
#
# Prerequisites:
# - bouncer package installed (duckdb NOT required)
# - arrow package installed
# - GITHUB_PAT environment variable set
#
# Usage: Run via cron at 7 AM UTC daily
#   0 7 * * * /home/opc/bouncer-scraper/run_scrape.sh

devtools::load_all()
library(arrow)
library(cli)
library(httr2)
library(jsonlite)
library(piggyback)
library(dplyr)

# ============================================================
# CONFIGURATION
# ============================================================

REPO <- "peteowen1/bouncerdata"
DATA_DIR <- Sys.getenv("BOUNCER_DATA_DIR", "~/bouncer-scraper/data")
PARQUET_DIR <- file.path(DATA_DIR, "parquet")
JSON_DIR <- file.path(DATA_DIR, "json_files")
ETAG_CACHE <- file.path(DATA_DIR, "etag_cache.json")

# Cricsheet URLs
CRICSHEET_BASE <- "https://cricsheet.org/downloads"
CRICSHEET_RECENT_JSON <- paste0(CRICSHEET_BASE, "/recently_added_7_json.zip")

# Match types for partitioning deliveries (like panna partitions by league)
MATCH_TYPES <- c("Test", "ODI", "T20", "IT20", "MDM", "ODM")

# ============================================================
# HELPER FUNCTIONS
# ============================================================

#' Check if Cricsheet has new data using ETag
check_cricsheet_updates <- function() {
  cli_h2("Checking Cricsheet for updates")

  old_etag <- NULL
  if (file.exists(ETAG_CACHE)) {
    cache <- fromJSON(ETAG_CACHE)
    old_etag <- cache$etag
    cli_alert_info("Cached ETag: {old_etag}")
  }

  resp <- request(CRICSHEET_RECENT_JSON) |>
    req_method("HEAD") |>
    req_timeout(30) |>
    req_perform()

  new_etag <- resp_header(resp, "ETag")
  if (is.null(new_etag)) new_etag <- ""
  cli_alert_info("Current ETag: {new_etag}")

  old_etag_str <- if (is.null(old_etag)) "" else old_etag
  if (nchar(old_etag_str) > 0 && nchar(new_etag) > 0 && old_etag_str == new_etag) {
    cli_alert_success("No new data available")
    return(FALSE)
  }

  cli_alert_info("New data available!")
  write_json(list(etag = new_etag, checked_at = Sys.time()), ETAG_CACHE, auto_unbox = TRUE)
  return(TRUE)
}

#' Download existing parquets as ZIP (like panna - memory efficient)
download_existing_parquets <- function() {
  cli_h2("Downloading existing data from GitHub")

  dir.create(PARQUET_DIR, showWarnings = FALSE, recursive = TRUE)
  temp_dir <- tempdir()
  zip_file <- file.path(temp_dir, "bouncerdata-parquet.zip")

  # Try to download ZIP
  tryCatch({
    pb_download(
      file = "bouncerdata-parquet.zip",
      repo = REPO,
      tag = "core",
      dest = temp_dir,
      overwrite = TRUE
    )

    if (file.exists(zip_file)) {
      size_mb <- file.size(zip_file) / 1024 / 1024
      cli_alert_success("Downloaded ZIP: {round(size_mb, 1)} MB")

      # Extract to parquet dir
      unzip(zip_file, exdir = PARQUET_DIR, overwrite = TRUE)
      file.remove(zip_file)

      n_files <- length(list.files(PARQUET_DIR, pattern = "\\.parquet$"))
      cli_alert_success("Extracted {n_files} parquet files")
    }
  }, error = function(e) {
    cli_alert_warning("No existing data found (first run?): {e$message}")
  })
}

#' Get existing match IDs (memory-efficient - only reads ID column)
get_existing_match_ids <- function() {
  matches_path <- file.path(PARQUET_DIR, "matches.parquet")

  if (!file.exists(matches_path)) {
    cli_alert_info("No existing matches.parquet - all matches are new")
    return(character(0))
  }

  existing_ids <- tryCatch({
    df <- read_parquet(matches_path, col_select = "match_id")
    unique(df$match_id)
  }, error = function(e) {
    cli_alert_warning("Could not read existing match IDs: {e$message}")
    character(0)
  })

  cli_alert_info("Found {length(existing_ids)} existing matches")
  existing_ids
}

#' Download and extract Cricsheet data
download_cricsheet_data <- function() {
  cli_h2("Downloading Cricsheet data")

  if (dir.exists(JSON_DIR)) {
    old_files <- list.files(JSON_DIR, pattern = "\\.json$", full.names = TRUE, recursive = TRUE)
    if (length(old_files) > 0) {
      cli_alert_info("Removing {length(old_files)} old JSON files...")
      file.remove(old_files)
    }
  }
  dir.create(JSON_DIR, showWarnings = FALSE, recursive = TRUE)

  zip_path <- tempfile(fileext = ".zip")
  cli_alert_info("Downloading recently_added_7_json.zip...")

  resp <- request(CRICSHEET_RECENT_JSON) |>
    req_timeout(600) |>
    req_perform(path = zip_path)

  size_mb <- file.size(zip_path) / 1024 / 1024
  cli_alert_success("Downloaded: {round(size_mb, 1)} MB")

  cli_alert_info("Extracting...")
  unzip(zip_path, exdir = JSON_DIR, overwrite = TRUE)
  file.remove(zip_path)

  n_files <- length(list.files(JSON_DIR, pattern = "\\.json$", recursive = TRUE))
  cli_alert_success("Extracted {n_files} JSON files")

  return(JSON_DIR)
}

#' Parse all JSON files to data frames
parse_all_matches <- function(json_dir) {
  cli_h2("Parsing JSON files")

  json_files <- list.files(json_dir, pattern = "\\.json$",
                           full.names = TRUE, recursive = TRUE)

  cli_alert_info("Found {length(json_files)} JSON files to parse")

  all_matches <- vector("list", length(json_files))
  all_deliveries <- vector("list", length(json_files))
  all_players <- vector("list", length(json_files))

  pb <- cli_progress_bar("Parsing", total = length(json_files))

  for (i in seq_along(json_files)) {
    tryCatch({
      parsed <- parse_cricsheet_json(json_files[i])
      all_matches[[i]] <- parsed$match_info
      all_deliveries[[i]] <- parsed$deliveries
      all_players[[i]] <- parsed$players
    }, error = function(e) {
      cli_alert_warning("Failed to parse {basename(json_files[i])}: {e$message}")
    })
    cli_progress_update(id = pb)
  }

  cli_progress_done(id = pb)

  cli_alert_info("Combining data frames...")

  matches_df <- do.call(rbind, Filter(function(x) nrow(x) > 0, all_matches))
  deliveries_df <- do.call(rbind, Filter(function(x) nrow(x) > 0, all_deliveries))
  players_df <- do.call(rbind, Filter(function(x) nrow(x) > 0, all_players))

  players_df <- players_df[!duplicated(players_df$player_id), ]

  cli_alert_success("Parsed: {nrow(matches_df)} matches, {nrow(deliveries_df)} deliveries, {nrow(players_df)} players")

  list(matches = matches_df, deliveries = deliveries_df, players = players_df)
}

#' Merge and write parquets (partitioned deliveries for memory efficiency)
merge_and_write_parquets <- function(new_data, new_match_ids) {
  cli_h2("Merging with existing data")

  if (length(new_match_ids) == 0) {
    cli_alert_info("No new matches to merge")
    return(invisible(FALSE))
  }

  # Filter to new data only
  new_matches <- new_data$matches[new_data$matches$match_id %in% new_match_ids, ]
  new_deliveries <- new_data$deliveries[new_data$deliveries$match_id %in% new_match_ids, ]

  # Add match_type to deliveries (for partitioning)
  new_deliveries <- merge(
    new_deliveries,
    new_data$matches[, c("match_id", "match_type")],
    by = "match_id",
    all.x = TRUE
  )

  # Get new players
  players_path <- file.path(PARQUET_DIR, "players.parquet")
  existing_player_ids <- if (file.exists(players_path)) {
    tryCatch(unique(read_parquet(players_path, col_select = "player_id")$player_id), error = function(e) character(0))
  } else character(0)
  new_player_ids <- setdiff(new_data$players$player_id, existing_player_ids)
  new_players <- new_data$players[new_data$players$player_id %in% new_player_ids, ]

  cli_alert_info("New data: {nrow(new_matches)} matches, {nrow(new_deliveries)} deliveries, {nrow(new_players)} players")

  # === MATCHES (small file, can load fully) ===
  matches_path <- file.path(PARQUET_DIR, "matches.parquet")
  if (file.exists(matches_path)) {
    existing <- read_parquet(matches_path)
    combined <- bind_rows(existing, new_matches)
    rm(existing)
  } else {
    combined <- new_matches
  }
  write_parquet(combined, matches_path, compression = "zstd")
  cli_alert_success("  matches.parquet: {nrow(combined)} total rows")
  rm(combined); gc()

  # === DELIVERIES (partitioned by match_type for memory efficiency) ===
  cli_alert_info("  Processing deliveries by match_type...")

  total_deliveries <- 0
  for (mt in MATCH_TYPES) {
    mt_path <- file.path(PARQUET_DIR, paste0("deliveries_", mt, ".parquet"))
    mt_new <- new_deliveries[new_deliveries$match_type == mt, ]

    if (file.exists(mt_path)) {
      existing <- read_parquet(mt_path)
      combined <- bind_rows(existing, mt_new)
      rm(existing)
    } else {
      combined <- mt_new
    }

    if (nrow(combined) > 0) {
      write_parquet(combined, mt_path, compression = "zstd")
      total_deliveries <- total_deliveries + nrow(combined)
      cli_alert_success("    deliveries_{mt}.parquet: {nrow(combined)} rows")
    }
    rm(combined); gc()
  }
  cli_alert_success("  Total deliveries: {total_deliveries} rows")

  # === PLAYERS (small file, can load fully) ===
  if (file.exists(players_path)) {
    existing <- read_parquet(players_path)
    combined <- bind_rows(existing, new_players)
    rm(existing)
  } else {
    combined <- new_players
  }
  write_parquet(combined, players_path, compression = "zstd")
  cli_alert_success("  players.parquet: {nrow(combined)} total rows")
  rm(combined); gc()

  invisible(TRUE)
}

#' Upload parquets as ZIP to GitHub (like panna)
upload_to_github <- function() {
  cli_h2("Uploading to GitHub")

  # Find all parquet files
  parquet_files <- list.files(PARQUET_DIR, pattern = "\\.parquet$", full.names = TRUE)

  if (length(parquet_files) == 0) {
    cli_alert_warning("No parquet files found to upload")
    return(invisible(FALSE))
  }

  total_size <- sum(file.size(parquet_files)) / 1024 / 1024
  cli_alert_info("Found {length(parquet_files)} parquet files ({round(total_size, 1)} MB total)")

  # Create ZIP
  temp_dir <- tempdir()
  zip_file <- file.path(temp_dir, "bouncerdata-parquet.zip")
  if (file.exists(zip_file)) file.remove(zip_file)

  cli_alert_info("Creating ZIP...")
  old_wd <- getwd()
  on.exit(setwd(old_wd), add = TRUE)
  setwd(PARQUET_DIR)

  rel_files <- basename(parquet_files)
  zip(zip_file, files = rel_files, flags = "-rq")

  zip_size <- file.size(zip_file) / 1024 / 1024
  cli_alert_success("Created ZIP: {round(zip_size, 1)} MB")

  # Ensure release exists
  tryCatch({
    pb_release_create(repo = REPO, tag = "core")
    cli_alert_info("Created 'core' release")
    Sys.sleep(3)
  }, error = function(e) {
    cli_alert_info("Release 'core' already exists")
  })

  # Upload with retry
  cli_alert_info("Uploading to GitHub...")
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
        cli_alert_warning("Attempt {attempt} failed, retrying in 3s...")
        Sys.sleep(3)
      }
      FALSE
    })
    if (success) break
  }

  if (success) {
    cli_alert_success("Upload complete!")
    cli_alert_info("Release: https://github.com/{REPO}/releases/tag/core")
  } else {
    cli_alert_danger("Upload failed after 3 attempts")
  }

  invisible(success)
}

# ============================================================
# MAIN EXECUTION
# ============================================================

cli_h1("Bouncer Daily Scrape")
cli_alert_info("Started at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}")
cli_alert_info("Using: recently_added_7_json (added in last 7 days)")
cli_alert_info("Partitioning: deliveries by match_type (like panna by league)")

dir.create(DATA_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(PARQUET_DIR, showWarnings = FALSE, recursive = TRUE)

# Step 1: Check for updates
has_updates <- check_cricsheet_updates()

if (!has_updates) {
  cli_alert_success("No updates needed. Exiting.")
  quit(save = "no", status = 0)
}

# Step 2: Download existing parquets as ZIP (like panna)
download_existing_parquets()

# Step 3: Get existing match IDs (memory-efficient)
existing_match_ids <- get_existing_match_ids()

# Step 4: Download new JSON data
json_dir <- download_cricsheet_data()

# Step 5: Parse JSON to data frames
new_data <- parse_all_matches(json_dir)

# Step 6: Identify new matches
new_match_ids <- setdiff(new_data$matches$match_id, existing_match_ids)
cli_alert_info("Found {length(new_match_ids)} new matches to add")

if (length(new_match_ids) == 0) {
  cli_alert_success("All matches already in database. Nothing to upload.")
  quit(save = "no", status = 0)
}

# Step 7: Merge with existing (partitioned for memory efficiency)
merge_and_write_parquets(new_data, new_match_ids)

# Step 8: Upload as ZIP
upload_to_github()

cli_h1("Complete!")
cli_alert_success("Finished at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}")
