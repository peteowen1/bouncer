#!/usr/bin/env Rscript
# daily_scrape_cricsheet.R - Optimized VM scraper for cricsheet release
#
# Key optimizations for 1GB VM:
# 1. Downloads manifest.json (~100KB) for existing match_ids instead of full ZIP
# 2. Only downloads recently_added_7_json.zip from Cricsheet
# 3. Downloads only affected partition files (not full dataset)
# 4. Merges new data using Arrow concat_tables (memory efficient)
# 5. Uploads individual changed files + updated manifest
#
# Partition scheme: match_type × gender (12 partitions max)
#
# Usage: Run via cron at 7 AM UTC daily
#   0 7 * * * /home/opc/bouncer-scraper/run_cricsheet_scrape.sh

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
TAG <- "cricsheet"
DATA_DIR <- Sys.getenv("BOUNCER_DATA_DIR", "~/bouncer-scraper/cricsheet_data")
PARQUET_DIR <- file.path(DATA_DIR, "parquet")
JSON_DIR <- file.path(DATA_DIR, "json_files")
TEMP_DIR <- file.path(DATA_DIR, "temp")
ETAG_CACHE <- file.path(DATA_DIR, "etag_cache.json")

CRICSHEET_BASE <- "https://cricsheet.org/downloads"
CRICSHEET_RECENT_JSON <- paste0(CRICSHEET_BASE, "/recently_added_7_json.zip")

# Partition dimensions
MATCH_TYPES <- c("Test", "ODI", "T20", "IT20", "MDM", "ODM")
GENDERS <- c("male", "female")

# ============================================================
# HELPER FUNCTIONS
# ============================================================

#' Create partition key from match attributes
make_partition_key <- function(match_type, gender) {
  paste(match_type, gender, sep = "_")
}

#' Get deliveries filename for a partition
deliveries_filename <- function(partition_key) {
  paste0("deliveries_", partition_key, ".parquet")
}

#' Check Cricsheet for updates via ETag
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

#' Download manifest.json from release to get existing match_ids
download_manifest <- function() {
  cli_h2("Downloading manifest from GitHub release")

  manifest_url <- sprintf(
    "https://github.com/%s/releases/download/%s/manifest.json",
    REPO, TAG
  )

  manifest <- tryCatch({
    resp <- request(manifest_url) |>
      req_timeout(30) |>
      req_perform()

    fromJSON(resp_body_string(resp), simplifyVector = TRUE)
  }, error = function(e) {
    cli_alert_warning("Could not download manifest (first run?): {e$message}")
    list(match_ids = character(0), partitions = list())
  })

  cli_alert_success("Found {length(manifest$match_ids)} existing matches in manifest")
  gc()
  manifest
}

#' Download specific partition files from GitHub release
download_partition_files <- function(partition_keys) {
  cli_h2("Downloading affected partition files")

  dir.create(PARQUET_DIR, showWarnings = FALSE, recursive = TRUE)

  for (key in partition_keys) {
    filename <- deliveries_filename(key)
    url <- sprintf("https://github.com/%s/releases/download/%s/%s", REPO, TAG, filename)
    dest <- file.path(PARQUET_DIR, filename)

    tryCatch({
      cli_alert_info("  Downloading {filename}...")
      resp <- request(url) |>
        req_timeout(300) |>
        req_perform(path = dest)
      cli_alert_success("  Downloaded {filename}")
    }, error = function(e) {
      cli_alert_warning("  Could not download {filename}: {e$message}")
    })

    gc()
  }
}

#' Download core parquet files (matches, players, etc.)
download_core_files <- function() {
  cli_h2("Downloading core parquet files")

  dir.create(PARQUET_DIR, showWarnings = FALSE, recursive = TRUE)

  core_files <- c("matches.parquet", "players.parquet", "match_innings.parquet", "innings_powerplays.parquet")

  for (filename in core_files) {
    url <- sprintf("https://github.com/%s/releases/download/%s/%s", REPO, TAG, filename)
    dest <- file.path(PARQUET_DIR, filename)

    tryCatch({
      cli_alert_info("  Downloading {filename}...")
      resp <- request(url) |>
        req_timeout(300) |>
        req_perform(path = dest)
      cli_alert_success("  Downloaded {filename}")
    }, error = function(e) {
      cli_alert_warning("  Could not download {filename} (may not exist): {e$message}")
    })

    gc()
  }
}

#' Download Cricsheet recently added data
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

  gc()
  return(JSON_DIR)
}

#' Parse JSON files and identify new matches
parse_new_matches <- function(json_dir, existing_match_ids) {
  cli_h2("Parsing JSON files")

  dir.create(TEMP_DIR, showWarnings = FALSE, recursive = TRUE)

  json_files <- list.files(json_dir, pattern = "\\.json$",
                           full.names = TRUE, recursive = TRUE)

  cli_alert_info("Found {length(json_files)} JSON files to parse")

  # Clean old temp files
  old_temps <- list.files(TEMP_DIR, full.names = TRUE)
  if (length(old_temps) > 0) file.remove(old_temps)

  # Accumulators
  all_matches <- list()
  all_players <- list()
  all_innings <- list()
  all_powerplays <- list()
  deliveries_by_partition <- list()  # Named list, keys are partition keys

  partition_counts <- list()
  n_new <- 0
  n_skipped <- 0

  pb <- cli_progress_bar("Parsing", total = length(json_files))

  for (i in seq_along(json_files)) {
    tryCatch({
      parsed <- parse_cricsheet_json(json_files[i])

      # Skip if match already exists
      if (parsed$match_info$match_id %in% existing_match_ids) {
        n_skipped <- n_skipped + 1
        cli_progress_update(id = pb)
        next
      }

      n_new <- n_new + 1

      # Get partition key (match_type × gender)
      mt <- parsed$match_info$match_type
      gender <- parsed$match_info$gender

      if (!mt %in% MATCH_TYPES) mt <- "Other"
      if (!gender %in% GENDERS) gender <- "unknown"

      partition_key <- make_partition_key(mt, gender)

      # Track partition counts
      if (is.null(partition_counts[[partition_key]])) {
        partition_counts[[partition_key]] <- 0L
      }
      partition_counts[[partition_key]] <- partition_counts[[partition_key]] + 1L

      # Accumulate data
      all_matches[[length(all_matches) + 1]] <- parsed$match_info
      all_players[[length(all_players) + 1]] <- parsed$players

      if (nrow(parsed$innings) > 0) {
        all_innings[[length(all_innings) + 1]] <- parsed$innings
      }

      if (nrow(parsed$powerplays) > 0) {
        all_powerplays[[length(all_powerplays) + 1]] <- parsed$powerplays
      }

      if (nrow(parsed$deliveries) > 0) {
        if (is.null(deliveries_by_partition[[partition_key]])) {
          deliveries_by_partition[[partition_key]] <- list()
        }
        deliveries_by_partition[[partition_key]][[length(deliveries_by_partition[[partition_key]]) + 1]] <- parsed$deliveries
      }

    }, error = function(e) {
      cli_alert_warning("Failed to parse {basename(json_files[i])}: {e$message}")
    })

    cli_progress_update(id = pb)
  }

  cli_progress_done(id = pb)

  cli_alert_success("Parsed {n_new} new matches, skipped {n_skipped} existing")

  if (n_new > 0 && length(partition_counts) > 0) {
    sorted_partitions <- partition_counts[order(-unlist(partition_counts))]
    partition_summary <- paste(
      names(sorted_partitions),
      unlist(sorted_partitions),
      sep = ": ",
      collapse = ", "
    )
    cli_alert_info("Partition breakdown: {partition_summary}")
  }

  gc()

  list(
    n_new = n_new,
    partition_counts = partition_counts,
    all_matches = all_matches,
    all_players = all_players,
    all_innings = all_innings,
    all_powerplays = all_powerplays,
    deliveries_by_partition = deliveries_by_partition
  )
}

#' Merge new data with existing parquets (Arrow concat for memory efficiency)
merge_with_existing <- function(parsed_data) {
  cli_h2("Merging with existing data")

  if (parsed_data$n_new == 0) {
    cli_alert_info("No new matches to merge")
    return(invisible(FALSE))
  }

  # === MATCHES ===
  if (length(parsed_data$all_matches) > 0) {
    matches_path <- file.path(PARQUET_DIR, "matches.parquet")
    new_matches <- do.call(rbind, parsed_data$all_matches)

    if (file.exists(matches_path)) {
      existing_tbl <- read_parquet(matches_path, as_data_frame = FALSE)
      new_tbl <- arrow_table(new_matches)
      combined_tbl <- concat_tables(existing_tbl, new_tbl)
      write_parquet(combined_tbl, matches_path, compression = "zstd")
      cli_alert_success("  matches.parquet: {combined_tbl$num_rows} total rows")
      rm(existing_tbl, new_tbl, combined_tbl)
    } else {
      write_parquet(new_matches, matches_path, compression = "zstd")
      cli_alert_success("  matches.parquet: {nrow(new_matches)} rows (new file)")
    }
    rm(new_matches)
    gc()
  }

  # === MATCH INNINGS ===
  if (length(parsed_data$all_innings) > 0) {
    innings_path <- file.path(PARQUET_DIR, "match_innings.parquet")
    new_innings <- do.call(rbind, parsed_data$all_innings)

    if (file.exists(innings_path)) {
      existing_tbl <- read_parquet(innings_path, as_data_frame = FALSE)
      new_tbl <- arrow_table(new_innings)
      combined_tbl <- concat_tables(existing_tbl, new_tbl)
      write_parquet(combined_tbl, innings_path, compression = "zstd")
      cli_alert_success("  match_innings.parquet: {combined_tbl$num_rows} total rows")
      rm(existing_tbl, new_tbl, combined_tbl)
    } else {
      write_parquet(new_innings, innings_path, compression = "zstd")
      cli_alert_success("  match_innings.parquet: {nrow(new_innings)} rows (new file)")
    }
    rm(new_innings)
    gc()
  }

  # === POWERPLAYS ===
  if (length(parsed_data$all_powerplays) > 0) {
    powerplays_path <- file.path(PARQUET_DIR, "innings_powerplays.parquet")
    new_powerplays <- do.call(rbind, parsed_data$all_powerplays)

    if (file.exists(powerplays_path)) {
      existing_tbl <- read_parquet(powerplays_path, as_data_frame = FALSE)
      new_tbl <- arrow_table(new_powerplays)
      combined_tbl <- concat_tables(existing_tbl, new_tbl)
      write_parquet(combined_tbl, powerplays_path, compression = "zstd")
      cli_alert_success("  innings_powerplays.parquet: {combined_tbl$num_rows} total rows")
      rm(existing_tbl, new_tbl, combined_tbl)
    } else {
      write_parquet(new_powerplays, powerplays_path, compression = "zstd")
      cli_alert_success("  innings_powerplays.parquet: {nrow(new_powerplays)} rows (new file)")
    }
    rm(new_powerplays)
    gc()
  }

  # === DELIVERIES (per partition) ===
  cli_alert_info("Processing deliveries by partition...")

  for (pk in names(parsed_data$deliveries_by_partition)) {
    deliveries_list <- parsed_data$deliveries_by_partition[[pk]]
    if (length(deliveries_list) == 0) next

    new_del <- do.call(rbind, deliveries_list)
    final_path <- file.path(PARQUET_DIR, deliveries_filename(pk))

    if (file.exists(final_path)) {
      existing_tbl <- read_parquet(final_path, as_data_frame = FALSE)
      new_tbl <- arrow_table(new_del)
      combined_tbl <- concat_tables(existing_tbl, new_tbl)
      write_parquet(combined_tbl, final_path, compression = "zstd")
      cli_alert_success("    {pk}: {combined_tbl$num_rows} rows")
      rm(existing_tbl, combined_tbl)
    } else {
      write_parquet(new_del, final_path, compression = "zstd")
      cli_alert_success("    {pk}: {nrow(new_del)} rows (new)")
    }
    rm(new_del)
    gc()
  }

  # === PLAYERS ===
  if (length(parsed_data$all_players) > 0) {
    players_path <- file.path(PARQUET_DIR, "players.parquet")
    new_players <- do.call(rbind, parsed_data$all_players)
    new_players <- new_players[!duplicated(new_players$player_id), ]

    if (file.exists(players_path)) {
      existing_ids <- read_parquet(players_path, col_select = "player_id")$player_id
      new_players <- new_players[!new_players$player_id %in% existing_ids, ]
      rm(existing_ids)

      if (nrow(new_players) > 0) {
        existing_tbl <- read_parquet(players_path, as_data_frame = FALSE)
        new_tbl <- arrow_table(new_players)
        combined_tbl <- concat_tables(existing_tbl, new_tbl)
        write_parquet(combined_tbl, players_path, compression = "zstd")
        cli_alert_success("  players.parquet: {combined_tbl$num_rows} total")
        rm(existing_tbl, new_tbl, combined_tbl)
      } else {
        cli_alert_info("  players.parquet: no new players")
      }
    } else {
      write_parquet(new_players, players_path, compression = "zstd")
      cli_alert_success("  players.parquet: {nrow(new_players)} rows (new file)")
    }
    rm(new_players)
    gc()
  }

  # Cleanup temp dir
  unlink(TEMP_DIR, recursive = TRUE)

  invisible(TRUE)
}

#' Generate updated manifest
generate_updated_manifest <- function() {
  cli_h2("Generating updated manifest")

  matches_path <- file.path(PARQUET_DIR, "matches.parquet")
  if (!file.exists(matches_path)) {
    cli_alert_danger("matches.parquet not found!")
    return(NULL)
  }

  matches <- read_parquet(matches_path, col_select = c("match_id", "match_type", "gender"))
  match_ids <- sort(unique(matches$match_id))

  # Build partition info
  partitions <- list()
  for (mt in unique(matches$match_type)) {
    for (gender in unique(matches$gender[matches$match_type == mt])) {
      key <- make_partition_key(mt, gender)
      count <- sum(matches$match_type == mt & matches$gender == gender)
      filepath <- file.path(PARQUET_DIR, deliveries_filename(key))
      file_size <- if (file.exists(filepath)) file.size(filepath) else 0

      partitions[[key]] <- list(
        match_count = count,
        file_size_bytes = file_size
      )
    }
  }

  manifest <- list(
    updated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    match_count = length(match_ids),
    match_ids = match_ids,
    partitions = partitions
  )

  manifest_path <- file.path(PARQUET_DIR, "manifest.json")
  write_json(manifest, manifest_path, auto_unbox = TRUE, pretty = TRUE)

  cli_alert_success("Generated manifest.json ({length(match_ids)} match_ids)")

  rm(matches)
  gc()

  manifest_path
}

#' Upload changed files to GitHub release
upload_to_github <- function(changed_partitions) {
  cli_h2("Uploading to GitHub")

  # Files to upload: manifest, matches, players, match_innings, innings_powerplays, changed deliveries
  files_to_upload <- c(
    file.path(PARQUET_DIR, "manifest.json"),
    file.path(PARQUET_DIR, "matches.parquet"),
    file.path(PARQUET_DIR, "players.parquet"),
    file.path(PARQUET_DIR, "match_innings.parquet"),
    file.path(PARQUET_DIR, "innings_powerplays.parquet")
  )

  # Add changed delivery partitions
  for (pk in changed_partitions) {
    files_to_upload <- c(files_to_upload, file.path(PARQUET_DIR, deliveries_filename(pk)))
  }

  # Filter to existing files
  files_to_upload <- files_to_upload[file.exists(files_to_upload)]

  if (length(files_to_upload) == 0) {
    cli_alert_warning("No files to upload")
    return(invisible(FALSE))
  }

  cli_alert_info("Uploading {length(files_to_upload)} files...")

  # Ensure release exists
  tryCatch({
    pb_release_create(repo = REPO, tag = TAG, name = "Cricsheet Raw Data")
    cli_alert_info("Created '{TAG}' release")
    Sys.sleep(3)
  }, error = function(e) {
    cli_alert_info("Release '{TAG}' already exists")
  })

  # Upload each file
  for (file_path in files_to_upload) {
    for (attempt in 1:3) {
      success <- tryCatch({
        pb_upload(
          file = file_path,
          repo = REPO,
          tag = TAG,
          overwrite = TRUE
        )
        TRUE
      }, error = function(e) {
        if (attempt < 3) {
          cli_alert_warning("Attempt {attempt} failed for {basename(file_path)}, retrying...")
          Sys.sleep(3)
        }
        FALSE
      })
      if (success) {
        cli_alert_success("  Uploaded {basename(file_path)}")
        break
      }
    }
  }

  cli_alert_success("Upload complete!")
  cli_alert_info("Release: https://github.com/{REPO}/releases/tag/{TAG}")

  invisible(TRUE)
}

# ============================================================
# MAIN EXECUTION
# ============================================================

cli_h1("Bouncer Cricsheet Scraper (Optimized)")
cli_alert_info("Started at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}")
cli_alert_info("Partition scheme: match_type × gender (12 partitions max)")

# Print memory info
mem_info <- system("free -h 2>/dev/null || echo 'N/A'", intern = TRUE)
if (length(mem_info) > 1) {
  cli_alert_info("Memory: {mem_info[2]}")
}

dir.create(DATA_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(PARQUET_DIR, showWarnings = FALSE, recursive = TRUE)

# Step 1: Check for updates
has_updates <- check_cricsheet_updates()

if (!has_updates) {
  cli_alert_success("No updates needed. Exiting.")
  quit(save = "no", status = 0)
}

# Step 2: Download manifest to get existing match_ids (lightweight!)
manifest <- download_manifest()
existing_match_ids <- manifest$match_ids

# Step 3: Download Cricsheet data
json_dir <- download_cricsheet_data()

# Step 4: Parse and identify new matches
parsed_data <- parse_new_matches(json_dir, existing_match_ids)

if (parsed_data$n_new == 0) {
  cli_alert_success("All matches already in release. Nothing to upload.")
  quit(save = "no", status = 0)
}

# Step 5: Download affected partition files from release
affected_partitions <- names(parsed_data$partition_counts)
download_partition_files(affected_partitions)
download_core_files()

# Step 6: Merge new data with existing
merge_with_existing(parsed_data)

# Step 7: Generate updated manifest
generate_updated_manifest()

# Step 8: Upload changed files
upload_to_github(affected_partitions)

cli_h1("Complete!")
cli_alert_success("Finished at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}")
cli_alert_success("Added {parsed_data$n_new} new matches")
