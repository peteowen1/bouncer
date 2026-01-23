#!/usr/bin/env Rscript
# daily_scrape_lowmem.R - Memory-optimized version for 1GB VM
#
# Key optimizations:
# 1. Stream-append to parquet instead of load->bind->write
# 2. Process one JSON at a time (don't hold all in memory)
# 3. Aggressive gc() between operations
# 4. Write new deliveries to temp files, concat with Arrow (not bind_rows)
# 5. 3-way partitioning: match_type x gender x team_type (16 partitions max)
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
TEMP_DIR <- file.path(DATA_DIR, "temp")  # For intermediate files
ETAG_CACHE <- file.path(DATA_DIR, "etag_cache.json")

CRICSHEET_BASE <- "https://cricsheet.org/downloads"
CRICSHEET_RECENT_JSON <- paste0(CRICSHEET_BASE, "/recently_added_7_json.zip")

# Valid values for partition dimensions
MATCH_TYPES <- c("Test", "ODI", "T20", "IT20", "MDM", "ODM")
GENDERS <- c("male", "female")
TEAM_TYPES <- c("international", "club")

# ============================================================
# HELPER FUNCTIONS
# ============================================================

#' Create partition key from match attributes
#' Format: {match_type}_{gender}_{team_type}
#' Example: T20_male_club, ODI_female_international
make_partition_key <- function(match_type, gender, team_type) {

  paste(match_type, gender, team_type, sep = "_")
}

#' Get deliveries filename for a partition
deliveries_filename <- function(partition_key) {
  paste0("deliveries_", partition_key, ".parquet")
}

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

download_existing_parquets <- function() {
  cli_h2("Downloading existing data from GitHub")

  dir.create(PARQUET_DIR, showWarnings = FALSE, recursive = TRUE)
  temp_dir <- tempdir()
  zip_file <- file.path(temp_dir, "bouncerdata-parquet.zip")

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
      unzip(zip_file, exdir = PARQUET_DIR, overwrite = TRUE)
      file.remove(zip_file)
      n_files <- length(list.files(PARQUET_DIR, pattern = "\\.parquet$"))
      cli_alert_success("Extracted {n_files} parquet files")
    }
  }, error = function(e) {
    cli_alert_warning("No existing data found (first run?): {e$message}")
  })

  gc()  # Clean up after download
}

get_existing_match_ids <- function() {
  matches_path <- file.path(PARQUET_DIR, "matches.parquet")

  if (!file.exists(matches_path)) {
    cli_alert_info("No existing matches.parquet - all matches are new")
    return(character(0))
  }

  # Use Arrow to read only the column we need (memory efficient)
  existing_ids <- tryCatch({
    ds <- open_dataset(matches_path)
    ids <- ds |> select(match_id) |> collect()
    unique(ids$match_id)
  }, error = function(e) {
    cli_alert_warning("Could not read existing match IDs: {e$message}")
    character(0)
  })

  cli_alert_info("Found {length(existing_ids)} existing matches")
  gc()
  existing_ids
}

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

#' Parse JSON files ONE AT A TIME and write directly to temp parquets
#' Partitions by match_type x gender x team_type (up to 16 partitions)
parse_and_write_incrementally <- function(json_dir, existing_match_ids) {
  cli_h2("Parsing JSON files (incremental mode)")

  dir.create(TEMP_DIR, showWarnings = FALSE, recursive = TRUE)

  json_files <- list.files(json_dir, pattern = "\\.json$",
                           full.names = TRUE, recursive = TRUE)

  cli_alert_info("Found {length(json_files)} JSON files to parse")

  # Temp files for new data (will be concat'd with existing later)
  temp_matches <- file.path(TEMP_DIR, "new_matches.parquet")
  temp_players <- file.path(TEMP_DIR, "new_players.parquet")

  # Clean old temp files
  old_temps <- list.files(TEMP_DIR, full.names = TRUE)
  if (length(old_temps) > 0) file.remove(old_temps)

  # Dynamic accumulators - will grow as we encounter partitions
  batch_matches <- list()
  batch_players <- list()
  batch_deliveries <- list()  # Named list, keys are partition keys
  temp_deliveries <- list()   # Track temp file paths per partition

  BATCH_SIZE <- 10  # Write every 10 files to keep memory low

  n_new <- 0
  n_skipped <- 0
  partition_counts <- list()  # Track matches per partition

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

      # Get partition key
      mt <- parsed$match_info$match_type
      gender <- parsed$match_info$gender
      team_type <- parsed$match_info$team_type

      # Validate partition dimensions
      if (!mt %in% MATCH_TYPES) mt <- "Other"
      if (!gender %in% GENDERS) gender <- "unknown"
      if (!team_type %in% TEAM_TYPES) team_type <- "unknown"

      partition_key <- make_partition_key(mt, gender, team_type)

      # Track partition counts
      if (is.null(partition_counts[[partition_key]])) {
        partition_counts[[partition_key]] <- 0L
      }
      partition_counts[[partition_key]] <- partition_counts[[partition_key]] + 1L

      # Accumulate match and players
      batch_matches[[length(batch_matches) + 1]] <- parsed$match_info
      batch_players[[length(batch_players) + 1]] <- parsed$players

      # Add partition_key to deliveries and accumulate
      if (nrow(parsed$deliveries) > 0) {
        parsed$deliveries$partition_key <- partition_key

        # Initialize partition accumulator if needed
        if (is.null(batch_deliveries[[partition_key]])) {
          batch_deliveries[[partition_key]] <- list()
          temp_deliveries[[partition_key]] <- file.path(
            TEMP_DIR,
            paste0("new_", deliveries_filename(partition_key))
          )
        }

        batch_deliveries[[partition_key]][[length(batch_deliveries[[partition_key]]) + 1]] <- parsed$deliveries
      }

      # Flush batch to disk periodically
      if (length(batch_matches) >= BATCH_SIZE) {
        flush_batches_dynamic(
          batch_matches, batch_players, batch_deliveries,
          temp_matches, temp_players, temp_deliveries
        )
        # Reset accumulators
        batch_matches <<- list()
        batch_players <<- list()
        for (pk in names(batch_deliveries)) {
          batch_deliveries[[pk]] <<- list()
        }
      }

    }, error = function(e) {
      cli_alert_warning("Failed to parse {basename(json_files[i])}: {e$message}")
    })
    cli_progress_update(id = pb)
  }

  cli_progress_done(id = pb)

  # Final flush
  flush_batches_dynamic(
    batch_matches, batch_players, batch_deliveries,
    temp_matches, temp_players, temp_deliveries
  )

  # Summary logging
  cli_alert_success("Parsed {n_new} new matches, skipped {n_skipped} existing")

  # Log breakdown by partition (only partitions with matches)
  if (n_new > 0 && length(partition_counts) > 0) {
    # Sort by count descending
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

  return(list(
    n_new = n_new,
    partition_counts = partition_counts,
    temp_matches = temp_matches,
    temp_players = temp_players,
    temp_deliveries = temp_deliveries
  ))
}

#' Flush accumulated batches to parquet files (dynamic partitions)
flush_batches_dynamic <- function(batch_matches, batch_players, batch_deliveries,
                                   temp_matches, temp_players, temp_deliveries) {

  # Matches

  if (length(batch_matches) > 0) {
    df <- do.call(rbind, batch_matches)
    if (file.exists(temp_matches)) {
      existing <- read_parquet(temp_matches)
      df <- rbind(existing, df)
      rm(existing)
    }
    write_parquet(df, temp_matches)
    rm(df)
  }

  # Players
  if (length(batch_players) > 0) {
    df <- do.call(rbind, batch_players)
    df <- df[!duplicated(df$player_id), ]
    if (file.exists(temp_players)) {
      existing <- read_parquet(temp_players)
      df <- rbind(existing, df)
      df <- df[!duplicated(df$player_id), ]
      rm(existing)
    }
    write_parquet(df, temp_players)
    rm(df)
  }

  # Deliveries per partition
  for (pk in names(batch_deliveries)) {
    if (length(batch_deliveries[[pk]]) > 0) {
      df <- do.call(rbind, batch_deliveries[[pk]])
      temp_path <- temp_deliveries[[pk]]
      if (file.exists(temp_path)) {
        existing <- read_parquet(temp_path)
        df <- rbind(existing, df)
        rm(existing)
      }
      write_parquet(df, temp_path)
      rm(df)
    }
  }

  gc()
}

#' Merge temp parquets with existing (stream-append style)
#' Key optimization: Use Arrow concat_tables to avoid loading both into R memory
merge_parquets_lowmem <- function(temp_info) {
  cli_h2("Merging with existing data (low-memory mode)")

  if (temp_info$n_new == 0) {
    cli_alert_info("No new matches to merge")
    return(invisible(FALSE))
  }

  # === MATCHES ===
  matches_path <- file.path(PARQUET_DIR, "matches.parquet")
  if (file.exists(temp_info$temp_matches)) {
    new_matches <- read_parquet(temp_info$temp_matches)
    cli_alert_info("New matches: {nrow(new_matches)}")

    if (file.exists(matches_path)) {
      # Use Arrow Tables for efficient concat
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

  # === DELIVERIES (per partition) ===
  cli_alert_info("Processing deliveries by partition...")

  for (pk in names(temp_info$temp_deliveries)) {
    temp_path <- temp_info$temp_deliveries[[pk]]
    final_path <- file.path(PARQUET_DIR, deliveries_filename(pk))

    if (!file.exists(temp_path)) next

    new_del <- read_parquet(temp_path, as_data_frame = FALSE)

    if (file.exists(final_path)) {
      # Arrow concat - much more memory efficient than bind_rows
      existing_tbl <- read_parquet(final_path, as_data_frame = FALSE)
      combined_tbl <- concat_tables(existing_tbl, new_del)
      write_parquet(combined_tbl, final_path, compression = "zstd")
      cli_alert_success("    {pk}: {combined_tbl$num_rows} rows")
      rm(existing_tbl, combined_tbl)
    } else {
      write_parquet(new_del, final_path, compression = "zstd")
      cli_alert_success("    {pk}: {new_del$num_rows} rows (new)")
    }
    rm(new_del)
    gc()
  }

  # === PLAYERS ===
  players_path <- file.path(PARQUET_DIR, "players.parquet")
  if (file.exists(temp_info$temp_players)) {
    new_players <- read_parquet(temp_info$temp_players)

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

upload_to_github <- function() {
  cli_h2("Uploading to GitHub")

  parquet_files <- list.files(PARQUET_DIR, pattern = "\\.parquet$", full.names = TRUE)

  if (length(parquet_files) == 0) {
    cli_alert_warning("No parquet files found to upload")
    return(invisible(FALSE))
  }

  total_size <- sum(file.size(parquet_files)) / 1024 / 1024
  cli_alert_info("Found {length(parquet_files)} parquet files ({round(total_size, 1)} MB total)")

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

  tryCatch({
    pb_release_create(repo = REPO, tag = "core")
    cli_alert_info("Created 'core' release")
    Sys.sleep(3)
  }, error = function(e) {
    cli_alert_info("Release 'core' already exists")
  })

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

cli_h1("Bouncer Daily Scrape (Low-Memory Mode)")
cli_alert_info("Started at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}")
cli_alert_info("Partitioning: match_type x gender x team_type (up to 16 partitions)")

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

# Step 2: Download existing parquets
download_existing_parquets()

# Step 3: Get existing match IDs
existing_match_ids <- get_existing_match_ids()

# Step 4: Download new JSON data
json_dir <- download_cricsheet_data()

# Step 5: Parse incrementally and write temp parquets
# This is the key optimization - processes one file at a time
temp_info <- parse_and_write_incrementally(json_dir, existing_match_ids)

if (temp_info$n_new == 0) {
  cli_alert_success("All matches already in database. Nothing to upload.")
  quit(save = "no", status = 0)
}

# Step 6: Merge using Arrow concat (low memory)
merge_parquets_lowmem(temp_info)

# Step 7: Upload
upload_to_github()

cli_h1("Complete!")
cli_alert_success("Finished at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}")
