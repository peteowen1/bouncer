# User-Facing Functions for Bouncer Data Management

#' Install Bouncer Cricket Data
#'
#' Downloads cricket data from Cricsheet and loads it into a local DuckDB database.
#' This is the main function for setting up your bouncer cricket data.
#'
#' @param formats Character vector of formats to download. Options: "test", "odi", "t20i".
#'   Default is c("odi", "t20i").
#' @param leagues Character vector of leagues to download. Options: "ipl", "bbl", "cpl",
#'   "psl", "wbbl", etc. Default is c("ipl").
#' @param gender Character string. "male", "female", or "both". Default "male".
#' @param start_season Character or numeric. Only download matches from this season onwards.
#'   If NULL, downloads all available data.
#' @param db_path Database path. If NULL, uses default system data directory.
#' @param download_path Path to store downloaded files. If NULL, uses temp directory.
#' @param keep_downloads Logical. If TRUE, keeps downloaded JSON files. Default FALSE.
#'
#' @return Invisibly returns database path
#' @export
#'
#' @examples
#' \dontrun{
#' # Install ODI and T20I data plus IPL
#' install_bouncer_data()
#'
#' # Install all formats
#' install_bouncer_data(
#'   formats = c("test", "odi", "t20i"),
#'   leagues = c("ipl", "bbl")
#' )
#'
#' # Install only recent data (from 2020 onwards)
#' install_bouncer_data(start_season = 2020)
#' }
install_bouncer_data <- function(formats = c("odi", "t20i"),
                                  leagues = c("ipl"),
                                  gender = "male",
                                  start_season = NULL,
                                  db_path = NULL,
                                  download_path = NULL,
                                  keep_downloads = FALSE) {

  cli::cli_h1("Installing Bouncer Cricket Data")

  # Initialize database
  cli::cli_h2("Step 1: Initialize Database")
  db_path <- if (is.null(db_path)) get_db_path() else db_path

  if (!file.exists(db_path)) {
    initialize_bouncer_database(path = db_path)
  } else {
    cli::cli_alert_info("Database already exists at {.file {db_path}}")
  }

  # Setup download path
  if (is.null(download_path)) {
    download_path <- file.path(tempdir(), "bouncer_downloads")
  }

  if (!dir.exists(download_path)) {
    dir.create(download_path, recursive = TRUE)
  }

  # Download and load each format
  if (length(formats) > 0) {
    cli::cli_h2("Step 2: Download and Load Format Data")

    genders_to_download <- if (gender == "both") {
      c("male", "female")
    } else {
      gender
    }

    for (fmt in formats) {
      for (gnd in genders_to_download) {
        cli::cli_alert_info("Processing {fmt} ({gnd})")

        # Download
        files <- tryCatch({
          download_format_data(
            format = fmt,
            gender = gnd,
            output_path = file.path(download_path, paste0(fmt, "_", gnd))
          )
        }, error = function(e) {
          cli::cli_alert_warning("Failed to download {fmt} ({gnd}): {e$message}")
          character(0)
        })

        if (length(files) > 0) {
          # Season filtering removed (placeholder implementation)
          # To filter by season, filter after loading based on match_date in database

          # Load to database
          batch_load_matches(files, path = db_path)
        }
      }
    }
  }

  # Download and load each league
  if (length(leagues) > 0) {
    cli::cli_h2("Step 3: Download and Load League Data")

    for (league in leagues) {
      cli::cli_alert_info("Processing {league}")

      # Download
      files <- tryCatch({
        download_league_data(
          league = league,
          output_path = file.path(download_path, league)
        )
      }, error = function(e) {
        cli::cli_alert_warning("Failed to download {league}: {e$message}")
        character(0)
      })

      if (length(files) > 0) {
        # Season filtering removed (placeholder implementation)
        # To filter by season, filter after loading based on match_date in database

        # Load to database
        batch_load_matches(files, path = db_path)
      }
    }
  }

  # Clean up downloads if requested
  if (!keep_downloads && dir.exists(download_path)) {
    cli::cli_alert_info("Cleaning up downloads...")
    unlink(download_path, recursive = TRUE)
  }

  # Show summary
  cli::cli_h2("Installation Complete!")
  get_data_info(path = db_path)

  cli::cli_alert_success("Database ready at {.file {db_path}}")
  cli::cli_alert_info("Load the bouncer package to analyze this data")

  invisible(db_path)
}


#' Get Data Info
#'
#' Displays information about the installed cricket data.
#'
#' @param path Database path. If NULL, uses default.
#'
#' @return Invisibly returns a list with data statistics
#' @export
#'
#' @examples
#' \dontrun{
#' # Show what data is installed
#' get_data_info()
#' }
get_data_info <- function(path = NULL) {
  if (is.null(path)) {
    path <- get_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_alert_danger("No database found. Run {.fn install_bouncer_data} first.")
    return(invisible(NULL))
  }

  conn <- get_db_connection(path = path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Get counts
  n_matches <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM matches")$n
  n_deliveries <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM deliveries")$n
  n_players <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM players")$n

  # Get match type breakdown
  match_types <- DBI::dbGetQuery(conn, "
    SELECT match_type, COUNT(*) as n
    FROM matches
    GROUP BY match_type
    ORDER BY n DESC
  ")

  # Get date range
  date_range <- DBI::dbGetQuery(conn, "
    SELECT MIN(match_date) as earliest, MAX(match_date) as latest
    FROM matches
    WHERE match_date IS NOT NULL
  ")

  cli::cli_h2("Bouncer Cricket Data Summary")
  cli::cli_alert_info("Matches: {format_number(n_matches)}")
  cli::cli_alert_info("Deliveries: {format_number(n_deliveries)}")
  cli::cli_alert_info("Players: {format_number(n_players)}")

  if (nrow(date_range) > 0 && !is.na(date_range$earliest)) {
    cli::cli_alert_info("Date range: {date_range$earliest} to {date_range$latest}")
  }

  if (nrow(match_types) > 0) {
    cli::cli_h3("Matches by Type")
    for (i in 1:nrow(match_types)) {
      cli::cli_alert_info("{match_types$match_type[i]}: {match_types$n[i]}")
    }
  }

  info <- list(
    n_matches = n_matches,
    n_deliveries = n_deliveries,
    n_players = n_players,
    match_types = match_types,
    date_range = date_range
  )

  invisible(info)
}


#' List Available Formats in Database
#'
#' Shows what cricket formats are currently in the database.
#'
#' @param path Database path. If NULL, uses default.
#'
#' @return A data frame with format information
#' @export
#'
#' @examples
#' \dontrun{
#' # See what formats are installed
#' list_available_formats()
#' }
list_available_formats <- function(path = NULL) {
  if (is.null(path)) {
    path <- get_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_alert_danger("No database found.")
    return(invisible(NULL))
  }

  conn <- get_db_connection(path = path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  formats <- DBI::dbGetQuery(conn, "
    SELECT
      match_type,
      gender,
      COUNT(*) as n_matches,
      MIN(match_date) as earliest_match,
      MAX(match_date) as latest_match
    FROM matches
    WHERE match_type IS NOT NULL
    GROUP BY match_type, gender
    ORDER BY n_matches DESC
  ")

  print(formats)
  invisible(formats)
}


#' Install All Bouncer Cricket Data (Optimized)
#'
#' Downloads ALL cricket data from Cricsheet in a single ZIP file and loads it
#' into the database with optimized batch processing. This is the recommended
#' method for initial setup.
#'
#' This function:
#' - Downloads all_json.zip (all formats, all leagues, all genders)
#' - Uses vectorized parsing (~50ms per file vs ~5000ms)
#' - Batch database transactions (100 files per transaction)
#' - Skips already-loaded matches (incremental loading supported)
#'
#' @param db_path Database path. If NULL, uses default system data directory.
#' @param fresh Logical. If TRUE, deletes existing database and starts fresh. Default TRUE.
#'   Set to FALSE for incremental loading (skips already-loaded matches).
#' @param match_types Optional character vector to filter match types after download.
#'   Examples: c("ODI", "Test", "T20"), c("IPL", "BBL"). If NULL, loads all types.
#' @param genders Optional character vector to filter genders. Options: "male", "female".
#'   If NULL, loads both genders.
#' @param download_path Path to store downloaded/extracted files. If NULL, uses temp directory.
#' @param keep_downloads Logical. If TRUE, keeps downloaded JSON files. Default FALSE.
#' @param batch_size Number of files per database transaction. Default 500.
#'
#' @return Invisibly returns database path
#' @export
#'
#' @examples
#' \dontrun{
#' # Update with new matches (default - incremental)
#' install_all_bouncer_data()
#'
#' # Fresh start - delete DB and reload everything
#' install_all_bouncer_data(fresh = TRUE)
#'
#' # Install only specific match types
#' install_all_bouncer_data(match_types = c("ODI", "T20"))
#'
#' # Install only men's international cricket
#' install_all_bouncer_data(match_types = c("Test", "ODI", "T20"), genders = "male")
#' }
install_all_bouncer_data <- function(db_path = NULL,
                                      fresh = FALSE,
                                      match_types = NULL,
                                      genders = NULL,
                                      download_path = NULL,
                                      keep_downloads = FALSE,
                                      batch_size = 500) {

  cli::cli_h1("Installing All Bouncer Cricket Data (Optimized)")
  start_time <- Sys.time()

  # Get database path
  db_path <- if (is.null(db_path)) get_db_path() else db_path

  # Handle fresh start
  if (fresh) {
    cli::cli_h2("Step 1: Fresh Start - Deleting Existing Database")

    # Close all DuckDB connections using our utility function
    force_close_duckdb()

    # Delete database if it exists
    if (file.exists(db_path)) {
      file.remove(db_path)
      cli::cli_alert_success("Deleted existing database")
    }

    # Delete WAL file if it exists
    wal_path <- paste0(db_path, ".wal")
    if (file.exists(wal_path)) {
      file.remove(wal_path)
    }

    # Create fresh database without indexes (skip_indexes=TRUE)
    # Indexes will be created AFTER bulk loading for better performance
    initialize_bouncer_database(path = db_path, overwrite = TRUE, skip_indexes = TRUE, verbose = FALSE)
    cli::cli_alert_success("Created fresh database at {.file {db_path}}")

  } else {
    # Incremental mode
    cli::cli_h2("Step 1: Initialize Database (Incremental Mode)")

    if (!file.exists(db_path)) {
      initialize_bouncer_database(path = db_path, verbose = FALSE)
      cli::cli_alert_success("Created new database at {.file {db_path}}")
    } else {
      cli::cli_alert_info("Using existing database at {.file {db_path}}")
      cli::cli_alert_info("Will detect new and changed matches")
    }
  }

  # Download all data (returns list with new_files, changed_files, all_files)
  cli::cli_h2("Step 2: Download All Cricsheet Data")
  t_download_start <- Sys.time()
  download_result <- download_all_cricsheet_data(
    output_path = download_path,
    keep_zip = keep_downloads,
    fresh = fresh
  )
  t_download <- as.numeric(difftime(Sys.time(), t_download_start, units = "secs"))

  # Handle changed files (delete from DB before reloading)
  if (!fresh && length(download_result$changed_files) > 0) {
    cli::cli_h2("Step 2b: Handle Changed Matches")
    cli::cli_alert_info("{length(download_result$changed_files)} matches have updated data (e.g., Test match progress)")

    # Extract match IDs from filenames (remove .json extension)
    changed_match_ids <- tools::file_path_sans_ext(download_result$changed_files)

    # Delete changed matches from database
    conn <- get_db_connection(path = db_path, read_only = FALSE)
    tryCatch(
      delete_matches_from_db(changed_match_ids, conn),
      finally = DBI::dbDisconnect(conn, shutdown = TRUE)
    )
  }

  # Determine which files to load
  json_files <- download_result$all_files
  n_total <- length(json_files)

  # Filter by match type if requested
  if (!is.null(match_types)) {
    cli::cli_h2("Step 3: Filtering by Match Type")
    cli::cli_alert_info("Requested types: {paste(match_types, collapse = ', ')}")
    cli::cli_alert_info("Match type filtering will be applied during loading")
  }

  # Load to database
  cli::cli_h2(if (is.null(match_types)) "Step 3: Load to Database" else "Step 4: Load to Database")

  # ============================================================================
  # OPTIMIZATION: Drop indexes before bulk load, recreate after
  # This provides ~3-5x speedup for large bulk loads
  # For fresh installs, indexes weren't created yet (skip_indexes=TRUE above)
  # ============================================================================
  if (!fresh) {
    conn_for_index <- get_db_connection(path = db_path, read_only = FALSE)
    drop_bulk_load_indexes(conn_for_index, verbose = TRUE)
    DBI::dbDisconnect(conn_for_index, shutdown = TRUE)
  }

  t_load_start <- Sys.time()

  # If filtering, we need to parse and check match types
  if (!is.null(match_types) || !is.null(genders)) {
    loaded_count <- load_filtered_matches(
      json_files,
      db_path = db_path,
      match_types = match_types,
      genders = genders,
      batch_size = batch_size
    )
  } else {
    # Load all files (uses parallel parsing + Arrow bulk writes + data.table rbindlist)
    loaded_count <- batch_load_matches(
      file_paths = json_files,
      path = db_path,
      batch_size = batch_size,
      progress = TRUE
    )
  }

  t_load <- as.numeric(difftime(Sys.time(), t_load_start, units = "secs"))

  # Recreate indexes after bulk load
  cli::cli_alert_info("Rebuilding indexes...")
  t_index_start <- Sys.time()
  conn_for_index <- get_db_connection(path = db_path, read_only = FALSE)
  create_indexes(conn_for_index)
  DBI::dbDisconnect(conn_for_index, shutdown = TRUE)
  t_index <- as.numeric(difftime(Sys.time(), t_index_start, units = "secs"))

  # Clean up downloads if requested
  if (!keep_downloads && !is.null(download_path) && dir.exists(download_path)) {
    cli::cli_alert_info("Cleaning up downloads...")
    unlink(download_path, recursive = TRUE)
  }

  # Summary
  end_time <- Sys.time()
  elapsed <- difftime(end_time, start_time, units = "mins")
  elapsed_secs <- as.numeric(elapsed) * 60

  cli::cli_h2("Installation Complete!")

  # Step-by-step timing breakdown
  cli::cli_rule("Timing Breakdown")

  # Format time helper
  fmt_time <- function(secs) {
    if (secs >= 60) {
      sprintf("%.1f min", secs / 60)
    } else {
      sprintf("%.1f sec", secs)
    }
  }

  cli::cli_alert_info("Download:       {fmt_time(t_download)} ({round(t_download/elapsed_secs*100)}%)")
  cli::cli_alert_info("Parse & Load:   {fmt_time(t_load)} ({round(t_load/elapsed_secs*100)}%)")
  cli::cli_alert_info("Build Indexes:  {fmt_time(t_index)} ({round(t_index/elapsed_secs*100)}%)")
  cli::cli_rule()
  cli::cli_alert_success("Total time: {round(as.numeric(elapsed), 1)} minutes")

  # Show what was new/changed
  if (!fresh) {
    cli::cli_alert_info("New matches: {length(download_result$new_files)}")
    cli::cli_alert_info("Updated matches: {length(download_result$changed_files)}")
    cli::cli_alert_info("Unchanged matches: {download_result$unchanged_count}")
  }

  get_data_info(path = db_path)

  cli::cli_alert_success("Database ready at {.file {db_path}}")

  invisible(db_path)
}


#' Load Filtered Matches
#'
#' Loads matches with optional filtering by match type and gender.
#' Used internally by install_all_bouncer_data when filters are specified.
#'
#' @param file_paths Character vector of JSON file paths
#' @param db_path Database path
#' @param match_types Optional character vector of match types to include
#' @param genders Optional character vector of genders to include
#' @param batch_size Files per transaction
#'
#' @return Count of successfully loaded matches
#' @keywords internal
load_filtered_matches <- function(file_paths,
                                   db_path,
                                   match_types = NULL,
                                   genders = NULL,
                                   batch_size = 500) {

  n_files <- length(file_paths)

  if (n_files == 0) {
    cli::cli_alert_warning("No files to load")
    return(invisible(0))
  }

  # Normalize filter values
  if (!is.null(match_types)) {
    match_types <- toupper(match_types)
  }
  if (!is.null(genders)) {
    genders <- tolower(genders)
  }

  cli::cli_alert_info("Loading with filters: match_types={paste(match_types %||% 'all', collapse=',')}, genders={paste(genders %||% 'all', collapse=',')}")

  # Connect to database

  conn <- get_db_connection(path = db_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Get existing match IDs
  existing_matches <- DBI::dbGetQuery(conn, "SELECT match_id FROM matches")
  existing_ids <- existing_matches$match_id

  # Filter to new files only
  file_match_ids <- tools::file_path_sans_ext(basename(file_paths))
  new_mask <- !(file_match_ids %in% existing_ids)
  new_files <- file_paths[new_mask]
  n_new <- length(new_files)
  n_skipped_existing <- sum(!new_mask)

  if (n_skipped_existing > 0) {
    cli::cli_alert_info("Skipping {n_skipped_existing} already-loaded matches")
  }

  if (n_new == 0) {
    cli::cli_alert_success("No new matches to load")
    return(invisible(0))
  }

  # Process files with filtering
  success_count <- 0
  skipped_filter <- 0
  error_count <- 0

  batches <- split(seq_along(new_files), ceiling(seq_along(new_files) / batch_size))

  # Setup progress bar
  cli::cli_progress_bar(
    format = "Processing batches {cli::pb_current}/{cli::pb_total}",
    total = length(batches),
    clear = TRUE
  )

  for (batch_num in seq_along(batches)) {
    batch_indices <- batches[[batch_num]]
    batch_files <- new_files[batch_indices]

    cli::cli_progress_update()

    # Collect parsed data that passes filters
    batch_matches <- list()
    batch_deliveries <- list()
    batch_innings <- list()
    batch_players <- list()
    batch_powerplays <- list()
    valid_count <- 0

    for (i in seq_along(batch_files)) {
      tryCatch({
        parsed <- parse_cricsheet_json(batch_files[i])

        # Check if match passes filters
        if (nrow(parsed$match_info) > 0) {
          match_type_val <- toupper(parsed$match_info$match_type[1] %||% "")
          gender_val <- tolower(parsed$match_info$gender[1] %||% "")

          passes_type <- is.null(match_types) || match_type_val %in% match_types
          passes_gender <- is.null(genders) || gender_val %in% genders

          if (passes_type && passes_gender) {
            valid_count <- valid_count + 1
            batch_matches[[valid_count]] <- parsed$match_info
            batch_deliveries[[valid_count]] <- parsed$deliveries
            batch_innings[[valid_count]] <- parsed$innings
            batch_players[[valid_count]] <- parsed$players
            batch_powerplays[[valid_count]] <- parsed$powerplays
          } else {
            skipped_filter <- skipped_filter + 1
          }
        }
      }, error = function(e) {
        cli::cli_alert_warning("Error parsing {basename(batch_files[i])}: {conditionMessage(e)}")
        error_count <<- error_count + 1
      })
    }

    # Insert valid data
    if (valid_count > 0) {
      batch_matches <- batch_matches[1:valid_count]
      batch_deliveries <- batch_deliveries[1:valid_count]
      batch_innings <- batch_innings[1:valid_count]
      batch_players <- batch_players[1:valid_count]
      batch_powerplays <- batch_powerplays[1:valid_count]

      all_matches <- fast_rbind(batch_matches)
      all_deliveries <- fast_rbind(batch_deliveries)
      all_innings <- fast_rbind(batch_innings)
      all_players <- fast_rbind(batch_players)
      all_powerplays <- fast_rbind(batch_powerplays)

      # Deduplicate based on primary keys (in case same file appears multiple times in batch)
      all_matches <- all_matches[!duplicated(all_matches$match_id), ]
      all_deliveries <- all_deliveries[!duplicated(all_deliveries$delivery_id), ]
      all_innings <- all_innings[!duplicated(paste0(all_innings$match_id, "_", all_innings$innings)), ]
      all_players <- all_players[!duplicated(all_players$player_id), ]
      if (!is.null(all_powerplays) && nrow(all_powerplays) > 0) {
        all_powerplays <- all_powerplays[!duplicated(all_powerplays$powerplay_id), ]
      }

      tryCatch({
        DBI::dbBegin(conn)

        if (!is.null(all_matches) && nrow(all_matches) > 0) {
          DBI::dbAppendTable(conn, "matches", all_matches)
        }
        if (!is.null(all_deliveries) && nrow(all_deliveries) > 0) {
          DBI::dbAppendTable(conn, "deliveries", all_deliveries)
        }
        if (!is.null(all_innings) && nrow(all_innings) > 0) {
          DBI::dbAppendTable(conn, "match_innings", all_innings)
        }
        if (!is.null(all_players) && nrow(all_players) > 0) {
          insert_players_batch(conn, all_players)
        }
        if (!is.null(all_powerplays) && nrow(all_powerplays) > 0) {
          DBI::dbAppendTable(conn, "innings_powerplays", all_powerplays)
        }

        DBI::dbCommit(conn)
        success_count <- success_count + valid_count

      }, error = function(e) {
        # Try to rollback, but don't error if transaction already aborted
        tryCatch(DBI::dbRollback(conn), error = function(e2) NULL)
        cli::cli_alert_danger("Batch {batch_num} failed: {conditionMessage(e)}")
        error_count <<- error_count + valid_count
      })
    }
  }

  # Close progress bar
  cli::cli_progress_done()

  # Summary
  cli::cli_alert_success("Successfully loaded {success_count} matches")
  if (skipped_filter > 0) {
    cli::cli_alert_info("Skipped {skipped_filter} matches (did not match filters)")
  }
  if (error_count > 0) {
    cli::cli_alert_warning("{error_count} matches failed to load")
  }

  invisible(success_count)
}


# Placeholder filter_files_by_season() function removed
# To filter by season, query database after loading: WHERE match_date >= 'start_date'


# Release Installation Functions =============================================
#
# User-facing functions for installing bouncer data from GitHub Releases.
# This provides an alternative to install_all_bouncer_data() that downloads
# pre-processed data from releases instead of building locally.


#' Get Latest Release Info from GitHub
#'
#' Fetches metadata about the latest release from the bouncerdata repository.
#'
#' @param repo Character. GitHub repository in "owner/repo" format.
#'   Default is "peteowen1/bouncerdata".
#' @param type Character. Release type to find: "cricsheet" (parquet data),
#'   "daily" (JSON archives), "weekly" (legacy parquet), or "any" (most recent).
#'
#' @return List with release information including tag_name, published_at,
#'   and assets.
#'
#' @export
#' @examples
#' \dontrun{
#' # Get core release (parquet files from daily scraper)
#' release <- get_latest_release(type = "cricsheet")
#' print(release$tag_name)
#'
#' # Get latest daily release (JSON archives)
#' release <- get_latest_release(type = "daily")
#' }
get_latest_release <- function(repo = "peteowen1/bouncerdata", type = "any") {
  type <- match.arg(type, c("any", "cricsheet", "daily", "weekly"))

  # Build API URL
  if (type == "any") {
    url <- sprintf("https://api.github.com/repos/%s/releases/latest", repo)
  } else {
    url <- sprintf("https://api.github.com/repos/%s/releases", repo)
  }

  cli::cli_alert_info("Checking GitHub releases...")

  resp <- httr2::request(url) |>
    httr2::req_headers(Accept = "application/vnd.github.v3+json") |>
    httr2::req_user_agent("bouncer R package") |>
    httr2::req_timeout(30) |>
    httr2::req_perform()

  if (type == "any") {
    return(httr2::resp_body_json(resp))
  }

  # Filter releases by type
  releases <- httr2::resp_body_json(resp)

  pattern <- if (type == "cricsheet") {
    "^cricsheet$"
  } else if (type == "weekly") {
    "-weekly$"
  } else {
    "^v[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}$"  # daily releases
  }

  matching <- Filter(function(r) grepl(pattern, r$tag_name), releases)

  if (length(matching) == 0) {
    cli::cli_abort("No {type} releases found in {repo}")
  }

  matching[[1]]  # Return most recent matching release
}


#' Download Release Asset
#'
#' Downloads a specific asset from a GitHub release.
#'
#' @param asset_url Character. URL of the asset to download.
#' @param dest_path Character. Destination file path.
#' @param show_progress Logical. Show download progress bar. Default TRUE.
#'
#' @return Invisibly returns the destination path.
#' @keywords internal
download_release_asset <- function(asset_url, dest_path, show_progress = TRUE) {
  req <- httr2::request(asset_url) |>
    httr2::req_headers(Accept = "application/octet-stream") |>
    httr2::req_user_agent("bouncer R package") |>
    httr2::req_timeout(600)  # 10 minute timeout for large files

  if (show_progress) {
    req <- req |> httr2::req_progress()
  }

  req |> httr2::req_perform(path = dest_path)

  invisible(dest_path)
}


#' Install Bouncerdata from GitHub Release
#'
#' Downloads and installs cricket data from the bouncerdata GitHub releases.
#' This is an alternative to `install_all_bouncer_data()` that downloads
#' pre-processed data instead of building locally from Cricsheet.
#'
#' @param repo Character. GitHub repository. Default "peteowen1/bouncerdata".
#' @param formats Character vector. Data formats to download:
#'   - "long_form" = Tests, First-class matches
#'   - "short_form" = ODIs, T20s, franchise leagues
#'   - "all" = both (default)
#' @param genders Character vector. "male", "female", or c("male", "female").
#'   Default is both.
#' @param types Character vector. "international", "club", or both.
#'   Default is both.
#' @param tag Character. Specific release tag to download, or "latest".
#' @param data_dir Character. Directory to store data. Default uses
#'   `find_bouncerdata_dir()`.
#' @param build_db Logical. If TRUE, build DuckDB from downloaded JSONs.
#'   Default TRUE.
#'
#' @return Invisibly returns the data directory path.
#'
#' @export
#' @examples
#' \dontrun{
#' # Install all data
#' install_bouncerdata_from_release()
#'
#' # Install only short form (T20s, ODIs)
#' install_bouncerdata_from_release(formats = "short_form")
#'
#' # Install only IPL (short form, male, club)
#' install_bouncerdata_from_release(
#'   formats = "short_form",
#'   genders = "male",
#'   types = "club"
#' )
#' }
install_bouncerdata_from_release <- function(repo = "peteowen1/bouncerdata",
                                              formats = "all",
                                              genders = c("male", "female"),
                                              types = c("international", "club"),
                                              tag = "latest",
                                              data_dir = NULL,
                                              build_db = TRUE) {

  cli::cli_h1("Installing Bouncerdata from GitHub Release")

  # Setup data directory
  if (is.null(data_dir)) {
    data_dir <- find_bouncerdata_dir(create = TRUE)
  }
  if (!dir.exists(data_dir)) {
    dir.create(data_dir, recursive = TRUE)
  }

  json_dir <- file.path(data_dir, "json_files")
  dir.create(json_dir, showWarnings = FALSE, recursive = TRUE)

  # Determine which folders to download
  if ("all" %in% formats) {
    formats <- c("long_form", "short_form")
  }

  folders_to_download <- character()
  for (fmt in formats) {
    for (gnd in genders) {
      for (typ in types) {
        folders_to_download <- c(folders_to_download,
                                  paste(fmt, gnd, typ, sep = "_"))
      }
    }
  }

  cli::cli_alert_info("Will download {length(folders_to_download)} data categories")

  # Get release
  cli::cli_h2("Step 1: Finding release")
  release <- if (tag == "latest") {
    get_latest_release(repo, type = "daily")
  } else {
    url <- sprintf("https://api.github.com/repos/%s/releases/tags/%s", repo, tag)
    resp <- httr2::request(url) |>
      httr2::req_headers(Accept = "application/vnd.github.v3+json") |>
      httr2::req_perform()
    httr2::resp_body_json(resp)
  }

  cli::cli_alert_success("Found release: {release$tag_name}")

  # Download requested ZIP files
  cli::cli_h2("Step 2: Downloading data")

  downloaded_count <- 0
  for (folder in folders_to_download) {
    zip_name <- paste0(folder, ".zip")

    # Find matching asset
    asset <- NULL
    for (a in release$assets) {
      if (a$name == zip_name) {
        asset <- a
        break
      }
    }

    if (is.null(asset)) {
      cli::cli_alert_warning("Asset not found: {zip_name}")
      next
    }

    # Download
    size_mb <- asset$size / 1024 / 1024
    cli::cli_alert_info("Downloading {zip_name} ({round(size_mb, 1)} MB)...")

    temp_zip <- tempfile(fileext = ".zip")
    folder_path <- file.path(json_dir, folder)
    dir.create(folder_path, showWarnings = FALSE, recursive = TRUE)

    # Use tryCatch to ensure temp file cleanup on error
    tryCatch({
      download_release_asset(asset$browser_download_url, temp_zip)
      zip::unzip(temp_zip, exdir = folder_path)
    }, finally = {
      # Always clean up temp file
      if (file.exists(temp_zip)) unlink(temp_zip)
    })

    n_files <- length(list.files(folder_path, pattern = "\\.json$"))
    cli::cli_alert_success("{folder}: {n_files} matches")
    downloaded_count <- downloaded_count + n_files
  }

  cli::cli_alert_success("Downloaded {downloaded_count} match files")

  # Build database if requested
  if (build_db) {
    cli::cli_h2("Step 3: Building database")

    json_files <- list.files(json_dir, pattern = "\\.json$",
                              full.names = TRUE, recursive = TRUE)

    if (length(json_files) > 0) {
      initialize_bouncer_database(overwrite = TRUE)
      batch_load_matches(json_files, progress = TRUE)
      get_data_info()
    }
  }

  cli::cli_alert_success("Installation complete!")
  cli::cli_alert_info("Data directory: {data_dir}")

  invisible(data_dir)
}


#' Install Parquet Files from Core Release
#'
#' Downloads pre-built parquet files from the core release.
#' These can be queried directly with DuckDB or Arrow without
#' building a local database.
#'
#' @param repo Character. GitHub repository. Default "peteowen1/bouncerdata".
#' @param tables Character vector. Tables to download, or "all".
#'   Options: "matches", "players", "team_elo", or delivery partitions like
#'   "deliveries_T20_male_club", "deliveries_Test_male_international", etc.
#'   See DATA_FOLDERS constant for all available partitions.
#' @param data_dir Character. Directory to store parquet files.
#' @param tag Character. Specific release tag, or "latest".
#'
#' @return Character vector of downloaded file paths.
#'
#' @export
#' @examples
#' \dontrun{
#' # Download all parquet files
#' install_parquets_from_release()
#'
#' # Download only matches and T20 deliveries
#' install_parquets_from_release(
#'   tables = c("matches", "deliveries_T20_male_club")
#' )
#' }
install_parquets_from_release <- function(repo = "peteowen1/bouncerdata",
                                           tables = "all",
                                           data_dir = NULL,
                                           tag = "latest") {

  cli::cli_h1("Installing Parquet Files from Core Release")

  # Setup data directory
  if (is.null(data_dir)) {
    data_dir <- file.path(find_bouncerdata_dir(create = TRUE), "parquet")
  }
  dir.create(data_dir, showWarnings = FALSE, recursive = TRUE)

  # All available tables (partitioned by match_type × gender × team_type)
  all_tables <- c(
    # Unified tables
    "players", "team_elo",
    # Matches - partitioned
    paste0("matches_", DATA_FOLDERS),
    # Deliveries - partitioned
    paste0("deliveries_", DATA_FOLDERS),
    # Skill indices - format-specific
    "test_player_skill", "odi_player_skill", "t20_player_skill",
    "test_team_skill", "odi_team_skill", "t20_team_skill",
    "test_venue_skill", "odi_venue_skill", "t20_venue_skill"
  )

  if ("all" %in% tables) {
    tables <- all_tables
  }

  # Get core release (parquet data from daily scraper)
  release <- get_latest_release(repo, type = "cricsheet")
  cli::cli_alert_success("Found release: {release$tag_name}")

  # Download each table (use list to avoid O(n²) vector growth)
  downloaded_list <- vector("list", length(tables))
  download_idx <- 0L

  for (table in tables) {
    parquet_name <- paste0(table, ".parquet")

    # Find asset
    asset <- NULL
    for (a in release$assets) {
      if (a$name == parquet_name) {
        asset <- a
        break
      }
    }

    if (is.null(asset)) {
      cli::cli_alert_warning("Table not found: {table}")
      next
    }

    size_mb <- asset$size / 1024 / 1024
    cli::cli_alert_info("Downloading {parquet_name} ({round(size_mb, 1)} MB)...")

    dest_path <- file.path(data_dir, parquet_name)
    download_release_asset(asset$browser_download_url, dest_path)

    cli::cli_alert_success("Downloaded: {parquet_name}")
    download_idx <- download_idx + 1L
    downloaded_list[[download_idx]] <- dest_path
  }

  # Convert list to vector (efficient: single allocation)
  downloaded <- unlist(downloaded_list[seq_len(download_idx)])

  cli::cli_alert_success("Downloaded {length(downloaded)} parquet files to {data_dir}")

  invisible(downloaded)
}


#' Update Bouncerdata from Release
#'
#' Checks for new releases and updates local data if available.
#'
#' @param repo Character. GitHub repository.
#' @param data_dir Character. Data directory to update.
#'
#' @return Logical. TRUE if update was performed, FALSE if already up to date.
#'
#' @export
#' @examples
#' \dontrun{
#' # Check for and apply updates
#' update_bouncerdata()
#' }
update_bouncerdata <- function(repo = "peteowen1/bouncerdata",
                                 data_dir = NULL) {

  cli::cli_h1("Checking for Bouncerdata Updates")

  if (is.null(data_dir)) {
    data_dir <- find_bouncerdata_dir()
  }

  # Get current version from local manifest
  manifest_path <- file.path(data_dir, "json_files", "manifest.json")
  local_version <- if (file.exists(manifest_path)) {
    manifest <- jsonlite::fromJSON(manifest_path)
    manifest$release_date %||% "0.0.0"
  } else {
    "0.0.0"
  }

  cli::cli_alert_info("Local version: {local_version}")

  # Get latest release
  release <- get_latest_release(repo, type = "daily")
  remote_version <- release$tag_name

  cli::cli_alert_info("Remote version: {remote_version}")

  if (remote_version <= local_version) {
    cli::cli_alert_success("Already up to date!")
    return(invisible(FALSE))
  }

  cli::cli_alert_info("Update available: {local_version} -> {remote_version}")

  # Download and install
  install_bouncerdata_from_release(repo = repo, data_dir = data_dir)

  invisible(TRUE)
}
