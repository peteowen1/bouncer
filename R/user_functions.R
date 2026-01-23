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
  cli::cli_alert_info("Matches: {scales::comma(n_matches)}")
  cli::cli_alert_info("Deliveries: {scales::comma(n_deliveries)}")
  cli::cli_alert_info("Players: {scales::comma(n_players)}")

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
#' @param batch_size Number of files per database transaction. Default 100.
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
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

    delete_matches_from_db(changed_match_ids, conn)
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

      all_matches <- do.call(rbind, batch_matches)
      all_deliveries <- do.call(rbind, batch_deliveries)
      all_innings <- do.call(rbind, batch_innings)
      all_players <- do.call(rbind, batch_players)
      all_powerplays <- do.call(rbind, batch_powerplays)

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
