# Data Loading Functions (Optimized)
#
# This module provides high-performance data loading into DuckDB.
# Key optimizations:
# - Single database connection for entire batch
# - Batch transactions (100 files per transaction)
# - Pre-parse duplicate check (skip already-loaded files)
# - Bulk inserts instead of row-by-row

#' Batch Load Matches
#'
#' Loads multiple match files into the database with optimal performance.
#' Uses single connection, batch transactions, and pre-parse duplicate detection.
#'
#' @param file_paths Character vector of paths to JSON files
#' @param path Database path. If NULL, uses default.
#' @param batch_size Number of files per transaction (default 100)
#' @param progress Logical. If TRUE, shows progress.
#'
#' @return Invisibly returns count of successfully loaded matches
#' @keywords internal
batch_load_matches <- function(file_paths, path = NULL, batch_size = 500, progress = TRUE) {
  n_files <- length(file_paths)

  if (n_files == 0) {
    cli::cli_alert_warning("No files to load")
    return(invisible(0))
  }

  cli::cli_h2("Batch loading {n_files} matches (optimized)")

  # CRITICAL: Remove duplicate filenames upfront (same match_id in multiple files)
  file_match_ids <- tools::file_path_sans_ext(basename(file_paths))
  dup_mask <- duplicated(file_match_ids)

  if (any(dup_mask)) {
    n_dups <- sum(dup_mask)
    cli::cli_alert_warning("Found {n_dups} duplicate match_ids in input files")
    cli::cli_alert_info("Keeping first occurrence of each match_id")

    # Keep only first occurrence of each match_id
    file_paths <- file_paths[!dup_mask]
    file_match_ids <- file_match_ids[!dup_mask]
    n_files <- length(file_paths)

    cli::cli_alert_success("Deduplicated to {n_files} unique matches")
  }

  # Get database path (we'll reconnect for each batch to avoid transaction state issues)
  if (is.null(path)) {
    path <- get_db_path()
  }

  # Get existing match IDs ONCE upfront (fast check before parsing)
  conn_check <- get_db_connection(path = path, read_only = TRUE)
  existing_matches <- DBI::dbGetQuery(conn_check, "SELECT match_id FROM matches")
  DBI::dbDisconnect(conn_check, shutdown = TRUE)
  existing_ids <- existing_matches$match_id

  # Filter to only new files (check BEFORE parsing - saves time!)
  new_mask <- !(file_match_ids %in% existing_ids)
  new_files <- file_paths[new_mask]
  n_new <- length(new_files)
  n_skipped <- n_files - n_new

  if (n_skipped > 0) {
    cli::cli_alert_info("Skipping {n_skipped} already-loaded matches")
  }

  if (n_new == 0) {
    cli::cli_alert_success("No new matches to load")
    return(invisible(0))
  }

  cli::cli_alert_info("Loading {n_new} new matches")

  # Process in batches for transaction efficiency
  success_count <- 0
  error_count <- 0
  error_files <- character()
  files_processed <- 0

  batches <- split(seq_along(new_files), ceiling(seq_along(new_files) / batch_size))

  # Setup progress bar showing actual file counts
  if (progress) {
    cli::cli_progress_bar(
      format = "Loading matches [{cli::pb_bar}] {files_processed}/{n_new} ({cli::pb_percent}) | ETA: {cli::pb_eta}",
      total = n_new,
      clear = FALSE
    )
  }

  for (batch_num in seq_along(batches)) {
    batch_indices <- batches[[batch_num]]
    batch_files <- new_files[batch_indices]

    # Collect all parsed data for this batch
    batch_matches <- vector("list", length(batch_files))
    batch_deliveries <- vector("list", length(batch_files))
    batch_innings <- vector("list", length(batch_files))
    batch_players <- vector("list", length(batch_files))
    valid_count <- 0

    for (i in seq_along(batch_files)) {
      tryCatch({
        parsed <- parse_cricsheet_json(batch_files[i])

        # Only include if we got valid data
        if (nrow(parsed$match_info) > 0) {
          valid_count <- valid_count + 1
          batch_matches[[valid_count]] <- parsed$match_info
          batch_deliveries[[valid_count]] <- parsed$deliveries
          batch_innings[[valid_count]] <- parsed$innings
          batch_players[[valid_count]] <- parsed$players
        }
      }, error = function(e) {
        if (progress) {
          cli::cli_alert_warning("Error parsing {basename(batch_files[i])}: {conditionMessage(e)}")
        }
        error_count <<- error_count + 1
        error_files <<- c(error_files, basename(batch_files[i]))
      })
    }

    all_matches <- data.frame()
    all_deliveries <- data.frame()
    all_innings <- data.frame()
    all_players <- data.frame()

    # Trim lists to valid entries
    if (valid_count > 0) {
      batch_matches <- batch_matches[1:valid_count]
      batch_deliveries <- batch_deliveries[1:valid_count]
      batch_innings <- batch_innings[1:valid_count]
      batch_players <- batch_players[1:valid_count]

      # Combine into single data frames (efficient rbind since we're doing it once per batch)
      all_matches <- do.call(rbind, batch_matches)
      all_deliveries <- do.call(rbind, batch_deliveries)
      all_innings <- do.call(rbind, batch_innings)
      all_players <- do.call(rbind, batch_players)

      # Deduplicate based on primary keys (in case same file appears multiple times in batch)
      all_matches <- all_matches[!duplicated(all_matches$match_id), ]
      all_deliveries <- all_deliveries[!duplicated(all_deliveries$delivery_id), ]
      all_innings <- all_innings[!duplicated(paste0(all_innings$match_id, "_", all_innings$innings)), ]
      all_players <- all_players[!duplicated(all_players$player_id), ]

      # Open connection for this batch
      conn_batch <- get_db_connection(path = path, read_only = FALSE)

      # Single transaction for entire batch
      # If ANY insert fails, entire batch rolls back
      batch_succeeded <- FALSE

      tryCatch({
        DBI::dbBegin(conn_batch)

        # Insert matches
        if (!is.null(all_matches) && nrow(all_matches) > 0) {
          DBI::dbAppendTable(conn_batch, "matches", all_matches)
        }

        # Insert deliveries
        if (!is.null(all_deliveries) && nrow(all_deliveries) > 0) {
          DBI::dbAppendTable(conn_batch, "deliveries", all_deliveries)
        }

        # Insert innings
        if (!is.null(all_innings) && nrow(all_innings) > 0) {
          DBI::dbAppendTable(conn_batch, "match_innings", all_innings)
        }

        # Insert players (handles duplicates with INSERT OR IGNORE)
        if (!is.null(all_players) && nrow(all_players) > 0) {
          insert_players_batch(conn_batch, all_players)
        }

        DBI::dbCommit(conn_batch)
        success_count <- success_count + valid_count
        batch_succeeded <- TRUE

      }, error = function(e) {
        # Rollback on any error
        tryCatch(DBI::dbRollback(conn_batch), error = function(e2) NULL)

        # Show clear error message
        cli::cli_alert_danger("Batch {batch_num}/{length(batches)} FAILED")
        cli::cli_alert_danger("Error: {conditionMessage(e)}")
        cli::cli_alert_info("You can restart with fresh=FALSE to skip already-loaded matches")

        error_count <<- error_count + valid_count
        error_files <<- c(error_files, basename(batch_files))

        # Stop processing - don't continue to next batch
        stop(paste0("Batch loading failed at batch ", batch_num, ". ", conditionMessage(e)))
      })

      # Close batch connection
      DBI::dbDisconnect(conn_batch, shutdown = TRUE)

      # If batch failed, we've already stopped above
    }

    # Update progress bar after each batch
    files_processed <- files_processed + length(batch_files)
    if (progress) {
      cli::cli_progress_update(set = files_processed)
    }
  }

  # Close progress bar
  if (progress) {
    cli::cli_progress_done()
  }

  # Summary
  cli::cli_alert_success("Successfully loaded {success_count} new matches")
  if (n_skipped > 0) {
    cli::cli_alert_info("Skipped {n_skipped} already-loaded matches")
  }
  if (error_count > 0) {
    cli::cli_alert_warning("{error_count} matches failed to load")
    if (length(error_files) > 0 && length(error_files) <= 10) {
      cli::cli_alert_info("Failed files: {paste(error_files, collapse = ', ')}")
    }
  }

  invisible(success_count)
}


#' Insert Players in Batch
#'
#' Inserts players efficiently, skipping duplicates using INSERT OR IGNORE.
#'
#' @param conn DuckDB connection
#' @param players_df Data frame with player data
#'
#' @keywords internal
insert_players_batch <- function(conn, players_df) {
  if (is.null(players_df) || nrow(players_df) == 0) return()

  # Remove duplicates within batch
  players_df <- players_df[!duplicated(players_df$player_id), ]

  # Ensure all required columns exist
  required_cols <- c("player_id", "player_name", "country", "dob", "batting_style", "bowling_style")
  for (col in required_cols) {
    if (!col %in% names(players_df)) {
      players_df[[col]] <- NA
    }
  }

  # Select only required columns in correct order
  players_df <- players_df[, required_cols]

  # Use INSERT OR IGNORE to handle duplicates
  # DuckDB syntax: INSERT OR IGNORE INTO table VALUES (...)
  for (i in 1:nrow(players_df)) {
    tryCatch({
      DBI::dbExecute(conn,
        "INSERT OR IGNORE INTO players (player_id, player_name, country, dob, batting_style, bowling_style)
         VALUES (?, ?, ?, ?, ?, ?)",
        params = unname(as.list(players_df[i, ]))
      )
    }, error = function(e) {
      # Silently ignore duplicate key errors, re-throw others
      if (!grepl("duplicate key|UNIQUE constraint", conditionMessage(e), ignore.case = TRUE)) {
        stop(e)
      }
    })
  }
}


#' Load Single Match Data
#'
#' Loads parsed cricket match data into the database.
#' For bulk loading, use batch_load_matches() instead.
#'
#' @param parsed_data List returned from parse_cricsheet_json
#' @param path Database path. If NULL, uses default.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
load_match_data <- function(parsed_data, path = NULL) {
  conn <- get_db_connection(path = path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Check if match already exists
  match_id <- parsed_data$match_info$match_id[1]
  existing <- DBI::dbGetQuery(
    conn,
    "SELECT match_id FROM matches WHERE match_id = ?",
    params = list(match_id)
  )

  if (nrow(existing) > 0) {
    cli::cli_alert_info("Match {match_id} already exists, skipping")
    return(invisible(FALSE))
  }

  # Single transaction
  DBI::dbBegin(conn)

  tryCatch({
    # Load matches
    if (nrow(parsed_data$match_info) > 0) {
      DBI::dbAppendTable(conn, "matches", parsed_data$match_info)
    }

    # Load deliveries
    if (nrow(parsed_data$deliveries) > 0) {
      DBI::dbAppendTable(conn, "deliveries", parsed_data$deliveries)
    }

    # Load innings
    if (nrow(parsed_data$innings) > 0) {
      DBI::dbAppendTable(conn, "match_innings", parsed_data$innings)
    }

    # Load players
    if (nrow(parsed_data$players) > 0) {
      insert_players_batch(conn, parsed_data$players)
    }

    DBI::dbCommit(conn)

    cli::cli_alert_success("Loaded match {match_id}")

    invisible(TRUE)
  }, error = function(e) {
    DBI::dbRollback(conn)
    cli::cli_abort("Failed to load match data: {conditionMessage(e)}")
  })
}
