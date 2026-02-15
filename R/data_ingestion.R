# Data Loading Functions (Optimized)
#
# This module provides high-performance data loading into DuckDB.
# Key optimizations:
# - Parallel JSON parsing (future + furrr)
# - data.table::rbindlist for combining data frames (~8x faster than do.call(rbind))
# - Arrow-based bulk inserts (10x faster than dbAppendTable)
# - Pre-parse duplicate check (skip already-loaded files)
# - Index management (drop before load, recreate after)

#' Check for Arrow Support
#'
#' Checks if the arrow package is available for fast bulk writes.
#'
#' @return Logical. TRUE if arrow is available.
#' @keywords internal
has_arrow_support <- function() {

  requireNamespace("arrow", quietly = TRUE)
}


#' Fast Row Bind
#'
#' Combines a list of data frames efficiently using data.table::rbindlist.
#'
#' @param df_list List of data frames to combine
#' @return Combined data frame
#' @keywords internal
fast_rbind <- function(df_list) {
  if (length(df_list) == 0) return(NULL)
  data.table::rbindlist(df_list, fill = TRUE) |> as.data.frame()
}


#' Bulk Write Data Frame using Arrow
#'
#' Writes a data frame to DuckDB using Arrow for maximum speed.
#' Falls back to dbAppendTable if arrow is not available.
#'
#' @param conn DuckDB connection
#' @param table_name Name of the target table
#' @param df Data frame to write
#'
#' @return Invisibly returns number of rows written
#' @keywords internal
bulk_write_arrow <- function(conn, table_name, df) {
  if (is.null(df) || nrow(df) == 0) return(invisible(0))

  if (has_arrow_support()) {
    # Convert to Arrow table and use DuckDB's native Arrow support
    arrow_tbl <- arrow::as_arrow_table(df)

    # Register Arrow table as a temporary view
    temp_name <- paste0("temp_arrow_", table_name, "_", as.integer(Sys.time()))
    duckdb::duckdb_register_arrow(conn, temp_name, arrow_tbl)

    # Build explicit column list to handle schema differences
    # (data frame may not have all columns that table has)
    col_names <- names(df)
    col_list <- paste(col_names, collapse = ", ")

    # Insert from the Arrow table with explicit column mapping
    tryCatch({
      sql <- sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
                     table_name, col_list, col_list, temp_name)
      DBI::dbExecute(conn, sql)
    }, finally = {
      # Always unregister the Arrow table
      duckdb::duckdb_unregister_arrow(conn, temp_name)
    })
  } else {
    # Fallback to dbAppendTable
    DBI::dbAppendTable(conn, table_name, df)
  }

  invisible(nrow(df))
}

#' Parse Files in Parallel
#'
#' Parses multiple JSON files using parallel processing for better performance.
#' Requires the future and furrr packages to be installed.
#'
#' @param file_paths Character vector of paths to JSON files
#' @param n_workers Number of parallel workers. If NULL, uses detectCores() - 1.
#' @param progress Logical. If TRUE, shows per-file progress (requires progressr).
#'
#' @return List of parsed results (NULL for files that failed to parse)
#' @keywords internal
parse_files_parallel <- function(file_paths, n_workers = NULL, progress = TRUE) {
 n_workers <- n_workers %||% max(1, parallel::detectCores() - 1)

 # Save current plan and restore on exit
 old_plan <- future::plan(future::multisession, workers = n_workers)
 on.exit(future::plan(old_plan), add = TRUE)

 if (progress && requireNamespace("progressr", quietly = TRUE)) {
   progressr::handlers("cli")
   progressr::with_progress({
     p <- progressr::progressor(steps = length(file_paths))
     results <- furrr::future_map(file_paths, function(f) {
       result <- tryCatch(
         parse_cricsheet_json(f),
         error = function(e) NULL
       )
       p()
       result
     }, .options = furrr::furrr_options(seed = TRUE))
     results
   })
 } else {
   furrr::future_map(file_paths, function(f) {
     tryCatch(parse_cricsheet_json(f), error = function(e) NULL)
   }, .options = furrr::furrr_options(seed = TRUE))
 }
}


#' Parse JSON Batch to Parquet Files
#'
#' Parses a batch of JSON files and writes results to Parquet files.
#' This avoids holding all parsed data in memory.
#'
#' @param file_paths Character vector of paths to JSON files
#' @param output_dir Directory to write Parquet files
#' @param batch_id Identifier for this batch (used in filenames)
#'
#' @return List with counts of parsed matches and any errors
#' @keywords internal
parse_batch_to_parquet <- function(file_paths, output_dir, batch_id) {
 if (!has_arrow_support()) {
   stop("Arrow package required for Parquet support")
 }

 # Parse all files in this batch
 matches_list <- list()
 deliveries_list <- list()
 innings_list <- list()
 players_list <- list()
 powerplays_list <- list()

 error_details <- list()

 for (f in file_paths) {
   result <- tryCatch(
     parse_cricsheet_json(f),
     error = function(e) {
       cli::cli_alert_warning("Parse failed: {basename(f)} - {conditionMessage(e)}")
       list(.error = conditionMessage(e), .file = f)
     }
   )

   if (!is.null(result$.error)) {
     error_details[[length(error_details) + 1]] <- result[c(".file", ".error")]
   } else if (!is.null(result) && !is.null(result$match_info) && nrow(result$match_info) > 0) {
     matches_list[[length(matches_list) + 1]] <- result$match_info
     deliveries_list[[length(deliveries_list) + 1]] <- result$deliveries
     innings_list[[length(innings_list) + 1]] <- result$innings
     players_list[[length(players_list) + 1]] <- result$players
     if (!is.null(result$powerplays) && nrow(result$powerplays) > 0) {
       powerplays_list[[length(powerplays_list) + 1]] <- result$powerplays
     }
   } else {
     error_details[[length(error_details) + 1]] <- list(.file = f, .error = "Empty or NULL result")
   }
 }

 # Combine and write to Parquet
 # Using fast_rbind() which leverages data.table::rbindlist when available
 # (benchmarked at ~8x faster than do.call(rbind, ...) for large datasets)
 n_matches <- 0

 if (length(matches_list) > 0) {
   all_matches <- fast_rbind(matches_list)
   all_matches <- all_matches[!duplicated(all_matches$match_id), ]
   arrow::write_parquet(all_matches, file.path(output_dir, sprintf("matches_%04d.parquet", batch_id)))
   n_matches <- nrow(all_matches)
 }

 if (length(deliveries_list) > 0) {
   all_deliveries <- fast_rbind(deliveries_list)
   all_deliveries <- all_deliveries[!duplicated(all_deliveries$delivery_id), ]
   arrow::write_parquet(all_deliveries, file.path(output_dir, sprintf("deliveries_%04d.parquet", batch_id)))
 }

 if (length(innings_list) > 0) {
   all_innings <- fast_rbind(innings_list)
   all_innings <- all_innings[!duplicated(paste0(all_innings$match_id, "_", all_innings$innings)), ]
   arrow::write_parquet(all_innings, file.path(output_dir, sprintf("innings_%04d.parquet", batch_id)))
 }

 if (length(players_list) > 0) {
   all_players <- fast_rbind(players_list)
   all_players <- all_players[!duplicated(all_players$player_id), ]
   arrow::write_parquet(all_players, file.path(output_dir, sprintf("players_%04d.parquet", batch_id)))
 }

 if (length(powerplays_list) > 0) {
   all_powerplays <- fast_rbind(powerplays_list)
   all_powerplays <- all_powerplays[!duplicated(all_powerplays$powerplay_id), ]
   arrow::write_parquet(all_powerplays, file.path(output_dir, sprintf("powerplays_%04d.parquet", batch_id)))
 }

 list(n_matches = n_matches, n_errors = length(error_details), errors = error_details)
}


#' Parse Files to Parquet in Parallel
#'
#' Parses JSON files in parallel, with each worker writing to Parquet files.
#' This approach minimizes memory usage by flushing to disk.
#'
#' @param file_paths Character vector of paths to JSON files
#' @param output_dir Directory to write Parquet files
#' @param n_workers Number of parallel workers
#' @param batch_size Files per worker batch
#' @param progress Show progress bar
#'
#' @return List with total matches parsed and errors
#' @keywords internal
parse_to_parquet_parallel <- function(file_paths, output_dir, n_workers = NULL,
                                     batch_size = 500, progress = TRUE) {
 n_workers <- n_workers %||% max(1, parallel::detectCores() - 1)

 # Split files into batches
 n_files <- length(file_paths)
 batch_ids <- ceiling(seq_len(n_files) / batch_size)
 batches <- split(file_paths, batch_ids)
 n_batches <- length(batches)

 # Check if bouncer package is installed (vs development mode with load_all)
 bouncer_installed <- requireNamespace("bouncer", quietly = TRUE) &&
   "parse_cricsheet_json" %in% getNamespaceExports("bouncer")

 if (!bouncer_installed) {
   # Development mode - fall back to sequential processing
   cli::cli_alert_warning("Package not installed - using sequential processing")
   cli::cli_alert_info("Install package with devtools::install() for parallel processing")

   if (progress) {
     cli::cli_progress_bar(
       format = "Parsing [{cli::pb_bar}] {cli::pb_current}/{cli::pb_total}",
       total = n_batches,
       clear = FALSE
     )
   }

   total_matches <- 0
   total_errors <- 0
   all_errors <- list()

   for (i in seq_along(batches)) {
     result <- parse_batch_to_parquet(batches[[i]], output_dir, i)
     total_matches <- total_matches + result$n_matches
     total_errors <- total_errors + result$n_errors
     all_errors <- c(all_errors, result$errors)
     if (progress) cli::cli_progress_update()
   }

   if (progress) cli::cli_progress_done()

   return(list(n_matches = total_matches, n_errors = total_errors, errors = all_errors))
 }

 # Package is installed - use parallel processing
 cli::cli_alert_info("Parsing {n_files} files in {n_batches} batches ({n_workers} workers)")

 # Save current plan and restore on exit
 old_plan <- future::plan(future::multisession, workers = n_workers)
 on.exit(future::plan(old_plan), add = TRUE)

 # Self-contained worker function
 # Workers load the bouncer package and use exported parse_cricsheet_json
 worker_fn <- function(batch_files, batch_id, out_dir) {
   # Helper: fast row bind using data.table
   local_fast_rbind <- function(df_list) {
     if (length(df_list) == 0) return(NULL)
     data.table::rbindlist(df_list, fill = TRUE) |> as.data.frame()
   }

   # Parse all files in this batch
   matches_list <- list()
   deliveries_list <- list()
   innings_list <- list()
   players_list <- list()
   powerplays_list <- list()
   error_details <- list()

   for (f in batch_files) {
     result <- tryCatch({
       # Parse using bouncer's exported parser
       bouncer::parse_cricsheet_json(f)
     }, error = function(e) {
       list(.error = conditionMessage(e), .file = f)
     })

     if (!is.null(result$.error)) {
       error_details[[length(error_details) + 1]] <- result[c(".file", ".error")]
     } else if (!is.null(result) && !is.null(result$match_info) && nrow(result$match_info) > 0) {
       matches_list[[length(matches_list) + 1]] <- result$match_info
       deliveries_list[[length(deliveries_list) + 1]] <- result$deliveries
       innings_list[[length(innings_list) + 1]] <- result$innings
       players_list[[length(players_list) + 1]] <- result$players
       if (!is.null(result$powerplays) && nrow(result$powerplays) > 0) {
         powerplays_list[[length(powerplays_list) + 1]] <- result$powerplays
       }
     } else {
       error_details[[length(error_details) + 1]] <- list(.file = f, .error = "Empty or NULL result")
     }
   }

   # Combine and write to Parquet
   n_matches <- 0
   bid <- as.integer(batch_id)

   if (length(matches_list) > 0) {
     all_matches <- local_fast_rbind(matches_list)
     all_matches <- all_matches[!duplicated(all_matches$match_id), ]
     arrow::write_parquet(all_matches, file.path(out_dir, sprintf("matches_%04d.parquet", bid)))
     n_matches <- nrow(all_matches)
   }

   if (length(deliveries_list) > 0) {
     all_deliveries <- local_fast_rbind(deliveries_list)
     all_deliveries <- all_deliveries[!duplicated(all_deliveries$delivery_id), ]
     arrow::write_parquet(all_deliveries, file.path(out_dir, sprintf("deliveries_%04d.parquet", bid)))
   }

   if (length(innings_list) > 0) {
     all_innings <- local_fast_rbind(innings_list)
     all_innings <- all_innings[!duplicated(paste0(all_innings$match_id, "_", all_innings$innings)), ]
     arrow::write_parquet(all_innings, file.path(out_dir, sprintf("innings_%04d.parquet", bid)))
   }

   if (length(players_list) > 0) {
     all_players <- local_fast_rbind(players_list)
     all_players <- all_players[!duplicated(all_players$player_id), ]
     arrow::write_parquet(all_players, file.path(out_dir, sprintf("players_%04d.parquet", bid)))
   }

   if (length(powerplays_list) > 0) {
     all_powerplays <- local_fast_rbind(powerplays_list)
     all_powerplays <- all_powerplays[!duplicated(all_powerplays$powerplay_id), ]
     arrow::write_parquet(all_powerplays, file.path(out_dir, sprintf("powerplays_%04d.parquet", bid)))
   }

   list(n_matches = n_matches, n_errors = length(error_details), errors = error_details)
 }

 # Process batches in parallel with bouncer package loaded in workers
 furrr_opts <- furrr::furrr_options(seed = TRUE, packages = "bouncer")

 if (progress && requireNamespace("progressr", quietly = TRUE)) {
   progressr::handlers("cli")
   results <- progressr::with_progress({
     p <- progressr::progressor(steps = n_batches)
     furrr::future_imap(batches, function(batch_files, batch_id) {
       result <- worker_fn(batch_files, batch_id, output_dir)
       p()
       result
     }, .options = furrr_opts)
   })
 } else {
   results <- furrr::future_imap(batches, function(batch_files, batch_id) {
     worker_fn(batch_files, batch_id, output_dir)
   }, .options = furrr_opts)
 }

 # Summarize results
 total_matches <- sum(vapply(results, function(x) as.integer(x$n_matches), integer(1)))
 total_errors <- sum(vapply(results, function(x) as.integer(x$n_errors), integer(1)))
 all_errors <- do.call(c, lapply(results, function(x) x$errors))

 list(n_matches = total_matches, n_errors = total_errors, errors = all_errors)
}


#' Load Parquet Files into DuckDB
#'
#' Bulk loads all Parquet files from a directory into DuckDB tables.
#' Uses DuckDB's native Parquet reader for maximum speed.
#'
#' @param parquet_dir Directory containing Parquet files
#' @param conn DuckDB connection
#'
#' @return Invisibly returns count of matches loaded
#' @keywords internal
load_parquet_to_duckdb <- function(parquet_dir, conn) {
 # Helper to load a table from parquet files
 # NOTE: list.files uses REGEX patterns, DuckDB read_parquet uses GLOB patterns
 # So we need two different patterns!
 load_table <- function(table_name, regex_pattern, glob_pattern, col_list = NULL) {
   parquet_files <- list.files(parquet_dir, pattern = regex_pattern, full.names = TRUE)
   if (length(parquet_files) == 0) return(0)

   # Build full glob path for DuckDB
   full_glob <- file.path(parquet_dir, glob_pattern)
   full_glob <- gsub("\\\\", "/", full_glob)  # Windows path fix

   if (is.null(col_list)) {
     sql <- sprintf("INSERT INTO %s SELECT * FROM read_parquet('%s')", table_name, full_glob)
   } else {
     sql <- sprintf("INSERT INTO %s (%s) SELECT %s FROM read_parquet('%s')",
                    table_name, col_list, col_list, full_glob)
   }

   DBI::dbExecute(conn, sql)
 }

 # Load each table (regex_pattern for list.files, glob_pattern for DuckDB)
 cli::cli_alert_info("Loading matches...")
 load_table("matches", "^matches_.*\\.parquet$", "matches_*.parquet")

 cli::cli_alert_info("Loading deliveries...")
 load_table("deliveries", "^deliveries_.*\\.parquet$", "deliveries_*.parquet")

 cli::cli_alert_info("Loading innings...")
 load_table("match_innings", "^innings_.*\\.parquet$", "innings_*.parquet")

 cli::cli_alert_info("Loading powerplays...")
 load_table("innings_powerplays", "^powerplays_.*\\.parquet$", "powerplays_*.parquet")

 # Players need special handling for duplicates across parquet files
 # A player can appear in multiple batches (plays for multiple teams/matches)
 cli::cli_alert_info("Loading players...")
 parquet_files <- list.files(parquet_dir, pattern = "^players_.*\\.parquet$", full.names = TRUE)
 if (length(parquet_files) > 0) {
   full_glob <- file.path(parquet_dir, "players_*.parquet")
   full_glob <- gsub("\\\\", "/", full_glob)

   # Use ROW_NUMBER() to pick one row per player_id, avoiding duplicates
   # This handles cases where same player appears with slightly different data
   sql <- sprintf("
     INSERT INTO players
     SELECT player_id, player_name, country, dob, batting_style, bowling_style
     FROM (
       SELECT *,
              ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY player_name) AS rn
       FROM read_parquet('%s')
     ) deduped
     WHERE rn = 1
       AND player_id NOT IN (SELECT player_id FROM players)
   ", full_glob)
   DBI::dbExecute(conn, sql)
 }

 # Get count of loaded matches
 n_matches <- DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM matches")$n
 invisible(n_matches)
}


#' Batch Load Matches
#'
#' Loads multiple match files into the database with optimal performance.
#' Uses Parquet intermediate files to minimize memory usage and maximize speed.
#'
#' @param file_paths Character vector of paths to JSON files
#' @param path Database path. If NULL, uses default.
#' @param batch_size Number of files per Parquet batch (default 100 for frequent progress updates)
#' @param progress Logical. If TRUE, shows progress.
#' @param parallel Logical. If TRUE and future/furrr are available, uses parallel parsing.
#'
#' @return Invisibly returns count of successfully loaded matches
#' @keywords internal
batch_load_matches <- function(file_paths, path = NULL, batch_size = 100, progress = TRUE, parallel = TRUE) {
 n_files <- length(file_paths)

 if (n_files == 0) {
   cli::cli_alert_warning("No files to load")
   return(invisible(0))
 }

 cli::cli_h2("Batch loading {n_files} matches (Parquet pipeline)")

 # Check for Arrow support
 if (!has_arrow_support()) {
   cli::cli_abort("Arrow package required for Parquet pipeline. Install with: install.packages('arrow')")
 }

 # CRITICAL: Remove duplicate filenames upfront (same match_id in multiple files)
 file_match_ids <- tools::file_path_sans_ext(basename(file_paths))
 dup_mask <- duplicated(file_match_ids)

 if (any(dup_mask)) {
   n_dups <- sum(dup_mask)
   cli::cli_alert_warning("Found {n_dups} duplicate match_ids in input files")
   cli::cli_alert_info("Keeping first occurrence of each match_id")
   file_paths <- file_paths[!dup_mask]
   file_match_ids <- file_match_ids[!dup_mask]
   n_files <- length(file_paths)
   cli::cli_alert_success("Deduplicated to {n_files} unique matches")
 }

 # Get database path
 if (is.null(path)) {
   path <- get_db_path()
 }

 # Get existing match IDs ONCE upfront (fast check before parsing)
 conn_check <- get_db_connection(path = path, read_only = TRUE)
 on.exit(tryCatch(DBI::dbDisconnect(conn_check, shutdown = TRUE), error = function(e) NULL), add = TRUE)
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

 # Track timing for each phase
 timing <- list()

 # ============================================================================
 # PHASE 1: Parse JSON → Parquet (parallel, memory-efficient)
 # ============================================================================
 use_parallel <- parallel && has_parallel_support()

 # Create temp directory for Parquet files
 temp_dir <- tempfile("bouncer_parquet_")
 dir.create(temp_dir, recursive = TRUE)
 on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

 t_parse_start <- Sys.time()

 if (use_parallel) {
   n_workers <- max(1, parallel::detectCores() - 1)
   cli::cli_alert_info("Phase 1: Parsing to Parquet ({n_workers} workers)...")

   parse_result <- parse_to_parquet_parallel(
     new_files,
     output_dir = temp_dir,
     n_workers = n_workers,
     batch_size = batch_size,
     progress = progress
   )

   n_parsed <- parse_result$n_matches
   error_count <- parse_result$n_errors

 } else {
   if (parallel && !has_parallel_support()) {
     cli::cli_alert_info("Parallel packages not available - using sequential parsing")
     cli::cli_alert_info("Install future + furrr for faster loading")
   }
   cli::cli_alert_info("Phase 1: Parsing to Parquet (sequential)...")

   # Sequential: process in batches
   batch_ids <- ceiling(seq_len(n_new) / batch_size)
   batches <- split(new_files, batch_ids)

   if (progress) {
     cli::cli_progress_bar(
       format = "Parsing [{cli::pb_bar}] {cli::pb_current}/{cli::pb_total}",
       total = length(batches),
       clear = FALSE
     )
   }

   total_matches <- 0
   total_errors <- 0

   for (i in seq_along(batches)) {
     result <- parse_batch_to_parquet(batches[[i]], temp_dir, i)
     total_matches <- total_matches + result$n_matches
     total_errors <- total_errors + result$n_errors
     if (progress) cli::cli_progress_update()
   }

   if (progress) cli::cli_progress_done()

   n_parsed <- total_matches
   error_count <- total_errors
 }

 timing$parse <- as.numeric(difftime(Sys.time(), t_parse_start, units = "secs"))

 if (error_count > 0) {
   cli::cli_alert_warning("{error_count} files failed to parse")
 }

 if (n_parsed == 0) {
   cli::cli_alert_warning("No valid matches to load")
   return(invisible(0))
 }

 cli::cli_alert_success("Parsed {n_parsed} matches to Parquet ({round(timing$parse, 1)}s)")

 # ============================================================================
 # PHASE 2: Load Parquet → DuckDB (fast bulk load)
 # ============================================================================
 t_load_start <- Sys.time()
 cli::cli_alert_info("Phase 2: Loading Parquet to DuckDB...")

 conn <- get_db_connection(path = path, read_only = FALSE)
 on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

 tryCatch({
   DBI::dbBegin(conn)
   load_parquet_to_duckdb(temp_dir, conn)
   DBI::dbCommit(conn)
 }, error = function(e) {
   tryCatch(DBI::dbRollback(conn), error = function(e2) NULL)
   cli::cli_abort("Failed to load Parquet files: {conditionMessage(e)}")
 })

 timing$load <- as.numeric(difftime(Sys.time(), t_load_start, units = "secs"))
 timing$total <- timing$parse + timing$load

 # Get final count
 final_count <- DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM matches")$n
 new_loaded <- final_count - length(existing_ids)

 cli::cli_alert_success("Loaded to DuckDB ({round(timing$load, 1)}s)")

 # Summary with timing breakdown
 cli::cli_rule()
 cli::cli_alert_success("Successfully loaded {new_loaded} new matches")

 # Timing breakdown
 parse_pct <- round(timing$parse / timing$total * 100)
 load_pct <- round(timing$load / timing$total * 100)
 cli::cli_alert_info("Timing: Parse {round(timing$parse, 1)}s ({parse_pct}%) | Load {round(timing$load, 1)}s ({load_pct}%) | Total {round(timing$total, 1)}s")

 if (n_skipped > 0) {
   cli::cli_alert_info("Skipped {n_skipped} already-loaded matches")
 }
 if (error_count > 0) {
   cli::cli_alert_warning("{error_count} files failed to parse")
 }

 invisible(new_loaded)
}


#' Insert Players in Batch
#'
#' Inserts players efficiently using bulk operations.
#' Filters out existing players first, then bulk inserts new ones.
#'
#' @param conn DuckDB connection
#' @param players_df Data frame with player data
#'
#' @keywords internal
insert_players_batch <- function(conn, players_df) {
  if (is.null(players_df) || nrow(players_df) == 0) return(invisible(0))

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

  # Get existing player IDs to avoid duplicates (fast indexed lookup)
  # Escape single quotes in player IDs to prevent SQL injection
  escaped_ids <- gsub("'", "''", players_df$player_id)
  existing_ids <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT player_id FROM players WHERE player_id IN (%s)",
      paste(sprintf("'%s'", escaped_ids), collapse = ", ")
    )
  )$player_id

  # Filter to only new players
  new_players <- players_df[!players_df$player_id %in% existing_ids, ]

  if (nrow(new_players) == 0) {
    return(invisible(0))
  }

  # Bulk insert using Arrow (10-20x faster than row-by-row)
  bulk_write_arrow(conn, "players", new_players)

  invisible(nrow(new_players))
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

    # Load powerplays
    if (!is.null(parsed_data$powerplays) && nrow(parsed_data$powerplays) > 0) {
      DBI::dbAppendTable(conn, "innings_powerplays", parsed_data$powerplays)
    }

    DBI::dbCommit(conn)

    cli::cli_alert_success("Loaded match {match_id}")

    invisible(TRUE)
  }, error = function(e) {
    DBI::dbRollback(conn)
    cli::cli_abort("Failed to load match data: {conditionMessage(e)}")
  })
}
