# Database Utility Functions
#
# Verification, deletion, and helper functions for database operations.
# Split from database_setup.R for better maintainability.

#' Verify Database
#'
#' Checks that the database exists and has the correct schema.
#'
#' @param path Character string specifying the database file path. If NULL,
#'   uses the default system data directory.
#' @param detailed Logical. If TRUE, returns detailed information about tables.
#'
#' @return Invisibly returns a list with database info
#' @export
#'
#' @examples
#' \dontrun{
#' # Quick verification
#' verify_database()
#'
#' # Detailed information
#' verify_database(detailed = TRUE)
#' }
verify_database <- function(path = NULL, detailed = FALSE) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_alert_danger("Database not found at {.file {path}}")
    cli::cli_alert_info("Run {.fn initialize_bouncer_database} to create it")
    return(invisible(NULL))
  }

  cli::cli_alert_success("Database found at {.file {path}}")

  check_duckdb_available()
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Get list of tables
  tables <- DBI::dbListTables(conn)
  expected_tables <- c("matches", "deliveries", "players", "match_innings", "innings_powerplays",
                       "match_metrics", "player_elo_history", "team_elo", "pre_match_features",
                       "pre_match_predictions", "simulation_results", "t20_player_elo",
                       "elo_calibration_metrics", "elo_normalization_log", "elo_calculation_params",
                       "t20_player_skill", "skill_calculation_params",
                       "venue_aliases", "t20_venue_skill", "odi_venue_skill", "test_venue_skill",
                       "venue_skill_calculation_params", "t20_team_skill", "odi_team_skill",
                       "test_team_skill", "team_skill_calculation_params",
                       "projection_params", "t20_score_projection", "odi_score_projection",
                       "test_score_projection",
                       "t20_3way_elo", "odi_3way_elo", "test_3way_elo",
                       "three_way_elo_params", "three_way_elo_drift_metrics")

  cli::cli_h2("Tables")
  for (tbl in expected_tables) {
    if (tbl %in% tables) {
      if (detailed) {
        row_count <- DBI::dbGetQuery(conn, paste0("SELECT COUNT(*) as n FROM ", tbl))$n
        cli::cli_alert_success("{tbl}: {row_count} rows")
      } else {
        cli::cli_alert_success("{tbl}")
      }
    } else {
      cli::cli_alert_danger("{tbl}: MISSING")
    }
  }

  info <- list(
    path = path,
    tables = tables,
    valid = all(expected_tables %in% tables)
  )

  if (detailed) {
    info$row_counts <- lapply(setNames(expected_tables, expected_tables), function(tbl) {
      if (tbl %in% tables) {
        DBI::dbGetQuery(conn, paste0("SELECT COUNT(*) as n FROM ", tbl))$n
      } else {
        0
      }
    })
  }

  invisible(info)
}


#' Delete Matches from Database
#'
#' Deletes matches and all related data from the database.
#' Used for incremental updates when match data has changed (e.g., Test match updates).
#'
#' This function removes data from all related tables:
#' - Core: matches, deliveries, match_innings
#' - Skill indices: t20/odi/test_player_skill, t20/odi/test_team_skill, t20/odi/test_venue_skill
#' - Ratings: team_elo, t20_player_elo
#' - Predictions: pre_match_features, pre_match_predictions
#' - Simulations: simulation_results (for matches)
#'
#' @param match_ids Character vector of match IDs to delete
#' @param conn DuckDB connection (must have write access)
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return Invisibly returns the number of matches deleted
#' @keywords internal
delete_matches_from_db <- function(match_ids, conn, verbose = TRUE) {
  if (length(match_ids) == 0) {
    if (verbose) cli::cli_alert_info("No matches to delete")
    return(invisible(0))
  }

  n_matches <- length(match_ids)
  if (verbose) cli::cli_alert_info("Deleting {n_matches} matches and related data...")

  # Build SQL IN clause
  match_ids_sql <- paste0("'", match_ids, "'", collapse = ", ")

  # Tables to delete from, in order (to handle foreign keys if any)
  tables_with_match_id <- c(
    # Core tables
    "deliveries",
    "match_innings",
    "match_metrics",
    # Skill index tables
    "t20_player_skill",
    "odi_player_skill",
    "test_player_skill",
    "t20_team_skill",
    "odi_team_skill",
    "test_team_skill",
    "t20_venue_skill",
    "odi_venue_skill",
    "test_venue_skill",
    # ELO tables
    "t20_player_elo",
    "team_elo",
    # Prediction tables
    "pre_match_features",
    "pre_match_predictions",
    # Main matches table (last)
    "matches"
  )

  # Get list of existing tables
  existing_tables <- DBI::dbListTables(conn)

  total_deleted <- 0

  for (tbl in tables_with_match_id) {
    if (!tbl %in% existing_tables) {
      next
    }

    sql <- sprintf("DELETE FROM %s WHERE match_id IN (%s)", tbl, match_ids_sql)
    n_deleted <- tryCatch({
      DBI::dbExecute(conn, sql)
    }, error = function(e) {
      if (verbose) cli::cli_alert_warning("Could not delete from {tbl}: {e$message}")
      0
    })

    if (n_deleted > 0 && verbose) {
      cli::cli_alert_success("Deleted {n_deleted} rows from {tbl}")
    }
    total_deleted <- total_deleted + n_deleted
  }

  if (verbose) {
    cli::cli_alert_success("Deleted {n_matches} matches ({total_deleted} total rows)")
  }

  invisible(n_matches)
}


#' Create Dual ELO Table
#'
#' Creates a table to store dual ELO ratings (survival/scoring for batters,
#' strike/economy for bowlers). Can be joined to deliveries via delivery_id.
#'
#' @param path Character. Database file path. If NULL, uses default.
#' @param overwrite Logical. If TRUE, drops and recreates the table. Default FALSE.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
create_dual_elo_table <- function(path = NULL, overwrite = FALSE) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_alert_danger("Database not found at {.file {path}}")
    cli::cli_alert_info("Run {.fn initialize_bouncer_database} first")
    return(invisible(FALSE))
  }

  check_duckdb_available()
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Check if table exists
  tables <- DBI::dbListTables(conn)

  if ("dual_elos" %in% tables) {
    if (!overwrite) {
      cli::cli_alert_warning("Table 'dual_elos' already exists")
      cli::cli_alert_info("Use overwrite = TRUE to replace it")
      return(invisible(FALSE))
    }
    cli::cli_alert_warning("Dropping existing 'dual_elos' table")
    DBI::dbExecute(conn, "DROP TABLE dual_elos")
  }

  # Create dual_elos table
  cli::cli_alert_info("Creating 'dual_elos' table...")

  DBI::dbExecute(conn, "
    CREATE TABLE dual_elos (
      delivery_id VARCHAR PRIMARY KEY,
      batter_survival_elo DOUBLE,
      batter_scoring_elo DOUBLE,
      bowler_strike_elo DOUBLE,
      bowler_economy_elo DOUBLE
    )
  ")

  # Create index on delivery_id for fast joins
  DBI::dbExecute(conn, "CREATE INDEX idx_dual_elos_delivery ON dual_elos(delivery_id)")

  cli::cli_alert_success("Created 'dual_elos' table")
  cli::cli_alert_info("Join to deliveries: SELECT * FROM deliveries d JOIN dual_elos e ON d.delivery_id = e.delivery_id")

  invisible(TRUE)
}


#' Insert Dual ELO Data
#'
#' Inserts dual ELO ratings into the dual_elos table.
#'
#' @param dt data.table/data.frame with columns: delivery_id, batter_survival_elo,
#'           batter_scoring_elo, bowler_strike_elo, bowler_economy_elo
#' @param path Character. Database file path. If NULL, uses default.
#' @param batch_size Integer. Number of rows to insert per transaction.
#'
#' @return Invisibly returns the number of rows inserted
#' @keywords internal
insert_dual_elos <- function(dt, path = NULL, batch_size = 10000) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  check_duckdb_available()
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Check table exists
  if (!"dual_elos" %in% DBI::dbListTables(conn)) {
    cli::cli_alert_danger("Table 'dual_elos' does not exist")
    cli::cli_alert_info("Run {.fn create_dual_elo_table} first")
    return(invisible(0))
  }

  # Required columns
  required_cols <- c("delivery_id", "batter_survival_elo", "batter_scoring_elo",
                     "bowler_strike_elo", "bowler_economy_elo")
  missing_cols <- setdiff(required_cols, names(dt))
  if (length(missing_cols) > 0) {
    cli::cli_alert_danger("Missing columns: {paste(missing_cols, collapse = ', ')}")
    return(invisible(0))
  }

  # Select only required columns
  dt_insert <- dt[, required_cols, with = FALSE]

  # Insert in batches
  n_total <- nrow(dt_insert)
  n_batches <- ceiling(n_total / batch_size)

  cli::cli_progress_bar("Inserting dual ELOs", total = n_batches)

  rows_inserted <- 0
  for (i in seq_len(n_batches)) {
    start_idx <- (i - 1) * batch_size + 1
    end_idx <- min(i * batch_size, n_total)

    batch <- dt_insert[start_idx:end_idx, ]

    DBI::dbWriteTable(conn, "dual_elos", batch, append = TRUE)
    rows_inserted <- rows_inserted + nrow(batch)

    cli::cli_progress_update()
  }

  cli::cli_progress_done()
  cli::cli_alert_success("Inserted {rows_inserted} rows into 'dual_elos'")

  invisible(rows_inserted)
}


#' Get Database Size Information
#'
#' Returns size information for the database and its tables.
#'
#' @param path Character. Database file path. If NULL, uses default.
#'
#' @return List with file_size and table_sizes
#' @keywords internal
get_database_size <- function(path = NULL) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_alert_danger("Database not found at {.file {path}}")
    return(NULL)
  }

  file_size <- file.info(path)$size

  # Format file size
  format_size <- function(bytes) {
    if (bytes >= 1e9) {
      sprintf("%.2f GB", bytes / 1e9)
    } else if (bytes >= 1e6) {
      sprintf("%.2f MB", bytes / 1e6)
    } else if (bytes >= 1e3) {
      sprintf("%.2f KB", bytes / 1e3)
    } else {
      sprintf("%d bytes", bytes)
    }
  }

  cli::cli_alert_info("Database size: {format_size(file_size)}")

  list(
    path = path,
    file_size_bytes = file_size,
    file_size_formatted = format_size(file_size)
  )
}


# Note: check_duckdb_available() is defined in database_connection.R
# It checks both DBI and duckdb packages for complete dependency validation
