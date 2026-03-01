# Database Maintenance Functions
#
# Utilities for database verification, index management, and maintenance.

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
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  # Get all tables across all schemas via information_schema
  all_tables <- DBI::dbGetQuery(conn,
    "SELECT table_schema, table_name,
            CASE WHEN table_schema = 'main' THEN table_name
                 ELSE table_schema || '.' || table_name END AS qualified_name
     FROM information_schema.tables
     WHERE table_schema NOT IN ('information_schema', 'pg_catalog')")
  all_qualified <- all_tables$qualified_name

  expected_tables <- c(
    # Cricsheet schema
    "cricsheet.matches", "cricsheet.deliveries", "cricsheet.players",
    "cricsheet.match_innings", "cricsheet.innings_powerplays",
    # Main schema (ratings, skills, predictions)
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
    "three_way_elo_params", "three_way_elo_drift_metrics",
    # Cricinfo schema
    "cricinfo.matches", "cricinfo.balls", "cricinfo.innings", "cricinfo.fixtures"
  )

  cli::cli_h2("Tables")
  for (tbl in expected_tables) {
    if (tbl %in% all_qualified) {
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
    tables = all_qualified,
    valid = all(expected_tables %in% all_qualified)
  )

  if (detailed) {
    info$row_counts <- lapply(setNames(expected_tables, expected_tables), function(tbl) {
      if (tbl %in% all_qualified) {
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
#' - Core: cricsheet.deliveries, cricsheet.match_innings, cricsheet.innings_powerplays, match_metrics
#' - Skill indices: t20/odi/test_player_skill, t20/odi/test_team_skill, t20/odi/test_venue_skill
#' - Ratings: team_elo, t20_player_elo
#' - Predictions: pre_match_features, pre_match_predictions
#' - Main: cricsheet.matches (last)
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
  match_ids_sql <- paste0("'", escape_sql_quotes(match_ids), "'", collapse = ", ")

  # Tables to delete from, in order (to handle foreign keys if any)
  tables_with_match_id <- c(
    # Core tables (cricsheet schema)
    "cricsheet.deliveries",
    "cricsheet.match_innings",
    "cricsheet.innings_powerplays",
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
    "cricsheet.matches"
  )

  # Get list of existing tables (schema-aware)
  existing_tables <- DBI::dbGetQuery(conn,
    "SELECT CASE WHEN table_schema = 'main' THEN table_name
                 ELSE table_schema || '.' || table_name END AS qualified_name
     FROM information_schema.tables
     WHERE table_schema NOT IN ('information_schema', 'pg_catalog')")$qualified_name

  total_deleted <- 0
  failed_tables <- character(0)

  for (tbl in tables_with_match_id) {
    if (!tbl %in% existing_tables) {
      next
    }

    sql <- sprintf("DELETE FROM %s WHERE match_id IN (%s)", tbl, match_ids_sql)
    n_deleted <- tryCatch({
      DBI::dbExecute(conn, sql)
    }, error = function(e) {
      if (verbose) cli::cli_alert_warning("Could not delete from {tbl}: {e$message}")
      failed_tables[[length(failed_tables) + 1L]] <<- tbl
      0
    })

    if (n_deleted > 0 && verbose) {
      cli::cli_alert_success("Deleted {n_deleted} rows from {tbl}")
    }
    total_deleted <- total_deleted + n_deleted
  }

  if (length(failed_tables) > 0) {
    if (verbose) cli::cli_alert_warning("Failed to delete from {length(failed_tables)} table{?s}: {paste(failed_tables, collapse = ', ')}")
  }

  if (verbose) {
    if (length(failed_tables) == 0) {
      cli::cli_alert_success("Deleted {n_matches} matches ({total_deleted} total rows)")
    } else {
      cli::cli_alert_warning("Partially deleted {n_matches} matches ({total_deleted} rows, {length(failed_tables)} table{?s} failed)")
    }
  }

  invisible(n_matches)
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


# ============================================================================
# INDEX MANAGEMENT
# ============================================================================

drop_bulk_load_indexes <- function(conn, verbose = TRUE) {
  if (verbose) cli::cli_alert_info("Dropping indexes for bulk loading...")

  # Get all existing indexes
  indexes <- tryCatch({
    DBI::dbGetQuery(conn, "SELECT index_name FROM duckdb_indexes()")
  }, error = function(e) {
    if (verbose) cli::cli_alert_warning("Could not query indexes: {e$message}")
    data.frame(index_name = character(0))
  })

  # Indexes on core tables that slow down bulk inserts
  core_index_patterns <- c(
    "idx_matches_",
    "idx_deliveries_",
    "idx_players_"
  )

  dropped_count <- 0
  for (idx_name in indexes$index_name) {
    # Check if this is a core table index
    is_core <- any(sapply(core_index_patterns, function(p) grepl(p, idx_name)))

    if (is_core) {
      tryCatch({
        DBI::dbExecute(conn, sprintf("DROP INDEX IF EXISTS %s", idx_name))
        dropped_count <- dropped_count + 1
      }, error = function(e) {
        if (verbose) cli::cli_alert_warning("Could not drop index {idx_name}: {e$message}")
      })
    }
  }

  if (verbose) cli::cli_alert_success("Dropped {dropped_count} indexes")
  invisible(TRUE)
}


#' Create Database Indexes
#'
#' Creates indexes on key columns for query performance.
#'
#' @param conn A DuckDB connection object
#' @param core_only Logical. If TRUE, only creates indexes on core tables
#'   (matches, deliveries, players). Default FALSE creates all indexes.
#' @param verbose Logical. If TRUE, shows progress for each index group. Default TRUE.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
create_indexes <- function(conn, core_only = FALSE, verbose = TRUE) {
  if (verbose) cli::cli_h2("Creating indexes")

  failed_indexes <- character(0)

  # Helper to create an index, catching errors per-statement
  safe_index <- function(sql) {
    tryCatch(
      DBI::dbExecute(conn, sql),
      error = function(e) {
        failed_indexes[[length(failed_indexes) + 1L]] <<- conditionMessage(e)
        NULL
      }
    )
  }

  # Helper to log index creation
  log_index <- function(name) {
    if (verbose) cli::cli_alert_info("Creating {name} indexes...")
  }

  # Matches indexes (cricsheet schema)
  log_index("cricsheet.matches")
  safe_index("CREATE INDEX IF NOT EXISTS idx_matches_date ON cricsheet.matches(match_date)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_matches_venue ON cricsheet.matches(venue)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_matches_type ON cricsheet.matches(match_type)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_matches_season ON cricsheet.matches(season)")

  # Deliveries indexes (cricsheet schema)
  log_index("cricsheet.deliveries")
  safe_index("CREATE INDEX IF NOT EXISTS idx_deliveries_match ON cricsheet.deliveries(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_deliveries_batter ON cricsheet.deliveries(batter_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_deliveries_bowler ON cricsheet.deliveries(bowler_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_deliveries_type ON cricsheet.deliveries(match_type)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_deliveries_date ON cricsheet.deliveries(match_date)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_deliveries_venue ON cricsheet.deliveries(venue)")

  # Players indexes (cricsheet schema)
  log_index("cricsheet.players")
  safe_index("CREATE INDEX IF NOT EXISTS idx_players_name ON cricsheet.players(player_name)")

  # Innings powerplays indexes (cricsheet schema)
  log_index("cricsheet.innings_powerplays")
  safe_index("CREATE INDEX IF NOT EXISTS idx_powerplays_match ON cricsheet.innings_powerplays(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_powerplays_innings ON cricsheet.innings_powerplays(match_id, innings)")

  # Player ELO indexes
  log_index("player_elo_history")
  safe_index("CREATE INDEX IF NOT EXISTS idx_elo_player_date ON player_elo_history(player_id, match_date)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_elo_match_type ON player_elo_history(match_type)")

  # Team ELO indexes
  log_index("team_elo")
  safe_index("CREATE INDEX IF NOT EXISTS idx_team_elo_date ON team_elo(team_id, match_date)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_team_elo_event ON team_elo(event_name)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_team_elo_type ON team_elo(match_type)")

  # Pre-match features indexes
  log_index("pre_match_features")
  safe_index("CREATE INDEX IF NOT EXISTS idx_prematch_date ON pre_match_features(match_date)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_prematch_event ON pre_match_features(event_name)")

  # Pre-match predictions indexes
  log_index("pre_match_predictions")
  safe_index("CREATE INDEX IF NOT EXISTS idx_predictions_match ON pre_match_predictions(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_predictions_date ON pre_match_predictions(prediction_date)")

  # Simulation results indexes
  log_index("simulation_results")
  safe_index("CREATE INDEX IF NOT EXISTS idx_simulation_type ON simulation_results(simulation_type)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_simulation_event ON simulation_results(event_name)")

  # T20 player ELO indexes
  log_index("t20_player_elo")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_elo_match ON t20_player_elo(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_elo_batter ON t20_player_elo(batter_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_elo_bowler ON t20_player_elo(bowler_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_elo_date ON t20_player_elo(match_date)")

  # T20 player skill indexes
  log_index("t20_player_skill")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_skill_match ON t20_player_skill(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_skill_batter ON t20_player_skill(batter_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_skill_bowler ON t20_player_skill(bowler_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_skill_date ON t20_player_skill(match_date)")

  # Venue aliases index
  log_index("venue_aliases")
  safe_index("CREATE INDEX IF NOT EXISTS idx_venue_aliases_canonical ON venue_aliases(canonical_venue)")

  # T20 venue skill indexes
  log_index("t20_venue_skill")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_venue_skill_match ON t20_venue_skill(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_venue_skill_venue ON t20_venue_skill(venue)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_venue_skill_date ON t20_venue_skill(match_date)")

  # ODI venue skill indexes
  log_index("odi_venue_skill")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_venue_skill_match ON odi_venue_skill(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_venue_skill_venue ON odi_venue_skill(venue)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_venue_skill_date ON odi_venue_skill(match_date)")

  # Test venue skill indexes
  log_index("test_venue_skill")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_venue_skill_match ON test_venue_skill(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_venue_skill_venue ON test_venue_skill(venue)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_venue_skill_date ON test_venue_skill(match_date)")

  # T20 team skill indexes
  log_index("t20_team_skill")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_team_skill_match ON t20_team_skill(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_team_skill_batting ON t20_team_skill(batting_team_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_team_skill_bowling ON t20_team_skill(bowling_team_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_team_skill_date ON t20_team_skill(match_date)")

  # ODI team skill indexes
  log_index("odi_team_skill")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_team_skill_match ON odi_team_skill(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_team_skill_batting ON odi_team_skill(batting_team_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_team_skill_bowling ON odi_team_skill(bowling_team_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_team_skill_date ON odi_team_skill(match_date)")

  # Test team skill indexes
  log_index("test_team_skill")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_team_skill_match ON test_team_skill(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_team_skill_batting ON test_team_skill(batting_team_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_team_skill_bowling ON test_team_skill(bowling_team_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_team_skill_date ON test_team_skill(match_date)")

  # Projection params indexes
  log_index("projection_params")
  safe_index("CREATE INDEX IF NOT EXISTS idx_projection_params_format ON projection_params(format)")

  # T20 score projection indexes
  log_index("t20_score_projection")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_proj_match ON t20_score_projection(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_proj_date ON t20_score_projection(match_date)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_proj_team ON t20_score_projection(batting_team_id)")

  # ODI score projection indexes
  log_index("odi_score_projection")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_proj_match ON odi_score_projection(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_proj_date ON odi_score_projection(match_date)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_proj_team ON odi_score_projection(batting_team_id)")

  # Test score projection indexes
  log_index("test_score_projection")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_proj_match ON test_score_projection(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_proj_date ON test_score_projection(match_date)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_proj_team ON test_score_projection(batting_team_id)")

  # T20 3-way ELO indexes
  log_index("t20_3way_elo")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_3way_match ON t20_3way_elo(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_3way_batter ON t20_3way_elo(batter_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_3way_bowler ON t20_3way_elo(bowler_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_3way_venue ON t20_3way_elo(venue)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_t20_3way_date ON t20_3way_elo(match_date)")

  # ODI 3-way ELO indexes
  log_index("odi_3way_elo")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_3way_match ON odi_3way_elo(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_3way_batter ON odi_3way_elo(batter_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_3way_bowler ON odi_3way_elo(bowler_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_3way_venue ON odi_3way_elo(venue)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_odi_3way_date ON odi_3way_elo(match_date)")

  # Test 3-way ELO indexes
  log_index("test_3way_elo")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_3way_match ON test_3way_elo(match_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_3way_batter ON test_3way_elo(batter_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_3way_bowler ON test_3way_elo(bowler_id)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_3way_venue ON test_3way_elo(venue)")
  safe_index("CREATE INDEX IF NOT EXISTS idx_test_3way_date ON test_3way_elo(match_date)")

  if (length(failed_indexes) > 0) {
    cli::cli_alert_warning("{length(failed_indexes)} index{?es} failed to create")
    if (verbose) {
      for (msg in unique(failed_indexes)) cli::cli_alert_warning("  {msg}")
    }
    cli::cli_alert_info("Indexes partially created ({length(failed_indexes)} failures)")
    invisible(FALSE)
  } else {
    cli::cli_alert_success("Indexes created successfully")
    invisible(TRUE)
  }
}
