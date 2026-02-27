# Database Migration Functions
#
# Functions for migrating existing databases to new schemas.
# Split from database_setup.R for better maintainability.

#' Add Prediction Tables to Database
#'
#' Adds the new prediction and simulation tables to an existing database.
#' This is a non-destructive migration that preserves all existing data.
#'
#' @param path Character. Database file path. If NULL, uses default.
#'
#' @return Invisibly returns TRUE on success
#' @export
#'
#' @examples
#' \dontrun{
#' # Add new tables to existing database
#' add_prediction_tables()
#' }
add_prediction_tables <- function(path = NULL) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_alert_danger("Database not found at {.file {path}}")
    return(invisible(FALSE))
  }

  check_duckdb_available()
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  existing_tables <- DBI::dbListTables(conn)

  cli::cli_h2("Adding prediction tables to database")

  # Add team_elo table
  if (!"team_elo" %in% existing_tables) {
    cli::cli_alert_info("Creating team_elo table...")
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS team_elo (
        team_id VARCHAR,
        match_id VARCHAR,
        match_date DATE,
        match_type VARCHAR,
        event_name VARCHAR,
        elo_result DOUBLE,
        elo_roster_batting DOUBLE,
        elo_roster_bowling DOUBLE,
        elo_roster_combined DOUBLE,
        matches_played INTEGER,
        PRIMARY KEY (team_id, match_id)
      )
    ")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_team_elo_date ON team_elo(team_id, match_date)")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_team_elo_event ON team_elo(event_name)")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_team_elo_type ON team_elo(match_type)")
    cli::cli_alert_success("Created team_elo table")
  } else {
    cli::cli_alert_info("team_elo table already exists")
  }

  # Add pre_match_features table
  if (!"pre_match_features" %in% existing_tables) {
    cli::cli_alert_info("Creating pre_match_features table...")
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS pre_match_features (
        match_id VARCHAR PRIMARY KEY,
        match_date DATE,
        match_type VARCHAR,
        event_name VARCHAR,
        team1 VARCHAR,
        team2 VARCHAR,
        team1_elo_result DOUBLE,
        team1_elo_roster DOUBLE,
        team1_form_last5 DOUBLE,
        team1_h2h_wins INTEGER,
        team1_h2h_total INTEGER,
        team2_elo_result DOUBLE,
        team2_elo_roster DOUBLE,
        team2_form_last5 DOUBLE,
        team2_h2h_wins INTEGER,
        team2_h2h_total INTEGER,
        venue VARCHAR,
        venue_avg_score DOUBLE,
        venue_chase_success_rate DOUBLE,
        venue_matches INTEGER,
        is_knockout BOOLEAN,
        is_neutral_venue BOOLEAN,
        created_at TIMESTAMP
      )
    ")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_prematch_date ON pre_match_features(match_date)")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_prematch_event ON pre_match_features(event_name)")
    cli::cli_alert_success("Created pre_match_features table")
  } else {
    cli::cli_alert_info("pre_match_features table already exists")
  }

  # Add pre_match_predictions table
  if (!"pre_match_predictions" %in% existing_tables) {
    cli::cli_alert_info("Creating pre_match_predictions table...")
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS pre_match_predictions (
        prediction_id VARCHAR PRIMARY KEY,
        match_id VARCHAR,
        model_version VARCHAR,
        model_type VARCHAR,
        prediction_date TIMESTAMP,
        team1_win_prob DOUBLE,
        team2_win_prob DOUBLE,
        predicted_winner VARCHAR,
        confidence DOUBLE,
        actual_winner VARCHAR,
        prediction_correct BOOLEAN
      )
    ")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_predictions_match ON pre_match_predictions(match_id)")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_predictions_date ON pre_match_predictions(prediction_date)")
    cli::cli_alert_success("Created pre_match_predictions table")
  } else {
    cli::cli_alert_info("pre_match_predictions table already exists")
  }

  # Add simulation_results table
  if (!"simulation_results" %in% existing_tables) {
    cli::cli_alert_info("Creating simulation_results table...")
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS simulation_results (
        simulation_id VARCHAR PRIMARY KEY,
        simulation_type VARCHAR,
        event_name VARCHAR,
        season VARCHAR,
        simulation_date TIMESTAMP,
        n_simulations INTEGER,
        parameters VARCHAR,
        team_results VARCHAR,
        match_results VARCHAR,
        created_at TIMESTAMP
      )
    ")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_simulation_type ON simulation_results(simulation_type)")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_simulation_event ON simulation_results(event_name)")
    cli::cli_alert_success("Created simulation_results table")
  } else {
    cli::cli_alert_info("simulation_results table already exists")
  }

  cli::cli_alert_success("Migration complete!")

  # Verify
  new_tables <- DBI::dbListTables(conn)
  cli::cli_h3("Database tables")
  for (tbl in new_tables) {
    cli::cli_alert_success("{tbl}")
  }

  invisible(TRUE)
}


#' Add Skill Index Columns to Pre-Match Features
#'
#' Adds the new skill index columns to an existing pre_match_features table.
#' This is a non-destructive migration that preserves all existing data.
#'
#' @param conn DBI connection. Optional existing database connection.
#'   If NULL, opens a new connection using path.
#' @param path Character. Database file path. If NULL, uses default.
#'   Ignored if conn is provided.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
add_skill_columns_to_features <- function(conn = NULL, path = NULL) {
  # Handle connection - use provided or create new
  own_connection <- FALSE
  if (is.null(conn)) {
    if (is.null(path)) {
      path <- get_default_db_path()
    }

    if (!file.exists(path)) {
      cli::cli_alert_danger("Database not found at {.file {path}}")
      return(invisible(FALSE))
    }

    check_duckdb_available()
    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
    own_connection <- TRUE
  }

  # Only disconnect if we created the connection
  if (own_connection) {
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))
  }

  existing_tables <- DBI::dbListTables(conn)

  if (!"pre_match_features" %in% existing_tables) {
    cli::cli_alert_danger("pre_match_features table does not exist")
    cli::cli_alert_info("Run add_prediction_tables() first")
    return(invisible(FALSE))
  }

  cli::cli_h2("Adding skill index columns to pre_match_features")

  # Get existing columns
  existing_cols <- DBI::dbGetQuery(conn, "
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'pre_match_features'
  ")$column_name

  # Define new skill columns
  skill_columns <- c(
    "team1_bat_scoring_avg DOUBLE",
    "team1_bat_scoring_top5 DOUBLE",
    "team1_bat_survival_avg DOUBLE",
    "team1_bowl_economy_avg DOUBLE",
    "team1_bowl_economy_top5 DOUBLE",
    "team1_bowl_strike_avg DOUBLE",
    "team2_bat_scoring_avg DOUBLE",
    "team2_bat_scoring_top5 DOUBLE",
    "team2_bat_survival_avg DOUBLE",
    "team2_bowl_economy_avg DOUBLE",
    "team2_bowl_economy_top5 DOUBLE",
    "team2_bowl_strike_avg DOUBLE"
  )

  columns_added <- 0

  for (col_def in skill_columns) {
    col_name <- strsplit(col_def, " ")[[1]][1]

    if (!col_name %in% existing_cols) {
      tryCatch({
        DBI::dbExecute(conn, sprintf(
          "ALTER TABLE pre_match_features ADD COLUMN %s", col_def
        ))
        cli::cli_alert_success("Added column: {col_name}")
        columns_added <- columns_added + 1
      }, error = function(e) {
        cli::cli_alert_warning("Failed to add {col_name}: {e$message}")
      })
    } else {
      cli::cli_alert_info("Column {col_name} already exists")
    }
  }

  if (columns_added > 0) {
    cli::cli_alert_success("Added {columns_added} new skill columns")
  } else {
    cli::cli_alert_info("All skill columns already exist")
  }

  # Also add toss columns if missing
  toss_columns <- c(
    "team1_won_toss INTEGER",
    "toss_elect_bat INTEGER"
  )

  for (col_def in toss_columns) {
    col_name <- strsplit(col_def, " ")[[1]][1]

    if (!col_name %in% existing_cols) {
      tryCatch({
        DBI::dbExecute(conn, sprintf(
          "ALTER TABLE pre_match_features ADD COLUMN %s", col_def
        ))
        cli::cli_alert_success("Added column: {col_name}")
      }, error = function(e) {
        cli::cli_alert_warning("Failed to add {col_name}: {e$message}")
      })
    }
  }

  # Add team skill columns (per-delivery residual-based team skills)
  cli::cli_h2("Adding team skill columns to pre_match_features")

  team_skill_columns <- c(
    "team1_team_runs_skill DOUBLE",
    "team1_team_wicket_skill DOUBLE",
    "team2_team_runs_skill DOUBLE",
    "team2_team_wicket_skill DOUBLE"
  )

  for (col_def in team_skill_columns) {
    col_name <- strsplit(col_def, " ")[[1]][1]

    if (!col_name %in% existing_cols) {
      tryCatch({
        DBI::dbExecute(conn, sprintf(
          "ALTER TABLE pre_match_features ADD COLUMN %s", col_def
        ))
        cli::cli_alert_success("Added column: {col_name}")
        columns_added <- columns_added + 1
      }, error = function(e) {
        cli::cli_alert_warning("Failed to add {col_name}: {e$message}")
      })
    } else {
      cli::cli_alert_info("Column {col_name} already exists")
    }
  }

  # Add venue skill columns (residual-based venue skills)
  cli::cli_h2("Adding venue skill columns to pre_match_features")

  venue_skill_columns <- c(
    "venue_run_rate_skill DOUBLE",
    "venue_wicket_rate_skill DOUBLE",
    "venue_boundary_rate DOUBLE",
    "venue_dot_rate DOUBLE"
  )

  for (col_def in venue_skill_columns) {
    col_name <- strsplit(col_def, " ")[[1]][1]

    if (!col_name %in% existing_cols) {
      tryCatch({
        DBI::dbExecute(conn, sprintf(
          "ALTER TABLE pre_match_features ADD COLUMN %s", col_def
        ))
        cli::cli_alert_success("Added column: {col_name}")
        columns_added <- columns_added + 1
      }, error = function(e) {
        cli::cli_alert_warning("Failed to add {col_name}: {e$message}")
      })
    } else {
      cli::cli_alert_info("Column {col_name} already exists")
    }
  }

  invisible(TRUE)
}


#' Ensure Match Metrics Table Exists
#'
#' Ensures the match_metrics table exists for storing calculated match data
#' like unified_margin. Also ensures pre_match_features has margin columns.
#'
#' @param conn DBI connection. If provided, uses this connection (does not close it).
#' @param path Character. Database file path. If NULL, uses default. Ignored if conn is provided.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
ensure_match_metrics_table <- function(conn = NULL, path = NULL) {
  # If connection provided, use it (caller is responsible for closing)
  own_conn <- is.null(conn)

  if (own_conn) {
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
  }

  cli::cli_h2("Ensuring match_metrics table exists")

  # Check if match_metrics table exists
  tables <- DBI::dbListTables(conn)

  if (!"match_metrics" %in% tables) {
    cli::cli_alert_info("Creating match_metrics table...")
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS match_metrics (
        match_id VARCHAR PRIMARY KEY,
        unified_margin DOUBLE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP
      )
    ")
    cli::cli_alert_success("Created match_metrics table")
  } else {
    cli::cli_alert_info("match_metrics table already exists")
  }

  # Check if pre_match_features table has margin columns
  pmf_cols <- DBI::dbGetQuery(conn, "SELECT column_name FROM information_schema.columns WHERE table_name = 'pre_match_features'")$column_name

  if (!"expected_margin" %in% pmf_cols) {
    cli::cli_alert_info("Adding expected_margin column to pre_match_features table...")
    DBI::dbExecute(conn, "ALTER TABLE pre_match_features ADD COLUMN expected_margin DOUBLE")
    cli::cli_alert_success("Added expected_margin column to pre_match_features")
  }

  if (!"actual_margin" %in% pmf_cols) {
    cli::cli_alert_info("Adding actual_margin column to pre_match_features table...")
    DBI::dbExecute(conn, "ALTER TABLE pre_match_features ADD COLUMN actual_margin DOUBLE")
    cli::cli_alert_success("Added actual_margin column to pre_match_features")
  }

  cli::cli_alert_success("Match metrics tables ready")
  invisible(TRUE)
}


#' Add Margin of Victory Columns to Existing Database
#'
#' @description
#' `r lifecycle::badge("deprecated")`
#'
#' This function is deprecated. Use ensure_match_metrics_table() instead.
#' Margin data is now stored in the match_metrics table, not in matches.
#'
#' @param conn DBI connection.
#' @param path Character. Database file path.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
add_margin_columns <- function(conn = NULL, path = NULL) {
  cli::cli_alert_warning("add_margin_columns() is deprecated - use ensure_match_metrics_table()")
  ensure_match_metrics_table(conn = conn, path = path)
}


#' Add Cricinfo Tables to Existing Database
#'
#' Adds the Cricinfo Hawkeye data tables (balls, matches, innings, fixtures)
#' to an existing database. Non-destructive: preserves all existing data.
#'
#' @param path Character. Database file path. If NULL, uses default.
#'
#' @return Invisibly returns TRUE on success
#' @export
#'
#' @examples
#' \dontrun{
#' add_cricinfo_tables()
#' }
add_cricinfo_tables <- function(path = NULL) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_alert_danger("Database not found at {.file {path}}")
    return(invisible(FALSE))
  }

  check_duckdb_available()
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Check for schema-qualified Cricinfo tables
  existing <- DBI::dbGetQuery(conn,
    "SELECT table_schema || '.' || table_name AS qualified_name
     FROM information_schema.tables
     WHERE table_schema = 'cricinfo'")$qualified_name

  cricinfo_tables <- c("cricinfo.balls", "cricinfo.matches",
                        "cricinfo.innings", "cricinfo.fixtures")
  needed <- setdiff(cricinfo_tables, existing)

  if (length(needed) == 0) {
    cli::cli_alert_info("All Cricinfo tables already exist")
    return(invisible(TRUE))
  }

  cli::cli_h2("Adding Cricinfo tables to database")
  create_cricinfo_tables(conn, verbose = TRUE)
  cli::cli_alert_success("Migration complete! Added {length(needed)} Cricinfo table{?s}")

  invisible(TRUE)
}


#' Migrate Existing Database to Schema Namespaces
#'
#' Moves Cricsheet tables from the default `main` schema to the `cricsheet`
#' schema, and Cricinfo tables from `main` to the `cricinfo` schema.
#' This is a one-time migration for existing databases.
#'
#' Uses CREATE TABLE AS SELECT + DROP since DuckDB doesn't support
#' ALTER TABLE SET SCHEMA.
#'
#' @param path Character. Database file path. If NULL, uses default.
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return Invisibly returns TRUE on success.
#' @export
#'
#' @examples
#' \dontrun{
#' migrate_to_schemas()
#' }
migrate_to_schemas <- function(path = NULL, verbose = TRUE) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_abort("Database not found at {.file {path}}")
  }

  check_duckdb_available()
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Create schemas
  create_schemas(conn)

  # Get existing tables in main schema
  main_tables <- DBI::dbGetQuery(conn,
    "SELECT table_name FROM information_schema.tables
     WHERE table_schema = 'main'")$table_name

  # Cricsheet tables to migrate
  cricsheet_map <- c(
    "matches" = "cricsheet.matches",
    "deliveries" = "cricsheet.deliveries",
    "players" = "cricsheet.players",
    "match_innings" = "cricsheet.match_innings",
    "innings_powerplays" = "cricsheet.innings_powerplays"
  )

  # Cricinfo tables to migrate
  cricinfo_map <- c(
    "cricinfo_balls" = "cricinfo.balls",
    "cricinfo_matches" = "cricinfo.matches",
    "cricinfo_innings" = "cricinfo.innings",
    "cricinfo_fixtures" = "cricinfo.fixtures"
  )

  all_map <- c(cricsheet_map, cricinfo_map)
  migrated <- 0L

  cli::cli_h2("Migrating tables to schemas")

  for (old_name in names(all_map)) {
    new_name <- all_map[[old_name]]

    if (!old_name %in% main_tables) {
      if (verbose) cli::cli_alert_info("Skipping {old_name} (not in main schema)")
      next
    }

    # Check if target already exists
    target_exists <- nrow(DBI::dbGetQuery(conn, sprintf(
      "SELECT 1 FROM information_schema.tables
       WHERE table_schema || '.' || table_name = '%s'", new_name
    ))) > 0

    if (target_exists) {
      if (verbose) cli::cli_alert_info("Skipping {old_name} ({new_name} already exists)")
      next
    }

    if (verbose) cli::cli_alert_info("Migrating {old_name} -> {new_name}...")

    row_count <- DBI::dbGetQuery(conn, sprintf(
      "SELECT COUNT(*) as n FROM %s", old_name))$n

    # Wrap CREATE + DROP in a transaction for atomicity
    DBI::dbBegin(conn)
    tryCatch({
      DBI::dbExecute(conn, sprintf(
        "CREATE TABLE %s AS SELECT * FROM %s", new_name, old_name))

      # Verify row count before dropping source table
      new_count <- DBI::dbGetQuery(conn, sprintf(
        "SELECT COUNT(*) as n FROM %s", new_name))$n
      if (new_count != row_count) {
        DBI::dbRollback(conn)
        cli::cli_abort("Row count mismatch migrating {old_name}: expected {row_count}, got {new_count}. Source table preserved.")
      }

      DBI::dbExecute(conn, sprintf("DROP TABLE %s", old_name))
      DBI::dbCommit(conn)
    }, error = function(e) {
      tryCatch(DBI::dbRollback(conn), error = function(e2) NULL)
      cli::cli_abort("Migration failed for {old_name}: {e$message}")
    })

    if (verbose) cli::cli_alert_success("Migrated {old_name} -> {new_name} ({format(row_count, big.mark=',')} rows)")
    migrated <- migrated + 1L
  }

  # Verify no mapped tables remain in main schema
  if (migrated > 0) {
    current_main <- DBI::dbGetQuery(conn,
      "SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'main'")$table_name
    remaining <- intersect(current_main, names(all_map))
    if (length(remaining) > 0) {
      cli::cli_alert_warning("{length(remaining)} table{?s} still in main schema: {paste(remaining, collapse = ', ')}")
    }
  }

  # Rebuild indexes on migrated tables
  if (migrated > 0) {
    if (verbose) cli::cli_alert_info("Rebuilding indexes...")
    create_indexes(conn, verbose = verbose)
  }

  if (verbose) {
    cli::cli_alert_success("Migration complete! Moved {migrated} table{?s} to schemas")
  }

  invisible(TRUE)
}
