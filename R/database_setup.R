# Database Setup Functions for Bouncer

#' Initialize Bouncer Database
#'
#' Creates a new DuckDB database file with the schema for cricket data storage.
#'
#' @param path Character string specifying the database file path. If NULL,
#'   uses the default system data directory.
#' @param overwrite Logical. If TRUE, overwrites existing database. Default FALSE.
#'
#' @return Invisibly returns the database path
#' @export
#'
#' @examples
#' \dontrun{
#' # Initialize database in default location
#' initialize_bouncer_database()
#'
#' # Initialize in custom location
#' initialize_bouncer_database("~/cricket_data/bouncer.duckdb")
#' }
initialize_bouncer_database <- function(path = NULL, overwrite = FALSE) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  # Create directory if it doesn't exist
  db_dir <- dirname(path)
  if (!dir.exists(db_dir)) {
    dir.create(db_dir, recursive = TRUE)
    cli::cli_alert_success("Created directory: {.file {db_dir}}")
  }

  # Check if database already exists
  if (file.exists(path) && !overwrite) {
    cli::cli_alert_warning("Database already exists at {.file {path}}")
    cli::cli_alert_info("Use overwrite = TRUE to replace it")
    return(invisible(path))
  }

  if (file.exists(path) && overwrite) {
    cli::cli_alert_warning("Overwriting existing database at {.file {path}}")
    file.remove(path)
  }

  # Create database and schema
  cli::cli_alert_info("Initializing DuckDB database...")

  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Create schema
  create_schema(conn)

  # Create indexes
  create_indexes(conn)

  cli::cli_alert_success("Database initialized at {.file {path}}")

  invisible(path)
}


#' Get Default Database Path
#'
#' Returns the default path for the Bouncer DuckDB database.
#'
#' @return Character string with database path
#' @keywords internal
get_default_db_path <- function() {
  # Use system data directory
  data_dir <- tools::R_user_dir("bouncer", which = "data")
  file.path(data_dir, "bouncer.duckdb")
}


#' Create Database Schema
#'
#' Creates all tables needed for cricket data storage.
#'
#' @param conn A DuckDB connection object
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
create_schema <- function(conn) {
  cli::cli_h2("Creating database schema")

  # Create matches table
  cli::cli_alert_info("Creating matches table...")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS matches (
      match_id VARCHAR PRIMARY KEY,
      season VARCHAR,
      match_type VARCHAR,
      match_date DATE,
      venue VARCHAR,
      city VARCHAR,
      gender VARCHAR,

      -- Teams
      team1 VARCHAR,
      team2 VARCHAR,

      -- Match Details
      balls_per_over INTEGER,
      overs_per_innings INTEGER,

      -- Toss
      toss_winner VARCHAR,
      toss_decision VARCHAR,

      -- Outcome
      outcome_type VARCHAR,
      outcome_winner VARCHAR,
      outcome_by_runs INTEGER,
      outcome_by_wickets INTEGER,
      outcome_method VARCHAR,

      -- Match Officials
      umpire1 VARCHAR,
      umpire2 VARCHAR,
      tv_umpire VARCHAR,
      referee VARCHAR,

      -- Player of Match
      player_of_match_id VARCHAR,

      -- Event Info
      event_name VARCHAR,
      event_match_number INTEGER
    )
  ")

  # Create deliveries table
  cli::cli_alert_info("Creating deliveries table...")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS deliveries (
      -- Primary Keys
      delivery_id BIGINT PRIMARY KEY,
      match_id VARCHAR,

      -- Match Context
      season VARCHAR,
      match_type VARCHAR,
      match_date DATE,
      venue VARCHAR,
      city VARCHAR,
      gender VARCHAR,

      -- Teams
      batting_team VARCHAR,
      bowling_team VARCHAR,

      -- Innings Context
      innings INTEGER,
      over INTEGER,
      ball INTEGER,
      over_ball DECIMAL(3,1),

      -- Players
      batter_id VARCHAR,
      bowler_id VARCHAR,
      non_striker_id VARCHAR,

      -- Outcomes
      runs_batter INTEGER,
      runs_extras INTEGER,
      runs_total INTEGER,
      is_boundary BOOLEAN,
      is_four BOOLEAN,
      is_six BOOLEAN,

      -- Extras Detail
      wides INTEGER,
      noballs INTEGER,
      byes INTEGER,
      legbyes INTEGER,
      penalty INTEGER,

      -- Wicket Information
      is_wicket BOOLEAN,
      wicket_kind VARCHAR,
      player_out_id VARCHAR,
      fielder1_id VARCHAR,
      fielder2_id VARCHAR,

      -- Match State (running totals)
      total_runs INTEGER,
      wickets_fallen INTEGER,

      -- ELO Ratings (before this delivery)
      batter_elo_before DOUBLE,
      bowler_elo_before DOUBLE,
      batter_elo_after DOUBLE,
      bowler_elo_after DOUBLE
    )
  ")

  # Create players table
  cli::cli_alert_info("Creating players table...")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS players (
      player_id VARCHAR PRIMARY KEY,
      player_name VARCHAR,
      country VARCHAR,
      dob DATE,
      batting_style VARCHAR,
      bowling_style VARCHAR
    )
  ")

  # Create match_innings table
  cli::cli_alert_info("Creating match_innings table...")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS match_innings (
      match_id VARCHAR,
      innings INTEGER,
      batting_team VARCHAR,
      bowling_team VARCHAR,
      total_runs INTEGER,
      total_wickets INTEGER,
      total_overs DECIMAL(4,1),
      declared BOOLEAN,
      forfeited BOOLEAN,
      PRIMARY KEY (match_id, innings)
    )
  ")

  # Create player_elo_history table
  cli::cli_alert_info("Creating player_elo_history table...")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS player_elo_history (
      player_id VARCHAR,
      match_id VARCHAR,
      match_date DATE,
      match_type VARCHAR,

      -- Overall ELO
      elo_batting DOUBLE,
      elo_bowling DOUBLE,

      -- Format-specific
      elo_batting_test DOUBLE,
      elo_batting_odi DOUBLE,
      elo_batting_t20 DOUBLE,
      elo_bowling_test DOUBLE,
      elo_bowling_odi DOUBLE,
      elo_bowling_t20 DOUBLE,

      -- Opposition-adjusted (stored as JSON for flexibility)
      elo_batting_vs_opposition VARCHAR,
      elo_bowling_vs_opposition VARCHAR,

      PRIMARY KEY (player_id, match_id)
    )
  ")

  cli::cli_alert_success("Schema created successfully")
  invisible(TRUE)
}


#' Create Database Indexes
#'
#' Creates indexes on key columns for query performance.
#'
#' @param conn A DuckDB connection object
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
create_indexes <- function(conn) {
  cli::cli_h2("Creating indexes")

  # Matches indexes
  cli::cli_alert_info("Creating matches indexes...")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_matches_venue ON matches(venue)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_matches_type ON matches(match_type)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_matches_season ON matches(season)")

  # Deliveries indexes
  cli::cli_alert_info("Creating deliveries indexes...")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_deliveries_match ON deliveries(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_deliveries_batter ON deliveries(batter_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_deliveries_bowler ON deliveries(bowler_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_deliveries_type ON deliveries(match_type)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_deliveries_date ON deliveries(match_date)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_deliveries_venue ON deliveries(venue)")

  # Players indexes
  cli::cli_alert_info("Creating players indexes...")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_players_name ON players(player_name)")

  # Player ELO indexes
  cli::cli_alert_info("Creating player_elo_history indexes...")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_elo_player_date ON player_elo_history(player_id, match_date)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_elo_match_type ON player_elo_history(match_type)")

  cli::cli_alert_success("Indexes created successfully")
  invisible(TRUE)
}


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

  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Get list of tables
  tables <- DBI::dbListTables(conn)
  expected_tables <- c("matches", "deliveries", "players", "match_innings", "player_elo_history")

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
