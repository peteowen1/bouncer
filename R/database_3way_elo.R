# 3-Way ELO Database Functions
#
# Functions for managing 3-way ELO tables (batter + bowler + venue).
# Split from database_setup.R for better maintainability.

#' Create 3-Way ELO Table for Format
#'
#' Creates or recreates the format-specific 3-way ELO table.
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#' @param overwrite Logical. If TRUE, drops and recreates the table.
#'
#' @return Invisibly returns TRUE on success.
#' @keywords internal
create_3way_elo_table <- function(format, conn, overwrite = FALSE) {
  # Keep gender prefix in table name to avoid overwrites between men's and women's
  # e.g., "mens_t20" -> "mens_t20_3way_elo", "womens_t20" -> "womens_t20_3way_elo"
  table_name <- paste0(tolower(format), "_3way_elo")

  if (overwrite) {
    DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", table_name))
    cli::cli_alert_info("Dropped existing table: {table_name}")
  }

  # Check if table exists
  if (table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_info("Table {table_name} already exists")
    return(invisible(TRUE))
  }

  # Create the table
  DBI::dbExecute(conn, sprintf("
    CREATE TABLE IF NOT EXISTS %s (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      batter_id VARCHAR,
      bowler_id VARCHAR,
      venue VARCHAR,

      batter_run_elo_before DOUBLE,
      batter_run_elo_after DOUBLE,
      bowler_run_elo_before DOUBLE,
      bowler_run_elo_after DOUBLE,

      batter_wicket_elo_before DOUBLE,
      batter_wicket_elo_after DOUBLE,
      bowler_wicket_elo_before DOUBLE,
      bowler_wicket_elo_after DOUBLE,

      venue_perm_run_elo_before DOUBLE,
      venue_perm_run_elo_after DOUBLE,
      venue_perm_wicket_elo_before DOUBLE,
      venue_perm_wicket_elo_after DOUBLE,

      venue_session_run_elo_before DOUBLE,
      venue_session_run_elo_after DOUBLE,
      venue_session_wicket_elo_before DOUBLE,
      venue_session_wicket_elo_after DOUBLE,

      k_batter_run DOUBLE,
      k_bowler_run DOUBLE,
      k_venue_perm_run DOUBLE,
      k_venue_session_run DOUBLE,
      k_batter_wicket DOUBLE,
      k_bowler_wicket DOUBLE,
      k_venue_perm_wicket DOUBLE,
      k_venue_session_wicket DOUBLE,

      exp_runs DOUBLE,
      exp_wicket DOUBLE,
      actual_runs INTEGER,
      is_wicket BOOLEAN,

      batter_balls INTEGER,
      bowler_balls INTEGER,
      venue_balls INTEGER,
      balls_in_match INTEGER,
      days_inactive_batter INTEGER,
      days_inactive_bowler INTEGER,

      is_knockout BOOLEAN,
      event_tier INTEGER,
      phase VARCHAR
    )
  ", table_name))

  # Create indexes
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_match ON %s(match_id)",
                               gsub("_3way_elo", "_3way", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_batter ON %s(batter_id)",
                               gsub("_3way_elo", "_3way", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_bowler ON %s(bowler_id)",
                               gsub("_3way_elo", "_3way", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_venue ON %s(venue)",
                               gsub("_3way_elo", "_3way", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_date ON %s(match_date)",
                               gsub("_3way_elo", "_3way", table_name), table_name))

  cli::cli_alert_success("Created table: {table_name}")
  invisible(TRUE)
}


#' Insert 3-Way ELO Data
#'
#' Inserts 3-way ELO data into the format-specific table.
#'
#' @param df data.frame/data.table with 3-way ELO columns.
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#'
#' @return Invisibly returns TRUE on success.
#' @keywords internal
insert_3way_elos <- function(df, format, conn) {
  # Keep gender prefix in table name
  table_name <- paste0(tolower(format), "_3way_elo")

  # Check table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_danger("Table {table_name} does not exist")
    return(invisible(FALSE))
  }

  # Use DBI::dbWriteTable with append mode
  DBI::dbWriteTable(conn, table_name, df, append = TRUE, row.names = FALSE)

  invisible(TRUE)
}


#' Get 3-Way ELO Statistics
#'
#' Returns summary statistics for the 3-way ELO table.
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#'
#' @return List with statistics or NULL if table doesn't exist.
#' @keywords internal
get_3way_elo_stats <- function(format, conn) {
  # Keep gender prefix in table name
  table_name <- paste0(tolower(format), "_3way_elo")

  # Check table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  stats <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(*) as total_records,
      COUNT(DISTINCT batter_id) as unique_batters,
      COUNT(DISTINCT bowler_id) as unique_bowlers,
      COUNT(DISTINCT venue) as unique_venues,
      MIN(match_date) as first_date,
      MAX(match_date) as last_date,
      AVG(batter_run_elo_after) as mean_batter_run_elo,
      AVG(bowler_run_elo_after) as mean_bowler_run_elo,
      AVG(batter_wicket_elo_after) as mean_batter_wicket_elo,
      AVG(bowler_wicket_elo_after) as mean_bowler_wicket_elo,
      AVG(venue_perm_run_elo_after) as mean_venue_perm_run_elo,
      AVG(venue_session_run_elo_after) as mean_venue_session_run_elo
    FROM %s
  ", table_name))

  as.list(stats)
}


#' Delete 3-Way ELO Data for Matches
#'
#' Deletes 3-way ELO data for specified matches.
#'
#' @param match_ids Character vector of match IDs to delete.
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return Invisibly returns number of rows deleted.
#' @keywords internal
delete_3way_elo_matches <- function(match_ids, format, conn, verbose = TRUE) {
  if (length(match_ids) == 0) {
    return(invisible(0))
  }

  table_name <- paste0(tolower(format), "_3way_elo")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(invisible(0))
  }

  match_ids_sql <- paste0("'", match_ids, "'", collapse = ", ")
  sql <- sprintf("DELETE FROM %s WHERE match_id IN (%s)", table_name, match_ids_sql)

  n_deleted <- tryCatch({
    DBI::dbExecute(conn, sql)
  }, error = function(e) {
    if (verbose) cli::cli_alert_warning("Could not delete from {table_name}: {e$message}")
    0
  })

  if (n_deleted > 0 && verbose) {
    cli::cli_alert_success("Deleted {n_deleted} rows from {table_name}")
  }

  invisible(n_deleted)
}


#' Get Latest 3-Way ELO State for Players
#'
#' Returns the most recent ELO values for all players in a format.
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#' @param elo_type Character. "run" or "wicket". Default "run".
#'
#' @return data.frame with player_id, role (batter/bowler), and latest ELO.
#' @keywords internal
get_latest_3way_player_elos <- function(format, conn, elo_type = "run") {
  table_name <- paste0(tolower(format), "_3way_elo")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(data.frame())
  }

  elo_col <- if (elo_type == "run") "run_elo_after" else "wicket_elo_after"

  # Get latest batter ELOs
  batters <- DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        batter_id as player_id,
        batter_%s as elo,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT player_id, elo, match_date, 'batter' as role
    FROM ranked
    WHERE rn = 1
  ", elo_col, table_name))

  # Get latest bowler ELOs
  bowlers <- DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        bowler_id as player_id,
        bowler_%s as elo,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT player_id, elo, match_date, 'bowler' as role
    FROM ranked
    WHERE rn = 1
  ", elo_col, table_name))

  rbind(batters, bowlers)
}


#' Get Latest 3-Way ELO State for Venues
#'
#' Returns the most recent ELO values for all venues in a format.
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#'
#' @return data.frame with venue, permanent ELOs, and latest match date.
#' @keywords internal
get_latest_3way_venue_elos <- function(format, conn) {
  table_name <- paste0(tolower(format), "_3way_elo")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(data.frame())
  }

  DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        venue,
        venue_perm_run_elo_after,
        venue_perm_wicket_elo_after,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY venue ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT
      venue,
      venue_perm_run_elo_after as perm_run_elo,
      venue_perm_wicket_elo_after as perm_wicket_elo,
      match_date as last_match_date
    FROM ranked
    WHERE rn = 1
  ", table_name))
}
