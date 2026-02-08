# 3-Way Skill Index Database Functions
#
# Functions for managing 3-way skill index tables (batter + bowler + venue).
# These store additive skill indices that are directly interpretable:
#   - Skill = deviation from baseline in natural units (runs, probability)
#   - Start at 0 (neutral) instead of 1400 (ELO)
#   - Typical range: -0.5 to +0.5 for run skills
#
# Split from database_setup.R for better maintainability.

#' Create 3-Way Skill Index Table for Format
#'
#' Creates or recreates the format-specific 3-way skill index table.
#' Schema mirrors ELO table but with different semantic interpretation:
#' - Values centered at 0 (neutral) not 1400
#' - Range typically ±0.5 for runs, ±0.05 for wickets
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#' @param overwrite Logical. If TRUE, drops and recreates the table.
#'
#' @return Invisibly returns TRUE on success.
#' @keywords internal
create_3way_skill_table <- function(format, conn, overwrite = FALSE) {
  # Keep gender prefix in table name to avoid overwrites between men's and women's
  # e.g., "mens_t20" -> "mens_t20_3way_skill", "womens_t20" -> "womens_t20_3way_skill"
  table_name <- paste0(tolower(format), "_3way_skill")

  if (overwrite) {
    DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", table_name))
    cli::cli_alert_info("Dropped existing table: {table_name}")
  }

  # Check if table exists
  if (table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_info("Table {table_name} already exists")
    return(invisible(TRUE))
  }

  # Create the table - columns use "skill" suffix instead of "elo"
  # Values are now centered at 0 (neutral) with typical range ±0.5
  DBI::dbExecute(conn, sprintf("
    CREATE TABLE IF NOT EXISTS %s (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      batter_id VARCHAR,
      bowler_id VARCHAR,
      venue VARCHAR,

      -- Player Run Skills (deviation from expected runs/ball)
      batter_run_skill_before DOUBLE,
      batter_run_skill_after DOUBLE,
      bowler_run_skill_before DOUBLE,
      bowler_run_skill_after DOUBLE,

      -- Player Wicket Skills (deviation from expected wicket probability)
      batter_wicket_skill_before DOUBLE,
      batter_wicket_skill_after DOUBLE,
      bowler_wicket_skill_before DOUBLE,
      bowler_wicket_skill_after DOUBLE,

      -- Venue Permanent Skills (long-term venue characteristics)
      venue_perm_run_skill_before DOUBLE,
      venue_perm_run_skill_after DOUBLE,
      venue_perm_wicket_skill_before DOUBLE,
      venue_perm_wicket_skill_after DOUBLE,

      -- Venue Session Skills (current match conditions, resets each match)
      venue_session_run_skill_before DOUBLE,
      venue_session_run_skill_after DOUBLE,
      venue_session_wicket_skill_before DOUBLE,
      venue_session_wicket_skill_after DOUBLE,

      -- Learning Rates (alpha, equivalent to K-factors)
      alpha_batter_run DOUBLE,
      alpha_bowler_run DOUBLE,
      alpha_venue_perm_run DOUBLE,
      alpha_venue_session_run DOUBLE,
      alpha_batter_wicket DOUBLE,
      alpha_bowler_wicket DOUBLE,
      alpha_venue_perm_wicket DOUBLE,
      alpha_venue_session_wicket DOUBLE,

      -- Predictions/actuals (from agnostic model)
      exp_runs DOUBLE,
      exp_wicket DOUBLE,
      actual_runs INTEGER,
      is_wicket BOOLEAN,

      -- Context
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
                               gsub("_3way_skill", "_3way_sk", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_batter ON %s(batter_id)",
                               gsub("_3way_skill", "_3way_sk", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_bowler ON %s(bowler_id)",
                               gsub("_3way_skill", "_3way_sk", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_venue ON %s(venue)",
                               gsub("_3way_skill", "_3way_sk", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_date ON %s(match_date)",
                               gsub("_3way_skill", "_3way_sk", table_name), table_name))

  cli::cli_alert_success("Created table: {table_name}")
  invisible(TRUE)
}


#' Insert 3-Way Skill Index Data
#'
#' Inserts 3-way skill index data into the format-specific table.
#'
#' @param df data.frame/data.table with 3-way skill columns.
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#'
#' @return Invisibly returns TRUE on success.
#' @keywords internal
insert_3way_skills <- function(df, format, conn) {
  # Keep gender prefix in table name
  table_name <- paste0(tolower(format), "_3way_skill")

  # Check table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_danger("Table {table_name} does not exist")
    return(invisible(FALSE))
  }

  # Use DBI::dbWriteTable with append mode
  DBI::dbWriteTable(conn, table_name, df, append = TRUE, row.names = FALSE)

  invisible(TRUE)
}


#' Get 3-Way Skill Index Statistics
#'
#' Returns summary statistics for the 3-way skill index table.
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#'
#' @return List with statistics or NULL if table doesn't exist.
#' @keywords internal
get_3way_skill_stats <- function(format, conn) {
  # Keep gender prefix in table name
  table_name <- paste0(tolower(format), "_3way_skill")

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
      AVG(batter_run_skill_after) as mean_batter_run_skill,
      AVG(bowler_run_skill_after) as mean_bowler_run_skill,
      AVG(batter_wicket_skill_after) as mean_batter_wicket_skill,
      AVG(bowler_wicket_skill_after) as mean_bowler_wicket_skill,
      AVG(venue_perm_run_skill_after) as mean_venue_perm_run_skill,
      AVG(venue_session_run_skill_after) as mean_venue_session_run_skill,
      STDDEV(batter_run_skill_after) as sd_batter_run_skill,
      STDDEV(bowler_run_skill_after) as sd_bowler_run_skill,
      AVG(exp_runs) as mean_exp_runs,
      AVG(actual_runs) as mean_actual_runs,
      AVG(exp_wicket) as mean_exp_wicket,
      AVG(CAST(is_wicket AS DOUBLE)) as actual_wicket_rate
    FROM %s
  ", table_name))

  as.list(stats)
}


#' Delete 3-Way Skill Index Data for Matches
#'
#' Deletes 3-way skill index data for specified matches.
#'
#' @param match_ids Character vector of match IDs to delete.
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return Invisibly returns number of rows deleted.
#' @keywords internal
delete_3way_skill_matches <- function(match_ids, format, conn, verbose = TRUE) {
  if (length(match_ids) == 0) {
    return(invisible(0))
  }

  table_name <- paste0(tolower(format), "_3way_skill")

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


#' Get Latest 3-Way Skill Index State for Players
#'
#' Returns the most recent skill values for all players in a format.
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#' @param skill_type Character. "run" or "wicket". Default "run".
#'
#' @return data.frame with player_id, role (batter/bowler), and latest skill.
#' @keywords internal
get_latest_3way_player_skills <- function(format, conn, skill_type = "run") {
  table_name <- paste0(tolower(format), "_3way_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(data.frame())
  }

  skill_col <- if (skill_type == "run") "run_skill_after" else "wicket_skill_after"

  # Get latest batter skills
  batters <- DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        batter_id as player_id,
        batter_%s as skill,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT player_id, skill, match_date, 'batter' as role
    FROM ranked
    WHERE rn = 1
  ", skill_col, table_name))

  # Get latest bowler skills
  bowlers <- DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        bowler_id as player_id,
        bowler_%s as skill,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT player_id, skill, match_date, 'bowler' as role
    FROM ranked
    WHERE rn = 1
  ", skill_col, table_name))

  rbind(batters, bowlers)
}


#' Get Latest 3-Way Skill Index State for Venues
#'
#' Returns the most recent skill values for all venues in a format.
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#'
#' @return data.frame with venue, permanent skills, and latest match date.
#' @keywords internal
get_latest_3way_venue_skills <- function(format, conn) {
  table_name <- paste0(tolower(format), "_3way_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(data.frame())
  }

  DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        venue,
        venue_perm_run_skill_after,
        venue_perm_wicket_skill_after,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY venue ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT
      venue,
      venue_perm_run_skill_after as perm_run_skill,
      venue_perm_wicket_skill_after as perm_wicket_skill,
      match_date as last_match_date
    FROM ranked
    WHERE rn = 1
  ", table_name))
}


#' Store 3-Way Skill Index Parameters
#'
#' Stores the parameters used for skill index calculation for reproducibility.
#'
#' @param params List of parameters used for calculation.
#' @param last_delivery_id Character. ID of last processed delivery.
#' @param last_match_date Date. Date of last processed match.
#' @param total_deliveries Integer. Total deliveries processed.
#' @param conn A DuckDB connection object.
#'
#' @return Invisibly returns TRUE on success.
#' @keywords internal
store_3way_skill_params <- function(params, last_delivery_id, last_match_date,
                                     total_deliveries, conn) {
  # Ensure table exists
  if (!"skill_index_params" %in% DBI::dbListTables(conn)) {
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS skill_index_params (
        format VARCHAR PRIMARY KEY,
        alpha_run_max DOUBLE,
        alpha_run_min DOUBLE,
        alpha_run_halflife DOUBLE,
        alpha_wicket_max DOUBLE,
        alpha_wicket_min DOUBLE,
        alpha_wicket_halflife DOUBLE,
        decay_rate DOUBLE,
        w_batter_run DOUBLE,
        w_bowler_run DOUBLE,
        w_venue_session_run DOUBLE,
        w_venue_perm_run DOUBLE,
        last_delivery_id VARCHAR,
        last_match_date DATE,
        total_deliveries INTEGER,
        calculated_at TIMESTAMP
      )
    ")
  }

  # Delete existing entry for this format
  DBI::dbExecute(conn, "DELETE FROM skill_index_params WHERE format = ?",
                 params = list(params$format))

  # Insert new parameters
  DBI::dbExecute(conn, "
    INSERT INTO skill_index_params
    (format, alpha_run_max, alpha_run_min, alpha_run_halflife,
     alpha_wicket_max, alpha_wicket_min, alpha_wicket_halflife,
     decay_rate, w_batter_run, w_bowler_run, w_venue_session_run, w_venue_perm_run,
     last_delivery_id, last_match_date, total_deliveries, calculated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ", params = list(
    params$format,
    params$alpha_run_max, params$alpha_run_min, params$alpha_run_halflife,
    params$alpha_wicket_max, params$alpha_wicket_min, params$alpha_wicket_halflife,
    params$decay_rate,
    params$w_batter_run, params$w_bowler_run,
    params$w_venue_session_run, params$w_venue_perm_run,
    last_delivery_id, as.character(last_match_date),
    total_deliveries, Sys.time()
  ))

  invisible(TRUE)
}


NULL
