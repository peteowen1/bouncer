# Format-Specific ELO Table Functions
#
# Functions for creating and managing format-specific ELO tables
# (t20_player_elo, odi_player_elo, test_player_elo).


#' Create Format-Specific ELO Table
#'
#' Creates an ELO table for a specific format. Uses migration pattern to
#' add table without affecting existing data.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#' @param overwrite Logical. If TRUE, drops and recreates table. Default FALSE.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
create_format_elo_table <- function(format = "t20", conn, overwrite = FALSE) {

  table_name <- paste0(format, "_player_elo")

  # Check if table exists
  tables <- DBI::dbListTables(conn)

  if (table_name %in% tables) {
    if (!overwrite) {
      cli::cli_alert_info("Table '{table_name}' already exists")
      return(invisible(TRUE))
    }
    cli::cli_alert_warning("Dropping existing '{table_name}' table")
    DBI::dbExecute(conn, paste0("DROP TABLE ", table_name))
  }

  # Create table
  cli::cli_alert_info("Creating '{table_name}' table...")

  DBI::dbExecute(conn, sprintf("
    CREATE TABLE %s (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      batter_id VARCHAR,
      bowler_id VARCHAR,

      -- Run-based ELO (scoring ability)
      batter_run_elo_before DOUBLE,
      batter_run_elo_after DOUBLE,
      bowler_run_elo_before DOUBLE,
      bowler_run_elo_after DOUBLE,

      -- Wicket-based ELO (survival/strike ability)
      batter_wicket_elo_before DOUBLE,
      batter_wicket_elo_after DOUBLE,
      bowler_wicket_elo_before DOUBLE,
      bowler_wicket_elo_after DOUBLE,

      -- For calibration validation
      exp_runs DOUBLE,
      exp_wicket DOUBLE,
      actual_runs INTEGER,
      is_wicket BOOLEAN
    )
  ", table_name))

  # Create indexes
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_match ON %s(match_id)", format, table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_batter ON %s(batter_id)", format, table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_bowler ON %s(bowler_id)", format, table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_date ON %s(match_date)", format, table_name))

  cli::cli_alert_success("Created '{table_name}' table with indexes")

  invisible(TRUE)
}


#' Insert Format ELO Records
#'
#' Batch inserts ELO records into a format-specific table.
#'
#' @param elos_df Data frame. ELO records to insert with columns:
#'   delivery_id, match_id, match_date, batter_id, bowler_id,
#'   batter_run_elo_before, batter_run_elo_after, bowler_run_elo_before, bowler_run_elo_after,
#'   batter_wicket_elo_before, batter_wicket_elo_after, bowler_wicket_elo_before, bowler_wicket_elo_after,
#'   exp_runs, exp_wicket, actual_runs, is_wicket
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns number of rows inserted
#' @keywords internal
insert_format_elos <- function(elos_df, format = "t20", conn) {

  table_name <- paste0(format, "_player_elo")

  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_danger("Table '{table_name}' does not exist")
    cli::cli_alert_info("Run create_format_elo_table('{format}', conn) first")
    return(invisible(0))
  }

  # Required columns
  required_cols <- c("delivery_id", "match_id", "match_date", "batter_id", "bowler_id",
                     "batter_run_elo_before", "batter_run_elo_after",
                     "bowler_run_elo_before", "bowler_run_elo_after",
                     "batter_wicket_elo_before", "batter_wicket_elo_after",
                     "bowler_wicket_elo_before", "bowler_wicket_elo_after",
                     "exp_runs", "exp_wicket", "actual_runs", "is_wicket")

  missing_cols <- setdiff(required_cols, names(elos_df))
  if (length(missing_cols) > 0) {
    cli::cli_alert_danger("Missing columns: {paste(missing_cols, collapse = ', ')}")
    return(invisible(0))
  }

  # Select only required columns
  elos_df <- elos_df[, required_cols]

  # Insert
  DBI::dbWriteTable(conn, table_name, elos_df, append = TRUE)

  invisible(nrow(elos_df))
}


#' Get ELO Statistics for Format
#'
#' Retrieves summary statistics for ELO ratings in a format table.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with ELO statistics
#' @keywords internal
get_format_elo_stats <- function(format = "t20", conn) {

  table_name <- paste0(format, "_player_elo")

  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Table '{table_name}' does not exist")
    return(NULL)
  }

  DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(*) as total_records,
      COUNT(DISTINCT batter_id) as unique_batters,
      COUNT(DISTINCT bowler_id) as unique_bowlers,
      MIN(match_date) as first_date,
      MAX(match_date) as last_date,
      AVG(batter_run_elo_after) as mean_batter_run_elo,
      AVG(bowler_run_elo_after) as mean_bowler_run_elo,
      AVG(batter_wicket_elo_after) as mean_batter_wicket_elo,
      AVG(bowler_wicket_elo_after) as mean_bowler_wicket_elo
    FROM %s
  ", table_name))
}


#' Clear Format ELO Table
#'
#' Removes all records from a format-specific ELO table.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns number of rows deleted
#' @keywords internal
clear_format_elo_table <- function(format = "t20", conn) {

  table_name <- paste0(format, "_player_elo")

  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Table '{table_name}' does not exist")
    return(invisible(0))
  }

  count <- DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) as n FROM %s", table_name))$n
  DBI::dbExecute(conn, sprintf("DELETE FROM %s", table_name))

  cli::cli_alert_success("Deleted {count} records from '{table_name}'")
  invisible(count)
}


# ============================================================================
# ELO CALCULATION PARAMS MANAGEMENT
# ============================================================================

#' Build Current ELO Parameters
#'
#' Creates a list of current ELO parameters from constants.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#'
#' @return Named list of parameters
#' @keywords internal
build_elo_params <- function(format = "t20") {
  # Get format-specific K factors
  k_run <- switch(format,
    "t20" = K_RUN_T20,
    "odi" = K_RUN_ODI,
    "test" = K_RUN_TEST,
    K_RUN_T20  # default
  )

  k_wicket <- switch(format,
    "t20" = K_WICKET_T20,
    "odi" = K_WICKET_ODI,
    "test" = K_WICKET_TEST,
    K_WICKET_T20
  )

  k_wicket_survival <- switch(format,
    "t20" = K_WICKET_SURVIVAL_T20,
    "odi" = K_WICKET_SURVIVAL_ODI,
    "test" = K_WICKET_SURVIVAL_TEST,
    K_WICKET_SURVIVAL_T20
  )

  list(
    format = format,
    k_run = k_run,
    k_wicket = k_wicket,
    k_wicket_survival = k_wicket_survival,
    run_score_wicket = RUN_SCORE_WICKET,
    run_score_dot = RUN_SCORE_DOT,
    run_score_single = RUN_SCORE_SINGLE,
    run_score_two = RUN_SCORE_DOUBLE,
    run_score_three = RUN_SCORE_THREE,
    run_score_four = RUN_SCORE_FOUR,
    run_score_six = RUN_SCORE_SIX,
    elo_start = DUAL_ELO_START,
    elo_divisor = DUAL_ELO_DIVISOR
  )
}


#' Get Stored ELO Parameters
#'
#' Retrieves stored ELO calculation parameters for a format.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Named list of parameters or NULL if not found
#' @keywords internal
get_stored_elo_params <- function(format = "t20", conn) {
  # Ensure table exists
  if (!"elo_calculation_params" %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  result <- DBI::dbGetQuery(conn, "
    SELECT * FROM elo_calculation_params WHERE format = ?
  ", params = list(format))

  if (nrow(result) == 0) {
    return(NULL)
  }

  as.list(result[1, ])
}


#' Store ELO Parameters
#'
#' Stores ELO calculation parameters after a run completes.
#'
#' @param params Named list of parameters from build_elo_params()
#' @param last_delivery_id Character. ID of last processed delivery
#' @param last_match_date Date. Date of last processed match
#' @param total_deliveries Integer. Total deliveries processed
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
store_elo_params <- function(params, last_delivery_id, last_match_date, total_deliveries, conn) {
  # Ensure table exists
  if (!"elo_calculation_params" %in% DBI::dbListTables(conn)) {
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS elo_calculation_params (
        format VARCHAR PRIMARY KEY,
        k_run DOUBLE,
        k_wicket DOUBLE,
        k_wicket_survival DOUBLE,
        run_score_wicket DOUBLE,
        run_score_dot DOUBLE,
        run_score_single DOUBLE,
        run_score_two DOUBLE,
        run_score_three DOUBLE,
        run_score_four DOUBLE,
        run_score_six DOUBLE,
        elo_start DOUBLE,
        elo_divisor DOUBLE,
        last_delivery_id VARCHAR,
        last_match_date DATE,
        total_deliveries INTEGER,
        calculated_at TIMESTAMP
      )
    ")
  }

  # Delete existing and insert new
 DBI::dbExecute(conn, "DELETE FROM elo_calculation_params WHERE format = ?",
                 params = list(params$format))

  DBI::dbExecute(conn, "
    INSERT INTO elo_calculation_params
    (format, k_run, k_wicket, k_wicket_survival, run_score_wicket, run_score_dot,
     run_score_single, run_score_two, run_score_three, run_score_four, run_score_six,
     elo_start, elo_divisor, last_delivery_id, last_match_date, total_deliveries, calculated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ", params = list(
    params$format, params$k_run, params$k_wicket, params$k_wicket_survival,
    params$run_score_wicket, params$run_score_dot, params$run_score_single,
    params$run_score_two, params$run_score_three, params$run_score_four,
    params$run_score_six, params$elo_start, params$elo_divisor,
    last_delivery_id, as.character(last_match_date), total_deliveries, Sys.time()
  ))

  invisible(TRUE)
}


#' Compare ELO Parameters
#'
#' Compares current parameters to stored parameters.
#'
#' @param current Named list of current parameters
#' @param stored Named list of stored parameters
#'
#' @return Logical. TRUE if parameters match, FALSE otherwise
#' @keywords internal
elo_params_match <- function(current, stored) {
  if (is.null(stored)) {
    return(FALSE)
  }

  # Compare all numeric params (not format, dates, counts)
  param_names <- c("k_run", "k_wicket", "k_wicket_survival",
                   "run_score_wicket", "run_score_dot", "run_score_single",
                   "run_score_two", "run_score_three", "run_score_four",
                   "run_score_six", "elo_start", "elo_divisor")

  for (p in param_names) {
    if (abs(current[[p]] - stored[[p]]) > 1e-6) {
      return(FALSE)
    }
  }

  TRUE
}


#' Get All Players' Final ELO State
#'
#' Loads the final ELO state for all players from existing data.
#' Used for incremental processing. Correctly handles players who both
#' bat and bowl by taking the ELO from their most recent appearance.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return List with four environments: batter_run_elo, batter_wicket_elo,
#'   bowler_run_elo, bowler_wicket_elo (separate batting and bowling ELOs)
#' @keywords internal
get_all_player_elo_state <- function(format = "t20", conn) {
  table_name <- paste0(format, "_player_elo")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  # Get final BATTER ELOs from their most recent batting appearance
  batter_elos <- DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        batter_id as player_id,
        batter_run_elo_after as run_elo,
        batter_wicket_elo_after as wicket_elo,
        ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT player_id, run_elo, wicket_elo
    FROM ranked
    WHERE rn = 1
  ", table_name))

  # Get final BOWLER ELOs from their most recent bowling appearance
  bowler_elos <- DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        bowler_id as player_id,
        bowler_run_elo_after as run_elo,
        bowler_wicket_elo_after as wicket_elo,
        ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT player_id, run_elo, wicket_elo
    FROM ranked
    WHERE rn = 1
  ", table_name))

  if (nrow(batter_elos) == 0 && nrow(bowler_elos) == 0) {
    return(NULL)
  }

  # Create environments for fast lookup - SEPARATE for batting and bowling
  batter_run_elo <- new.env(hash = TRUE)
  batter_wicket_elo <- new.env(hash = TRUE)
  bowler_run_elo <- new.env(hash = TRUE)
  bowler_wicket_elo <- new.env(hash = TRUE)

  # Populate batter ELOs
  for (i in seq_len(nrow(batter_elos))) {
    p <- batter_elos$player_id[i]
    batter_run_elo[[p]] <- batter_elos$run_elo[i]
    batter_wicket_elo[[p]] <- batter_elos$wicket_elo[i]
  }

  # Populate bowler ELOs
  for (i in seq_len(nrow(bowler_elos))) {
    p <- bowler_elos$player_id[i]
    bowler_run_elo[[p]] <- bowler_elos$run_elo[i]
    bowler_wicket_elo[[p]] <- bowler_elos$wicket_elo[i]
  }

  cli::cli_alert_success("Loaded ELO state for {nrow(batter_elos)} batters and {nrow(bowler_elos)} bowlers")

  list(
    batter_run_elo = batter_run_elo,
    batter_wicket_elo = batter_wicket_elo,
    bowler_run_elo = bowler_run_elo,
    bowler_wicket_elo = bowler_wicket_elo,
    n_batters = nrow(batter_elos),
    n_bowlers = nrow(bowler_elos)
  )
}


#' Get Last Processed Delivery Info
#'
#' Gets information about the last processed delivery for incremental updates.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with last_delivery_id and last_match_date, or NULL
#' @keywords internal
get_last_processed_delivery <- function(format = "t20", conn) {
  table_name <- paste0(format, "_player_elo")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  result <- DBI::dbGetQuery(conn, sprintf("
    SELECT delivery_id, match_date
    FROM %s
    ORDER BY match_date DESC, delivery_id DESC
    LIMIT 1
  ", table_name))

  if (nrow(result) == 0) {
    return(NULL)
  }

  result
}
