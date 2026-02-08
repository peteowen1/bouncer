# Player Network Centrality - History and Snapshots
#
# Functions for storing and retrieving dated centrality snapshots.
# Prevents data leakage during ELO calculation by using only centrality
# computed from matches BEFORE the current match date.
#
# Split from player_centrality.R for better maintainability.

# ============================================================================
# CENTRALITY HISTORY (DATED SNAPSHOTS FOR ELO INTEGRATION)
# ============================================================================
# Stores centrality as dated snapshots to prevent data leakage in ELO calculation.
# Each snapshot contains centrality computed from data UP TO that date only.
# During ELO calculation, we look up the most recent snapshot BEFORE match date.

#' Ensure Centrality History Table Exists
#'
#' Creates the dated centrality snapshot table for a format/gender if it doesn't exist.
#' This table stores historical centrality snapshots that can be looked up by date
#' to prevent data leakage during ELO calculation.
#'
#' The table includes additional columns for network analysis:
#' - centrality: The Opsahl weighted degree centrality score
#' - unique_opponents: Number of unique opponents faced
#' - avg_opponent_degree: Average degree of those opponents
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category: "mens" or "womens". Default NULL for
#'   backwards compatibility (uses format-only table name).
#'
#' @return Invisibly returns TRUE.
#' @export
ensure_centrality_history_table <- function(format, conn, gender = NULL) {
  format <- tolower(format)

  # Build table name with optional gender prefix
  if (!is.null(gender)) {
    gender <- tolower(gender)
    table_name <- paste0(gender, "_", format, "_player_centrality_history")
  } else {
    table_name <- paste0(format, "_player_centrality_history")
  }

  if (!table_name %in% DBI::dbListTables(conn)) {
    DBI::dbExecute(conn, sprintf("
      CREATE TABLE IF NOT EXISTS %s (
        snapshot_date DATE NOT NULL,
        player_id VARCHAR NOT NULL,
        role VARCHAR NOT NULL,
        centrality DOUBLE,
        percentile DOUBLE,
        quality_tier VARCHAR,
        deliveries INTEGER,
        unique_opponents INTEGER,
        avg_opponent_degree DOUBLE,
        PRIMARY KEY (snapshot_date, player_id, role)
      )
    ", table_name))

    # Create index for fast date-based lookups
    DBI::dbExecute(conn, sprintf("
      CREATE INDEX IF NOT EXISTS idx_%s_player_date
      ON %s (player_id, role, snapshot_date DESC)
    ", table_name, table_name))

    cli::cli_alert_success("Created table: {table_name}")
  }

  invisible(TRUE)
}


#' Store Centrality Snapshot
#'
#' Stores a dated centrality snapshot for later lookup during ELO calculation.
#' The snapshot_date should be the date of the most recent match INCLUDED
#' in the centrality calculation.
#'
#' @param centrality_result Result from calculate_player_centrality().
#' @param snapshot_date Date. The date of this snapshot (typically last match date).
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category: "mens" or "womens". Default NULL for
#'   backwards compatibility (uses format-only table name).
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return Invisibly returns the number of rows inserted.
#' @export
store_centrality_snapshot <- function(centrality_result,
                                       snapshot_date,
                                       format,
                                       conn,
                                       gender = NULL,
                                       verbose = TRUE) {
  format <- tolower(format)

  # Build table name with optional gender prefix
  if (!is.null(gender)) {
    gender <- tolower(gender)
    table_name <- paste0(gender, "_", format, "_player_centrality_history")
  } else {
    table_name <- paste0(format, "_player_centrality_history")
  }

  # Ensure table exists
  ensure_centrality_history_table(format, conn, gender)

  # Combine batters and bowlers
  batters_df <- centrality_result$batters
  batters_df$snapshot_date <- as.character(snapshot_date)

  bowlers_df <- centrality_result$bowlers
  bowlers_df$snapshot_date <- as.character(snapshot_date)

  combined_df <- rbind(batters_df, bowlers_df)

  # Reorder columns for insert
  combined_df <- combined_df[, c("snapshot_date", "player_id", "role",
                                  "centrality", "percentile", "quality_tier",
                                  "deliveries", "unique_opponents", "avg_opponent_degree")]

  # Delete existing snapshot for this date (in case of re-run)
  DBI::dbExecute(conn, sprintf("
    DELETE FROM %s WHERE snapshot_date = '%s'
  ", table_name, snapshot_date))

  # Insert new snapshot
  DBI::dbWriteTable(conn, table_name, combined_df, append = TRUE, row.names = FALSE)

  if (verbose) {
    cli::cli_alert_success(
      "Stored centrality snapshot for {snapshot_date}: {nrow(combined_df)} player-roles ({nrow(batters_df)} batters, {nrow(bowlers_df)} bowlers)"
    )
  }

  invisible(nrow(combined_df))
}


#' Get Centrality As Of Date
#'
#' Returns a player's centrality from the most recent snapshot BEFORE the given date.
#' This ensures no data leakage - we only use centrality computed from matches
#' that occurred before the current match being processed.
#'
#' @param player_id Character. The player ID to look up.
#' @param role Character. "batter" or "bowler".
#' @param match_date Date or character. The match date (centrality must be from BEFORE this).
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category: "mens" or "womens". Default NULL for
#'   backwards compatibility (uses format-only table name).
#'
#' @return List with centrality, percentile, quality_tier, snapshot_date,
#'   unique_opponents, avg_opponent_degree, or NULL if no snapshot exists before that date.
#' @export
get_centrality_as_of <- function(player_id, role, match_date, format, conn, gender = NULL) {
  format <- tolower(format)

  # Build table name with optional gender prefix
  if (!is.null(gender)) {
    gender <- tolower(gender)
    table_name <- paste0(gender, "_", format, "_player_centrality_history")
  } else {
    table_name <- paste0(format, "_player_centrality_history")
  }

  # Check if table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  query <- sprintf("
    SELECT centrality, percentile, quality_tier, snapshot_date, deliveries,
           unique_opponents, avg_opponent_degree
    FROM %s
    WHERE player_id = '%s'
      AND role = '%s'
      AND snapshot_date < '%s'
    ORDER BY snapshot_date DESC
    LIMIT 1
  ", table_name, player_id, role, as.character(match_date))

  result <- DBI::dbGetQuery(conn, query)

  if (nrow(result) == 0) {
    return(NULL)
  }

  as.list(result)
}


#' Batch Get Centrality For Match
#'
#' Efficiently retrieves centrality data for all players in a match at once.
#' Much faster than individual lookups when processing many deliveries.
#'
#' @param player_ids Character vector. All unique player IDs in the match.
#' @param match_date Date or character. The match date.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category: "mens" or "womens". Default NULL for
#'   backwards compatibility (uses format-only table name).
#'
#' @return Data frame with player_id, role, percentile columns,
#'   or empty data frame if no snapshots exist.
#' @export
batch_get_centrality_for_match <- function(player_ids, match_date, format, conn, gender = NULL) {
  format <- tolower(format)

  # Build table name with optional gender prefix
  if (!is.null(gender)) {
    gender <- tolower(gender)
    table_name <- paste0(gender, "_", format, "_player_centrality_history")
  } else {
    table_name <- paste0(format, "_player_centrality_history")
  }

  # Check if table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    return(data.frame(
      player_id = character(0),
      role = character(0),
      percentile = numeric(0),
      stringsAsFactors = FALSE
    ))
  }

  # Use window function to get most recent snapshot per player/role
  player_list <- paste(sprintf("'%s'", player_ids), collapse = ", ")

  query <- sprintf("
    WITH ranked AS (
      SELECT
        player_id,
        role,
        percentile,
        snapshot_date,
        ROW_NUMBER() OVER (PARTITION BY player_id, role ORDER BY snapshot_date DESC) as rn
      FROM %s
      WHERE player_id IN (%s)
        AND snapshot_date < '%s'
    )
    SELECT player_id, role, percentile
    FROM ranked
    WHERE rn = 1
  ", table_name, player_list, as.character(match_date))

  DBI::dbGetQuery(conn, query)
}


#' Get Centrality Snapshot Dates
#'
#' Returns all available snapshot dates for a format.
#' Useful for checking snapshot coverage or debugging.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category: "mens" or "womens". Default NULL for
#'   backwards compatibility (uses format-only table name).
#'
#' @return Character vector of snapshot dates, or empty vector if none.
#' @export
get_centrality_snapshot_dates <- function(format, conn, gender = NULL) {
  format <- tolower(format)

  # Build table name with optional gender prefix
  if (!is.null(gender)) {
    gender <- tolower(gender)
    table_name <- paste0(gender, "_", format, "_player_centrality_history")
  } else {
    table_name <- paste0(format, "_player_centrality_history")
  }

  # Check if table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    return(character(0))
  }

  result <- DBI::dbGetQuery(conn, sprintf("
    SELECT DISTINCT snapshot_date
    FROM %s
    ORDER BY snapshot_date DESC
  ", table_name))

  if (nrow(result) == 0) {
    return(character(0))
  }

  as.character(result$snapshot_date)
}


#' Delete Old Centrality Snapshots
#'
#' Removes centrality snapshots older than the retention period to save space.
#' Typically called after generating new snapshots.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param keep_months Integer. Months of history to retain.
#'   Default uses CENTRALITY_SNAPSHOT_KEEP_MONTHS constant.
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category: "mens" or "womens". Default NULL for
#'   backwards compatibility (uses format-only table name).
#'
#' @return Invisibly returns the number of rows deleted.
#' @export
delete_old_centrality_snapshots <- function(format,
                                             keep_months = CENTRALITY_SNAPSHOT_KEEP_MONTHS,
                                             conn,
                                             gender = NULL) {
  format <- tolower(format)

  # Build table name with optional gender prefix
  if (!is.null(gender)) {
    gender <- tolower(gender)
    table_name <- paste0(gender, "_", format, "_player_centrality_history")
  } else {
    table_name <- paste0(format, "_player_centrality_history")
  }

  # Check if table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    return(invisible(0L))
  }

  cutoff_date <- Sys.Date() - (keep_months * 30)

  result <- DBI::dbExecute(conn, sprintf("
    DELETE FROM %s WHERE snapshot_date < '%s'
  ", table_name, cutoff_date))

  if (result > 0) {
    cli::cli_alert_info("Deleted {result} old centrality snapshots (before {cutoff_date})")
  }

  invisible(result)
}


# ============================================================================
# LEGACY PAGERANK HISTORY FUNCTIONS (Backward Compatibility)
# ============================================================================
# These functions are kept for backward compatibility with existing tables.
# New code should use the centrality functions above.

#' Ensure PageRank History Table Exists
#'
#' Creates the dated PageRank snapshot table for a format/gender if it doesn't exist.
#' This table stores historical PageRank snapshots that can be looked up by date
#' to prevent data leakage during ELO calculation.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category: "mens" or "womens". Default NULL for
#'   backwards compatibility (uses format-only table name).
#'
#' @return Invisibly returns TRUE.
#' @export
ensure_pagerank_history_table <- function(format, conn, gender = NULL) {
  format <- tolower(format)

  # Build table name with optional gender prefix
  if (!is.null(gender)) {
    gender <- tolower(gender)
    table_name <- paste0(gender, "_", format, "_player_pagerank_history")
  } else {
    table_name <- paste0(format, "_player_pagerank_history")
  }

  if (!table_name %in% DBI::dbListTables(conn)) {
    DBI::dbExecute(conn, sprintf("
      CREATE TABLE IF NOT EXISTS %s (
        snapshot_date DATE NOT NULL,
        player_id VARCHAR NOT NULL,
        role VARCHAR NOT NULL,
        pagerank DOUBLE,
        percentile DOUBLE,
        quality_tier VARCHAR,
        deliveries INTEGER,
        PRIMARY KEY (snapshot_date, player_id, role)
      )
    ", table_name))

    # Create index for fast date-based lookups
    DBI::dbExecute(conn, sprintf("
      CREATE INDEX IF NOT EXISTS idx_%s_player_date
      ON %s (player_id, role, snapshot_date DESC)
    ", table_name, table_name))

    cli::cli_alert_success("Created table: {table_name}")
  }

  invisible(TRUE)
}


#' Store PageRank Snapshot
#'
#' Stores a dated PageRank snapshot for later lookup during ELO calculation.
#' The snapshot_date should be the date of the most recent match INCLUDED
#' in the PageRank calculation.
#'
#' @param pagerank_result Result from calculate_player_pagerank().
#' @param snapshot_date Date. The date of this snapshot (typically last match date).
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category: "mens" or "womens". Default NULL for
#'   backwards compatibility (uses format-only table name).
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return Invisibly returns the number of rows inserted.
#' @export
store_pagerank_snapshot <- function(pagerank_result,
                                     snapshot_date,
                                     format,
                                     conn,
                                     gender = NULL,
                                     verbose = TRUE) {
  format <- tolower(format)

  # Build table name with optional gender prefix
  if (!is.null(gender)) {
    gender <- tolower(gender)
    table_name <- paste0(gender, "_", format, "_player_pagerank_history")
  } else {
    table_name <- paste0(format, "_player_pagerank_history")
  }

  # Ensure table exists
  ensure_pagerank_history_table(format, conn, gender)

  # Combine batters and bowlers
  batters_df <- pagerank_result$batters
  batters_df$snapshot_date <- as.character(snapshot_date)

  bowlers_df <- pagerank_result$bowlers
  bowlers_df$snapshot_date <- as.character(snapshot_date)

  combined_df <- rbind(batters_df, bowlers_df)

  # Reorder columns for insert
  combined_df <- combined_df[, c("snapshot_date", "player_id", "role",
                                  "pagerank", "percentile", "quality_tier",
                                  "deliveries")]

  # Delete existing snapshot for this date (in case of re-run)
  DBI::dbExecute(conn, sprintf("
    DELETE FROM %s WHERE snapshot_date = '%s'
  ", table_name, snapshot_date))

  # Insert new snapshot
  DBI::dbWriteTable(conn, table_name, combined_df, append = TRUE, row.names = FALSE)

  if (verbose) {
    cli::cli_alert_success(
      "Stored snapshot for {snapshot_date}: {nrow(combined_df)} player-roles ({nrow(batters_df)} batters, {nrow(bowlers_df)} bowlers)"
    )
  }

  invisible(nrow(combined_df))
}


#' Get PageRank As Of Date
#'
#' Returns a player's PageRank from the most recent snapshot BEFORE the given date.
#' This ensures no data leakage - we only use PageRank computed from matches
#' that occurred before the current match being processed.
#'
#' @param player_id Character. The player ID to look up.
#' @param role Character. "batter" or "bowler".
#' @param match_date Date or character. The match date (PageRank must be from BEFORE this).
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category: "mens" or "womens". Default NULL for
#'   backwards compatibility (uses format-only table name).
#'
#' @return List with pagerank, percentile, quality_tier, snapshot_date,
#'   or NULL if no snapshot exists before that date.
#' @export
get_pagerank_as_of <- function(player_id, role, match_date, format, conn, gender = NULL) {
  format <- tolower(format)

  # Build table name with optional gender prefix
  if (!is.null(gender)) {
    gender <- tolower(gender)
    table_name <- paste0(gender, "_", format, "_player_pagerank_history")
  } else {
    table_name <- paste0(format, "_player_pagerank_history")
  }

  # Check if table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  query <- sprintf("
    SELECT pagerank, percentile, quality_tier, snapshot_date, deliveries
    FROM %s
    WHERE player_id = '%s'
      AND role = '%s'
      AND snapshot_date < '%s'
    ORDER BY snapshot_date DESC
    LIMIT 1
  ", table_name, player_id, role, as.character(match_date))

  result <- DBI::dbGetQuery(conn, query)

  if (nrow(result) == 0) {
    return(NULL)
  }

  as.list(result)
}


#' Batch Get PageRank For Match
#'
#' Efficiently retrieves PageRank data for all players in a match at once.
#' Much faster than individual lookups when processing many deliveries.
#'
#' @param player_ids Character vector. All unique player IDs in the match.
#' @param match_date Date or character. The match date.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category: "mens" or "womens". Default NULL for
#'   backwards compatibility (uses format-only table name).
#'
#' @return Data frame with player_id, role, percentile columns,
#'   or empty data frame if no snapshots exist.
#' @export
batch_get_pagerank_for_match <- function(player_ids, match_date, format, conn, gender = NULL) {
  format <- tolower(format)

  # Build table name with optional gender prefix
  if (!is.null(gender)) {
    gender <- tolower(gender)
    table_name <- paste0(gender, "_", format, "_player_pagerank_history")
  } else {
    table_name <- paste0(format, "_player_pagerank_history")
  }

  # Check if table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    return(data.frame(
      player_id = character(0),
      role = character(0),
      percentile = numeric(0),
      stringsAsFactors = FALSE
    ))
  }

  # Use window function to get most recent snapshot per player/role
  player_list <- paste(sprintf("'%s'", player_ids), collapse = ", ")

  query <- sprintf("
    WITH ranked AS (
      SELECT
        player_id,
        role,
        percentile,
        snapshot_date,
        ROW_NUMBER() OVER (PARTITION BY player_id, role ORDER BY snapshot_date DESC) as rn
      FROM %s
      WHERE player_id IN (%s)
        AND snapshot_date < '%s'
    )
    SELECT player_id, role, percentile
    FROM ranked
    WHERE rn = 1
  ", table_name, player_list, as.character(match_date))

  DBI::dbGetQuery(conn, query)
}


#' Get PageRank Snapshot Dates
#'
#' Returns all available snapshot dates for a format.
#' Useful for checking snapshot coverage or debugging.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category: "mens" or "womens". Default NULL for
#'   backwards compatibility (uses format-only table name).
#'
#' @return Character vector of snapshot dates, or empty vector if none.
#' @export
get_pagerank_snapshot_dates <- function(format, conn, gender = NULL) {
  format <- tolower(format)

  # Build table name with optional gender prefix
  if (!is.null(gender)) {
    gender <- tolower(gender)
    table_name <- paste0(gender, "_", format, "_player_pagerank_history")
  } else {
    table_name <- paste0(format, "_player_pagerank_history")
  }

  # Check if table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    return(character(0))
  }

  result <- DBI::dbGetQuery(conn, sprintf("
    SELECT DISTINCT snapshot_date
    FROM %s
    ORDER BY snapshot_date DESC
  ", table_name))

  if (nrow(result) == 0) {
    return(character(0))
  }

  as.character(result$snapshot_date)
}


#' Delete Old PageRank Snapshots
#'
#' Removes PageRank snapshots older than the retention period to save space.
#' Typically called after generating new snapshots.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param keep_months Integer. Months of history to retain.
#'   Default uses CENTRALITY_SNAPSHOT_KEEP_MONTHS constant.
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category: "mens" or "womens". Default NULL for
#'   backwards compatibility (uses format-only table name).
#'
#' @return Invisibly returns the number of rows deleted.
#' @export
delete_old_pagerank_snapshots <- function(format,
                                           keep_months = CENTRALITY_SNAPSHOT_KEEP_MONTHS,
                                           conn,
                                           gender = NULL) {
  format <- tolower(format)

  # Build table name with optional gender prefix
  if (!is.null(gender)) {
    gender <- tolower(gender)
    table_name <- paste0(gender, "_", format, "_player_pagerank_history")
  } else {
    table_name <- paste0(format, "_player_pagerank_history")
  }

  # Check if table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    return(invisible(0L))
  }

  cutoff_date <- Sys.Date() - (keep_months * 30)

  result <- DBI::dbExecute(conn, sprintf("
    DELETE FROM %s WHERE snapshot_date < '%s'
  ", table_name, cutoff_date))

  if (result > 0) {
    cli::cli_alert_info("Deleted {result} old PageRank snapshots (before {cutoff_date})")
  }

  invisible(result)
}
