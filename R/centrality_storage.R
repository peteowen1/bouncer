# Player Network Centrality - Storage and Interface
#
# Database storage (snapshots) and user-facing functions for centrality/PageRank:
# - History table management (centrality + PageRank snapshots)
# - Batch retrieval for match processing
# - High-level interface (calculate, display, compare)
#
# Consolidated from centrality_history.R, centrality_interface.R, player_centrality.R

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


calculate_player_pagerank <- function(conn,
                                       format = "t20",
                                       gender = "male",
                                       min_deliveries = CENTRALITY_MIN_DELIVERIES,
                                       damping = 0.85,
                                       verbose = TRUE) {

  if (verbose) {
    cli::cli_h2("Calculating Cricket PageRank")
    cli::cli_alert_info("Format: {format}, Gender: {gender}, Min deliveries: {min_deliveries}")
  }

  # Build query based on format and gender
  format_filter <- if (format == "all") {
    "1=1"
  } else {
    format_lower <- tolower(format)
    glue::glue("LOWER(m.match_type) IN ('{format_lower}', 'i{format_lower}')")
  }

  gender_filter <- if (gender == "all") {
    "1=1"
  } else {
    glue::glue("m.gender = '{gender}'")
  }

  query <- glue::glue("
    SELECT
      d.batter_id,
      d.bowler_id,
      d.runs_batter,
      CASE WHEN d.player_out_id IS NOT NULL AND d.player_out_id != '' THEN 1 ELSE 0 END as is_wicket,
      m.match_type
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    WHERE {format_filter}
      AND {gender_filter}
      AND d.batter_id IS NOT NULL
      AND d.bowler_id IS NOT NULL
  ")

  if (verbose) {
    cli::cli_alert_info("Loading delivery data...")
  }

  deliveries <- DBI::dbGetQuery(conn, query)

  if (nrow(deliveries) == 0) {
    cli::cli_abort("No deliveries found for format: {format}, gender: {gender}")
  }

  if (verbose) {
    cli::cli_alert_success("Loaded {nrow(deliveries)} deliveries")
  }

  # Rename for consistency with build_pagerank_matrices
  deliveries$is_wicket_delivery <- deliveries$is_wicket

  # Build matrices
  matrices <- build_matchup_matrices(
    deliveries,
    format = "all",  # Already filtered in query
    min_deliveries = min_deliveries
  )

  # Compute PageRank
  pr_result <- compute_cricket_pagerank(
    matchup_matrix = matrices$matchup_matrix,
    performance_matrix = matrices$performance_matrix,
    wicket_matrix = matrices$wicket_matrix,
    damping = damping,
    verbose = verbose
  )

  # Classify into tiers
  batter_df <- classify_pagerank_tiers(
    pr_result$batter_pagerank,
    n_players = length(matrices$batter_ids)
  )
  batter_df$deliveries <- matrices$batter_deliveries[batter_df$player_id]
  batter_df$role <- "batter"

  bowler_df <- classify_pagerank_tiers(
    pr_result$bowler_pagerank,
    n_players = length(matrices$bowler_ids)
  )
  bowler_df$deliveries <- matrices$bowler_deliveries[bowler_df$player_id]
  bowler_df$role <- "bowler"

  list(
    batters = batter_df,
    bowlers = bowler_df,
    matrices = matrices,
    algorithm = list(
      iterations = pr_result$iterations,
      converged = pr_result$converged,
      format = format,
      gender = gender,
      min_deliveries = min_deliveries,
      damping = damping
    )
  )
}


#' Get Top Players by PageRank
#'
#' Returns the top N players ranked by PageRank quality score.
#'
#' @param pagerank_result Result from calculate_player_pagerank().
#' @param role Character. "batter", "bowler", or "both".
#' @param n Integer. Number of top players to return.
#'
#' @return Data frame with top players.
#'
#' @export
get_top_pagerank_players <- function(pagerank_result, role = "both", n = 20) {
  if (role == "batter") {
    df <- pagerank_result$batters
  } else if (role == "bowler") {
    df <- pagerank_result$bowlers
  } else {
    df <- rbind(pagerank_result$batters, pagerank_result$bowlers)
  }

  df <- df[order(-df$pagerank), ]
  head(df, n)
}


#' Compare PageRank with ELO Ratings
#'
#' Merges PageRank scores with existing ELO ratings to identify players
#' whose ELO may be inflated (high ELO, low PageRank) or underrated
#' (low ELO, high PageRank).
#'
#' @param pagerank_result Result from calculate_player_pagerank().
#' @param elo_ratings Data frame with player_id and run_elo columns.
#' @param role Character. "batter" or "bowler".
#'
#' @return Data frame with pagerank, elo, and discrepancy metrics.
#'
#' @keywords internal
compare_pagerank_elo <- function(pagerank_result, elo_ratings, role = "batter") {
  pr_df <- if (role == "batter") {
    pagerank_result$batters
  } else {
    pagerank_result$bowlers
  }

  # Merge with ELO
  merged <- merge(pr_df, elo_ratings, by = "player_id", all.x = TRUE)

  # Calculate discrepancy
  # Standardize both metrics to 0-1 scale for comparison
  if (nrow(merged) > 0 && "run_elo" %in% names(merged)) {
    elo_min <- min(merged$run_elo, na.rm = TRUE)
    elo_max <- max(merged$run_elo, na.rm = TRUE)
    merged$elo_normalized <- (merged$run_elo - elo_min) / (elo_max - elo_min + 0.01)

    # PageRank is already normalized (sums to 1), but rescale to similar range
    pr_min <- min(merged$pagerank, na.rm = TRUE)
    pr_max <- max(merged$pagerank, na.rm = TRUE)
    merged$pr_normalized <- (merged$pagerank - pr_min) / (pr_max - pr_min + 0.01)

    # Discrepancy: positive = ELO higher than PageRank suggests (potentially inflated)
    merged$elo_pr_discrepancy <- merged$elo_normalized - merged$pr_normalized

    # Flag potentially inflated players
    merged$potentially_inflated <- merged$elo_pr_discrepancy > 0.3 &
                                    merged$percentile < 50
  }

  merged[order(-merged$elo_pr_discrepancy), ]
}


#' Print PageRank Summary
#'
#' Prints a formatted summary of PageRank results.
#'
#' @param pagerank_result Result from calculate_player_pagerank().
#' @param n_top Integer. Number of top players to show per role.
#'
#' @export
print_pagerank_summary <- function(pagerank_result, n_top = 10) {
  cat("\n")
  cli::cli_h1("Cricket PageRank Summary")

  cli::cli_text("Format: {pagerank_result$algorithm$format}")
  cli::cli_text("Min deliveries: {pagerank_result$algorithm$min_deliveries}")
  cli::cli_text("Converged: {pagerank_result$algorithm$converged} ({pagerank_result$algorithm$iterations} iterations)")
  cli::cli_text("")

  # Top batters
  cli::cli_h2("Top {n_top} Batters by PageRank")
  top_bat <- get_top_pagerank_players(pagerank_result, role = "batter", n = n_top)

  for (i in seq_len(nrow(top_bat))) {
    cli::cli_text("{i}. {top_bat$player_id[i]} - PR: {signif(top_bat$pagerank[i], 3)} ({top_bat$quality_tier[i]}, {top_bat$deliveries[i]} balls)")
  }

  cat("\n")

  # Top bowlers
  cli::cli_h2("Top {n_top} Bowlers by PageRank")
  top_bowl <- get_top_pagerank_players(pagerank_result, role = "bowler", n = n_top)

  for (i in seq_len(nrow(top_bowl))) {
    cli::cli_text("{i}. {top_bowl$player_id[i]} - PR: {signif(top_bowl$pagerank[i], 3)} ({top_bowl$quality_tier[i]}, {top_bowl$deliveries[i]} balls)")
  }

  invisible(pagerank_result)
}


# ============================================================================
# HIGH-LEVEL CENTRALITY INTERFACE (PREFERRED)
# ============================================================================

#' Calculate Player Network Centrality from Database
#'
#' High-level function that loads delivery data from the database and computes
#' network centrality quality scores for all players. This is the preferred
#' method over PageRank as it better separates elite players from isolated clusters.
#'
#' @param conn DBI connection to the bouncer database.
#' @param format Character. Match format: "t20", "odi", "test", or "all".
#' @param gender Character. Gender filter: "male", "female", or "all".
#' @param min_deliveries Integer. Minimum deliveries for inclusion.
#' @param alpha Numeric. Opsahl's alpha parameter (default 0.5 = geometric mean).
#' @param verbose Logical. Print progress messages.
#'
#' @return List with:
#'   - batters: Data frame with batter centrality results
#'   - bowlers: Data frame with bowler centrality results
#'   - matrices: The underlying matchup matrices
#'   - algorithm: Algorithm metadata (format, gender, alpha)
#'
#' @export
#' @examples
#' \dontrun{
#' conn <- get_db_connection(read_only = TRUE)
#' cent <- calculate_player_centrality(conn, format = "t20")
#' head(cent$batters[order(-cent$batters$centrality), ])
#' DBI::dbDisconnect(conn, shutdown = TRUE)
#' }
calculate_player_centrality <- function(conn,
                                         format = "t20",
                                         gender = "male",
                                         min_deliveries = CENTRALITY_MIN_DELIVERIES,
                                         alpha = CENTRALITY_ALPHA,
                                         verbose = TRUE) {

  if (verbose) {
    cli::cli_h2("Calculating Network Centrality")
    cli::cli_alert_info("Format: {format}, Gender: {gender}, Min deliveries: {min_deliveries}, Alpha: {alpha}")
  }

  # Build query based on format and gender
  format_filter <- if (format == "all") {
    "1=1"
  } else {
    format_lower <- tolower(format)
    glue::glue("LOWER(m.match_type) IN ('{format_lower}', 'i{format_lower}')")
  }

  gender_filter <- if (gender == "all") {
    "1=1"
  } else {
    glue::glue("m.gender = '{gender}'")
  }

  query <- glue::glue("
    SELECT
      d.batter_id,
      d.bowler_id,
      d.runs_batter,
      CASE WHEN d.player_out_id IS NOT NULL AND d.player_out_id != '' THEN 1 ELSE 0 END as is_wicket,
      m.match_type
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    WHERE {format_filter}
      AND {gender_filter}
      AND d.batter_id IS NOT NULL
      AND d.bowler_id IS NOT NULL
  ")

  if (verbose) {
    cli::cli_alert_info("Loading delivery data...")
  }

  deliveries <- DBI::dbGetQuery(conn, query)

  if (nrow(deliveries) == 0) {
    cli::cli_abort("No deliveries found for format: {format}, gender: {gender}")
  }

  if (verbose) {
    cli::cli_alert_success("Loaded {nrow(deliveries)} deliveries")
  }

  # Rename for consistency with build_pagerank_matrices
  deliveries$is_wicket_delivery <- deliveries$is_wicket

  # Build matrices (reuse existing function)
  matrices <- build_matchup_matrices(
    deliveries,
    format = "all",  # Already filtered in query
    min_deliveries = min_deliveries
  )

  # Compute Network Centrality (new algorithm)
  cent_result <- calculate_network_centrality(
    matchup_matrix = matrices$matchup_matrix,
    alpha = alpha
  )

  # Classify into tiers
  batter_df <- classify_centrality_tiers(
    cent_result$batter_centrality,
    n_players = length(matrices$batter_ids)
  )
  batter_df$deliveries <- matrices$batter_deliveries[batter_df$player_id]
  batter_df$unique_opponents <- cent_result$batter_unique_opps[batter_df$player_id]
  batter_df$avg_opponent_degree <- cent_result$batter_avg_opp_degree[batter_df$player_id]
  batter_df$role <- "batter"

  bowler_df <- classify_centrality_tiers(
    cent_result$bowler_centrality,
    n_players = length(matrices$bowler_ids)
  )
  bowler_df$deliveries <- matrices$bowler_deliveries[bowler_df$player_id]
  bowler_df$unique_opponents <- cent_result$bowler_unique_opps[bowler_df$player_id]
  bowler_df$avg_opponent_degree <- cent_result$bowler_avg_opp_degree[bowler_df$player_id]
  bowler_df$role <- "bowler"

  if (verbose) {
    # Show sample of elite vs weak for validation
    top_batter <- batter_df[which.max(batter_df$centrality), ]
    weak_batter <- batter_df[which.min(batter_df$centrality), ]
    cli::cli_alert_info("Centrality range - Top: {top_batter$player_id} ({round(top_batter$percentile, 1)}%), Lowest: {weak_batter$player_id} ({round(weak_batter$percentile, 1)}%)")
  }

  list(
    batters = batter_df,
    bowlers = bowler_df,
    matrices = matrices,
    algorithm = list(
      method = "network_centrality",
      alpha = alpha,
      format = format,
      gender = gender,
      min_deliveries = min_deliveries
    )
  )
}


#' Print Centrality Summary
#'
#' Prints a formatted summary of network centrality results.
#'
#' @param centrality_result Result from calculate_player_centrality().
#' @param n_top Integer. Number of top players to show per role.
#'
#' @export
print_centrality_summary <- function(centrality_result, n_top = 10) {
  cat("\n")
  cli::cli_h1("Network Centrality Summary")

  cli::cli_text("Format: {centrality_result$algorithm$format}")
  cli::cli_text("Min deliveries: {centrality_result$algorithm$min_deliveries}")
  cli::cli_text("Alpha: {centrality_result$algorithm$alpha}")
  cli::cli_text("")

  # Top batters
  cli::cli_h2("Top {n_top} Batters by Centrality")
  top_bat <- centrality_result$batters[order(-centrality_result$batters$centrality), ]
  top_bat <- head(top_bat, n_top)

  for (i in seq_len(nrow(top_bat))) {
    cli::cli_text("{i}. {top_bat$player_id[i]} - C: {round(top_bat$centrality[i], 1)} ({top_bat$quality_tier[i]}, {top_bat$unique_opponents[i]} opponents, avg deg {round(top_bat$avg_opponent_degree[i], 1)})")
  }

  cat("\n")

  # Top bowlers
  cli::cli_h2("Top {n_top} Bowlers by Centrality")
  top_bowl <- centrality_result$bowlers[order(-centrality_result$bowlers$centrality), ]
  top_bowl <- head(top_bowl, n_top)

  for (i in seq_len(nrow(top_bowl))) {
    cli::cli_text("{i}. {top_bowl$player_id[i]} - C: {round(top_bowl$centrality[i], 1)} ({top_bowl$quality_tier[i]}, {top_bowl$unique_opponents[i]} opponents, avg deg {round(top_bowl$avg_opponent_degree[i], 1)})")
  }

  invisible(centrality_result)
}
