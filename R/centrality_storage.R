# Player Network Centrality - Storage and Interface
#
# Database storage (snapshots) and user-facing functions for centrality/PageRank:
# - History table management (centrality + PageRank snapshots)
# - Batch retrieval for match processing
# - High-level interface (calculate, display, compare)
#
# Consolidated from centrality_history.R, centrality_interface.R, player_centrality.R
#
# Internal helpers (build_metric_table_name, ensure_metric_history_table, etc.)
# provide shared logic for both centrality and pagerank metric types.

# ============================================================================
# INTERNAL HELPERS (shared by centrality + pagerank public functions)
# ============================================================================

#' Build table name for a metric type
#' @param metric "centrality" or "pagerank"
#' @param format Format string (t20, odi, test)
#' @param gender Optional gender prefix
#' @return Table name string
#' @keywords internal
build_metric_table_name <- function(metric, format, gender = NULL) {
  format <- tolower(format)
  prefix <- if (!is.null(gender)) paste0(tolower(gender), "_") else ""
  table_name <- paste0(prefix, format, "_player_", metric, "_history")
  validate_sql_identifier(table_name, context = "build_metric_table_name")
  table_name
}


#' Ensure a metric history table exists
#' @keywords internal
ensure_metric_history_table <- function(metric, format, conn, gender = NULL) {
  table_name <- build_metric_table_name(metric, format, gender)

  if (!table_exists(conn, table_name)) {
    # Centrality tables have extra network columns
    extra_cols <- if (metric == "centrality") {
      ",\n        unique_opponents INTEGER,\n        avg_opponent_degree DOUBLE"
    } else {
      ""
    }

    DBI::dbExecute(conn, sprintf("
      CREATE TABLE IF NOT EXISTS %s (
        snapshot_date DATE NOT NULL,
        player_id VARCHAR NOT NULL,
        role VARCHAR NOT NULL,
        %s DOUBLE,
        percentile DOUBLE,
        quality_tier VARCHAR,
        deliveries INTEGER%s,
        PRIMARY KEY (snapshot_date, player_id, role)
      )
    ", table_name, metric, extra_cols))

    DBI::dbExecute(conn, sprintf("
      CREATE INDEX IF NOT EXISTS idx_%s_player_date
      ON %s (player_id, role, snapshot_date DESC)
    ", table_name, table_name))

    cli::cli_alert_success("Created table: {table_name}")
  }

  invisible(TRUE)
}


#' Build SQL filter clauses for format and gender
#' @param format Format string (t20, odi, test, or "all")
#' @param gender Gender string (male, female, or "all")
#' @return List with format_filter and gender_filter SQL strings
#' @keywords internal
build_format_gender_filters <- function(format, gender) {
  valid_formats <- c("all", "t20", "odi", "test")
  valid_genders <- c("all", "male", "female")
  format <- match.arg(tolower(format), valid_formats)
  gender <- match.arg(tolower(gender), valid_genders)

  format_filter <- if (format == "all") {
    "1=1"
  } else {
    sprintf("LOWER(m.match_type) IN ('%s', 'i%s')", escape_sql_quotes(format), escape_sql_quotes(format))
  }

  gender_filter <- if (gender == "all") {
    "1=1"
  } else {
    sprintf("m.gender = '%s'", escape_sql_quotes(gender))
  }

  list(format_filter = format_filter, gender_filter = gender_filter)
}


#' Store a metric snapshot (centrality or pagerank)
#' @keywords internal
store_metric_snapshot <- function(result, snapshot_date, metric, format, conn,
                                  gender = NULL, verbose = TRUE) {
  table_name <- build_metric_table_name(metric, format, gender)
  ensure_metric_history_table(metric, format, conn, gender)

  batters_df <- result$batters
  batters_df$snapshot_date <- as.character(snapshot_date)

  bowlers_df <- result$bowlers
  bowlers_df$snapshot_date <- as.character(snapshot_date)

  combined_df <- rbind(batters_df, bowlers_df)

  # Select columns that exist in this metric's table
  base_cols <- c("snapshot_date", "player_id", "role", metric,
                 "percentile", "quality_tier", "deliveries")
  if (metric == "centrality") {
    base_cols <- c(base_cols, "unique_opponents", "avg_opponent_degree")
  }
  available_cols <- intersect(base_cols, names(combined_df))
  combined_df <- combined_df[, available_cols]

  # Atomic replace: delete old snapshot + insert new in a single transaction
  DBI::dbBegin(conn)
  tryCatch({
    DBI::dbExecute(conn, sprintf(
      "DELETE FROM %s WHERE snapshot_date = ?", table_name),
      params = list(as.character(snapshot_date)))
    DBI::dbWriteTable(conn, table_name, combined_df, append = TRUE, row.names = FALSE)
    DBI::dbCommit(conn)
  }, error = function(e) {
    DBI::dbRollback(conn)
    cli::cli_abort("Failed to store {metric} snapshot: {conditionMessage(e)}")
  })

  if (verbose) {
    cli::cli_alert_success(
      "Stored {metric} snapshot for {snapshot_date}: {nrow(combined_df)} player-roles ({nrow(batters_df)} batters, {nrow(bowlers_df)} bowlers)"
    )
  }

  invisible(nrow(combined_df))
}


#' Get metric value as of a date (most recent snapshot before match_date)
#' @keywords internal
get_metric_as_of <- function(player_id, role, match_date, metric, format, conn, gender = NULL) {
  table_name <- build_metric_table_name(metric, format, gender)

  if (!table_exists(conn, table_name)) {
    return(NULL)
  }

  # Build SELECT columns based on metric type
  extra_cols <- if (metric == "centrality") {
    ", unique_opponents, avg_opponent_degree"
  } else {
    ""
  }

  query <- sprintf("
    SELECT %s, percentile, quality_tier, snapshot_date, deliveries%s
    FROM %s
    WHERE player_id = ?
      AND role = ?
      AND snapshot_date < ?
    ORDER BY snapshot_date DESC
    LIMIT 1
  ", metric, extra_cols, table_name)

  result <- DBI::dbGetQuery(conn, query,
    params = list(player_id, role, as.character(match_date)))

  if (nrow(result) == 0) {
    return(NULL)
  }

  as.list(result)
}


#' Batch get metric values for a match
#' @keywords internal
batch_get_metric_for_match <- function(player_ids, match_date, metric, format, conn, gender = NULL) {
  table_name <- build_metric_table_name(metric, format, gender)

  if (!table_exists(conn, table_name)) {
    return(data.frame(
      player_id = character(0),
      role = character(0),
      percentile = numeric(0),
      stringsAsFactors = FALSE
    ))
  }

  player_list <- paste(sprintf("'%s'", escape_sql_quotes(player_ids)), collapse = ", ")

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
        AND snapshot_date < ?
    )
    SELECT player_id, role, percentile
    FROM ranked
    WHERE rn = 1
  ", table_name, player_list)

  DBI::dbGetQuery(conn, query, params = list(as.character(match_date)))
}


#' Get all snapshot dates for a metric
#' @keywords internal
get_metric_snapshot_dates <- function(metric, format, conn, gender = NULL) {
  table_name <- build_metric_table_name(metric, format, gender)

  if (!table_exists(conn, table_name)) {
    return(character(0))
  }

  result <- DBI::dbGetQuery(conn, sprintf("
    SELECT DISTINCT snapshot_date
    FROM %s
    ORDER BY snapshot_date DESC
  ", table_name))

  if (nrow(result) == 0) return(character(0))
  as.character(result$snapshot_date)
}


#' Delete old metric snapshots
#' @keywords internal
delete_old_metric_snapshots <- function(metric, format,
                                         keep_months = CENTRALITY_SNAPSHOT_KEEP_MONTHS,
                                         conn, gender = NULL) {
  table_name <- build_metric_table_name(metric, format, gender)

  if (!table_exists(conn, table_name)) {
    return(invisible(0L))
  }

  cutoff_date <- seq(Sys.Date(), length.out = 2, by = paste0("-", keep_months, " months"))[2]

  result <- DBI::dbExecute(conn, sprintf(
    "DELETE FROM %s WHERE snapshot_date < ?", table_name),
    params = list(as.character(cutoff_date)))

  if (result > 0) {
    cli::cli_alert_info("Deleted {result} old {metric} snapshots (before {cutoff_date})")
  }

  invisible(result)
}


# ============================================================================
# CENTRALITY PUBLIC FUNCTIONS (delegate to internal helpers)
# ============================================================================

ensure_centrality_history_table <- function(format, conn, gender = NULL) {
  ensure_metric_history_table("centrality", format, conn, gender)
}


#' Store Centrality Snapshot
#'
#' Stores a dated centrality snapshot for later lookup during ELO calculation.
#'
#' @param centrality_result List with `batters` and `bowlers` data frames, as
#'   built by data-raw/ratings/player/3way-elo/00_compute_centrality_snapshots.R.
#' @param snapshot_date Date. The date of this snapshot (typically last match date).
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category: "mens" or "womens". Default NULL.
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return Invisibly returns the number of rows inserted.
#' @keywords internal
store_centrality_snapshot <- function(centrality_result, snapshot_date, format,
                                       conn, gender = NULL, verbose = TRUE) {
  store_metric_snapshot(centrality_result, snapshot_date, "centrality", format,
                        conn, gender, verbose)
}


#' Get Centrality As Of Date
#'
#' Returns a player's centrality from the most recent snapshot BEFORE the given date.
#'
#' @param player_id Character. The player ID to look up.
#' @param role Character. "batter" or "bowler".
#' @param match_date Date or character. The match date.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category. Default NULL.
#'
#' @return List with centrality, percentile, quality_tier, snapshot_date,
#'   unique_opponents, avg_opponent_degree, or NULL if no snapshot exists.
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection(read_only = TRUE)
#' result <- get_centrality_as_of("VKohli", "batter", "2024-01-15", "t20", conn)
#' DBI::dbDisconnect(conn, shutdown = TRUE)
#' }
#'
#' @export
get_centrality_as_of <- function(player_id, role, match_date, format, conn, gender = NULL) {
  get_metric_as_of(player_id, role, match_date, "centrality", format, conn, gender)
}


#' Batch Get Centrality For Match
#'
#' Efficiently retrieves centrality data for all players in a match at once.
#'
#' @param player_ids Character vector. All unique player IDs in the match.
#' @param match_date Date or character. The match date.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category. Default NULL.
#'
#' @return Data frame with player_id, role, percentile columns.
#' @keywords internal
batch_get_centrality_for_match <- function(player_ids, match_date, format, conn, gender = NULL) {
  batch_get_metric_for_match(player_ids, match_date, "centrality", format, conn, gender)
}


#' Get Centrality Snapshot Dates
#'
#' Returns all available snapshot dates for a format.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category. Default NULL.
#'
#' @return Character vector of snapshot dates, or empty vector if none.
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection(read_only = TRUE)
#' dates <- get_centrality_snapshot_dates("t20", conn)
#' DBI::dbDisconnect(conn, shutdown = TRUE)
#' }
#'
#' @export
get_centrality_snapshot_dates <- function(format, conn, gender = NULL) {
  get_metric_snapshot_dates("centrality", format, conn, gender)
}


#' Delete Old Centrality Snapshots
#'
#' Removes centrality snapshots older than the retention period.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param keep_months Integer. Months of history to retain.
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category. Default NULL.
#'
#' @return Invisibly returns the number of rows deleted.
#' @keywords internal
delete_old_centrality_snapshots <- function(format,
                                             keep_months = CENTRALITY_SNAPSHOT_KEEP_MONTHS,
                                             conn, gender = NULL) {
  delete_old_metric_snapshots("centrality", format, keep_months, conn, gender)
}


# ============================================================================
# PAGERANK PUBLIC FUNCTIONS (delegate to internal helpers)
# ============================================================================

#' Batch Get PageRank For Match
#'
#' Efficiently retrieves PageRank data for all players in a match at once.
#'
#' @param player_ids Character vector. All unique player IDs in the match.
#' @param match_date Date or character. The match date.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category. Default NULL.
#'
#' @return Data frame with player_id, role, percentile columns.
#' @keywords internal
batch_get_pagerank_for_match <- function(player_ids, match_date, format, conn, gender = NULL) {
  batch_get_metric_for_match(player_ids, match_date, "pagerank", format, conn, gender)
}


#' Get PageRank Snapshot Dates
#'
#' Returns all available snapshot dates for a format.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category. Default NULL.
#'
#' @return Character vector of snapshot dates, or empty vector if none.
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection(read_only = TRUE)
#' dates <- get_pagerank_snapshot_dates("odi", conn)
#' DBI::dbDisconnect(conn, shutdown = TRUE)
#' }
#'
#' @export
get_pagerank_snapshot_dates <- function(format, conn, gender = NULL) {
  get_metric_snapshot_dates("pagerank", format, conn, gender)
}

