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
  paste0(prefix, format, "_player_", metric, "_history")
}


#' Ensure a metric history table exists
#' @keywords internal
ensure_metric_history_table <- function(metric, format, conn, gender = NULL) {
  table_name <- build_metric_table_name(metric, format, gender)

  if (!table_name %in% DBI::dbListTables(conn)) {
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

  # Delete existing snapshot for this date (in case of re-run)
  DBI::dbExecute(conn, sprintf(
    "DELETE FROM %s WHERE snapshot_date = ?", table_name),
    params = list(as.character(snapshot_date)))

  DBI::dbWriteTable(conn, table_name, combined_df, append = TRUE, row.names = FALSE)

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

  if (!table_name %in% DBI::dbListTables(conn)) {
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

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(data.frame(
      player_id = character(0),
      role = character(0),
      percentile = numeric(0),
      stringsAsFactors = FALSE
    ))
  }

  player_list <- paste(sprintf("'%s'", escape_sql_strings(player_ids)), collapse = ", ")

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

  if (!table_name %in% DBI::dbListTables(conn)) {
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

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(invisible(0L))
  }

  cutoff_date <- Sys.Date() - (keep_months * 30)

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
#' @param centrality_result Result from calculate_player_centrality().
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

#' Ensure PageRank History Table Exists
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category. Default NULL.
#'
#' @return Invisibly returns TRUE.
#' @keywords internal
ensure_pagerank_history_table <- function(format, conn, gender = NULL) {
  ensure_metric_history_table("pagerank", format, conn, gender)
}


#' Store PageRank Snapshot
#'
#' Stores a dated PageRank snapshot for later lookup during ELO calculation.
#'
#' @param pagerank_result Result from calculate_player_pagerank().
#' @param snapshot_date Date. The date of this snapshot.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category. Default NULL.
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return Invisibly returns the number of rows inserted.
#' @keywords internal
store_pagerank_snapshot <- function(pagerank_result, snapshot_date, format,
                                     conn, gender = NULL, verbose = TRUE) {
  store_metric_snapshot(pagerank_result, snapshot_date, "pagerank", format,
                        conn, gender, verbose)
}


#' Get PageRank As Of Date
#'
#' Returns a player's PageRank from the most recent snapshot BEFORE the given date.
#'
#' @param player_id Character. The player ID to look up.
#' @param role Character. "batter" or "bowler".
#' @param match_date Date or character. The match date.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category. Default NULL.
#'
#' @return List with pagerank, percentile, quality_tier, snapshot_date,
#'   or NULL if no snapshot exists.
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection(read_only = TRUE)
#' result <- get_pagerank_as_of("SPSmith", "batter", "2024-03-01", "test", conn)
#' DBI::dbDisconnect(conn, shutdown = TRUE)
#' }
#'
#' @export
get_pagerank_as_of <- function(player_id, role, match_date, format, conn, gender = NULL) {
  get_metric_as_of(player_id, role, match_date, "pagerank", format, conn, gender)
}


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


#' Delete Old PageRank Snapshots
#'
#' Removes PageRank snapshots older than the retention period.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param keep_months Integer. Months of history to retain.
#' @param conn DBI connection to the database.
#' @param gender Character. Gender category. Default NULL.
#'
#' @return Invisibly returns the number of rows deleted.
#' @keywords internal
delete_old_pagerank_snapshots <- function(format,
                                           keep_months = CENTRALITY_SNAPSHOT_KEEP_MONTHS,
                                           conn, gender = NULL) {
  delete_old_metric_snapshots("pagerank", format, keep_months, conn, gender)
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
#' @examples
#' \dontrun{
#' conn <- get_db_connection(read_only = TRUE)
#' pr <- calculate_player_pagerank(conn, format = "t20")
#' top_batters <- get_top_pagerank_players(pr, role = "batter", n = 10)
#' top_all <- get_top_pagerank_players(pr, role = "both", n = 20)
#' DBI::dbDisconnect(conn, shutdown = TRUE)
#' }
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
#' @return Called for side effects (printing). Returns `NULL` invisibly.
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection(read_only = TRUE)
#' pr <- calculate_player_pagerank(conn, format = "t20")
#' print_pagerank_summary(pr)
#' print_pagerank_summary(pr, n_top = 5)
#' DBI::dbDisconnect(conn, shutdown = TRUE)
#' }
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
#' @return Called for side effects (printing). Returns `NULL` invisibly.
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection(read_only = TRUE)
#' cent <- calculate_player_centrality(conn, format = "t20")
#' print_centrality_summary(cent)
#' print_centrality_summary(cent, n_top = 5)
#' DBI::dbDisconnect(conn, shutdown = TRUE)
#' }
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
