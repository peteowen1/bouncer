# Player Network Centrality - High-Level Interface
#
# User-facing functions for computing and displaying player network centrality
# and PageRank from the database.
#
# Split from player_centrality.R for better maintainability.

# ============================================================================
# HIGH-LEVEL PAGERANK INTERFACE
# ============================================================================

#' Calculate Player PageRank from Database
#'
#' High-level function that loads delivery data from the database and computes
#' PageRank quality scores for all players.
#'
#' @param conn DBI connection to the bouncer database.
#' @param format Character. Match format: "t20", "odi", "test", or "all".
#' @param gender Character. Gender filter: "male", "female", or "all".
#' @param min_deliveries Integer. Minimum deliveries for inclusion.
#' @param damping Numeric. PageRank damping factor.
#' @param verbose Logical. Print progress messages.
#'
#' @return List with:
#'   - batters: Data frame with batter PageRank results
#'   - bowlers: Data frame with bowler PageRank results
#'   - matrices: The underlying matchup matrices
#'   - algorithm: Algorithm metadata (iterations, convergence)
#'
#' @export
#' @examples
#' \dontrun{
#' conn <- get_db_connection(read_only = TRUE)
#' pr <- calculate_player_pagerank(conn, format = "t20")
#' head(pr$batters[order(-pr$batters$pagerank), ])
#' DBI::dbDisconnect(conn, shutdown = TRUE)
#' }
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
#' @export
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
