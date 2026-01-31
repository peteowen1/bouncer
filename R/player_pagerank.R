# Player PageRank Quality Assessment
#
# A PageRank-style algorithm for computing global player quality by propagating
# "authority" through the batter↔bowler matchup network.
#
# Core concept:
#   - A batter is good if they score well against good bowlers
#   - A bowler is good if they restrict good batters
#   - Quality flows through the bipartite matchup graph
#
# This addresses the isolated cluster inflation problem where players in
# weak leagues accumulate inflated ELOs by only facing weak opponents.
#
# Mathematical formulation (simplified):
#   batter_quality[i] = α * Σ_j (performance[i,j] * bowler_quality[j]) / norm + (1-α) * base
#   bowler_quality[j] = α * Σ_i ((1 - performance[i,j]) * batter_quality[i]) / norm + (1-α) * base
#
# Where α is the damping factor (typically 0.85).

# ============================================================================
# MATCHUP GRAPH CONSTRUCTION
# ============================================================================

#' Build Matchup Matrices from Deliveries Data
#'
#' Constructs the adjacency and performance matrices needed for PageRank
#' from ball-by-ball delivery data.
#'
#' @param deliveries Data frame with columns: batter_id, bowler_id, runs_batter,
#'   is_wicket_delivery (or player_out_id), match_type.
#' @param format Character. Match format filter: "t20", "odi", "test", or "all".
#' @param min_deliveries Integer. Minimum deliveries for a player to be included.
#'   Default uses PAGERANK_MIN_DELIVERIES constant.
#'
#' @return List with:
#'   - matchup_matrix: Sparse matrix of delivery counts (batter x bowler)
#'   - performance_matrix: Normalized runs/ball (0-1 scale)
#'   - wicket_matrix: Wicket proportions (0-1 scale)
#'   - batter_ids: Character vector of batter IDs (row names)
#'   - bowler_ids: Character vector of bowler IDs (col names)
#'   - batter_deliveries: Named vector of total deliveries faced
#'   - bowler_deliveries: Named vector of total deliveries bowled
#'
#' @keywords internal
build_pagerank_matrices <- function(deliveries,
                                     format = "all",
                                     min_deliveries = PAGERANK_MIN_DELIVERIES) {

  # Filter by format if specified
  if (format != "all") {
    format_lower <- tolower(format)
    deliveries <- deliveries[tolower(deliveries$match_type) %in%
                               c(format_lower, paste0("i", format_lower)), ]
  }

  # Detect wicket column (different naming conventions in data)
  wicket_col <- if ("is_wicket_delivery" %in% names(deliveries)) {
    "is_wicket_delivery"
  } else if ("player_out_id" %in% names(deliveries)) {
    # Create boolean from player_out_id
    deliveries$is_wicket <- !is.na(deliveries$player_out_id) &
                            deliveries$player_out_id != ""
    "is_wicket"
  } else {
    # Fallback: assume no wicket data
    deliveries$is_wicket <- FALSE
    "is_wicket"
  }

  # Aggregate by batter-bowler pairs
  agg <- stats::aggregate(
    cbind(
      count = rep(1, nrow(deliveries)),
      runs = deliveries$runs_batter,
      wickets = as.numeric(deliveries[[wicket_col]])
    ) ~ batter_id + bowler_id,
    data = deliveries,
    FUN = sum,
    na.action = stats::na.pass
  )

  # Calculate total deliveries per player
  batter_totals <- stats::aggregate(count ~ batter_id, data = agg, FUN = sum)
  bowler_totals <- stats::aggregate(count ~ bowler_id, data = agg, FUN = sum)

  # Filter to players with minimum deliveries
  valid_batters <- batter_totals$batter_id[batter_totals$count >= min_deliveries]
  valid_bowlers <- bowler_totals$bowler_id[bowler_totals$count >= min_deliveries]

  if (length(valid_batters) == 0 || length(valid_bowlers) == 0) {
    cli::cli_abort("No players meet minimum deliveries threshold of {min_deliveries}")
  }

  # Filter aggregated data
  agg <- agg[agg$batter_id %in% valid_batters & agg$bowler_id %in% valid_bowlers, ]

  # Create matrices using sparse representation (as dense for now, can optimize later)
  batter_ids <- sort(unique(agg$batter_id))
  bowler_ids <- sort(unique(agg$bowler_id))

  n_batters <- length(batter_ids)
  n_bowlers <- length(bowler_ids)

  # Initialize matrices
  matchup_matrix <- matrix(0, nrow = n_batters, ncol = n_bowlers,
                           dimnames = list(batter_ids, bowler_ids))
  runs_matrix <- matrix(0, nrow = n_batters, ncol = n_bowlers,
                        dimnames = list(batter_ids, bowler_ids))
  wicket_matrix <- matrix(0, nrow = n_batters, ncol = n_bowlers,
                          dimnames = list(batter_ids, bowler_ids))

  # Fill matrices
  for (i in seq_len(nrow(agg))) {
    b <- agg$batter_id[i]
    w <- agg$bowler_id[i]
    matchup_matrix[b, w] <- agg$count[i]
    runs_matrix[b, w] <- agg$runs[i]
    wicket_matrix[b, w] <- agg$wickets[i]
  }

  # Calculate normalized performance (runs per ball, scaled 0-1)
  # Avoid division by zero
  perf_matrix <- runs_matrix / pmax(matchup_matrix, 1)
  perf_matrix[matchup_matrix == 0] <- 0

  # Normalize to 0-1 scale using performance scale constant
  perf_matrix <- pmin(perf_matrix / PAGERANK_PERFORMANCE_SCALE, 1)

  # Calculate wicket proportion
  wicket_prop_matrix <- wicket_matrix / pmax(matchup_matrix, 1)
  wicket_prop_matrix[matchup_matrix == 0] <- 0

  # Calculate delivery totals for included players
  batter_deliveries <- rowSums(matchup_matrix)
  bowler_deliveries <- colSums(matchup_matrix)

  cli::cli_alert_success(
    "Built matchup matrices: {n_batters} batters x {n_bowlers} bowlers, {sum(matchup_matrix)} deliveries"
  )

  list(
    matchup_matrix = matchup_matrix,
    performance_matrix = perf_matrix,
    wicket_matrix = wicket_prop_matrix,
    batter_ids = batter_ids,
    bowler_ids = bowler_ids,
    batter_deliveries = batter_deliveries,
    bowler_deliveries = bowler_deliveries
  )
}


# ============================================================================
# BIPARTITE PAGERANK ALGORITHM
# ============================================================================

#' Compute Cricket PageRank
#'
#' Computes PageRank-style quality scores for batters and bowlers using
#' an iterative algorithm on the bipartite matchup graph.
#'
#' The algorithm alternates between updating batter and bowler scores:
#' - Batter quality = weighted average of bowler quality faced, scaled by performance
#' - Bowler quality = weighted average of batter quality restricted, scaled by economy
#'
#' @param matchup_matrix Matrix of delivery counts (batter x bowler).
#' @param performance_matrix Matrix of normalized runs/ball (0-1 scale).
#' @param wicket_matrix Matrix of wicket proportions (0-1 scale). Optional.
#' @param damping Numeric. Damping factor (default from PAGERANK_DAMPING).
#' @param max_iter Integer. Maximum iterations (default from PAGERANK_MAX_ITER).
#' @param tolerance Numeric. Convergence threshold (default from PAGERANK_TOLERANCE).
#' @param wicket_weight Numeric. How much wickets contribute to bowler quality
#'   (default from PAGERANK_WICKET_WEIGHT).
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return List with:
#'   - batter_pagerank: Named vector of batter PageRank scores
#'   - bowler_pagerank: Named vector of bowler PageRank scores
#'   - iterations: Number of iterations until convergence
#'   - converged: Logical, whether algorithm converged
#'
#' @keywords internal
compute_cricket_pagerank <- function(matchup_matrix,
                                      performance_matrix,
                                      wicket_matrix = NULL,
                                      damping = PAGERANK_DAMPING,
                                      max_iter = PAGERANK_MAX_ITER,
                                      tolerance = PAGERANK_TOLERANCE,
                                      wicket_weight = PAGERANK_WICKET_WEIGHT,
                                      verbose = TRUE) {

  n_batters <- nrow(matchup_matrix)
  n_bowlers <- ncol(matchup_matrix)

  batter_ids <- rownames(matchup_matrix)
  bowler_ids <- colnames(matchup_matrix)

  # Initialize with uniform scores (sum to 1 for each group)
  batter_pr <- rep(1 / n_batters, n_batters)
  bowler_pr <- rep(1 / n_bowlers, n_bowlers)

  names(batter_pr) <- batter_ids
  names(bowler_pr) <- bowler_ids

  # Pre-compute row/column sums for normalization
  row_sums <- rowSums(matchup_matrix)
  col_sums <- colSums(matchup_matrix)

  # Handle dangling nodes (players with no matchups after filtering)
  row_sums[row_sums == 0] <- 1
  col_sums[col_sums == 0] <- 1

  # Base score (for damping teleportation)
  base_batter <- (1 - damping) / n_batters
  base_bowler <- (1 - damping) / n_bowlers

  converged <- FALSE
  iter <- 0

  if (verbose) {
    cli::cli_progress_bar("Computing Cricket PageRank", total = max_iter)
  }

  for (iter in seq_len(max_iter)) {
    # Store previous values for convergence check
    prev_batter_pr <- batter_pr
    prev_bowler_pr <- bowler_pr

    # =========================================================================
    # Update Batter PageRank
    # =========================================================================
    # Key insight: facing GOOD bowlers should increase batter PageRank,
    # regardless of performance. Performance is a BONUS.
    #
    # Formula: batter_PR = base + damping * Σ (bowler_PR * matchups * (1 + perf_bonus))
    #
    # - Base authority comes from facing quality opponents
    # - Performance bonus rewards scoring well against good bowlers
    # - This prevents weak-league players from getting inflated scores

    # Weight by opponent quality (bowler PageRank) and matchup count
    # matchup_matrix[i,j] * bowler_pr[j] = authority transfer from facing that bowler
    base_authority <- matchup_matrix %*% bowler_pr

    # Performance bonus: small multiplier (0.5 to 1.5) based on runs scored
    # performance_matrix is 0-1, so (0.5 + performance) gives 0.5-1.5 range
    perf_bonus_matrix <- 0.5 + performance_matrix
    performance_weighted <- (matchup_matrix * perf_bonus_matrix) %*% bowler_pr

    # Combine: 70% from opponent quality, 30% from performance bonus
    weighted_bowler <- 0.7 * base_authority + 0.3 * performance_weighted

    # Normalize by total matchups faced
    new_batter_pr <- base_batter + damping * (weighted_bowler / row_sums)

    # =========================================================================
    # Update Bowler PageRank
    # =========================================================================
    # Key insight: bowling TO good batters increases bowler PageRank,
    # with a bonus for restricting them (low runs/wickets).
    #
    # Formula: bowler_PR = base + damping * Σ (batter_PR * matchups * (1 + econ_bonus))

    # Base authority from facing quality batters
    base_bowler_authority <- t(matchup_matrix) %*% batter_pr

    # Economy bonus: reward for restricting batters
    # (1 - performance) is 0-1, so (0.5 + (1-perf)) gives 0.5-1.5 range
    econ_bonus_matrix <- 0.5 + (1 - performance_matrix)

    # Add wicket component if available
    if (!is.null(wicket_matrix)) {
      # Bowlers get extra credit for taking wickets
      econ_bonus_matrix <- econ_bonus_matrix + wicket_weight * wicket_matrix
    }

    economy_weighted <- t(matchup_matrix * econ_bonus_matrix) %*% batter_pr

    # Combine: 70% from opponent quality, 30% from economy bonus
    weighted_batter <- 0.7 * base_bowler_authority + 0.3 * economy_weighted

    # Normalize by total matchups bowled
    new_bowler_pr <- base_bowler + damping * (weighted_batter / col_sums)

    # Normalize to sum to 1 (maintain probability distribution)
    new_batter_pr <- as.vector(new_batter_pr)
    new_bowler_pr <- as.vector(new_bowler_pr)

    new_batter_pr <- new_batter_pr / sum(new_batter_pr)
    new_bowler_pr <- new_bowler_pr / sum(new_bowler_pr)

    names(new_batter_pr) <- batter_ids
    names(new_bowler_pr) <- bowler_ids

    # Check convergence
    max_diff_batter <- max(abs(new_batter_pr - prev_batter_pr))
    max_diff_bowler <- max(abs(new_bowler_pr - prev_bowler_pr))
    max_diff <- max(max_diff_batter, max_diff_bowler)

    batter_pr <- new_batter_pr
    bowler_pr <- new_bowler_pr

    if (verbose) {
      cli::cli_progress_update()
    }

    if (max_diff < tolerance) {
      converged <- TRUE
      break
    }
  }

  if (verbose) {
    cli::cli_progress_done()
    if (converged) {
      cli::cli_alert_success("PageRank converged in {iter} iterations (max diff: {signif(max_diff, 3)})")
    } else {
      cli::cli_alert_warning("PageRank did not converge after {max_iter} iterations (max diff: {signif(max_diff, 3)})")
    }
  }

  list(
    batter_pagerank = batter_pr,
    bowler_pagerank = bowler_pr,
    iterations = iter,
    converged = converged
  )
}


# ============================================================================
# QUALITY TIER CLASSIFICATION
# ============================================================================

#' Classify Players into Quality Tiers by PageRank
#'
#' Assigns quality tier labels based on PageRank percentiles.
#'
#' @param pagerank_scores Named numeric vector of PageRank scores.
#' @param n_players Total number of players (for percentile calculation).
#'
#' @return Data frame with columns: player_id, pagerank, percentile, quality_tier
#'
#' @keywords internal
classify_pagerank_tiers <- function(pagerank_scores, n_players = NULL) {
  if (is.null(n_players)) {
    n_players <- length(pagerank_scores)
  }

  # Calculate percentile rank
  ranks <- rank(pagerank_scores, ties.method = "average")
  percentiles <- ranks / n_players * 100

  # Assign tiers based on percentiles
  quality_tier <- dplyr::case_when(
    percentiles >= 95 ~ "Elite",
    percentiles >= 80 ~ "Very Good",
    percentiles >= 60 ~ "Above Average",
    percentiles >= 40 ~ "Average",
    percentiles >= 20 ~ "Below Average",
    TRUE ~ "Weak"
  )

  data.frame(
    player_id = names(pagerank_scores),
    pagerank = as.numeric(pagerank_scores),
    percentile = percentiles,
    quality_tier = quality_tier,
    stringsAsFactors = FALSE
  )
}


# ============================================================================
# HIGH-LEVEL INTERFACE
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
                                       min_deliveries = PAGERANK_MIN_DELIVERIES,
                                       damping = PAGERANK_DAMPING,
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
  matrices <- build_pagerank_matrices(
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


NULL
