# Player Network Centrality - PageRank Algorithm
#
# Bipartite PageRank algorithm implementation for cricket matchup networks.
# This is a legacy algorithm - prefer network centrality for new code.
#
# Split from player_centrality.R for better maintainability.

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
#' @param damping Numeric. Damping factor (default 0.85).
#' @param max_iter Integer. Maximum iterations (default 100).
#' @param tolerance Numeric. Convergence threshold (default 1e-6).
#' @param wicket_weight Numeric. How much wickets contribute to bowler quality
#'   (default 0.3).
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
                                      damping = 0.85,
                                      max_iter = 100,
                                      tolerance = 1e-6,
                                      wicket_weight = 0.3,
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
    # Formula: batter_PR = base + damping * sum (bowler_PR * matchups * (1 + perf_bonus))
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
    # Formula: bowler_PR = base + damping * sum (batter_PR * matchups * (1 + econ_bonus))

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
#' Assigns quality tier labels based on PageRank percentiles. Applies opponent
#' diversity normalization to penalize players in isolated clusters who face
#' few unique opponents.
#'
#' @param pagerank_scores Named numeric vector of PageRank scores.
#' @param n_players Total number of players (for percentile calculation).
#' @param unique_opponent_counts Optional named vector of unique opponent counts.
#'   If provided, applies diversity normalization before ranking.
#' @param player_component Deprecated. Use unique_opponent_counts instead.
#' @param component_sizes Deprecated. Use unique_opponent_counts instead.
#'
#' @return Data frame with columns: player_id, pagerank, percentile, quality_tier
#'
#' @keywords internal
classify_pagerank_tiers <- function(pagerank_scores,
                                     n_players = NULL,
                                     unique_opponent_counts = NULL,
                                     player_component = NULL,
                                     component_sizes = NULL) {
  if (is.null(n_players)) {
    n_players <- length(pagerank_scores)
  }

  # Store original raw PageRank
  raw_pagerank <- pagerank_scores

  # Apply opponent diversity normalization if provided (preferred method)
  if (!is.null(unique_opponent_counts)) {
    pagerank_scores <- normalize_pagerank_by_opponent_diversity(
      pagerank_scores,
      unique_opponent_counts
    )
  } else if (!is.null(player_component) && !is.null(component_sizes)) {
    # Legacy: component normalization (less effective, kept for backwards compatibility)
    pagerank_scores <- normalize_pagerank_by_component(
      pagerank_scores,
      player_component,
      component_sizes
    )
  }

  # Calculate percentile rank on normalized scores
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
    player_id = names(raw_pagerank),
    pagerank = as.numeric(raw_pagerank),  # Store original raw PageRank
    percentile = percentiles,
    quality_tier = quality_tier,
    stringsAsFactors = FALSE
  )
}
