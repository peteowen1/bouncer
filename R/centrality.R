# Player Network Centrality - Algorithms
#
# Core algorithms for network centrality and PageRank calculation:
# - Matchup matrix construction (sparse, memory-efficient)
# - Connected component detection (union-find)
# - Opsahl's weighted degree centrality
# - Bipartite PageRank (legacy)
# - ELO integration (K-factor multipliers, league starting ELO, regression)
# - Tier classification
#
# Consolidated from centrality_core.R, centrality_pagerank.R, centrality_integration.R
#
# Reference: Opsahl, Agneessens & Skvoretz (2010) 'Node centrality in weighted networks'

build_matchup_matrices <- function(deliveries,
                                    format = "all",
                                    min_deliveries = CENTRALITY_MIN_DELIVERIES) {

  # Convert to data.table for fast aggregation (copy to avoid mutating caller's data)
  if (!is.data.table(deliveries)) {
    deliveries <- data.table::as.data.table(deliveries)
  } else {
    deliveries <- data.table::copy(deliveries)
  }

  # Filter by format if specified
  if (format != "all") {
    format_lower <- tolower(format)
    deliveries <- deliveries[tolower(match_type) %in% c(format_lower, paste0("i", format_lower))]
  }

  # Detect wicket column and add is_wicket_val
  if ("is_wicket_delivery" %in% names(deliveries)) {
    deliveries[, is_wicket_val := as.numeric(is_wicket_delivery)]
  } else if ("player_out_id" %in% names(deliveries)) {
    deliveries[, is_wicket_val := as.numeric(!is.na(player_out_id) & player_out_id != "")]
  } else {
    deliveries[, is_wicket_val := 0]
  }

  # Fast aggregation with data.table - this is already sparse (only non-zero matchups)
  agg <- deliveries[, .(
    count = .N,
    runs = sum(runs_batter, na.rm = TRUE),
    wickets = sum(is_wicket_val, na.rm = TRUE)
  ), by = .(batter_id, bowler_id)]

  # Calculate total deliveries per player
  batter_totals <- agg[, .(total = sum(count)), by = batter_id]
  bowler_totals <- agg[, .(total = sum(count)), by = bowler_id]

  # Filter to players with minimum deliveries
  valid_batters <- batter_totals[total >= min_deliveries, batter_id]
  valid_bowlers <- bowler_totals[total >= min_deliveries, bowler_id]

  if (length(valid_batters) == 0 || length(valid_bowlers) == 0) {
    cli::cli_abort("No players meet minimum deliveries threshold of {min_deliveries}")
  }

  # Filter aggregated data
  agg <- agg[batter_id %in% valid_batters & bowler_id %in% valid_bowlers]

  # Create sorted ID vectors for consistent matrix ordering

  batter_ids <- sort(unique(agg$batter_id))
  bowler_ids <- sort(unique(agg$bowler_id))

  n_batters <- length(batter_ids)
  n_bowlers <- length(bowler_ids)

  # Map player IDs to matrix indices
  batter_idx <- match(agg$batter_id, batter_ids)
  bowler_idx <- match(agg$bowler_id, bowler_ids)

  # Build SPARSE matrices directly from aggregated data
  # This uses ~95% less memory than dense matrices for typical cricket data
  # (each batter faces ~100-200 bowlers, not all 5000+)
  matchup_matrix <- Matrix::sparseMatrix(
    i = batter_idx,
    j = bowler_idx,
    x = agg$count,
    dims = c(n_batters, n_bowlers),
    dimnames = list(batter_ids, bowler_ids)
  )

  runs_matrix <- Matrix::sparseMatrix(
    i = batter_idx,
    j = bowler_idx,
    x = agg$runs,
    dims = c(n_batters, n_bowlers),
    dimnames = list(batter_ids, bowler_ids)
  )

  wicket_matrix <- Matrix::sparseMatrix(
    i = batter_idx,
    j = bowler_idx,
    x = agg$wickets,
    dims = c(n_batters, n_bowlers),
    dimnames = list(batter_ids, bowler_ids)
  )

  # Calculate normalized performance (runs per ball, scaled 0-1)
  # For sparse matrices, we work with the non-zero entries directly
  # perf = runs / count, capped at 1.0 after dividing by 2.0
  perf_values <- pmin(agg$runs / pmax(agg$count, 1) / 2.0, 1)
  perf_matrix <- Matrix::sparseMatrix(
    i = batter_idx,
    j = bowler_idx,
    x = perf_values,
    dims = c(n_batters, n_bowlers),
    dimnames = list(batter_ids, bowler_ids)
  )

  # Calculate wicket proportion
  wicket_prop_values <- agg$wickets / pmax(agg$count, 1)
  wicket_prop_matrix <- Matrix::sparseMatrix(
    i = batter_idx,
    j = bowler_idx,
    x = wicket_prop_values,
    dims = c(n_batters, n_bowlers),
    dimnames = list(batter_ids, bowler_ids)
  )

  # Calculate delivery totals for included players
  batter_deliveries <- Matrix::rowSums(matchup_matrix)
  bowler_deliveries <- Matrix::colSums(matchup_matrix)
  names(batter_deliveries) <- batter_ids
  names(bowler_deliveries) <- bowler_ids

  # Calculate sparsity for logging
  n_nonzero <- length(agg$count)
  n_total <- n_batters * n_bowlers
  sparsity_pct <- round(100 * (1 - n_nonzero / n_total), 1)

  cli::cli_alert_success(
    "Built matchup matrices: {n_batters} batters x {n_bowlers} bowlers, {sum(agg$count)} deliveries ({sparsity_pct}% sparse)"
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
# CONNECTED COMPONENT DETECTION
# ============================================================================

#' Find Connected Components in Bipartite Graph
#'
#' Detects connected components in the batter-bowler matchup graph using
#' a union-find algorithm. Players in isolated clusters (small components)
#' will have their PageRank penalized proportionally to component size.
#'
#' @param matchup_matrix Matrix of delivery counts (batter x bowler).
#'
#' @return List with:
#'   - batter_component: Named vector mapping batter_id to component ID
#'   - bowler_component: Named vector mapping bowler_id to component ID
#'   - component_sizes: Named vector of component sizes (total players)
#'   - n_components: Number of connected components found
#'
#' @keywords internal
find_bipartite_components <- function(matchup_matrix) {
  batter_ids <- rownames(matchup_matrix)
  bowler_ids <- colnames(matchup_matrix)

  n_batters <- length(batter_ids)
  n_bowlers <- length(bowler_ids)

  # Create unified player index: batters 1:n_batters, bowlers (n_batters+1):(n_batters+n_bowlers)
  # Union-find parent array
  parent <- seq_len(n_batters + n_bowlers)

  # Find with path compression
  find_root <- function(i) {
    if (parent[i] != i) {
      parent[i] <<- find_root(parent[i])
    }
    parent[i]
  }

  # Union operation
  union_nodes <- function(i, j) {
    ri <- find_root(i)
    rj <- find_root(j)
    if (ri != rj) {
      parent[ri] <<- rj
    }
  }

  # Union batters and bowlers that have faced each other
  # OPTIMIZED: Only iterate over non-zero entries (sparse iteration)
  # Use Matrix::which() for sparse matrix compatibility (base::which fails on lgCMatrix)
  nonzero_idx <- Matrix::which(matchup_matrix > 0, arr.ind = TRUE)
  for (k in seq_len(nrow(nonzero_idx))) {
    union_nodes(nonzero_idx[k, 1], n_batters + nonzero_idx[k, 2])
  }

  # Get final component assignments
  batter_component <- sapply(seq_len(n_batters), find_root)
  bowler_component <- sapply(seq_len(n_bowlers), function(j) find_root(n_batters + j))

  names(batter_component) <- batter_ids
  names(bowler_component) <- bowler_ids

  # Map to sequential component IDs
  unique_roots <- unique(c(batter_component, bowler_component))
  root_to_id <- setNames(seq_along(unique_roots), unique_roots)

  batter_component <- root_to_id[as.character(batter_component)]
  bowler_component <- root_to_id[as.character(bowler_component)]

  names(batter_component) <- batter_ids
  names(bowler_component) <- bowler_ids

  # Calculate component sizes
  all_components <- c(batter_component, bowler_component)
  component_sizes <- table(all_components)
  component_sizes <- setNames(as.integer(component_sizes), names(component_sizes))

  list(
    batter_component = batter_component,
    bowler_component = bowler_component,
    component_sizes = component_sizes,
    n_components = length(unique_roots)
  )
}


#' Normalize PageRank by Component Size
#'
#' Applies a penalty to PageRank scores based on connected component size.
#' Players in small isolated clusters get their scores reduced proportionally,
#' fixing the issue where isolated clusters have artificially inflated PageRank.
#'
#' @param pagerank_scores Named numeric vector of PageRank scores.
#' @param player_component Named vector mapping player_id to component ID.
#' @param component_sizes Named vector of component sizes.
#' @param penalty_exponent Numeric. Exponent for size penalty (default 0.5).
#'   Higher values penalize small components more aggressively.
#'
#' @return Named numeric vector of normalized PageRank scores.
#'
#' @details
#' The normalization formula is:
#'   normalized_pr = raw_pr * (component_size / max_component_size) ^ exponent
#'
#' With exponent = 0.5:
#'   - Main component (3000 players): multiplier = 1.0
#'   - Medium cluster (300 players): multiplier = 0.32
#'   - Small cluster (30 players): multiplier = 0.10
#'
#' @keywords internal
normalize_pagerank_by_component <- function(pagerank_scores,
                                             player_component,
                                             component_sizes,
                                             penalty_exponent = 0.5) {
  max_size <- max(component_sizes)

  # Get component size for each player
  player_ids <- names(pagerank_scores)
  player_comp_ids <- player_component[player_ids]
  player_sizes <- component_sizes[as.character(player_comp_ids)]


  # Calculate penalty multiplier
  penalty <- (player_sizes / max_size) ^ penalty_exponent

  # Apply penalty
  normalized <- pagerank_scores * penalty
  names(normalized) <- player_ids

  normalized
}


#' Calculate Unique Opponent Counts
#'
#' Computes the number of unique opponents faced by each player from the matchup matrix.
#' This captures network integration: players in isolated clusters face few unique opponents.
#'
#' @param matchup_matrix Matrix of delivery counts (batter x bowler).
#'
#' @return List with:
#'   - batter_unique_opps: Named vector of unique bowler count for each batter
#'   - bowler_unique_opps: Named vector of unique batter count for each bowler
#'
#' @keywords internal
calculate_unique_opponent_counts <- function(matchup_matrix) {
  batter_ids <- rownames(matchup_matrix)
  bowler_ids <- colnames(matchup_matrix)

  # For each batter: count unique bowlers faced
  batter_unique_opps <- rowSums(matchup_matrix > 0)
  names(batter_unique_opps) <- batter_ids

  # For each bowler: count unique batters faced
  bowler_unique_opps <- colSums(matchup_matrix > 0)
  names(bowler_unique_opps) <- bowler_ids

  list(
    batter_unique_opps = batter_unique_opps,
    bowler_unique_opps = bowler_unique_opps
  )
}


# ============================================================================
# NETWORK CENTRALITY ALGORITHM (Opsahl's Weighted Degree)
# ============================================================================

#' Calculate Network Centrality from Matchup Matrix
#'
#' Computes weighted degree centrality for batters and bowlers using either
#' arithmetic or geometric mean. This replaces the iterative PageRank algorithm
#' with a simpler, theoretically grounded measure that better separates elite
#' players from isolated clusters.
#'
#' Two mean types are supported:
#'   - arithmetic: (1-alpha)*degree + alpha*avg_opp_degree (RECOMMENDED)
#'   - geometric:  degree^(1-alpha) * avg_opp_degree^alpha (Opsahl's original)
#'
#' Arithmetic mean is recommended because it gives immediate boost to players
#' in elite leagues even with few appearances. A new IPL player facing 4
#' bowlers with avg 300-degree gets ~241 centrality vs ~35 with geometric.
#'
#' @param matchup_matrix Matrix of delivery counts (batter x bowler).
#' @param alpha Numeric. Weight for opponent quality vs own breadth.
#'   - alpha = 0: pure degree centrality (just count unique opponents)
#'   - alpha = 0.8: quality-weighted (recommended for arithmetic)
#'   - alpha = 1: pure neighbor degree (only opponent quality matters)
#' @param mean_type Character. "arithmetic" (recommended) or "geometric".
#'
#' @return List with:
#'   - batter_centrality: Named vector of centrality scores for batters
#'   - bowler_centrality: Named vector of centrality scores for bowlers
#'   - batter_unique_opps: Named vector of unique bowler counts
#'   - bowler_unique_opps: Named vector of unique batter counts
#'   - batter_avg_opp_degree: Named vector of average bowler degree for each batter
#'   - bowler_avg_opp_degree: Named vector of average batter degree for each bowler
#'
#' @details
#' Why this works better than PageRank:
#'
#' 1. **Theoretical grounding**: Based on Opsahl et al. (2010)'s work on node
#'    centrality in weighted networks. The geometric mean balances breadth
#'    (how many opponents) with quality (how connected those opponents are).
#'
#' 2. **Separation**: PageRank converges to similar values (~5% variation) in
#'    cricket's highly connected network. Centrality produces 90+ percentile
#'    point gaps between elite and isolated players.
#'
#' 3. **Computational efficiency**: O(n) instead of O(iterations x n^2).
#'
#' 4. **Per-delivery fairness**: A player facing 100 opponents across IPL, BBL,
#'    and T20 World Cup scores high because those opponents have high degrees.
#'    A player facing 100 opponents all from the same isolated league scores
#'    low because those opponents have low degrees.
#'
#' @examples
#' \dontrun{
#' # Build matchup matrix
#' matrices <- build_matchup_matrices(deliveries, format = "t20", min_deliveries = 100)
#'
#' # Calculate centrality
#' centrality <- calculate_network_centrality(matrices$matchup_matrix)
#'
#' # View top batters by centrality
#' head(sort(centrality$batter_centrality, decreasing = TRUE), 10)
#' }
#'
#' @keywords internal
calculate_network_centrality <- function(matchup_matrix,
                                         alpha = CENTRALITY_ALPHA,
                                         mean_type = CENTRALITY_MEAN_TYPE) {
  batter_ids <- rownames(matchup_matrix)
  bowler_ids <- colnames(matchup_matrix)

  n_batters <- length(batter_ids)
  n_bowlers <- length(bowler_ids)

  # Step 1: Calculate degree (unique opponents) for each player
  # Create binary adjacency matrix once - reused for degree and multiplication
  # Convert logical sparse to numeric sparse explicitly to avoid dense conversion
  adj_matrix <- as(matchup_matrix != 0, "dgCMatrix")

  # Batter degree = number of unique bowlers faced (row sums of adjacency)
  batter_degree <- Matrix::rowSums(adj_matrix)
  names(batter_degree) <- batter_ids

  # Bowler degree = number of unique batters faced (col sums of adjacency)
  bowler_degree <- Matrix::colSums(adj_matrix)
  names(bowler_degree) <- bowler_ids

  # Step 2: Calculate average neighbor degree for each player
  # For each batter: average of (degree of each bowler they faced)
  # This captures "opponent quality" - facing well-connected bowlers is better

  # OPTIMIZED: Use sparse matrix-vector multiplication
  # adj_matrix %*% bowler_degree gives total degree of all bowlers faced by each batter
  batter_opp_degree_sum <- as.vector(adj_matrix %*% bowler_degree)
  batter_avg_opp_degree <- ifelse(batter_degree > 0,
                                   batter_opp_degree_sum / batter_degree, 0)
  names(batter_avg_opp_degree) <- batter_ids

  # For bowlers: use crossprod to avoid explicit transpose (memory efficient)
  # crossprod(adj_matrix, batter_degree) = t(adj_matrix) %*% batter_degree
  bowler_opp_degree_sum <- as.vector(Matrix::crossprod(adj_matrix, batter_degree))
  bowler_avg_opp_degree <- ifelse(bowler_degree > 0,
                                   bowler_opp_degree_sum / bowler_degree, 0)
  names(bowler_avg_opp_degree) <- bowler_ids

  # Clean up large intermediate object
  rm(adj_matrix)

  # Step 3: Calculate weighted degree centrality
  # Two options:
  #   arithmetic: (1-alpha)*degree + alpha*avg_opp_degree [RECOMMENDED for new players]
  #   geometric:  degree^(1-alpha) * avg_opp_degree^alpha [Opsahl's original]

  if (mean_type == "arithmetic") {
    # Arithmetic mean: gives immediate boost to players in elite leagues
    # New IPL player with 4 opps facing avg 300-degree bowlers:
    #   (1-0.8)*4 + 0.8*300 = 0.8 + 240 = 240.8
    batter_centrality <- (1 - alpha) * batter_degree + alpha * batter_avg_opp_degree
    bowler_centrality <- (1 - alpha) * bowler_degree + alpha * bowler_avg_opp_degree
  } else {
    # Geometric mean (Opsahl's original): penalizes low breadth heavily
    # New IPL player with 4 opps facing avg 300-degree bowlers:
    #   4^0.2 * 300^0.8 = 1.32 * 95 = 125
    batter_centrality <- (batter_degree ^ (1 - alpha)) * (batter_avg_opp_degree ^ alpha)
    bowler_centrality <- (bowler_degree ^ (1 - alpha)) * (bowler_avg_opp_degree ^ alpha)
  }

  names(batter_centrality) <- batter_ids
  names(bowler_centrality) <- bowler_ids

  # Handle any NaN/Inf from zero degrees
  batter_centrality[is.na(batter_centrality) | is.infinite(batter_centrality)] <- 0
  bowler_centrality[is.na(bowler_centrality) | is.infinite(bowler_centrality)] <- 0

  cli::cli_alert_success(
    "Calculated network centrality ({mean_type}, alpha={alpha}): {n_batters} batters, {n_bowlers} bowlers"
  )

  list(
    batter_centrality = batter_centrality,
    bowler_centrality = bowler_centrality,
    batter_unique_opps = batter_degree,
    bowler_unique_opps = bowler_degree,
    batter_avg_opp_degree = batter_avg_opp_degree,
    bowler_avg_opp_degree = bowler_avg_opp_degree
  )
}


#' Classify Players into Quality Tiers by Centrality
#'
#' Assigns quality tier labels based on network centrality percentiles.
#' This is the centrality-based version of classify_pagerank_tiers().
#'
#' @param centrality_scores Named numeric vector of centrality scores.
#' @param n_players Total number of players (for percentile calculation).
#'
#' @return Data frame with columns: player_id, centrality, percentile, quality_tier
#'
#' @keywords internal
classify_centrality_tiers <- function(centrality_scores, n_players = NULL) {
  if (is.null(n_players)) {
    n_players <- length(centrality_scores)
  }

  # Calculate percentile rank
  ranks <- rank(centrality_scores, ties.method = "average")
  percentiles <- ranks / n_players * 100

  # Assign tiers based on percentiles (thresholds from constants_skill.R)
  quality_tier <- data.table::fcase(
    percentiles >= QUALITY_TIER_ELITE, "Elite",
    percentiles >= QUALITY_TIER_VERY_GOOD, "Very Good",
    percentiles >= QUALITY_TIER_ABOVE_AVERAGE, "Above Average",
    percentiles >= QUALITY_TIER_AVERAGE, "Average",
    percentiles >= QUALITY_TIER_BELOW_AVERAGE, "Below Average",
    default = "Weak"
  )

  data.frame(
    player_id = names(centrality_scores),
    centrality = as.numeric(centrality_scores),
    percentile = percentiles,
    quality_tier = quality_tier,
    stringsAsFactors = FALSE
  )
}


#' Normalize PageRank by Opponent Diversity
#'
#' Applies a penalty to PageRank scores based on the number of unique opponents faced.
#' Players in isolated clusters face few unique opponents and get penalized,
#' fixing the issue where isolated subgroups have inflated PageRank rankings.
#'
#' @param pagerank_scores Named numeric vector of PageRank scores.
#' @param unique_opponent_counts Named vector of unique opponent counts for each player.
#' @param penalty_exponent Numeric. Exponent for diversity penalty (default 0.3).
#'   Higher values penalize low-diversity players more aggressively.
#'
#' @return Named numeric vector of normalized PageRank scores.
#'
#' @details
#' The normalization formula is:
#'   normalized_pr = raw_pr * (unique_opps / max_unique_opps) ^ exponent
#'
#' With exponent = 0.3:
#'   - Kohli (429 unique bowlers, max ~600): penalty ~ 0.93
#'   - Taranjit (23 unique bowlers): penalty ~ 0.45
#'
#' This effectively captures "network integration" - players in isolated clusters
#' can't accumulate high unique opponent counts by definition.
#'
#' @keywords internal
normalize_pagerank_by_opponent_diversity <- function(pagerank_scores,
                                                      unique_opponent_counts,
                                                      penalty_exponent = 0.3) {
  player_ids <- names(pagerank_scores)

  # Get unique opponent count for each player
  player_unique_opps <- unique_opponent_counts[player_ids]
  player_unique_opps[is.na(player_unique_opps)] <- 1  # Minimum 1 to avoid divide by zero

  # Normalize to 0-1 scale

  max_unique_opps <- max(player_unique_opps, na.rm = TRUE)
  if (max_unique_opps <= 0 || is.infinite(max_unique_opps)) {
    return(pagerank_scores)  # No normalization possible
  }

  opp_diversity <- player_unique_opps / max_unique_opps

  # Apply penalty
  penalty <- opp_diversity ^ penalty_exponent
  normalized <- pagerank_scores * penalty
  names(normalized) <- player_ids

  normalized
}


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
    # Avoid creating dense matrix from 0.5 + sparse: distribute multiplication
    performance_weighted <- (matchup_matrix * 0.5 + matchup_matrix * performance_matrix) %*% bowler_pr

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

  # Assign tiers based on percentiles (thresholds from constants_skill.R)
  quality_tier <- dplyr::case_when(
    percentiles >= QUALITY_TIER_ELITE ~ "Elite",
    percentiles >= QUALITY_TIER_VERY_GOOD ~ "Very Good",
    percentiles >= QUALITY_TIER_ABOVE_AVERAGE ~ "Above Average",
    percentiles >= QUALITY_TIER_AVERAGE ~ "Average",
    percentiles >= QUALITY_TIER_BELOW_AVERAGE ~ "Below Average",
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


get_cold_start_percentile <- function(event_tier) {
  switch(as.character(event_tier),
    "1" = CENTRALITY_COLD_START_TIER_1,  # 60 - IPL/BBL debut
    "2" = CENTRALITY_COLD_START_TIER_2,  # 50 - Strong domestic
    "3" = CENTRALITY_COLD_START_TIER_3,  # 40 - Regional
    "4" = CENTRALITY_COLD_START_TIER_4,  # 30 - Development
    50  # Default fallback
  )
}


# ============================================================================
# K-FACTOR MULTIPLIERS
# ============================================================================

#' Get Centrality K-Factor Multiplier
#'
#' Calculates the K-factor multiplier based on opponent's centrality percentile.
#' Uses a sigmoid function to smoothly transition between floor and ceiling.
#'
#' This implements Option A (Preventive): learn more from elite opponents,
#' less from weak ones.
#'
#' @param opponent_percentile Numeric. Opponent's centrality percentile (0-100).
#'   Use get_centrality_as_of() or get_cold_start_percentile() to obtain.
#'
#' @return Numeric. K-factor multiplier (CENTRALITY_K_FLOOR to CENTRALITY_K_CEILING).
#' @export
#'
#' @examples
#' get_centrality_k_multiplier(99)  # ~1.5 for elite opponent
#' get_centrality_k_multiplier(50)  # ~1.0 for average opponent
#' get_centrality_k_multiplier(10)  # ~0.5 for weak opponent
get_centrality_k_multiplier <- function(opponent_percentile) {
  # Handle NA/NULL (scalar function)
  if (is.null(opponent_percentile) || length(opponent_percentile) == 0 ||
      is.na(opponent_percentile)) {
    return(1.0)  # Neutral multiplier
  }

  # Sigmoid scaling between floor and ceiling
  # Formula: floor + (ceiling - floor) / (1 + exp(-steepness * (percentile - midpoint)))
  range_size <- CENTRALITY_K_CEILING - CENTRALITY_K_FLOOR
  sigmoid_arg <- -CENTRALITY_K_STEEPNESS * (opponent_percentile - CENTRALITY_K_MIDPOINT)

  CENTRALITY_K_FLOOR + range_size / (1 + exp(sigmoid_arg))
}


# ============================================================================
# LEAGUE-BASED STARTING ELO
# ============================================================================

#' Calculate League-Based Starting ELO
#'
#' Calculates a player's starting ELO based on the average centrality of players
#' in the league/event where they debut. Players debuting in isolated leagues
#' (low centrality) start with lower ELO, preventing inflation.
#'
#' Formula: starting_elo = ELO_START + (league_avg_centrality - 50) * elo_per_percentile
#'
#' @param league_avg_centrality Numeric. Average centrality percentile of players
#'   in the debut league (0-100). If NULL or NA, uses default starting ELO.
#' @param elo_start Numeric. Base starting ELO. Default uses THREE_WAY_ELO_START (1400).
#' @param elo_per_percentile Numeric. ELO points per percentile point.
#'   Default uses CENTRALITY_ELO_PER_PERCENTILE (4).
#'
#' @return Numeric. The league-adjusted starting ELO.
#'
#' @examples
#' \dontrun{
#' # IPL debut (high centrality league ~78%)
#' calculate_league_starting_elo(78)  # ~1512
#'
#' # Central American league debut (low centrality ~4%)
#' calculate_league_starting_elo(4)   # ~1216
#'
#' # International debut (very high centrality ~95%)
#' calculate_league_starting_elo(95)  # ~1580
#' }
#'
#' @keywords internal
calculate_league_starting_elo <- function(league_avg_centrality,
                                           elo_start = THREE_WAY_ELO_START,
                                           elo_per_percentile = CENTRALITY_ELO_PER_PERCENTILE) {
  # Handle NULL/NA - use default starting ELO
  if (is.null(league_avg_centrality) || is.na(league_avg_centrality)) {
    return(elo_start)
  }

  # Clamp to valid range
  league_avg_centrality <- max(0, min(100, league_avg_centrality))

  # Calculate adjusted starting ELO
  # 50th percentile league → start at elo_start (neutral)
  # Higher centrality → start higher
  # Lower centrality → start lower
  elo_start + (league_avg_centrality - 50) * elo_per_percentile
}


# ============================================================================
# CENTRALITY-BASED ELO REGRESSION
# ============================================================================

#' Calculate Centrality-Based ELO Regression
#'
#' Applies a continuous "gravity" pull toward the player's centrality-implied ELO.
#' This creates a stronger Bayesian prior that prevents low-centrality players from
#' accumulating inflated ratings by dominating weak opponents in isolated ecosystems.
#'
#' The regression is proportional to the gap between current ELO and implied ELO:
#'   correction = regression_strength × (implied_elo - current_elo)
#'
#' This means:
#' - Players far above their implied ELO are pulled down more strongly
#' - Players near their implied ELO experience minimal correction
#' - Elite players (high centrality) have higher implied ELOs, so their high ratings
#'   are "justified" by their network position
#'
#' @param current_elo Numeric. The player's current ELO rating after normal update.
#' @param player_centrality_percentile Numeric. The player's centrality percentile (0-100).
#'   If NULL or NA, no regression is applied.
#' @param elo_start Numeric. Base ELO (default THREE_WAY_ELO_START = 1400).
#' @param elo_per_percentile Numeric. ELO points per percentile point (default 4).
#' @param regression_strength Numeric. Pull strength per delivery (default 0.002).
#'
#' @return Numeric. The ELO correction to ADD to current_elo (can be negative).
#'
#' @examples
#' # Low centrality player with inflated ELO
#' calculate_centrality_regression(2400, 5)   # ~ -2.36 (strong pull down)
#'
#' # Elite player with high ELO
#' calculate_centrality_regression(2000, 95)  # ~ -0.72 (small pull down, justified)
#'
#' # Average player at average ELO
#' calculate_centrality_regression(1400, 50)  # 0 (no correction needed)
#'
#' @export
calculate_centrality_regression <- function(current_elo,
                                             player_centrality_percentile,
                                             elo_start = THREE_WAY_ELO_START,
                                             elo_per_percentile = CENTRALITY_ELO_PER_PERCENTILE,
                                             regression_strength = CENTRALITY_REGRESSION_STRENGTH) {
  # No correction if centrality unknown
  if (is.null(player_centrality_percentile) || is.na(player_centrality_percentile)) {
    return(0)
  }

  # Clamp to valid range
  player_centrality_percentile <- max(0, min(100, player_centrality_percentile))

  # Calculate centrality-implied ELO
  # Same formula as league_starting_elo: higher centrality = higher implied ELO
  implied_elo <- elo_start + (player_centrality_percentile - 50) * elo_per_percentile

  # Calculate correction (positive if current < implied, negative if current > implied)
  correction <- regression_strength * (implied_elo - current_elo)

  correction
}


# ============================================================================
# EVENT/LEAGUE CENTRALITY LOOKUP
# ============================================================================

#' Build Event Centrality Lookup Table
#'
#' Pre-computes average centrality for each event/league based on the players
#' who have participated in it. This is used to determine league-based
#' starting ELOs for new players.
#'
#' @param conn DBI connection to the database.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param gender Character. Gender: "mens" or "womens".
#' @param min_players Integer. Minimum players in an event to include it.
#'   Default 10.
#'
#' @return Named list (event_name -> avg_centrality) for quick lookup.
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection(read_only = TRUE)
#' event_centrality <- build_event_centrality_lookup(conn, "t20", "mens")
#' event_centrality[["Indian Premier League"]]  # ~78
#' }
#'
#' @keywords internal
build_event_centrality_lookup <- function(conn, format, gender, min_players = 10) {
  format <- tolower(format)
  gender <- tolower(gender)

  # Build centrality table name
  centrality_table <- paste0(gender, "_", format, "_player_centrality_history")

  # Check if centrality table exists
  if (!centrality_table %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Centrality table {centrality_table} not found, using default starting ELOs")
    return(list())
  }

  # Determine match types for this format
  match_types <- get_match_types_for_format(format)
  match_types_sql <- paste0("'", escape_sql_strings(match_types), "'", collapse = ", ")

  # Get average centrality by event
  query <- sprintf("
    WITH event_players AS (
      SELECT DISTINCT m.event_name, d.batter_id as player_id
      FROM deliveries d
      JOIN matches m ON d.match_id = m.match_id
      WHERE m.match_type IN (%s)
        AND m.gender = '%s'
        AND m.event_name IS NOT NULL
      UNION
      SELECT DISTINCT m.event_name, d.bowler_id as player_id
      FROM deliveries d
      JOIN matches m ON d.match_id = m.match_id
      WHERE m.match_type IN (%s)
        AND m.gender = '%s'
        AND m.event_name IS NOT NULL
    ),
    latest_centrality AS (
      SELECT player_id, AVG(percentile) as percentile
      FROM %s
      WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM %s)
      GROUP BY player_id
    )
    SELECT
      ep.event_name,
      AVG(lc.percentile) as avg_centrality
    FROM event_players ep
    LEFT JOIN latest_centrality lc ON ep.player_id = lc.player_id
    GROUP BY ep.event_name
    HAVING COUNT(*) >= %d AND AVG(lc.percentile) IS NOT NULL
  ", match_types_sql, ifelse(gender == "mens", "male", "female"),
     match_types_sql, ifelse(gender == "mens", "male", "female"),
     centrality_table, centrality_table, min_players)

  result <- tryCatch(
    DBI::dbGetQuery(conn, query),
    error = function(e) {
      cli::cli_alert_warning("Failed to build event centrality lookup: {e$message}")
      return(data.frame(event_name = character(0), avg_centrality = numeric(0)))
    }
  )

  if (nrow(result) == 0) {
    return(list())
  }

  # Convert to named list for fast lookup
  event_lookup <- as.list(result$avg_centrality)
  names(event_lookup) <- result$event_name

  cli::cli_alert_success("Built centrality lookup for {length(event_lookup)} events")
  event_lookup
}


# ============================================================================
# PLAYER DEBUT EVENT TRACKING
# ============================================================================

#' Get Player Debut Event
#'
#' Finds the first event/league where a player appeared, used to determine
#' their league-based starting ELO.
#'
#' @param player_id Character. The player's ID.
#' @param deliveries_dt Data.table. The deliveries data with event_name column.
#' @param role Character. "batter" or "bowler" to determine which column to check.
#'
#' @return Character. The event name, or NA if not found.
#' @keywords internal
get_player_debut_event <- function(player_id, deliveries_dt, role = "batter") {
  if (role == "batter") {
    idx <- which(deliveries_dt$batter_id == player_id)[1]
  } else {
    idx <- which(deliveries_dt$bowler_id == player_id)[1]
  }

  if (is.na(idx)) return(NA_character_)

  deliveries_dt$event_name[idx]
}


#' Batch Get Player Debut Events
#'
#' Efficiently finds the debut event for multiple players at once.
#' Much faster than calling get_player_debut_event in a loop.
#'
#' @param player_ids Character vector. Player IDs to look up.
#' @param deliveries_dt Data.table. The deliveries data (must have event_name).
#' @param role Character. "batter" or "bowler".
#'
#' @return Named character vector (player_id -> event_name).
#' @keywords internal
batch_get_player_debut_events <- function(player_ids, deliveries_dt, role = "batter") {
  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("data.table package required for batch_get_player_debut_events")
  }

  # Ensure data.table
  if (!data.table::is.data.table(deliveries_dt)) {
    deliveries_dt <- data.table::as.data.table(deliveries_dt)
  }

  if (!"event_name" %in% names(deliveries_dt)) {
    cli::cli_abort("{.field event_name} column required in {.arg deliveries_dt}")
  }

  id_col <- if (role == "batter") "batter_id" else "bowler_id"

  # Get first occurrence of each player (data is chronologically sorted)
  first_appearances <- deliveries_dt[, .SD[1], by = id_col, .SDcols = "event_name"]

  # Create lookup
  result <- first_appearances$event_name
  names(result) <- first_appearances[[id_col]]

  # Return in requested order, with NA for missing
  result[player_ids]
}

