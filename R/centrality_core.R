# Player Network Centrality - Core Algorithms
#
# Core algorithms for network centrality calculation:
# - Matchup matrix construction
# - Connected component detection
# - Opsahl's weighted degree centrality
# - Tier classification
#
# Split from player_centrality.R for better maintainability.
#
# Reference: Opsahl, Agneessens & Skvoretz (2010) "Node centrality in weighted networks"

# ============================================================================
# MATCHUP GRAPH CONSTRUCTION
# ============================================================================

#' Build Matchup Matrices from Deliveries Data
#'
#' Constructs the adjacency and performance matrices needed for centrality
#' calculation from ball-by-ball delivery data. Uses sparse matrices for
#' memory efficiency - with 7000+ batters × 5000+ bowlers, dense matrices
#' would require ~1GB, but sparse matrices use ~95% less memory since most
#' batter-bowler pairs never faced each other.
#'
#' @param deliveries Data frame with columns: batter_id, bowler_id, runs_batter,
#'   is_wicket_delivery (or player_out_id), match_type.
#' @param format Character. Match format filter: "t20", "odi", "test", or "all".
#' @param min_deliveries Integer. Minimum deliveries for a player to be included.
#'   Default uses CENTRALITY_MIN_DELIVERIES constant.
#'
#' @return List with:
#'   - matchup_matrix: Sparse matrix of delivery counts (batter x bowler)
#'   - performance_matrix: Sparse matrix of normalized runs/ball (0-1 scale)
#'   - wicket_matrix: Sparse matrix of wicket proportions (0-1 scale)
#'   - batter_ids: Character vector of batter IDs (row names)
#'   - bowler_ids: Character vector of bowler IDs (col names)
#'   - batter_deliveries: Named vector of total deliveries faced
#'   - bowler_deliveries: Named vector of total deliveries bowled
#'
#' @import data.table
#' @importFrom Matrix sparseMatrix
#' @keywords internal
build_matchup_matrices <- function(deliveries,
                                    format = "all",
                                    min_deliveries = CENTRALITY_MIN_DELIVERIES) {

  # Convert to data.table for fast aggregation
  if (!is.data.table(deliveries)) {
    setDT(deliveries)
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
  nonzero_idx <- which(matchup_matrix > 0, arr.ind = TRUE)
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
#'   - arithmetic: (1-alpha)*degree + alpha*avg_opp_degree [RECOMMENDED]
#'   - geometric:  degree^(1-alpha) * avg_opp_degree^alpha [Opsahl's original]
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

  # Assign tiers based on percentiles (using data.table::fcase for performance)
  quality_tier <- data.table::fcase(
    percentiles >= 95, "Elite",
    percentiles >= 80, "Very Good",
    percentiles >= 60, "Above Average",
    percentiles >= 40, "Average",
    percentiles >= 20, "Below Average",
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
  if (max_unique_opps == 0) {
    return(pagerank_scores)  # No normalization possible
  }

  opp_diversity <- player_unique_opps / max_unique_opps

  # Apply penalty
  penalty <- opp_diversity ^ penalty_exponent
  normalized <- pagerank_scores * penalty
  names(normalized) <- player_ids

  normalized
}
