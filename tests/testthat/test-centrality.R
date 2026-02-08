# Test centrality and PageRank functions
#
# Tests the core computation functions in centrality.R:
# - Connected component detection (union-find)
# - Weighted degree centrality (Opsahl's method)
# - K-factor multipliers (sigmoid scaling)
# - League starting ELO
# - Centrality-based ELO regression

# Helper: create a minimal matchup matrix for testing
# Uses dgCMatrix (compressed sparse column) which is what build_matchup_matrices produces
make_test_matchup <- function() {
  # 4 batters x 3 bowlers with known connectivity
  # Batter1 faces Bowler1, Bowler2
  # Batter2 faces Bowler1, Bowler3
  # Batter3 faces Bowler2, Bowler3
  # Batter4 faces Bowler3 only (less connected)
  mat <- Matrix::sparseMatrix(
    i = c(1, 1, 2, 2, 3, 3, 4),
    j = c(1, 2, 1, 3, 2, 3, 3),
    x = c(50, 30, 40, 60, 20, 45, 10),
    dims = c(4, 3),
    dimnames = list(
      c("bat1", "bat2", "bat3", "bat4"),
      c("bowl1", "bowl2", "bowl3")
    )
  )
  as(mat, "dgCMatrix")
}

# Helper: create a disconnected matchup matrix (two components)
make_disconnected_matchup <- function() {
  # Component 1: bat1, bat2 face bowl1, bowl2
  # Component 2: bat3 faces bowl3 (isolated cluster)
  mat <- Matrix::sparseMatrix(
    i = c(1, 1, 2, 2, 3),
    j = c(1, 2, 1, 2, 3),
    x = c(50, 30, 40, 60, 25),
    dims = c(3, 3),
    dimnames = list(
      c("bat1", "bat2", "bat3"),
      c("bowl1", "bowl2", "bowl3")
    )
  )
  as(mat, "dgCMatrix")
}


# ============================================================================
# CONNECTED COMPONENT DETECTION
# ============================================================================

test_that("find_bipartite_components detects single component", {
  mat <- make_test_matchup()
  result <- find_bipartite_components(mat)

  expect_equal(result$n_components, 1)
  expect_named(result$batter_component)
  expect_named(result$bowler_component)
  expect_length(result$batter_component, 4)
  expect_length(result$bowler_component, 3)

  # All players should be in the same component
  all_comps <- unique(c(result$batter_component, result$bowler_component))
  expect_length(all_comps, 1)
})


test_that("find_bipartite_components detects disconnected components", {
  mat <- make_disconnected_matchup()
  result <- find_bipartite_components(mat)

  expect_equal(result$n_components, 2)

  # bat1 and bat2 should be in the same component
  expect_equal(
    unname(result$batter_component["bat1"]),
    unname(result$batter_component["bat2"])
  )

  # bat3 should be in a different component
  expect_false(
    unname(result$batter_component["bat3"]) == unname(result$batter_component["bat1"])
  )

  # bowl3 should be in the same component as bat3
  expect_equal(
    unname(result$bowler_component["bowl3"]),
    unname(result$batter_component["bat3"])
  )

  # Component sizes should reflect the split
  sizes <- result$component_sizes
  expect_true(max(sizes) == 4)  # bat1, bat2, bowl1, bowl2
  expect_true(min(sizes) == 2)  # bat3, bowl3
})


# ============================================================================
# UNIQUE OPPONENT COUNTS
# ============================================================================

test_that("calculate_unique_opponent_counts returns correct counts", {
  mat <- make_test_matchup()
  result <- calculate_unique_opponent_counts(mat)

  # bat1 faces bowl1, bowl2 → 2 opponents
  expect_equal(unname(result$batter_unique_opps["bat1"]), 2)

  # bat4 faces bowl3 only → 1 opponent
  expect_equal(unname(result$batter_unique_opps["bat4"]), 1)

  # bowl3 faces bat2, bat3, bat4 → 3 opponents
  expect_equal(unname(result$bowler_unique_opps["bowl3"]), 3)

  # bowl1 faces bat1, bat2 → 2 opponents
  expect_equal(unname(result$bowler_unique_opps["bowl1"]), 2)
})


# ============================================================================
# NETWORK CENTRALITY (Opsahl's Weighted Degree)
# ============================================================================

test_that("calculate_network_centrality returns expected structure", {
  mat <- make_test_matchup()
  result <- calculate_network_centrality(mat, alpha = 0.8, mean_type = "arithmetic")

  expect_named(result, c(
    "batter_centrality", "bowler_centrality",
    "batter_unique_opps", "bowler_unique_opps",
    "batter_avg_opp_degree", "bowler_avg_opp_degree"
  ))

  expect_length(result$batter_centrality, 4)
  expect_length(result$bowler_centrality, 3)

  # All centrality scores should be positive
  expect_true(all(result$batter_centrality >= 0))
  expect_true(all(result$bowler_centrality >= 0))
})


test_that("centrality reflects both breadth and opponent quality", {
  mat <- make_test_matchup()

  # With alpha=0 (pure breadth), bat4 (1 opp) < bat1 (2 opps)
  result_breadth <- calculate_network_centrality(mat, alpha = 0, mean_type = "arithmetic")
  expect_true(
    result_breadth$batter_centrality["bat4"] < result_breadth$batter_centrality["bat1"]
  )

  # With high alpha, opponent quality matters more
  # bat4 faces bowl3 (degree 3, most connected bowler), so gets quality boost
  result_quality <- calculate_network_centrality(mat, alpha = 0.8, mean_type = "arithmetic")
  expect_true(result_quality$batter_centrality["bat4"] > 0)  # positive score
})


test_that("alpha=0 gives pure degree centrality", {
  mat <- make_test_matchup()
  result <- calculate_network_centrality(mat, alpha = 0, mean_type = "arithmetic")

  # With alpha=0: centrality = (1-0)*degree + 0*avg_opp_degree = degree
  # bat1 faces 2 bowlers → centrality = 2
  expect_equal(unname(result$batter_centrality["bat1"]), 2)

  # bat4 faces 1 bowler → centrality = 1
  expect_equal(unname(result$batter_centrality["bat4"]), 1)
})


test_that("geometric mean type works", {
  mat <- make_test_matchup()
  result <- calculate_network_centrality(mat, alpha = 0.8, mean_type = "geometric")

  # Should still return valid positive scores
  expect_true(all(result$batter_centrality >= 0))
  expect_true(all(result$bowler_centrality >= 0))
})


# ============================================================================
# PAGERANK COMPONENT NORMALIZATION
# ============================================================================

test_that("normalize_pagerank_by_component penalizes small components", {
  pagerank_scores <- c(bat1 = 0.4, bat2 = 0.3, bat3 = 0.5)
  player_component <- c(bat1 = 1L, bat2 = 1L, bat3 = 2L)
  component_sizes <- c("1" = 100L, "2" = 10L)

  result <- normalize_pagerank_by_component(
    pagerank_scores, player_component, component_sizes, penalty_exponent = 0.5
  )

  # bat1 and bat2 are in the main component → no penalty (multiplier = 1.0)
  expect_equal(unname(result["bat1"]), 0.4)

  # bat3 is in a small component → penalized
  # penalty = (10/100)^0.5 = 0.316
  expect_lt(result["bat3"], pagerank_scores["bat3"])
  expect_equal(unname(result["bat3"]), 0.5 * sqrt(10 / 100), tolerance = 0.01)
})


# ============================================================================
# K-FACTOR MULTIPLIER (Sigmoid)
# ============================================================================

test_that("get_centrality_k_multiplier returns valid range", {
  # Check at various percentiles
  for (p in c(0, 10, 25, 50, 75, 90, 100)) {
    result <- get_centrality_k_multiplier(p)
    expect_gte(result, CENTRALITY_K_FLOOR)
    expect_lte(result, CENTRALITY_K_CEILING)
  }
})


test_that("get_centrality_k_multiplier is monotonically increasing", {
  percentiles <- seq(0, 100, by = 5)
  values <- sapply(percentiles, get_centrality_k_multiplier)

  # Each value should be >= the previous
  for (i in 2:length(values)) {
    expect_gte(values[i], values[i - 1])
  }
})


test_that("get_centrality_k_multiplier handles NA/NULL", {
  expect_equal(get_centrality_k_multiplier(NA), 1.0)
  expect_equal(get_centrality_k_multiplier(NULL), 1.0)
})


test_that("get_centrality_k_multiplier is ~1.0 at midpoint", {
  result <- get_centrality_k_multiplier(CENTRALITY_K_MIDPOINT)
  expect_equal(result, 1.0, tolerance = 0.01)
})


# ============================================================================
# LEAGUE STARTING ELO
# ============================================================================

test_that("calculate_league_starting_elo scales with centrality", {
  # Higher centrality → higher starting ELO
  low <- calculate_league_starting_elo(10)
  mid <- calculate_league_starting_elo(50)
  high <- calculate_league_starting_elo(90)

  expect_lt(low, mid)
  expect_lt(mid, high)
})


test_that("calculate_league_starting_elo returns base at 50th percentile", {
  result <- calculate_league_starting_elo(50, elo_start = 1400)
  expect_equal(result, 1400)
})


test_that("calculate_league_starting_elo handles NA/NULL", {
  expect_equal(calculate_league_starting_elo(NA, elo_start = 1400), 1400)
  expect_equal(calculate_league_starting_elo(NULL, elo_start = 1400), 1400)
})


test_that("calculate_league_starting_elo clamps extreme values", {
  # Centrality > 100 should be clamped to 100
  at_100 <- calculate_league_starting_elo(100, elo_start = 1400, elo_per_percentile = 6)
  above <- calculate_league_starting_elo(150, elo_start = 1400, elo_per_percentile = 6)
  expect_equal(at_100, above)

  # Centrality < 0 should be clamped to 0
  at_0 <- calculate_league_starting_elo(0, elo_start = 1400, elo_per_percentile = 6)
  below <- calculate_league_starting_elo(-50, elo_start = 1400, elo_per_percentile = 6)
  expect_equal(at_0, below)
})


# ============================================================================
# CENTRALITY-BASED ELO REGRESSION
# ============================================================================

test_that("calculate_centrality_regression pulls toward implied ELO", {
  # Player at 1800 with low centrality (percentile 10)
  # Implied ELO = 1400 + (10 - 50) * 6 = 1160
  # Correction should pull DOWN (negative)
  correction <- calculate_centrality_regression(
    1800, 10, elo_start = 1400, elo_per_percentile = 6, regression_strength = 0.005
  )
  expect_lt(correction, 0)

  # Player at 1200 with high centrality (percentile 90)
  # Implied ELO = 1400 + (90 - 50) * 6 = 1640
  # Correction should pull UP (positive)
  correction2 <- calculate_centrality_regression(
    1200, 90, elo_start = 1400, elo_per_percentile = 6, regression_strength = 0.005
  )
  expect_gt(correction2, 0)
})


test_that("calculate_centrality_regression returns 0 at implied ELO", {
  # Player at exactly their implied ELO → no correction
  # For 50th percentile: implied = 1400 + (50 - 50) * 6 = 1400
  correction <- calculate_centrality_regression(
    1400, 50, elo_start = 1400, elo_per_percentile = 6
  )
  expect_equal(correction, 0)
})


test_that("calculate_centrality_regression handles NA/NULL", {
  expect_equal(calculate_centrality_regression(1500, NA), 0)
  expect_equal(calculate_centrality_regression(1500, NULL), 0)
})
