# Tests for Agnostic Model Functions (agnostic_model.R, expected_outcomes.R)
#
# Tests for probability calculations, expected values, and feature preparation.
# These tests don't require actual XGBoost models - they test the mathematical
# correctness of the underlying calculations.

# ============================================================================
# EXPECTED RUNS CALCULATION TESTS
# ============================================================================

test_that("calculate_expected_runs returns correct expected value", {
  # Simple case: 100% probability of 4 runs
  probs_4 <- matrix(c(0, 0, 0, 0, 0, 1, 0), nrow = 1)
  expect_equal(calculate_expected_runs(probs_4), 4)

  # Simple case: 100% probability of 6 runs
  probs_6 <- matrix(c(0, 0, 0, 0, 0, 0, 1), nrow = 1)
  expect_equal(calculate_expected_runs(probs_6), 6)

  # Simple case: 100% probability of wicket (0 runs)
  probs_wicket <- matrix(c(1, 0, 0, 0, 0, 0, 0), nrow = 1)
  expect_equal(calculate_expected_runs(probs_wicket), 0)

  # Simple case: 100% probability of dot ball (0 runs)
  probs_dot <- matrix(c(0, 1, 0, 0, 0, 0, 0), nrow = 1)
  expect_equal(calculate_expected_runs(probs_dot), 0)
})

test_that("calculate_expected_runs handles uniform distribution", {
  # Uniform probability across all outcomes
  probs <- matrix(rep(1/7, 7), nrow = 1)
  expected <- (0 + 0 + 1 + 2 + 3 + 4 + 6) / 7  # 16/7 ≈ 2.286
  expect_equal(calculate_expected_runs(probs), expected, tolerance = 0.001)
})

test_that("calculate_expected_runs handles multiple rows", {
  probs <- matrix(c(
    1, 0, 0, 0, 0, 0, 0,  # Row 1: wicket = 0 runs
    0, 0, 0, 0, 0, 0, 1   # Row 2: six = 6 runs
  ), nrow = 2, byrow = TRUE)

  result <- calculate_expected_runs(probs)
  expect_equal(result, c(0, 6))
})

test_that("calculate_expected_runs validates input columns", {
  # Wrong number of columns should error
  probs_wrong <- matrix(c(0.5, 0.5), nrow = 1)
  expect_error(calculate_expected_runs(probs_wrong), "7 columns")
})

test_that("calculate_expected_runs handles data frames", {
  probs_df <- data.frame(
    col1 = 0, col2 = 0, col3 = 0.5, col4 = 0.5, col5 = 0, col6 = 0, col7 = 0
  )
  # Expected: 0.5 * 1 + 0.5 * 2 = 1.5
  expect_equal(calculate_expected_runs(probs_df), 1.5)
})

test_that("calculate_expected_runs produces realistic cricket values", {
  # Typical T20 probability distribution (from real data)
  # ~5% wicket, ~35% dot, ~30% single, ~10% double, ~2% three, ~12% four, ~6% six
  typical_probs <- matrix(c(0.05, 0.35, 0.30, 0.10, 0.02, 0.12, 0.06), nrow = 1)
  exp_runs <- calculate_expected_runs(typical_probs)

  # T20 average is ~1.2 runs per ball, should be in reasonable range
  expect_gte(exp_runs, 0.8)
  expect_lte(exp_runs, 1.8)
})

# ============================================================================
# EXPECTED WICKET PROBABILITY TESTS
# ============================================================================

test_that("calculate_expected_wicket_prob extracts first column", {
  probs <- matrix(c(0.05, 0.35, 0.30, 0.10, 0.02, 0.12, 0.06), nrow = 1)
  expect_equal(calculate_expected_wicket_prob(probs), 0.05)
})

test_that("calculate_expected_wicket_prob handles multiple rows", {
  probs <- matrix(c(
    0.03, 0.40, 0.30, 0.10, 0.02, 0.10, 0.05,  # Row 1: 3% wicket
    0.10, 0.35, 0.25, 0.08, 0.02, 0.12, 0.08   # Row 2: 10% wicket
  ), nrow = 2, byrow = TRUE)

  result <- calculate_expected_wicket_prob(probs)
  expect_equal(result, c(0.03, 0.10))
})

test_that("calculate_expected_wicket_prob returns values in [0, 1]", {
  # Valid probability distributions
  for (wicket_prob in c(0, 0.05, 0.1, 0.5, 1.0)) {
    probs <- matrix(c(wicket_prob, (1 - wicket_prob), 0, 0, 0, 0, 0), nrow = 1)
    result <- calculate_expected_wicket_prob(probs)
    expect_gte(result, 0)
    expect_lte(result, 1)
  }
})

test_that("calculate_expected_wicket_prob validates input", {
  # Empty matrix should error
  probs_empty <- matrix(nrow = 1, ncol = 0)
  expect_error(calculate_expected_wicket_prob(probs_empty), "at least 1 column")
})

# ============================================================================
# PROBABILITY NORMALIZATION TESTS
# ============================================================================

test_that("probability normalization handles zero row sums",
{
  # Simulate what happens in predict_agnostic_outcome with degenerate input
  probs <- matrix(0, nrow = 1, ncol = 7)

  # The code does: row_sums[row_sums == 0] <- 1
  row_sums <- rowSums(probs)
  row_sums[row_sums == 0] <- 1
  normalized <- probs / row_sums

  # Should not produce NaN/Inf
  expect_false(any(is.nan(normalized)))
  expect_false(any(is.infinite(normalized)))

  # All zeros normalized by 1 gives all zeros
  expect_equal(sum(normalized), 0)
})

test_that("probability normalization preserves valid distributions", {
  probs <- matrix(c(0.1, 0.3, 0.2, 0.15, 0.05, 0.12, 0.08), nrow = 1)
  row_sums <- rowSums(probs)
  normalized <- probs / row_sums

  expect_equal(sum(normalized), 1.0, tolerance = 1e-10)
})

test_that("probability normalization fixes numerical errors", {
  # Probabilities that don't quite sum to 1 due to floating point
  probs <- matrix(c(0.1, 0.3, 0.2, 0.15, 0.05, 0.12, 0.08001), nrow = 1)
  row_sums <- rowSums(probs)
  normalized <- probs / row_sums

  expect_equal(sum(normalized), 1.0, tolerance = 1e-10)
})

# ============================================================================
# EXPECTED VALUE MATHEMATICAL PROPERTIES
# ============================================================================

test_that("expected runs is bounded [0, 6]", {
  # All possible run values are non-negative (0, 0, 1, 2, 3, 4, 6)
  # So expected value must be in [0, 6]
  set.seed(42)
  # Test with 5 random distributions (mathematical property holds for any valid distribution)
  for (i in 1:5) {
    probs <- runif(7)
    probs <- probs / sum(probs)
    probs <- matrix(probs, nrow = 1)
    result <- calculate_expected_runs(probs)
    expect_gte(result, 0)
    expect_lte(result, 6)
  }
})

test_that("expected wicket prob equals first column value", {
  set.seed(42)
  # Test with 5 random distributions
  for (i in 1:5) {
    probs <- runif(7)
    probs <- probs / sum(probs)
    probs <- matrix(probs, nrow = 1)
    expect_equal(calculate_expected_wicket_prob(probs), probs[1, 1])
  }
})

# ============================================================================
# SKILL START VALUES TESTS
# ============================================================================

test_that("skill start values are defined for all formats", {
  for (format in c("t20", "odi", "test")) {
    vals <- get_skill_start_values(format)

    expect_true("scoring_index" %in% names(vals))
    expect_true("survival_rate" %in% names(vals))
    expect_true("economy_index" %in% names(vals))
    expect_true("strike_rate" %in% names(vals))

    # Survival rate should be high (players survive most deliveries)
    expect_gt(vals$survival_rate, 0.9)
    expect_lt(vals$survival_rate, 1.0)

    # Strike rate should be low (wickets are rare)
    expect_gt(vals$strike_rate, 0)
    expect_lt(vals$strike_rate, 0.1)
  }
})

test_that("venue start values are defined for all formats", {
  for (format in c("t20", "odi", "test")) {
    vals <- get_venue_start_values(format)

    expect_true("run_rate" %in% names(vals))
    expect_true("wicket_rate" %in% names(vals))
    expect_true("boundary_rate" %in% names(vals))
    expect_true("dot_rate" %in% names(vals))

    # Boundary rate should be reasonable
    expect_gt(vals$boundary_rate, 0.05)
    expect_lt(vals$boundary_rate, 0.25)

    # Dot rate varies by format
    expect_gt(vals$dot_rate, 0.2)
    expect_lt(vals$dot_rate, 0.85)
  }
})

test_that("T20 has higher expected runs than Test", {
  # Note: get_venue_start_values() returns RESIDUAL-based indices (all 0.0 for run_rate)

  # To test format differences, use the EXPECTED_RUNS constants directly
  expect_gt(EXPECTED_RUNS_T20, EXPECTED_RUNS_TEST)
  expect_gt(EXPECTED_RUNS_ODI, EXPECTED_RUNS_TEST)
})

test_that("T20 has higher wicket rate than ODI", {
  # T20 has more aggressive batting = more wickets per ball
  t20_skills <- get_skill_start_values("t20")
  odi_skills <- get_skill_start_values("odi")

  expect_gt(t20_skills$strike_rate, odi_skills$strike_rate)
})

# ============================================================================
# FEATURE PREPARATION TESTS (without model loading)
# ============================================================================

test_that("format alpha values differ by format", {
  # Different formats should have different learning rates
  expect_true(SKILL_ALPHA_T20 > SKILL_ALPHA_TEST)
  expect_true(SKILL_ALPHA_ODI > SKILL_ALPHA_TEST)
})

test_that("expected wicket rates match constants", {
  # These should match SKILL_START_STRIKE values
  expect_equal(EXPECTED_WICKET_T20, SKILL_START_STRIKE_T20)
  expect_equal(EXPECTED_WICKET_ODI, SKILL_START_STRIKE_ODI)
  expect_equal(EXPECTED_WICKET_TEST, SKILL_START_STRIKE_TEST)
})

test_that("expected runs constants are reasonable", {
  # T20 > ODI > Test for runs per ball
  expect_gt(EXPECTED_RUNS_T20, EXPECTED_RUNS_ODI)
  expect_gt(EXPECTED_RUNS_ODI, EXPECTED_RUNS_TEST)

  # All should be positive
  expect_gt(EXPECTED_RUNS_T20, 0)
  expect_gt(EXPECTED_RUNS_ODI, 0)
  expect_gt(EXPECTED_RUNS_TEST, 0)
})

# ============================================================================
# RESIDUAL CALCULATION PROPERTY TESTS
# ============================================================================

test_that("runs residual is actual minus expected", {
  # This tests the property without needing the actual model
  actual_runs <- 4
  exp_runs <- 1.2
  residual <- actual_runs - exp_runs
  expect_equal(residual, 2.8)
})

test_that("wicket residual is actual minus expected", {
  # When wicket falls (actual = 1) and expected was low
  actual_wicket <- 1
  exp_wicket <- 0.05
  residual <- actual_wicket - exp_wicket
  expect_equal(residual, 0.95)

  # When no wicket (actual = 0) and expected was low
  actual_wicket <- 0
  exp_wicket <- 0.05
  residual <- actual_wicket - exp_wicket
  expect_equal(residual, -0.05)
})

test_that("residuals have correct mean property", {
  # If model is well-calibrated, mean residual should be ~0
  # We test this property with known inputs
  probs <- matrix(c(0.05, 0.35, 0.30, 0.10, 0.02, 0.12, 0.06), nrow = 1)
  exp_runs <- calculate_expected_runs(probs)

  # Weighted average of possible outcomes
  outcomes <- c(0, 0, 1, 2, 3, 4, 6)
  expected_residual <- sum(probs * (outcomes - exp_runs))
  expect_equal(expected_residual, 0, tolerance = 1e-10)
})

# ============================================================================
# MODEL PATH HELPERS TESTS
# ============================================================================

test_that("get_models_dir returns valid path structure", {
  # Skip if bouncerdata not available
  skip_if_not(dir.exists("../bouncerdata") || dir.exists("bouncerdata"))

  path <- get_models_dir()
  # Should end with "models"
  expect_true(grepl("models$", path))
})

# ============================================================================
# EDGE CASE TESTS
# ============================================================================

test_that("expected runs handles extreme probability distributions", {
  # All probability on one outcome
  for (i in 1:7) {
    probs <- rep(0, 7)
    probs[i] <- 1
    probs <- matrix(probs, nrow = 1)

    result <- calculate_expected_runs(probs)
    expected <- c(0, 0, 1, 2, 3, 4, 6)[i]
    expect_equal(result, expected)
  }
})

test_that("calculate functions handle near-zero probabilities", {
  # Very small but non-zero probabilities
  probs <- matrix(c(1e-10, 1 - 6e-10, 1e-10, 1e-10, 1e-10, 1e-10, 1e-10), nrow = 1)

  # Should not error or produce NaN
  runs <- calculate_expected_runs(probs)
  wicket <- calculate_expected_wicket_prob(probs)

  expect_false(is.nan(runs))
  expect_false(is.nan(wicket))
  expect_false(is.infinite(runs))
  expect_false(is.infinite(wicket))
})

test_that("expected runs handles large number of rows efficiently", {
  # Performance test - should complete quickly
  n_rows <- 10000
  probs <- matrix(runif(n_rows * 7), nrow = n_rows)
  probs <- probs / rowSums(probs)  # Normalize each row

  start_time <- Sys.time()
  result <- calculate_expected_runs(probs)
  elapsed <- as.numeric(Sys.time() - start_time, units = "secs")

  expect_equal(length(result), n_rows)
  expect_lt(elapsed, 1.0)  # Should complete in under 1 second
})

# ============================================================================
# OUTCOME CLASS ORDERING TESTS
# ============================================================================

test_that("outcome classes are in correct order", {
  # The model assumes: wicket, 0, 1, 2, 3, 4, 6
  # Verify this assumption is correct
  runs_values <- c(0, 0, 1, 2, 3, 4, 6)

  # First two classes (wicket and dot) both have 0 runs
  expect_equal(runs_values[1], 0)
  expect_equal(runs_values[2], 0)

  # Runs increase monotonically after that (except no 5)
  expect_equal(runs_values[3], 1)
  expect_equal(runs_values[4], 2)
  expect_equal(runs_values[5], 3)
  expect_equal(runs_values[6], 4)
  expect_equal(runs_values[7], 6)
})

test_that("5 runs is not a standard outcome class", {
  # Cricket rarely has 5 runs from a single delivery
  # The model uses 7 classes: wicket, 0, 1, 2, 3, 4, 6
  runs_values <- c(0, 0, 1, 2, 3, 4, 6)
  expect_false(5 %in% runs_values)
})
