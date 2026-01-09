# Tests for ELO Core Functions

test_that("calculate_expected_outcome returns valid probabilities", {
  # Equal ratings should give 0.5
  expect_equal(calculate_expected_outcome(1500, 1500), 0.5)

  # Higher rating should give higher probability
  expect_gt(calculate_expected_outcome(1600, 1400), 0.5)
  expect_lt(calculate_expected_outcome(1400, 1600), 0.5)

  # Result should always be between 0 and 1
  expect_gte(calculate_expected_outcome(2000, 1000), 0)
  expect_lte(calculate_expected_outcome(2000, 1000), 1)
  expect_gte(calculate_expected_outcome(1000, 2000), 0)
  expect_lte(calculate_expected_outcome(1000, 2000), 1)
})

test_that("calculate_expected_outcome follows ELO formula", {
  # 400 point difference should give ~0.91 for higher rated
  result <- calculate_expected_outcome(1900, 1500)
  expect_equal(round(result, 2), 0.91)

  # Symmetric property: E(A vs B) + E(B vs A) = 1
  e_ab <- calculate_expected_outcome(1600, 1400)
  e_ba <- calculate_expected_outcome(1400, 1600)
  expect_equal(e_ab + e_ba, 1, tolerance = 0.001)
})

test_that("calculate_k_factor returns appropriate values by format", {
  # Test matches should have higher K-factor
  expect_gt(calculate_k_factor("test", 0), calculate_k_factor("t20", 0))

  # K-factor should decrease with experience (more matches)
  expect_gt(calculate_k_factor("t20", 0), calculate_k_factor("t20", 100))
})

test_that("calculate_k_factor handles different match types", {
  # Should not error for valid match types
  expect_no_error(calculate_k_factor("test", 10))
  expect_no_error(calculate_k_factor("odi", 10))
  expect_no_error(calculate_k_factor("t20", 10))

  # Should return numeric
  expect_type(calculate_k_factor("t20", 50), "double")
})

test_that("calculate_elo_update adjusts rating correctly", {
  current_elo <- 1500
  k_factor <- 20

  # Win (actual = 1) when expected = 0.5 should increase ELO
  new_elo <- calculate_elo_update(current_elo, 0.5, 1.0, k_factor)
  expect_gt(new_elo, current_elo)
  expect_equal(new_elo, 1500 + 20 * 0.5)  # 1510

  # Loss (actual = 0) when expected = 0.5 should decrease ELO
  new_elo <- calculate_elo_update(current_elo, 0.5, 0.0, k_factor)
  expect_lt(new_elo, current_elo)
  expect_equal(new_elo, 1500 - 20 * 0.5)  # 1490

  # Meeting expectations (actual = expected) should not change ELO
  new_elo <- calculate_elo_update(current_elo, 0.5, 0.5, k_factor)
  expect_equal(new_elo, current_elo)
})

test_that("calculate_delivery_outcome_score returns valid scores", {
  # Wicket should be lowest score (0)
  wicket_score <- calculate_delivery_outcome_score(0, is_wicket = TRUE, is_boundary = FALSE)
  expect_equal(wicket_score, 0)

  # Dot ball should be low
  dot_score <- calculate_delivery_outcome_score(0, is_wicket = FALSE, is_boundary = FALSE)
  expect_lt(dot_score, 0.5)

  # Six should be highest
  six_score <- calculate_delivery_outcome_score(6, is_wicket = FALSE, is_boundary = TRUE)
  expect_equal(six_score, 1)

  # Scores should be between 0 and 1
  for (runs in 0:6) {
    score <- calculate_delivery_outcome_score(runs, is_wicket = FALSE, is_boundary = runs >= 4)
    expect_gte(score, 0)
    expect_lte(score, 1)
  }
})

test_that("initialize_player_elo returns starting ELO", {
  # Should return the starting ELO constant
  batting_start <- initialize_player_elo("batting")
  bowling_start <- initialize_player_elo("bowling")

  expect_type(batting_start, "double")
  expect_type(bowling_start, "double")

  # Starting ELO should be positive
  expect_gt(batting_start, 0)
  expect_gt(bowling_start, 0)
})

test_that("normalize_match_type handles variations", {
  # Should normalize to lowercase
  expect_equal(normalize_match_type("T20"), "t20")
  expect_equal(normalize_match_type("ODI"), "odi")
  expect_equal(normalize_match_type("TEST"), "test")

  # Should handle lowercase input

  expect_equal(normalize_match_type("t20"), "t20")
  expect_equal(normalize_match_type("odi"), "odi")
})
