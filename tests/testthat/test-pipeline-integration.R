# Pipeline Integration Tests
#
# Tests for inter-step data flow and consistency across pipeline stages.

# ============================================================================
# DATA FLOW TESTS (using mock from test-database-mock.R pattern)
# ============================================================================

test_that("update_skill_index EMA converges to residual value", {
  alpha <- SKILL_ALPHA_T20

  # Single update from zero
  result <- update_skill_index(old_value = 0.0, observation = 2.0, alpha = alpha)
  expect_equal(result, alpha * 2.0, tolerance = 1e-10)

  # After many updates with same observation, should converge to observation
  skill <- 0.0
  for (i in seq_len(1000)) {
    skill <- update_skill_index(skill, observation = 2.0, alpha = alpha)
  }
  expect_equal(skill, 2.0, tolerance = 0.01)
})

test_that("calculate_expected_outcome returns correct ELO expectations", {
  # Equal ratings -> 0.5
  expect_equal(calculate_expected_outcome(1500, 1500), 0.5, tolerance = 1e-10)

  # 400-point advantage -> 10/11 (~0.909)
  expect_equal(calculate_expected_outcome(1900, 1500), 10/11, tolerance = 1e-10)

  # Symmetry: A vs B + B vs A = 1
  expect_equal(
    calculate_expected_outcome(1600, 1400) + calculate_expected_outcome(1400, 1600),
    1.0, tolerance = 1e-10
  )
})

# ============================================================================
# FORMAT CONSISTENCY TESTS
# ============================================================================

test_that("format normalization is consistent across all aliases", {
  t20_aliases <- c("t20", "T20", "IT20", "it20")
  for (alias in t20_aliases) {
    expect_equal(normalize_format(alias), "t20", info = paste("Failed for:", alias))
  }

  odi_aliases <- c("odi", "ODI", "ODM", "odm")
  for (alias in odi_aliases) {
    expect_equal(normalize_format(alias), "odi", info = paste("Failed for:", alias))
  }

  test_aliases <- c("test", "Test", "TEST", "MDM", "mdm")
  for (alias in test_aliases) {
    expect_equal(normalize_format(alias), "test", info = paste("Failed for:", alias))
  }
})

test_that("max overs are correct per format", {
  expect_equal(get_max_overs("t20"), 20)
  expect_equal(get_max_overs("odi"), 50)
  # Test matches have no fixed limit - returns NULL by design
  expect_null(get_max_overs("test"))
})

test_that("phase boundaries are internally consistent for limited overs", {
  # T20/ODI have powerplay → middle → death phases
  for (fmt in c("t20", "odi")) {
    bounds <- get_phase_boundaries(fmt)
    max_overs <- get_max_overs(fmt)

    # Should have powerplay_end and middle_end
    expect_true("powerplay_end" %in% names(bounds))
    expect_true("middle_end" %in% names(bounds))

    # Powerplay should end before middle phase ends
    expect_lt(bounds$powerplay_end, bounds$middle_end)

    # Middle phase should end at or before max overs
    expect_lte(bounds$middle_end, max_overs)
  }
})

test_that("phase boundaries exist for test format", {
  # Test matches have new_ball → middle → old_ball phases
  bounds <- get_phase_boundaries("test")

  expect_true("new_ball_end" %in% names(bounds))
  expect_true("middle_end" %in% names(bounds))
  expect_lt(bounds$new_ball_end, bounds$middle_end)
})

# ============================================================================
# CONSTANT ORDERING TESTS
# ============================================================================

test_that("expected runs are format-ordered (T20 > ODI > Test)", {
  expect_gt(EXPECTED_RUNS_T20, EXPECTED_RUNS_ODI)
  expect_gt(EXPECTED_RUNS_ODI, EXPECTED_RUNS_TEST)
})

test_that("expected wicket rates are format-ordered (T20 > ODI > Test)", {
  expect_gt(EXPECTED_WICKET_T20, EXPECTED_WICKET_ODI)
  expect_gt(EXPECTED_WICKET_ODI, EXPECTED_WICKET_TEST)
})

test_that("skill alpha values are format-ordered (T20 > ODI > Test)", {
  # T20 adapts faster (higher alpha) due to shorter matches
  expect_gt(SKILL_ALPHA_T20, SKILL_ALPHA_ODI)
  expect_gt(SKILL_ALPHA_ODI, SKILL_ALPHA_TEST)
})

test_that("3-way ELO attribution weights sum to 1", {
  w <- get_run_elo_weights("T20", "male")
  total <- w$w_batter + w$w_bowler + w$w_venue_session + w$w_venue_perm
  expect_equal(total, 1.0, tolerance = 1e-10)
})

# ============================================================================
# CROSS-FUNCTION CONSISTENCY TESTS
# ============================================================================

test_that("match_types from get_match_types_for_format match valid types", {
  valid_types <- c("T20", "IT20", "ODI", "ODM", "Test", "MDM")

  for (fmt in c("t20", "odi", "test")) {
    types <- get_match_types_for_format(fmt)
    for (t in types) {
      expect_true(t %in% valid_types, info = paste("Invalid type:", t, "for format:", fmt))
    }
  }
})

test_that("skill start values are consistent with expected values", {
  # Strike rate start should equal expected wicket rate
  expect_equal(SKILL_START_STRIKE_T20, EXPECTED_WICKET_T20)
  expect_equal(SKILL_START_STRIKE_ODI, EXPECTED_WICKET_ODI)
  expect_equal(SKILL_START_STRIKE_TEST, EXPECTED_WICKET_TEST)
})

# ============================================================================
# PIPELINE DATA FLOW TESTS
# ============================================================================
# These tests verify the contract between pipeline stages:
#   ELO ratings → Skill indices → Expected runs → Residuals → Skill updates
# using synthetic fixture data (no database required).

test_that("ELO → expected outcome → skill residual pipeline is consistent", {
  # Stage 1: ELO gives expected outcome
  batter_elo <- 1600
  bowler_elo <- 1400
  expected <- calculate_expected_outcome(batter_elo, bowler_elo)

  # Strong batter should be favoured

  expect_gt(expected, 0.5)

  # Stage 2: ELO update is zero-sum between batter and bowler perspectives
  k <- 20
  actual_outcome <- 1  # batter wins the exchange
  batter_new <- calculate_elo_update(batter_elo, expected, actual_outcome, k)
  bowler_new <- calculate_elo_update(bowler_elo, 1 - expected, 1 - actual_outcome, k)

  # Zero-sum: total ELO should be preserved
  expect_equal(batter_new + bowler_new, batter_elo + bowler_elo, tolerance = 1e-10)

  # Batter won as expected (was favoured) → small gain
  batter_gain <- batter_new - batter_elo
  expect_gt(batter_gain, 0)
  expect_lt(batter_gain, k)  # Gain bounded by K

  # Stage 3: Skill index update uses residual (actual - expected)
  skill <- 0.0
  agnostic_expected_runs <- EXPECTED_RUNS_T20
  actual_runs <- 4  # hit a boundary
  residual <- actual_runs - agnostic_expected_runs

  # Positive residual → skill should increase
  alpha <- SKILL_ALPHA_T20
  new_skill <- update_skill_index(skill, residual, alpha)
  expect_gt(new_skill, 0)

  # Stage 4: Skill-adjusted expected runs should differ from agnostic baseline
  adjusted <- calculate_expected_runs_skill(
    agnostic_runs = agnostic_expected_runs,
    batter_run_skill = new_skill,
    bowler_run_skill = 0,
    venue_perm_run_skill = 0,
    venue_session_run_skill = 0,
    format = "t20"
  )

  # Good batter skill → higher expected runs
  expect_gt(adjusted, agnostic_expected_runs)
})

test_that("skill indices converge correctly over repeated deliveries", {
  # Simulate a batter who consistently scores above average
  alpha <- SKILL_ALPHA_T20
  baseline <- EXPECTED_RUNS_T20
  actual_rpb <- 1.8  # above average T20 scoring rate
  residual <- actual_rpb - baseline

  # Run 500 deliveries of consistent above-average performance
  skill <- 0.0
  for (i in seq_len(500)) {
    skill <- update_skill_index(skill, residual, alpha)
  }

  # Skill should converge toward the residual (how much above average)
  expect_equal(skill, residual, tolerance = 0.05)

  # The adjusted expected runs should be above baseline
  # (Note: skill effect is weighted by w_batter, so adjusted < actual_rpb)
  adjusted <- calculate_expected_runs_skill(
    agnostic_runs = baseline,
    batter_run_skill = skill,
    bowler_run_skill = 0,
    venue_perm_run_skill = 0,
    venue_session_run_skill = 0,
    format = "t20"
  )
  expect_gt(adjusted, baseline)
})

test_that("pipeline respects format-specific parameters", {
  # Same player performance should produce different skill values per format
  # because alpha (learning rate) differs: T20 > ODI > Test
  skill_t20 <- 0.0
  skill_odi <- 0.0
  skill_test <- 0.0

  residual <- 0.5  # consistently above average

  for (i in seq_len(100)) {
    skill_t20 <- update_skill_index(skill_t20, residual, SKILL_ALPHA_T20)
    skill_odi <- update_skill_index(skill_odi, residual, SKILL_ALPHA_ODI)
    skill_test <- update_skill_index(skill_test, residual, SKILL_ALPHA_TEST)
  }

  # T20 adapts fastest (highest alpha) → closest to residual after 100 updates
  expect_gt(skill_t20, skill_odi)
  expect_gt(skill_odi, skill_test)
})

test_that("chronological ordering invariant affects ELO outcomes", {
  # This test demonstrates WHY chronological ordering matters.
  # Different orderings of wins/losses produce different final ratings
  # because ELO updates compound (K-factor depends on experience).

  start_elo <- 1400

  # Sequence 1: win first, then lose
  elo_a <- start_elo
  expected_a <- calculate_expected_outcome(elo_a, 1400)
  elo_a <- calculate_elo_update(elo_a, expected_a, 1, 32)  # win
  expected_a <- calculate_expected_outcome(elo_a, 1400)
  elo_a <- calculate_elo_update(elo_a, expected_a, 0, 32)  # lose

  # Sequence 2: lose first, then win
  elo_b <- start_elo
  expected_b <- calculate_expected_outcome(elo_b, 1400)
  elo_b <- calculate_elo_update(elo_b, expected_b, 0, 32)  # lose
  expected_b <- calculate_expected_outcome(elo_b, 1400)
  elo_b <- calculate_elo_update(elo_b, expected_b, 1, 32)  # win

  # With equal start/opponent, win-then-lose vs lose-then-win
  # produce different final ELOs because the expected value shifts
  expect_false(elo_a == elo_b)
})

test_that("3-way ELO attribution weights are valid for all format-gender combos", {
  for (fmt in c("T20", "ODI", "Test")) {
    for (gender in c("male", "female")) {
      w <- get_run_elo_weights(fmt, gender)

      # All weights should be positive
      expect_true(all(c(w$w_batter, w$w_bowler, w$w_venue_session, w$w_venue_perm) > 0),
                  info = paste("Negative weight for", fmt, gender))

      # Weights must sum to 1
      total <- w$w_batter + w$w_bowler + w$w_venue_session + w$w_venue_perm
      expect_equal(total, 1.0, tolerance = 1e-10,
                   info = paste("Weights don't sum to 1 for", fmt, gender))

      # Batter should have highest weight (primary contributor)
      expect_true(w$w_batter > w$w_bowler,
                  info = paste("Bowler weight exceeds batter for", fmt, gender))
    }
  }
})
