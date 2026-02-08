# Pipeline Integration Tests
#
# Tests for inter-step data flow and consistency across pipeline stages.

# ============================================================================
# DATA FLOW TESTS (using mock from test-database-mock.R pattern)
# ============================================================================

test_that("skill index EMA update formula is correct", {
  # Test the exponential moving average formula used in skill indices
  # new_skill = (1 - alpha) * old_skill + alpha * residual

  alpha <- SKILL_ALPHA_T20
  old_skill <- 0.0
  residual <- 2.0  # e.g., batter scored 2 runs above expected

  new_skill <- (1 - alpha) * old_skill + alpha * residual

  # With alpha=0.01, new_skill should be 0.02
  expect_equal(new_skill, alpha * residual, tolerance = 1e-10)

  # After many updates with same residual, should converge to residual
  skill <- 0.0
  for (i in 1:1000) {
    skill <- (1 - alpha) * skill + alpha * residual
  }
  expect_equal(skill, residual, tolerance = 0.01)
})

test_that("ELO expected score formula is correct", {
  # E = 1 / (1 + 10^((Rb - Ra) / 400))
  # When Ra = Rb, expected score = 0.5

  rating_a <- 1500
  rating_b <- 1500

  expected <- 1 / (1 + 10^((rating_b - rating_a) / 400))
  expect_equal(expected, 0.5, tolerance = 1e-10)

  # When Ra > Rb by 400, expected ~0.909
  rating_a <- 1900
  rating_b <- 1500
  expected <- 1 / (1 + 10^((rating_b - rating_a) / 400))
  expect_equal(expected, 10/11, tolerance = 1e-10)
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
  total <- THREE_WAY_W_BATTER + THREE_WAY_W_BOWLER +
           THREE_WAY_W_VENUE_SESSION + THREE_WAY_W_VENUE_PERM
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
