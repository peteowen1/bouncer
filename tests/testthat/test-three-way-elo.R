# Tests for 3-Way ELO System (three_way_elo.R)
#
# Core tests for the 3-way ELO rating system that involves Batter, Bowler, and Venue.
# Tests mathematical properties, zero-sum behavior, and formula correctness.

# ============================================================================
# ATTRIBUTION WEIGHTS TESTS
# ============================================================================

test_that("attribution weights sum to 1.0 for all format-gender combos", {
  formats <- c("T20", "ODI", "TEST")
  genders <- c("male", "female")

  for (fmt in formats) {
    for (gen in genders) {
      w <- get_run_elo_weights(fmt, gen)
      total <- w$w_batter + w$w_bowler + w$w_venue_session + w$w_venue_perm
      expect_equal(total, 1.0, tolerance = 1e-10,
                   label = paste("Run weights sum for", gen, fmt))

      wk <- get_wicket_elo_weights(fmt, gen)
      total_wk <- wk$w_batter + wk$w_bowler + wk$w_venue_session + wk$w_venue_perm
      expect_equal(total_wk, 1.0, tolerance = 1e-10,
                   label = paste("Wicket weights sum for", gen, fmt))
    }
  }
})

test_that("attribution weights are within valid range", {
  w <- get_run_elo_weights("T20", "male")

  # All weights should be positive
  expect_gt(w$w_batter, 0)
  expect_gt(w$w_bowler, 0)
  expect_gt(w$w_venue_session, 0)
  expect_gt(w$w_venue_perm, 0)

  # All weights should be less than 1
  expect_lt(w$w_batter, 1)
  expect_lt(w$w_bowler, 1)
  expect_lt(w$w_venue_session, 1)
  expect_lt(w$w_venue_perm, 1)
})

# ============================================================================
# EXPECTED RUNS CALCULATION TESTS
# ============================================================================

test_that("calculate_3way_expected_runs returns bounded values", {
  # At baseline ELOs, expected runs should be close to agnostic prediction
  result <- calculate_3way_expected_runs(
    agnostic_runs = 1.2,
    batter_run_elo = THREE_WAY_ELO_START,
    bowler_run_elo = THREE_WAY_ELO_START,
    venue_perm_run_elo = THREE_WAY_ELO_START,
    venue_session_run_elo = THREE_WAY_ELO_START,
    format = "t20"
  )

  expect_equal(result, 1.2, tolerance = 0.01)
})

test_that("calculate_3way_expected_runs is bounded 0-6", {
  # Test extreme high case
  high_result <- calculate_3way_expected_runs(
    agnostic_runs = 5.0,
    batter_run_elo = 2000,
    bowler_run_elo = 1000,
    venue_perm_run_elo = 2000,
    venue_session_run_elo = 2000,
    format = "t20"
  )
  expect_lte(high_result, 6)

  # Test extreme low case
  low_result <- calculate_3way_expected_runs(
    agnostic_runs = 0.5,
    batter_run_elo = 1000,
    bowler_run_elo = 2000,
    venue_perm_run_elo = 1000,
    venue_session_run_elo = 1000,
    format = "t20"
  )
  expect_gte(low_result, 0)
})

test_that("higher batter ELO increases expected runs", {
  baseline <- calculate_3way_expected_runs(
    agnostic_runs = 1.2,
    batter_run_elo = THREE_WAY_ELO_START,
    bowler_run_elo = THREE_WAY_ELO_START,
    venue_perm_run_elo = THREE_WAY_ELO_START,
    venue_session_run_elo = THREE_WAY_ELO_START,
    format = "t20"
  )

  higher_batter <- calculate_3way_expected_runs(
    agnostic_runs = 1.2,
    batter_run_elo = THREE_WAY_ELO_START + 100,
    bowler_run_elo = THREE_WAY_ELO_START,
    venue_perm_run_elo = THREE_WAY_ELO_START,
    venue_session_run_elo = THREE_WAY_ELO_START,
    format = "t20"
  )

  expect_gt(higher_batter, baseline)
})

test_that("higher bowler ELO decreases expected runs", {
  baseline <- calculate_3way_expected_runs(
    agnostic_runs = 1.2,
    batter_run_elo = THREE_WAY_ELO_START,
    bowler_run_elo = THREE_WAY_ELO_START,
    venue_perm_run_elo = THREE_WAY_ELO_START,
    venue_session_run_elo = THREE_WAY_ELO_START,
    format = "t20"
  )

  higher_bowler <- calculate_3way_expected_runs(
    agnostic_runs = 1.2,
    batter_run_elo = THREE_WAY_ELO_START,
    bowler_run_elo = THREE_WAY_ELO_START + 100,
    venue_perm_run_elo = THREE_WAY_ELO_START,
    venue_session_run_elo = THREE_WAY_ELO_START,
    format = "t20"
  )

  expect_lt(higher_bowler, baseline)
})

# ============================================================================
# EXPECTED WICKET PROBABILITY TESTS
# ============================================================================

test_that("calculate_3way_expected_wicket returns valid probability", {
  result <- calculate_3way_expected_wicket(
    agnostic_wicket = 0.05,
    batter_wicket_elo = THREE_WAY_ELO_START,
    bowler_wicket_elo = THREE_WAY_ELO_START,
    venue_perm_wicket_elo = THREE_WAY_ELO_START,
    venue_session_wicket_elo = THREE_WAY_ELO_START
  )

  expect_gte(result, 0.001)
  expect_lte(result, 0.5)
})

test_that("higher batter wicket ELO decreases wicket probability", {
  baseline <- calculate_3way_expected_wicket(
    agnostic_wicket = 0.05,
    batter_wicket_elo = THREE_WAY_ELO_START,
    bowler_wicket_elo = THREE_WAY_ELO_START,
    venue_perm_wicket_elo = THREE_WAY_ELO_START,
    venue_session_wicket_elo = THREE_WAY_ELO_START
  )

  higher_batter <- calculate_3way_expected_wicket(
    agnostic_wicket = 0.05,
    batter_wicket_elo = THREE_WAY_ELO_START + 100,
    bowler_wicket_elo = THREE_WAY_ELO_START,
    venue_perm_wicket_elo = THREE_WAY_ELO_START,
    venue_session_wicket_elo = THREE_WAY_ELO_START
  )

  # Higher batter wicket ELO = better at surviving = lower wicket probability
  expect_lt(higher_batter, baseline)
})

test_that("higher bowler wicket ELO increases wicket probability", {
  baseline <- calculate_3way_expected_wicket(
    agnostic_wicket = 0.05,
    batter_wicket_elo = THREE_WAY_ELO_START,
    bowler_wicket_elo = THREE_WAY_ELO_START,
    venue_perm_wicket_elo = THREE_WAY_ELO_START,
    venue_session_wicket_elo = THREE_WAY_ELO_START
  )

  higher_bowler <- calculate_3way_expected_wicket(
    agnostic_wicket = 0.05,
    batter_wicket_elo = THREE_WAY_ELO_START,
    bowler_wicket_elo = THREE_WAY_ELO_START + 100,
    venue_perm_wicket_elo = THREE_WAY_ELO_START,
    venue_session_wicket_elo = THREE_WAY_ELO_START
  )

  # Higher bowler wicket ELO = better at taking wickets = higher wicket probability
  expect_gt(higher_bowler, baseline)
})

# ============================================================================
# K-FACTOR TESTS
# ============================================================================

test_that("K-factor decreases with experience", {
  k_new <- get_3way_player_k(deliveries = 10, format = "t20", elo_type = "run")
  k_exp <- get_3way_player_k(deliveries = 1000, format = "t20", elo_type = "run")

  expect_gt(k_new, k_exp)
})

test_that("K-factor respects min/max bounds", {
  k_params <- get_run_k_factors("T20", "male")

  # Brand new player should be close to max
  k_new <- get_3way_player_k(deliveries = 0, format = "t20", elo_type = "run")
  expect_gte(k_new, k_params$k_min)
  expect_lte(k_new, k_params$k_max * 2)  # Allow for multipliers

  # Very experienced player should be close to min
  k_exp <- get_3way_player_k(deliveries = 10000, format = "t20", elo_type = "run")
  expect_gte(k_exp, k_params$k_min * 0.5)  # Allow for multipliers
})

test_that("knockout multiplier increases K-factor", {
  k_normal <- get_3way_player_k(deliveries = 500, format = "t20",
                                 elo_type = "run", is_knockout = FALSE)
  k_knockout <- get_3way_player_k(deliveries = 500, format = "t20",
                                   elo_type = "run", is_knockout = TRUE)

  expect_gt(k_knockout, k_normal)
  expect_equal(k_knockout / k_normal, THREE_WAY_KNOCKOUT_MULT, tolerance = 0.01)
})

test_that("phase multipliers are applied correctly", {
  k_powerplay <- get_3way_player_k(deliveries = 500, format = "t20",
                                    elo_type = "run", phase = "powerplay")
  k_middle <- get_3way_player_k(deliveries = 500, format = "t20",
                                 elo_type = "run", phase = "middle")
  k_death <- get_3way_player_k(deliveries = 500, format = "t20",
                                elo_type = "run", phase = "death")

  # Death > powerplay > middle (in T20)
  expect_gt(k_death, k_middle)
  expect_gt(k_powerplay, k_middle)
})

# ============================================================================
# ELO UPDATE TESTS (ZERO-SUM PROPERTY)
# ============================================================================

test_that("run ELO updates have correct sign directions", {
  # When batter scores above expectation (positive delta)
  result <- update_3way_run_elos(
    actual_run_score = 0.8,
    expected_run_score = 0.5,
    k_batter = 20,
    k_bowler = 20,
    k_venue_perm = 5,
    k_venue_session = 10
  )

  # Batter gains, bowler loses
  expect_gt(result$delta_batter, 0)
  expect_lt(result$delta_bowler, 0)

  # With equal K-factors, magnitudes should be equal (opposite signs)
  expect_equal(abs(result$delta_batter), abs(result$delta_bowler), tolerance = 0.001)
})

test_that("run ELO updates with equal K are zero-sum for batter/bowler", {
  k <- 20

  result <- update_3way_run_elos(
    actual_run_score = 0.7,
    expected_run_score = 0.4,
    k_batter = k,
    k_bowler = k,
    k_venue_perm = 5,
    k_venue_session = 10
  )

  # Batter + bowler should sum to zero with equal K
  expect_equal(result$delta_batter + result$delta_bowler, 0, tolerance = 1e-10)
})

test_that("wicket ELO updates have correct sign directions", {
  # When wicket falls (actual = 1, batter did poorly)
  result <- update_3way_wicket_elos(
    actual_wicket = 1,
    expected_wicket = 0.05,
    k_batter = 10,
    k_bowler = 10,
    k_venue_perm = 3,
    k_venue_session = 5
  )

  # Batter loses (expected - actual = 0.05 - 1 = -0.95)
  expect_lt(result$delta_batter, 0)

  # Bowler gains (actual - expected = 1 - 0.05 = 0.95)
  expect_gt(result$delta_bowler, 0)
})

test_that("wicket ELO updates with equal K are zero-sum for batter/bowler", {
  k <- 10

  result <- update_3way_wicket_elos(
    actual_wicket = 1,
    expected_wicket = 0.05,
    k_batter = k,
    k_bowler = k,
    k_venue_perm = 3,
    k_venue_session = 5
  )

  # Batter + bowler should sum to zero with equal K
  expect_equal(result$delta_batter + result$delta_bowler, 0, tolerance = 1e-10)
})

# ============================================================================
# INACTIVITY DECAY TESTS
# ============================================================================

test_that("no decay below threshold", {
  decayed <- apply_inactivity_decay(1600, days_inactive = 30)
  expect_equal(decayed, 1600)
})

test_that("decay occurs above threshold", {
  decayed <- apply_inactivity_decay(1600, days_inactive = 365)

  # Should be between replacement level and original
  expect_lt(decayed, 1600)
  expect_gt(decayed, THREE_WAY_REPLACEMENT_LEVEL)
})

test_that("long inactivity decays toward replacement level", {
  decayed <- apply_inactivity_decay(1600, days_inactive = 3650)  # 10 years

  # Should be very close to replacement level
  expect_equal(decayed, THREE_WAY_REPLACEMENT_LEVEL, tolerance = 50)
})

# ============================================================================
# RELIABILITY AND BLENDING TESTS
# ============================================================================

test_that("reliability is 0 with 0 balls", {
  rel <- calculate_reliability(0)
  expect_equal(rel, 0)
})
test_that("reliability is 0.5 at halflife balls", {
  rel <- calculate_reliability(THREE_WAY_RELIABILITY_HALFLIFE)
  expect_equal(rel, 0.5, tolerance = 0.001)
})

test_that("reliability approaches 1 with many balls", {
  rel <- calculate_reliability(10000)
  expect_gt(rel, 0.95)
})

test_that("blend returns replacement level with 0 balls", {
  blended <- blend_elo_with_replacement(1800, balls = 0)
  expect_equal(blended, THREE_WAY_REPLACEMENT_LEVEL)
})

test_that("blend returns midpoint at halflife balls", {
  raw_elo <- 1600
  blended <- blend_elo_with_replacement(raw_elo, balls = THREE_WAY_RELIABILITY_HALFLIFE)

  expected_blend <- 0.5 * raw_elo + 0.5 * THREE_WAY_REPLACEMENT_LEVEL
  expect_equal(blended, expected_blend, tolerance = 0.01)
})

# ============================================================================
# MATCH PHASE DETECTION TESTS
# ============================================================================

test_that("T20 phase detection is correct", {
  expect_equal(get_match_phase(0, "t20"), "powerplay")
  expect_equal(get_match_phase(5, "t20"), "powerplay")
  expect_equal(get_match_phase(6, "t20"), "middle")
  expect_equal(get_match_phase(15, "t20"), "middle")
  expect_equal(get_match_phase(16, "t20"), "death")
  expect_equal(get_match_phase(19, "t20"), "death")
})

test_that("ODI phase detection is correct", {
  expect_equal(get_match_phase(0, "odi"), "powerplay")
  expect_equal(get_match_phase(9, "odi"), "powerplay")
  expect_equal(get_match_phase(10, "odi"), "middle")
  expect_equal(get_match_phase(39, "odi"), "middle")
  expect_equal(get_match_phase(40, "odi"), "death")
  expect_equal(get_match_phase(49, "odi"), "death")
})

test_that("Test match has no traditional phases", {
  expect_equal(get_match_phase(0, "test"), "middle")
  expect_equal(get_match_phase(50, "test"), "middle")
  expect_equal(get_match_phase(100, "test"), "middle")
})

# ============================================================================
# CALIBRATION K-FACTOR MULTIPLIER TESTS
# ============================================================================

test_that("fully calibrated player has no K boost", {
  mult <- get_calibration_k_multiplier(100)
  expect_equal(mult, 1.0, tolerance = 0.001)
})

test_that("uncalibrated player has maximum K boost", {
  mult <- get_calibration_k_multiplier(0)
  expect_equal(mult, 1 + THREE_WAY_CALIBRATION_K_BOOST, tolerance = 0.001)
})

test_that("half calibrated player has half K boost", {
  mult <- get_calibration_k_multiplier(50)
  expected <- 1 + 0.5 * THREE_WAY_CALIBRATION_K_BOOST
  expect_equal(mult, expected, tolerance = 0.001)
})

test_that("NULL calibration score uses max boost", {
  mult <- get_calibration_k_multiplier(NULL)
  expect_equal(mult, 1 + THREE_WAY_CALIBRATION_K_BOOST, tolerance = 0.001)
})

# ============================================================================
# FORMAT-SPECIFIC PARAMETER TESTS
# ============================================================================

test_that("all formats have consistent parameters", {
  for (format in c("t20", "odi", "test")) {
    params <- build_3way_elo_params(format)

    # Check all required parameters exist
    expect_true("k_run_max" %in% names(params))
    expect_true("k_run_min" %in% names(params))
    expect_true("k_wicket_max" %in% names(params))
    expect_true("k_wicket_min" %in% names(params))
    expect_true("w_batter" %in% names(params))
    expect_true("w_bowler" %in% names(params))
    expect_true("w_venue_perm" %in% names(params))
    expect_true("w_venue_session" %in% names(params))

    # K-max should be greater than K-min
    expect_gt(params$k_run_max, params$k_run_min)
    expect_gt(params$k_wicket_max, params$k_wicket_min)

    # Attribution weights should sum to 1
    weight_sum <- params$w_batter + params$w_bowler +
                  params$w_venue_perm + params$w_venue_session
    expect_equal(weight_sum, 1.0, tolerance = 1e-10)
  }
})
