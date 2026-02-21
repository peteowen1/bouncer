# Tests for player_skill_index.R and venue_skill_index.R
#
# Covers: EMA update formula, expected run/wicket calculations, skill alphas,
# starting values, venue alphas, venue starting values, utility functions,
# and format-specific constant consistency.

# ==============================================================================
# PLAYER SKILL: EMA UPDATE FORMULA
# ==============================================================================

test_that("update_skill_index performs correct EMA calculation", {
  # Formula: new = alpha * observation + (1 - alpha) * old
  expect_equal(update_skill_index(0, 1, alpha = 0.1), 0.1)
  expect_equal(update_skill_index(1, 0, alpha = 0.1), 0.9)
  expect_equal(update_skill_index(0.5, 0.5, alpha = 0.1), 0.5)
})

test_that("update_skill_index converges to observation with alpha=1", {
  expect_equal(update_skill_index(100, 0, alpha = 1), 0)
  expect_equal(update_skill_index(0, 42, alpha = 1), 42)
})

test_that("update_skill_index ignores observation with alpha=0", {
  expect_equal(update_skill_index(100, 0, alpha = 0), 100)
  expect_equal(update_skill_index(3.14, 999, alpha = 0), 3.14)
})

test_that("update_skill_index is monotonic in observation", {
  old <- 0.5
  alpha <- 0.01
  low <- update_skill_index(old, 0, alpha)
  mid <- update_skill_index(old, 1, alpha)
  high <- update_skill_index(old, 4, alpha)
  expect_true(low < mid)
  expect_true(mid < high)
})

test_that("update_skill_index handles repeated updates toward stable value", {
  # Simulating 1000 balls of 1.0 runs/ball — should converge toward 1.0
  value <- 0
  for (i in 1:1000) {
    value <- update_skill_index(value, 1.0, alpha = 0.01)
  }
  expect_true(abs(value - 1.0) < 0.001)
})


# ==============================================================================
# SKILL INDICES: EXPECTED RUNS (4-entity model from skill_indices.R)
# ==============================================================================

test_that("calculate_expected_runs_skill adjusts baseline with skill effects", {
  # All skills at 0 -> returns baseline
  result <- calculate_expected_runs_skill(1.0, 0, 0, 0, 0)
  expect_equal(result, 1.0)

  # Positive batter skill increases expected runs
  result_good_batter <- calculate_expected_runs_skill(1.0, 0.2, 0, 0, 0)
  expect_true(result_good_batter > 1.0)

  # Positive bowler skill (restricts runs) decreases expected runs
  result_good_bowler <- calculate_expected_runs_skill(1.0, 0, 0.2, 0, 0)
  expect_true(result_good_bowler < 1.0)
})

test_that("calculate_expected_runs_skill bounds to [0, 6]", {
  # Extreme positive skills
  result <- calculate_expected_runs_skill(5, 0.5, -0.5, 0.3, 0.3)
  expect_true(result <= 6)

  # Extreme negative -> clamped to 0

  result <- calculate_expected_runs_skill(0, -0.5, 0.5, -0.3, -0.3)
  expect_true(result >= 0)
})

test_that("calculate_expected_runs_skill venue effects work", {
  baseline <- calculate_expected_runs_skill(1.0, 0, 0, 0, 0)

  # High-scoring venue (positive permanent + session)
  high_venue <- calculate_expected_runs_skill(1.0, 0, 0, 0.2, 0.1)
  expect_true(high_venue > baseline)
})


# ==============================================================================
# SKILL INDICES: EXPECTED WICKET (4-entity model from skill_indices.R)
# ==============================================================================

test_that("calculate_expected_wicket_skill adds skills directly", {
  # All skills at 0 -> returns baseline
  result <- calculate_expected_wicket_skill(0.05, 0, 0, 0, 0)
  expect_equal(result, 0.05)

  # Positive batter wicket skill = gets out more
  result <- calculate_expected_wicket_skill(0.05, 0.01, 0, 0, 0)
  expect_equal(result, 0.06)
})

test_that("calculate_expected_wicket_skill clamps to [0.001, 0.50]", {
  # Very low
  result <- calculate_expected_wicket_skill(0.01, -0.05, -0.05, -0.05, -0.05)
  expect_true(result >= 0.001)

  # Very high
  result <- calculate_expected_wicket_skill(0.3, 0.1, 0.1, 0.1, 0.1)
  expect_true(result <= 0.50)
})


# ==============================================================================
# PLAYER SKILL: ALPHAS AND STARTING VALUES
# ==============================================================================

test_that("get_skill_alpha returns max at 0 deliveries and decays with experience", {
  # Formula: alpha_min + (alpha_max - alpha_min) * exp(-deliveries / halflife)
  # At 0 deliveries, alpha = alpha_max
  alpha_0 <- get_skill_alpha(0, format = "t20", skill_type = "run", gender = "male")
  expect_equal(alpha_0, SKILL_ALPHA_RUN_MAX_MENS_T20)

  # At many deliveries, alpha approaches alpha_min
  alpha_big <- get_skill_alpha(10000, format = "t20", skill_type = "run", gender = "male")
  expect_true(alpha_big < alpha_0)
  expect_true(abs(alpha_big - SKILL_ALPHA_RUN_MIN_MENS_T20) < 0.001)
})

test_that("get_skill_alpha is monotonically decreasing", {
  alphas <- sapply(seq(0, 2000, by = 100), function(d) {
    get_skill_alpha(d, format = "t20", skill_type = "run")
  })
  # Each alpha should be >= the next one
  expect_true(all(diff(alphas) <= 0))
})

test_that("get_skill_alpha works for wicket skill type", {
  alpha_run <- get_skill_alpha(0, "t20", "run")
  alpha_wkt <- get_skill_alpha(0, "t20", "wicket")
  # Both should be positive
  expect_true(alpha_run > 0)
  expect_true(alpha_wkt > 0)
  # Run max should differ from wicket max
  expect_true(alpha_run != alpha_wkt)
})

test_that("get_skill_start_values returns named list", {
  vals <- get_skill_start_values("t20")
  expect_type(vals, "list")
  expect_named(vals, c("scoring_index", "survival_rate", "economy_index", "strike_rate"))
})

test_that("get_skill_start_values indices start at zero", {
  for (fmt in c("t20", "odi", "test")) {
    vals <- get_skill_start_values(fmt)
    expect_equal(vals$scoring_index, 0, label = paste(fmt, "scoring"))
    expect_equal(vals$economy_index, 0, label = paste(fmt, "economy"))
  }
})

test_that("get_skill_start_values survival rates are format-appropriate", {
  t20 <- get_skill_start_values("t20")
  odi <- get_skill_start_values("odi")
  test <- get_skill_start_values("test")

  # Survival rate increases with format length (Tests harder to get out)
  expect_true(t20$survival_rate < odi$survival_rate)
  expect_true(odi$survival_rate < test$survival_rate)

  # All survival rates in (0, 1)
  for (fmt in list(t20, odi, test)) {
    expect_true(fmt$survival_rate > 0 && fmt$survival_rate < 1)
  }
})

test_that("get_skill_start_values strike rates are format-appropriate", {
  t20 <- get_skill_start_values("t20")
  odi <- get_skill_start_values("odi")
  test <- get_skill_start_values("test")

  # T20 has highest wicket rate per ball (aggressive cricket)
  expect_true(t20$strike_rate > odi$strike_rate)
  expect_true(odi$strike_rate > test$strike_rate)
})

test_that("survival_rate + strike_rate ~ 1 within format", {
  # Survival rate = 1 - wicket_rate; strike_rate IS the wicket rate
  for (fmt in c("t20", "odi", "test")) {
    vals <- get_skill_start_values(fmt)
    expect_equal(vals$survival_rate + vals$strike_rate, 1.0, tolerance = 0.001,
                 label = paste(fmt, "survival + strike"))
  }
})


# ==============================================================================
# VENUE SKILL: ALPHAS AND STARTING VALUES
# ==============================================================================

test_that("get_venue_alpha returns correct values per format", {
  expect_equal(get_venue_alpha("t20"), VENUE_ALPHA_T20)
  expect_equal(get_venue_alpha("odi"), VENUE_ALPHA_ODI)
  expect_equal(get_venue_alpha("test"), VENUE_ALPHA_TEST)
})

test_that("get_venue_alpha ordering: t20 > odi > test", {
  expect_true(get_venue_alpha("t20") > get_venue_alpha("odi"))
  expect_true(get_venue_alpha("odi") > get_venue_alpha("test"))
})

test_that("venue permanent alphas are smaller than player alphas at 0 deliveries", {
  # Venues change slower than player form
  expect_true(get_venue_alpha("t20") < get_skill_alpha(0, "t20", "run"))
  expect_true(get_venue_alpha("odi") < get_skill_alpha(0, "odi", "run"))
  expect_true(get_venue_alpha("test") < get_skill_alpha(0, "test", "run"))
})

test_that("get_venue_start_values returns named list with 4 metrics", {
  vals <- get_venue_start_values("t20")
  expect_type(vals, "list")
  expect_named(vals, c("run_rate", "wicket_rate", "boundary_rate", "dot_rate"))
})

test_that("get_venue_start_values residual indices start at zero", {
  for (fmt in c("t20", "odi", "test")) {
    vals <- get_venue_start_values(fmt)
    expect_equal(vals$run_rate, 0, label = paste(fmt, "run_rate"))
    expect_equal(vals$wicket_rate, 0, label = paste(fmt, "wicket_rate"))
  }
})

test_that("get_venue_start_values boundary rates decrease with format length", {
  t20 <- get_venue_start_values("t20")
  odi <- get_venue_start_values("odi")
  test <- get_venue_start_values("test")

  # T20 has highest boundary rate (aggressive batting)
  expect_true(t20$boundary_rate > odi$boundary_rate)
  expect_true(odi$boundary_rate > test$boundary_rate)
})

test_that("get_venue_start_values dot rates increase with format length", {
  t20 <- get_venue_start_values("t20")
  odi <- get_venue_start_values("odi")
  test <- get_venue_start_values("test")

  # Tests have highest dot ball rate (defensive cricket)
  expect_true(t20$dot_rate < odi$dot_rate)
  expect_true(odi$dot_rate < test$dot_rate)
})

test_that("get_venue_min_balls returns reasonable minimums", {
  t20 <- get_venue_min_balls("t20")
  odi <- get_venue_min_balls("odi")
  test <- get_venue_min_balls("test")

  # All positive
  expect_true(t20 > 0)
  expect_true(odi > 0)
  expect_true(test > 0)

  # More data needed for longer formats
  expect_true(t20 < odi)
  expect_true(odi < test)
})


# ==============================================================================
# SKILL CONSTANTS: CONSISTENCY CHECKS
# ==============================================================================

test_that("skill alpha bounds are valid", {
  # All alphas should be in (0, 1)
  for (fmt in c("t20", "odi", "test")) {
    alpha <- get_skill_alpha(0, fmt, "run")  # Max alpha at 0 deliveries
    expect_true(alpha > 0 && alpha < 1, label = paste("player run", fmt))

    alpha_wkt <- get_skill_alpha(0, fmt, "wicket")
    expect_true(alpha_wkt > 0 && alpha_wkt < 1, label = paste("player wicket", fmt))

    v_alpha <- get_venue_alpha(fmt)
    expect_true(v_alpha > 0 && v_alpha < 1, label = paste("venue", fmt))
  }
})

test_that("dynamic skill alphas have max > min", {
  # Check player dynamic alpha params
  expect_true(SKILL_ALPHA_RUN_MAX_MENS_T20 > SKILL_ALPHA_RUN_MIN_MENS_T20)
  expect_true(SKILL_ALPHA_WICKET_MAX_MENS_T20 > SKILL_ALPHA_WICKET_MIN_MENS_T20)
  expect_true(SKILL_ALPHA_RUN_MAX_MENS_ODI > SKILL_ALPHA_RUN_MIN_MENS_ODI)
  expect_true(SKILL_ALPHA_RUN_MAX_MENS_TEST > SKILL_ALPHA_RUN_MIN_MENS_TEST)

  # Check womens too
  expect_true(SKILL_ALPHA_RUN_MAX_WOMENS_T20 > SKILL_ALPHA_RUN_MIN_WOMENS_T20)
  expect_true(SKILL_ALPHA_RUN_MAX_WOMENS_ODI > SKILL_ALPHA_RUN_MIN_WOMENS_ODI)
  expect_true(SKILL_ALPHA_RUN_MAX_WOMENS_TEST > SKILL_ALPHA_RUN_MIN_WOMENS_TEST)
})

test_that("dynamic skill alpha halflife values are positive", {
  expect_true(SKILL_ALPHA_RUN_HALFLIFE_MENS_T20 > 0)
  expect_true(SKILL_ALPHA_RUN_HALFLIFE_MENS_ODI > 0)
  expect_true(SKILL_ALPHA_RUN_HALFLIFE_MENS_TEST > 0)
  expect_true(SKILL_ALPHA_WICKET_HALFLIFE_MENS_T20 > 0)
  expect_true(SKILL_ALPHA_WICKET_HALFLIFE_MENS_ODI > 0)
  expect_true(SKILL_ALPHA_WICKET_HALFLIFE_MENS_TEST > 0)
})

test_that("skill index bounds are symmetric around zero", {
  expect_equal(SKILL_INDEX_RUN_MAX, -SKILL_INDEX_RUN_MIN)
  expect_equal(SKILL_INDEX_WICKET_MAX, -SKILL_INDEX_WICKET_MIN)
  expect_equal(SKILL_VENUE_RUN_MAX, -SKILL_VENUE_RUN_MIN)
  expect_equal(SKILL_VENUE_WICKET_MAX, -SKILL_VENUE_WICKET_MIN)
})

test_that("skill attribution weights sum to 1", {
  # Mens T20 run weights
  run_sum <- SKILL_W_BATTER_RUN_MENS_T20 + SKILL_W_BOWLER_RUN_MENS_T20 +
    SKILL_W_VENUE_SESSION_RUN_MENS_T20 + SKILL_W_VENUE_PERM_RUN_MENS_T20
  expect_equal(run_sum, 1.0, tolerance = 0.001)

  # Mens T20 wicket weights
  wkt_sum <- SKILL_W_BATTER_WICKET_MENS_T20 + SKILL_W_BOWLER_WICKET_MENS_T20 +
    SKILL_W_VENUE_SESSION_WICKET_MENS_T20 + SKILL_W_VENUE_PERM_WICKET_MENS_T20
  expect_equal(wkt_sum, 1.0, tolerance = 0.001)
})

test_that("skill attribution weights sum to 1 for all format-gender combos", {
  formats <- c("MENS_T20", "MENS_ODI", "MENS_TEST",
               "WOMENS_T20", "WOMENS_ODI", "WOMENS_TEST")

  for (fg in formats) {
    run_sum <- get(paste0("SKILL_W_BATTER_RUN_", fg)) +
      get(paste0("SKILL_W_BOWLER_RUN_", fg)) +
      get(paste0("SKILL_W_VENUE_SESSION_RUN_", fg)) +
      get(paste0("SKILL_W_VENUE_PERM_RUN_", fg))
    expect_equal(run_sum, 1.0, tolerance = 0.001, label = paste(fg, "run weights"))

    wkt_sum <- get(paste0("SKILL_W_BATTER_WICKET_", fg)) +
      get(paste0("SKILL_W_BOWLER_WICKET_", fg)) +
      get(paste0("SKILL_W_VENUE_SESSION_WICKET_", fg)) +
      get(paste0("SKILL_W_VENUE_PERM_WICKET_", fg))
    expect_equal(wkt_sum, 1.0, tolerance = 0.001, label = paste(fg, "wicket weights"))
  }
})


# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================

test_that("get_skill_table_name constructs correct names", {
  expect_equal(get_skill_table_name("t20", "player_skill"), "t20_player_skill")
  expect_equal(get_skill_table_name("odi", "team_skill"), "odi_team_skill")
  expect_equal(get_skill_table_name("test", "venue_skill"), "test_venue_skill")
})

test_that("get_skill_table_name normalizes format", {
  expect_equal(get_skill_table_name("T20", "player_skill"), "t20_player_skill")
  expect_equal(get_skill_table_name("ODI", "team_skill"), "odi_team_skill")
  expect_equal(get_skill_table_name("Test", "venue_skill"), "test_venue_skill")
})

test_that("escape_sql_quotes escapes single quotes", {
  expect_equal(escape_sql_quotes("O'Brien"), "O''Brien")
  expect_equal(escape_sql_quotes("no quotes"), "no quotes")
  expect_equal(escape_sql_quotes("it's a 'test'"), "it''s a ''test''")
})

test_that("escape_sql_quotes handles vectors", {
  input <- c("Smith", "O'Brien", "D'Arcy")
  result <- escape_sql_quotes(input)
  expect_length(result, 3)
  expect_equal(result[2], "O''Brien")
  expect_equal(result[3], "D''Arcy")
})

test_that("batch_skill_query returns directly for small batches", {
  # If ids <= batch_size, should call query_fn once
  call_count <- 0
  mock_fn <- function(ids) {
    call_count <<- call_count + 1
    data.frame(id = ids, value = 1)
  }
  result <- batch_skill_query(c("a", "b", "c"), mock_fn, batch_size = 10, verbose = FALSE)
  expect_equal(call_count, 1)
  expect_equal(nrow(result), 3)
})

test_that("batch_skill_query splits into batches for large inputs", {
  call_count <- 0
  mock_fn <- function(ids) {
    call_count <<- call_count + 1
    data.frame(id = ids, value = seq_along(ids))
  }
  ids <- paste0("id_", 1:25)
  result <- batch_skill_query(ids, mock_fn, batch_size = 10, verbose = FALSE)
  expect_equal(call_count, 3)  # 10 + 10 + 5
  expect_equal(nrow(result), 25)
})


# ==============================================================================
# VENUE: NORMALIZATION (pure function edge cases)
# ==============================================================================

test_that("venue start values have all rates in valid range", {
  for (fmt in c("t20", "odi", "test")) {
    vals <- get_venue_start_values(fmt)
    # Boundary rate should be in (0, 1)
    expect_true(vals$boundary_rate > 0 && vals$boundary_rate < 1,
                label = paste(fmt, "boundary_rate"))
    # Dot rate should be in (0, 1)
    expect_true(vals$dot_rate > 0 && vals$dot_rate < 1,
                label = paste(fmt, "dot_rate"))
    # boundary + dot < 1 (they're independent per-ball probabilities)
    expect_true(vals$boundary_rate + vals$dot_rate < 1,
                label = paste(fmt, "boundary + dot < 1"))
  }
})


# ==============================================================================
# VENUE CONSTANTS: CROSS-FORMAT CONSISTENCY
# ==============================================================================

test_that("venue start run rates match EXPECTED_RUNS constants", {
  # Run rates should match the package-level expected runs per ball
  expect_equal(VENUE_START_RUN_RATE_T20, 1.138, tolerance = 0.001)
  expect_equal(VENUE_START_RUN_RATE_ODI, 0.782, tolerance = 0.001)
  expect_equal(VENUE_START_RUN_RATE_TEST, 0.518, tolerance = 0.001)
})

test_that("venue start wicket rates match survival complement", {
  # Wicket rate should = 1 - survival rate (approximately)
  expect_equal(VENUE_START_WICKET_RATE_T20, 1 - SKILL_START_SURVIVAL_T20,
               tolerance = 0.001)
  expect_equal(VENUE_START_WICKET_RATE_ODI, 1 - SKILL_START_SURVIVAL_ODI,
               tolerance = 0.001)
  expect_equal(VENUE_START_WICKET_RATE_TEST, 1 - SKILL_START_SURVIVAL_TEST,
               tolerance = 0.001)
})

test_that("skill decay rates are positive and decrease with format length", {
  expect_true(SKILL_DECAY_T20 > 0)
  expect_true(SKILL_DECAY_ODI > 0)
  expect_true(SKILL_DECAY_TEST > 0)
  expect_true(SKILL_DECAY_T20 > SKILL_DECAY_ODI)
  expect_true(SKILL_DECAY_ODI > SKILL_DECAY_TEST)
})


# ==============================================================================
# SKILL INDICES: DECAY AND BOUNDS (skill_indices.R)
# ==============================================================================

test_that("apply_skill_decay pulls skill toward zero", {
  # Positive skill decays toward zero
  expect_true(apply_skill_decay(0.3) < 0.3)
  expect_true(apply_skill_decay(0.3) > 0)

  # Negative skill also decays toward zero
  expect_true(apply_skill_decay(-0.3) > -0.3)
  expect_true(apply_skill_decay(-0.3) < 0)

  # Zero stays at zero
  expect_equal(apply_skill_decay(0), 0)
})

test_that("apply_skill_decay uses correct formula", {
  skill <- 0.5
  decay <- 0.001
  expect_equal(apply_skill_decay(skill, decay), skill * (1 - decay))
})

test_that("bound_skill clips player run skills to valid range", {
  expect_equal(bound_skill(0.6, "run", "player"), SKILL_INDEX_RUN_MAX)
  expect_equal(bound_skill(-0.6, "run", "player"), SKILL_INDEX_RUN_MIN)
  expect_equal(bound_skill(0.1, "run", "player"), 0.1)  # Within bounds
})

test_that("bound_skill clips player wicket skills to valid range", {
  expect_equal(bound_skill(0.1, "wicket", "player"), SKILL_INDEX_WICKET_MAX)
  expect_equal(bound_skill(-0.1, "wicket", "player"), SKILL_INDEX_WICKET_MIN)
  expect_equal(bound_skill(0.01, "wicket", "player"), 0.01)  # Within bounds
})

test_that("bound_skill clips venue skills to narrower range than player", {
  # Venue bounds should be tighter
  expect_true(SKILL_VENUE_RUN_MAX < SKILL_INDEX_RUN_MAX)
  expect_equal(bound_skill(0.4, "run", "venue"), SKILL_VENUE_RUN_MAX)
  expect_equal(bound_skill(-0.4, "run", "venue"), SKILL_VENUE_RUN_MIN)
})

test_that("bound_skill is vectorized", {
  input <- c(-1, -0.1, 0, 0.1, 1)
  result <- bound_skill(input, "run", "player")
  expect_length(result, 5)
  expect_equal(result[1], SKILL_INDEX_RUN_MIN)
  expect_equal(result[5], SKILL_INDEX_RUN_MAX)
  expect_equal(result[3], 0)
})


# ==============================================================================
# SKILL INDICES: FULL UPDATE (update_run_skills, update_wicket_skills)
# ==============================================================================

test_that("update_run_skills returns named list with 4 entities", {
  result <- update_run_skills(
    actual_runs = 1.0, expected_runs = 1.0,
    alpha_batter = 0.03, alpha_bowler = 0.03,
    alpha_venue_perm = 0.002, alpha_venue_session = 0.05,
    old_batter_skill = 0, old_bowler_skill = 0,
    old_venue_perm_skill = 0, old_venue_session_skill = 0
  )
  expect_type(result, "list")
  expect_named(result, c("new_batter", "new_bowler", "new_venue_perm", "new_venue_session"))
})

test_that("update_run_skills: zero residual still decays", {
  # When actual = expected, residual = 0, but decay still applies
  result <- update_run_skills(
    actual_runs = 1.0, expected_runs = 1.0,
    alpha_batter = 0.03, alpha_bowler = 0.03,
    alpha_venue_perm = 0.002, alpha_venue_session = 0.05,
    old_batter_skill = 0.1, old_bowler_skill = 0.1,
    old_venue_perm_skill = 0.1, old_venue_session_skill = 0.1
  )
  # All skills should decay toward 0
  expect_true(result$new_batter < 0.1)
  expect_true(result$new_bowler < 0.1)
  expect_true(result$new_venue_perm < 0.1)
  expect_true(result$new_venue_session < 0.1)
})

test_that("update_run_skills: positive residual benefits batter, hurts bowler", {
  # Batter scores 4 when expected 1 => residual = +3
  result <- update_run_skills(
    actual_runs = 4.0, expected_runs = 1.0,
    alpha_batter = 0.03, alpha_bowler = 0.03,
    alpha_venue_perm = 0.002, alpha_venue_session = 0.05,
    old_batter_skill = 0, old_bowler_skill = 0,
    old_venue_perm_skill = 0, old_venue_session_skill = 0
  )
  expect_true(result$new_batter > 0)    # Batter improves
  expect_true(result$new_bowler < 0)    # Bowler gets worse (conceded runs)
})

test_that("update_wicket_skills returns named list with 4 entities", {
  result <- update_wicket_skills(
    actual_wicket = 0, expected_wicket = 0.05,
    alpha_batter = 0.02, alpha_bowler = 0.02,
    alpha_venue_perm = 0.001, alpha_venue_session = 0.03,
    old_batter_skill = 0, old_bowler_skill = 0,
    old_venue_perm_skill = 0, old_venue_session_skill = 0
  )
  expect_type(result, "list")
  expect_named(result, c("new_batter", "new_bowler", "new_venue_perm", "new_venue_session"))
})

test_that("update_wicket_skills: wicket fell when unexpected -> bowler benefits", {
  # Wicket fell (actual=1) when expected was low (0.02) -> positive residual
  result <- update_wicket_skills(
    actual_wicket = 1, expected_wicket = 0.02,
    alpha_batter = 0.02, alpha_bowler = 0.02,
    alpha_venue_perm = 0.001, alpha_venue_session = 0.03,
    old_batter_skill = 0, old_bowler_skill = 0,
    old_venue_perm_skill = 0, old_venue_session_skill = 0
  )
  # Positive residual: batter wicket skill increases (gets out more = bad),
  # bowler wicket skill increases (takes more wickets = good)
  expect_true(result$new_batter > 0)
  expect_true(result$new_bowler > 0)
})


# ==============================================================================
# SKILL INDICES: HELPER FUNCTIONS (constants_skill.R)
# ==============================================================================

test_that("get_skill_alpha_params returns valid parameters", {
  params <- get_skill_alpha_params("t20", "male", "run")
  expect_type(params, "list")
  expect_true(all(c("alpha_max", "alpha_min", "halflife") %in% names(params)))
  expect_true(params$alpha_max > params$alpha_min)
  expect_true(params$halflife > 0)
})

test_that("get_skill_alpha_params works for all format-gender combos", {
  for (fmt in c("t20", "odi", "test")) {
    for (gender in c("male", "female")) {
      for (skill in c("run", "wicket")) {
        params <- get_skill_alpha_params(fmt, gender, skill)
        expect_true(params$alpha_max > 0, label = paste(fmt, gender, skill, "max"))
        expect_true(params$alpha_min > 0, label = paste(fmt, gender, skill, "min"))
        expect_true(params$alpha_max > params$alpha_min, label = paste(fmt, gender, skill))
      }
    }
  }
})

test_that("get_skill_weights returns 4 weights", {
  w <- get_skill_weights("t20", "male", "run")
  expect_type(w, "list")
  expect_true(all(c("w_batter", "w_bowler", "w_venue_session", "w_venue_perm") %in% names(w)))
})

test_that("get_skill_decay returns format-appropriate values", {
  expect_equal(get_skill_decay("t20"), SKILL_DECAY_T20)
  expect_equal(get_skill_decay("odi"), SKILL_DECAY_ODI)
  expect_equal(get_skill_decay("test"), SKILL_DECAY_TEST)
})

test_that("get_skill_decay handles case variants", {
  expect_equal(get_skill_decay("T20"), SKILL_DECAY_T20)
  expect_equal(get_skill_decay("IT20"), SKILL_DECAY_T20)
  expect_equal(get_skill_decay("ODI"), SKILL_DECAY_ODI)
})


# ==============================================================================
# SKILL INDICES: VENUE ALPHA FUNCTIONS (skill_indices.R)
# ==============================================================================

test_that("get_venue_perm_skill_alpha decays with ball count", {
  alpha_0 <- get_venue_perm_skill_alpha(0, "t20")
  alpha_mid <- get_venue_perm_skill_alpha(5000, "t20")
  alpha_high <- get_venue_perm_skill_alpha(50000, "t20")

  expect_true(alpha_0 > alpha_mid)
  expect_true(alpha_mid > alpha_high)
  # Doesn't decay below 20% of base
  expect_true(alpha_high >= SKILL_VENUE_ALPHA_PERM_T20 * 0.2)
})

test_that("get_venue_session_skill_alpha decays within match", {
  alpha_start <- get_venue_session_skill_alpha(0)
  alpha_mid <- get_venue_session_skill_alpha(100)
  alpha_late <- get_venue_session_skill_alpha(500)

  # Starts high, decays
  expect_equal(alpha_start, SKILL_VENUE_ALPHA_SESSION_MAX)
  expect_true(alpha_start > alpha_mid)
  expect_true(alpha_mid > alpha_late)
  # Approaches min
  expect_true(alpha_late > SKILL_VENUE_ALPHA_SESSION_MIN)
})
