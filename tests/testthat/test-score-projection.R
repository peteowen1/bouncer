# Tests for Score Projection Functions

test_that("calculate_projection_resource returns 1 at start of innings", {
  # T20: 10 wickets, 120 balls (note: parameter order is wickets_remaining, balls_remaining)
  resource <- calculate_projection_resource(
    wickets_remaining = 10,
    balls_remaining = 120,
    format = "t20"
  )
  expect_equal(resource, 1, tolerance = 0.001)

  # ODI: 10 wickets, 300 balls
  resource <- calculate_projection_resource(
    wickets_remaining = 10,
    balls_remaining = 300,
    format = "odi"
  )
  expect_equal(resource, 1, tolerance = 0.001)
})

test_that("calculate_projection_resource returns 0 when no balls left", {
  resource <- calculate_projection_resource(
    wickets_remaining = 10,
    balls_remaining = 0,
    format = "t20"
  )
  expect_equal(resource, 0)

  resource <- calculate_projection_resource(
    wickets_remaining = 5,
    balls_remaining = 0,
    format = "odi"
  )
  expect_equal(resource, 0)
})

test_that("calculate_projection_resource returns 0 when no wickets left", {
  resource <- calculate_projection_resource(
    wickets_remaining = 0,
    balls_remaining = 60,
    format = "t20"
  )
  expect_equal(resource, 0)

  resource <- calculate_projection_resource(
    wickets_remaining = 0,
    balls_remaining = 120,
    format = "odi"
  )
  expect_equal(resource, 0)
})

test_that("calculate_projection_resource decreases as balls decrease", {
  resources <- sapply(c(120, 90, 60, 30, 0), function(b) {
    calculate_projection_resource(
      wickets_remaining = 10,
      balls_remaining = b,
      format = "t20"
    )
  })
  expect_true(all(diff(resources) <= 0))
})

test_that("calculate_projection_resource decreases as wickets decrease", {
  resources <- sapply(10:0, function(w) {
    calculate_projection_resource(
      wickets_remaining = w,
      balls_remaining = 60,
      format = "t20"
    )
  })
  expect_true(all(diff(resources) <= 0))
})

test_that("calculate_projection_resource handles custom parameters", {
  # With z=1, y=1, should get simple linear product
  # 5 wickets remaining, 60 balls remaining
  resource <- calculate_projection_resource(
    wickets_remaining = 5,
    balls_remaining = 60,
    format = "t20",
    z = 1,
    y = 1
  )
  expected <- (60/120) * (5/10)  # balls_pct * wickets_pct
  expect_equal(resource, expected, tolerance = 0.001)
})

test_that("get_agnostic_expected_score returns correct values", {
  # T20 male international
  eis <- get_agnostic_expected_score("t20", "male", "international")
  expect_equal(eis, EIS_T20_MALE_INTL)

  # T20 female club
  eis <- get_agnostic_expected_score("t20", "female", "club")
  expect_equal(eis, EIS_T20_FEMALE_CLUB)

  # ODI male
  eis <- get_agnostic_expected_score("odi", "male", "international")
  expect_equal(eis, EIS_ODI_MALE_INTL)

  # Test female
  eis <- get_agnostic_expected_score("test", "female", "international")
  expect_equal(eis, EIS_TEST_FEMALE_INTL)
})

test_that("get_agnostic_expected_score handles format aliases", {
  expect_equal(
    get_agnostic_expected_score("IT20", "male", "international"),
    get_agnostic_expected_score("t20", "male", "international")
  )
  expect_equal(
    get_agnostic_expected_score("ODM", "male", "international"),
    get_agnostic_expected_score("odi", "male", "international")
  )
})

test_that("calculate_projected_score returns EIS at start of innings", {
  eis <- get_agnostic_expected_score("t20", "male", "international")

  # At very start: cs=0, wickets fallen=0, overs bowled=0
  # New interface: wickets = wickets fallen, overs = overs bowled
  projected <- calculate_projected_score(
    current_score = 0,
    wickets = 0,
    overs = 0,
    format = "t20"
  )

  # Should be close to EIS
  expect_equal(projected, eis, tolerance = 1)
})

test_that("calculate_projected_score returns current score at end", {
  # All wickets down (10 wickets fallen, 15 overs bowled = 90 balls, 30 remaining)
  projected <- calculate_projected_score(
    current_score = 145,
    wickets = 10,    # All out
    overs = 15,      # 90 balls bowled
    format = "t20"
  )
  expect_equal(projected, 145)

  # No balls remaining (20 overs = 120 balls = all used)
  projected <- calculate_projected_score(
    current_score = 180,
    wickets = 6,     # 6 wickets fallen
    overs = 20,      # All overs bowled
    format = "t20"
  )
  expect_equal(projected, 180)
})

test_that("calculate_projected_score increases with more runs", {
  # Same game state (2 wickets fallen, 10 overs bowled), different scores
  # Old: 60 balls remaining = 10 overs bowled; 8 wickets remaining = 2 fallen
  proj1 <- calculate_projected_score(
    current_score = 50,
    wickets = 2,
    overs = 10,
    format = "t20"
  )
  proj2 <- calculate_projected_score(
    current_score = 80,
    wickets = 2,
    overs = 10,
    format = "t20"
  )

  expect_true(proj2 > proj1)
})

test_that("calculate_projected_score decreases with more wickets fallen", {
  # Same score and overs, different wickets fallen
  # Old: 8 wickets remaining = 2 fallen; 4 wickets remaining = 6 fallen
  proj1 <- calculate_projected_score(
    current_score = 80,
    wickets = 2,     # 2 wickets fallen (8 remaining)
    overs = 10,
    format = "t20"
  )
  proj2 <- calculate_projected_score(
    current_score = 80,
    wickets = 6,     # 6 wickets fallen (4 remaining)
    overs = 10,
    format = "t20"
  )

  # More wickets fallen = lower projection
  expect_true(proj2 < proj1)
})

test_that("calculate_projected_score respects bounds", {
  # Very low projection should be bounded (all out, all overs done)
  projected <- calculate_projected_score(
    current_score = 10,
    wickets = 10,    # All out
    overs = 20,      # All overs bowled
    format = "t20"
  )
  expect_true(projected >= PROJ_MIN_SCORE_T20 || projected == 10)

  # Projected should be at least current score
  # 5 wickets fallen, 10 overs bowled (60 balls remaining)
  projected <- calculate_projected_score(
    current_score = 100,
    wickets = 5,
    overs = 10,
    format = "t20"
  )
  expect_true(projected >= 100)
})

test_that("calculate_projected_score handles all formats", {
  formats <- c("t20", "odi", "test")

  for (fmt in formats) {
    max_overs <- switch(fmt,
      "t20" = 20,
      "odi" = 50,
      "test" = 90
    )

    # Test at halfway point (half overs bowled, 3 wickets fallen)
    projected <- calculate_projected_score(
      current_score = 50,
      wickets = 3,               # 3 wickets fallen (7 remaining)
      overs = max_overs / 2,     # Half overs bowled
      format = fmt
    )

    expect_true(is.numeric(projected))
    expect_true(projected >= 50)
  }
})

test_that("load_projection_params returns defaults when no file exists", {
  params <- load_projection_params("t20", "male", "international",
                                   params_dir = tempdir())

  expect_true(is.list(params))
  expect_equal(params$a, PROJ_DEFAULT_A)
  expect_equal(params$b, PROJ_DEFAULT_B)
  expect_equal(params$z, PROJ_DEFAULT_Z)
  expect_equal(params$y, PROJ_DEFAULT_Y)
  expect_true(!is.null(params$eis_agnostic))
})

test_that("calculate_full_expected_score adjusts for team skills", {
  baseline <- get_agnostic_expected_score("t20", "male", "international")

  # Strong batting team
  eis_strong <- calculate_full_expected_score(
    batting_team_skills = list(runs_skill = 0.2),
    bowling_team_skills = list(runs_skill = 0),
    format = "t20"
  )

  # Weak batting team
  eis_weak <- calculate_full_expected_score(
    batting_team_skills = list(runs_skill = -0.2),
    bowling_team_skills = list(runs_skill = 0),
    format = "t20"
  )

  expect_true(eis_strong > baseline)
  expect_true(eis_weak < baseline)
})

test_that("calculate_projection_change computes correct change", {
  result <- calculate_projection_change(
    projected_before = 150,
    projected_after = 156,
    runs_scored = 6,
    is_wicket = FALSE
  )

  expect_equal(result$projection_change, 6)
  expect_equal(result$runs_scored, 6)
  expect_false(result$is_wicket)
})

test_that("calculate_projection_change handles wickets", {
  result <- calculate_projection_change(
    projected_before = 160,
    projected_after = 148,
    runs_scored = 0,
    is_wicket = TRUE
  )

  expect_equal(result$projection_change, -12)
  expect_true(result$is_wicket)
})

test_that("calculate_projected_scores_vectorized matches scalar version", {
  # Test multiple deliveries
  # Internal representation (used by vectorized function)
  current_scores <- c(0, 50, 100)
  balls_remaining <- c(120, 60, 30)
  wickets_remaining <- c(10, 7, 4)
  eis <- rep(160, 3)

  # Vectorized function uses internal representation
  vectorized <- calculate_projected_scores_vectorized(
    current_scores, wickets_remaining, balls_remaining, eis,
    a = PROJ_DEFAULT_A, b = PROJ_DEFAULT_B,
    z = PROJ_DEFAULT_Z, y = PROJ_DEFAULT_Y,
    max_balls = 120
  )

  # Scalar function uses public interface (wickets fallen, overs bowled)
  # Convert: wickets = 10 - wickets_remaining, overs = (120 - balls_remaining) / 6
  scalar <- sapply(1:3, function(i) {
    wickets_fallen <- 10 - wickets_remaining[i]
    overs_bowled <- (120 - balls_remaining[i]) / 6
    calculate_projected_score(
      current_score = current_scores[i],
      wickets = wickets_fallen,
      overs = overs_bowled,
      expected_initial_score = eis[i],
      format = "t20",
      apply_bounds = FALSE
    )
  })

  expect_equal(vectorized, scalar, tolerance = 0.01)
})

test_that("get_projection_segment_id constructs correct IDs", {
  expect_equal(
    get_projection_segment_id("t20", "male", "international"),
    "t20_male_international"
  )
  expect_equal(
    get_projection_segment_id("ODI", "Female", "club"),
    "odi_female_club"
  )
  expect_equal(
    get_projection_segment_id("IT20", "male", "intl"),
    "t20_male_international"
  )
})

# Test match specific tests
test_that("get_test_overs_per_day returns correct values", {
  expect_equal(get_test_overs_per_day(5), TEST_OVERS_PER_DAY_5DAY)
  expect_equal(get_test_overs_per_day(4), TEST_OVERS_PER_DAY_4DAY)
})

test_that("estimate_test_total_overs calculates correctly", {
  expect_equal(estimate_test_total_overs(5), 5 * 90)
  expect_equal(estimate_test_total_overs(4), 4 * 98)
})

test_that("calculate_test_overs_remaining works for day 1", {
  # Start of day 1
  overs_remaining <- calculate_test_overs_remaining(
    match_days = 5,
    day_number = 1,
    overs_bowled_today = 0
  )
  expect_equal(overs_remaining, 450)

  # After 45 overs on day 1
  overs_remaining <- calculate_test_overs_remaining(
    match_days = 5,
    day_number = 1,
    overs_bowled_today = 45
  )
  expect_equal(overs_remaining, 405)
})

test_that("calculate_test_overs_remaining works for later days", {
  # Day 3, 30 overs bowled
  overs_remaining <- calculate_test_overs_remaining(
    match_days = 5,
    day_number = 3,
    overs_bowled_today = 30
  )
  # Days 1 and 2 complete (180 overs) + 30 today = 210 bowled
  # 450 - 210 = 240 remaining
  expect_equal(overs_remaining, 240)
})

test_that("test_overs_to_balls converts correctly", {
  # Cricket notation
  expect_equal(test_overs_to_balls(45), 270)
  expect_equal(test_overs_to_balls(45.3), 273)

  # Edge cases
  expect_equal(test_overs_to_balls(0), 0)
})
