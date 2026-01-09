# Tests for Team Prediction Functions

test_that("calculate_roster_elo returns expected structure", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  # Use a small test roster
  roster <- c("V Kohli", "R Sharma")

  result <- calculate_roster_elo(roster, match_type = "t20")

  expect_type(result, "list")
  expect_true("team_batting_elo" %in% names(result))
  expect_true("team_bowling_elo" %in% names(result))
  expect_true("combined_elo" %in% names(result))
  expect_true("player_details" %in% names(result))

  expect_type(result$team_batting_elo, "double")
  expect_type(result$team_bowling_elo, "double")
  expect_s3_class(result$player_details, "data.frame")
})

test_that("calculate_roster_elo errors on empty roster", {
  expect_error(calculate_roster_elo(character(0)))
})

test_that("calculate_roster_elo respects weights", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  roster <- c("V Kohli")

  # All batting weight
  result_batting <- calculate_roster_elo(
    roster,
    match_type = "t20",
    weights = list(batting = 1, bowling = 0)
  )

  # All bowling weight
  result_bowling <- calculate_roster_elo(
    roster,
    match_type = "t20",
    weights = list(batting = 0, bowling = 1)
  )

  # Combined should equal batting ELO when weight is all batting
  expect_equal(result_batting$combined_elo, result_batting$team_batting_elo)

  # Combined should equal bowling ELO when weight is all bowling
  expect_equal(result_bowling$combined_elo, result_bowling$team_bowling_elo)
})

test_that("compare_team_rosters returns expected structure", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  team1 <- c("V Kohli")
  team2 <- c("S Smith")

  result <- compare_team_rosters(team1, team2, "India", "Australia")

  expect_type(result, "list")
  expect_true("team1_elo" %in% names(result))
  expect_true("team2_elo" %in% names(result))
  expect_true("batting_advantage" %in% names(result))
  expect_true("bowling_advantage" %in% names(result))
  expect_true("expected_win_prob" %in% names(result))

  # Win probability should be between 0 and 1
  expect_gte(result$expected_win_prob, 0)
  expect_lte(result$expected_win_prob, 1)
})

test_that("predict_roster_matchup returns valid probabilities", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  team1 <- c("V Kohli")
  team2 <- c("S Smith")

  result <- predict_roster_matchup(team1, team2, "India", "Australia")

  expect_type(result, "list")
  expect_true("team1_win_prob" %in% names(result))
  expect_true("team2_win_prob" %in% names(result))
  expect_true("predicted_winner" %in% names(result))

  # Probabilities should sum to 1
  expect_equal(result$team1_win_prob + result$team2_win_prob, 1, tolerance = 0.001)

  # Probabilities should be valid
  expect_gte(result$team1_win_prob, 0)
  expect_lte(result$team1_win_prob, 1)
})

test_that("predict_innings_score returns valid projections", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  batters <- c("V Kohli")
  bowlers <- c("J Bumrah")

  result <- predict_innings_score(
    batters, bowlers,
    current_score = 50, current_wickets = 2, current_overs = 6
  )

  expect_type(result, "list")
  expect_true("projected_score" %in% names(result))
  expect_true("projected_range" %in% names(result))
  expect_true("current_run_rate" %in% names(result))

  # Projected score should be at least current score
  expect_gte(result$projected_score, result$current_score)

  # Range should be valid
  expect_true(result$projected_range["low"] <= result$projected_score)
  expect_true(result$projected_range["high"] >= result$projected_score)
})

test_that("predict_innings_score handles start of innings", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  batters <- c("V Kohli")
  bowlers <- c("J Bumrah")

  # Start of innings
  result <- predict_innings_score(
    batters, bowlers,
    current_score = 0, current_wickets = 0, current_overs = 0
  )

  expect_type(result, "list")
  expect_gt(result$projected_score, 0)  # Should project some runs
})

test_that("simulate_match returns expected structure", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  team1 <- c("V Kohli")
  team2 <- c("S Smith")

  # Run with small number of simulations for speed
  result <- simulate_match(team1, team2, "India", "Australia", n_simulations = 100)

  expect_type(result, "list")
  expect_true("team1_win_pct" %in% names(result))
  expect_true("team2_win_pct" %in% names(result))
  expect_true("tie_pct" %in% names(result))
  expect_true("confidence_interval" %in% names(result))

  # Percentages should sum to 100
  total_pct <- result$team1_win_pct + result$team2_win_pct + result$tie_pct
  expect_equal(total_pct, 100, tolerance = 0.1)
})

test_that("simulate_match confidence interval is valid", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  team1 <- c("V Kohli")
  team2 <- c("S Smith")

  result <- simulate_match(team1, team2, n_simulations = 100)

  # CI should be between 0 and 100
  expect_gte(result$confidence_interval["low"], 0)
  expect_lte(result$confidence_interval["high"], 100)

  # Low should be less than or equal to high
  expect_lte(result$confidence_interval["low"], result$confidence_interval["high"])
})

test_that("team functions handle different match types", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  roster <- c("V Kohli")

  # Should not error for different match types
  expect_no_error(calculate_roster_elo(roster, match_type = "t20"))
  expect_no_error(calculate_roster_elo(roster, match_type = "odi"))
  expect_no_error(calculate_roster_elo(roster, match_type = "test"))
})
