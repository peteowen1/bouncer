# Tests for Simulation Functions

# ============================================================================
# elo_win_probability() tests
# ============================================================================

test_that("elo_win_probability returns valid probabilities", {
  # Equal ratings should give 0.5
  expect_equal(elo_win_probability(1500, 1500), 0.5)

  # Higher rating should give higher probability
  expect_gt(elo_win_probability(1600, 1400), 0.5)
  expect_lt(elo_win_probability(1400, 1600), 0.5)

  # Results should be between 0 and 1
  expect_gte(elo_win_probability(2000, 1000), 0)
  expect_lte(elo_win_probability(2000, 1000), 1)
  expect_gte(elo_win_probability(1000, 2000), 0)
  expect_lte(elo_win_probability(1000, 2000), 1)
})

test_that("elo_win_probability is symmetric", {
  # P(A beats B) + P(B beats A) should equal 1
  prob_ab <- elo_win_probability(1600, 1400)
  prob_ba <- elo_win_probability(1400, 1600)

  expect_equal(prob_ab + prob_ba, 1, tolerance = 0.0001)
})

test_that("elo_win_probability handles extreme differences", {
  # Very large difference should approach but not reach 1
  high_prob <- elo_win_probability(2500, 1000)

  expect_lt(high_prob, 1)
  expect_gt(high_prob, 0.99)
})

test_that("elo_win_probability handles custom divisor", {
  # Custom divisor should change the spread
  prob_default <- elo_win_probability(1600, 1400, divisor = 400)
  prob_narrow <- elo_win_probability(1600, 1400, divisor = 200)

  # Narrower divisor should give more extreme probability

  expect_gt(prob_narrow, prob_default)
})

# ============================================================================
# simulate_match_outcome() tests
# ============================================================================

test_that("simulate_match_outcome returns valid structure", {
  set.seed(42)
  result <- simulate_match_outcome(0.6, "Team A", "Team B")

  expect_type(result, "list")
  expect_true("winner" %in% names(result))
  expect_true("loser" %in% names(result))
  expect_true("margin" %in% names(result))
  expect_true("team1_won" %in% names(result))

  # Winner should be one of the teams
  expect_true(result$winner %in% c("Team A", "Team B"))

  # Loser should be the other team
  expect_true(result$loser %in% c("Team A", "Team B"))
  expect_false(result$winner == result$loser)
})

test_that("simulate_match_outcome respects probability", {
  set.seed(42)

  # With very high probability, team1 should win most of the time
  n_sims <- 1000
  team1_wins <- 0

  for (i in 1:n_sims) {
    result <- simulate_match_outcome(0.99, "Team A", "Team B")
    if (result$team1_won) team1_wins <- team1_wins + 1
  }

  # Should win around 99% of simulations (allow some variance)
  expect_gt(team1_wins / n_sims, 0.95)
})

# ============================================================================
# simulate_season() tests
# ============================================================================

test_that("simulate_season returns valid standings", {
  # Create minimal fixture data
  fixtures <- data.frame(
    team1 = c("A", "A", "B"),
    team2 = c("B", "C", "C"),
    team1_win_prob = c(0.6, 0.5, 0.4),
    stringsAsFactors = FALSE
  )

  set.seed(42)
  result <- simulate_season(fixtures)

  expect_true(is.data.frame(result))
  expect_true("team" %in% names(result))
  expect_true("wins" %in% names(result))
  expect_true("losses" %in% names(result))
  expect_true("points" %in% names(result))
  expect_true("position" %in% names(result))

  # Should have all teams
  expect_equal(sort(result$team), c("A", "B", "C"))

  # Wins + losses should equal games played
  total_games <- nrow(fixtures)
  expect_equal(sum(result$wins), total_games)
  expect_equal(sum(result$losses), total_games)

  # Positions should be 1 to n_teams
  expect_equal(sort(result$position), 1:3)
})

test_that("simulate_season handles minimal fixtures", {
  # Test with minimal valid input (1 game, 2 teams)
  fixtures <- data.frame(
    team1 = "A",
    team2 = "B",
    team1_win_prob = 0.5,
    stringsAsFactors = FALSE
  )

  set.seed(42)
  result <- simulate_season(fixtures)

  expect_true(is.data.frame(result))
  expect_equal(nrow(result), 2)  # 2 teams
  expect_equal(sum(result$wins), 1)  # 1 game total
})

# ============================================================================
# simulate_season_n() tests
# ============================================================================

test_that("simulate_season_n aggregates results correctly", {
  fixtures <- data.frame(
    team1 = c("A", "A", "B"),
    team2 = c("B", "C", "C"),
    team1_win_prob = c(0.6, 0.5, 0.4),
    stringsAsFactors = FALSE
  )

  set.seed(42)
  result <- simulate_season_n(fixtures, n_simulations = 100, progress = FALSE)

  expect_true(is.data.frame(result))
  expect_true("team" %in% names(result))
  expect_true("avg_wins" %in% names(result))
  expect_true("playoff_pct" %in% names(result))

  # All teams should be present
  expect_equal(nrow(result), 3)
})

# ============================================================================
# create_simulation_config() tests
# ============================================================================

test_that("create_simulation_config returns valid config", {
  config <- create_simulation_config(
    simulation_type = "season",
    event_name = "Test League",
    season = "2024",
    n_simulations = 1000
  )

  expect_type(config, "list")
  expect_equal(config$simulation_type, "season")
  expect_equal(config$event_name, "Test League")
  expect_equal(config$season, "2024")
  expect_equal(config$n_simulations, 1000)
  expect_true("simulation_id" %in% names(config))
  expect_true("created_at" %in% names(config))
})

# ============================================================================
# aggregate_match_results() tests
# ============================================================================

test_that("aggregate_match_results calculates correctly", {
  # Create mock results
  results <- list(
    list(team1_won = TRUE),
    list(team1_won = TRUE),
    list(team1_won = FALSE),
    list(team1_won = TRUE)
  )

  agg <- aggregate_match_results(results, "Team A", "Team B")

  expect_equal(agg$n_simulations, 4)
  expect_equal(agg$team1_wins, 3)
  expect_equal(agg$team2_wins, 1)
  expect_equal(agg$team1_win_pct, 0.75)
  expect_equal(agg$team2_win_pct, 0.25)
})

# ============================================================================
# simulate_ipl_playoffs() tests
# ============================================================================

test_that("simulate_ipl_playoffs returns valid result", {
  teams <- data.frame(
    team = c("A", "B", "C", "D"),
    elo = c(1600, 1550, 1500, 1450),
    position = 1:4,
    stringsAsFactors = FALSE
  )

  set.seed(42)
  result <- simulate_ipl_playoffs(teams)

  expect_type(result, "list")
  expect_true("champion" %in% names(result))
  expect_true("finalist_q1" %in% names(result))
  expect_true("finalist_q2" %in% names(result))

  # Champion should be one of the teams
  expect_true(result$champion %in% teams$team)
})

# ============================================================================
# Reproducibility tests
# ============================================================================

test_that("simulations are reproducible with seed", {
  fixtures <- data.frame(
    team1 = c("A", "B"),
    team2 = c("B", "C"),
    team1_win_prob = c(0.6, 0.4),
    stringsAsFactors = FALSE
  )

  set.seed(123)
  result1 <- simulate_season(fixtures)

  set.seed(123)
  result2 <- simulate_season(fixtures)

  expect_identical(result1, result2)
})
