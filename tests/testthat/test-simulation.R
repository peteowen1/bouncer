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
# simulate_delivery() expected-mode wicket tests
# ============================================================================

test_that("simulate_delivery expected mode draws wickets stochastically from exp_wicket", {
  # Regression: is_wicket used to be `exp_wicket > 0.5`, which is virtually
  # always FALSE since ball-level wicket probabilities are ~0.02-0.05 - so
  # innings in "expected" mode never ended on a wicket. Mock a deliberately
  # high P(wicket) = 0.3 and confirm the empirical wicket rate tracks it,
  # rather than being permanently FALSE.
  fixed_probs <- matrix(c(0.3, 0.3, 0.2, 0.1, 0.05, 0.03, 0.02), nrow = 1)
  local_mocked_bindings(
    predict_full_outcome = function(model, delivery_data, format) fixed_probs,
    .package = "bouncer"
  )

  match_state <- list(format = "t20", innings = 1, over = 5, ball = 3,
                       wickets_fallen = 1, runs_scored = 42)
  player <- list(batter_scoring_index = 1.25, batter_survival_rate = 0.975,
                 bowler_economy_index = 1.25, bowler_strike_rate = 0.025)
  team <- list(batting_team_runs_skill = 0, batting_team_wicket_skill = 0,
               bowling_team_runs_skill = 0, bowling_team_wicket_skill = 0)
  venue <- list(venue_run_rate = 0, venue_wicket_rate = 0,
                venue_boundary_rate = 0.15, venue_dot_rate = 0.35)

  set.seed(42)
  n <- 2000
  wickets <- vapply(seq_len(n), function(i) {
    simulate_delivery(model = NULL, match_state, player, team, venue, mode = "expected")$is_wicket
  }, logical(1))

  expect_gt(mean(wickets), 0.2)
  expect_lt(mean(wickets), 0.4)
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

test_that("get_simulation_seeds produces deterministic unique seeds", {
  seeds1 <- get_simulation_seeds(100, base_seed = 42)
  seeds2 <- get_simulation_seeds(100, base_seed = 42)

  # Same base seed → identical seed vectors

  expect_identical(seeds1, seeds2)

  # All seeds should be unique (no collisions)
  expect_equal(length(unique(seeds1)), 100)

  # Different base seed → different seeds
  seeds3 <- get_simulation_seeds(100, base_seed = 99)
  expect_false(identical(seeds1, seeds3))
})

test_that("set_simulation_seed produces deterministic outcomes", {
  set_simulation_seed(42)
  vals1 <- runif(10)

  set_simulation_seed(42)
  vals2 <- runif(10)

  expect_identical(vals1, vals2)
})
