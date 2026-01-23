# Tests for stat functions (player, team, venue)
# These tests require a database connection, so they skip if unavailable

skip_if_no_db <- function() {
  db_path <- tryCatch(
    get_db_path(),
    error = function(e) NULL
  )
  if (is.null(db_path) || !file.exists(db_path)) {
    skip("Database not available")
  }
}

# Helper to skip test if no data returned (empty database)
skip_if_no_data <- function(result, msg = "No data available in database") {
  if (is.null(result) || (is.data.frame(result) && nrow(result) == 0)) {
    skip(msg)
  }
}

# =============================================================================
# Player Stats Tests
# =============================================================================

test_that("player_batting_stats returns data frame for all players", {
  skip_if_no_db()

  # Use very low threshold to ensure we get some data if any exists
  result <- player_batting_stats(min_balls = 1)
  skip_if_no_data(result, "No batting data in database")

  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
  expect_true("batter_id" %in% names(result))
  expect_true("balls_faced" %in% names(result))
  expect_true("runs_scored" %in% names(result))
  expect_true("strike_rate" %in% names(result))
  expect_true("runs_per_ball" %in% names(result))
  expect_true("wickets_per_ball" %in% names(result))
})

test_that("player_batting_stats filters by match_type", {
  skip_if_no_db()

  # Try T20 first with low threshold
  t20_result <- player_batting_stats(match_type = "T20", min_balls = 1)
  skip_if_no_data(t20_result, "No T20 batting data in database")

  expect_s3_class(t20_result, "data.frame")
  expect_true(nrow(t20_result) > 0)
})

test_that("player_bowling_stats returns data frame for all players", {
  skip_if_no_db()

  result <- player_bowling_stats(min_balls = 1)
  skip_if_no_data(result, "No bowling data in database")

  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
  expect_true("bowler_id" %in% names(result))
  expect_true("balls_bowled" %in% names(result))
  expect_true("wickets" %in% names(result))
  expect_true("economy_rate" %in% names(result))
  expect_true("runs_per_ball" %in% names(result))
  expect_true("wickets_per_ball" %in% names(result))
})

# =============================================================================
# Team Stats Tests
# =============================================================================

test_that("team_batting_stats returns data frame for all teams", {
  skip_if_no_db()

  result <- team_batting_stats(min_matches = 1)
  skip_if_no_data(result, "No team batting data in database")

  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
  expect_true("team" %in% names(result))
  expect_true("matches" %in% names(result))
  expect_true("runs_scored" %in% names(result))
  expect_true("batting_average" %in% names(result))
  expect_true("strike_rate" %in% names(result))
  expect_true("runs_per_ball" %in% names(result))
  expect_true("wickets_per_ball" %in% names(result))
})

test_that("team_batting_stats returns data for single team", {
  skip_if_no_db()

  result <- team_batting_stats("India", min_matches = 1)
  skip_if_no_data(result, "No data for India in database")

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 1)
  expect_equal(result$team, "India")
})

test_that("team_batting_stats filters by match_type", {
  skip_if_no_db()

  t20_result <- team_batting_stats("India", match_type = "T20", min_matches = 1)
  skip_if_no_data(t20_result, "No T20 data for India in database")

  expect_s3_class(t20_result, "data.frame")
  expect_true(nrow(t20_result) > 0)
})

test_that("team_bowling_stats returns data frame for all teams", {
  skip_if_no_db()

  result <- team_bowling_stats(min_matches = 1)
  skip_if_no_data(result, "No team bowling data in database")

  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
  expect_true("team" %in% names(result))
  expect_true("wickets_taken" %in% names(result))
  expect_true("economy_rate" %in% names(result))
  expect_true("runs_per_ball" %in% names(result))
  expect_true("wickets_per_ball" %in% names(result))
})

test_that("team_bowling_stats returns data for single team", {
  skip_if_no_db()

  result <- team_bowling_stats("Australia", min_matches = 1)
  skip_if_no_data(result, "No bowling data for Australia in database")

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 1)
  expect_equal(result$team, "Australia")
})

# =============================================================================
# Head to Head Tests
# =============================================================================

test_that("head_to_head returns summary for two teams", {
  skip_if_no_db()

  result <- head_to_head("India", "Australia")
  skip_if_no_data(result, "No head-to-head data for India vs Australia")

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 1)
  expect_equal(result$team1, "India")
  expect_equal(result$team2, "Australia")
  expect_true(result$matches > 0)
  expect_true(result$team1_wins >= 0)
  expect_true(result$team2_wins >= 0)
})

test_that("head_to_head filters by match_type", {
  skip_if_no_db()

  t20_result <- head_to_head("India", "Pakistan", match_type = "T20")
  skip_if_no_data(t20_result, "No T20 data for India vs Pakistan")

  expect_s3_class(t20_result, "data.frame")
  expect_equal(tolower(t20_result$match_type), "t20")
})

test_that("head_to_head returns NULL for non-existent matchup", {
  skip_if_no_db()

  result <- head_to_head("NonExistentTeam1", "NonExistentTeam2")

  expect_null(result)
})

# =============================================================================
# Venue Stats Tests
# =============================================================================

test_that("venue_stats returns data frame for all venues", {
  skip_if_no_db()

  result <- venue_stats(min_matches = 1)
  skip_if_no_data(result, "No venue data in database")

  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) > 0)
  expect_true("venue" %in% names(result))
  expect_true("matches" %in% names(result))
  expect_true("run_rate" %in% names(result))
  expect_true("runs_per_ball" %in% names(result))
  expect_true("wickets_per_ball" %in% names(result))
  # Check no duplicate venues
  expect_equal(nrow(result), length(unique(result$venue)))
})

test_that("venue_stats returns data for single venue", {
  skip_if_no_db()

  result <- venue_stats("Melbourne Cricket Ground", min_matches = 1)
  skip_if_no_data(result, "No data for MCG in database")

  expect_s3_class(result, "data.frame")
  expect_true(nrow(result) >= 1)
  expect_true(any(grepl("Melbourne", result$venue, ignore.case = TRUE)))
})

test_that("venue_stats filters by match_type", {
  skip_if_no_db()

  t20_result <- venue_stats(match_type = "T20", min_matches = 1)
  skip_if_no_data(t20_result, "No T20 venue data in database")

  expect_s3_class(t20_result, "data.frame")
  expect_true(nrow(t20_result) > 0)
})

test_that("venue_stats has no duplicate venues", {
  skip_if_no_db()

  result <- venue_stats(min_matches = 1)
  skip_if_no_data(result, "No venue data in database")

  expect_equal(nrow(result), length(unique(result$venue)),
               info = "Venues should not be duplicated")
})

test_that("venue_stats is ordered by matches descending", {
  skip_if_no_db()

  result <- venue_stats(min_matches = 1)
  skip_if_no_data(result, "No venue data in database")

  if (nrow(result) > 1) {
    # Check that matches are in descending order
    expect_true(all(diff(result$matches) <= 0),
                info = "Venues should be ordered by matches descending")
  }
})
