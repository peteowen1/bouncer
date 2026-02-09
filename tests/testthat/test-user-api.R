# Tests for User API Functions

# Helper to check if database is available
db_available <- function() {
  tryCatch({
    file.exists(get_default_db_path())
  }, error = function(e) FALSE)
}

# ============================================================================
# get_player() tests
# ============================================================================

test_that("get_player returns bouncer_player class for valid player", {
  skip_if_not(db_available(), "Database not available")

  # Query a known player (using partial match)
  result <- get_player("Kohli")
  skip_if(is.null(result), "Player not found in database")

  expect_s3_class(result, "bouncer_player")
  expect_true("player_id" %in% names(result))
  expect_true("player_name" %in% names(result))
})

test_that("get_player returns NULL for non-existent player", {
  skip_if_not(db_available(), "Database not available")

  # Query a player that definitely doesn't exist
  result <- suppressMessages(get_player("ZZZNONEXISTENTPLAYERZZZ"))

  expect_null(result)
})

test_that("get_player handles format parameter", {
  skip_if_not(db_available(), "Database not available")

  result <- get_player("Kohli", format = "t20")
  skip_if(is.null(result), "Player not found in database")

  expect_s3_class(result, "bouncer_player")
})

test_that("get_player validates format parameter", {
  skip_if_not(db_available(), "Database not available")

  # Invalid format should error or be handled gracefully
  expect_error(
    get_player("Kohli", format = "invalid_format"),
    regexp = NULL  # Just check it errors
  )
})

# ============================================================================
# search_players() tests
# ============================================================================

test_that("search_players returns data frame", {
  skip_if_not(db_available(), "Database not available")

  result <- search_players("Smith")

  expect_true(is.data.frame(result) || is.null(result))

  if (!is.null(result) && nrow(result) > 0) {
    expect_true("player_id" %in% names(result))
    expect_true("player_name" %in% names(result))
  }
})

test_that("search_players respects limit parameter", {
  skip_if_not(db_available(), "Database not available")

  result <- search_players("a", limit = 5)

  if (!is.null(result)) {
    expect_lte(nrow(result), 5)
  }
})

test_that("search_players returns empty for no matches", {
  skip_if_not(db_available(), "Database not available")

  result <- suppressMessages(search_players("ZZZNONEXISTENTZZZ"))

  expect_true(is.null(result) || nrow(result) == 0)
})

# ============================================================================
# compare_players() tests
# ============================================================================

test_that("compare_players requires at least 2 players", {
  skip_if_not(db_available(), "Database not available")

  # Should error with less than 2 players
  expect_error(compare_players("Kohli"))
})

test_that("compare_players returns comparison object for valid players", {
  skip_if_not(db_available(), "Database not available")

  result <- suppressMessages(compare_players("Kohli", "Sharma", format = "t20"))
  skip_if(is.null(result), "Players not found in database")

  expect_s3_class(result, "bouncer_player_comparison")
})

# ============================================================================
# analyze_player() tests
# ============================================================================

test_that("analyze_player returns analysis object", {
  skip_if_not(db_available(), "Database not available")

  result <- suppressMessages(analyze_player("Kohli", format = "t20"))
  skip_if(is.null(result), "Player not found in database")

  expect_s3_class(result, "bouncer_player_analysis")
})

# ============================================================================
# Print methods tests
# ============================================================================

test_that("print.bouncer_player doesn't error", {
  skip_if_not(db_available(), "Database not available")

  player <- get_player("Kohli")
  skip_if(is.null(player), "Player not found in database")

  expect_output(print(player))
})

test_that("print.bouncer_player_comparison doesn't error", {
  skip_if_not(db_available(), "Database not available")

  comparison <- suppressMessages(compare_players("Kohli", "Sharma", format = "t20"))
  skip_if(is.null(comparison), "Players not found in database")

  expect_output(print(comparison))
})

# ============================================================================
# Edge cases and error handling
# ============================================================================

test_that("get_player handles special characters in name", {
  skip_if_not(db_available(), "Database not available")

  # Test with apostrophe (potential SQL injection)
  result <- suppressMessages(get_player("O'Brien"))

  # Should not error - either finds player or returns NULL
  expect_true(is.null(result) || inherits(result, "bouncer_player"))
})

test_that("search_players handles special characters", {
  skip_if_not(db_available(), "Database not available")

  # Test with potential SQL injection characters
  result <- suppressMessages(search_players("'; DROP TABLE players; --"))

  # Should not error and should return empty or NULL
  expect_true(is.null(result) || is.data.frame(result))
})

test_that("get_player rejects empty string", {
  skip_if_not(db_available(), "Database not available")

  # Empty strings should be rejected with an error
  expect_error(get_player(""), regexp = "cannot be empty")
})

# ============================================================================
# get_team() tests
# ============================================================================

test_that("get_team returns bouncer_team class for valid team", {
  skip_if_not(db_available(), "Database not available")

  result <- suppressMessages(get_team("India"))
  skip_if(is.null(result), "Team not found in database")

  expect_s3_class(result, "bouncer_team")
  expect_true("name" %in% names(result))
  expect_true("elo" %in% names(result))
})

test_that("get_team returns NULL for non-existent team", {
  skip_if_not(db_available(), "Database not available")

  result <- suppressMessages(get_team("ZZZNONEXISTENTTEAMZZZ"))

  expect_null(result)
})

test_that("get_team handles format parameter", {
  skip_if_not(db_available(), "Database not available")

  result <- suppressMessages(get_team("India", format = "t20"))
  skip_if(is.null(result), "Team not found in database")

  expect_s3_class(result, "bouncer_team")
})

test_that("get_team rejects empty string", {
  # Empty strings should be rejected with an error
  expect_error(get_team(""), regexp = "cannot be empty")
})

test_that("get_team rejects NULL name", {
  expect_error(get_team(NULL), regexp = "name is required")
})

test_that("get_team rejects non-character name", {
  expect_error(get_team(123), regexp = "must be a single character string")
})

# ============================================================================
# compare_teams() tests
# ============================================================================

test_that("compare_teams requires two team names", {
  # Should error with less than 2 teams
  expect_error(compare_teams("India"))
})

test_that("compare_teams returns comparison object", {
  skip_if_not(db_available(), "Database not available")

  result <- suppressMessages(compare_teams("India", "Australia", format = "t20"))
  skip_if(is.null(result), "Teams not found in database")

  expect_s3_class(result, "bouncer_team_comparison")
  expect_true("team1" %in% names(result))
  expect_true("team2" %in% names(result))
})

test_that("compare_teams rejects empty team names", {
  expect_error(compare_teams("", "Australia"), regexp = "non-empty")
  expect_error(compare_teams("India", ""), regexp = "non-empty")
})

# ============================================================================
# analyze_match() tests
# ============================================================================

test_that("analyze_match returns analysis for valid match_id", {
  skip_if_not(db_available(), "Database not available")

  # Get a valid match_id from the database
  conn <- get_db_connection(read_only = TRUE)
  match_ids <- DBI::dbGetQuery(conn, "SELECT match_id FROM matches LIMIT 1")
  DBI::dbDisconnect(conn, shutdown = TRUE)

  skip_if(nrow(match_ids) == 0, "No matches in database")

  result <- suppressMessages(analyze_match(match_ids$match_id[1]))
  skip_if(is.null(result), "Match analysis returned NULL")

  expect_s3_class(result, "bouncer_match")
  expect_true("match_id" %in% names(result))
})

test_that("analyze_match rejects empty match_id", {
  expect_error(analyze_match(""), regexp = "cannot be empty")
})

test_that("analyze_match rejects NULL match_id", {
  expect_error(analyze_match(NULL), regexp = "match_id is required")
})

# ============================================================================
# predict_match() tests
# ============================================================================

test_that("predict_match returns prediction object for valid teams", {
  skip_if_not(db_available(), "Database not available")

  result <- suppressMessages(
    predict_match("India", "Australia", format = "t20")
  )
  skip_if(is.null(result), "Prediction returned NULL")

  expect_s3_class(result, "bouncer_prediction")
  expect_true("team1" %in% names(result))
  expect_true("team2" %in% names(result))
  expect_true("probabilities" %in% names(result))
})

test_that("predict_match probabilities sum to 1", {
  skip_if_not(db_available(), "Database not available")

  result <- suppressMessages(
    predict_match("India", "Australia", format = "t20")
  )
  skip_if(is.null(result), "Prediction returned NULL")
  skip_if(is.null(result$probabilities), "No probabilities in prediction")

  prob_sum <- sum(unlist(result$probabilities), na.rm = TRUE)
  expect_true(prob_sum > 0.99 && prob_sum < 1.01)
})

test_that("predict_match rejects empty team names", {
  expect_error(predict_match("", "Australia"), regexp = "non-empty")
  expect_error(predict_match("India", ""), regexp = "non-empty")
})

test_that("predict_match rejects NULL team names", {
  expect_error(predict_match(NULL, "Australia"), regexp = "team1 is required")
  expect_error(predict_match("India", NULL), regexp = "team2 is required")
})

# ============================================================================
# Print methods for team objects
# ============================================================================

test_that("print.bouncer_team doesn't error", {
  skip_if_not(db_available(), "Database not available")

  team <- suppressMessages(get_team("India"))
  skip_if(is.null(team), "Team not found in database")

  expect_output(print(team))
})

test_that("print.bouncer_team_comparison doesn't error", {
  skip_if_not(db_available(), "Database not available")

  comparison <- suppressMessages(compare_teams("India", "Australia", format = "t20"))
  skip_if(is.null(comparison), "Team comparison returned NULL")

  expect_output(print(comparison))
})

test_that("print.bouncer_prediction doesn't error", {
  skip_if_not(db_available(), "Database not available")

  prediction <- suppressMessages(predict_match("India", "Australia", format = "t20"))
  skip_if(is.null(prediction), "Prediction returned NULL")

  expect_output(print(prediction))
})
