# Tests for Fox Sports Scraper Functions
#
# These tests verify the parsing functions using mock JSON data,
# allowing them to run without making actual API calls.

# ============================================================================
# TEST DATA: MOCK FOX SPORTS API RESPONSES
# ============================================================================

# Mock aggregatestats.json response (ball-by-ball data)
create_mock_aggregatestats <- function() {
  list(
    innings_list = list(
      list(
        innings = 1,
        batting_team = list(id = 101, name = "Australia", code = "AUS"),
        bowling_team = list(name = "India"),
        overs = list(
          list(
            over = 1,
            bowler = list(
              id = 501,
              full_name = "Jasprit Bumrah",
              short_name = "J Bumrah",
              bowling_arm = "Right"
            ),
            balls = list(
              list(
                ball = 1,
                legal_ball = TRUE,
                running_over = 0.1,
                striker = list(
                  id = 201,
                  full_name = "David Warner",
                  short_name = "D Warner",
                  batting_arm = "Left"
                ),
                dismissed_batsman = list(id = NA, full_name = NA),
                byes = 0,
                runs = 4,
                wides = 0,
                commentary = "FOUR! Warner drives through covers",
                leg_byes = 0,
                is_boundary = TRUE,
                is_wicket = FALSE,
                no_ball_runs = 0,
                no_balls = 0,
                shot_x = 0.6,
                shot_y = 0.3,
                total_runs_per_ball = 4,
                wide_runs = 0
              ),
              list(
                ball = 2,
                legal_ball = TRUE,
                running_over = 0.2,
                striker = list(
                  id = 201,
                  full_name = "David Warner",
                  short_name = "D Warner",
                  batting_arm = "Left"
                ),
                dismissed_batsman = list(id = 201, full_name = "David Warner"),
                byes = 0,
                runs = 0,
                wides = 0,
                commentary = "OUT! Caught behind",
                leg_byes = 0,
                is_boundary = FALSE,
                is_wicket = TRUE,
                no_ball_runs = 0,
                no_balls = 0,
                shot_x = NULL,
                shot_y = NULL,
                total_runs_per_ball = 0,
                wide_runs = 0
              )
            )
          )
        )
      )
    )
  )
}

# Mock players.json response
create_mock_players <- function() {
  list(
    team_A = list(
      id = 101,
      name = "Australia",
      code = "AUS",
      players = list(
        list(
          id = 201,
          full_name = "David Warner",
          short_name = "D Warner",
          surname = "Warner",
          other_names = "David Andrew",
          country = "Australia",
          country_code = "AUS",
          batting_arm = "Left",
          bowling_arm = NULL,
          position_code = "BAT",
          position_description = "Opener",
          position_order = 1,
          jumper_number = 31,
          is_captain = FALSE,
          is_interchange = FALSE
        ),
        list(
          id = 202,
          full_name = "Steve Smith",
          short_name = "S Smith",
          surname = "Smith",
          other_names = "Steven Peter Devereux",
          country = "Australia",
          country_code = "AUS",
          batting_arm = "Right",
          bowling_arm = "Right",
          position_code = "BAT",
          position_description = "Middle Order",
          position_order = 4,
          jumper_number = 49,
          is_captain = TRUE,
          is_interchange = FALSE
        )
      )
    ),
    team_B = list(
      id = 102,
      name = "India",
      code = "IND",
      players = list(
        list(
          id = 501,
          full_name = "Jasprit Bumrah",
          short_name = "J Bumrah",
          surname = "Bumrah",
          other_names = "Jasprit Jasbirsingh",
          country = "India",
          country_code = "IND",
          batting_arm = "Right",
          bowling_arm = "Right",
          position_code = "BOWL",
          position_description = "Fast Bowler",
          position_order = 11,
          jumper_number = 93,
          is_captain = FALSE,
          is_interchange = FALSE
        )
      )
    )
  )
}

# Mock details.json response
create_mock_details <- function() {
  list(
    match_details = list(
      internal_match_id = "12345",
      match_type = "Test",
      match_day = 1,
      match_result = "In Progress",
      match_note = "Day 1 of 5",
      venue = list(
        id = 1001,
        name = "Melbourne Cricket Ground",
        city = "Melbourne",
        country = "Australia",
        state = "Victoria"
      ),
      weather = "Sunny",
      pitch_state = "Good for batting",
      surface_state = "Dry",
      crowd = 75000,
      team_A = list(id = 101, name = "Australia", code = "AUS"),
      team_B = list(id = 102, name = "India", code = "IND"),
      won_toss_team_id = 101,
      won_toss_team_elected_to_bat = TRUE
    ),
    performers = list(
      player_of_the_match = list(
        list(id = 201, full_name = "David Warner")
      )
    ),
    referees = list(
      list(type = "Umpire", full_name = "Kumar Dharmasena"),
      list(type = "Umpire", full_name = "Marais Erasmus"),
      list(type = "3rd Umpire", full_name = "Richard Kettleborough"),
      list(type = "Match Referee", full_name = "David Boon")
    )
  )
}

# ============================================================================
# PARSE AGGREGATESTATS TESTS
# ============================================================================

test_that("parse_fox_aggregatestats parses ball-by-ball data correctly", {
  mock_data <- create_mock_aggregatestats()
  result <- bouncer:::parse_fox_aggregatestats(mock_data, "TEST2024-260101")

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 2)

  # Check match_id is correctly assigned

  expect_true(all(result$match_id == "TEST2024-260101"))

  # Check innings data
  expect_equal(result$innings[1], 1)
  expect_equal(result$team_name[1], "Australia")
  expect_equal(result$bowling_team[1], "India")
})

test_that("parse_fox_aggregatestats extracts ball details", {
  mock_data <- create_mock_aggregatestats()
  result <- bouncer:::parse_fox_aggregatestats(mock_data, "TEST2024-260101")

  # First ball - boundary
  ball1 <- result[result$ball == 1, ]
  expect_equal(ball1$runs, 4)
  expect_true(ball1$is_boundary)
  expect_false(ball1$is_wicket)
  expect_equal(ball1$striker_name, "David Warner")
  expect_equal(ball1$bowler_name, "Jasprit Bumrah")

  # Second ball - wicket
  ball2 <- result[result$ball == 2, ]
  expect_equal(ball2$runs, 0)
  expect_true(ball2$is_wicket)
  expect_equal(ball2$dismissed_name, "David Warner")
})

test_that("parse_fox_aggregatestats handles NULL innings", {
  mock_data <- list(innings_list = NULL)
  result <- bouncer:::parse_fox_aggregatestats(mock_data, "TEST2024-260101")

  expect_null(result)
})

test_that("parse_fox_aggregatestats handles empty innings list", {
  mock_data <- list(innings_list = list())
  result <- bouncer:::parse_fox_aggregatestats(mock_data, "TEST2024-260101")

  expect_null(result)
})

test_that("parse_fox_aggregatestats handles missing overs in innings", {
  mock_data <- list(
    innings_list = list(
      list(
        innings = 1,
        batting_team = list(id = 101, name = "Australia", code = "AUS"),
        bowling_team = list(name = "India"),
        overs = NULL
      )
    )
  )
  result <- bouncer:::parse_fox_aggregatestats(mock_data, "TEST2024-260101")

  expect_null(result)
})

test_that("parse_fox_aggregatestats handles NULL field values gracefully", {
  mock_data <- create_mock_aggregatestats()
  # Set shot coordinates to NULL
  mock_data$innings_list[[1]]$overs[[1]]$balls[[1]]$shot_x <- NULL
  mock_data$innings_list[[1]]$overs[[1]]$balls[[1]]$shot_y <- NULL

  result <- bouncer:::parse_fox_aggregatestats(mock_data, "TEST2024-260101")

  # Should still parse, with NA values for NULL fields
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 2)
  expect_true(is.na(result$shot_x[1]) || is.null(result$shot_x[1]))
})

# ============================================================================
# PARSE PLAYERS TESTS
# ============================================================================

test_that("parse_fox_players parses squad data correctly", {
  mock_data <- create_mock_players()
  result <- bouncer:::parse_fox_players(mock_data, "TEST2024-260101")

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 3)  # 2 from team_A, 1 from team_B

  # Check match_id is correctly assigned
  expect_true(all(result$match_id == "TEST2024-260101"))
})

test_that("parse_fox_players extracts player details", {
  mock_data <- create_mock_players()
  result <- bouncer:::parse_fox_players(mock_data, "TEST2024-260101")

  # Find Warner
  warner <- result[result$player_id == 201, ]
  expect_equal(nrow(warner), 1)
  expect_equal(warner$full_name, "David Warner")
  expect_equal(warner$batting_arm, "Left")
  expect_equal(warner$team_name, "Australia")
  expect_false(warner$is_captain)

  # Find Smith (captain)
  smith <- result[result$player_id == 202, ]
  expect_equal(nrow(smith), 1)
  expect_true(smith$is_captain)

  # Find Bumrah (from team_B)
  bumrah <- result[result$player_id == 501, ]
  expect_equal(nrow(bumrah), 1)
  expect_equal(bumrah$team_name, "India")
})

test_that("parse_fox_players handles NULL teams", {
  mock_data <- list(team_A = NULL, team_B = NULL)
  result <- bouncer:::parse_fox_players(mock_data, "TEST2024-260101")

  expect_null(result)
})

test_that("parse_fox_players handles empty player list", {
  mock_data <- list(
    team_A = list(id = 101, name = "Australia", code = "AUS", players = list()),
    team_B = NULL
  )
  result <- bouncer:::parse_fox_players(mock_data, "TEST2024-260101")

  expect_null(result)
})

test_that("parse_fox_players handles single team only", {
  mock_data <- create_mock_players()
  mock_data$team_B <- NULL

  result <- bouncer:::parse_fox_players(mock_data, "TEST2024-260101")

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 2)  # Only team_A players
  expect_true(all(result$team_name == "Australia"))
})

# ============================================================================
# PARSE DETAILS TESTS
# ============================================================================

test_that("parse_fox_details parses match metadata correctly", {
  mock_data <- create_mock_details()
  result <- bouncer:::parse_fox_details(mock_data, "TEST2024-260101")

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 1)

  # Check match_id
  expect_equal(result$match_id, "TEST2024-260101")

  # Check basic details
  expect_equal(result$match_type, "Test")
  expect_equal(result$match_day, 1)
  expect_equal(result$weather, "Sunny")
})

test_that("parse_fox_details extracts venue information", {
  mock_data <- create_mock_details()
  result <- bouncer:::parse_fox_details(mock_data, "TEST2024-260101")

  expect_equal(result$venue_id, 1001)
  expect_equal(result$venue_name, "Melbourne Cricket Ground")
  expect_equal(result$venue_city, "Melbourne")
  expect_equal(result$venue_country, "Australia")
  expect_equal(result$venue_state, "Victoria")
})

test_that("parse_fox_details extracts team information", {
  mock_data <- create_mock_details()
  result <- bouncer:::parse_fox_details(mock_data, "TEST2024-260101")

  expect_equal(result$team_a_id, 101)
  expect_equal(result$team_a_name, "Australia")
  expect_equal(result$team_a_code, "AUS")
  expect_equal(result$team_b_id, 102)
  expect_equal(result$team_b_name, "India")
})

test_that("parse_fox_details extracts toss information", {
  mock_data <- create_mock_details()
  result <- bouncer:::parse_fox_details(mock_data, "TEST2024-260101")

  expect_equal(result$toss_winner_id, 101)
  expect_true(result$toss_elected_bat)
})

test_that("parse_fox_details extracts player of match", {
  mock_data <- create_mock_details()
  result <- bouncer:::parse_fox_details(mock_data, "TEST2024-260101")

  expect_equal(result$player_of_match_id, 201)
  expect_equal(result$player_of_match_name, "David Warner")
})

test_that("parse_fox_details extracts referee information", {
  mock_data <- create_mock_details()
  result <- bouncer:::parse_fox_details(mock_data, "TEST2024-260101")

  expect_true(grepl("Kumar Dharmasena", result$umpires))
  expect_true(grepl("Marais Erasmus", result$umpires))
  expect_equal(result$third_umpire, "Richard Kettleborough")
  expect_equal(result$match_referee, "David Boon")
})

test_that("parse_fox_details handles NULL match_details", {
  mock_data <- list(match_details = NULL)
  result <- bouncer:::parse_fox_details(mock_data, "TEST2024-260101")

  expect_null(result)
})

test_that("parse_fox_details handles missing player of match", {
  mock_data <- create_mock_details()
  mock_data$performers$player_of_the_match <- NULL

  result <- bouncer:::parse_fox_details(mock_data, "TEST2024-260101")

  expect_s3_class(result, "data.frame")
  expect_true(is.na(result$player_of_match_id))
  expect_true(is.na(result$player_of_match_name))
})

test_that("parse_fox_details handles missing referees", {
  mock_data <- create_mock_details()
  mock_data$referees <- NULL

  result <- bouncer:::parse_fox_details(mock_data, "TEST2024-260101")

  expect_s3_class(result, "data.frame")
  expect_true(is.na(result$umpires))
  expect_true(is.na(result$third_umpire))
  expect_true(is.na(result$match_referee))
})

# ============================================================================
# FOX_LIST_FORMATS TESTS
# ============================================================================

test_that("fox_list_formats returns character vector by default", {
  result <- fox_list_formats()

  expect_type(result, "character")
  expect_true(length(result) > 0)
  expect_true("TEST" %in% result)
  expect_true("T20I" %in% result)
  expect_true("ODI" %in% result)
  expect_true("BBL" %in% result)
})
test_that("fox_list_formats returns data.frame with details=TRUE", {
  result <- fox_list_formats(details = TRUE)

  expect_s3_class(result, "data.frame")
  expect_true("format" %in% names(result))
  expect_true("prefix" %in% names(result))
  expect_true("max_innings" %in% names(result))
  expect_true("max_series" %in% names(result))
  expect_true("max_matches" %in% names(result))
})

test_that("fox_list_formats has correct max_innings values", {
  result <- fox_list_formats(details = TRUE)

  # Test matches have 4 innings
  test_row <- result[result$format == "TEST", ]
  expect_equal(test_row$max_innings, 4)

  # Limited overs have 2 innings
  t20_row <- result[result$format == "T20I", ]
  expect_equal(t20_row$max_innings, 2)

  odi_row <- result[result$format == "ODI", ]
  expect_equal(odi_row$max_innings, 2)
})

# ============================================================================
# GENERATE_MATCH_ID_VARIANTS TESTS
# ============================================================================

test_that("generate_match_id_variants creates correct patterns", {
  result <- bouncer:::generate_match_id_variants(2024, 5, 3, "TEST")

  expect_length(result, 2)
  # Pattern A: TEST2024-250503 (hyphen + season suffix)
  expect_true("TEST2024-250503" %in% result)
  # Pattern B: TEST20240503 (no hyphen)
  expect_true("TEST20240503" %in% result)
})

test_that("generate_match_id_variants works for different formats", {
  # T20I format
  t20_result <- bouncer:::generate_match_id_variants(2025, 1, 1, "T20I")
  expect_true("T20I2025-260101" %in% t20_result)
  expect_true("T20I20250101" %in% t20_result)

  # BBL format
  bbl_result <- bouncer:::generate_match_id_variants(2024, 10, 50, "BBL")
  expect_true("BBL2024-251050" %in% bbl_result)
  expect_true("BBL20241050" %in% bbl_result)
})

test_that("generate_match_id_variants pads numbers correctly", {
  result <- bouncer:::generate_match_id_variants(2024, 1, 1, "TEST")

  # Series and match should be zero-padded to 2 digits
  expect_true(any(grepl("0101", result)))
})

test_that("generate_match_id_variants errors on unknown format", {
  expect_error(
    bouncer:::generate_match_id_variants(2024, 1, 1, "INVALID"),
    "Unknown format"
  )
})

# ============================================================================
# FOX_FORMATS CONFIGURATION TESTS
# ============================================================================

test_that("FOX_FORMATS has all expected formats", {
  formats <- names(bouncer:::FOX_FORMATS)

  expect_true("TEST" %in% formats)
  expect_true("T20I" %in% formats)
  expect_true("WT20I" %in% formats)
  expect_true("ODI" %in% formats)
  expect_true("WODI" %in% formats)
  expect_true("BBL" %in% formats)
  expect_true("WBBL" %in% formats)
})

test_that("FOX_FORMATS has correct structure", {
  for (format_name in names(bouncer:::FOX_FORMATS)) {
    config <- bouncer:::FOX_FORMATS[[format_name]]

    expect_true("prefix" %in% names(config),
                info = paste("Missing prefix in", format_name))
    expect_true("max_innings" %in% names(config),
                info = paste("Missing max_innings in", format_name))
    expect_true("max_series" %in% names(config),
                info = paste("Missing max_series in", format_name))
    expect_true("max_matches" %in% names(config),
                info = paste("Missing max_matches in", format_name))
  }
})

test_that("FOX_FORMATS prefixes match format names", {
  # Most formats use same prefix as name
  expect_equal(bouncer:::FOX_FORMATS$TEST$prefix, "TEST")
  expect_equal(bouncer:::FOX_FORMATS$T20I$prefix, "T20I")
  expect_equal(bouncer:::FOX_FORMATS$BBL$prefix, "BBL")
})

# ============================================================================
# DATA TYPE CONSISTENCY TESTS
# ============================================================================

test_that("parsed aggregatestats has correct column types", {
  mock_data <- create_mock_aggregatestats()
  result <- bouncer:::parse_fox_aggregatestats(mock_data, "TEST2024-260101")

  # Character columns
  expect_type(result$match_id, "character")
  expect_type(result$team_name, "character")
  expect_type(result$striker_name, "character")
  expect_type(result$bowler_name, "character")
  expect_type(result$commentary, "character")

  # Numeric columns (JSON parses as double by default in R)
  expect_type(result$innings, "double")
  expect_type(result$over, "double")
  expect_type(result$ball, "double")
  expect_type(result$runs, "double")
  expect_type(result$total_runs, "double")

  # Logical columns
  expect_type(result$is_boundary, "logical")
  expect_type(result$is_wicket, "logical")
  expect_type(result$legal_ball, "logical")
})

test_that("parsed players has correct column types", {
  mock_data <- create_mock_players()
  result <- bouncer:::parse_fox_players(mock_data, "TEST2024-260101")

  # Character columns
  expect_type(result$match_id, "character")
  expect_type(result$full_name, "character")
  expect_type(result$team_name, "character")

  # Numeric columns (JSON parses as double by default in R)
  expect_type(result$player_id, "double")
  expect_type(result$team_id, "double")

  # Logical columns
  expect_type(result$is_captain, "logical")
  expect_type(result$is_interchange, "logical")
})

test_that("parsed details has correct column types", {
  mock_data <- create_mock_details()
  result <- bouncer:::parse_fox_details(mock_data, "TEST2024-260101")

  # Character columns
  expect_type(result$match_id, "character")
  expect_type(result$match_type, "character")
  expect_type(result$venue_name, "character")
  expect_type(result$weather, "character")

  # Numeric columns (JSON parses as double by default in R)
  expect_type(result$venue_id, "double")
  expect_type(result$crowd, "double")

  # Logical columns
  expect_type(result$toss_elected_bat, "logical")
})

# ============================================================================
# EDGE CASE HANDLING
# ============================================================================

test_that("parsers handle special characters in names", {
  mock_data <- create_mock_players()
  mock_data$team_A$players[[1]]$full_name <- "Jos\u00e9 O'Brien-Smith"

  result <- bouncer:::parse_fox_players(mock_data, "TEST2024-260101")

  expect_s3_class(result, "data.frame")
  # Name should be preserved
  player <- result[result$player_id == 201, ]
  expect_true(grepl("O'Brien", player$full_name))
})

test_that("parsers handle very long commentary", {
  mock_data <- create_mock_aggregatestats()
  long_commentary <- paste(rep("word", 1000), collapse = " ")
  mock_data$innings_list[[1]]$overs[[1]]$balls[[1]]$commentary <- long_commentary

  result <- bouncer:::parse_fox_aggregatestats(mock_data, "TEST2024-260101")

  expect_s3_class(result, "data.frame")
  expect_equal(nchar(result$commentary[1]), nchar(long_commentary))
})

test_that("parsers handle zero runs innings", {
  mock_data <- create_mock_aggregatestats()
  mock_data$innings_list[[1]]$overs[[1]]$balls[[1]]$runs <- 0
  mock_data$innings_list[[1]]$overs[[1]]$balls[[1]]$total_runs_per_ball <- 0

  result <- bouncer:::parse_fox_aggregatestats(mock_data, "TEST2024-260101")

  expect_s3_class(result, "data.frame")
  expect_equal(result$runs[1], 0)
  expect_equal(result$total_runs[1], 0)
})
