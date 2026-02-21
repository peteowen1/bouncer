# Tests for ESPN Cricinfo Scraper Functions
#
# These tests verify the parsing functions using mock JSON data,
# allowing them to run without making actual API calls.

# ============================================================================
# TEST DATA: MOCK CRICINFO RESPONSES
# ============================================================================

# Mock commentary response (ball-by-ball data from CDP capture)
# Real API uses "title" for names, not separate name fields
create_mock_cricinfo_commentary <- function() {
  list(
    comments = list(
      list(
        inningNumber = 1,
        oversActual = 0.1,
        overNumber = 0,
        ballNumber = 1,
        batsmanPlayerId = 50710,
        nonStrikerPlayerId = 50803,
        bowlerPlayerId = 20431,
        totalRuns = 0,
        batsmanRuns = 0,
        isFour = FALSE,
        isSix = FALSE,
        isWicket = FALSE,
        dismissalType = NULL,
        dismissalText = NULL,
        outPlayerId = NULL,
        wides = 0,
        noballs = 0,
        byes = 0,
        legbyes = 0,
        pitchLine = "OUTSIDE_OFFSTUMP",
        pitchLength = "GOOD_LENGTH",
        shotType = "DEFENSIVE",
        shotControl = 1,
        wagonX = 250.5,
        wagonY = 180.3,
        wagonZone = 3,
        predictions = list(
          score = 285.5,
          winProbability = 0.45
        ),
        totalInningRuns = 0,
        totalInningWickets = 0,
        title = "Woakes to Khawaja, no run",
        timestamp = "2025-11-21T00:30:00.000Z"
      ),
      list(
        inningNumber = 1,
        oversActual = 0.2,
        overNumber = 0,
        ballNumber = 2,
        batsmanPlayerId = 50710,
        nonStrikerPlayerId = 50803,
        bowlerPlayerId = 20431,
        totalRuns = 4,
        batsmanRuns = 4,
        isFour = TRUE,
        isSix = FALSE,
        isWicket = FALSE,
        dismissalType = NULL,
        dismissalText = NULL,
        outPlayerId = NULL,
        wides = 0,
        noballs = 0,
        byes = 0,
        legbyes = 0,
        pitchLine = "MIDDLE_STUMP",
        pitchLength = "FULL",
        shotType = "COVER_DRIVE",
        shotControl = 1,
        wagonX = 380.2,
        wagonY = 120.8,
        wagonZone = 5,
        predictions = list(
          score = 290.1,
          winProbability = 0.47
        ),
        totalInningRuns = 4,
        totalInningWickets = 0,
        title = "Woakes to Khawaja, FOUR",
        timestamp = "2025-11-21T00:31:00.000Z"
      ),
      list(
        inningNumber = 1,
        oversActual = 0.3,
        overNumber = 0,
        ballNumber = 3,
        batsmanPlayerId = 50710,
        nonStrikerPlayerId = 50803,
        bowlerPlayerId = 20431,
        totalRuns = 0,
        batsmanRuns = 0,
        isFour = FALSE,
        isSix = FALSE,
        isWicket = TRUE,
        dismissalType = 1,
        dismissalText = "c Crawley b Woakes",
        outPlayerId = 50710,
        wides = 0,
        noballs = 0,
        byes = 0,
        legbyes = 0,
        pitchLine = "OFF_STUMP",
        pitchLength = "SHORT_OF_GOOD_LENGTH",
        shotType = "EDGED",
        shotControl = 2,
        wagonX = NULL,
        wagonY = NULL,
        wagonZone = NULL,
        predictions = list(
          score = 260.0,
          winProbability = 0.40
        ),
        totalInningRuns = 4,
        totalInningWickets = 1,
        title = "Woakes to Khawaja, OUT",
        timestamp = "2025-11-21T00:32:00.000Z"
      )
    ),
    nextInningOver = NULL
  )
}

# Mock scorecard __NEXT_DATA__
create_mock_cricinfo_scorecard <- function() {
  list(
    content = list(
      innings = list(
        list(
          inningNumber = 1,
          team = list(name = "Australia"),
          inningBatsmen = list(
            list(
              player = list(id = 50710, name = "Usman Khawaja"),
              runs = 65,
              ballsFaced = 120,
              fours = 8,
              sixes = 0,
              strikeRate = 54.16,
              dismissalText = list(
                short = "caught", long = "c Crawley b Woakes",
                commentary = "Usman Khawaja c Crawley b Woakes 65 (120b 8x4 0x6)",
                fielderText = "c Crawley", bowlerText = "b Woakes"
              ),
              battingPosition = 1,
              isOut = TRUE,
              minutes = 180
            ),
            list(
              player = list(id = 50803, name = "Steve Smith"),
              runs = 142,
              ballsFaced = 250,
              fours = 15,
              sixes = 2,
              strikeRate = 56.80,
              dismissalText = list(
                short = "not out", long = "not out",
                commentary = "Steve Smith not out 142 (250b 15x4 2x6)",
                fielderText = NULL, bowlerText = NULL
              ),
              battingPosition = 4,
              isOut = FALSE,
              minutes = 350
            )
          ),
          inningBowlers = list(
            list(
              player = list(id = 20431, name = "Chris Woakes"),
              overs = 25,
              maidens = 7,
              conceded = 62,
              wickets = 3,
              economy = 2.48,
              dots = 95,
              fours = 6,
              sixes = 0,
              wides = 1,
              noballs = 0
            )
          ),
          inningOvers = list(
            list(
              overNumber = 1,
              overRuns = 4,
              overWickets = 1,
              totalRuns = 4,
              totalWickets = 1,
              isComplete = TRUE,
              balls = list(
                list(
                  inningNumber = 1,
                  oversActual = 0.1,
                  overNumber = 0,
                  ballNumber = 1,
                  batsmanPlayerId = 50710,
                  nonStrikerPlayerId = 50803,
                  bowlerPlayerId = 20431,
                  totalRuns = 0,
                  batsmanRuns = 0,
                  isFour = FALSE,
                  isSix = FALSE,
                  isWicket = FALSE,
                  pitchLine = "OUTSIDE_OFFSTUMP",
                  pitchLength = "GOOD_LENGTH",
                  shotType = "DEFENSIVE",
                  shotControl = 1,
                  wagonX = 250.5,
                  wagonY = 180.3,
                  wagonZone = 3,
                  predictions = list(score = 285.5, winProbability = 0.45),
                  totalInningRuns = 0,
                  totalInningWickets = 0,
                  timestamp = "2025-11-21T00:30:00.000Z"
                )
              )
            ),
            list(
              overNumber = 2,
              overRuns = 8,
              overWickets = 0,
              totalRuns = 12,
              totalWickets = 1,
              isComplete = TRUE,
              balls = NULL
            )
          )
        )
      )
    )
  )
}

# Mock match details __NEXT_DATA__
create_mock_cricinfo_details <- function() {
  list(
    match = list(
      objectId = 1455611,
      slug = "1st-test-australia-vs-england",
      title = "1st Test, Australia vs England",
      format = "Test",
      status = "RESULT",
      statusText = "Australia won by 184 runs",
      stage = "FINISHED",
      startDate = "2025-11-21",
      endDate = "2025-11-25",
      startTime = "2025-11-21T00:00:00.000Z",
      ground = list(
        id = 131,
        name = "Gabba",
        longName = "Brisbane Cricket Ground, Woolloongabba",
        town = list(name = "Brisbane"),
        country = list(name = "Australia")
      ),
      teams = list(
        list(team = list(id = 2, name = "Australia", longName = "Australia")),
        list(team = list(id = 1, name = "England", longName = "England"))
      ),
      toss = list(
        tossWinnerId = 2,
        decision = "bat"
      ),
      result = "Australia",
      resultText = "Australia won by 184 runs"
    )
  )
}

# Mock series schedule __NEXT_DATA__
create_mock_cricinfo_schedule <- function() {
  list(
    content = list(
      matches = list(
        list(
          objectId = 1455611,
          title = "1st Test",
          slug = "1st-test-australia-vs-england",
          format = "Test",
          status = "RESULT",
          stage = "FINISHED",
          startDate = "2025-11-21",
          endDate = "2025-11-25",
          ground = list(
            name = "Gabba",
            town = list(name = "Brisbane")
          ),
          teams = list(
            list(team = list(name = "Australia")),
            list(team = list(name = "England"))
          )
        ),
        list(
          objectId = 1455613,
          title = "2nd Test",
          slug = "2nd-test-australia-vs-england",
          format = "Test",
          status = "RESULT",
          stage = "FINISHED",
          startDate = "2025-12-06",
          endDate = "2025-12-10",
          ground = list(
            name = "Adelaide Oval",
            town = list(name = "Adelaide")
          ),
          teams = list(
            list(team = list(name = "Australia")),
            list(team = list(name = "England"))
          )
        )
      )
    )
  )
}

# ============================================================================
# HELPER FUNCTION TESTS
# ============================================================================

test_that("to_scalar converts non-scalars to NA", {
  expect_true(is.na(bouncer:::to_scalar(list())))
  expect_true(is.na(bouncer:::to_scalar(NULL)))
  expect_true(is.na(bouncer:::to_scalar(list(a = 1, b = 2))))
  expect_true(is.na(bouncer:::to_scalar(character(0))))
  expect_equal(bouncer:::to_scalar(42), 42)
  expect_equal(bouncer:::to_scalar("hello"), "hello")
  expect_equal(bouncer:::to_scalar(FALSE), FALSE)
})

# ============================================================================
# URL BUILDER TESTS
# ============================================================================

test_that("cricinfo_match_url builds correct URLs", {
  url <- bouncer:::cricinfo_match_url(1455609, 1455611, "full-scorecard")
  expect_equal(url, "https://www.espncricinfo.com/series/s-1455609/m-1455611/full-scorecard")

  url2 <- bouncer:::cricinfo_match_url(1502138, 1512737, "ball-by-ball-commentary")
  expect_equal(url2, "https://www.espncricinfo.com/series/s-1502138/m-1512737/ball-by-ball-commentary")
})

test_that("cricinfo_match_url defaults to full-scorecard", {
  url <- bouncer:::cricinfo_match_url(100, 200)
  expect_true(grepl("full-scorecard$", url))
})

test_that("cricinfo_series_url builds correct URLs", {
  url <- bouncer:::cricinfo_series_url(1455609)
  expect_equal(url, "https://www.espncricinfo.com/series/s-1455609/match-schedule-fixtures-and-results")
})

# ============================================================================
# PARSE COMMENTARY TESTS
# ============================================================================

test_that("parse_cricinfo_commentary parses ball-by-ball data correctly", {
  mock_data <- create_mock_cricinfo_commentary()
  result <- bouncer:::parse_cricinfo_commentary(mock_data, match_id = 1455611, series_id = 1455609)

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 3)
  expect_true(all(result$match_id == 1455611))
  expect_true(all(result$series_id == 1455609))
})

test_that("parse_cricinfo_commentary extracts ball details", {
  mock_data <- create_mock_cricinfo_commentary()
  result <- bouncer:::parse_cricinfo_commentary(mock_data, match_id = 1455611)

  # First ball - defensive
  ball1 <- result[result$ball_number == 1, ]
  expect_equal(ball1$batsman_runs, 0)
  expect_false(ball1$is_four)
  expect_false(ball1$is_wicket)
  # Name extracted from title "Woakes to Khawaja, no run"
  expect_equal(ball1$batsman_name, "Khawaja")
  expect_equal(ball1$bowler_name, "Woakes")

  # Second ball - four
  ball2 <- result[result$ball_number == 2, ]
  expect_equal(ball2$batsman_runs, 4)
  expect_true(ball2$is_four)
  expect_equal(ball2$total_innings_runs, 4)
})

test_that("parse_cricinfo_commentary uses player_map for full names", {
  mock_data <- create_mock_cricinfo_commentary()
  player_map <- list("50710" = "Usman Khawaja", "20431" = "Chris Woakes")

  result <- bouncer:::parse_cricinfo_commentary(
    mock_data, match_id = 1455611, player_map = player_map
  )

  ball1 <- result[result$ball_number == 1, ]
  expect_equal(ball1$batsman_name, "Usman Khawaja")
  expect_equal(ball1$bowler_name, "Chris Woakes")
})

test_that("parse_cricinfo_commentary extracts enriched data", {
  mock_data <- create_mock_cricinfo_commentary()
  result <- bouncer:::parse_cricinfo_commentary(mock_data, match_id = 1455611)

  # Pitch line/length
  expect_equal(result$pitch_line[1], "OUTSIDE_OFFSTUMP")
  expect_equal(result$pitch_length[1], "GOOD_LENGTH")
  expect_equal(result$pitch_length[2], "FULL")

  # Shot type and control
  expect_equal(result$shot_type[1], "DEFENSIVE")
  expect_equal(result$shot_type[2], "COVER_DRIVE")
  expect_equal(result$shot_control[1], 1)
  expect_equal(result$shot_control[3], 2)  # Edged = poor control

  # Wagon wheel
  expect_equal(result$wagon_zone[1], 3)
  expect_equal(result$wagon_zone[2], 5)
})

test_that("parse_cricinfo_commentary extracts predictions", {
  mock_data <- create_mock_cricinfo_commentary()
  result <- bouncer:::parse_cricinfo_commentary(mock_data, match_id = 1455611)

  expect_equal(result$predicted_score[1], 285.5)
  expect_equal(result$win_probability[1], 0.45)
  expect_equal(result$win_probability[2], 0.47)
})

test_that("parse_cricinfo_commentary handles wicket data", {
  mock_data <- create_mock_cricinfo_commentary()
  result <- bouncer:::parse_cricinfo_commentary(mock_data, match_id = 1455611)

  wicket_ball <- result[result$is_wicket == TRUE, ]
  expect_equal(nrow(wicket_ball), 1)
  expect_equal(wicket_ball$out_player_id, 50710)
  expect_equal(wicket_ball$dismissal_type, 1)
  expect_equal(wicket_ball$dismissal_text, "c Crawley b Woakes")
  expect_equal(wicket_ball$total_innings_wickets, 1)
})

test_that("parse_cricinfo_commentary extracts title column", {
  mock_data <- create_mock_cricinfo_commentary()
  result <- bouncer:::parse_cricinfo_commentary(mock_data, match_id = 1455611)

  expect_true("title" %in% names(result))
  expect_equal(result$title[1], "Woakes to Khawaja, no run")
  expect_equal(result$title[2], "Woakes to Khawaja, FOUR")
})

test_that("parse_cricinfo_commentary handles NULL fields gracefully", {
  mock_data <- create_mock_cricinfo_commentary()
  result <- bouncer:::parse_cricinfo_commentary(mock_data, match_id = 1455611)

  # Third ball has NULL wagon coords (caught behind)
  expect_true(is.na(result$wagon_x[3]))
  expect_true(is.na(result$wagon_y[3]))
  expect_true(is.na(result$wagon_zone[3]))
})

test_that("parse_cricinfo_commentary returns NULL for empty comments", {
  result <- bouncer:::parse_cricinfo_commentary(list(comments = list()), match_id = 1455611)
  expect_null(result)

  result2 <- bouncer:::parse_cricinfo_commentary(list(comments = NULL), match_id = 1455611)
  expect_null(result2)
})

# ============================================================================
# PARSE SCORECARD TESTS
# ============================================================================

test_that("parse_cricinfo_scorecard parses batting/bowling data", {
  mock_data <- create_mock_cricinfo_scorecard()
  result <- bouncer:::parse_cricinfo_scorecard(mock_data, match_id = 1455611)

  expect_true(is.list(result))
  expect_true("batting" %in% names(result))
  expect_true("bowling" %in% names(result))
})

test_that("parse_cricinfo_scorecard extracts batting entries", {
  mock_data <- create_mock_cricinfo_scorecard()
  result <- bouncer:::parse_cricinfo_scorecard(mock_data, match_id = 1455611)

  bat <- result$batting
  expect_s3_class(bat, "data.frame")
  expect_equal(nrow(bat), 2)
  expect_true(all(bat$match_id == 1455611))

  # Check Khawaja
  khawaja <- bat[bat$player_id == 50710, ]
  expect_equal(khawaja$runs, 65)
  expect_equal(khawaja$balls_faced, 120)
  expect_equal(khawaja$fours, 8)
  expect_equal(khawaja$position, 1)
  expect_equal(khawaja$dismissal, "c Crawley b Woakes")
  expect_equal(khawaja$dismissal_short, "caught")
  expect_equal(khawaja$minutes, 180)
  expect_true(khawaja$is_out)

  # Check Smith
  smith <- bat[bat$player_id == 50803, ]
  expect_equal(smith$runs, 142)
  expect_equal(smith$dismissal, "not out")
  expect_equal(smith$dismissal_short, "not out")
  expect_equal(smith$minutes, 350)
  expect_false(smith$is_out)
})

test_that("parse_cricinfo_scorecard extracts bowling entries", {
  mock_data <- create_mock_cricinfo_scorecard()
  result <- bouncer:::parse_cricinfo_scorecard(mock_data, match_id = 1455611)

  bowl <- result$bowling
  expect_s3_class(bowl, "data.frame")
  expect_equal(nrow(bowl), 1)

  woakes <- bowl[bowl$player_id == 20431, ]
  expect_equal(woakes$overs, 25)
  expect_equal(woakes$wickets, 3)
  expect_equal(woakes$maidens, 7)
  expect_equal(woakes$economy, 2.48)
})

test_that("parse_cricinfo_scorecard returns NULL for empty innings", {
  result <- bouncer:::parse_cricinfo_scorecard(list(content = list(innings = list())),
                                                match_id = 1455611)
  expect_null(result)

  result2 <- bouncer:::parse_cricinfo_scorecard(list(content = list(innings = NULL)),
                                                 match_id = 1455611)
  expect_null(result2)
})

# ============================================================================
# PARSE DETAILS TESTS
# ============================================================================

test_that("parse_cricinfo_details parses match metadata", {
  mock_data <- create_mock_cricinfo_details()
  result <- bouncer:::parse_cricinfo_details(mock_data, match_id = 1455611)

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 1)
  expect_equal(result$match_id, 1455611)
  expect_equal(result$format, "Test")
  expect_equal(result$status, "RESULT")
})

test_that("parse_cricinfo_details extracts ground information", {
  mock_data <- create_mock_cricinfo_details()
  result <- bouncer:::parse_cricinfo_details(mock_data, match_id = 1455611)

  expect_equal(result$ground_id, 131)
  expect_equal(result$ground_name, "Gabba")
  expect_equal(result$ground_city, "Brisbane")
  expect_equal(result$ground_country, "Australia")
})

test_that("parse_cricinfo_details extracts team information", {
  mock_data <- create_mock_cricinfo_details()
  result <- bouncer:::parse_cricinfo_details(mock_data, match_id = 1455611)

  expect_equal(result$team1_id, 2)
  expect_equal(result$team1_name, "Australia")
  expect_equal(result$team2_id, 1)
  expect_equal(result$team2_name, "England")
})

test_that("parse_cricinfo_details extracts toss and result", {
  mock_data <- create_mock_cricinfo_details()
  result <- bouncer:::parse_cricinfo_details(mock_data, match_id = 1455611)

  expect_equal(result$toss_winner_id, 2)
  expect_equal(result$toss_decision, "bat")
  expect_equal(result$result, "Australia")
})

test_that("parse_cricinfo_details handles NULL match data", {
  result <- bouncer:::parse_cricinfo_details(list(match = NULL), match_id = 1455611)
  expect_null(result)
})

# ============================================================================
# PARSE SCHEDULE TESTS
# ============================================================================

test_that("parse_cricinfo_schedule parses match listing", {
  mock_data <- create_mock_cricinfo_schedule()
  result <- bouncer:::parse_cricinfo_schedule(mock_data, series_id = 1455609)

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 2)
  expect_true(all(result$series_id == 1455609))
})

test_that("parse_cricinfo_schedule extracts match details", {
  mock_data <- create_mock_cricinfo_schedule()
  result <- bouncer:::parse_cricinfo_schedule(mock_data, series_id = 1455609)

  first <- result[result$match_id == 1455611, ]
  expect_equal(first$title, "1st Test")
  expect_equal(first$team1_name, "Australia")
  expect_equal(first$team2_name, "England")
  expect_equal(first$ground_name, "Gabba")
  expect_equal(first$start_date, "2025-11-21")
})

test_that("parse_cricinfo_schedule returns NULL for empty matches", {
  result <- bouncer:::parse_cricinfo_schedule(list(content = list(matches = list())),
                                               series_id = 1455609)
  expect_null(result)
})

# ============================================================================
# PLAYER MAP TESTS
# ============================================================================

test_that("cricinfo_player_map builds lookup from scorecard data", {
  mock_data <- create_mock_cricinfo_scorecard()
  pmap <- bouncer:::cricinfo_player_map(mock_data)

  expect_type(pmap, "list")
  expect_true(length(pmap) >= 3)
  expect_equal(pmap[["50710"]], "Usman Khawaja")
  expect_equal(pmap[["50803"]], "Steve Smith")
  expect_equal(pmap[["20431"]], "Chris Woakes")
})

test_that("cricinfo_player_map returns empty list for missing data", {
  pmap <- bouncer:::cricinfo_player_map(list(content = list(innings = NULL)))
  expect_type(pmap, "list")
  expect_equal(length(pmap), 0)
})

# ============================================================================
# PARSE OVER SUMMARIES TESTS
# ============================================================================

test_that("parse_cricinfo_over_summaries extracts per-over data", {
  mock_data <- create_mock_cricinfo_scorecard()
  result <- bouncer:::parse_cricinfo_over_summaries(mock_data, match_id = 1455611)

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 2)  # 2 overs in mock data
  expect_true(all(result$match_id == 1455611))
  expect_true(all(result$innings == 1))

  over1 <- result[result$over_number == 1, ]
  expect_equal(over1$over_runs, 4)
  expect_equal(over1$over_wickets, 1)
  expect_true(over1$is_complete)

  over2 <- result[result$over_number == 2, ]
  expect_equal(over2$over_runs, 8)
  expect_equal(over2$over_wickets, 0)
})

test_that("parse_cricinfo_over_summaries returns NULL for empty data", {
  result <- bouncer:::parse_cricinfo_over_summaries(
    list(content = list(innings = list())), match_id = 1455611
  )
  expect_null(result)
})

# ============================================================================
# PARSE BALLS FROM OVERS TESTS
# ============================================================================

test_that("parse_cricinfo_balls_from_overs extracts ball data", {
  mock_data <- create_mock_cricinfo_scorecard()
  innings_data <- mock_data$content$innings

  result <- bouncer:::parse_cricinfo_balls_from_overs(
    innings_data, match_id = 1455611, series_id = 1455609
  )

  # Only over 1 has balls in mock data
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 1)
  expect_equal(result$match_id[1], 1455611)
  expect_equal(result$series_id[1], 1455609)
  expect_equal(result$batsman_id[1], 50710)
  expect_equal(result$bowler_id[1], 20431)
  expect_equal(result$pitch_line[1], "OUTSIDE_OFFSTUMP")
})

test_that("parse_cricinfo_balls_from_overs uses player_map", {
  mock_data <- create_mock_cricinfo_scorecard()
  innings_data <- mock_data$content$innings
  player_map <- list("50710" = "Usman Khawaja", "20431" = "Chris Woakes")

  result <- bouncer:::parse_cricinfo_balls_from_overs(
    innings_data, match_id = 1455611, player_map = player_map
  )

  expect_equal(result$batsman_name[1], "Usman Khawaja")
  expect_equal(result$bowler_name[1], "Chris Woakes")
})

test_that("parse_cricinfo_balls_from_overs returns NULL for no ball data", {
  # Innings with overs but no balls
  innings_data <- list(
    list(
      inningNumber = 1,
      inningOvers = list(
        list(overNumber = 1, overRuns = 5, balls = NULL)
      )
    )
  )
  result <- bouncer:::parse_cricinfo_balls_from_overs(innings_data, match_id = 1)
  expect_null(result)
})

# ============================================================================
# AUTH TOKEN TESTS
# ============================================================================

test_that("cricinfo_token_expiry parses token expiry time", {
  token <- "exp=1735689600~hmac=abc123def456"
  expiry <- bouncer:::cricinfo_token_expiry(token)

  expect_s3_class(expiry, "POSIXct")
  expect_equal(as.numeric(expiry), 1735689600)
})

test_that("cricinfo_token_expiry returns NA for invalid tokens", {
  expect_true(is.na(bouncer:::cricinfo_token_expiry("invalid_token")))
  expect_true(is.na(bouncer:::cricinfo_token_expiry("")))
})

test_that("cricinfo_token_valid returns FALSE for NULL/empty tokens", {
  expect_false(bouncer:::cricinfo_token_valid(NULL))
  expect_false(bouncer:::cricinfo_token_valid(""))
})

test_that("cricinfo_token_valid detects expired tokens", {
  old_token <- "exp=1577836800~hmac=abc123"
  expect_false(bouncer:::cricinfo_token_valid(old_token))
})

test_that("cricinfo_token_valid accepts future tokens", {
  future_ts <- as.numeric(Sys.time()) + 86400
  future_token <- sprintf("exp=%d~hmac=abc123", as.integer(future_ts))
  expect_true(bouncer:::cricinfo_token_valid(future_token))
})

# ============================================================================
# CRICINFO_LIST_FORMATS TESTS
# ============================================================================

test_that("cricinfo_list_formats returns character vector by default", {
  result <- cricinfo_list_formats()

  expect_type(result, "character")
  expect_true(length(result) > 0)
  expect_true("T20" %in% result)
  expect_true("ODI" %in% result)
  expect_true("Test" %in% result)
})

test_that("cricinfo_list_formats returns data.frame with details=TRUE", {
  result <- cricinfo_list_formats(details = TRUE)

  expect_s3_class(result, "data.frame")
  expect_true("format" %in% names(result))
  expect_true("name" %in% names(result))
  expect_true("max_innings" %in% names(result))
})

test_that("cricinfo_list_formats has correct max_innings values", {
  result <- cricinfo_list_formats(details = TRUE)

  test_row <- result[result$format == "Test", ]
  expect_equal(test_row$max_innings, 4)

  t20_row <- result[result$format == "T20", ]
  expect_equal(t20_row$max_innings, 2)
})

# ============================================================================
# DATA TYPE CONSISTENCY TESTS
# ============================================================================

test_that("parsed commentary has correct column types", {
  mock_data <- create_mock_cricinfo_commentary()
  result <- bouncer:::parse_cricinfo_commentary(mock_data, match_id = 1455611)

  # Numeric columns
  expect_type(result$match_id, "double")
  expect_type(result$innings, "double")
  expect_type(result$batsman_runs, "double")
  expect_type(result$total_runs, "double")
  expect_type(result$wagon_x, "double")
  expect_type(result$predicted_score, "double")
  expect_type(result$win_probability, "double")

  # Character columns
  expect_type(result$batsman_name, "character")
  expect_type(result$bowler_name, "character")
  expect_type(result$pitch_line, "character")
  expect_type(result$pitch_length, "character")
  expect_type(result$shot_type, "character")
  expect_type(result$title, "character")

  # Logical columns
  expect_type(result$is_four, "logical")
  expect_type(result$is_six, "logical")
  expect_type(result$is_wicket, "logical")
})

test_that("parsed scorecard batting has correct column types", {
  mock_data <- create_mock_cricinfo_scorecard()
  result <- bouncer:::parse_cricinfo_scorecard(mock_data, match_id = 1455611)

  bat <- result$batting
  expect_type(bat$match_id, "double")
  expect_type(bat$player_name, "character")
  expect_type(bat$runs, "double")
  expect_type(bat$balls_faced, "double")
  expect_type(bat$dismissal, "character")
  expect_type(bat$dismissal_short, "character")
  expect_type(bat$minutes, "double")
  expect_type(bat$is_out, "logical")
})

test_that("parsed details has correct column types", {
  mock_data <- create_mock_cricinfo_details()
  result <- bouncer:::parse_cricinfo_details(mock_data, match_id = 1455611)

  expect_type(result$match_id, "double")
  expect_type(result$format, "character")
  expect_type(result$ground_name, "character")
  expect_type(result$team1_name, "character")
})

# ============================================================================
# CONFIGURATION TESTS
# ============================================================================

test_that("CRICINFO_FORMATS has all expected formats", {
  formats <- names(bouncer:::CRICINFO_FORMATS)
  expect_true("T20" %in% formats)
  expect_true("ODI" %in% formats)
  expect_true("Test" %in% formats)
})

test_that("CRICINFO_FORMATS has correct structure", {
  for (fmt_name in names(bouncer:::CRICINFO_FORMATS)) {
    config <- bouncer:::CRICINFO_FORMATS[[fmt_name]]
    expect_true("name" %in% names(config),
                info = paste("Missing name in", fmt_name))
    expect_true("max_innings" %in% names(config),
                info = paste("Missing max_innings in", fmt_name))
  }
})

test_that("CRICINFO_API_BASE is correct", {
  expect_equal(bouncer:::CRICINFO_API_BASE, "https://hs-consumer-api.espncricinfo.com")
})

# ============================================================================
# EDGE CASES
# ============================================================================

test_that("commentary parser handles extras (wides, noballs, byes)", {
  mock_data <- create_mock_cricinfo_commentary()
  wide_ball <- mock_data$comments[[1]]
  wide_ball$wides <- 1
  wide_ball$totalRuns <- 1
  wide_ball$batsmanRuns <- 0
  wide_ball$ballNumber <- 4
  wide_ball$oversActual <- 0.4
  mock_data$comments[[4]] <- wide_ball

  result <- bouncer:::parse_cricinfo_commentary(mock_data, match_id = 1455611)
  expect_equal(nrow(result), 4)

  wide_row <- result[result$ball_number == 4, ]
  expect_equal(wide_row$wides, 1)
  expect_equal(wide_row$batsman_runs, 0)
  expect_equal(wide_row$total_runs, 1)
})

test_that("commentary parser handles missing predictions", {
  mock_data <- create_mock_cricinfo_commentary()
  mock_data$comments[[1]]$predictions <- NULL

  result <- bouncer:::parse_cricinfo_commentary(mock_data, match_id = 1455611)
  expect_true(is.na(result$predicted_score[1]))
  expect_true(is.na(result$win_probability[1]))
})

test_that("scorecard parser handles innings with no bowlers", {
  mock_data <- create_mock_cricinfo_scorecard()
  mock_data$content$innings[[1]]$inningBowlers <- NULL

  result <- bouncer:::parse_cricinfo_scorecard(mock_data, match_id = 1455611)
  expect_s3_class(result$batting, "data.frame")
  expect_null(result$bowling)
})
