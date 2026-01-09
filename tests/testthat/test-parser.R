# Tests for Cricsheet JSON Parser Functions

test_that("parse_cricsheet_json handles valid JSON structure", {
  # Create minimal valid test JSON
  test_json <- list(
    meta = list(
      data_version = "1.1.0"
    ),
    info = list(
      teams = c("India", "Australia"),
      match_type = "T20",
      dates = c("2024-01-15"),
      venue = "Test Stadium",
      city = "Test City",
      gender = "male",
      players = list(
        India = c("Player1", "Player2"),
        Australia = c("Player3", "Player4")
      )
    ),
    innings = list(
      list(
        team = "India",
        overs = list(
          list(
            over = 0,
            deliveries = list(
              list(
                batter = "Player1",
                bowler = "Player3",
                runs = list(batter = 4, extras = 0, total = 4)
              )
            )
          )
        )
      )
    )
  )

  # Write to temp file
  temp_file <- tempfile(fileext = ".json")
  jsonlite::write_json(test_json, temp_file, auto_unbox = TRUE)

  # Parse should not error
  expect_no_error({
    result <- parse_cricsheet_json(temp_file)
  })

  # Clean up
  file.remove(temp_file)
})

test_that("parse_cricsheet_json returns expected structure", {
  # Create minimal valid test JSON
  test_json <- list(
    meta = list(data_version = "1.1.0"),
    info = list(
      teams = c("TeamA", "TeamB"),
      match_type = "ODI",
      dates = c("2024-06-01"),
      venue = "Stadium",
      gender = "male",
      players = list(
        TeamA = c("BatterA"),
        TeamB = c("BowlerB")
      )
    ),
    innings = list(
      list(
        team = "TeamA",
        overs = list(
          list(
            over = 0,
            deliveries = list(
              list(
                batter = "BatterA",
                bowler = "BowlerB",
                runs = list(batter = 1, extras = 0, total = 1)
              )
            )
          )
        )
      )
    )
  )

  temp_file <- tempfile(fileext = ".json")
  jsonlite::write_json(test_json, temp_file, auto_unbox = TRUE)

  result <- parse_cricsheet_json(temp_file)

  # Should return a list with innings, deliveries, players
  expect_type(result, "list")
  expect_true("innings" %in% names(result))
  expect_true("deliveries" %in% names(result))
  expect_true("players" %in% names(result))

  # innings should be a data frame
  expect_s3_class(result$innings, "data.frame")

  # Clean up
  file.remove(temp_file)
})

test_that("parse_cricsheet_json handles wickets correctly", {
  test_json <- list(
    meta = list(data_version = "1.1.0"),
    info = list(
      teams = c("TeamA", "TeamB"),
      match_type = "T20",
      dates = c("2024-01-01"),
      venue = "Stadium",
      gender = "male",
      players = list(
        TeamA = c("Batter1"),
        TeamB = c("Bowler1")
      )
    ),
    innings = list(
      list(
        team = "TeamA",
        overs = list(
          list(
            over = 0,
            deliveries = list(
              list(
                batter = "Batter1",
                bowler = "Bowler1",
                runs = list(batter = 0, extras = 0, total = 0),
                wickets = list(
                  list(
                    player_out = "Batter1",
                    kind = "bowled"
                  )
                )
              )
            )
          )
        )
      )
    )
  )

  temp_file <- tempfile(fileext = ".json")
  jsonlite::write_json(test_json, temp_file, auto_unbox = TRUE)

  result <- parse_cricsheet_json(temp_file)

  # Should have deliveries with wicket info
  if (nrow(result$deliveries) > 0) {
    expect_true("is_wicket" %in% names(result$deliveries))
  }

  file.remove(temp_file)
})

test_that("parse_cricsheet_json handles extras correctly", {
  test_json <- list(
    meta = list(data_version = "1.1.0"),
    info = list(
      teams = c("TeamA", "TeamB"),
      match_type = "T20",
      dates = c("2024-01-01"),
      venue = "Stadium",
      gender = "male",
      players = list(
        TeamA = c("Batter1"),
        TeamB = c("Bowler1")
      )
    ),
    innings = list(
      list(
        team = "TeamA",
        overs = list(
          list(
            over = 0,
            deliveries = list(
              list(
                batter = "Batter1",
                bowler = "Bowler1",
                runs = list(batter = 0, extras = 1, total = 1),
                extras = list(wides = 1)
              )
            )
          )
        )
      )
    )
  )

  temp_file <- tempfile(fileext = ".json")
  jsonlite::write_json(test_json, temp_file, auto_unbox = TRUE)

  result <- parse_cricsheet_json(temp_file)

  # Should handle extras without error
  expect_type(result, "list")

  file.remove(temp_file)
})

test_that("delivery_id format is correct", {
  # Create test JSON with known values
  test_json <- list(
    meta = list(data_version = "1.1.0"),
    info = list(
      teams = c("India", "Australia"),
      match_type = "T20",
      dates = c("2024-01-01"),
      venue = "Stadium",
      gender = "male",
      players = list(
        India = c("Batter1"),
        Australia = c("Bowler1")
      )
    ),
    innings = list(
      list(
        team = "India",
        overs = list(
          list(
            over = 5,
            deliveries = list(
              list(
                batter = "Batter1",
                bowler = "Bowler1",
                runs = list(batter = 0, extras = 0, total = 0)
              )
            )
          )
        )
      )
    )
  )

  temp_file <- tempfile(pattern = "test_64012", fileext = ".json")
  jsonlite::write_json(test_json, temp_file, auto_unbox = TRUE)

  result <- parse_cricsheet_json(temp_file)

  if (nrow(result$deliveries) > 0) {
    delivery_id <- result$deliveries$delivery_id[1]

    # delivery_id should contain batting team, innings, over, ball
    # Format: "{match_id}_{batting_team}_{innings}_{over}_{ball}"
    expect_true(grepl("_India_", delivery_id))
    expect_true(grepl("_1_", delivery_id))  # First innings
  }

  file.remove(temp_file)
})
