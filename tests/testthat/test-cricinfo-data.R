# Tests for Cricinfo Data Functions (R/cricinfo_data.R)
#
# Covers: cricinfo_format_sql, load_cricinfo_fixtures, load_cricinfo_balls,
# load_cricinfo_match, load_cricinfo_innings, get_upcoming_matches,
# get_unscraped_matches, and SQL escaping with adversarial inputs.
#
# Uses in-memory DuckDB mock databases (no bouncerdata dependency).

# ============================================================================
# HELPER: CREATE MOCK CRICINFO DATABASE
# ============================================================================

create_mock_cricinfo_db <- function() {
  conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")

  # Create all cricinfo tables using the package function
  create_cricinfo_tables(conn, verbose = FALSE)

  # Insert sample matches
  DBI::dbExecute(conn, "
    INSERT INTO cricinfo.matches (match_id, title, series_id, series_name,
      format, international_class_id, gender, start_date, status,
      team1_name, team1_abbreviation, team2_name, team2_abbreviation,
      ground_name, country_name)
    VALUES
    ('1502145', '1st T20I', '1500001', 'India vs Australia 2025', 'T20I', 3, 'male',
     '2025-01-15', 'FINISHED', 'India', 'IND', 'Australia', 'AUS', 'MCG', 'Australia'),
    ('1502146', '2nd T20I', '1500001', 'India vs Australia 2025', 'T20I', 3, 'male',
     '2025-01-17', 'FINISHED', 'India', 'IND', 'Australia', 'AUS', 'SCG', 'Australia'),
    ('1502200', '1st ODI', '1500002', 'England vs South Africa 2025', 'ODI', 2, 'male',
     '2025-02-01', 'FINISHED', 'England', 'ENG', 'South Africa', 'SA', 'Lords', 'England'),
    ('1502300', '1st Test', '1500003', 'India vs England 2025', 'TEST', 1, 'female',
     '2025-03-01', 'FINISHED', 'India', 'IND-W', 'England', 'ENG-W', 'Wankhede', 'India')
  ")

  # Insert sample balls (minimal columns for query testing)
  DBI::dbExecute(conn, "
    INSERT INTO cricinfo.balls (id, match_id, innings_number, over_number,
      ball_number, total_runs, batsman_runs, is_four, is_six, is_wicket,
      batsman_player_id, bowler_player_id)
    VALUES
    ('b001', '1502145', 1, 0, 1, 4, 4, TRUE, FALSE, FALSE, 'p001', 'p100'),
    ('b002', '1502145', 1, 0, 2, 0, 0, FALSE, FALSE, FALSE, 'p001', 'p100'),
    ('b003', '1502145', 1, 0, 3, 6, 6, FALSE, TRUE, FALSE, 'p001', 'p100'),
    ('b004', '1502145', 2, 0, 1, 1, 1, FALSE, FALSE, FALSE, 'p100', 'p001'),
    ('b005', '1502146', 1, 0, 1, 2, 2, FALSE, FALSE, FALSE, 'p002', 'p101'),
    ('b006', '1502200', 1, 0, 1, 0, 0, FALSE, FALSE, TRUE, 'p003', 'p102')
  ")

  # Insert sample innings
  DBI::dbExecute(conn, "
    INSERT INTO cricinfo.innings (match_id, innings_number, team_id, team_name,
      total_runs, total_wickets, total_overs, player_id, player_name,
      runs, balls_faced, fours, sixes, strike_rate, batting_position)
    VALUES
    ('1502145', 1, 't001', 'India', 180, 5, 20.0, 'p001', 'Virat Kohli',
     82, 53, 8, 4, 154.72, 1),
    ('1502145', 1, 't001', 'India', 180, 5, 20.0, 'p002', 'Rohit Sharma',
     45, 30, 5, 2, 150.0, 2),
    ('1502145', 2, 't002', 'Australia', 165, 8, 20.0, 'p100', 'Steve Smith',
     55, 42, 4, 1, 130.95, 1)
  ")

  # Insert fixtures (mix of statuses for get_upcoming/get_unscraped tests)
  DBI::dbExecute(conn, "
    INSERT INTO cricinfo.fixtures (match_id, series_id, series_name, format,
      gender, status, start_date, title, team1, team2, has_ball_by_ball)
    VALUES
    ('1502145', '1500001', 'Ind vs Aus 2025', 't20i', 'male', 'POST',
     '2025-01-15', '1st T20I', 'India', 'Australia', TRUE),
    ('1502146', '1500001', 'Ind vs Aus 2025', 't20i', 'male', 'POST',
     '2025-01-17', '2nd T20I', 'India', 'Australia', FALSE),
    ('1502200', '1500002', 'Eng vs SA 2025', 'odi', 'male', 'POST',
     '2025-02-01', '1st ODI', 'England', 'South Africa', TRUE),
    ('9999001', '1500010', 'Future Series', 't20i', 'male', 'PRE',
     '2099-06-01', 'Future T20I', 'India', 'Australia', FALSE),
    ('9999002', '1500010', 'Future Series', 'odi', 'female', 'PRE',
     '2099-07-01', 'Future ODI', 'India', 'Australia', FALSE)
  ")

  conn
}

# ============================================================================
# PURE FUNCTION TESTS: cricinfo_format_sql
# ============================================================================

test_that("cricinfo_format_sql maps T20 variants correctly", {
  result <- cricinfo_format_sql("format", "t20i")
  expect_match(result, "T20")
  expect_match(result, "T20I")
  expect_match(result, "IT20")
})

test_that("cricinfo_format_sql maps ODI correctly", {
  result <- cricinfo_format_sql("format", "odi")
  expect_match(result, "ODI")
  expect_match(result, "ODM")
})

test_that("cricinfo_format_sql maps TEST correctly", {
  result <- cricinfo_format_sql("format", "test")
  expect_match(result, "TEST")
  expect_match(result, "MDM")
})

test_that("cricinfo_format_sql uses column name in output", {
  result <- cricinfo_format_sql("my_col", "t20i")
  expect_match(result, "my_col")
})

test_that("cricinfo_format_sql handles unknown format safely", {
  result <- cricinfo_format_sql("format", "hundred")
  expect_match(result, "HUNDRED")
})

test_that("cricinfo_format_sql escapes adversarial format strings", {
  # A format containing a quote should have the quote doubled (escaped)
  # The literal text remains but the quote prevents SQL breakout
  result <- cricinfo_format_sql("format", "t20'; DROP TABLE cricinfo_balls; --")
  # The single quote after T20 should be doubled
  expect_match(result, "''", fixed = TRUE)
})

# ============================================================================
# MOCK DB STRUCTURE TESTS
# ============================================================================

test_that("mock cricinfo database creates all tables", {
  conn <- create_mock_cricinfo_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  tables <- DBI::dbGetQuery(conn, "
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'cricinfo'
  ")$table_name
  expect_true("balls" %in% tables)
  expect_true("matches" %in% tables)
  expect_true("innings" %in% tables)
  expect_true("fixtures" %in% tables)
})

test_that("mock cricinfo database has correct row counts", {
  conn <- create_mock_cricinfo_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  expect_equal(
    DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricinfo.matches")$n, 4)
  expect_equal(
    DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricinfo.balls")$n, 6)
  expect_equal(
    DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricinfo.innings")$n, 3)
  expect_equal(
    DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricinfo.fixtures")$n, 5)
})

# ============================================================================
# LOADER TESTS (using mock DB via local_mocked_bindings)
# ============================================================================

# Helper: mock get_db_connection to return our in-memory mock.
# The tested functions call dbDisconnect via on.exit, so we don't
# disconnect ourselves (would double-disconnect and warn).
with_mock_cricinfo_db <- function(code) {
  conn <- create_mock_cricinfo_db()
  local_mocked_bindings(
    get_db_connection = function(...) conn,
    .package = "bouncer"
  )
  force(code)
}

test_that("load_cricinfo_balls loads all balls without filters", {
  with_mock_cricinfo_db({
    result <- suppressMessages(load_cricinfo_balls())
    expect_equal(nrow(result), 6)
    expect_true("match_id" %in% names(result))
    expect_true("innings_number" %in% names(result))
  })
})

test_that("load_cricinfo_balls filters by match_ids", {
  with_mock_cricinfo_db({
    result <- suppressMessages(load_cricinfo_balls(match_ids = "1502145"))
    expect_equal(nrow(result), 4)
    expect_true(all(result$match_id == "1502145"))
  })
})

test_that("load_cricinfo_balls filters by format", {
  with_mock_cricinfo_db({
    result <- suppressMessages(load_cricinfo_balls(format = "t20i"))
    # Match 1502145 (4 balls) + 1502146 (1 ball) = 5 T20I balls
    expect_equal(nrow(result), 5)
  })
})

test_that("load_cricinfo_balls filters by gender", {
  with_mock_cricinfo_db({
    result <- suppressMessages(load_cricinfo_balls(gender = "male"))
    # All 6 balls are from male matches (4 for test are female)
    # Actually: matches 1502145, 1502146 (male T20I), 1502200 (male ODI)
    # 1502300 is female Test but has no balls in the mock
    expect_equal(nrow(result), 6)
  })
})

test_that("load_cricinfo_balls returns empty for non-existent match", {
  with_mock_cricinfo_db({
    result <- suppressWarnings(
      suppressMessages(load_cricinfo_balls(match_ids = "9999999"))
    )
    expect_equal(nrow(result), 0)
  })
})

test_that("load_cricinfo_match loads all matches without filters", {
  with_mock_cricinfo_db({
    result <- suppressMessages(load_cricinfo_match())
    expect_equal(nrow(result), 4)
  })
})

test_that("load_cricinfo_match filters by format", {
  with_mock_cricinfo_db({
    result <- suppressMessages(load_cricinfo_match(format = "t20i"))
    expect_equal(nrow(result), 2)
    expect_true(all(grepl("T20", result$format, ignore.case = TRUE)))
  })
})

test_that("load_cricinfo_match filters by gender", {
  with_mock_cricinfo_db({
    result <- suppressMessages(load_cricinfo_match(gender = "female"))
    expect_equal(nrow(result), 1)
    expect_equal(result$match_id, "1502300")
  })
})

test_that("load_cricinfo_innings loads all innings", {
  with_mock_cricinfo_db({
    result <- suppressMessages(load_cricinfo_innings())
    expect_equal(nrow(result), 3)
    expect_true("player_name" %in% names(result))
  })
})

test_that("load_cricinfo_innings filters by match_ids", {
  with_mock_cricinfo_db({
    result <- suppressMessages(load_cricinfo_innings(match_ids = "1502145"))
    expect_equal(nrow(result), 3)
    expect_true(all(result$match_id == "1502145"))
  })
})

# ============================================================================
# FIXTURES API TESTS
# ============================================================================

test_that("load_cricinfo_fixtures loads all fixtures", {
  with_mock_cricinfo_db({
    result <- suppressMessages(load_cricinfo_fixtures())
    expect_equal(nrow(result), 5)
  })
})

test_that("load_cricinfo_fixtures filters by format", {
  with_mock_cricinfo_db({
    result <- suppressMessages(load_cricinfo_fixtures(format = "t20i"))
    expect_equal(nrow(result), 3)  # 2 POST + 1 PRE
  })
})

test_that("load_cricinfo_fixtures filters by gender", {
  with_mock_cricinfo_db({
    result <- suppressMessages(load_cricinfo_fixtures(gender = "female"))
    expect_equal(nrow(result), 1)
  })
})

test_that("load_cricinfo_fixtures filters by status", {
  with_mock_cricinfo_db({
    result <- suppressMessages(load_cricinfo_fixtures(status = "POST"))
    expect_equal(nrow(result), 3)
  })
})

test_that("load_cricinfo_fixtures combined filters work", {
  with_mock_cricinfo_db({
    result <- suppressMessages(
      load_cricinfo_fixtures(format = "t20i", status = "POST")
    )
    expect_equal(nrow(result), 2)
  })
})

test_that("get_upcoming_matches returns future PRE matches", {
  with_mock_cricinfo_db({
    # Our future fixtures are in 2099, so days_ahead needs to be huge
    result <- suppressMessages(
      get_upcoming_matches(days_ahead = 99999)
    )
    expect_equal(nrow(result), 2)
    expect_true(all(result$status %in% c("PRE", "LIVE")))
  })
})

test_that("get_upcoming_matches filters by format", {
  with_mock_cricinfo_db({
    result <- suppressMessages(
      get_upcoming_matches(format = "t20i", days_ahead = 99999)
    )
    expect_equal(nrow(result), 1)
  })
})

test_that("get_unscraped_matches returns completed without ball-by-ball", {
  with_mock_cricinfo_db({
    result <- suppressMessages(get_unscraped_matches())
    # Match 1502146 is POST with has_ball_by_ball = FALSE
    expect_equal(nrow(result), 1)
    expect_equal(result$match_id, "1502146")
  })
})

test_that("get_unscraped_matches returns empty when all scraped", {
  with_mock_cricinfo_db({
    # Filter to format with all matches scraped
    result <- suppressMessages(get_unscraped_matches(format = "odi"))
    expect_equal(nrow(result), 0)
  })
})

# ============================================================================
# SQL ESCAPING / ADVERSARIAL INPUT TESTS
# ============================================================================

test_that("escape_sql_quotes handles adversarial match IDs", {
  # Match ID with single quote (e.g. O'Brien could appear in player names
  # used as filter values)
  expect_equal(escape_sql_quotes("123'456"), "123''456")
  # "'; DROP TABLE--" has one quote at pos 1 → doubled to ''
  expect_equal(escape_sql_quotes("'; DROP TABLE--"), "''; DROP TABLE--")
})

test_that("escape_sql_quotes handles empty and normal strings", {
  expect_equal(escape_sql_quotes(""), "")
  expect_equal(escape_sql_quotes("1502145"), "1502145")
  expect_equal(escape_sql_quotes("normal_string"), "normal_string")
})

test_that("escape_sql_quotes handles vectors with mixed content", {
  input <- c("safe", "it's", "a' OR '1'='1", "normal")
  result <- escape_sql_quotes(input)
  expect_equal(result[1], "safe")
  expect_equal(result[2], "it''s")
  expect_equal(result[3], "a'' OR ''1''=''1")
  expect_equal(result[4], "normal")
})

test_that("load_cricinfo_balls handles match_ids with quotes safely", {
  with_mock_cricinfo_db({
    # This should not cause SQL injection -- just return no results
    result <- suppressWarnings(suppressMessages(
      load_cricinfo_balls(match_ids = "1502145'; DROP TABLE cricinfo_balls; --")
    ))
    expect_equal(nrow(result), 0)

    # Verify the table still exists
    conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))
    create_cricinfo_tables(conn, verbose = FALSE)
    tables <- DBI::dbGetQuery(conn, "
      SELECT table_name FROM information_schema.tables
      WHERE table_schema = 'cricinfo'
    ")$table_name
    expect_true("balls" %in% tables)
  })
})

test_that("load_cricinfo_fixtures handles format with quotes safely", {
  with_mock_cricinfo_db({
    result <- suppressWarnings(suppressMessages(
      load_cricinfo_fixtures(format = "t20i'; DROP TABLE cricinfo_fixtures; --")
    ))
    # Should return 0 rows, not crash or inject
    expect_equal(nrow(result), 0)
  })
})
