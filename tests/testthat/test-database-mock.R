# Tests for Database Functions Using Mock Data
#
# These tests use in-memory DuckDB instances with sample data,
# allowing them to run without the full bouncerdata database.
# This improves CI reliability and test coverage.

# ============================================================================
# HELPER: CREATE MOCK DATABASE
# ============================================================================

#' Create an in-memory DuckDB with sample cricket data
#' @keywords internal
create_mock_db <- function() {
  conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")

  # Create schema
  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS cricsheet")

  # Create matches table
  DBI::dbExecute(conn, "
    CREATE TABLE cricsheet.matches (
      match_id VARCHAR PRIMARY KEY,
      match_type VARCHAR,
      match_date DATE,
      team1 VARCHAR,
      team2 VARCHAR,
      venue VARCHAR,
      city VARCHAR,
      winner VARCHAR,
      toss_winner VARCHAR,
      toss_decision VARCHAR,
      player_of_match VARCHAR,
      competition VARCHAR
    )
  ")

  # Create players table
  DBI::dbExecute(conn, "
    CREATE TABLE cricsheet.players (
      player_id VARCHAR PRIMARY KEY,
      player_name VARCHAR,
      gender VARCHAR
    )
  ")

  # Create deliveries table
  DBI::dbExecute(conn, "
    CREATE TABLE cricsheet.deliveries (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      innings INTEGER,
      over_number INTEGER,
      ball_number INTEGER,
      batting_team VARCHAR,
      bowling_team VARCHAR,
      batter_id VARCHAR,
      bowler_id VARCHAR,
      non_striker_id VARCHAR,
      runs_batter INTEGER,
      runs_extras INTEGER,
      runs_total INTEGER,
      wicket_type VARCHAR,
      player_dismissed_id VARCHAR,
      is_wicket INTEGER
    )
  ")

  # Insert sample matches
  DBI::dbExecute(conn, "
    INSERT INTO cricsheet.matches VALUES
    ('M001', 'T20', '2024-01-15', 'India', 'Australia', 'MCG', 'Melbourne', 'India', 'India', 'bat', 'P001', 'International'),
    ('M002', 'ODI', '2024-01-20', 'England', 'South Africa', 'Lords', 'London', 'England', 'South Africa', 'field', 'P003', 'International'),
    ('M003', 'Test', '2024-02-01', 'India', 'England', 'Wankhede', 'Mumbai', NULL, 'India', 'bat', NULL, 'International')
  ")

  # Insert sample players
  DBI::dbExecute(conn, "
    INSERT INTO cricsheet.players VALUES
    ('P001', 'Virat Kohli', 'male'),
    ('P002', 'Rohit Sharma', 'male'),
    ('P003', 'Joe Root', 'male'),
    ('P004', 'Pat Cummins', 'male'),
    ('P005', 'Jasprit Bumrah', 'male'),
    ('P006', 'Smriti Mandhana', 'female')
  ")

  # Insert sample deliveries
  DBI::dbExecute(conn, "
    INSERT INTO cricsheet.deliveries VALUES
    ('M001_India_1_001_01', 'M001', 1, 1, 1, 'India', 'Australia', 'P002', 'P004', 'P001', 4, 0, 4, NULL, NULL, 0),
    ('M001_India_1_001_02', 'M001', 1, 1, 2, 'India', 'Australia', 'P002', 'P004', 'P001', 0, 0, 0, NULL, NULL, 0),
    ('M001_India_1_001_03', 'M001', 1, 1, 3, 'India', 'Australia', 'P002', 'P004', 'P001', 1, 0, 1, NULL, NULL, 0),
    ('M001_India_1_001_04', 'M001', 1, 1, 4, 'India', 'Australia', 'P001', 'P004', 'P002', 6, 0, 6, NULL, NULL, 0),
    ('M001_India_1_001_05', 'M001', 1, 1, 5, 'India', 'Australia', 'P001', 'P004', 'P002', 0, 0, 0, 'caught', 'P001', 1),
    ('M001_India_1_001_06', 'M001', 1, 1, 6, 'India', 'Australia', 'P003', 'P004', 'P002', 2, 0, 2, NULL, NULL, 0)
  ")

  conn
}

# ============================================================================
# MOCK DATABASE STRUCTURE TESTS
# ============================================================================

test_that("mock database has correct table structure", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  tables <- DBI::dbGetQuery(conn, "
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'cricsheet'
  ")$table_name

  expect_true("matches" %in% tables)
  expect_true("players" %in% tables)
  expect_true("deliveries" %in% tables)
})

test_that("mock database has sample matches", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  matches <- DBI::dbGetQuery(conn, "SELECT * FROM cricsheet.matches")

  expect_equal(nrow(matches), 3)
  expect_true("M001" %in% matches$match_id)
  expect_true("T20" %in% matches$match_type)
  expect_true("ODI" %in% matches$match_type)
  expect_true("Test" %in% matches$match_type)
})

test_that("mock database has sample players", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  players <- DBI::dbGetQuery(conn, "SELECT * FROM cricsheet.players")

  expect_equal(nrow(players), 6)
  expect_true("Virat Kohli" %in% players$player_name)
  expect_true("Rohit Sharma" %in% players$player_name)
})

test_that("mock database has sample deliveries", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  deliveries <- DBI::dbGetQuery(conn, "SELECT * FROM cricsheet.deliveries")

  expect_equal(nrow(deliveries), 6)
  expect_true("M001_India_1_001_01" %in% deliveries$delivery_id)
})

# ============================================================================
# QUERY TESTS WITH MOCK DATA
# ============================================================================

test_that("can query matches by format using mock db", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  t20_matches <- DBI::dbGetQuery(conn, "
    SELECT * FROM cricsheet.matches WHERE match_type = 'T20'
  ")

  expect_equal(nrow(t20_matches), 1)
  expect_equal(t20_matches$team1[1], "India")
  expect_equal(t20_matches$team2[1], "Australia")
})

test_that("can query players by name using mock db", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Parameterized query pattern
  player <- DBI::dbGetQuery(conn, "
    SELECT * FROM cricsheet.players WHERE LOWER(player_name) LIKE LOWER(?)
  ", params = list("%Kohli%"))

  expect_equal(nrow(player), 1)
  expect_equal(player$player_id[1], "P001")
})

test_that("can aggregate delivery statistics using mock db", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  stats <- DBI::dbGetQuery(conn, "
    SELECT
      batter_id,
      COUNT(*) as balls_faced,
      SUM(runs_batter) as total_runs,
      SUM(is_wicket) as dismissals
    FROM cricsheet.deliveries
    GROUP BY batter_id
  ")

  expect_gte(nrow(stats), 1)
  expect_true("balls_faced" %in% names(stats))
  expect_true("total_runs" %in% names(stats))
})

test_that("can join tables using mock db", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  result <- DBI::dbGetQuery(conn, "
    SELECT
      p.player_name,
      COUNT(d.delivery_id) as deliveries
    FROM cricsheet.players p
    LEFT JOIN cricsheet.deliveries d ON p.player_id = d.batter_id
    GROUP BY p.player_id, p.player_name
  ")

  expect_gte(nrow(result), 1)
  expect_true("player_name" %in% names(result))
  expect_true("deliveries" %in% names(result))
})

# ============================================================================
# SQL INJECTION PREVENTION TESTS
# ============================================================================

test_that("parameterized queries prevent SQL injection", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # This should NOT execute the DROP TABLE
  malicious_input <- "'; DROP TABLE players; --"

  result <- DBI::dbGetQuery(conn, "
    SELECT * FROM cricsheet.players WHERE player_name = ?
  ", params = list(malicious_input))

  # Should return empty result, not execute DROP TABLE

  expect_equal(nrow(result), 0)

  # Verify players table still exists
  tables <- DBI::dbGetQuery(conn, "
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'cricsheet'
  ")$table_name
  expect_true("players" %in% tables)

  # Verify players still have data
  remaining <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM cricsheet.players")
  expect_equal(remaining$n, 6)
})

test_that("LIKE queries with parameterization are safe", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Injection attempt through LIKE
  malicious_input <- "%' OR '1'='1"

  result <- DBI::dbGetQuery(conn, "
    SELECT * FROM cricsheet.players WHERE player_name LIKE ?
  ", params = list(malicious_input))

  # Should return 0 rows (literal match), not all rows
  expect_equal(nrow(result), 0)
})

# ============================================================================
# EDGE CASE HANDLING TESTS
# ============================================================================

test_that("empty result handling", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  result <- DBI::dbGetQuery(conn, "
    SELECT * FROM cricsheet.matches WHERE match_type = 'NONEXISTENT'
  ")

  expect_equal(nrow(result), 0)
  expect_true(is.data.frame(result))
})

test_that("NULL value handling", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  result <- DBI::dbGetQuery(conn, "
    SELECT * FROM cricsheet.matches WHERE winner IS NULL
  ")

  # Test match (M003) has NULL winner (ongoing)
  expect_equal(nrow(result), 1)
  expect_equal(result$match_id[1], "M003")
})

test_that("aggregate with empty groups", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  result <- DBI::dbGetQuery(conn, "
    SELECT
      match_type,
      COUNT(*) as match_count,
      AVG(CASE WHEN winner IS NOT NULL THEN 1 ELSE 0 END) as completion_rate
    FROM cricsheet.matches
    GROUP BY match_type
  ")

  expect_equal(nrow(result), 3)
  expect_true("completion_rate" %in% names(result))
})

# ============================================================================
# DATA TYPE HANDLING TESTS
# ============================================================================

test_that("date columns are properly typed", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  result <- DBI::dbGetQuery(conn, "SELECT match_date FROM cricsheet.matches")

  # DuckDB returns dates as Date class
  expect_true(inherits(result$match_date, "Date"))
})

test_that("integer columns are properly typed", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  result <- DBI::dbGetQuery(conn, "SELECT runs_batter FROM cricsheet.deliveries")

  expect_type(result$runs_batter, "integer")
})

test_that("character columns are properly typed", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  result <- DBI::dbGetQuery(conn, "SELECT player_name FROM cricsheet.players")

  expect_type(result$player_name, "character")
})

# ============================================================================
# DELIVERY ID FORMAT TESTS
# ============================================================================

test_that("delivery IDs follow expected format", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  deliveries <- DBI::dbGetQuery(conn, "SELECT delivery_id FROM cricsheet.deliveries")

  # Format: {match_id}_{batting_team}_{innings}_{over}_{ball}
  pattern <- "^[A-Z0-9]+_[A-Za-z]+_\\d+_\\d{3}_\\d{2}$"

  for (id in deliveries$delivery_id) {
    expect_true(grepl(pattern, id),
                info = paste("ID doesn't match format:", id))
  }
})

test_that("delivery IDs can be parsed for components", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  delivery_id <- "M001_India_1_001_01"

  # Parse components
  parts <- strsplit(delivery_id, "_")[[1]]
  expect_equal(length(parts), 5)

  match_id <- parts[1]
  batting_team <- parts[2]
  innings <- as.integer(parts[3])
  over <- as.integer(parts[4])
  ball <- as.integer(parts[5])

  expect_equal(match_id, "M001")
  expect_equal(batting_team, "India")
  expect_equal(innings, 1)
  expect_equal(over, 1)
  expect_equal(ball, 1)
})

# ============================================================================
# CONCURRENT CONNECTION TESTS
# ============================================================================

test_that("multiple read connections work", {
  conn1 <- create_mock_db()
  conn2 <- DBI::dbConnect(duckdb::duckdb(), ":memory:")

  on.exit({
    DBI::dbDisconnect(conn1, shutdown = TRUE)
    DBI::dbDisconnect(conn2, shutdown = TRUE)
  })

  # Both connections should work independently
  result1 <- DBI::dbGetQuery(conn1, "SELECT COUNT(*) as n FROM cricsheet.matches")
  result2 <- DBI::dbGetQuery(conn2, "SELECT 1 as test")

  expect_equal(result1$n, 3)
  expect_equal(result2$test, 1)
})

# ============================================================================
# TRANSACTION TESTS
# ============================================================================

test_that("transaction rollback works", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  DBI::dbBegin(conn)

  DBI::dbExecute(conn, "DELETE FROM cricsheet.players WHERE player_id = 'P001'")

  # Before rollback, player should be gone
  during <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM cricsheet.players")
  expect_equal(during$n, 5)

  DBI::dbRollback(conn)

  # After rollback, player should be back
  after <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM cricsheet.players")
  expect_equal(after$n, 6)
})

test_that("transaction commit persists changes", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  DBI::dbBegin(conn)

  DBI::dbExecute(conn, "
    INSERT INTO cricsheet.players VALUES ('P007', 'Test Player', 'male')
  ")

  DBI::dbCommit(conn)

  # After commit, player should exist
  result <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM cricsheet.players")
  expect_equal(result$n, 7)
})

# ============================================================================
# STATISTICS CALCULATION TESTS
# ============================================================================

test_that("batting statistics can be calculated", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  stats <- DBI::dbGetQuery(conn, "
    SELECT
      batter_id,
      COUNT(*) as balls_faced,
      SUM(runs_batter) as total_runs,
      SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END) as fours,
      SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END) as sixes,
      SUM(CASE WHEN runs_batter = 0 AND is_wicket = 0 THEN 1 ELSE 0 END) as dots,
      SUM(is_wicket) as dismissals
    FROM cricsheet.deliveries
    GROUP BY batter_id
  ")

  # P001 (Kohli): 1 six, 1 wicket in sample data
  kohli <- stats[stats$batter_id == "P001", ]
  if (nrow(kohli) > 0) {
    expect_equal(kohli$sixes, 1)
    expect_equal(kohli$dismissals, 1)
  }

  # P002 (Sharma): 1 four in sample data
  sharma <- stats[stats$batter_id == "P002", ]
  if (nrow(sharma) > 0) {
    expect_equal(sharma$fours, 1)
  }
})

test_that("bowling statistics can be calculated", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  stats <- DBI::dbGetQuery(conn, "
    SELECT
      bowler_id,
      COUNT(*) as balls_bowled,
      SUM(runs_total) as runs_conceded,
      SUM(is_wicket) as wickets
    FROM cricsheet.deliveries
    GROUP BY bowler_id
  ")

  # P004 (Cummins): bowled all deliveries in sample
  cummins <- stats[stats$bowler_id == "P004", ]
  if (nrow(cummins) > 0) {
    expect_equal(cummins$balls_bowled, 6)
    expect_equal(cummins$wickets, 1)
  }
})

# ============================================================================
# EXPECTED VALUE CALCULATION TESTS
# ============================================================================

test_that("strike rate calculation", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Strike rate = (runs / balls) * 100
  result <- DBI::dbGetQuery(conn, "
    SELECT
      batter_id,
      SUM(runs_batter) as runs,
      COUNT(*) as balls,
      CAST(SUM(runs_batter) AS DOUBLE) / COUNT(*) * 100 as strike_rate
    FROM cricsheet.deliveries
    WHERE batter_id = 'P001'
    GROUP BY batter_id
  ")

  if (nrow(result) > 0) {
    # P001 scored 6 runs off 2 balls = SR 300
    expect_equal(result$strike_rate, 300, tolerance = 0.01)
  }
})

test_that("economy rate calculation", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Economy = (runs conceded / balls) * 6
  result <- DBI::dbGetQuery(conn, "
    SELECT
      bowler_id,
      SUM(runs_total) as runs,
      COUNT(*) as balls,
      CAST(SUM(runs_total) AS DOUBLE) / COUNT(*) * 6 as economy
    FROM cricsheet.deliveries
    WHERE bowler_id = 'P004'
    GROUP BY bowler_id
  ")

  if (nrow(result) > 0) {
    # P004 conceded 13 runs in 6 balls = 13.0 economy
    expect_equal(result$economy, 13.0, tolerance = 0.01)
  }
})

# ============================================================================
# WICKET TYPE HANDLING TESTS
# ============================================================================

test_that("wicket types are properly tracked", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  wickets <- DBI::dbGetQuery(conn, "
    SELECT wicket_type, player_dismissed_id
    FROM cricsheet.deliveries
    WHERE is_wicket = 1
  ")

  expect_equal(nrow(wickets), 1)
  expect_equal(wickets$wicket_type[1], "caught")
  expect_equal(wickets$player_dismissed_id[1], "P001")
})

test_that("non-wicket deliveries have NULL wicket fields", {
  conn <- create_mock_db()
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  non_wickets <- DBI::dbGetQuery(conn, "
    SELECT wicket_type, player_dismissed_id
    FROM cricsheet.deliveries
    WHERE is_wicket = 0
  ")

  expect_true(all(is.na(non_wickets$wicket_type)))
  expect_true(all(is.na(non_wickets$player_dismissed_id)))
})
