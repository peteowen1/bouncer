# Test helper: creates a minimal DuckDB fixture for stat function tests.
#
# This runs automatically before tests (testthat loads helper-*.R files).
# Provides get_fixture_db_path() which returns a temp DuckDB with ~5 matches
# and ~60 deliveries — enough to exercise player/team/venue stat queries.

# Build fixture DB once per test session
.fixture_env <- new.env(parent = emptyenv())

get_fixture_db_path <- function() {
  if (!is.null(.fixture_env$db_path) && file.exists(.fixture_env$db_path)) {
    return(.fixture_env$db_path)
  }

  db_path <- tempfile(fileext = ".duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), db_path)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS cricsheet")

  # Matches table (5 matches: 2 T20, 2 ODI, 1 Test)
  DBI::dbExecute(conn, "
    CREATE TABLE cricsheet.matches (
      match_id VARCHAR PRIMARY KEY,
      season VARCHAR,
      match_type VARCHAR,
      match_date DATE,
      venue VARCHAR,
      city VARCHAR,
      gender VARCHAR,
      team1 VARCHAR,
      team2 VARCHAR,
      balls_per_over INTEGER,
      outcome_type VARCHAR,
      outcome_winner VARCHAR,
      event_name VARCHAR
    )
  ")

  DBI::dbExecute(conn, "
    INSERT INTO cricsheet.matches VALUES
      ('1001', '2024', 'T20', '2024-01-15', 'Melbourne Cricket Ground', 'Melbourne', 'male', 'India', 'Australia', 6, 'normal', 'India', 'Bilateral'),
      ('1002', '2024', 'T20', '2024-01-17', 'Sydney Cricket Ground', 'Sydney', 'male', 'India', 'Australia', 6, 'normal', 'Australia', 'Bilateral'),
      ('1003', '2024', 'ODI', '2024-02-01', 'Melbourne Cricket Ground', 'Melbourne', 'male', 'India', 'England', 6, 'normal', 'India', 'Bilateral'),
      ('1004', '2024', 'ODI', '2024-02-03', 'Lords', 'London', 'male', 'England', 'Australia', 6, 'normal', 'England', 'Bilateral'),
      ('1005', '2024', 'Test', '2024-03-01', 'Melbourne Cricket Ground', 'Melbourne', 'male', 'Australia', 'England', 6, 'normal', 'Australia', 'Ashes')
  ")

  # Players table
  DBI::dbExecute(conn, "
    CREATE TABLE cricsheet.players (
      player_id VARCHAR PRIMARY KEY,
      player_name VARCHAR,
      country VARCHAR
    )
  ")

  DBI::dbExecute(conn, "
    INSERT INTO cricsheet.players VALUES
      ('bat1', 'V Kohli', 'India'),
      ('bat2', 'S Smith', 'Australia'),
      ('bat3', 'J Root', 'England'),
      ('bowl1', 'JJ Bumrah', 'India'),
      ('bowl2', 'P Cummins', 'Australia'),
      ('bowl3', 'J Anderson', 'England')
  ")

  # Deliveries table — synthetic ball-by-ball data
  DBI::dbExecute(conn, "
    CREATE TABLE cricsheet.deliveries (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      season VARCHAR,
      match_type VARCHAR,
      match_date DATE,
      venue VARCHAR,
      city VARCHAR,
      gender VARCHAR,
      batting_team VARCHAR,
      bowling_team VARCHAR,
      innings INTEGER,
      over INTEGER,
      ball INTEGER,
      batter_id VARCHAR,
      bowler_id VARCHAR,
      runs_batter INTEGER,
      runs_extras INTEGER,
      runs_total INTEGER,
      is_boundary BOOLEAN,
      is_four BOOLEAN,
      is_six BOOLEAN,
      wides INTEGER,
      noballs INTEGER,
      is_wicket BOOLEAN,
      wicket_kind VARCHAR,
      player_out_id VARCHAR,
      total_runs INTEGER,
      wickets_fallen INTEGER
    )
  ")

  # Generate ~60 deliveries across 5 matches
  # Match 1001: T20, India batting (Kohli vs Cummins) — 12 balls
  # Match 1002: T20, Australia batting (Smith vs Bumrah) — 12 balls
  # Match 1003: ODI, India batting (Kohli vs Anderson) — 12 balls
  # Match 1004: ODI, England batting (Root vs Cummins) — 12 balls
  # Match 1005: Test, Australia batting (Smith vs Bumrah) — 12 balls

  matches <- list(
    list(id = "1001", type = "T20", date = "2024-01-15", venue = "Melbourne Cricket Ground",
         city = "Melbourne", bat_team = "India", bowl_team = "Australia",
         batter = "bat1", bowler = "bowl2"),
    list(id = "1002", type = "T20", date = "2024-01-17", venue = "Sydney Cricket Ground",
         city = "Sydney", bat_team = "Australia", bowl_team = "India",
         batter = "bat2", bowler = "bowl1"),
    list(id = "1003", type = "ODI", date = "2024-02-01", venue = "Melbourne Cricket Ground",
         city = "Melbourne", bat_team = "India", bowl_team = "England",
         batter = "bat1", bowler = "bowl3"),
    list(id = "1004", type = "ODI", date = "2024-02-03", venue = "Lords",
         city = "London", bat_team = "England", bowl_team = "Australia",
         batter = "bat3", bowler = "bowl2"),
    list(id = "1005", type = "Test", date = "2024-03-01", venue = "Melbourne Cricket Ground",
         city = "Melbourne", bat_team = "Australia", bowl_team = "England",
         batter = "bat2", bowler = "bowl3")
  )

  # Deterministic delivery outcomes per match
  set.seed(42)
  runs_options <- c(0L, 0L, 1L, 1L, 2L, 4L)  # weighted toward dots and singles

  for (m in matches) {
    for (ball_num in seq_len(12)) {
      over <- (ball_num - 1) %/% 6
      ball <- ((ball_num - 1) %% 6) + 1
      runs <- runs_options[sample.int(6, 1)]
      is_wkt <- ball_num == 12  # last ball of each match is a wicket
      del_id <- sprintf("%s_%s_1_%03d_%02d", m$id, m$bat_team, over, ball)

      DBI::dbExecute(conn, "
        INSERT INTO cricsheet.deliveries VALUES (
          ?, ?, '2024', ?, ?, ?, ?, 'male',
          ?, ?, 1, ?, ?,
          ?, ?,
          ?, 0, ?, FALSE, ?, FALSE, 0, 0,
          ?, ?, ?,
          ?, ?
        )
      ", params = list(
        del_id, m$id, m$type, m$date, m$venue, m$city,
        m$bat_team, m$bowl_team, over, ball,
        m$batter, m$bowler,
        runs, runs, runs == 4L,
        is_wkt, if (is_wkt) "bowled" else NA_character_, if (is_wkt) m$batter else NA_character_,
        as.integer(runs * ball_num / 2), if (is_wkt) 1L else 0L
      ))
    }
  }

  .fixture_env$db_path <- db_path
  db_path
}
