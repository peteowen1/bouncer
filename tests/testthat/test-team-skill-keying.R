# Team skill state is keyed by composite team id (2026-10-13): men's and
# women's sides of the same name must not share one running skill.

library(data.table)

test_that("key_team_skill_deliveries splits same-name sides by gender and team type", {
  d <- data.table(
    match_id = c("1", "1", "2", "2", "3"),
    gender = c("male", "male", "female", "female", "male"),
    batting_team = c("India", "Australia", "India", "Australia", "Perth Scorchers"),
    bowling_team = c("Australia", "India", "Australia", "India", "Sydney Sixers"),
    runs_batter = 0L)
  tt <- data.frame(match_id = c(1, 2, 3), team_type = c("international", "international", "club"))
  out <- key_team_skill_deliveries(d, "t20", team_types = tt)

  expect_equal(out$batting_team,
               c("india_male_t20_international", "australia_male_t20_international",
                 "india_female_t20_international", "australia_female_t20_international",
                 "perth_scorchers_male_t20_club"))
  expect_equal(out$bowling_team[5], "sydney_sixers_male_t20_club")
  # Same ids team Elo uses
  expect_equal(out$batting_team[3], make_team_id("India", "female", "t20", "international"))
  expect_false("team_type" %in% names(out))
  expect_equal(nrow(out), nrow(d))
})

test_that("key_team_skill_deliveries refuses deliveries it cannot key", {
  d <- data.table(match_id = c("1", "9"), gender = "male",
                  batting_team = "India", bowling_team = "Australia")
  tt <- data.frame(match_id = "1", team_type = "international")
  expect_error(key_team_skill_deliveries(d, "t20", team_types = tt), "no team_type or gender")

  dup <- data.frame(match_id = c("1", "1"), team_type = c("international", "club"))
  expect_error(key_team_skill_deliveries(d[1], "t20", team_types = dup), "more than one team_type")
})

test_that("get_team_delivery_skill_fast reads only the requested side", {
  sk <- data.table(
    batting_team_id = c("india_male_t20_international", "india_female_t20_international"),
    match_date = as.Date(c("2026-01-01", "2026-02-01")),
    batting_team_runs_skill = c(0.10, -0.20),
    batting_team_wicket_skill = c(0.01, -0.02))
  setkey(sk, batting_team_id, match_date)
  m <- get_team_delivery_skill_fast("india_male_t20_international", as.Date("2026-03-01"), sk)
  w <- get_team_delivery_skill_fast("india_female_t20_international", as.Date("2026-03-01"), sk)
  expect_equal(m$runs_skill, 0.10)
  expect_equal(w$runs_skill, -0.20)
  # As of the date: nothing before 2026-01-01
  expect_equal(get_team_delivery_skill_fast("india_male_t20_international", as.Date("2026-01-01"), sk)$runs_skill, 0)
})

test_that("get_all_team_skill_state refuses a name-keyed table", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(con, "CREATE TABLE t20_team_skill AS SELECT 'd1' AS delivery_id, DATE '2026-01-01' AS match_date,
    'India' AS batting_team_id, 'Australia' AS bowling_team_id, 0.1 AS batting_team_runs_skill,
    0.0 AS batting_team_wicket_skill, 1 AS batting_team_balls, 0.1 AS bowling_team_runs_skill,
    0.0 AS bowling_team_wicket_skill, 1 AS bowling_team_balls")
  expect_error(get_all_team_skill_state("t20", con), "FORCE_FULL")

  DBI::dbExecute(con, "UPDATE t20_team_skill SET batting_team_id = 'india_male_t20_international',
    bowling_team_id = 'australia_male_t20_international'")
  st <- suppressMessages(get_all_team_skill_state("t20", con))
  expect_equal(st$batting_runs_skill[["india_male_t20_international"]], 0.1)
})

test_that("load_pre_match_inputs refuses a name-keyed team skill table", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(con, "CREATE SCHEMA cricsheet")
  DBI::dbExecute(con, "CREATE TABLE cricsheet.matches AS SELECT '1' AS match_id, DATE '2026-01-01' AS match_date,
    'T20' AS match_type, 'male' AS gender, 'international' AS team_type, NULL::VARCHAR AS event_name,
    'V' AS venue, 'India' AS team1, 'West Indies' AS team2, 'India' AS outcome_winner,
    NULL::VARCHAR AS toss_winner, NULL::VARCHAR AS toss_decision, NULL::INTEGER AS event_match_number,
    NULL::VARCHAR AS event_group")
  DBI::dbExecute(con, "CREATE TABLE cricsheet.match_innings AS SELECT '1' AS match_id, 1 AS innings, 180 AS total_runs, 'India' AS batting_team")
  DBI::dbExecute(con, "CREATE TABLE cricsheet.deliveries AS SELECT '1' AS match_id, DATE '2026-01-01' AS match_date,
    'T20' AS match_type, 'India' AS batting_team, 'West Indies' AS bowling_team, 'p1' AS batter_id, 'p2' AS bowler_id")
  DBI::dbExecute(con, "CREATE TABLE team_elo AS SELECT 'india_male_t20_international' AS team_id, '1' AS match_id,
    DATE '2026-01-01' AS match_date, 1600.0 AS elo_result")
  DBI::dbExecute(con, "CREATE TABLE t20_team_skill AS SELECT 'd1' AS delivery_id, DATE '2026-01-01' AS match_date,
    'India' AS batting_team_id, 'West Indies' AS bowling_team_id, 0.1 AS batting_team_runs_skill,
    0.0 AS batting_team_wicket_skill, 0.1 AS bowling_team_runs_skill, 0.0 AS bowling_team_wicket_skill")
  expect_error(suppressMessages(load_pre_match_inputs(con, "t20")), "by team name")

  DBI::dbExecute(con, "UPDATE t20_team_skill SET batting_team_id = 'india_male_t20_international'")
  inp <- suppressMessages(load_pre_match_inputs(con, "t20"))
  expect_equal(inp$team_skill_dt$batting_team_id, "india_male_t20_international")
})
