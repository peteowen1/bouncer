# build_fixture_features() / predict_fixture_probs(): the live fixture path
# that replaced calculate_pre_match_features() (2026-10-13).

library(data.table)

make_inputs <- function() {
  hist <- data.table(
    match_id = c("h1", "h2"), match_date = as.Date(c("2026-01-01", "2026-02-01")),
    match_type = "T20", gender = c("male", "female"), team_type = "international",
    season = "2026", event_name = NA_character_, venue = "Somewhere",
    team1 = c("India", "Australia"), team2 = c("West Indies", "Bangladesh"),
    outcome_winner = c("India", "Australia"), toss_winner = NA_character_,
    toss_decision = NA_character_, event_match_number = NA_integer_,
    event_group = NA_character_, format = "t20")
  hist[, `:=`(team1_id = make_team_id_vec(team1, gender, "t20", team_type),
              team2_id = make_team_id_vec(team2, gender, "t20", team_type),
              outcome_winner_id = make_team_id_vec(outcome_winner, gender, "t20", team_type))]
  elo <- data.table(
    team_id = c(hist$team1_id, hist$team2_id, "perth_scorchers_male_t20_club"),
    match_id = c("h1", "h2", "h1", "h2", "h1"),
    match_date = as.Date(c("2026-01-01", "2026-02-01", "2026-01-01", "2026-02-01", "2026-01-01")),
    elo_result = c(1800, 1700, 1400, 1300, 1600))
  elo[, elo_roster_combined := elo_result]
  list(matches_dt = hist, team_elo_dt = elo,
       participation_dt = data.table(match_id = character(), match_date = as.Date(character()),
                                     batting_team = character(), bowling_team = character(),
                                     batter_id = character(), bowler_id = character()),
       venue_stats_dt = data.table(venue = character(), match_date = as.Date(character()),
                                   innings = integer(), total_runs = numeric(),
                                   batting_team = character(), outcome_winner = character()),
       team_skill_dt = data.table(batting_team_id = character()),
       venue_skill_dt = data.table(venue = character()),
       batter_skills = data.table(player_id = character(), scoring_index = numeric()),
       bowler_skills = data.table(player_id = character(), economy_index = numeric()),
       missing = character())
}

test_that("build_fixture_features resolves team type and finds each team's Elo by id", {
  fx <- data.frame(
    match_id = c("f1", "f2", "f3"), match_date = as.Date("2026-03-01"),
    team1 = c("India", "Australia Women", "Perth Scorchers"),
    team2 = c("West Indies", "Bangladesh Women", "Unknown XI"),
    gender = c("male", "female", "male"))
  out <- build_fixture_features(fx, make_inputs())

  expect_equal(nrow(out), 3)
  expect_equal(out$team_type_resolved, c("international", "international", "club"))
  # India/West Indies: real Elo, not the 1500 default the old path returned
  expect_equal(c(out$team1_elo_result[1], out$team2_elo_result[1]), c(1800, 1400))
  # "Australia Women" is "Australia" (female) in the history tables
  expect_equal(c(out$team1_elo_result[2], out$team2_elo_result[2]), c(1700, 1300))
  # A team with no history keeps the default
  expect_equal(out$team1_elo_result[3], 1600)
  expect_equal(out$team2_elo_result[3], ELO_START_RATING)
})

test_that("predict_fixture_probs keeps the 3-class (Test) columns in order", {
  feats <- data.frame(match_id = c("a", "b"))
  P <- matrix(c(0.6, 0.1, 0.3, 0.2, 0.5, 0.3), nrow = 2)  # rows sum to 1; class order win/draw/loss
  local_mocked_bindings(
    prepare_prediction_features = function(df) df,
    get_prediction_feature_cols = function() character(0)
  )
  local_mocked_bindings(predict = function(object, newdata, ...) P, .package = "stats")
  local_mocked_bindings(xgb.DMatrix = function(x, ...) x, .package = "xgboost")
  expect_message(out <- predict_fixture_probs(feats, model = NULL, margin_model = NULL), "No margin model")
  expect_equal(out$team1_win_prob, P[, 1])
  expect_equal(out$draw_prob, P[, 2])
  expect_equal(out$team2_win_prob, P[, 3])
})

test_that("predict_fixture_probs gives team2 = 1 - team1 and no draw for binary models", {
  feats <- data.frame(match_id = c("a", "b", "c"))
  local_mocked_bindings(
    prepare_prediction_features = function(df) df,
    get_prediction_feature_cols = function() character(0)
  )
  local_mocked_bindings(predict = function(object, newdata, ...) c(0.7, 0.4, 0.55), .package = "stats")
  local_mocked_bindings(xgb.DMatrix = function(x, ...) x, .package = "xgboost")
  out <- suppressMessages(predict_fixture_probs(feats, model = NULL, margin_model = NULL))
  expect_equal(out$team2_win_prob, 1 - c(0.7, 0.4, 0.55))
  expect_equal(out$draw_prob, c(0, 0, 0))
})

test_that("build_fixture_features flags teams with no rating history", {
  fx <- data.frame(match_id = c("f1", "f2"), match_date = as.Date("2026-03-01"),
                   team1 = c("India", "Nowhere XI"), team2 = c("West Indies", "Elsewhere XI"),
                   gender = "male")
  out <- build_fixture_features(fx, make_inputs())
  expect_equal(out$team1_elo_found, c(TRUE, FALSE))
  expect_equal(out$team2_elo_found, c(TRUE, FALSE))
  expect_equal(out$team1_in_history, c(TRUE, FALSE))
})

test_that("build_fixture_features refuses fixtures with no gender", {
  fx <- data.frame(match_id = "f1", match_date = as.Date("2026-03-01"),
                   team1 = "India", team2 = "West Indies", gender = NA_character_)
  expect_error(build_fixture_features(fx, make_inputs()), "no gender")
})

test_that("check_fixture_coverage aborts when known teams are unrated, not when teams are new", {
  # Two unknown sides (no history at all) defaulting is fine
  new_sides <- data.frame(format = "test", team1_elo_found = c(TRUE, FALSE), team2_elo_found = c(TRUE, FALSE),
                          team1_in_history = c(TRUE, FALSE), team2_in_history = c(TRUE, FALSE))
  cov <- suppressMessages(check_fixture_coverage(new_sides))
  expect_equal(cov$in_history_unrated, 0)
  expect_equal(cov$both_rated, 1)

  # Known teams whose rating lookup fails: the 2026-10-13 failure
  broken <- data.frame(format = "t20", team1_elo_found = FALSE, team2_elo_found = c(TRUE, FALSE, FALSE),
                       team1_in_history = TRUE, team2_in_history = TRUE)
  expect_error(suppressMessages(check_fixture_coverage(broken)), "lookup is probably broken")
})

test_that("predict_fixture_probs reads a flat 3-class vector row by row", {
  feats <- data.frame(match_id = c("a", "b"))
  local_mocked_bindings(
    prepare_prediction_features = function(df) df,
    get_prediction_feature_cols = function() character(0)
  )
  # Older xgboost returns softprob as a flat vector, row-major: a's 3 classes, then b's
  local_mocked_bindings(predict = function(object, newdata, ...) c(0.6, 0.1, 0.3, 0.2, 0.5, 0.3), .package = "stats")
  local_mocked_bindings(xgb.DMatrix = function(x, ...) x, .package = "xgboost")
  out <- suppressMessages(predict_fixture_probs(feats, model = NULL, margin_model = NULL))
  expect_equal(out$team1_win_prob, c(0.6, 0.2))
  expect_equal(out$draw_prob, c(0.1, 0.5))
})

test_that("load_pre_match_inputs works without a season column and strict mode refuses missing tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(con, "CREATE SCHEMA cricsheet")
  # The prediction cache's fallback matches file has no season column
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

  inp <- suppressMessages(load_pre_match_inputs(con, "t20"))
  expect_equal(nrow(inp$matches_dt), 1)
  expect_true(all(c("t20_team_skill", "t20_venue_skill", "t20_player_skill") %in% inp$missing))
  expect_error(suppressMessages(load_pre_match_inputs(con, "t20", strict = TRUE)), "missing or empty")
})
