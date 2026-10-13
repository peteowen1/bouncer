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
