# Tests for Match Prediction Functions
#
# Regression coverage for a bug where predict_match_outcome() built an ad-hoc
# ~14-column feature matrix instead of the 34 named columns
# (get_prediction_feature_cols()) the pre-match models are actually trained
# on. xgb.DMatrix() is positional, so a differently-shaped/ordered matrix
# produces confidently wrong predictions without erroring.
#
# Also covers prepare_prediction_features() erroring on the single-match
# "slow path" (calculate_pre_match_features()), which doesn't compute the
# team/venue per-delivery skill columns the bulk/training path does.

make_slow_path_features <- function() {
  # Shape matches calculate_pre_match_features()'s real output columns -
  # deliberately omits team1_team_runs_skill/team2_team_runs_skill/
  # venue_run_rate_skill/venue_wicket_rate_skill/venue_boundary_rate/
  # venue_dot_rate, which only the bulk/fast path (calc_match_features)
  # produces.
  data.frame(
    match_id = "123", team1 = "India", team2 = "Australia",
    team1_elo_result = 1550, team1_elo_roster = 1560, team1_form_last5 = 0.6,
    team1_h2h_wins = 3, team1_h2h_total = 5,
    team1_bat_scoring_avg = 1.1, team1_bat_scoring_top5 = 1.2, team1_bat_survival_avg = 0.97,
    team1_bowl_economy_avg = 1.0, team1_bowl_economy_top5 = 0.9, team1_bowl_strike_avg = 0.04,
    team2_elo_result = 1480, team2_elo_roster = 1490, team2_form_last5 = 0.4,
    team2_h2h_wins = 2, team2_h2h_total = 5,
    team2_bat_scoring_avg = 1.0, team2_bat_scoring_top5 = 1.1, team2_bat_survival_avg = 0.96,
    team2_bowl_economy_avg = 1.05, team2_bowl_economy_top5 = 0.95, team2_bowl_strike_avg = 0.035,
    venue_avg_score = 165, venue_chase_success_rate = 0.55, venue_matches = 20,
    is_knockout = FALSE, is_neutral_venue = FALSE,
    team1_won_toss = 1L, toss_elect_bat = 1L,
    stringsAsFactors = FALSE
  )
}

test_that("prepare_prediction_features handles the slow single-match path without erroring", {
  slow_path_row <- make_slow_path_features()

  expect_no_error(prepared <- prepare_prediction_features(slow_path_row))

  feature_cols <- get_prediction_feature_cols()
  expect_true(all(feature_cols %in% names(prepared)))

  feature_matrix <- prepared[, feature_cols, drop = FALSE]
  expect_equal(ncol(feature_matrix), length(feature_cols))
  expect_true(all(vapply(feature_matrix, is.numeric, logical(1))))
})

test_that("prepare_prediction_features defaults missing skill columns to documented neutral values", {
  slow_path_row <- make_slow_path_features()
  prepared <- prepare_prediction_features(slow_path_row)

  expect_equal(prepared$team_runs_skill_diff, 0)
  expect_equal(prepared$team_wicket_skill_diff, 0)
  expect_equal(prepared$venue_run_skill, 0)
  expect_equal(prepared$venue_wicket_skill, 0)
  expect_equal(prepared$venue_boundary, 0.15)
  expect_equal(prepared$venue_dot, 0.35)
})

test_that("prepare_prediction_features is unaffected when skill columns are already present (fast/bulk path)", {
  fast_path_row <- make_slow_path_features()
  fast_path_row$team1_team_runs_skill <- 0.05
  fast_path_row$team2_team_runs_skill <- -0.02
  fast_path_row$venue_run_rate_skill <- 0.01
  fast_path_row$venue_wicket_rate_skill <- -0.01
  fast_path_row$venue_boundary_rate <- 0.18
  fast_path_row$venue_dot_rate <- 0.3

  prepared <- prepare_prediction_features(fast_path_row)

  expect_equal(prepared$team_runs_skill_diff, 0.05 - (-0.02))
  expect_equal(prepared$venue_boundary, 0.18)
  expect_equal(prepared$venue_dot, 0.3)
})

test_that("predict_match_outcome falls back cleanly and builds the full canonical feature set", {
  slow_path_row <- make_slow_path_features()

  local_mocked_bindings(
    get_pre_match_features = function(...) data.frame(),
    calculate_pre_match_features = function(match_id, conn) slow_path_row,
    .package = "bouncer"
  )

  result <- predict_match_outcome("123", model = NULL, conn = NULL, model_type = "elo")

  expect_type(result, "list")
  expect_true(result$team1_win_prob >= 0 && result$team1_win_prob <= 1)
  expect_equal(result$team1_win_prob + result$team2_win_prob, 1, tolerance = 1e-8)
  expect_true(result$predicted_winner %in% c("India", "Australia"))
})
