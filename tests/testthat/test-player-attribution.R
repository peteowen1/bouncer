# Tests for Player Attribution (zero-ablation)
#
# Regression coverage for a bug where ablated batter/bowler skill columns
# were set to a non-existent skill_start$runs_per_ball (always NULL),
# which deleted those columns from the data frame instead of neutralizing
# them (see calculate_player_attribution / calculate_wicket_attribution).

fake_full_outcome <- function(captured_env) {
  function(model, delivery_data, format) {
    captured_env$calls <- c(captured_env$calls, list(delivery_data))
    # length(OUTCOME_CATEGORIES) columns (8): the full model is retrained to
    # this shape as of #81/D-P50 stage 5. What this test actually verifies --
    # ablated columns get neutralized, not dropped -- doesn't depend on the
    # column count, so this just matches the real model's current shape
    # rather than pinning an arbitrary width.
    matrix(1 / length(OUTCOME_CATEGORIES), nrow = nrow(delivery_data),
           ncol = length(OUTCOME_CATEGORIES))
  }
}

make_attribution_input <- function() {
  data.frame(
    batter_scoring_index = c(0.1, 0.2),
    batter_survival_rate = c(0.9, 0.85),
    batter_balls_faced = c(10, 20),
    bowler_economy_index = c(0.05, 0.03),
    bowler_strike_rate = c(0.05, 0.06),
    bowler_balls_bowled = c(15, 25),
    batting_team_runs_skill = c(0, 0),
    batting_team_wicket_skill = c(0, 0),
    bowling_team_runs_skill = c(0, 0),
    bowling_team_wicket_skill = c(0, 0),
    venue_run_rate = c(0, 0),
    venue_wicket_rate = c(0, 0),
    venue_boundary_rate = c(0.15, 0.15),
    venue_dot_rate = c(0.35, 0.35),
    stringsAsFactors = FALSE
  )
}

test_that("calculate_player_attribution neutralizes (not NULLs) ablated skill columns", {
  captured <- new.env()
  captured$calls <- list()

  local_mocked_bindings(
    predict_full_outcome = fake_full_outcome(captured),
    .package = "bouncer"
  )

  input_data <- make_attribution_input()
  skill_start <- get_skill_start_values("t20")

  result <- calculate_player_attribution(model = NULL, input_data, format = "t20")

  # Call order: full, no_batter, no_bowler, no_team, no_venue, context_only
  data_no_batter <- captured$calls[[2]]
  data_no_bowler <- captured$calls[[3]]

  # Columns must be preserved (not dropped via NULL assignment)
  expect_setequal(names(data_no_batter), names(input_data))
  expect_setequal(names(data_no_bowler), names(input_data))

  # Ablated values must equal the documented skill starting values
  expect_equal(unique(data_no_batter$batter_scoring_index), skill_start$scoring_index)
  expect_equal(unique(data_no_bowler$bowler_economy_index), skill_start$economy_index)

  # Output still has all expected attribution columns
  expect_true(all(c("batter_contribution", "bowler_contribution",
                     "team_contribution", "venue_contribution",
                     "exp_runs_full", "context_baseline") %in% names(result)))
})

test_that("calculate_wicket_attribution neutralizes (not NULLs) ablated skill columns", {
  captured <- new.env()
  captured$calls <- list()

  local_mocked_bindings(
    predict_full_outcome = fake_full_outcome(captured),
    .package = "bouncer"
  )

  input_data <- make_attribution_input()
  skill_start <- get_skill_start_values("t20")

  result <- calculate_wicket_attribution(model = NULL, input_data, format = "t20")

  # Call order: full, no_batter, no_bowler
  data_no_batter <- captured$calls[[2]]
  data_no_bowler <- captured$calls[[3]]

  expect_setequal(names(data_no_batter), names(input_data))
  expect_setequal(names(data_no_bowler), names(input_data))

  expect_equal(unique(data_no_batter$batter_scoring_index), skill_start$scoring_index)
  expect_equal(unique(data_no_bowler$bowler_economy_index), skill_start$economy_index)

  expect_true(all(c("batter_wicket_contribution", "bowler_wicket_contribution") %in% names(result)))
})

test_that("summarize_player_contributions groups by batter_id/bowler_id", {
  attribution_df <- data.frame(
    batter_id = c("A", "A", "B"),
    bowler_id = c("X", "Y", "X"),
    batter_contribution = c(1, 2, 3),
    bowler_contribution = c(-1, -2, -3),
    runs_batter = c(1, 2, 3),
    context_baseline = c(0.5, 0.5, 0.5),
    is_wicket = c(0, 0, 1),
    stringsAsFactors = FALSE
  )

  result <- summarize_player_contributions(attribution_df)

  expect_false(is.null(result$batter))
  expect_false(is.null(result$bowler))
  expect_true("batter_id" %in% names(result$batter))
  expect_true("bowler_id" %in% names(result$bowler))
  expect_equal(nrow(result$batter), 2)
  expect_equal(nrow(result$bowler), 2)
})

test_that("summarize_player_contributions returns NULL when batter_id column missing", {
  attribution_df <- data.frame(x = 1:3)
  expect_null(summarize_player_contributions(attribution_df))
})
