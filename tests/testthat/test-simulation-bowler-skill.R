# The bowler had NO effect on any simulated innings (bouncerverse#66).
#
# simulate_innings() looked up `current_bowler`, updated its ball count, and
# then called simulate_delivery(model, match_state, current_batter, ...) --
# never passing the bowler. simulate_delivery() reads BOTH batter and bowler
# skills out of that one list, so every bowler field fell through its %||%
# default (1.25 / 0.025 / 0) on every ball. The rotation code ran; the bowler
# was inert. Nothing errored.
#
# No existing test could have caught it: the only simulate_delivery() test
# mocks predict_full_outcome() entirely, and nothing exercised simulate_innings().

fake_model <- structure(list(), class = "xgb.Booster")

# A predictor that RESPONDS to bowler skill, so an inert bowler is detectable.
# Better bowling (lower economy) shifts mass from boundaries to dots.
mock_predict <- function(model, delivery_data, format) {
  econ <- delivery_data$bowler_economy_index[1]
  boundary <- max(0, min(0.4, 0.20 * econ))
  dot <- 1 - boundary - 0.55
  matrix(c(0.02, dot, 0.40, 0.10, 0.03, boundary, 0.00), nrow = 1)
}

test_that("bowler skill reaches the delivery, so a better bowler concedes less", {
  skip_if_not(is.function(simulate_innings), "simulation not available")
  batters <- replicate(11, list(batter_scoring_index = 1.25,
                                batter_survival_rate = 0.975), simplify = FALSE)
  good <- replicate(6, list(bowler_economy_index = 0.5,
                            bowler_strike_rate = 0.05), simplify = FALSE)
  poor <- replicate(6, list(bowler_economy_index = 2.0,
                            bowler_strike_rate = 0.01), simplify = FALSE)

  testthat::local_mocked_bindings(predict_full_outcome = mock_predict,
                                  .package = "bouncer")
  set.seed(1)
  a <- simulate_innings(fake_model, "t20", 1L, NULL, list(), list(), list(),
                        batters, good, mode = "expected")
  set.seed(1)
  b <- simulate_innings(fake_model, "t20", 1L, NULL, list(), list(), list(),
                        batters, poor, mode = "expected")

  # If the bowler is dropped, both innings see the SAME default economy and
  # score identically. That equality is precisely the bug.
  expect_false(identical(a$total_runs, b$total_runs),
               info = "identical scores mean bowler skill never reached the model")
  expect_lt(a$total_runs, b$total_runs)
})

test_that("venue skills are read under either spelling", {
  # get_venue_skill() returns run_rate/wicket_rate/...; the simulator reads the
  # venue_-prefixed names. Passing the former straight through used to
  # neutralise every venue effect silently.
  ms <- list(format = "t20", innings = 1L, over = 5L, ball = 1L,
             wickets_fallen = 1L, runs_scored = 40, target = NULL,
             gender = "male", is_knockout = FALSE, event_tier = 2)
  captured <- NULL
  testthat::local_mocked_bindings(
    predict_full_outcome = function(model, delivery_data, format) {
      captured <<- delivery_data
      matrix(c(0.02, 0.35, 0.40, 0.10, 0.03, 0.10, 0.00), nrow = 1)
    }, .package = "bouncer")
  invisible(simulate_delivery(fake_model, ms, list(), list(),
                              list(run_rate = 1.4), mode = "expected"))
  expect_equal(captured$venue_run_rate[1], 1.4)
})
