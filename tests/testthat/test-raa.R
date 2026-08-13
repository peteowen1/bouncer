# RAA: the per-ball construction and its lambda.
#
# The validation suite (anchors, position neutrality, pot regression,
# reliability, face validity, the Stubbs test) runs against the database and
# lives in the ticket record for bouncerverse#11; what is pinned here is the
# arithmetic and the guardrails that do not need data.

test_that("get_raa_lambda returns the fitted values and refuses unfitted formats", {
  expect_equal(get_raa_lambda("t20"), 9.0)
  # ODI fitted 22.5/23.4 by innings from actual outcomes (bouncerverse#19);
  # a wicket in a 300-ball innings is worth ~2.5x its T20 value.
  expect_equal(get_raa_lambda("odi"), 23.0)
  expect_error(get_raa_lambda("test"), "not fitted")
})

test_that("the RAA formula prices runs and wickets the way the spec says", {
  lambda <- get_raa_lambda("t20")

  # A boundary where an average batter expected ~1.3 runs and ~6% wicket risk:
  # credit for the extra runs plus a small survival credit.
  exp_runs <- 1.3; exp_wkt <- 0.06
  raa_boundary <- (4 - exp_runs) - lambda * (0 - exp_wkt)
  expect_equal(raa_boundary, 2.7 + 0.54)

  # A dismissal on the same ball: the run shortfall plus the lambda-weighted
  # wicket surprise, bounded, never a projected-score collapse.
  raa_out <- (0 - exp_runs) - lambda * (1 - exp_wkt)
  expect_equal(raa_out, -1.3 - 8.46)
  expect_gt(raa_out, -12)

  # Meeting expectation exactly scores zero.
  expect_equal((exp_runs - exp_runs) - lambda * (exp_wkt - exp_wkt), 0)
})

test_that("prepare_agnostic_features carries the league features the model was trained with", {
  # The 2026-03-14 models have 16 features; xgboost silently default-routes
  # absent trailing columns instead of erroring, which biased every served
  # expectation by +0.17 runs/ball until 2026-08-13. Never again.
  row <- data.frame(
    innings = 1, over = 5, ball = 3, wickets_fallen = 1,
    runs_difference = 42, gender = "male", is_knockout = 0L, event_tier = 1
  )
  f <- prepare_agnostic_features(row, "t20")
  expect_equal(ncol(f), 16)
  expect_identical(tail(names(f), 2), c("league_avg_runs", "league_avg_wicket"))
  # No supplied league history -> training's own format defaults, not zero.
  expect_equal(f$league_avg_runs, EXPECTED_RUNS_T20)
  expect_equal(f$league_avg_wicket, EXPECTED_WICKET_T20)

  f_test <- prepare_agnostic_features(
    cbind(row, phase = "middle"), "test"
  )
  expect_identical(tail(names(f_test), 2), c("league_avg_runs", "league_avg_wicket"))
  expect_equal(f_test$league_avg_runs, EXPECTED_RUNS_TEST)

  # Supplied values pass through untouched.
  row$league_avg_runs <- 1.5; row$league_avg_wicket <- 0.05
  f2 <- prepare_agnostic_features(row, "t20")
  expect_equal(f2$league_avg_runs, 1.5)
  expect_equal(f2$league_avg_wicket, 0.05)
})
