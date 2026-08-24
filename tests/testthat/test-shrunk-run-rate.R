# The run rate after one ball is noise, and the model was reading it as signal.
#
# Measured sd of the observed rate across innings: 6.51 (ODI), 7.66 (T20), 6.34
# (Test) at ONE ball — larger than the mean rate itself — falling to 1.16 / 1.80
# / 0.91 by 120 balls. At the first ball of an ODI, one run moved the projected
# score by +30.7, and because the before-state of ball one is 0 runs off 0 balls
# a dot leaves it unchanged, so first-ball TSA had almost no negative branch:
# 1.8% negative against 42.6% later, mean +4.572 against +0.008.
#
# The shrinkage weight is derived, not chosen: sd^2(n) = B + N/n, k = N/B.

test_that("zero balls returns the prior rather than zero or NaN", {
  # This is the state the before-side of the first ball sits in. Raw division
  # gives 0/0; the honest answer is "what an innings usually scores".
  for (f in c("t20", "odi", "test")) {
    expect_equal(shrunk_run_rate(0, 0, f), unname(RUN_RATE_PRIOR_RATE[[f]]))
  }
})

test_that("one ball barely moves the rate off the prior", {
  # A single scoring shot used to imply a run rate of 6, 24 or 36.
  prior <- RUN_RATE_PRIOR_RATE[["odi"]]
  dot <- shrunk_run_rate(0, 1, "odi")
  six <- shrunk_run_rate(6, 1, "odi")
  expect_lt(abs(dot - prior), 0.2)
  expect_lt(abs(six - prior), 1.2)
  # Raw would have been 0 and 36.
  expect_gt(36 - six, 30)
})

test_that("a dot ball moves the rate DOWN, which is what restores symmetry", {
  # The whole first-ball bias was that a dot could not produce a negative
  # delta, because 0 runs off 0 balls and 0 runs off 1 ball both read as 0.
  expect_lt(shrunk_run_rate(0, 1, "odi"), shrunk_run_rate(0, 0, "odi"))
  expect_lt(shrunk_run_rate(0, 6, "odi"), shrunk_run_rate(0, 1, "odi"))
})

test_that("by mid-innings the data dominates the prior", {
  # 300 balls at 7 an over in an ODI: the prior must be nearly irrelevant.
  raw <- 7
  got <- shrunk_run_rate(raw * 300 / 6, 300, "odi")
  expect_lt(abs(got - raw), 0.35)
})

test_that("the shrinkage is monotone in evidence", {
  prior <- RUN_RATE_PRIOR_RATE[["t20"]]
  # A constant true rate of 12 pulls closer to 12 as balls accumulate.
  gaps <- vapply(c(6, 24, 60, 120), function(b)
    abs(shrunk_run_rate(12 * b / 6, b, "t20") - 12), numeric(1))
  expect_true(all(diff(gaps) < 0))
  expect_gt(gaps[1], 0)
})

test_that("Test is shrunk hardest, T20 least", {
  # Test's between-innings spread is smallest relative to per-ball noise, so it
  # needs the heaviest prior. That ordering is a property of the game, and if it
  # ever inverts the derivation has gone wrong.
  expect_gt(RUN_RATE_PRIOR_BALLS[["test"]], RUN_RATE_PRIOR_BALLS[["odi"]])
  expect_gt(RUN_RATE_PRIOR_BALLS[["odi"]], RUN_RATE_PRIOR_BALLS[["t20"]])
})

test_that("the priors match the formats' actual scoring rates", {
  expect_gt(RUN_RATE_PRIOR_RATE[["t20"]], RUN_RATE_PRIOR_RATE[["odi"]])
  expect_gt(RUN_RATE_PRIOR_RATE[["odi"]], RUN_RATE_PRIOR_RATE[["test"]])
})

test_that("it is vectorised and NA-safe", {
  got <- shrunk_run_rate(c(0, 10, NA), c(0, 60, 30), "t20")
  expect_length(got, 3)
  expect_true(all(is.finite(got)))
})

test_that("an unknown format is named rather than silently defaulted", {
  expect_error(shrunk_run_rate(10, 60, "hundred"), "hundred")
})

test_that("calculate_run_rate is left RAW for completed innings", {
  # Shrinking a finished innings would be wrong: the raw rate IS the answer.
  expect_equal(calculate_run_rate(300, 300), 6)
  expect_equal(calculate_run_rate(0, 0), 0)
})
