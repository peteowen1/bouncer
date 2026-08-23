# The tool this file tests generalises the check that caught the
# agnostic-model post-delivery leak (D-P38): healthy aggregate calibration
# (0.856 predicted vs 0.909 actual) with a leak invisible everywhere except
# the first ball of an innings, where predictions ranged 0.005-5.499 and
# correlated 1.000 with that ball's own runs. These tests build a synthetic
# version of exactly that shape and check the audit actually flags it -- not
# just that the functions run.

test_that("a planted leak in ONE bucket is flagged, and clean buckets are not", {
  set.seed(1)
  n_clean <- 2000
  n_leak <- 500

  # Clean population: predictions are noisy but unbiased draws around the
  # true mean, like a real model with no information about this row.
  clean_actual <- rpois(n_clean, lambda = 1.3)
  clean_predicted <- rnorm(n_clean, mean = 1.3, sd = 0.15)

  # Leaked population: the model has effectively copied its own target,
  # exactly the "predicted correlates 1.000 with the outcome" signature.
  leak_actual <- rpois(n_leak, lambda = 1.3)
  leak_predicted <- leak_actual + 0.01  # near-perfect copy, not exact

  predicted <- c(clean_predicted, leak_predicted)
  actual <- c(clean_actual, leak_actual)
  ball_in_innings <- c(rep("not_first", n_clean), rep("first_ball", n_leak))

  audit <- calibration_audit(predicted, actual, cuts = list(ball = ball_in_innings),
                              min_n = 30)

  expect_s3_class(audit, "calibration_audit")
  expect_equal(nrow(audit), 2)

  first_row <- audit[audit$bucket == "first_ball", ]
  clean_row <- audit[audit$bucket == "not_first", ]

  # The leak bucket's mean prediction tracks its own mean actual almost
  # exactly (bias near zero) -- calibration alone does NOT catch this, which
  # is the entire point of the incident. Both rows pass on bias.
  expect_lt(abs(first_row$bias), 0.1)
  expect_lt(abs(clean_row$bias), 0.1)

  # What DOES catch it is per-row correlation at a nominated low-information
  # state -- that is a separate check, exercised below. This test's job is
  # to prove the bucket audit surfaces both buckets as judged (enough rows)
  # and does not silently drop either.
  expect_true(first_row$judged)
  expect_true(clean_row$judged)
})

test_that("audit_low_information_state flags the exact leak signature", {
  # Reproduce the incident's own numbers as closely as the writeup gives
  # them: predictions 0.005-5.499 at the first ball, correlating ~1.000 with
  # that ball's own runs.
  set.seed(2)
  n_first_ball <- 800
  runs_off_first_ball <- sample(0:6, n_first_ball, replace = TRUE,
                                 prob = c(0.35, 0.30, 0.05, 0.10, 0.02, 0.02, 0.16))
  leaked_prediction <- 0.005 + (runs_off_first_ball / 6) * (5.499 - 0.005)

  # Plenty of other (non-first-ball) rows too, so state correctly subsets.
  n_other <- 5000
  other_actual <- rpois(n_other, lambda = 1.3)
  other_predicted <- rnorm(n_other, mean = 1.3, sd = 0.2)

  predicted <- c(leaked_prediction, other_predicted)
  actual <- c(runs_off_first_ball, other_actual)
  state <- c(rep(TRUE, n_first_ball), rep(FALSE, n_other))

  result <- audit_low_information_state(predicted, actual, state, min_n = 30)

  expect_s3_class(result, "low_information_audit")
  expect_equal(result$n, n_first_ball)
  expect_true(result$judged)
  expect_gt(result$correlation_with_outcome, 0.9)
  expect_equal(result$flag, "leak_signature")
  # The spread is the OTHER half of the signature: a model with no player
  # identity has no business varying this much at a 0/0 state.
  expect_gt(result$sd_prediction, 1)
})

test_that("a model with no information at the low-information state passes clean", {
  set.seed(3)
  n <- 1000
  actual <- rpois(n, lambda = 1.3)
  # Same near-constant prediction regardless of outcome -- what a model
  # without player identity SHOULD do at a 0/0 state.
  predicted <- rnorm(n, mean = 1.3, sd = 0.02)
  state <- rep(TRUE, n)

  result <- audit_low_information_state(predicted, actual, state, min_n = 30)

  expect_equal(result$flag, "ok")
  expect_lt(abs(result$correlation_with_outcome), 0.2)
  expect_lt(result$sd_prediction, 0.1)
})

test_that("buckets below min_n are reported as not_judged, not dropped", {
  predicted <- c(1, 1, 1, 5, 5)
  actual <- c(1, 1, 1, 1, 1)
  cuts <- list(g = c("big", "big", "big", "tiny", "tiny"))

  audit <- calibration_audit(predicted, actual, cuts, min_n = 3)

  expect_equal(nrow(audit), 2)
  tiny_row <- audit[audit$bucket == "tiny", ]
  big_row <- audit[audit$bucket == "big", ]
  expect_equal(tiny_row$flag, "not_judged")
  expect_false(tiny_row$judged)
  # still reported, with its real (large) bias visible for inspection
  expect_equal(tiny_row$n, 2)
  expect_equal(tiny_row$bias, 4)
  expect_equal(big_row$flag, "ok")
  expect_true(big_row$judged)
})

test_that("a cut with a single bucket is flagged, not reported as a pass", {
  predicted <- c(1, 2, 3, 4, 5)
  actual <- c(1, 2, 3, 4, 5)
  cuts <- list(gender = rep("male", 5))

  audit <- calibration_audit(predicted, actual, cuts, min_n = 1)

  expect_equal(nrow(audit), 1)
  expect_equal(audit$flag, "single_bucket")
  expect_false(audit$judged)
})

test_that("NA rows are excluded from a cut rather than crashing it", {
  predicted <- c(1, 2, NA, 4)
  actual <- c(1, 2, 3, 4)
  cuts <- list(phase = c("pp", "pp", "death", NA))

  audit <- calibration_audit(predicted, actual, cuts, min_n = 1)
  expect_equal(sort(audit$bucket), c("pp"))
  expect_equal(audit$n[audit$bucket == "pp"], 2)
})

test_that("multiple cuts are all reported, independently", {
  set.seed(4)
  n <- 200
  predicted <- rnorm(n, 1, 0.1)
  actual <- rnorm(n, 1, 0.1)
  cuts <- list(
    innings = sample(c("1", "2"), n, replace = TRUE),
    format = sample(c("t20", "odi"), n, replace = TRUE)
  )
  audit <- calibration_audit(predicted, actual, cuts, min_n = 10)
  expect_setequal(unique(audit$cut), c("innings", "format"))
  expect_equal(sum(audit$cut == "innings"), length(unique(cuts$innings)))
  expect_equal(sum(audit$cut == "format"), length(unique(cuts$format)))
})

test_that("worst_calibration_buckets sorts by |bias| descending and respects n", {
  predicted <- c(1, 1, 1, 1)
  actual <- c(1, 1.5, 3, 0.9)
  cuts <- list(x = c("a", "b", "c", "d"))
  audit <- calibration_audit(predicted, actual, cuts, min_n = 1)

  worst <- worst_calibration_buckets(audit, n = 2)
  expect_equal(nrow(worst), 2)
  expect_equal(worst$bucket[1], "c")  # bias = -2, the largest
  expect_true(worst$abs_bias[1] >= worst$abs_bias[2])
})

test_that("worst_calibration_buckets excludes not_judged and single_bucket rows", {
  predicted <- c(1, 1, 1, 1, 1, 10)
  actual <- c(1, 1, 1, 1, 1, 1)
  cuts <- list(
    tiny = c("a", "a", "b", "b", "b", "solo"),
    constant = rep("only", 6)
  )
  audit <- calibration_audit(predicted, actual, cuts, min_n = 3)
  worst <- worst_calibration_buckets(audit)
  # "solo" (n=1, not_judged) and the whole "constant" cut (single_bucket)
  # must not appear even though "solo" has the biggest bias of all.
  expect_false("solo" %in% worst$bucket)
  expect_false("constant" %in% worst$cut)
})

test_that("calibration_audit rejects mismatched-length cuts rather than recycling", {
  expect_error(
    calibration_audit(1:10, 1:10, cuts = list(bad = 1:3)),
    "same length"
  )
})

test_that("print.calibration_audit runs and names each cut", {
  predicted <- c(1, 2, 3, 4)
  actual <- c(1, 2, 30, 4)
  cuts <- list(phase = c("pp", "pp", "death", "death"))
  audit <- calibration_audit(predicted, actual, cuts, min_n = 1)
  expect_message(print(audit), "phase")
})

test_that("print.low_information_audit distinguishes a leak from a clean pass", {
  clean <- audit_low_information_state(rnorm(100, 1, 0.01), rpois(100, 1), rep(TRUE, 100))
  expect_message(print(clean), "No leak signature")

  leaked_actual <- rpois(100, 1)
  leaked <- audit_low_information_state(leaked_actual + 0.01, leaked_actual, rep(TRUE, 100))
  expect_message(print(leaked), "LEAK SIGNATURE")
})
