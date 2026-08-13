# rating_reliability() recovers a known signal-to-noise ratio.
#
# This function exists because the EPR leaderboards turned out to be roughly
# half sampling noise (2026-08-13) and nothing in their output revealed it. A
# tool for detecting that is only worth having if it is itself correct, so these
# tests build data with a KNOWN between/within split and check it is recovered.

simulate_ratings <- function(n_players, n_obs, between_sd, within_sd, seed = 1) {
  set.seed(seed)
  true <- stats::rnorm(n_players, 0, between_sd)
  data.frame(
    player = rep(seq_len(n_players), each = n_obs),
    value  = rep(true, each = n_obs) +
      stats::rnorm(n_players * n_obs, 0, within_sd)
  )
}

test_that("a known between/within split is recovered", {
  d <- simulate_ratings(300, 40, between_sd = 3, within_sd = 12)
  r <- rating_reliability(d$value, d$player)

  expect_equal(r$within_sd, 12, tolerance = 0.05)
  expect_equal(r$between_sd, 3, tolerance = 0.25)
  expect_equal(r$n_players, 300L)
  expect_equal(r$mean_obs_per_player, 40)

  # ICC = s2b / (s2b + s2w) = 9 / 153
  expect_equal(r$icc, 9 / 153, tolerance = 0.2)
})

test_that("pure noise yields no recoverable signal", {
  # Every player identical: all observed spread is sampling error.
  d <- simulate_ratings(200, 30, between_sd = 0, within_sd = 10, seed = 7)
  r <- rating_reliability(d$value, d$player)

  expect_equal(r$between_sd, 0, tolerance = 0.35)
  expect_lt(r$icc, 0.02)
  # The naive between-player sd would be ~10/sqrt(30) = 1.8 and look like signal.
  expect_lt(r$reliability, 0.25)
})

test_that("a clean signal is recognised as reliable", {
  d <- simulate_ratings(150, 30, between_sd = 10, within_sd = 2, seed = 3)
  r <- rating_reliability(d$value, d$player)

  expect_gt(r$icc, 0.90)
  expect_gt(r$reliability, 0.99)
  expect_lt(r$obs_for(0.8), 2)
})

test_that("obs_for inverts the Spearman-Brown relation", {
  d <- simulate_ratings(200, 25, between_sd = 4, within_sd = 16, seed = 11)
  r <- rating_reliability(d$value, d$player)

  for (target in c(0.5, 0.7, 0.8, 0.9)) {
    n <- r$obs_for(target)
    achieved <- n * r$icc / (1 + (n - 1) * r$icc)
    expect_equal(achieved, target, tolerance = 1e-8)
  }
  expect_identical(r$obs_for(1), Inf)
})

test_that("unbalanced group sizes use the ANOVA effective n, not the mean", {
  # A few players with many innings and many with few -- the shape of a real
  # leaderboard. Using mean(n_i) here would bias the between-player variance.
  set.seed(21)
  true <- stats::rnorm(120, 0, 3)
  n_i <- c(rep(200L, 10), rep(5L, 110))
  d <- do.call(rbind, lapply(seq_along(n_i), function(i) {
    data.frame(player = i, value = true[i] + stats::rnorm(n_i[i], 0, 12))
  }))
  r <- rating_reliability(d$value, d$player)

  expect_equal(r$within_sd, 12, tolerance = 0.05)
  expect_equal(r$between_sd, 3, tolerance = 0.6)
})

test_that("players below min_obs are excluded rather than contributing zero variance", {
  d <- simulate_ratings(50, 20, between_sd = 3, within_sd = 10, seed = 5)
  singles <- data.frame(player = 1000 + 1:40, value = stats::rnorm(40, 0, 10))
  both <- rbind(d, singles)

  r <- rating_reliability(both$value, both$player, min_obs = 2L)
  expect_equal(r$n_players, 50L)   # the 40 single-innings players are dropped
  expect_equal(r$n_obs, nrow(d))
})

test_that("the contract is enforced", {
  expect_error(rating_reliability(1:5, 1:4), "same length")
  expect_error(rating_reliability(c(1, 2), c("a", "b")), "at least 2 players")
})

test_that("NA values are dropped, not propagated", {
  d <- simulate_ratings(100, 20, between_sd = 3, within_sd = 10, seed = 9)
  clean <- rating_reliability(d$value, d$player)

  d$value[sample(nrow(d), 50)] <- NA_real_
  dirty <- rating_reliability(d$value, d$player)

  expect_false(is.na(dirty$icc))
  expect_equal(dirty$icc, clean$icc, tolerance = 0.05)
})
