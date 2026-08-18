test_that(".rating_match_types refuses an unsupported format instead of guessing", {
  expect_equal(.rating_match_types("t20"), "'t20','it20'")
  expect_equal(.rating_match_types("odi"), "'odi','odm'")
  expect_equal(.rating_match_types("T20"), "'t20','it20'")

  # Test pairs with MDM, as ODI pairs with ODM. The bug this replaced was a t20
  # branch plus an ODI catch-all, so "test" silently returned ODI deliveries.
  expect_equal(.rating_match_types("test"), "'test','mdm'")

  # An unknown format must still abort rather than inherit someone else's data.
  expect_error(.rating_match_types("hundred"), "No rating match-types")
  expect_error(.rating_match_types("t10"), "No rating match-types")
})

# get_raa_lambda's fitted values (including Test's 33) are asserted in
# test-raa.R, which owns that function -- not duplicated here.

test_that("derive_shrinkage_prior recovers a known prior from simulated data", {
  # Ground truth: build players whose true means have a known between-player
  # variance and whose matches have a known within-player variance, then check
  # the method-of-moments estimate returns k ~ s2_within / s2_between.
  set.seed(1)
  n_players <- 400L
  n_matches <- 30L
  s2_between_true <- 4
  s2_within_true <- 160

  mu <- stats::rnorm(n_players, 0, sqrt(s2_between_true))
  pm <- data.table::data.table(
    player_id = rep(sprintf("p%03d", seq_len(n_players)), each = n_matches),
    v = stats::rnorm(n_players * n_matches,
                     rep(mu, each = n_matches), sqrt(s2_within_true))
  )

  est <- derive_shrinkage_prior(pm)

  expect_equal(est$s2_within, s2_within_true, tolerance = 0.05)
  expect_equal(est$s2_between, s2_between_true, tolerance = 0.25)
  expect_equal(est$k, s2_within_true / s2_between_true, tolerance = 0.25)
  expect_equal(est$players, n_players)
  # The docstring's stated band, measured across the six real buckets.
  expect_gt(est$share, 0.005)
  expect_lt(est$share, 0.25)
})

# Both boundary cases are built DETERMINISTICALLY rather than sampled. With
# zero true between-player variance E[msb] = msw exactly, so a random draw
# lands either side of the abort boundary by luck -- a coin-flip test that
# fails on roughly half of all seeds. Each player instead gets the same
# fixed, mean-zero pattern added to a chosen mean, which pins msw and msb.
.prior_fixture <- function(n_players, n_matches, spread, within_sd) {
  stopifnot(n_matches %% 2L == 0L, n_players %% 2L == 0L)
  # Mean-zero, fixed within-player pattern: identical SS for every player.
  pattern <- rep(c(-within_sd, within_sd), each = n_matches / 2L)
  # Mean-zero, fixed between-player spread.
  mu <- rep(c(-spread, spread), each = n_players / 2L)
  data.table::data.table(
    player_id = rep(sprintf("p%03d", seq_len(n_players)), each = n_matches),
    v = as.numeric(rep(mu, each = n_matches) + rep(pattern, times = n_players))
  )
}

test_that("derive_shrinkage_prior refuses a bucket with no between-player variance", {
  # Every player has an identical mean, so msb is exactly 0 and msb - msw < 0:
  # the between-player variance is not identified. The old `max(., 1e-9)` floor
  # turned this into k = msw / 1e-9, a prior of order 1e11 -- the "145 billion
  # matches" incident. At that k every player collapses onto the population
  # mean and the leaderboard ranks correctly with entirely fabricated spread,
  # which a rank-based anchor check cannot detect. Abort, do not return.
  pm <- .prior_fixture(n_players = 200L, n_matches = 40L,
                       spread = 0, within_sd = 14)

  expect_error(derive_shrinkage_prior(pm), "not identified")
})

test_that("derive_shrinkage_prior warns when the implied player share is implausible", {
  # Between-player variance real but tiny relative to match noise: estimable
  # and not degenerate, so it returns a number -- but the spread it implies is
  # far outside anything measured on a real bucket, and the caller is told
  # rather than left to trust it.
  pm <- .prior_fixture(n_players = 200L, n_matches = 40L,
                       spread = 2.34, within_sd = 13.96)

  expect_warning(est <- derive_shrinkage_prior(pm), "outside the plausible")
  expect_gt(est$share, 0)
  expect_lt(est$share, 0.005)
})

test_that("derive_shrinkage_prior falls back loudly on a thin bucket", {
  # Reachable in normal use: the `as_at` backtesting parameter routinely yields
  # buckets with a handful of qualifying players in early history and in the
  # women's formats. The fallback must be flagged as NOT this bucket's own
  # number -- share is NA precisely so the caller can say so.
  pm <- data.table::data.table(
    player_id = rep(c("a", "b", "c"), each = 10L),
    v = as.numeric(seq_len(30L))
  )

  expect_warning(est <- derive_shrinkage_prior(pm), "falling back to 20")
  expect_equal(est$k, 20)
  expect_true(is.na(est$share))
  expect_true(is.na(est$s2_between))
})

# Split-half estimator -------------------------------------------------------

test_that("split-half recovers a known prior", {
  # Construct data where the true prior is known exactly. With a per-player
  # effect of variance s2b and per-match noise s2w, the shrinkage prior IS
  # s2w / s2b, and the split-half correlation over n matches per half is
  # n*s2b / (n*s2b + s2w). Here s2b = 1 and s2w = 9, so the true prior is 9.
  set.seed(1)
  P <- 120L; M <- 40L; s2b <- 1; s2w <- 9
  eff <- stats::rnorm(P, 0, sqrt(s2b))
  d <- data.table::data.table(
    player_id = rep(sprintf("p%03d", seq_len(P)), each = M),
    match_id  = rep(sprintf("m%03d", seq_len(M)), times = P),
    v         = rep(eff, each = M) + stats::rnorm(P * M, 0, sqrt(s2w)))
  out <- suppressMessages(derive_shrinkage_prior(d))
  expect_equal(out$method, "split_half")
  # recovery within 40% -- this is a variance-ratio estimate on 120 players
  expect_gt(out$k, 9 * 0.6)
  expect_lt(out$k, 9 * 1.4)
  expect_true(is.finite(out$split_half_r) && out$split_half_r > 0)
  expect_true(is.finite(out$k_anova))
})

test_that("split-half is deterministic without a seed", {
  # The split alternates over a stable sort rather than sampling, so two calls
  # must agree exactly. A seeded split over an unordered result would not.
  set.seed(2)
  P <- 60L; M <- 30L
  # NOTE: a real per-player effect is required. Without one the ANOVA guard
  # aborts first (correctly -- between-player variance is not identified) and
  # split-half never runs, because it refines the ANOVA result rather than
  # replacing it.
  d <- data.table::data.table(
    player_id = rep(sprintf("p%03d", seq_len(P)), each = M),
    match_id  = rep(sprintf("m%03d", seq_len(M)), times = P),
    v         = rep(stats::rnorm(P), each = M) + stats::rnorm(P * M, 0, 2))
  a <- suppressMessages(derive_shrinkage_prior(data.table::copy(d)))
  b <- suppressMessages(derive_shrinkage_prior(data.table::copy(d)[sample(nrow(d))]))
  expect_equal(a$k, b$k)
})

test_that("too little data falls back to the ANOVA estimate", {
  # Fewer players than sh_min_players, so split-half must not be attempted and
  # the existing ANOVA path (with all its guards) still governs.
  set.seed(3)
  P <- 35L; M <- 25L
  d <- data.table::data.table(
    player_id = rep(sprintf("p%03d", seq_len(P)), each = M),
    match_id  = rep(sprintf("m%03d", seq_len(M)), times = P),
    v         = rep(stats::rnorm(P), each = M) + stats::rnorm(P * M, 0, 3))
  out <- suppressMessages(derive_shrinkage_prior(d))
  expect_equal(out$method, "anova")
  expect_null(out$split_half_r)
})

test_that("reported share is consistent with the prior actually used", {
  # share and k are two views of one quantity: share = 1 / (1 + k). Leaving the
  # ANOVA share beside a split-half k reported "35.4 matches (3.84%)", where
  # 3.84% is the share implied by k = 25 -- two different answers in one line.
  set.seed(4)
  P <- 120L; M <- 40L
  d <- data.table::data.table(
    player_id = rep(sprintf("p%03d", seq_len(P)), each = M),
    match_id  = rep(sprintf("m%03d", seq_len(M)), times = P),
    v         = rep(stats::rnorm(P), each = M) + stats::rnorm(P * M, 0, 3))
  out <- suppressMessages(derive_shrinkage_prior(d))
  expect_equal(out$method, "split_half")
  expect_equal(out$share, 1 / (1 + out$k), tolerance = 1e-9)
  expect_true(out$share_anova != out$share)
})
