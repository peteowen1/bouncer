# Tests for score_rating()'s population handling (bouncer#30).
#
# The defect these pin: the baselines were scored on ALL qualifying players
# while the rating was scored only on those with a pre-origin rating, and
# summarise_rating_score() divided straight across the two. That flattered the
# rating whenever anyone qualified without a rating -- a debutant, or any gap in
# the ratings table -- for a reason unrelated to the rating's quality.

library(data.table)

# Minimal frame in the shape build_rating_frame() returns. `rated_frac` controls
# how many players carry a pre-origin rating; the rating is built to correlate
# with realised rate so the fit is well posed.
make_frame <- function(n_per_origin = 120, rated_frac = 1, seed = 1) {
  set.seed(seed)
  origins <- RATING_VAL_ORIGINS[1:4]
  rbindlist(lapply(seq_along(origins), function(oi) {
    n <- n_per_origin
    skill <- rnorm(n, 0.45, 0.06)
    f <- data.table(
      player_id = paste0("p", oi, "_", seq_len(n)),
      career_balls = sample(600:4000, n, TRUE),
      win_balls = sample(220:900, n, TRUE),
      origin = origins[oi]
    )
    f[, career_runs := rpois(n, skill * career_balls)]
    f[, win_runs := rpois(n, skill * win_balls)]
    f[, career_events := rpois(n, 0.03 * career_balls)]
    f[, win_events := rpois(n, 0.03 * win_balls)]
    for (h in RATING_VAL_H_GRID) {
      f[, (paste0("ew_b_", h)) := career_balls * 0.6]
      f[, (paste0("ew_r_", h)) := career_runs * 0.6]
      f[, (paste0("ew_v_", h)) := career_events * 0.6]
    }
    # rating tracks skill with noise, missing for the unrated share
    f[, run_elo := 1500 + (skill - 0.45) * 2000 + rnorm(n, 0, 40)]
    if (rated_frac < 1) {
      drop <- sample(seq_len(n), round(n * (1 - rated_frac)))
      f[drop, run_elo := NA_real_]
    }
    f[]
  }))
}

test_that("with full coverage the rated and full-population baselines are identical", {
  s <- score_rating(make_frame(rated_frac = 1), "runs", "run_elo",
                    origins = RATING_VAL_ORIGINS[1:4])
  expect_gt(nrow(s), 0)
  expect_identical(s$n, s$n_rated)
  # The bar from bouncer#30: where n == n_rated the change must be an exact
  # identity. If these move, the fix is wrong.
  expect_equal(s$loss_b1, s$loss_b1_all, tolerance = 1e-12)
  expect_equal(s$loss_b2, s$loss_b2_all, tolerance = 1e-12)
  expect_equal(s$rho_b1, s$rho_b1_all, tolerance = 1e-12)
})

test_that("with partial coverage the baselines are scored on the rated subset", {
  s <- score_rating(make_frame(rated_frac = 0.6, seed = 7), "runs", "run_elo",
                    origins = RATING_VAL_ORIGINS[1:4])
  expect_gt(nrow(s), 0)
  expect_true(all(s$n_rated < s$n))
  # Scored on different populations, so they must NOT coincide -- that is the
  # whole point. (Equality here would mean the fix silently did nothing.)
  expect_false(isTRUE(all.equal(s$loss_b1, s$loss_b1_all, tolerance = 1e-8)))
  # and every ratio component is finite on the rated subset
  expect_true(all(is.finite(s$loss_b1)))
  expect_true(all(is.finite(s$loss_b2)))
})

test_that("summarise_rating_score divides like with like and reports coverage", {
  s <- score_rating(make_frame(rated_frac = 0.6, seed = 11), "runs", "run_elo",
                    origins = RATING_VAL_ORIGINS[1:4])
  out <- summarise_rating_score(s, "test rating")
  expect_equal(out$skill_vs_career, 1 - out$loss_rating / out$loss_b1)
  expect_equal(out$skill_vs_recency, 1 - out$loss_rating / out$loss_b2)
  expect_true(out$rated_share > 0 && out$rated_share < 1)
  expect_equal(out$rated_share, out$n_rated / out$n)
})

test_that("build_rating_frame takes the chronologically last rating, not table order", {
  # Shuffled input: .SD[.N] on unsorted rows would take an arbitrary row.
  pool <- data.table(
    player_id = rep("p1", 40),
    match_date = seq(as.Date("2014-01-01"), by = "month", length.out = 40),
    balls = 40, runs = 20, events = 1
  )
  ratings <- data.table(
    player_id = "p1",
    match_date = as.Date(c("2015-06-01", "2014-02-01", "2015-01-01")),
    run_elo = c(1600, 1400, 1500)
  )
  ratings <- ratings[c(2, 1, 3)]  # deliberately not in date order
  f <- build_rating_frame(pool, ratings, "run_elo",
                          origins = as.Date("2016-01-01"))
  # latest strictly before 2016-01-01 is 2015-06-01 -> 1600
  expect_equal(f$run_elo[1], 1600)
})
