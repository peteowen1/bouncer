# Synthetic players with a known curve: true f(age) = -0.02 * (age - 30)^2,
# each player observed only over his own span of ages, with his own level.
make_pm <- function(seed = 1, n_players = 600L) {
  set.seed(seed)
  rows <- lapply(seq_len(n_players), function(i) {
    start <- stats::runif(1, 18, 30)
    span <- stats::runif(1, 3, 12)
    age <- sort(stats::runif(40, start, min(start + span, 41)))
    data.table::data.table(player_id = paste0("p", i), age = age,
                           v = stats::rnorm(1, 0, 2) - 0.02 * (age - 30)^2 + stats::rnorm(40, 0, 1))
  })
  data.table::rbindlist(rows)
}

test_that("fit_ageing_curve recovers a known peak from within-player data", {
  cv <- fit_ageing_curve(make_pm())
  expect_false(is.null(cv))
  expect_true(abs(cv$peak - 30) <= 2)
  expect_equal(cv$tab[ab == 27, f], 0)
})

test_that("the young-only curve never declines and the full curve keeps falling past the data", {
  cv <- fit_ageing_curve(make_pm())
  a <- seq(16, 90, by = 0.5)
  y <- .ageing_young(cv, a)
  expect_true(all(diff(y) >= -1e-12) || all(diff(y[a >= cv$peak]) == 0))
  expect_true(all(diff(y[a >= cv$peak]) == 0))
  full <- .ageing_full(cv, a)
  past <- a > cv$last
  expect_true(all(diff(full[past]) < 0))
  # continuous where the measured curve hands over to the extension
  expect_equal(.ageing_full(cv, cv$last + 1e-9), .ageing_full(cv, cv$last), tolerance = 1e-6)
  expect_lt(.ageing_full(cv, 80), .ageing_full(cv, 50))
})

test_that("ageing helpers return 0 without a curve or an age", {
  expect_equal(.ageing_young(NULL, c(25, 40)), c(0, 0))
  expect_equal(.ageing_full(NULL, c(25, NA)), c(0, 0))
  cv <- fit_ageing_curve(make_pm())
  expect_equal(.ageing_full(cv, NA_real_), 0)
})

test_that("fit_ageing_curve refuses thin data rather than fitting noise", {
  expect_null(fit_ageing_curve(make_pm(n_players = 20L)))
})
