# The 3-way ELO rebuild reads its K-factors from THREE_WAY_PARAMS. It used to
# read 39 standalone constants that the 2026-02-09 sweep deleted, which is why
# the rebuild could not run for six months (bouncerverse#63).
#
# These are the values recovered from 442f6ae^, the commit that removed them.
# Pinned so the restoration cannot drift, and so the men's/women's split cannot
# quietly collapse back.

OLD <- list(
  male = c(THREE_WAY_K_RUN_MAX = 11.0, THREE_WAY_K_RUN_MIN = 7.0,
           THREE_WAY_K_RUN_HALFLIFE = 150, THREE_WAY_K_WICKET_MAX = 12.0,
           THREE_WAY_K_WICKET_MIN = 4.0, THREE_WAY_K_WICKET_HALFLIFE = 150,
           THREE_WAY_RUNS_PER_100_ELO_POINTS = 0.0745),
  female = c(THREE_WAY_K_RUN_MAX = 13.6, THREE_WAY_K_RUN_MIN = 3.5,
             THREE_WAY_K_RUN_HALFLIFE = 150, THREE_WAY_K_WICKET_MAX = 12.0,
             THREE_WAY_K_WICKET_MIN = 4.0, THREE_WAY_K_WICKET_HALFLIFE = 443,
             THREE_WAY_RUNS_PER_100_ELO_POINTS = 0.128)
)

test_that("T20 params still equal the values the sweep deleted", {
  for (g in names(OLD)) {
    p <- get_3way_params("t20", g)
    for (nm in names(OLD[[g]])) {
      expect_equal(p[[nm]], unname(OLD[[g]][nm]), info = paste(g, nm))
    }
  }
})

test_that("women's T20 params are NOT men's", {
  # The deleted THREE_WAY_*_T20 constants were ALIASES to the MENS values, so
  # every women's rebuild silently used men's K-factors. Any table built before
  # 2026-08-20 carries that; the difference below is why they are not
  # comparable to a fresh women's rebuild.
  m <- get_3way_params("t20", "male")
  w <- get_3way_params("t20", "female")
  expect_false(m$THREE_WAY_K_RUN_MAX == w$THREE_WAY_K_RUN_MAX)
  expect_false(m$THREE_WAY_K_WICKET_HALFLIFE == w$THREE_WAY_K_WICKET_HALFLIFE)
  expect_false(m$THREE_WAY_RUNS_PER_100_ELO_POINTS ==
                 w$THREE_WAY_RUNS_PER_100_ELO_POINTS)
})

test_that("every format and gender the rebuild runs has a complete param set", {
  needed <- c("THREE_WAY_K_RUN_MAX", "THREE_WAY_K_RUN_MIN", "THREE_WAY_K_RUN_HALFLIFE",
              "THREE_WAY_K_WICKET_MAX", "THREE_WAY_K_WICKET_MIN", "THREE_WAY_K_WICKET_HALFLIFE",
              "THREE_WAY_K_VENUE_PERM_MAX", "THREE_WAY_K_VENUE_PERM_MIN",
              "THREE_WAY_K_VENUE_PERM_HALFLIFE", "THREE_WAY_K_VENUE_SESSION_MAX",
              "THREE_WAY_K_VENUE_SESSION_MIN", "THREE_WAY_K_VENUE_SESSION_HALFLIFE",
              "THREE_WAY_RUNS_PER_100_ELO_POINTS")
  for (fmt in c("t20", "odi", "test")) {
    for (g in c("male", "female")) {
      p <- get_3way_params(fmt, g)
      missing <- needed[!needed %in% names(p)]
      expect_equal(missing, character(0), info = paste(fmt, g))
      # A NULL would arrive as a NULL k_max deep inside the delivery loop.
      for (nm in needed) expect_true(is.finite(p[[nm]]), info = paste(fmt, g, nm))
    }
  }
})
