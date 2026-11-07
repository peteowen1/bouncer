# date_ordered_match_split() and .assert_recent_skill_coverage() were added
# 2026-11-07 after the ODI retrain of 2026-10-29: the two trainers held out
# different, unrepeatable test sets, and the all-history skill coverage check
# passed while 100% of Sep-Oct deliveries trained on the neutral fill.

test_that("split holds out the latest matches by date, whatever the row order", {
  ids   <- c("a", "a", "b", "c", "c", "d", "e")
  dates <- as.Date(c("2026-01-05", "2026-01-05", "2026-03-01", "2026-02-01",
                     "2026-02-01", "2026-05-01", "2026-04-01"))
  s1 <- date_ordered_match_split(ids, dates, test_frac = 0.4)
  expect_equal(s1$train_matches, c("a", "c", "b"))
  expect_equal(s1$test_matches, c("e", "d"))
  expect_equal(s1$cutoff_date, as.Date("2026-04-01"))

  # Same rows, reversed: identical split (the old row-order split was not).
  rev_i <- rev(seq_along(ids))
  s2 <- date_ordered_match_split(ids[rev_i], dates[rev_i], test_frac = 0.4)
  expect_identical(s1, s2)
  expect_length(intersect(s1$train_matches, s1$test_matches), 0)
})

test_that("ties on date break by match_id, so the split is deterministic", {
  s <- date_ordered_match_split(c("z", "y", "x", "w"),
                                rep(as.Date("2026-06-01"), 4), test_frac = 0.5)
  expect_equal(s$train_matches, c("w", "x"))
  expect_equal(s$test_matches, c("y", "z"))
})

test_that("split refuses undated matches, conflicting dates and bad fractions", {
  expect_error(date_ordered_match_split(c("a", "b"), as.Date(c("2026-01-01", NA))),
               "no match_date")
  expect_error(date_ordered_match_split(c("a", "a", "b"),
                                        as.Date(c("2026-01-01", "2026-01-02", "2026-01-03"))),
               "more than one date")
  expect_error(date_ordered_match_split("a", as.Date("2026-01-01")), "at least 2")
  expect_error(date_ordered_match_split(c("a", "b"), as.Date(c("2026-01-01", "2026-01-02")),
                                        test_frac = 1), "strictly between")
  # Non-ISO character dates must not be guessed at.
  expect_error(date_ordered_match_split(c("a", "b"), c("01/08/2024", "2024-08-02")),
               "not ISO")
})

test_that("ISO character dates are accepted", {
  s <- date_ordered_match_split(c("a", "b"), c("2026-01-02", "2026-01-01"), test_frac = 0.5)
  expect_equal(s$train_matches, "b")
})

# A frame where the table stops on 2026-08-05 but deliveries run to October:
# the all-history share is high, the recent share is zero.
stale_frame <- function() {
  d <- seq(as.Date("2025-01-01"), as.Date("2026-10-31"), by = "day")
  list(date = d, has = d <= as.Date("2026-08-05"))
}

test_that("recent-coverage check aborts on a stale tail the all-history check passes", {
  f <- stale_frame()
  expect_gt(mean(f$has), 0.5)  # what .assert_skill_coverage() would see
  expect_error(.assert_recent_skill_coverage(f$has, f$date, "player skills"),
               "only 3.3% of the")  # Aug 3-5 fall inside the 90-day window
  expect_error(.assert_recent_skill_coverage(f$has, f$date, "player skills"),
               "2026-10 0%")
})

test_that("allow_stale downgrades the abort to a warning", {
  f <- stale_frame()
  expect_warning(cov <- .assert_recent_skill_coverage(f$has, f$date, "venue skills",
                                                      allow_stale = TRUE),
                 "stops short")
  expect_lt(cov, 0.05)
})

test_that("a current table passes and returns its recent coverage", {
  f <- stale_frame()
  expect_no_error(cov <- .assert_recent_skill_coverage(rep(TRUE, length(f$date)), f$date,
                                                       "team skills"))
  expect_equal(cov, 1)
})

test_that("no dates at all is a warning, never a silent pass", {
  expect_warning(cov <- .assert_recent_skill_coverage(c(TRUE, FALSE), as.Date(c(NA, NA)),
                                                      "player skills"),
                 "recent-coverage check could")
  expect_true(is.na(cov))
})
