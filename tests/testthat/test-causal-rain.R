# The rain feature that replaces the one disabled in #24.
#
# The old `rain_days_so_far` was the match TOTAL rain scaled by progress through
# the match. On day 2 that still carries a share of days 3-5, which have not
# happened — a match rained off on day 5 had a day-2 feature that already knew.
# Scaling a leak down is not removing it (bouncerverse#72).
#
# The property under test: a row for day N may see days 1..N-1 and nothing else.

wx <- function(venue, dates, rain) {
  data.table::data.table(venue = venue, date = as.Date(dates),
                         rain_sum = rain)
}

md <- function(day, match_date = "2024-01-01", venue = "A", match_id = "m1") {
  data.table::data.table(match_id = match_id, venue = venue,
                         match_date = as.Date(match_date), day = day)
}

test_that("day one sees no rain, however wet the match becomes", {
  w <- wx("A", c("2024-01-01", "2024-01-02", "2024-01-03"), c(0, 40, 40))
  r <- causal_rain_features(md(1L), w)
  expect_equal(r$rain_mm_before, 0)
  expect_equal(r$rain_days_before, 0L)
})

test_that("day three sees days one and two, and not day three", {
  w <- wx("A", c("2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"),
          c(5, 10, 99, 99))
  r <- causal_rain_features(md(3L), w)
  expect_equal(r$rain_mm_before, 15)     # 5 + 10, not the 99s
  expect_equal(r$rain_days_before, 2L)
})

test_that("the future cannot change the past", {
  # Identical days 1-2, wildly different days 3-5. Day 3's feature must match.
  dry <- wx("A", paste0("2024-01-0", 1:5), c(5, 10, 0, 0, 0))
  wet <- wx("A", paste0("2024-01-0", 1:5), c(5, 10, 80, 80, 80))
  expect_equal(causal_rain_features(md(3L), dry)$rain_mm_before,
               causal_rain_features(md(3L), wet)$rain_mm_before)
})

test_that("rain days count only days over the 1mm threshold", {
  w <- wx("A", paste0("2024-01-0", 1:4), c(0.5, 0.9, 3, 0))
  r <- causal_rain_features(md(4L), w)
  expect_equal(r$rain_mm_before, 4.4)
  expect_equal(r$rain_days_before, 1L)   # only the 3mm day
})

test_that("venues do not see each other's weather", {
  w <- rbind(wx("A", c("2024-01-01", "2024-01-02"), c(0, 0)),
             wx("B", c("2024-01-01", "2024-01-02"), c(50, 50)))
  expect_equal(causal_rain_features(md(3L, venue = "A"), w)$rain_mm_before, 0)
})

test_that("the climatology uses EARLIER years only", {
  # Same calendar window across three years; the 2024 match may see 2022 and
  # 2023 and must not see its own year.
  w <- rbind(
    wx("A", as.Date("2022-01-01") + 0:5, rep(2, 6)),
    wx("A", as.Date("2023-01-01") + 0:5, rep(4, 6)),
    wx("A", as.Date("2024-01-01") + 0:5, rep(100, 6))
  )
  r <- causal_rain_features(md(1L, match_date = "2024-01-03"), w)
  expect_equal(r$venue_rain_climatology, 3)   # mean(2, 4), not touched by 100
})

test_that("a venue with no earlier years gets NA rather than its own weather", {
  w <- wx("A", as.Date("2024-01-01") + 0:5, rep(100, 6))
  r <- causal_rain_features(md(1L, match_date = "2024-01-03"), w)
  expect_true(is.na(r$venue_rain_climatology))
})

test_that("absent weather yields NA features rather than a silent zero", {
  r <- causal_rain_features(md(3L), NULL)
  expect_true(is.na(r$rain_mm_before))
  expect_true(is.na(r$venue_rain_climatology))
})

test_that("a missing input column is named", {
  bad <- data.table::data.table(match_id = "m1", venue = "A", day = 1L)
  expect_error(causal_rain_features(bad, wx("A", "2024-01-01", 0)), "match_date")
})
