# One exposure floor cannot serve three formats.
#
# min_balls = 500 was 33.9 T20 batter matches but only 3.5 Test bowler matches,
# and the split-half reliability it bought ran from 0.455 (T20 batter) to 0.169
# (Test bowler) — the latter barely distinguishable from noise. These floors
# equalise reliability at about 0.40 so a name on any leaderboard means roughly
# the same thing (bouncerverse#57).

test_that("every format and role has a floor", {
  for (fmt in c("t20", "odi", "test")) {
    for (role in c("batter", "bowler")) {
      f <- default_exposure_floor(fmt, role)
      expect_true(is.numeric(f) && length(f) == 1L && f > 0,
                  info = paste(fmt, role))
    }
  }
})

test_that("ODI and Test demand MORE balls than T20, not fewer", {
  # The counter-intuitive part, and the one most likely to be "fixed" back by
  # someone reasoning from matches instead of reliability: an ODI innings is a
  # smaller share of a career and per-ball noise is larger relative to the
  # between-player spread, so ODI needs more balls to reach the same
  # reliability.
  expect_gt(default_exposure_floor("odi", "batter"),
            default_exposure_floor("t20", "batter"))
  expect_gt(default_exposure_floor("odi", "bowler"),
            default_exposure_floor("t20", "bowler"))
  expect_gt(default_exposure_floor("test", "batter"),
            default_exposure_floor("t20", "batter"))
})

test_that("bowlers need more balls than batters where they are noisier", {
  # Bowling reliability at 500 balls is below batting in ODI and Test.
  expect_gte(default_exposure_floor("odi", "bowler"),
             default_exposure_floor("odi", "batter"))
  expect_gte(default_exposure_floor("test", "bowler"),
             default_exposure_floor("test", "batter"))
})

test_that("the floors are the measured values, not round numbers someone liked", {
  expect_equal(default_exposure_floor("t20", "batter"), 500L)
  expect_equal(default_exposure_floor("odi", "batter"), 1500L)
  expect_equal(default_exposure_floor("odi", "bowler"), 2000L)
  expect_equal(default_exposure_floor("test", "batter"), 1000L)
  expect_equal(default_exposure_floor("test", "bowler"), 1800L)
})

test_that("case does not matter", {
  expect_equal(default_exposure_floor("ODI", "Batter"),
               default_exposure_floor("odi", "batter"))
})

test_that("an unknown format or role is named rather than defaulted", {
  expect_error(default_exposure_floor("hundred", "batter"), "hundred")
  expect_error(default_exposure_floor("t20", "keeper"), "keeper")
})

test_that("EXPOSURE_FLOOR covers exactly the rated formats", {
  expect_setequal(names(EXPOSURE_FLOOR), c("t20", "odi", "test"))
  for (f in EXPOSURE_FLOOR) expect_setequal(names(f), c("batter", "bowler"))
})
