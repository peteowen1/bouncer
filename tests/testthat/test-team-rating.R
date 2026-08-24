# Composition rules for the team rating (bouncerverse#60).

test_that("value per match scales by exposure, not by accumulation", {
  # Two players, same rate, different volume -- must land on the same number.
  expect_equal(value_per_match(100, 1000, "test", "bat"),
               value_per_match(50, 500, "test", "bat"))
})

test_that("a player with no exposure is NA, not zero", {
  # Zero would assert "exactly average", which is a claim about someone we
  # have not measured. NA forces the caller to decide.
  expect_true(is.na(value_per_match(0, 0, "t20", "bat")))
  expect_true(is.na(value_per_match(5, 0, "t20", "bowl")))
})

test_that("bowling and batting use different standard exposures", {
  # The whole point: a Test bowler bowls far more than a Test batter faces, so
  # accumulated value grows faster for bowling regardless of who is better.
  expect_false(TEAM_RATING_EXPOSURE$test[["bat"]] ==
                 TEAM_RATING_EXPOSURE$test[["bowl"]])
  expect_gt(TEAM_RATING_EXPOSURE$test[["bowl"]], TEAM_RATING_EXPOSURE$test[["bat"]])
})

test_that("every rated format has both exposures and they are positive", {
  for (f in c("t20", "odi", "test")) {
    e <- TEAM_RATING_EXPOSURE[[f]]
    expect_setequal(names(e), c("bat", "bowl"))
    expect_true(all(e > 0), info = f)
  }
})

test_that("an unknown format is named rather than defaulted", {
  expect_error(value_per_match(1, 1, "hundred", "bat"), "hundred")
})

test_that("balance anchor passes on a balanced composition", {
  set.seed(1)
  expect_silent(assert_component_balance(rnorm(200, 0, 2), rnorm(200, 0, 2)))
})

test_that("balance anchor FIRES when one component swallows the other", {
  # The Test case measured in #60: batting 7.6% of the summed variance.
  set.seed(1)
  bat <- rnorm(200, 0, 3.6); bowl <- rnorm(200, 0, 12.6)
  expect_error(assert_component_balance(bat, bowl), "collapsed")
  # and it names which side
  expect_error(assert_component_balance(bat, bowl), "batting is")
})

test_that("balance anchor fires in EITHER direction", {
  set.seed(1)
  expect_error(assert_component_balance(rnorm(200, 0, 12), rnorm(200, 0, 1)),
               "collapsed")
})

test_that("balance anchor refuses to judge on too little data", {
  # A variance ratio on five players is not evidence of anything.
  expect_error(assert_component_balance(rnorm(5), rnorm(5)), "cannot judge")
})

# pick_snapshot: the off-by-one that is invisible in output ------------------

test_that("a snapshot dated the same day as the match is REFUSED", {
  # calculate_player_rating_v2(as_at = D) includes matches ON D, so using that
  # snapshot to score a match on D leaks it. This is the whole point.
  snaps <- as.Date(c("2024-01-01", "2024-07-01", "2025-01-01"))
  expect_equal(pick_snapshot(as.Date("2024-07-01"), snaps), as.Date("2024-01-01"))
})

test_that("it picks the latest snapshot strictly before the match", {
  snaps <- as.Date(c("2024-01-01", "2024-07-01", "2025-01-01"))
  expect_equal(pick_snapshot(as.Date("2024-08-15"), snaps), as.Date("2024-07-01"))
  expect_equal(pick_snapshot(as.Date("2025-06-01"), snaps), as.Date("2025-01-01"))
})

test_that("a match before every snapshot returns NA, not the earliest", {
  # Silently falling back to the earliest snapshot would score a 2023 match
  # with 2024 information.
  snaps <- as.Date(c("2024-01-01", "2024-07-01"))
  expect_true(is.na(pick_snapshot(as.Date("2023-06-01"), snaps)))
})

test_that("it is vectorised and order-independent", {
  snaps <- as.Date(c("2025-01-01", "2024-01-01", "2024-07-01"))  # unsorted
  got <- pick_snapshot(as.Date(c("2024-08-15", "2023-01-01", "2025-06-01")), snaps)
  expect_equal(got, as.Date(c("2024-07-01", NA, "2025-01-01")))
})

test_that("no snapshots at all gives NA rather than erroring", {
  expect_true(is.na(pick_snapshot(as.Date("2024-01-01"), as.Date(character(0)))))
})

test_that("compose_team_rating reports how many players were actually rated", {
  # Two rated players and nine unrated ones produce a plausible number, and
  # nothing downstream would otherwise know.
  p <- data.frame(player_id = paste0("p", 1:3),
                  bat_value = c(10, 20, NA), bowl_value = c(NA, 5, NA),
                  bat_balls = c(100, 200, 0), bowl_balls = c(0, 120, 0))
  r <- compose_team_rating(p, "t20")
  expect_equal(unname(r[["n_rated"]]), 2)
  expect_true(is.finite(r[["total"]]))
})
