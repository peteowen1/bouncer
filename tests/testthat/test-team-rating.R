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
