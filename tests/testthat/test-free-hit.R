# is_free_hit derivation (bouncerverse#81/D-P50).
#
# Cricsheet has no free_hit field -- verified against the published schema
# and a real no-ball delivery, see docs/plans/D-P50-WIDE-CATEGORY-REBUILD.md.
# compute_is_free_hit() derives it: a no-ball triggers a free hit on the next
# delivery, carrying forward through any further illegal deliveries until a
# legal one is bowled (ICC playing conditions). Wides alone do not trigger it.

dl <- function(over, ball, wides = 0L, noballs = 0L, match_id = "m1", innings = 1L) {
  data.table::data.table(match_id = match_id, innings = innings,
                         over = over, ball = ball,
                         wides = wides, noballs = noballs)
}

test_that("a legal ball right after a no-ball is a free hit", {
  d <- rbind(
    dl(0, 1),
    dl(0, 2, noballs = 1L),
    dl(0, 3)
  )
  r <- compute_is_free_hit(d)
  expect_equal(r, c(FALSE, FALSE, TRUE))
})

test_that("the no-ball itself is never the free hit", {
  d <- rbind(dl(0, 1, noballs = 1L))
  expect_false(compute_is_free_hit(d))
})

test_that("a wide is not a free-hit trigger on its own", {
  d <- rbind(dl(0, 1), dl(0, 2, wides = 1L), dl(0, 3))
  expect_equal(compute_is_free_hit(d), c(FALSE, FALSE, FALSE))
})

test_that("free-hit status carries through an intervening illegal delivery", {
  # no-ball, then a wide (still on the free hit), then a legal ball (still on it)
  d <- rbind(
    dl(0, 1, noballs = 1L),
    dl(0, 2, wides = 1L),
    dl(0, 3)
  )
  expect_equal(compute_is_free_hit(d), c(FALSE, TRUE, TRUE))
})

test_that("a second no-ball re-triggers its own free hit and outlasts the first", {
  d <- rbind(
    dl(0, 1, noballs = 1L),
    dl(0, 2, noballs = 1L),
    dl(0, 3)
  )
  # ball 2 is bowled ON the free hit from ball 1's no-ball; ball 3 is the free
  # hit from ball 2's no-ball -- both TRUE, for different reasons.
  expect_equal(compute_is_free_hit(d), c(FALSE, TRUE, TRUE))
})

test_that("free hit does not cross an over boundary", {
  d <- rbind(
    dl(0, 6, noballs = 1L),
    dl(1, 1)
  )
  expect_equal(compute_is_free_hit(d), c(FALSE, TRUE))
})

test_that("free hit does not cross an innings boundary", {
  d <- rbind(
    dl(19, 6, noballs = 1L, innings = 1L),
    dl(0, 1, innings = 2L)
  )
  expect_equal(compute_is_free_hit(d), c(FALSE, FALSE))
})

test_that("free hit does not cross a match boundary", {
  d <- rbind(
    dl(19, 6, noballs = 1L, match_id = "m1"),
    dl(0, 1, match_id = "m2")
  )
  expect_equal(compute_is_free_hit(d), c(FALSE, FALSE))
})

test_that("a no-ball as the last delivery of an innings does not error", {
  d <- rbind(dl(0, 1), dl(0, 2, noballs = 1L))
  expect_equal(compute_is_free_hit(d), c(FALSE, FALSE))
})

test_that("output is returned in the caller's row order, not resorted", {
  # deliberately out of bowling order
  d <- rbind(
    dl(0, 3, match_id = "m1"),
    dl(0, 1, noballs = 1L, match_id = "m1"),
    dl(0, 2, match_id = "m1")
  )
  r <- compute_is_free_hit(d)
  # row 1 is (over 0, ball 3): third bowled -> not the immediate next ball
  # row 2 is (over 0, ball 1, noball): the trigger itself -> FALSE
  # row 3 is (over 0, ball 2): immediately after the no-ball -> TRUE
  expect_equal(r, c(FALSE, FALSE, TRUE))
})

test_that("a missing required column is named", {
  bad <- data.table::data.table(match_id = "m1", innings = 1L, over = 0L, ball = 1L)
  expect_error(compute_is_free_hit(bad), "wides")
})

test_that("non-numeric wides/noballs abort rather than silently reading as 0", {
  bad_wides <- dl(0, 1)
  bad_wides$wides <- "NB"
  expect_error(compute_is_free_hit(bad_wides), "wides")

  bad_noballs <- dl(0, 1)
  bad_noballs$noballs <- "NB"
  expect_error(compute_is_free_hit(bad_noballs), "noballs")
})
