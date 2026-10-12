# Lookup fallbacks for compute_projected_margins(); the full function needs the
# database and is checked by the backfill run.

fake_lookup <- function() {
  list(
    lo = data.table::data.table(
      format = "t20", gender = c("male", "male", "female", "male"),
      balls_rem = c(6L, 6L, 6L, 30L), in_hand = c(5L, 4L, 5L, 5L),
      n = c(100L, 10L, 50L, 200L), runs = c(10, 9, 6, 40)),
    test = data.table::data.table(
      gender = c("male", "male", "female"), fourth = c(TRUE, FALSE, TRUE),
      in_hand = 3L, n = c(300L, 2000L, 5L), runs = c(42, 50, 30)))
}

test_that("limited-overs runs to come falls back from exact cell to thinner ones", {
  lk <- fake_lookup()
  r <- .lo_runs_to_come(lk, rep("t20", 4), c("male", "male", "female", "male"),
                        c(6L, 6L, 6L, 0L), c(5L, 4L, 5L, 5L), min_n = 30L)
  expect_equal(r$runs[1], 10)                       # exact male cell
  expect_equal(r$source[1], "exact")
  expect_equal(r$source[2], "nearest_thin")         # only cell for 4 in hand has n 10 < 30
  expect_equal(r$runs[2], 9)
  expect_equal(r$runs[3], 6)                        # exact female cell
  expect_equal(r$runs[4], 0)                        # no balls left
  expect_equal(r$source[4], "no_balls_left")
})

test_that("Test runs to come prefers the 4th-innings value for the gender", {
  lk <- fake_lookup()
  expect_equal(.test_runs_to_come(lk, "male", 3L, min_n = 30L), 42)
  # female 4th innings has 5 examples: pooled 4th innings (n 305)
  expect_equal(.test_runs_to_come(lk, "female", 3L, min_n = 30L), (42 * 300 + 30 * 5) / 305)
})
