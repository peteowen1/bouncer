# calculate_phase_features() is called with `over` straight from DuckDB, which
# hands back INTEGER. data.table::fcase() refuses to mix integer and double
# output branches, so an integer `over` aborted the T20 branch with
# "Argument #4 is of type double, however argument #2 is of type integer",
# while ODI -- which had already been patched with as.double() -- worked. T20
# in-match data preparation died on its first run because of it, after loading
# 2,988,980 deliveries.

test_that("phase features accept integer over for every format", {
  # The regression. Without the fix this errors for t20.
  for (fmt in c("t20", "odi", "test")) {
    expect_no_error(
      calculate_phase_features(over = 0:19L, ball = rep(1L, 20), match_type = fmt),
      message = fmt
    )
  }
})

test_that("integer and double over produce the same numbers", {
  for (fmt in c("t20", "odi", "test")) {
    i <- calculate_phase_features(0:19L, rep(1L, 20), fmt)
    d <- calculate_phase_features(as.double(0:19), rep(1, 20), fmt)
    for (col in names(i)) {
      expect_equal(as.numeric(i[[col]]), as.numeric(d[[col]]),
                   info = paste(fmt, col))
    }
  }
})

test_that("phase boundaries land where the format expects", {
  t20 <- calculate_phase_features(c(0L, 5L, 6L, 15L, 16L, 19L), rep(1L, 6), "t20")
  expect_equal(as.character(t20$phase),
               c("powerplay", "powerplay", "middle", "middle", "death", "death"))
  # overs_into_phase resets to 0 at each phase boundary.
  expect_equal(t20$overs_into_phase, c(0, 5, 0, 9, 0, 3))

  odi <- calculate_phase_features(c(0L, 9L, 10L, 39L, 40L, 49L), rep(1L, 6), "odi")
  expect_equal(as.character(odi$phase),
               c("powerplay", "powerplay", "middle", "middle", "death", "death"))
  expect_equal(odi$overs_into_phase, c(0, 9, 0, 29, 0, 9))
})
