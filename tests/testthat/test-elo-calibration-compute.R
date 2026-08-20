# The functions that COMPUTE and STORE the ELO calibration were deleted by the
# same 2026-02-09 sweep as get_calibration_data(). The rebuild kept working
# only because the table they write survived, so the stored calibration could
# not be refreshed for six months (bouncerverse#63).

test_that("outcome scores are ordered and bounded", {
  expect_equal(calculate_run_outcome_score(0, TRUE), RUN_SCORE_WICKET)
  scores <- vapply(c(0, 1, 2, 3, 4, 6), calculate_run_outcome_score,
                   numeric(1), is_wicket = FALSE)
  expect_true(all(diff(scores) > 0))          # more runs is never worth less
  expect_true(all(scores >= 0 & scores <= 1))
  expect_equal(calculate_run_outcome_score(6, FALSE), RUN_SCORE_SIX)
})

test_that("a wicket scores worse than a dot", {
  expect_lt(calculate_run_outcome_score(0, TRUE),
            calculate_run_outcome_score(0, FALSE))
})

test_that("unusual run values fall back rather than returning NULL", {
  # switch() with no match returns NULL invisibly, which would propagate a NULL
  # into a mean() and silently produce NA for the whole format.
  for (r in c(5, 7, 9)) {
    v <- calculate_run_outcome_score(r, FALSE)
    expect_true(is.numeric(v) && length(v) == 1 && is.finite(v), info = paste("runs", r))
    expect_lte(v, 1.0)
  }
})

test_that("the score constants are the values recovered from the sweep", {
  expect_equal(RUN_SCORE_WICKET, 0.0)
  expect_equal(RUN_SCORE_DOT, 0.15)
  expect_equal(RUN_SCORE_SINGLE, 0.35)
  expect_equal(RUN_SCORE_DOUBLE, 0.45)
  expect_equal(RUN_SCORE_THREE, 0.55)
  expect_equal(RUN_SCORE_FOUR, 0.75)
  expect_equal(RUN_SCORE_SIX, 1.0)
})

test_that("compute and store round-trip through get_calibration_data", {
  # The pair matters more than either half: a refresh must write what the
  # rebuild reads back.
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  cal <- list(format = "t20", total_balls = 1000L, wicket_rate = 0.054,
              mean_runs_per_ball = 1.31, mean_outcome_score = 0.402,
              # column names as calculate_calibration_metrics() emits them;
              # getting these wrong binds a zero-length parameter and the
              # transaction rolls the whole store back
              run_distribution = data.frame(runs_batter = c(0L, 1L),
                                            count = c(400L, 600L),
                                            proportion = c(0.4, 0.6)))
  store_calibration_metrics(cal, conn)
  back <- get_calibration_data("t20", conn)
  expect_equal(back$wicket_rate, 0.054)
  expect_equal(back$mean_runs, 1.31)
  expect_equal(back$mean_outcome_score, 0.402)
  expect_equal(back$sample_size, 1000L)
})


test_that("a failed store rolls back rather than half-writing", {
  # DELETE-then-INSERT unwrapped would leave the format with NO calibration,
  # and the ELO rebuild then falls back to defaults without saying so.
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  good <- list(format = "t20", total_balls = 1000L, wicket_rate = 0.054,
               mean_runs_per_ball = 1.31, mean_outcome_score = 0.402,
               run_distribution = data.frame(runs_batter = 0L, count = 1000L,
                                             proportion = 1.0))
  store_calibration_metrics(good, conn)
  expect_equal(get_calibration_data("t20", conn)$wicket_rate, 0.054)

  # run_distribution missing the columns the insert binds
  bad <- good
  bad$run_distribution <- data.frame(runs_batter = 0L, wrong = 1L)
  expect_error(store_calibration_metrics(bad, conn), "rolled back")

  # the good calibration must survive the failed overwrite
  expect_equal(get_calibration_data("t20", conn)$wicket_rate, 0.054)
})
