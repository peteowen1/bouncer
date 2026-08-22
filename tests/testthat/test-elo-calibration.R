# get_calibration_data() was deleted on 2026-02-09 as dead code. It was not
# dead: 01_calculate_3way_elo.R calls it, and that script is the only thing
# that populates the 3-way ELO tables -- which have been frozen at 2026-01-19
# ever since. data-raw/ is outside R CMD check, so a function called only from
# pipeline scripts looks unused to every automated check (bouncerverse#63).

make_cal <- function(conn, format, wicket = 0.05, runs = 1.3, outcome = 0.25) {
  if (!DBI::dbExistsTable(conn, "elo_calibration_metrics")) {
    DBI::dbExecute(conn, "CREATE TABLE elo_calibration_metrics (
      format VARCHAR, metric_type VARCHAR, metric_key VARCHAR,
      metric_value DOUBLE, sample_size INTEGER)")
  }
  for (m in list(c("wicket_rate", wicket), c("mean_runs", runs),
                 c("mean_outcome_score", outcome))) {
    DBI::dbExecute(conn, "INSERT INTO elo_calibration_metrics VALUES (?,?,'overall',?,1000)",
                   params = list(format, m[[1]], as.numeric(m[[2]])))
  }
}

test_that("calibration is read back for a format", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_cal(conn, "t20", wicket = 0.054, runs = 1.31)
  cal <- get_calibration_data("t20", conn)
  expect_equal(cal$wicket_rate, 0.054)
  expect_equal(cal$mean_runs, 1.31)
  expect_equal(cal$sample_size, 1000)
})

# `defaulted` names which metrics are hardcoded constants rather than
# measurements -- it is the one field that tells a caller the OTHER numbers
# in the same list are not to be trusted as data. A caller that ignores it
# cannot distinguish "all three measured" from "two defaulted, printed
# alongside a real sample_size", which is exactly how the original deletion
# went unnoticed: a fully populated, plausible-looking list (bouncerverse#63).

test_that("defaulted is empty when every metric is present", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_cal(conn, "t20")
  cal <- get_calibration_data("t20", conn)
  expect_equal(cal$defaulted, character(0))
})

test_that("defaulted names exactly the metrics that fell back to a constant", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(conn, "CREATE TABLE elo_calibration_metrics (
    format VARCHAR, metric_type VARCHAR, metric_key VARCHAR,
    metric_value DOUBLE, sample_size INTEGER)")
  # Only wicket_rate stored under the 'overall' key -- mean_runs and
  # mean_outcome_score are absent entirely, not merely mis-keyed.
  DBI::dbExecute(conn, "INSERT INTO elo_calibration_metrics
    VALUES ('t20','wicket_rate','overall',0.06,500)")

  expect_warning(cal <- get_calibration_data("t20", conn), "missing 2 metrics")

  expect_setequal(cal$defaulted, c("mean_runs", "mean_outcome_score"))
  expect_false("wicket_rate" %in% cal$defaulted)
  # The measured one is real; the defaulted ones are the hardcoded constants.
  expect_equal(cal$wicket_rate, 0.06)
  expect_equal(cal$mean_runs, 1.3)
  expect_equal(cal$mean_outcome_score, 0.25)
})

test_that("a fully missing set of metrics defaults and names all three", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(conn, "CREATE TABLE elo_calibration_metrics (
    format VARCHAR, metric_type VARCHAR, metric_key VARCHAR,
    metric_value DOUBLE, sample_size INTEGER)")
  # Present for the format, but none under the 'overall' key any of the
  # three metric_type pick() looks for -- every fallback branch fires.
  DBI::dbExecute(conn, "INSERT INTO elo_calibration_metrics
    VALUES ('t20','wicket_rate','phase_1',0.09,5)")

  expect_warning(cal <- get_calibration_data("t20", conn), "missing 3 metrics")
  expect_setequal(cal$defaulted, c("wicket_rate", "mean_runs", "mean_outcome_score"))
  expect_equal(cal$sample_size, 0)
})

test_that("formats do not read each other's calibration", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_cal(conn, "t20", wicket = 0.054)
  make_cal(conn, "test", wicket = 0.017)
  expect_equal(get_calibration_data("test", conn)$wicket_rate, 0.017)
})

test_that("a format with no rows returns NULL and says what to run", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_cal(conn, "t20")
  # cli_alert_warning() emits a MESSAGE, not a warning -- worth pinning, since
  # a caller wrapping this in tryCatch(warning=) would catch nothing.
  expect_message(res <- get_calibration_data("odi", conn), "No calibration data")
  expect_null(res)
})

test_that("the wicket fallback is per format and does not reference a deleted constant", {
  # The original defaulted every format to BASE_WICKET_PROB_T20, a constant the
  # same sweep removed -- so the fallback would error instead of defaulting.
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(conn, "CREATE TABLE elo_calibration_metrics (
    format VARCHAR, metric_type VARCHAR, metric_key VARCHAR,
    metric_value DOUBLE, sample_size INTEGER)")
  # present, but no 'overall' key -- exercises every fallback branch
  DBI::dbExecute(conn, "INSERT INTO elo_calibration_metrics
    VALUES ('test','wicket_rate','phase_1',0.9,5)")
  cal <- get_calibration_data("test", conn)
  expect_equal(cal$wicket_rate, EXPECTED_WICKET_TEST)
  expect_false(cal$wicket_rate == EXPECTED_WICKET_T20)
})

test_that("every data-raw caller of get_calibration_data can still find it", {
  # The guard that would have caught the deletion.
  root <- testthat::test_path("..", "..", "data-raw")
  skip_if_not(dir.exists(root), "data-raw not available")
  files <- list.files(root, pattern = "[.]R$", recursive = TRUE, full.names = TRUE)
  callers <- Filter(function(f) {
    any(grepl("get_calibration_data(", readLines(f, warn = FALSE), fixed = TRUE))
  }, files)
  expect_gt(length(callers), 0)
  expect_true(is.function(get_calibration_data))
})
