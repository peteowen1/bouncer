# The same whole-table-drop defect as #45, in a different file.
#
# `store_cricsheet_wp()` replaces ONE format's rows in a table that holds every
# format, and answered any shape mismatch by dropping the whole table. On a
# 5.5M-row table that feeds TSA and the kappa fit, a single added column would
# have destroyed the other formats' rows. It also ran DELETE then INSERT with no
# transaction, so a failed insert left that format permanently empty —
# "replacement" that destroys what it was replacing.
#
# Found while preparing the #51 rebuild, which is exactly the operation that
# would have triggered it.

wp_rows <- function(fmt, n, id0 = 0) {
  data.table::data.table(
    delivery_id = paste0(fmt, "_", id0 + seq_len(n)), match_id = "m1",
    match_date = as.Date("2026-01-01"), innings_number = 1L, over_number = 0L,
    ball_number = seq_len(n), format = fmt, gender = "male",
    batter_id = "b", bowler_id = "w", win_prob_before = 0.5,
    win_prob_after = 0.5, delta_wp = 0, proj_score_before = 150,
    proj_score_after = 150, delta_ps = 0)
}

wp_conn <- function(env = parent.frame()) {
  f <- withr::local_tempfile(fileext = ".duckdb", .local_envir = env)
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f)
  withr::defer(DBI::dbDisconnect(conn, shutdown = TRUE), envir = env)
  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS main")
  conn
}

counts <- function(conn) {
  d <- DBI::dbGetQuery(conn, "SELECT format, COUNT(*) AS n
    FROM main.cricsheet_ball_win_probability GROUP BY 1 ORDER BY 1")
  stats::setNames(d$n, d$format)
}

test_that("replacing one format leaves the others alone", {
  skip_if_not_installed("duckdb")
  conn <- wp_conn()
  suppressMessages(store_cricsheet_wp(conn, wp_rows("T20", 5), "t20"))
  suppressMessages(store_cricsheet_wp(conn, wp_rows("ODI", 3, 100), "odi"))
  expect_equal(counts(conn), c(ODI = 3, T20 = 5))

  suppressMessages(store_cricsheet_wp(conn, wp_rows("T20", 7, 200), "t20"))
  expect_equal(counts(conn), c(ODI = 3, T20 = 7))
})

test_that("REGRESSION: a schema change migrates rather than dropping every format", {
  skip_if_not_installed("duckdb")
  conn <- wp_conn()
  suppressMessages(store_cricsheet_wp(conn, wp_rows("T20", 7), "t20"))

  # Stand in for "someone added a column since this table was written".
  DBI::dbExecute(conn, "ALTER TABLE main.cricsheet_ball_win_probability DROP COLUMN delta_ps")
  suppressMessages(store_cricsheet_wp(conn, wp_rows("ODI", 2, 300), "odi"))

  got <- counts(conn)
  # Before the fix the T20 rows were gone and only ODI remained.
  expect_equal(unname(got[["T20"]]), 7)
  expect_equal(unname(got[["ODI"]]), 2)
  expect_true("delta_ps" %in% DBI::dbListFields(
    conn, DBI::Id(schema = "main", table = "cricsheet_ball_win_probability")))
})

test_that("a column the table has no home for is named, not silently dropped", {
  skip_if_not_installed("duckdb")
  conn <- wp_conn()
  d <- wp_rows("T20", 1)
  d[, surprise := 1]
  # Silently dropping it is how a shape drifts until something decides to
  # "recreate" the table.
  expect_error(store_cricsheet_wp(conn, d, "t20"), "surprise")
})

test_that("the declared schema matches what the builder produces", {
  # The CREATE TABLE body and the accepted column set are one object now, so
  # they cannot drift the way the rating tables' did (#45).
  expect_true(all(nzchar(names(.cricsheet_wp_schema))))
  expect_true(all(nzchar(unname(.cricsheet_wp_schema))))
  expect_setequal(names(.cricsheet_wp_schema), names(wp_rows("T20", 1)))
})
