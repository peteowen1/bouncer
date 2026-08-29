# store_cricinfo_win_probability() carried the same whole-table-drop defect as
# #45 until 2026-08-29 -- already found and fixed on this table's
# cricsheet-sourced twin (store_cricsheet_wp(), test-cricsheet-wp-storage.R)
# but never ported here, on the table that actually feeds the WPA reaching
# the player ratings (D-P6). Mirrors that file's test shape.

cwp_rows <- function(fmt, n, id0 = 0) {
  data.table::data.table(
    id = paste0(fmt, "_", id0 + seq_len(n)), match_id = "m1",
    innings_number = 1L, over_number = 0, ball_number = seq_len(n),
    format = fmt, win_prob_before = 0.5, win_prob_after = 0.5, delta_wp = 0,
    proj_score_before = 150, proj_score_after = 150, delta_ps = 0)
}

cwp_conn <- function(env = parent.frame()) {
  f <- withr::local_tempfile(fileext = ".duckdb", .local_envir = env)
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f)
  withr::defer(DBI::dbDisconnect(conn, shutdown = TRUE), envir = env)
  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS main")
  conn
}

cwp_counts <- function(conn) {
  d <- DBI::dbGetQuery(conn, "SELECT format, COUNT(*) AS n
    FROM main.bouncer_wp_from_cricinfo GROUP BY 1 ORDER BY 1")
  stats::setNames(d$n, d$format)
}

test_that("replacing one format leaves the others alone", {
  skip_if_not_installed("duckdb")
  conn <- cwp_conn()
  suppressMessages(store_cricinfo_win_probability(conn, cwp_rows("T20", 5), "t20"))
  suppressMessages(store_cricinfo_win_probability(conn, cwp_rows("ODI", 3, 100), "odi"))
  expect_equal(cwp_counts(conn), c(ODI = 3, T20 = 5))

  suppressMessages(store_cricinfo_win_probability(conn, cwp_rows("T20", 7, 200), "t20"))
  expect_equal(cwp_counts(conn), c(ODI = 3, T20 = 7))
})

test_that("REGRESSION: a schema change migrates rather than dropping every format", {
  skip_if_not_installed("duckdb")
  conn <- cwp_conn()
  suppressMessages(store_cricinfo_win_probability(conn, cwp_rows("T20", 7), "t20"))

  # Stand in for "someone added a column since this table was written".
  DBI::dbExecute(conn, "ALTER TABLE main.bouncer_wp_from_cricinfo DROP COLUMN delta_ps")
  suppressMessages(store_cricinfo_win_probability(conn, cwp_rows("ODI", 2, 300), "odi"))

  got <- cwp_counts(conn)
  # Before the fix the T20 rows were gone and only ODI remained.
  expect_equal(unname(got[["T20"]]), 7)
  expect_equal(unname(got[["ODI"]]), 2)
  expect_true("delta_ps" %in% DBI::dbListFields(
    conn, DBI::Id(schema = "main", table = "bouncer_wp_from_cricinfo")))
})

test_that("a column the table has no home for is named, not silently dropped", {
  skip_if_not_installed("duckdb")
  conn <- cwp_conn()
  d <- cwp_rows("T20", 1)
  d[, surprise := 1]
  expect_error(store_cricinfo_win_probability(conn, d, "t20"), "surprise")
})

test_that("the declared schema matches what the builder produces", {
  expect_true(all(nzchar(names(.cricinfo_wp_schema))))
  expect_true(all(nzchar(unname(.cricinfo_wp_schema))))
  expect_setequal(names(.cricinfo_wp_schema), names(cwp_rows("T20", 1)))
})
