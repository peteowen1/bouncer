# A rebuild must never leave the live table empty. create_3way_elo_table(
# overwrite = TRUE) drops first and inserts hours later, so an interruption in
# between empties it silently -- which is how t20_3way_elo reached zero rows
# (bouncerverse#63).

# Build staging through the REAL schema helper. Toy one-column fixtures pass a
# rename but not a column-wise copy, and the promote does a copy precisely
# because the primary key's index blocks a rename.
make_stage <- function(conn, category, n) {
  stage_cat <- three_way_elo_staging_category(category)
  stage <- paste0(stage_cat, "_3way_elo")
  create_3way_elo_table(stage_cat, conn, overwrite = TRUE)
  if (n > 0) {
    DBI::dbExecute(conn, sprintf(
      "INSERT INTO %s (delivery_id) SELECT 'd' || i FROM range(%d) t(i)", stage, n))
  }
  stage
}

make_live <- function(conn, category, n) {
  live <- paste0(tolower(category), "_3way_elo")
  create_3way_elo_table(tolower(category), conn, overwrite = TRUE)
  if (n > 0) {
    DBI::dbExecute(conn, sprintf(
      "INSERT INTO %s (delivery_id) SELECT 'old' || i FROM range(%d) t(i)", live, n))
  }
  live
}

test_that("promoting swaps a complete staging table over the live one", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_live(conn, "mens_t20", 1)
  make_stage(conn, "mens_t20", 5)

  expect_equal(promote_3way_elo_staging("mens_t20", conn), 5)
  live <- DBI::dbGetQuery(conn, "SELECT * FROM mens_t20_3way_elo")
  expect_equal(nrow(live), 5)
  expect_false("old" %in% live$delivery_id)
  # the staging name is consumed by the rename, not left behind
  expect_false(table_exists(conn, "mens_t20_staging_3way_elo"))
})

test_that("an EMPTY staging table is refused and the live one survives", {
  # The whole point: a failed rebuild must not destroy the ratings it was
  # meant to replace.
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_live(conn, "mens_odi", 1)
  DBI::dbExecute(conn, "UPDATE mens_odi_3way_elo SET delivery_id = 'keep'")
  make_stage(conn, "mens_odi", 0)

  expect_error(promote_3way_elo_staging("mens_odi", conn), "Refusing to promote")
  expect_equal(DBI::dbGetQuery(conn, "SELECT * FROM mens_odi_3way_elo")$delivery_id,
               "keep")
})

test_that("a suspiciously small staging table is refused too", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_live(conn, "mens_test", 0)
  make_stage(conn, "mens_test", 3)
  # cli wraps the message, so match a fragment that cannot straddle the wrap.
  expect_error(promote_3way_elo_staging("mens_test", conn, min_rows = 1000),
               "Refusing to promote")
  # left in place for inspection rather than cleaned up
  expect_true(table_exists(conn, "mens_test_staging_3way_elo"))
})

test_that("promoting with no staging table names it", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  expect_error(promote_3way_elo_staging("mens_t20", conn),
               "mens_t20_staging_3way_elo")
})

test_that("the staging category is distinct from the live one", {
  expect_equal(three_way_elo_staging_category("mens_t20"), "mens_t20_staging")
  expect_false(three_way_elo_staging_category("mens_t20") == "mens_t20")
})

test_that("a limited run cannot replace a full table with a handful of rows", {
  # min_rows only compares against what the RUN expected, so a MATCH_LIMIT
  # smoke test passes it and would then wipe the live ratings. This is the
  # guard that actually protects the table.
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_live(conn, "mens_t20", 1000)
  make_stage(conn, "mens_t20", 50)

  # 50 of 50 expected rows: min_rows is satisfied, and it is still refused.
  expect_error(promote_3way_elo_staging("mens_t20", conn, min_rows = 49),
               "refusing to promote")
  expect_equal(DBI::dbGetQuery(conn, "SELECT COUNT(*) n FROM mens_t20_3way_elo")$n, 1000)
})

test_that("a genuine shrink is allowed when the caller says so", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_live(conn, "mens_odi", 1000)
  make_stage(conn, "mens_odi", 50)
  expect_equal(
    promote_3way_elo_staging("mens_odi", conn, min_fraction_of_live = NULL), 50)
})

test_that("a normal full rebuild of similar size still promotes", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_live(conn, "mens_test", 1000)
  make_stage(conn, "mens_test", 1200)   # corpus grew, as it does
  expect_equal(promote_3way_elo_staging("mens_test", conn), 1200)
})

test_that("a staging table from an older schema is refused, not misaligned", {
  # The promote copies rows into a freshly created live table. With SELECT *
  # that is positional, so a staging table built before a schema change would
  # write ELOs into the wrong columns and succeed.
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_live(conn, "mens_t20", 1000)
  DBI::dbExecute(conn, "CREATE TABLE mens_t20_staging_3way_elo
                        (delivery_id VARCHAR, match_id VARCHAR)")
  DBI::dbExecute(conn, "INSERT INTO mens_t20_staging_3way_elo
                        SELECT 'd' || i, 'm' || i FROM range(2000) t(i)")
  expect_error(promote_3way_elo_staging("mens_t20", conn), "older schema")
  # and the live table is untouched
  expect_equal(DBI::dbGetQuery(conn, "SELECT COUNT(*) n FROM mens_t20_3way_elo")$n, 1000)
})
