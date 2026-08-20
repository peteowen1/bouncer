# A rebuild must never leave the live table empty. create_3way_elo_table(
# overwrite = TRUE) drops first and inserts hours later, so an interruption in
# between empties it silently -- which is how t20_3way_elo reached zero rows
# (bouncerverse#63).

make_stage <- function(conn, category, n) {
  stage <- paste0(three_way_elo_staging_category(category), "_3way_elo")
  DBI::dbExecute(conn, sprintf("CREATE TABLE %s (delivery_id VARCHAR)", stage))
  if (n > 0) {
    DBI::dbExecute(conn, sprintf(
      "INSERT INTO %s SELECT 'd' || i FROM range(%d) t(i)", stage, n))
  }
  stage
}

test_that("promoting swaps a complete staging table over the live one", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(conn, "CREATE TABLE mens_t20_3way_elo (delivery_id VARCHAR)")
  DBI::dbExecute(conn, "INSERT INTO mens_t20_3way_elo VALUES ('old')")
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
  DBI::dbExecute(conn, "CREATE TABLE mens_odi_3way_elo (delivery_id VARCHAR)")
  DBI::dbExecute(conn, "INSERT INTO mens_odi_3way_elo VALUES ('keep')")
  make_stage(conn, "mens_odi", 0)

  expect_error(promote_3way_elo_staging("mens_odi", conn), "Refusing to promote")
  expect_equal(DBI::dbGetQuery(conn, "SELECT * FROM mens_odi_3way_elo")$delivery_id,
               "keep")
})

test_that("a suspiciously small staging table is refused too", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(conn, "CREATE TABLE mens_test_3way_elo (delivery_id VARCHAR)")
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
