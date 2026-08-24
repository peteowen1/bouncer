# The schema-drift path used to DROP the whole table.
#
# `store_player_rating_v2()` is called once per bucket, and the file's own
# contract says replacement is per bucket, never a whole-table wipe. The
# recovery path contradicted it: on any shape mismatch it dropped the table. Add
# one column and the FIRST successful store wiped every bucket; if a later
# bucket then hit its `check_anchor()` abort, the buckets not yet re-stored were
# permanently gone. `.in_transaction()` does not help — the other buckets were
# never in that transaction (bouncerverse#45).
#
# These tests pin the property the contract asserts: an earlier bucket survives
# a schema change made between two stores.

skip_if_no_duckdb <- function() {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")
}

# A table deliberately one column short of the current schema, standing in for
# "the shape before someone added a column".
old_shape_table <- function(conn, table_name, schema, drop_col) {
  short <- schema[setdiff(names(schema), drop_col)]
  DBI::dbExecute(conn, sprintf("CREATE SCHEMA IF NOT EXISTS main"))
  DBI::dbExecute(conn, sprintf("CREATE TABLE main.%s (%s)", table_name,
                               paste(names(short), unname(short), collapse = ", ")))
  invisible(short)
}

fake_rating <- function(n = 3, seed = 1) {
  data.frame(
    rank = seq_len(n),
    player_id = paste0("p", seq_len(n) + seed),
    player_name = paste("Player", seq_len(n) + seed),
    rating = seq_len(n) / 10,
    average = seq_len(n) + 20,
    main_comp = "Indian Premier League",
    matches = seq_len(n) * 10L,
    balls = seq_len(n) * 100L,
    effective_matches = seq_len(n) * 9.5,
    last_match = as.Date("2026-08-01"),
    stringsAsFactors = FALSE
  )
}

test_that("a missing column is added, and every existing row survives", {
  skip_if_no_duckdb()
  f <- withr::local_tempfile(fileext = ".duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  old_shape_table(conn, "player_rating_v2", .rating_v2_schema, "main_comp")
  DBI::dbExecute(conn, "INSERT INTO main.player_rating_v2 (format, gender, role, player_id)
                        VALUES ('T20', 'male', 'batter', 'keepme')")

  expect_true(.migrate_schema(conn, "player_rating_v2", .rating_v2_schema))

  cols <- DBI::dbGetQuery(conn, "SELECT * FROM main.player_rating_v2")
  expect_true("main_comp" %in% names(cols))
  expect_equal(nrow(cols), 1L)            # the row that mattered
  expect_equal(cols$player_id, "keepme")
  expect_true(is.na(cols$main_comp))       # new column, no value to invent
})

test_that("a column the schema no longer names is reported, not dropped", {
  skip_if_no_duckdb()
  f <- withr::local_tempfile(fileext = ".duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS main")
  ddl <- paste(names(.rating_v2_schema), unname(.rating_v2_schema), collapse = ", ")
  DBI::dbExecute(conn, sprintf(
    "CREATE TABLE main.player_rating_v2 (%s, retired_metric DOUBLE)", ddl))
  DBI::dbExecute(conn, "INSERT INTO main.player_rating_v2 (player_id, retired_metric)
                        VALUES ('keepme', 1.5)")

  expect_true(.migrate_schema(conn, "player_rating_v2", .rating_v2_schema))

  got <- DBI::dbGetQuery(conn, "SELECT * FROM main.player_rating_v2")
  # Dropping it would be data loss to tidy up metadata.
  expect_true("retired_metric" %in% names(got))
  expect_equal(got$retired_metric, 1.5)
})

test_that("a type mismatch aborts instead of dropping the table", {
  skip_if_no_duckdb()
  f <- withr::local_tempfile(fileext = ".duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS main")
  wrong <- .rating_v2_schema
  wrong[["rating"]] <- "VARCHAR"
  DBI::dbExecute(conn, sprintf("CREATE TABLE main.player_rating_v2 (%s)",
                               paste(names(wrong), unname(wrong), collapse = ", ")))
  DBI::dbExecute(conn, "INSERT INTO main.player_rating_v2 (player_id) VALUES ('keepme')")

  expect_error(.migrate_schema(conn, "player_rating_v2", .rating_v2_schema),
               "wrong type")
  # The abort must leave the data alone -- that is the entire point.
  expect_equal(
    DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM main.player_rating_v2")$n, 1)
})

test_that("an absent table is a no-op, not an error", {
  skip_if_no_duckdb()
  f <- withr::local_tempfile(fileext = ".duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS main")
  expect_false(.migrate_schema(conn, "player_rating_v2", .rating_v2_schema))
})

test_that("REGRESSION: a schema change between two stores does not wipe the first bucket", {
  skip_if_no_duckdb()
  f <- withr::local_tempfile(fileext = ".duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  # Bucket one lands in a table of the OLD shape, exactly as it would have on
  # the day before someone added a column.
  old_shape_table(conn, "player_rating_v2", .rating_v2_schema, "main_comp")
  short <- setdiff(.rating_v2_cols, "main_comp")
  d1 <- fake_rating(seed = 0)
  d1$format <- "T20"; d1$gender <- "male"; d1$role <- "batter"
  d1$as_at <- as.Date("2026-08-01")
  DBI::dbWriteTable(conn, DBI::Id(schema = "main", table = "player_rating_v2"),
                    d1[, short], append = TRUE)
  expect_equal(
    DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM main.player_rating_v2")$n, 3)

  # Bucket two is stored through the real path, which now migrates the table.
  suppressMessages(
    store_player_rating_v2(conn, fake_rating(seed = 10), "odi", "male", "bowler",
                           as_at = "2026-08-02"))

  got <- DBI::dbGetQuery(conn, "SELECT format, role, COUNT(*) AS n
                                FROM main.player_rating_v2 GROUP BY 1, 2 ORDER BY 1")
  # Before the fix the T20 bucket was gone and only ODI remained.
  expect_equal(nrow(got), 2L)
  expect_setequal(got$format, c("T20", "ODI"))
  expect_true(all(got$n == 3))
})

test_that("REGRESSION: the same holds for the value table", {
  skip_if_no_duckdb()
  f <- withr::local_tempfile(fileext = ".duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  old_shape_table(conn, "player_value_v2", .value_v2_schema, "calibrated")
  DBI::dbExecute(conn, "INSERT INTO main.player_value_v2 (format, gender, player_id)
                        VALUES ('T20', 'male', 'keepme')")

  v <- data.frame(
    rank = 1:2, player_id = c("a", "b"), player_name = c("A", "B"),
    total_value = c(1, 2), bat_value = c(1, 1), bowl_value = c(0, 1),
    matches = c(10L, 20L), bat_balls = c(100L, 200L), bowl_balls = c(0L, 60L),
    calibrated = c(1.1, 2.2), stringsAsFactors = FALSE)
  suppressMessages(
    store_player_value_v2(conn, v, "odi", "male", as_at = "2026-08-02"))

  got <- DBI::dbGetQuery(conn, "SELECT format, COUNT(*) AS n
                                FROM main.player_value_v2 GROUP BY 1 ORDER BY 1")
  expect_setequal(got$format, c("T20", "ODI"))
})

test_that("the schema vector and the column list cannot drift apart", {
  # They were separate declarations, which is how the shape mismatch this
  # whole file is about became possible in the first place.
  expect_identical(.rating_v2_cols, names(.rating_v2_schema))
  expect_identical(.value_v2_cols, names(.value_v2_schema))
  expect_true(all(nzchar(unname(.rating_v2_schema))))
  expect_true(all(nzchar(unname(.value_v2_schema))))
})
