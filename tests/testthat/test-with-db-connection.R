# with_db_connection() exists to stop a leaked DuckDB connection holding the
# single-writer lock for the rest of the session. These tests cover the paths
# that matter: that the disconnect happens even when the body throws, and that
# a FAILING disconnect is never silent.

fake_conn_env <- function() new.env(parent = emptyenv())

test_that("with_db_connection returns the body's value and closes the connection", {
  skip_if_not_installed("duckdb")
  path <- file.path(withr::local_tempdir(), "t.duckdb")

  seen <- with_db_connection(function(conn) {
    expect_true(DBI::dbIsValid(conn))
    DBI::dbGetQuery(conn, "SELECT 42 AS n")$n
  }, path = path)

  expect_equal(seen, 42)
})

test_that("with_db_connection disconnects even when the body throws", {
  skip_if_not_installed("duckdb")
  path <- file.path(withr::local_tempdir(), "t.duckdb")

  captured <- NULL
  expect_error(
    with_db_connection(function(conn) {
      captured <<- conn
      stop("body blew up")
    }, path = path),
    "body blew up"
  )

  # The whole point: the connection must be closed despite the error, or the
  # write lock stays held for the session.
  expect_false(DBI::dbIsValid(captured))
})

test_that("a write connection is released after a failure, so the next write can proceed", {
  skip_if_not_installed("duckdb")
  path <- file.path(withr::local_tempdir(), "t.duckdb")

  expect_error(
    with_db_connection(function(conn) stop("first attempt failed"),
                       path = path, read_only = FALSE),
    "first attempt failed"
  )

  # Before with_db_connection() this second acquisition failed with
  # "Could not set lock" because the first connection was never released.
  expect_no_error(
    with_db_connection(function(conn) {
      DBI::dbExecute(conn, "CREATE TABLE t (x INTEGER)")
    }, path = path, read_only = FALSE)
  )
})

test_that("a failing disconnect warns rather than passing silently", {
  skip_if_not_installed("duckdb")
  path <- file.path(withr::local_tempdir(), "t.duckdb")

  # Create the database BEFORE mocking. get_db_connection() -> ensure_db_exists()
  # opens and closes its own connection to initialise the file, and that
  # disconnect is outside with_db_connection()'s tryCatch -- mocking first makes
  # the setup throw instead of the path under test.
  with_db_connection(function(conn) invisible(NULL), path = path)

  # Simulate dbDisconnect throwing. Swallowing this would hide a still-held
  # write lock -- the exact failure the function was written to prevent.
  local_mocked_bindings(
    dbDisconnect = function(...) stop("simulated disconnect failure"),
    .package = "DBI"
  )

  expect_warning(
    with_db_connection(function(conn) TRUE, path = path),
    "write lock may still be held"
  )
})

test_that("a failing disconnect does not mask an error from the body", {
  skip_if_not_installed("duckdb")
  path <- file.path(withr::local_tempdir(), "t.duckdb")
  with_db_connection(function(conn) invisible(NULL), path = path)

  local_mocked_bindings(
    dbDisconnect = function(...) stop("simulated disconnect failure"),
    .package = "DBI"
  )

  # The body's error is the informative one and must survive; the disconnect
  # failure is demoted to a warning so it cannot clobber it.
  expect_error(
    suppressWarnings(with_db_connection(function(conn) stop("the real problem"),
                                        path = path)),
    "the real problem"
  )
})
