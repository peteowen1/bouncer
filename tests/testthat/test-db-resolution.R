# get_db_connection() resolving to an EMPTY database, silently.
#
# find_bouncerdata_dir() falls back to the rappdirs user-data directory when the
# walk up the tree finds no bouncerdata/ sibling, and ensure_db_exists() then
# initialises a database there. Every schema exists, every table has zero rows,
# so every query succeeds and returns nothing — indistinguishable from a
# legitimate "this format has no data" answer. Same script, same SQL, only the
# working directory different: 22,266 matches from the repo, 0 from a scratch
# directory (bouncerverse#46).
#
# These tests pin the two signals that now catch it, and — just as important —
# that the healthy path stays SILENT. A check that cries wolf on every normal
# connection is a check that gets ignored.

test_that("a resolution tag round-trips and defaults to unknown", {
  p <- .tag_resolution("some/path", "sibling")
  expect_equal(.db_resolution(p), "sibling")
  expect_equal(.db_resolution("untagged/path"), "unknown")
})

test_that("tagging does not change the path's behaviour", {
  # The tag travels as an attribute precisely so the ~60 callers of
  # find_bouncerdata_dir() are unaffected. If this breaks, they all break.
  p <- .tag_resolution("a/b", "child")
  expect_identical(as.character(p), "a/b")
  expect_identical(file.path(p, "c"), file.path("a/b", "c"))
  expect_identical(basename(p), "b")
  expect_true(is.character(p))
})

test_that("resolution from inside the repo is the sibling walk, not the fallback", {
  skip_if_not_installed("duckdb")
  p <- get_db_path()
  skip_if(!file.exists(p), "no local database")
  expect_true(.db_resolution(p) %in% c("sibling", "child"),
              info = paste("resolved by", .db_resolution(p), "->", p))
})

local({
  # A database with the right shape and no rows: the exact trap.
  make_empty_db <- function(env = parent.frame()) {
    f <- withr::local_tempfile(fileext = ".duckdb", .local_envir = env)
    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f)
    DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS cricsheet")
    DBI::dbExecute(conn, "CREATE TABLE cricsheet.matches (match_id VARCHAR)")
    DBI::dbDisconnect(conn, shutdown = TRUE)
    f
  }

  test_that("a zero-row corpus is reported", {
    skip_if_not_installed("duckdb")
    f <- make_empty_db()
    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f, read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

    rm(list = ls(envir = .db_warned), envir = .db_warned)
    expect_warning(.check_db_is_plausible(conn, .tag_resolution(f, "sibling")),
                   "zero")
  })

  test_that("the warning fires once per path, not on every connection", {
    skip_if_not_installed("duckdb")
    f <- make_empty_db()
    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f, read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
    p <- .tag_resolution(f, "sibling")

    rm(list = ls(envir = .db_warned), envir = .db_warned)
    expect_warning(.check_db_is_plausible(conn, p), "zero")
    # get_db_connection() is called from dozens of places; a warning repeated
    # dozens of times is a warning nobody reads.
    expect_silent(.check_db_is_plausible(conn, p))
  })

  test_that("the fallback resolution is reported even before the row count", {
    skip_if_not_installed("duckdb")
    f <- make_empty_db()
    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f, read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

    rm(list = ls(envir = .db_warned), envir = .db_warned)
    expect_warning(.check_db_is_plausible(conn, .tag_resolution(f, "user_data")),
                   "fallback")
  })

  test_that("strict mode turns the warning into an error", {
    skip_if_not_installed("duckdb")
    f <- make_empty_db()
    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f, read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

    rm(list = ls(envir = .db_warned), envir = .db_warned)
    withr::local_options(bouncer.strict_db = TRUE)
    expect_error(.check_db_is_plausible(conn, .tag_resolution(f, "sibling")),
                 "zero")
  })

  test_that("a database with no cricsheet schema at all is reported, not ignored", {
    skip_if_not_installed("duckdb")
    f <- withr::local_tempfile(fileext = ".duckdb")
    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

    rm(list = ls(envir = .db_warned), envir = .db_warned)
    expect_warning(.check_db_is_plausible(conn, .tag_resolution(f, "sibling")),
                   "cricsheet.matches")
  })
})

test_that("a populated database connects silently", {
  skip_if_not_installed("duckdb")
  p <- get_db_path()
  skip_if(!file.exists(p), "no local database")
  rm(list = ls(envir = .db_warned), envir = .db_warned)
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = p, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  n <- DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricsheet.matches")$n
  skip_if(n == 0, "local database is empty")
  expect_silent(.check_db_is_plausible(conn, p))
})
