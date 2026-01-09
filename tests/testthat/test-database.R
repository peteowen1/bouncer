# Tests for Database Functions

test_that("find_bouncerdata_dir returns character",
{
  result <- find_bouncerdata_dir(create = FALSE)

  # Should return NULL or character
  if (!is.null(result)) {
    expect_type(result, "character")
  }
})

test_that("get_default_db_path returns valid path", {
  path <- get_default_db_path()

  expect_type(path, "character")
  expect_true(grepl("bouncer\\.duckdb$", path))
})

test_that("get_db_connection returns DuckDB connection", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  expect_s4_class(conn, "duckdb_connection")
  expect_true(DBI::dbIsValid(conn))
})

test_that("database has expected tables", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  tables <- DBI::dbListTables(conn)

  # Core tables should exist
  expect_true("matches" %in% tables)
  expect_true("deliveries" %in% tables)
  expect_true("players" %in% tables)
})

test_that("matches table has expected columns", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  columns <- DBI::dbListFields(conn, "matches")

  # Key columns should exist
  expect_true("match_id" %in% columns)
  expect_true("match_type" %in% columns)
  expect_true("match_date" %in% columns)
  expect_true("team1" %in% columns)
  expect_true("team2" %in% columns)
})

test_that("deliveries table has expected columns", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  columns <- DBI::dbListFields(conn, "deliveries")

  # Key columns should exist
  expect_true("delivery_id" %in% columns)
  expect_true("match_id" %in% columns)
  expect_true("batter_id" %in% columns)
  expect_true("bowler_id" %in% columns)
  expect_true("runs_batter" %in% columns)
})

test_that("initialize_bouncer_database creates tables", {
  # Use temp directory for test
  temp_dir <- tempdir()
  temp_db <- file.path(temp_dir, "test_bouncer.duckdb")

  # Clean up any existing test DB
  if (file.exists(temp_db)) {
    file.remove(temp_db)
  }

  # Initialize fresh database
  tryCatch({
    initialize_bouncer_database(path = temp_db, overwrite = TRUE)

    # Verify it was created
    expect_true(file.exists(temp_db))

    # Verify tables exist
    conn <- get_db_connection(path = temp_db, read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

    tables <- DBI::dbListTables(conn)
    expect_true("matches" %in% tables)
    expect_true("deliveries" %in% tables)
    expect_true("players" %in% tables)
  }, finally = {
    # Clean up
    if (file.exists(temp_db)) {
      file.remove(temp_db)
    }
  })
})
