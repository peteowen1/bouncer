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

  tables <- DBI::dbGetQuery(conn,
    "SELECT table_schema || '.' || table_name as full_name
     FROM information_schema.tables
     WHERE table_schema IN ('cricsheet', 'cricinfo')")$full_name

  # Core tables should exist in cricsheet schema
  expect_true("cricsheet.matches" %in% tables)
  expect_true("cricsheet.deliveries" %in% tables)
  expect_true("cricsheet.players" %in% tables)
})

test_that("matches table has expected columns", {
  skip_if_not(file.exists(get_default_db_path()), "Database not available")

  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  columns <- DBI::dbGetQuery(conn,
    "SELECT column_name FROM information_schema.columns
     WHERE table_schema = 'cricsheet' AND table_name = 'matches'")$column_name

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

  columns <- DBI::dbGetQuery(conn,
    "SELECT column_name FROM information_schema.columns
     WHERE table_schema = 'cricsheet' AND table_name = 'deliveries'")$column_name

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

    tables <- DBI::dbGetQuery(conn,
      "SELECT table_schema || '.' || table_name as full_name
       FROM information_schema.tables
       WHERE table_schema IN ('cricsheet', 'cricinfo')")$full_name
    expect_true("cricsheet.matches" %in% tables)
    expect_true("cricsheet.deliveries" %in% tables)
    expect_true("cricsheet.players" %in% tables)
  }, finally = {
    # Clean up
    if (file.exists(temp_db)) {
      file.remove(temp_db)
    }
  })
})
