# Tests for Data Loader Functions

# =============================================================================
# check_duckdb_available() Tests
# =============================================================================

test_that("check_duckdb_available returns TRUE when duckdb is installed", {
  # This test runs in an environment where duckdb IS installed
  # So it should return TRUE invisibly
  result <- bouncer:::check_duckdb_available()

  expect_true(result)
})

test_that("query_remote_parquet calls check_duckdb_available", {
  # Mock check_duckdb_available to throw — verify query_remote_parquet propagates
  local_mocked_bindings(
    check_duckdb_available = function() cli::cli_abort("duckdb not available (mocked)")
  )
  expect_error(
    bouncer:::query_remote_parquet("test_table", "SELECT 1"),
    "duckdb not available"
  )
})

test_that("create_remote_connection calls check_duckdb_available", {
  # Mock check_duckdb_available to throw — verify create_remote_connection propagates
  local_mocked_bindings(
    check_duckdb_available = function() cli::cli_abort("duckdb not available (mocked)")
  )
  expect_error(
    bouncer:::create_remote_connection(),
    "duckdb not available"
  )
})

# =============================================================================
# Remote Data Functions Existence Tests
# =============================================================================

test_that("remote data functions exist", {
  # Internal functions accessed via :::

  expect_true(exists("query_remote_parquet", where = asNamespace("bouncer")))
  expect_true(exists("create_remote_connection", where = asNamespace("bouncer")))

  # Exported functions
  expect_true(exists("get_remote_tables", where = asNamespace("bouncer")))
  expect_true(exists("load_matches", where = asNamespace("bouncer")))
  expect_true(exists("load_deliveries", where = asNamespace("bouncer")))
  expect_true(exists("load_players", where = asNamespace("bouncer")))
})

test_that("load_matches has source parameter", {
  # Verify the function signature includes source parameter
  args <- formals(bouncer::load_matches)
  expect_true("source" %in% names(args))

  # Default should include "local" as first option (match.arg behavior)
  source_default <- deparse(args$source)
  expect_true(grepl("local", source_default))
})

test_that("load_deliveries has source parameter", {
  args <- formals(bouncer::load_deliveries)
  expect_true("source" %in% names(args))

  # Default should include "local" as first option
  source_default <- deparse(args$source)
  expect_true(grepl("local", source_default))
})

# =============================================================================
# Remote Cache Tests
# =============================================================================

test_that("clear_remote_cache clears the cache", {
  # First, ensure cache exists
  # We can't easily populate it without network, so just test the function works
  result <- bouncer::clear_remote_cache()

  expect_null(result)
})
