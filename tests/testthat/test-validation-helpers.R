# Tests for Validation Helper Functions
#
# Covers: validate_sql_identifier, validate_match_ids, table_exists

# ============================================================================
# validate_sql_identifier() Tests
# ============================================================================

test_that("validate_sql_identifier accepts valid identifiers", {
  expect_invisible(validate_sql_identifier("matches"))
  expect_invisible(validate_sql_identifier("cricsheet.matches"))
  expect_invisible(validate_sql_identifier("t20_player_skill"))
  expect_invisible(validate_sql_identifier("main.team_elo"))
  expect_invisible(validate_sql_identifier("cricinfo.balls"))
})

test_that("validate_sql_identifier rejects dangerous input", {
  # SQL injection attempts

  expect_error(validate_sql_identifier("table; DROP TABLE x"), "Invalid SQL identifier")
  expect_error(validate_sql_identifier("table--comment"), "Invalid SQL identifier")
  expect_error(validate_sql_identifier("table' OR 1=1"), "Invalid SQL identifier")

  # Spaces and special characters
  expect_error(validate_sql_identifier("table name"), "Invalid SQL identifier")
  expect_error(validate_sql_identifier("table\nname"), "Invalid SQL identifier")
  expect_error(validate_sql_identifier("table*"), "Invalid SQL identifier")
})

test_that("validate_sql_identifier rejects uppercase", {
  # Uppercase is intentionally rejected for consistency
  expect_error(validate_sql_identifier("TABLE"), "Invalid SQL identifier")
  expect_error(validate_sql_identifier("T20_table"), "Invalid SQL identifier")
  expect_error(validate_sql_identifier("Matches"), "Invalid SQL identifier")
})

test_that("validate_sql_identifier rejects invalid types", {
  expect_error(validate_sql_identifier(NA), "single non-NA string")
  expect_error(validate_sql_identifier(NULL), "single non-NA string")
  expect_error(validate_sql_identifier(123), "single non-NA string")
  expect_error(validate_sql_identifier(c("a", "b")), "single non-NA string")
  expect_error(validate_sql_identifier(""), "Invalid SQL identifier")
})

# ============================================================================
# validate_match_ids() Tests
# ============================================================================

test_that("validate_match_ids accepts valid numeric IDs", {
  # Character numeric IDs
  expect_invisible(validate_match_ids(c("123", "456", "789")))
  expect_invisible(validate_match_ids("1502145"))

  # Numeric IDs (coerced to character internally)
  expect_invisible(validate_match_ids(c(123, 456)))
  expect_invisible(validate_match_ids(1502145))
})

test_that("validate_match_ids rejects non-numeric strings", {
  expect_error(validate_match_ids("abc"), "Non-numeric match IDs")
  expect_error(validate_match_ids(c("123", "abc")), "Non-numeric match IDs")
  expect_error(validate_match_ids("123; DROP TABLE"), "Non-numeric match IDs")
  expect_error(validate_match_ids("-1"), "Non-numeric match IDs")
  expect_error(validate_match_ids("12.5"), "Non-numeric match IDs")
})

test_that("validate_match_ids rejects invalid types", {
  expect_error(validate_match_ids(TRUE), "character or numeric")
  expect_error(validate_match_ids(list(1, 2)), "character or numeric")
})

test_that("validate_match_ids error message shows bad IDs", {
  expect_error(validate_match_ids(c("123", "bad_id")), "bad_id")
})

# ============================================================================
# table_exists() Tests
# ============================================================================

test_that("table_exists detects existing tables", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  DBI::dbExecute(conn, "CREATE TABLE test_table (id INTEGER)")

  expect_true(table_exists(conn, "test_table"))
  expect_true(table_exists(conn, "main.test_table"))
})

test_that("table_exists returns FALSE for non-existent tables", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  expect_false(table_exists(conn, "nonexistent_table"))
  expect_false(table_exists(conn, "main.nonexistent_table"))
})

test_that("table_exists handles schema-qualified names", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS test_schema")
  DBI::dbExecute(conn, "CREATE TABLE test_schema.my_table (id INTEGER)")

  expect_true(table_exists(conn, "test_schema.my_table"))
  expect_false(table_exists(conn, "main.my_table"))
  expect_false(table_exists(conn, "test_schema.other_table"))
})

test_that("table_exists defaults unqualified names to main schema", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  DBI::dbExecute(conn, "CREATE TABLE in_main (id INTEGER)")

  # Unqualified should check main schema
  expect_true(table_exists(conn, "in_main"))
})
