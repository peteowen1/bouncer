# The 3-way ELO tables are keyed by gender AND format. Two production readers
# built the name without the gender, which resolved to a legacy set that is
# empty in T20 and holds stale women's-only rows in ODI and Test. Because both
# readers coalesce a miss to a neutral 1400, the features were inert in every
# format and nothing failed (bouncerverse#63).

test_that("a table name carries both gender and format", {
  expect_equal(three_way_elo_table("t20", "male"), "mens_t20_3way_elo")
  expect_equal(three_way_elo_table("odi", "female"), "womens_odi_3way_elo")
  expect_equal(three_way_elo_table("Test", "male"), "mens_test_3way_elo")
})

test_that("the legacy gender-free name is never produced", {
  # The exact defect: paste0(format, "_3way_elo").
  for (fmt in c("t20", "odi", "test")) {
    for (g in c("male", "female")) {
      expect_false(three_way_elo_table(fmt, g) == paste0(fmt, "_3way_elo"))
    }
  }
})

test_that("format aliases resolve the same way normalize_format does", {
  expect_equal(three_way_elo_table("IT20", "male"), "mens_t20_3way_elo")
  expect_equal(three_way_elo_table("ODM", "male"), "mens_odi_3way_elo")
  expect_equal(three_way_elo_table("MDM", "male"), "mens_test_3way_elo")
})

test_that("gender spellings are accepted, and an unknown one is named", {
  expect_equal(three_way_elo_gender_prefix("men"), "mens")
  expect_equal(three_way_elo_gender_prefix("Female"), "womens")
  expect_error(three_way_elo_gender_prefix("mixed"), "mixed")
})

test_that("both genders are returned for a format", {
  expect_setequal(three_way_elo_tables("t20"),
                  c("mens_t20_3way_elo", "womens_t20_3way_elo"))
})

test_that("the union query covers every table and selects every column", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  for (tb in three_way_elo_tables("t20")) {
    DBI::dbExecute(conn, sprintf(
      "CREATE TABLE %s (delivery_id VARCHAR, batter_run_elo_before DOUBLE)", tb))
  }
  q <- three_way_elo_query("t20", c("delivery_id", "batter_run_elo_before"), conn)
  expect_match(q, "mens_t20_3way_elo", fixed = TRUE)
  expect_match(q, "womens_t20_3way_elo", fixed = TRUE)
  expect_match(q, "UNION ALL", fixed = TRUE)
  # It must be valid SQL, not just a plausible string.
  expect_silent(DBI::dbGetQuery(conn, q))
})

test_that("absent tables are skipped rather than producing a failing query", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(conn, "CREATE TABLE mens_odi_3way_elo (delivery_id VARCHAR)")
  expect_equal(three_way_elo_tables("odi", conn), "mens_odi_3way_elo")
  q <- three_way_elo_query("odi", "delivery_id", conn)
  expect_false(grepl("womens", q, fixed = TRUE))
  expect_silent(DBI::dbGetQuery(conn, q))
})

test_that("no table at all returns NULL rather than broken SQL", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  expect_equal(three_way_elo_tables("test", conn), character(0))
  expect_null(three_way_elo_query("test", "delivery_id", conn))
})
