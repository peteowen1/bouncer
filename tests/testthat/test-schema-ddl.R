# The per-format table DDL used to be written out by hand in create_schema()
# AND in the create_format_*_table() / create_3way_elo_table() helpers. One
# pair drifted: create_schema() built three_way_elo_params with
# k_venue_session_base / w_venue / venue_w_permanent / venue_w_session, while
# store_3way_elo_params() writes k_venue_session_max / w_venue_perm /
# w_venue_session. ensure_3way_params_table() only creates the table when it
# is missing, so on any database built by create_schema() the INSERT failed.
# In-memory DuckDB only.

test_that("store_3way_elo_params() works on a database built by create_schema()", {
  skip_if_not_installed("duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  create_schema(conn, verbose = FALSE)

  params <- list(
    format = "mens_t20",
    k_run_max = 1, k_run_min = 2, k_run_halflife = 3,
    k_wicket_max = 4, k_wicket_min = 5, k_wicket_halflife = 6,
    k_venue_perm_max = 7, k_venue_perm_min = 8, k_venue_perm_halflife = 9,
    k_venue_session_max = 10, k_venue_session_min = 11,
    k_venue_session_halflife = 12,
    w_batter = 0.5, w_bowler = 0.4, w_venue_perm = 0.07, w_venue_session = 0.03,
    runs_per_100_elo = 0.1, inactivity_halflife = 365, replacement_level = 1400
  )
  expect_no_error(
    store_3way_elo_params(params, "d1", as.Date("2024-01-01"), 10L, conn)
  )

  stored <- DBI::dbGetQuery(conn, "SELECT * FROM three_way_elo_params")
  expect_equal(nrow(stored), 1L)
  expect_equal(stored$k_venue_session_max, 10)
  expect_equal(stored$w_venue_perm, 0.07)
})

test_that("create_schema() and the per-format helpers give the same columns", {
  skip_if_not_installed("duckdb")
  c_schema <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  c_helper <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit({
    DBI::dbDisconnect(c_schema, shutdown = TRUE)
    DBI::dbDisconnect(c_helper, shutdown = TRUE)
  }, add = TRUE)

  create_schema(c_schema, verbose = FALSE)
  suppressMessages({
    create_format_skill_table("t20", c_helper)
    create_format_venue_skill_table("odi", c_helper)
    create_format_team_skill_table("test", c_helper)
  })

  cols <- function(conn, tbl) {
    DBI::dbGetQuery(conn, sprintf(
      "SELECT column_name, data_type FROM duckdb_columns()
       WHERE table_name = '%s' ORDER BY column_index", tbl))
  }
  for (tbl in c("t20_player_skill", "odi_venue_skill", "test_team_skill")) {
    expect_identical(cols(c_schema, tbl), cols(c_helper, tbl), info = tbl)
  }
})
