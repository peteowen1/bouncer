# .match_context_cases_sql() and .innings_score_ctes_sql() are the single
# definition of the knockout/tier flags and the pre-ball score CTEs that
# training (data-raw/models/ball-outcome) and serving both splice in.
# In-memory DuckDB only.

test_that(".match_context_cases_sql() flags knockouts and tiers", {
  skip_if_not_installed("duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  DBI::dbWriteTable(conn, "mt", data.frame(
    id = 1:4,
    event_match_number = c("Final", "12", "Semi Final", NA),
    event_name = c("Indian Premier League", "Big Bash League", "Some Cup", "Club T20"),
    match_type = c("T20", "T20", "ODI", "T20")
  ))
  out <- DBI::dbGetQuery(conn, sprintf(
    "SELECT id, %s FROM mt x ORDER BY id", .match_context_cases_sql("x")))

  expect_equal(out$is_knockout, c(1, 0, 1, 0))
  expect_equal(out$event_tier, c(1, 2, 1, 3))
})

test_that(".innings_score_ctes_sql() gives the PRE-ball batting score", {
  skip_if_not_installed("duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(conn, "CREATE SCHEMA cricsheet")
  # Innings 1 (A bats): 4 then 1 -> total 5. Innings 2 (B bats): a six.
  DBI::dbWriteTable(conn, DBI::Id(schema = "cricsheet", table = "deliveries"),
    data.frame(
      delivery_id = c("a1", "a2", "b1"), match_id = "m1",
      innings = c(1L, 1L, 2L),
      batting_team = c("A", "A", "B"), bowling_team = c("B", "B", "A"),
      runs_batter = c(4L, 1L, 6L), runs_extras = 0L,
      total_runs = c(4L, 5L, 6L), match_type = "T20"
    ))
  out <- DBI::dbGetQuery(conn, sprintf(
    "WITH %s SELECT delivery_id, batting_score, bowling_score
     FROM cumulative_scores ORDER BY delivery_id",
    .innings_score_ctes_sql("match_type = 'T20'", "d.match_type = 'T20'")))

  expect_equal(out$batting_score, c(0, 4, 0))
  expect_equal(out$bowling_score, c(0, 0, 5))
})
