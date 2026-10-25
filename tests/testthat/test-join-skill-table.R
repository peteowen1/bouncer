# join_skill_indices(), join_venue_skill_indices() and join_team_skill_indices()
# share .join_skill_table(), which fetches through batch_skill_query(). The
# batch path (more than 10,000 delivery IDs) and quote-escaping must give the
# same result as a plain left join. In-memory DuckDB only.

make_join_fixture <- function(conn, n) {
  ids <- sprintf("m1_Durban's_Super_Giants_1_%06d", seq_len(n))
  DBI::dbWriteTable(conn, "t20_player_skill", data.frame(
    delivery_id = ids,
    batter_scoring_index = seq_len(n) / 10,
    batter_survival_rate = 0.9,
    bowler_economy_index = 1.2,
    bowler_strike_rate = 0.05,
    batter_balls_faced = seq_len(n),
    bowler_balls_bowled = 1L
  ))
  DBI::dbWriteTable(conn, "t20_team_skill", data.frame(
    delivery_id = ids,
    batting_team_runs_skill = seq_len(n) / 100,
    batting_team_wicket_skill = 0,
    bowling_team_runs_skill = 0,
    bowling_team_wicket_skill = 0,
    batting_team_balls = 1L,
    bowling_team_balls = 1L
  ))
  ids
}

test_that("join_skill_indices() batch path matches a plain join", {
  skip_if_not_installed("duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  ids <- make_join_fixture(conn, 25000)
  # One unknown ID and a duplicate, so unmatched rows and row order are covered
  deliveries <- data.frame(delivery_id = c(rev(ids), "missing", ids[1]),
                           x = seq_len(length(ids) + 2))

  out <- suppressMessages(join_skill_indices(deliveries, "t20", conn))
  expected <- dplyr::left_join(
    deliveries, DBI::dbReadTable(conn, "t20_player_skill"), by = "delivery_id")

  expect_identical(nrow(out), nrow(deliveries))
  expect_identical(out$delivery_id, deliveries$delivery_id)
  expect_equal(out$batter_scoring_index, expected$batter_scoring_index)
  expect_true(is.na(out$batter_scoring_index[length(ids) + 1]))
})

test_that("join_team_skill_indices() small path and missing-table fallback", {
  skip_if_not_installed("duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  ids <- make_join_fixture(conn, 50)
  deliveries <- data.frame(delivery_id = ids[c(5, 1, 50)])

  out <- suppressMessages(join_team_skill_indices(deliveries, "t20", conn))
  expect_equal(out$batting_team_runs_skill, c(5, 1, 50) / 100)

  # No venue table: input comes back unchanged
  out_v <- suppressMessages(join_venue_skill_indices(deliveries, "t20", conn))
  expect_identical(out_v, deliveries)
})
