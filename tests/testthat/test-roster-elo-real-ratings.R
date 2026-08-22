# calculate_roster_elo() regression guard for bouncerverse#63.
#
# The existing test in test-team-predictions.R ("an unrated roster warns
# rather than silently scoring 1400") uses player ids that match under
# NEITHER the buggy legacy table name (`t20_3way_elo`) NOR the fixed
# gender-keyed one (`mens_t20_3way_elo` / `womens_t20_3way_elo`) -- so it
# passes whether the join works or not. It only proves the warning fires for
# an unrated squad, never that a RATED squad is actually read back.
#
# This file builds real gender-keyed 3-way ELO tables (via the package's own
# create_3way_elo_table(), so the schema can't drift from the query under
# test) and asserts the two things a silent regression breaks:
#   (a) a rated player's returned ELO is NOT THREE_WAY_ELO_START, and
#   (b) two rosters of different quality score DIFFERENTLY -- the assertion
#       that actually fails if bouncerverse#63 comes back, because the old
#       bug made every roster score an identical, plausible-looking 1400.
#
# In-memory DuckDB only (per session constraint) -- never get_db_connection()
# or the real bouncer.duckdb. calculate_roster_elo() takes a db_path, not a
# connection, and always opens its OWN connection internally, so a single
# in-memory dbdir=":memory:" can't be shared across calls (each dbConnect()
# to ":memory:" is a fresh, empty database -- verified empirically). Instead
# we hold one write connection open to a shared duckdb *driver* object for
# the life of the test and mock get_db_connection() to hand out additional
# connections to that SAME driver. dbDisconnect(conn, shutdown = TRUE) inside
# calculate_roster_elo() only tears down its own connection, not the shared
# driver, as long as our keep-alive connection is still open (verified
# empirically: a second connection surviving a first's shutdown = TRUE).

make_roster_elo_fixture <- function() {
  drv <- duckdb::duckdb()
  conn <- DBI::dbConnect(drv)

  create_3way_elo_table("mens_t20", conn, overwrite = TRUE)
  create_3way_elo_table("womens_t20", conn, overwrite = TRUE)

  insert_rating <- function(table, delivery_id, match_id, batter_id, bowler_id,
                             batter_run_elo, batter_wicket_elo,
                             bowler_run_elo, bowler_wicket_elo,
                             match_date = "2026-01-01") {
    DBI::dbExecute(conn, sprintf("
      INSERT INTO %s (delivery_id, match_id, match_date, batter_id, bowler_id,
                       batter_run_elo_after, batter_wicket_elo_after,
                       bowler_run_elo_after, bowler_wicket_elo_after)
      VALUES ('%s', '%s', DATE '%s', '%s', '%s', %f, %f, %f, %f)
    ", table, delivery_id, match_id, match_date, batter_id, bowler_id,
       batter_run_elo, batter_wicket_elo, bowler_run_elo, bowler_wicket_elo))
  }

  # A clearly-above-start batter and a clearly-below-start batter, men's T20.
  insert_rating("mens_t20_3way_elo", "d1", "m1", "star_batter", "some_bowler",
                1650, 1650, THREE_WAY_ELO_START, THREE_WAY_ELO_START)
  insert_rating("mens_t20_3way_elo", "d2", "m1", "weak_batter", "some_bowler",
                1150, 1150, THREE_WAY_ELO_START, THREE_WAY_ELO_START)

  # A rated player in the WOMEN'S table, to confirm both gender tables are
  # actually read (three_way_elo_tables() unions both).
  insert_rating("womens_t20_3way_elo", "d3", "m2", "star_batter_w", "some_bowler_w",
                1700, 1700, THREE_WAY_ELO_START, THREE_WAY_ELO_START)

  list(conn = conn, drv = drv)
}

test_that("calculate_roster_elo reads real gender-keyed ratings, not THREE_WAY_ELO_START", {
  fx <- make_roster_elo_fixture()
  on.exit(DBI::dbDisconnect(fx$conn, shutdown = TRUE), add = TRUE)

  local_mocked_bindings(
    get_db_connection = function(path = NULL, read_only = FALSE) {
      DBI::dbConnect(fx$drv, read_only = read_only)
    },
    .package = "bouncer"
  )

  star <- calculate_roster_elo(c("star_batter"), match_type = "t20")
  weak <- calculate_roster_elo(c("weak_batter"), match_type = "t20")
  star_w <- calculate_roster_elo(c("star_batter_w"), match_type = "t20")

  # (a) a rated player's ELO must move off the start value.
  expect_false(isTRUE(all.equal(star$team_batting_elo, THREE_WAY_ELO_START)))
  expect_equal(star$team_batting_elo, 1650)

  # (b) the assertion that actually catches bouncerverse#63 coming back:
  # two rosters of clearly different quality must NOT score identically.
  # The old bug made every roster fall back to THREE_WAY_ELO_START, so this
  # would fail with star == weak == 1400 if the table-name lookup regressed.
  expect_false(isTRUE(all.equal(star$team_batting_elo, weak$team_batting_elo)))
  expect_equal(weak$team_batting_elo, 1150)
  expect_true(star$team_batting_elo > weak$team_batting_elo)

  # The women's table is unioned in too, not just the men's.
  expect_equal(star_w$team_batting_elo, 1700)
})

test_that("compare_team_rosters tells two real rosters of different quality apart", {
  # The end-to-end symptom of bouncerverse#63: compare_team_rosters() calls
  # calculate_roster_elo() for each side, so if the table-name bug returns,
  # both teams collapse to THREE_WAY_ELO_START and the win probability is a
  # coin flip regardless of who is actually rated better.
  fx <- make_roster_elo_fixture()
  on.exit(DBI::dbDisconnect(fx$conn, shutdown = TRUE), add = TRUE)

  local_mocked_bindings(
    get_db_connection = function(path = NULL, read_only = FALSE) {
      DBI::dbConnect(fx$drv, read_only = read_only)
    },
    .package = "bouncer"
  )

  cmp <- compare_team_rosters(
    c("star_batter"), c("weak_batter"),
    "Strong XI", "Weak XI", match_type = "t20"
  )

  expect_equal(cmp$batting_advantage$team, "Strong XI")
  expect_gt(cmp$batting_advantage$margin, 0)
  # A genuine skill gap must move the win probability off 50/50.
  expect_false(isTRUE(all.equal(cmp$expected_win_prob, 0.5)))
  expect_gt(cmp$expected_win_prob, 0.5)
})
