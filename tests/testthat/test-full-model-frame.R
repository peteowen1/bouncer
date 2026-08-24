# build_full_model_frame() had NO test at all despite carrying the two leak
# fixes this codebase has been burned by twice (see R/full_model_frame.R):
# `total_runs` and `wickets_fallen` on cricsheet.deliveries are BOTH
# POST-delivery, and the query subtracts the current ball's own contribution
# to recover the PRE-delivery state a model is allowed to see. A test that
# would still pass if either subtraction were deleted is worthless -- these
# fixtures are built so a reverted fix produces a value that fails the
# assertion, not one that merely looks close.
#
# In-memory DuckDB only (per session constraint) -- never get_db_connection()
# or the real bouncer.duckdb. Schema built via the package's own
# create_schema() so column names/types can't drift from the query under
# test, following the pattern in test-three-way-elo-staging.R.

# Builds one player-skill row per delivery so add_skill_features() (called,
# unguarded, inside build_full_model_frame) has something to join -- without
# it the fill-missing step tries to coalesce a column that was never added
# and errors before the leak-fix columns are ever returned.
make_full_frame_fixture <- function(conn) {
  create_schema(conn, verbose = FALSE)

  DBI::dbExecute(conn, "
    INSERT INTO cricsheet.deliveries
      (delivery_id, match_id, match_type, innings, over, ball, over_ball,
       venue, gender, batter_id, bowler_id, batting_team, bowling_team,
       runs_batter, runs_extras, is_wicket, total_runs, wickets_fallen)
    VALUES
      -- m1 ball 1: first ball of the innings, a WICKET scoring 0 runs.
      -- Must read PRE-delivery as 0 runs down / 0 wickets down.
      ('m1_India_1_000_01', 'm1', 'T20', 1, 0, 1, 0.1, 'MCG', 'male',
       'bat1', 'bowl1', 'India', 'Australia', 0, 0, TRUE, 0, 1),
      -- m1 ball 2: a six, no wicket. Pre-state must carry forward ball 1's
      -- OUTCOME (0 runs, 1 down), not this ball's own post-state (6 runs).
      ('m1_India_1_000_02', 'm1', 'T20', 1, 0, 2, 0.2, 'MCG', 'male',
       'bat1', 'bowl1', 'India', 'Australia', 6, 0, FALSE, 6, 1),
      -- m1 ball 3: a single. Pre-state must show 6 runs / 1 down (balls 1-2).
      ('m1_India_1_000_03', 'm1', 'T20', 1, 0, 3, 0.3, 'MCG', 'male',
       'bat1', 'bowl1', 'India', 'Australia', 1, 0, FALSE, 7, 1),
      -- m2 ball 1: first ball of a DIFFERENT innings, scores a SIX and is
      -- NOT a wicket. The critical case: if batting_score were read straight
      -- off total_runs (the reverted fix), this row alone would show 6.
      ('m2_England_1_000_01', 'm2', 'T20', 1, 0, 1, 0.1, 'Lords', 'male',
       'bat2', 'bowl2', 'England', 'India', 6, 0, FALSE, 6, 0)
  ")

  DBI::dbExecute(conn, "
    INSERT INTO t20_player_skill
      (delivery_id, batter_scoring_index, batter_survival_rate,
       bowler_economy_index, bowler_strike_rate, batter_balls_faced,
       bowler_balls_bowled)
    SELECT delivery_id, 0.8, 0.95, 0.8, 0.03, 10, 10
    FROM cricsheet.deliveries
  ")

  # Team and venue skill coverage is asserted too (full_model_frame.R) -- an
  # empty table used to be swallowed into a neutral fill by a tryCatch that
  # caught its own coverage abort (bouncerverse#63's failure shape). Seed both
  # so these tests exercise the leak-fix columns without tripping a coverage
  # abort that has nothing to do with what they're testing.
  DBI::dbExecute(conn, "
    INSERT INTO t20_team_skill
      (delivery_id, batting_team_runs_skill, batting_team_wicket_skill,
       bowling_team_runs_skill, bowling_team_wicket_skill,
       batting_team_balls, bowling_team_balls)
    SELECT delivery_id, 0, 0, 0, 0, 10, 10
    FROM cricsheet.deliveries
  ")
  DBI::dbExecute(conn, "
    INSERT INTO t20_venue_skill
      (delivery_id, venue_run_rate, venue_wicket_rate, venue_boundary_rate,
       venue_dot_rate, venue_balls)
    SELECT delivery_id, 0, 0, 0.15, 0.35, 10
    FROM cricsheet.deliveries
  ")
}

# batting_score itself isn't a returned column -- the query only exposes it
# through runs_difference = batting_score - bowling_score. Every fixture ball
# here is the FIRST innings for its batting team in its match, so
# bowling_score (completed prior innings by the bowling team) is always 0,
# which makes runs_difference == batting_score for these rows.

test_that("the first ball of an innings is 0 runs / 0 wickets down, even as a wicket", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_full_frame_fixture(conn)

  frame <- build_full_model_frame(conn, "t20", include_elo = FALSE)
  row <- frame[frame$delivery_id == "m1_India_1_000_01", ]

  expect_equal(nrow(row), 1)
  expect_equal(row$runs_difference, 0)
  expect_equal(row$wickets_fallen, 0)
})

test_that("the first ball of an innings is 0 runs even when it scores a six", {
  # The case that actually falsifies a reverted batting_score fix: this ball
  # is not a wicket, so only the runs subtraction is being exercised.
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_full_frame_fixture(conn)

  frame <- build_full_model_frame(conn, "t20", include_elo = FALSE)
  row <- frame[frame$delivery_id == "m2_England_1_000_01", ]

  expect_equal(nrow(row), 1)
  expect_equal(row$runs_difference, 0)   # NOT 6
  expect_equal(row$wickets_fallen, 0)
})

test_that("pre-delivery state carries forward the PRIOR ball's outcome, not its own", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_full_frame_fixture(conn)

  frame <- build_full_model_frame(conn, "t20", include_elo = FALSE)
  frame <- frame[order(frame$delivery_id), ]
  m1 <- frame[frame$match_id == "m1", ]
  m1 <- m1[order(m1$over, m1$ball), ]

  # Ball 2 sees only what happened on ball 1 (0 runs, 1 wicket down) --
  # NOT its own six.
  expect_equal(m1$runs_difference[2], 0)
  expect_equal(m1$wickets_fallen[2], 1)

  # Ball 3 sees balls 1-2 (0 + 6 = 6 runs, still 1 down) -- NOT its own
  # single added in.
  expect_equal(m1$runs_difference[3], 6)
  expect_equal(m1$wickets_fallen[3], 1)
})

test_that("build_full_model_frame returns one row per delivery with the expected shape", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  make_full_frame_fixture(conn)

  frame <- build_full_model_frame(conn, "t20", include_elo = FALSE)

  expect_equal(nrow(frame), 4)
  expect_true(all(c("runs_difference", "wickets_fallen", "is_knockout",
                     "event_tier", "elo_run_diff") %in% names(frame)))
  # include_elo = FALSE must zero the ELO features rather than omit them.
  expect_true(all(frame$elo_run_diff == 0))
})

test_that("an empty format returns an empty frame instead of erroring", {
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  create_schema(conn, verbose = FALSE)   # no deliveries inserted at all

  expect_no_error(frame <- build_full_model_frame(conn, "t20", include_elo = FALSE))
  expect_equal(nrow(frame), 0)
})

# .assert_skill_coverage() -----------------------------------------------
#
# Tested directly as the pure function it is (bouncerverse#63's gate),
# not only indirectly through build_full_model_frame() -- which would leave
# the empty-frame and exact-threshold branches unexercised.

test_that(".assert_skill_coverage aborts below min_cov and names the join", {
  expect_error(.assert_skill_coverage(4, 10, "player skills", min_cov = 0.5),
               "player skills")
})

test_that(".assert_skill_coverage passes at or above min_cov", {
  # Exact boundary: cov == min_cov must pass (the check is `<`, not `<=`).
  expect_no_error(.assert_skill_coverage(5, 10, "player skills", min_cov = 0.5))
  expect_no_error(.assert_skill_coverage(10, 10, "player skills"))
})

test_that(".assert_skill_coverage aborts on an empty frame rather than dividing by zero", {
  expect_error(.assert_skill_coverage(0, 0, "venue skills"), "empty")
})

test_that(".assert_skill_coverage's success message also names the join", {
  expect_message(.assert_skill_coverage(9, 10, "team skills"), "team skills")
})

test_that("a zero-coverage join is refused, not laundered into a pass", {
  # The exact defect this gate exists to catch (bouncerverse#63): a join that
  # matches NOTHING must not print a green success line containing "0/N".
  expect_error(.assert_skill_coverage(0, 100, "3-way ELO"), "3-way ELO")
})
