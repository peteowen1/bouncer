# compute_context_features() (bouncerverse#84/#85): league_avg_runs/
# league_avg_wicket used to be a flat, unweighted, all-time causal running
# mean, computed independently (and identically) in both
# 01_train_agnostic_model.R and raa_cricsheet.R -- exactly the "same list
# typed out separately" drift shape bouncerverse#45 already happened once for
# a different feature. One shared function now, a decayed venue->league
# nested hierarchy instead of a flat league-only mean.
#
# The underlying decay math (.decayed_causal_prior(),
# time_causal_hierarchical_mean_decayed()) has its own thorough test coverage
# in test-venue-rates.R -- these tests are about the wiring: does this
# function query the right shape, canonicalise venues, and produce a real
# value for every row.

skip_if_no_cricsheet_db <- function() {
  conn <- tryCatch(get_db_connection(read_only = TRUE), error = function(e) NULL)
  skip_if(is.null(conn), "database unavailable")
  DBI::dbDisconnect(conn, shutdown = TRUE)
}

test_that("compute_context_features returns one row per match_id with no NAs", {
  skip_if_no_cricsheet_db()
  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  ipl_2026 <- DBI::dbGetQuery(conn, "
    SELECT match_id FROM cricsheet.matches
    WHERE event_name LIKE '%Indian Premier League%' AND season = '2026' LIMIT 10
  ")$match_id
  skip_if(length(ipl_2026) == 0, "no IPL 2026 matches in this database")

  ctx <- compute_context_features(conn, "'t20', 'it20'")
  expect_true(all(c("match_id", "league_avg_runs", "league_avg_wicket") %in% names(ctx)))
  expect_false(anyDuplicated(ctx$match_id) > 0)
  expect_false(anyNA(ctx$league_avg_runs))
  expect_false(anyNA(ctx$league_avg_wicket))
  # Sanity bounds -- a T20 runs/ball average outside [0.3, 3] or a wicket
  # rate outside [0, 0.3] would indicate a broken join, not a real value.
  expect_true(all(ctx$league_avg_runs > 0.3 & ctx$league_avg_runs < 3))
  expect_true(all(ctx$league_avg_wicket >= 0 & ctx$league_avg_wicket < 0.3))
})

test_that("compute_context_features moves IPL 2026 materially toward its real, current scoring rate", {
  skip_if_no_cricsheet_db()
  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  ipl_2026 <- DBI::dbGetQuery(conn, "
    SELECT match_id FROM cricsheet.matches
    WHERE event_name LIKE '%Indian Premier League%' AND season = '2026'
  ")$match_id
  skip_if(length(ipl_2026) == 0, "no IPL 2026 matches in this database")

  ctx <- compute_context_features(conn, "'t20', 'it20'")
  ipl_ctx <- ctx[ctx$match_id %in% ipl_2026, ]

  # REGRESSION guard for the bug this fix closes: the OLD flat feature sat
  # near 1.25-1.26 for IPL 2026 (measured 2026-08-29, docs/DECISIONS.md) while
  # actual IPL 2026 scoring is ~1.56 runs/ball. The new feature must sit
  # meaningfully above the old flat value -- not necessarily all the way to
  # 1.56 (that would mean zero shrinkage, which isn't the design), but a
  # real, large move, matching the cheap screen's ~65% gap closure.
  expect_gt(mean(ipl_ctx$league_avg_runs), 1.35)
})

test_that("compute_context_features degrades gracefully with no venue_aliases table", {
  # Simulates a fresh/test database that hasn't populated venue_aliases yet --
  # table_exists() should gate the canonicalisation step, not error.
  skip_if_not_installed("duckdb")
  f <- withr::local_tempfile(fileext = ".duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f)
  withr::defer(DBI::dbDisconnect(conn, shutdown = TRUE))
  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS cricsheet")
  DBI::dbExecute(conn, "
    CREATE TABLE cricsheet.matches (match_id VARCHAR, match_type VARCHAR, event_name VARCHAR,
      match_date DATE, venue VARCHAR)")
  DBI::dbExecute(conn, "
    CREATE TABLE cricsheet.deliveries (match_id VARCHAR,
      runs_batter INTEGER, runs_extras INTEGER, is_wicket BOOLEAN)")
  DBI::dbExecute(conn, "INSERT INTO cricsheet.matches VALUES
    ('m1', 't20', 'Test League', '2020-01-01', 'Ground A')")
  DBI::dbExecute(conn, "INSERT INTO cricsheet.deliveries VALUES
    ('m1', 1, 0, FALSE),
    ('m1', 4, 0, FALSE)")

  ctx <- compute_context_features(conn, "'t20', 'it20'")
  expect_equal(nrow(ctx), 1L)
  expect_false(anyNA(ctx$league_avg_runs))
})
