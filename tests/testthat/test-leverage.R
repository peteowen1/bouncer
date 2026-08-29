# leverage(state) = Var[WP(state + outcome)] weighted by P(outcome | state).
# Sized empirically against six-vs-wicket before building (2026-08-29,
# debug/leverage_formula_comparison.R, gitignored): full multinomial wins on
# signal in exactly the top-1% "most leveraged" tier a clutch read is built
# from. MODELLING-IDEAS.md "Leverage-weighted WPA".

test_that("uniform WP across every outcome means zero leverage", {
  p <- matrix(c(0.2, 0.2, 0.2, 0.2, 0.2), nrow = 1)
  wp <- matrix(rep(0.6, 5), nrow = 1)
  expect_equal(.leverage_from_probs(p, wp), 0, tolerance = 1e-12)
})

test_that("leverage matches a hand-computed weighted variance", {
  # Two outcomes, 50/50: WP 0.3 and 0.7. Weighted variance = 0.5*(0.3-0.5)^2 +
  # 0.5*(0.7-0.5)^2 = 0.5*0.04 + 0.5*0.04 = 0.04.
  p <- matrix(c(0.5, 0.5), nrow = 1)
  wp <- matrix(c(0.3, 0.7), nrow = 1)
  expect_equal(.leverage_from_probs(p, wp), 0.04, tolerance = 1e-12)
})

test_that("a near-certain outcome with one rare-but-extreme alternative has small leverage", {
  # p(dot) = 0.99, p(six) = 0.01, WP(dot) = 0.50, WP(six) = 0.90 -- a dramatic
  # swing that's unlikely to happen scores LOW, unlike a raw two-point spread.
  p <- matrix(c(0.99, 0.01), nrow = 1)
  wp <- matrix(c(0.50, 0.90), nrow = 1)
  lev <- .leverage_from_probs(p, wp)
  expect_lt(lev, 0.01)
  expect_gt(lev, 0)
})

test_that("rows are renormalized, so an un-normalized p_mat (e.g. after dropping 'wide') still works", {
  p <- matrix(c(1, 1), nrow = 1)  # sums to 2, not 1
  wp <- matrix(c(0.4, 0.6), nrow = 1)
  expect_equal(.leverage_from_probs(p, wp), 0.01, tolerance = 1e-12)
})

test_that("mismatched dimensions abort", {
  p <- matrix(c(0.5, 0.5), nrow = 1)
  wp <- matrix(c(0.3, 0.7, 0.9), nrow = 1)
  expect_error(.leverage_from_probs(p, wp), "dimensions")
})

test_that("leverage is non-negative for a batch of realistic-shaped rows", {
  set.seed(1)
  n <- 200
  raw <- matrix(runif(n * 7), nrow = n)
  wp <- matrix(runif(n * 7), nrow = n)
  lev <- .leverage_from_probs(raw, wp)
  expect_true(all(lev >= -1e-12))
  expect_length(lev, n)
})


# REGRESSION (2026-08-29, caught by review): build_ball_leverage()'s
# agnostic-model feature frame omitted is_knockout/event_tier on the wrong
# assumption that prepare_agnostic_features() defaults them the way it does
# is_free_hit/league_avg_*. It does not -- they're bare symbols inside a
# dplyr::mutate(), so every call crashed with "object 'is_knockout' not
# found". This exercises the exact frame shape build_ball_leverage() builds,
# without needing a live DB or a trained model.
test_that("build_ball_leverage()'s agnostic feature frame shape doesn't error", {
  feat <- data.frame(
    match_type = "T20", innings = c(1L, 2L), over = c(5L, 12L), ball = c(3L, 1L),
    wickets_fallen = c(1L, 4L), runs_difference = c(20, -15),
    gender = c("male", "female"), is_knockout = 0L, event_tier = 3L
  )
  expect_no_error(prepare_agnostic_features(feat, "t20"))
})


test_that("team_sign is +1 when the striker's team batted first", {
  expect_equal(.wpa_team_sign(striker_team_id = 1L, team1_id = 1L, innings_number = 1L), 1)
  expect_equal(.wpa_team_sign(striker_team_id = 2L, team1_id = 1L, innings_number = 1L), -1)
})

test_that("team_sign falls back to innings parity when a team id is missing", {
  expect_equal(.wpa_team_sign(NA_integer_, 1L, innings_number = 1L), 1)
  expect_equal(.wpa_team_sign(1L, NA_integer_, innings_number = 2L), -1)
  expect_equal(.wpa_team_sign(NA_integer_, NA_integer_, innings_number = 3L), 1)
  expect_equal(.wpa_team_sign(NA_integer_, NA_integer_, innings_number = 4L), -1)
})

test_that("team_sign is vectorized", {
  got <- .wpa_team_sign(
    striker_team_id = c(1L, 2L, NA, NA),
    team1_id        = c(1L, 1L, NA, NA),
    innings_number  = c(1L, 1L, 1L, 2L)
  )
  expect_equal(got, c(1, -1, 1, -1))
})


# --- store_ball_leverage(): same #45-shape regression coverage as
# test-cricinfo-wp-storage.R / test-cricsheet-wp-storage.R ------------------

lev_rows <- function(fmt, n, id0 = 0) {
  data.table::data.table(
    id = paste0(fmt, "_", id0 + seq_len(n)), match_id = "m1", format = fmt,
    leverage = 0.01, p_wicket = 0.05, p_six = 0.05)
}

lev_conn <- function(env = parent.frame()) {
  f <- withr::local_tempfile(fileext = ".duckdb", .local_envir = env)
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = f)
  withr::defer(DBI::dbDisconnect(conn, shutdown = TRUE), envir = env)
  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS main")
  conn
}

lev_counts <- function(conn) {
  d <- DBI::dbGetQuery(conn, "SELECT format, COUNT(*) AS n
    FROM main.bouncer_leverage_from_cricinfo GROUP BY 1 ORDER BY 1")
  stats::setNames(d$n, d$format)
}

test_that("replacing one format's leverage rows leaves the others alone", {
  skip_if_not_installed("duckdb")
  conn <- lev_conn()
  suppressMessages(store_ball_leverage(conn, lev_rows("T20", 5), "t20"))
  suppressMessages(store_ball_leverage(conn, lev_rows("ODI", 3, 100), "odi"))
  expect_equal(lev_counts(conn), c(ODI = 3, T20 = 5))

  suppressMessages(store_ball_leverage(conn, lev_rows("T20", 7, 200), "t20"))
  expect_equal(lev_counts(conn), c(ODI = 3, T20 = 7))
})

test_that("a schema change migrates rather than dropping every format", {
  skip_if_not_installed("duckdb")
  conn <- lev_conn()
  suppressMessages(store_ball_leverage(conn, lev_rows("T20", 7), "t20"))

  DBI::dbExecute(conn, "ALTER TABLE main.bouncer_leverage_from_cricinfo DROP COLUMN p_six")
  suppressMessages(store_ball_leverage(conn, lev_rows("ODI", 2, 300), "odi"))

  got <- lev_counts(conn)
  expect_equal(unname(got[["T20"]]), 7)
  expect_equal(unname(got[["ODI"]]), 2)
  expect_true("p_six" %in% DBI::dbListFields(
    conn, DBI::Id(schema = "main", table = "bouncer_leverage_from_cricinfo")))
})

test_that("a column the leverage table has no home for is named, not silently dropped", {
  skip_if_not_installed("duckdb")
  conn <- lev_conn()
  d <- lev_rows("T20", 1)
  d[, surprise := 1]
  expect_error(store_ball_leverage(conn, d, "t20"), "surprise")
})
