# The WPA perspective flip (bouncerverse#25).
#
# Both stored win probabilities are single-perspective numbers, so summing raw
# deltas docks half of all batters for their own good work. Before the flip,
# corr(batting_wpa, runs) on T20 male was +0.45 in innings 1 and -0.43 in
# innings 2. This test recomputes the correlation from the SAME SQL fragments
# the production aggregation uses and fails if either innings ever goes
# negative again.

test_that("batting WPA correlates positively with runs in BOTH innings", {
  conn <- tryCatch(get_db_connection(read_only = TRUE), error = function(e) NULL)
  skip_if(is.null(conn), "database not available")
  on.exit(try(DBI::dbDisconnect(conn, shutdown = TRUE), silent = TRUE), add = TRUE)

  has_wp <- tryCatch(
    DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM main.bouncer_wp_from_cricinfo")$n,
    error = function(e) 0
  )
  skip_if(has_wp == 0, "win probability table not built")

  wp <- .wp_source_sql("bouncer")
  per_batter <- DBI::dbGetQuery(conn, sprintf("
    SELECT b.innings_number AS innings, b.match_id, b.batsman_player_id,
           SUM(%s) AS wpa, SUM(b.batsman_runs) AS runs
    FROM cricinfo.balls b
    %s
    JOIN cricinfo.matches m ON m.match_id = b.match_id
    WHERE m.format = 'T20' AND m.gender = 'male'
      AND (b.wides IS NULL OR b.wides = 0)
      AND b.batsman_player_id IS NOT NULL
      AND b.innings_number IN (1, 2)
    GROUP BY b.innings_number, b.match_id, b.batsman_player_id
  ", wp$delta, wp$join))
  per_batter <- per_batter[!is.na(per_batter$wpa), ]
  skip_if(nrow(per_batter) < 1000, "too few scored batter-innings to test")

  for (inn in 1:2) {
    s <- per_batter[per_batter$innings == inn, ]
    expect_gt(cor(s$wpa, s$runs), 0.2,
              label = sprintf("innings-%d corr(batting_wpa, runs)", inn))
  }
})
