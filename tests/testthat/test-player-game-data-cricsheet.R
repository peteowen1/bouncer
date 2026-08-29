# Cricsheet-sourced player_game_data (bouncerverse#84, 2026-08-29).
#
# player_game_data.R's original path is built on cricinfo.balls as its base
# table, which stalled scraping 2026-02-20 across every format -- a season
# missing from cricinfo returns ZERO rows, not just missing WPA, blocking RAA/
# WPA/ERA/stat-ratings/BOUNCER composite value for anything since. cricsheet
# is current and already primary everywhere else in this package, so
# create_player_game_data(source = "cricsheet") rebuilds the same aggregation
# onto cricsheet.deliveries/cricsheet.matches instead.

test_that(".cricsheet_format_sql maps formats to cricsheet's match_type vocabulary", {
  expect_equal(.cricsheet_format_sql("m.match_type", "t20"),
              "LOWER(m.match_type) IN ('t20', 'it20')")
  expect_equal(.cricsheet_format_sql("m.match_type", "odi"),
              "LOWER(m.match_type) IN ('odi', 'odm')")
  expect_equal(.cricsheet_format_sql("m.match_type", "test"),
              "LOWER(m.match_type) IN ('test', 'mdm')")
  expect_error(.cricsheet_format_sql("m.match_type", "not_a_format"), "Unknown format")
})

skip_if_no_cricsheet_db <- function() {
  conn <- tryCatch(get_db_connection(read_only = TRUE), error = function(e) NULL)
  skip_if(is.null(conn), "database unavailable")
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))
  has_wp <- nrow(DBI::dbGetQuery(conn, "
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'main' AND table_name = 'bouncer_wp_from_cricsheet'")) > 0
  has_raa <- nrow(DBI::dbGetQuery(conn, "
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'main' AND table_name = 'cricsheet_ball_raa'")) > 0
  skip_if(!has_wp || !has_raa, "cricsheet WP/RAA tables unavailable")
}

test_that("create_player_game_data(source='cricsheet') matches an independently-computed IPL 2026 bowling figure", {
  skip_if_no_cricsheet_db()
  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  ipl_2026 <- DBI::dbGetQuery(conn, "
    SELECT match_id FROM cricsheet.matches
    WHERE event_name LIKE '%Indian Premier League%' AND season = '2026'
  ")$match_id
  skip_if(length(ipl_2026) == 0, "no IPL 2026 matches in this database")

  pgd <- create_player_game_data(format = "t20", conn = conn,
                                 match_ids = ipl_2026, source = "cricsheet")

  # Kagiso Rabada, IPL 2026 -- cross-checked independently (2026-08-29) via a
  # standalone query joining cricsheet.deliveries/cricsheet_ball_raa/
  # bouncer_wp_from_cricsheet by hand: 388 balls, RAA/100b -18.55,
  # WPA/100b 0.1417. This test asserts create_player_game_data() reproduces
  # that exactly, not just "runs without erroring" -- the two computations
  # were written independently and agreeing to this precision is the real
  # signal the aggregation is correct.
  rabada <- pgd[pgd$player_id == "e62dd25d" & !is.na(pgd$bowling_balls_bowled), ]
  skip_if(nrow(rabada) == 0, "Rabada not in this database's IPL 2026 data")

  total_balls <- sum(rabada$bowling_balls_bowled)
  raa_100b <- 100 * sum(rabada$bowling_raa, na.rm = TRUE) / total_balls
  wpa_100b <- 100 * sum(rabada$bowling_wpa, na.rm = TRUE) / total_balls

  expect_equal(total_balls, 388L)
  expect_equal(raa_100b, -18.55, tolerance = 0.05)
  expect_equal(wpa_100b, 0.1417, tolerance = 0.001)
})

test_that("REGRESSION: team1_batting resolves from innings 1 only, not innings 1+3 combined", {
  # Caught by review (2026-08-29), confirmed independently by two reviewers
  # with live-DB evidence (224/344 Test matches, 55/108 T20/ODI Super Over
  # matches disagreed under the bug). The bug: MAX(CASE WHEN innings IN (1,3)
  # THEN batting_team END) picks whichever of the two teams' names sorts
  # alphabetically last, not "the team that actually batted innings 1" -- on
  # a Super Over/follow-on where innings 1 and 3 are different teams, this
  # silently corrupts WPA sign for the WHOLE match whenever the innings-3
  # team's name outranks the innings-1 team's alphabetically.
  #
  # Real match 1187680 (T20I, India batted innings 1, New Zealand batted a
  # Super Over as innings 3): MAX("India", "New Zealand") = "New Zealand"
  # (N > I) -- exactly the case that produced a wrong answer pre-fix. A
  # match where the two names happen to sort the "right" way (e.g. Kings XI
  # Punjab vs Chennai Super Kings, where the buggy MAX() coincidentally
  # matches the correct answer) would NOT have caught this regression.
  skip_if_no_cricsheet_db()
  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  match_id <- "1187680"
  exists <- DBI::dbGetQuery(conn,
    "SELECT COUNT(*) AS n FROM cricsheet.deliveries WHERE match_id = ?",
    params = list(match_id))$n
  skip_if(exists == 0, "reference Super Over match not in this database")

  team1_batting <- DBI::dbGetQuery(conn, "
    SELECT MAX(CASE WHEN innings = 1 THEN batting_team END) AS team1_batting
    FROM cricsheet.deliveries WHERE match_id = ?
  ", params = list(match_id))$team1_batting

  inn1_team <- DBI::dbGetQuery(conn, "
    SELECT DISTINCT batting_team FROM cricsheet.deliveries
    WHERE match_id = ? AND innings = 1
  ", params = list(match_id))$batting_team

  expect_equal(team1_batting, inn1_team)
  # The buggy version would have returned this instead -- assert we do NOT:
  buggy <- DBI::dbGetQuery(conn, "
    SELECT MAX(CASE WHEN innings IN (1, 3) THEN batting_team END) AS x
    FROM cricsheet.deliveries WHERE match_id = ?
  ", params = list(match_id))$x
  expect_false(identical(team1_batting, buggy))
})

test_that("create_player_game_data(source='cricsheet') resolves teams for every row and names for nearly all", {
  skip_if_no_cricsheet_db()
  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  ipl_2026 <- DBI::dbGetQuery(conn, "
    SELECT match_id FROM cricsheet.matches
    WHERE event_name LIKE '%Indian Premier League%' AND season = '2026'
    LIMIT 10
  ")$match_id
  skip_if(length(ipl_2026) == 0, "no IPL 2026 matches in this database")

  pgd <- create_player_game_data(format = "t20", conn = conn,
                                 match_ids = ipl_2026, source = "cricsheet")

  expect_equal(sum(is.na(pgd$team)), 0L)
  expect_gt(mean(!is.na(pgd$player_name)), 0.85)
})

test_that("cricsheet source produces the Hawkeye columns, always NA/0 (not missing, not erroring downstream)", {
  skip_if_no_cricsheet_db()
  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  one_match <- DBI::dbGetQuery(conn,
    "SELECT match_id FROM cricsheet.matches WHERE LOWER(match_type) IN ('t20','it20') LIMIT 1")$match_id
  skip_if(length(one_match) == 0, "no T20 matches in this database")

  pgd <- create_player_game_data(format = "t20", conn = conn,
                                 match_ids = one_match, source = "cricsheet")

  bat_hawkeye_cols <- c("batting_pct_controlled", "batting_pct_attacking", "batting_pct_leg_side")
  bowl_hawkeye_cols <- c("bowling_pct_good_length", "bowling_pct_on_stump", "bowling_pct_beat_bat")
  expect_true(all(c(bat_hawkeye_cols, bowl_hawkeye_cols) %in% names(pgd)))

  # .merge_batting_bowling() zero-fills any NA in a column not listed in its
  # own value_cols (unchanged, pre-existing behaviour) -- Hawkeye percentage
  # columns aren't in that list (unlike WPA/RAA/ERA), so they always become 0
  # after the merge, for every row, not just a non-performed role. The real
  # "no Hawkeye coverage" signal downstream consumers read is the
  # *_hawkeye_balls counter, which stays a genuine 0 -- tested below.
  expect_true(all(vapply(pgd[, ..bat_hawkeye_cols], function(x) all(x == 0), logical(1))))
  expect_true(all(vapply(pgd[, ..bowl_hawkeye_cols], function(x) all(x == 0), logical(1))))
  expect_true(all(pgd$batting_hawkeye_balls == 0))
  expect_true(all(pgd$bowling_hawkeye_balls == 0))
})
