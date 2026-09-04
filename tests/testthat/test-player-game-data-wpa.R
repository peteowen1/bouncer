# Which NAs may be zeroed, and which must survive.
#
# .merge_batting_bowling() fills NA value columns with 0 so a pure bowler does
# not carry NA batting stats. That is right for counting stats and wrong for
# WPA/ERA, where NA also means "this match has no win probability at all".
# Conflating them fabricated a neutral performance for 13,668 of 15,012 ODI
# player-match rows on the scraped source -- 91% of the format -- and disarmed
# calculate_epr()'s coverage warning, which can only fire on NA.
#
# These tests build the two aggregation frames directly, so they run on CI
# without the 18GB database.

bat_row <- function(match_id = "m1", player_id = "p1", balls = 30,
                    wpa = 0.05, era = 4, raa = era, ...) {
  data.table::data.table(
    match_id = match_id, player_id = player_id, match_date = as.Date("2026-01-01"),
    batting_balls_faced = balls, batting_runs = 40, batting_wpa = wpa,
    batting_max_wpa = 0.1, batting_positive_wpa_pct = 0.5, batting_era = era,
    batting_raa = raa, batting_raa_balls = balls, ...
  )
}

bowl_row <- function(match_id = "m1", player_id = "p2", balls = 24,
                     wpa = 0.03, era = 3, raa = era, ...) {
  data.table::data.table(
    match_id = match_id, player_id = player_id, match_date = as.Date("2026-01-01"),
    bowling_balls_bowled = balls, bowling_wickets = 2, bowling_wpa = wpa,
    bowling_max_wpa = 0.08, bowling_era = era,
    bowling_raa = raa, bowling_raa_balls = balls, ...
  )
}

test_that("a player who did not bat gets zero batting WPA, not NA", {
  pgd <- .merge_batting_bowling(bat_row(player_id = "batter"), bowl_row(player_id = "bowler"))

  bowler <- pgd[player_id == "bowler"]
  expect_equal(nrow(bowler), 1L)
  expect_equal(bowler$batting_wpa, 0)
  expect_equal(bowler$batting_era, 0)
  expect_equal(bowler$batting_raa, 0)
  expect_equal(bowler$batting_runs, 0)
  expect_false(is.na(bowler$batting_wpa))
})

test_that("a player who DID bat keeps NA WPA when the match has no win probability", {
  # The aggregation returns NA for batting_wpa when SUM(delta_wp) ran over an
  # all-NULL group -- the player batted, the match simply has no WP.
  batted_unmeasured <- bat_row(player_id = "batter", wpa = NA_real_, era = NA_real_)
  pgd <- .merge_batting_bowling(batted_unmeasured, bowl_row(player_id = "bowler"))

  batter <- pgd[player_id == "batter"]
  expect_true(is.na(batter$batting_wpa))
  expect_true(is.na(batter$batting_era))

  # Counting stats are untouched by this: the player really did face 30 balls.
  expect_equal(batter$batting_balls_faced, 30)
  expect_equal(batter$batting_runs, 40)
})

test_that("the same rule applies to bowling", {
  bowled_unmeasured <- bowl_row(player_id = "bowler", wpa = NA_real_, era = NA_real_)
  pgd <- .merge_batting_bowling(bat_row(player_id = "batter"), bowled_unmeasured)

  bowler <- pgd[player_id == "bowler"]
  expect_true(is.na(bowler$bowling_wpa))
  expect_true(is.na(bowler$bowling_era))
  expect_equal(bowler$bowling_wickets, 2)

  batter <- pgd[player_id == "batter"]
  expect_equal(batter$bowling_wpa, 0)   # did not bowl -> genuinely zero
})

test_that("total_wpa propagates NA rather than treating an unmeasured innings as zero", {
  pgd <- .merge_batting_bowling(
    bat_row(player_id = "batter", wpa = NA_real_, era = NA_real_),
    bowl_row(player_id = "batter")       # same player: all-rounder
  )

  ar <- pgd[player_id == "batter"]
  expect_equal(ar$role, "all_rounder")
  expect_true(is.na(ar$total_wpa))
  expect_true(is.na(ar$total_era))
  expect_true(is.na(ar$total_raa))
})

test_that("an all-rounder with both halves measured still totals normally", {
  pgd <- .merge_batting_bowling(
    bat_row(player_id = "ar", wpa = 0.05, era = 4),
    bowl_row(player_id = "ar", wpa = 0.03, era = 3)
  )

  ar <- pgd[player_id == "ar"]
  expect_equal(ar$total_wpa, 0.08)
  expect_equal(ar$total_era, 7)
  expect_equal(ar$total_raa, 7)
})

test_that("the win probability source is chosen in one place and both options are valid SQL shapes", {
  ours <- .wp_source_sql("bouncer")
  theirs <- .wp_source_sql("cricinfo")

  # Ours must join the WP table; both must join the innings-team lookup the
  # perspective flip needs (bouncerverse#25).
  expect_match(ours$join, "bouncer_wp_from_cricinfo")
  expect_match(ours$join, "ON w\\.id = b\\.id")
  for (j in list(ours$join, theirs$join)) {
    expect_match(j, "FROM cricinfo\\.innings GROUP BY match_id, innings_number")
    expect_match(j, "WHERE innings_number = 1")
  }

  expect_identical(ours$col, "w.win_prob_after")
  expect_identical(theirs$col, "b.win_probability")

  # Ours uses the delta precomputed alongside the win probability (so the
  # innings-start "before" state is available for ball 1), flipped to the
  # batting team's perspective. The scraped column is P(chasing wins), so its
  # flip is NEGATED relative to ours -- if these two ever match in sign
  # structure, one of them is crediting the wrong team.
  expect_match(ours$delta, "w\\.delta_wp")
  expect_match(ours$delta, "WHEN ti\\.team_id = t1\\.team_id THEN 1 ELSE -1")
  expect_match(theirs$delta, "^-\\(")
  expect_false(grepl("^-\\(", ours$delta))

  # Theirs must LAG, never LEAD. Both stored columns are POST-delivery states,
  # so LEAD(wp) - wp is the NEXT delivery's swing credited to this delivery's
  # batter and bowler. Measured on 133,431 ODI deliveries, that put a wicket's
  # own row at 0.0104 mean absolute delta against 0.0096 for an ordinary ball;
  # under LAG it is 0.0507 against 0.0091.
  expect_match(theirs$delta, "LAG\\(b\\.win_probability\\)")
  expect_false(grepl("LEAD", theirs$delta))
  expect_false(grepl("LEAD", ours$delta))

  expect_match(theirs$delta, "ORDER BY b\\.over_number, b\\.ball_number")

  expect_error(.wp_source_sql("espn"), "should be one of")
})

test_that("the join key is id, because the composite is not unique", {
  # Guard against a well-meaning change to the 'obvious' key. Six T20/ODI rows
  # share (match_id, innings_number, over_number, ball_number); joining on it
  # would duplicate them inside the SUM()s.
  ours <- .wp_source_sql("bouncer")
  expect_false(grepl("over_number\\s*=", ours$join))
  expect_false(grepl("ball_number\\s*=", ours$join))
})
