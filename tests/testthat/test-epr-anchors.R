# Anchor checks for the EPR leaderboards.
#
# These are the checks that caught the 2026-08-13 finding, and they lived in a
# scratch file that would have been deleted. A statistic computed across a
# population is unfalsifiable-looking until you locate a few points on it that
# you already understand, so the points are committed here.
#
# Anchors chosen by Pete BEFORE any leaderboard was produced:
#   batting  Joe Root, Virat Kohli, Kane Williamson
#   bowling  Jasprit Bumrah
# Root is graded on ODI only -- he is barely a T20I regular, stated in advance.
#
# Player ids are in the cricinfo.balls space (batsman_player_id). Note that
# cricinfo.innings.player_id is a DIFFERENT id space -- 6 shared ids out of
# ~7,600/9,300, zero within any single match -- so do not "correct" these
# against that table.
ANCHOR_BAT <- c("52656" = "Joe Root", "49752" = "Virat Kohli",
                "51088" = "Kane Williamson")
ANCHOR_BOWL <- c("70640" = "Jasprit Bumrah")
ANCHOR_MIN_MATCHES <- 20L
ANCHOR_TOP_N <- 20L

anchor_epr <- function(format) {
  conn <- tryCatch(get_db_connection(read_only = TRUE), error = function(e) NULL)
  skip_if(is.null(conn), "database not available")
  on.exit(try(DBI::dbDisconnect(conn, shutdown = TRUE), silent = TRUE), add = TRUE)

  has_wp <- tryCatch(
    DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM main.cricinfo_ball_win_probability")$n,
    error = function(e) 0
  )
  skip_if(has_wp == 0,
          "main.cricinfo_ball_win_probability is empty -- run build_cricinfo_win_probability()")

  pgd <- create_player_game_data(format, conn = conn)
  epr <- calculate_epr(format, player_game_data = pgd)
  nm <- unique(pgd[!is.na(player_name), c("player_id", "player_name")])
  epr <- merge(epr, nm, by = "player_id", all.x = TRUE)
  epr <- epr[epr$n_matches >= ANCHOR_MIN_MATCHES, ]
  skip_if(nrow(epr) < 30, "too few qualifying players to rank")

  epr <- epr[order(-epr$batting_epr), ]
  epr$bat_rank <- seq_len(nrow(epr))
  epr <- epr[order(-epr$bowling_epr), ]
  epr$bowl_rank <- seq_len(nrow(epr))
  epr
}

test_that("ODI batting anchors rank in the top 20", {
  epr <- anchor_epr("odi")

  for (id in names(ANCHOR_BAT)) {
    row <- epr[epr$player_id == id, ]
    expect_equal(nrow(row), 1L,
                 info = paste(ANCHOR_BAT[[id]], "missing from the ODI pool"))
    expect_lte(row$bat_rank, ANCHOR_TOP_N,
               label = sprintf("%s ODI batting rank (%d of %d)",
                               ANCHOR_BAT[[id]], row$bat_rank, nrow(epr)))
  }
})

test_that("ODI bowling anchor ranks in the top 20", {
  epr <- anchor_epr("odi")

  for (id in names(ANCHOR_BOWL)) {
    row <- epr[epr$player_id == id, ]
    expect_equal(nrow(row), 1L)
    expect_lte(row$bowl_rank, ANCHOR_TOP_N,
               label = sprintf("%s ODI bowling rank (%d of %d)",
                               ANCHOR_BOWL[[id]], row$bowl_rank, nrow(epr)))
  }
})

test_that("the T20 leaderboard is still too noisy to rank -- remove this test when it is not", {
  # Deliberately asserts the BROKEN state. T20 batting EPR had reliability 0.403
  # at 33 innings per player on 2026-08-13, so ~60% of the observed spread was
  # sampling error and the anchors landed at Kohli 99/365 and Williamson
  # 349/365. Asserting "anchors pass" would be a permanently red test; asserting
  # nothing would let the finding rot.
  #
  # When someone fixes the reliability, this test FAILS. That is the signal to
  # delete it and enable the real anchor assertions above for T20.
  conn <- tryCatch(get_db_connection(read_only = TRUE), error = function(e) NULL)
  skip_if(is.null(conn), "database not available")
  on.exit(try(DBI::dbDisconnect(conn, shutdown = TRUE), silent = TRUE), add = TRUE)

  has_wp <- tryCatch(
    DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM main.cricinfo_ball_win_probability")$n,
    error = function(e) 0
  )
  skip_if(has_wp == 0, "win probability table not built")

  pgd <- create_player_game_data("t20", conn = conn)
  b <- pgd[pgd$batting_balls_faced > 0 & !is.na(pgd$batting_era), ]
  rel <- rating_reliability(b$batting_era, b$player_id, min_obs = ANCHOR_MIN_MATCHES)

  expect_lt(rel$reliability, 0.6)
  expect_gt(rel$obs_for(0.7), 60)
})
