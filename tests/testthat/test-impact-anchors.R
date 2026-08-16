# Anchor checks for the impact rating leaderboards (RAA + kappa*WPA, D-P11).
#
# A statistic computed across a population is unfalsifiable-looking until you
# locate a few points on it that you already understand, so the points are
# committed here. These caught the original EPR findings (2026-08-13) and now
# guard the replacement engine.
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

.anchor_pool_cache <- new.env(parent = emptyenv())

anchor_pool <- function(format) {
  if (!is.null(.anchor_pool_cache[[format]])) return(.anchor_pool_cache[[format]])
  conn <- tryCatch(get_db_connection(read_only = TRUE), error = function(e) NULL)
  skip_if(is.null(conn), "database not available")
  on.exit(try(DBI::dbDisconnect(conn, shutdown = TRUE), silent = TRUE), add = TRUE)

  for (tb in c("cricinfo_ball_win_probability", "cricinfo_ball_raa")) {
    n <- tryCatch(
      DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) AS n FROM main.%s", tb))$n,
      error = function(e) 0
    )
    skip_if(n == 0, sprintf("main.%s is empty -- run its builder", tb))
  }

  pgd <- create_player_game_data(format, conn = conn)
  imp <- calculate_impact(format, player_game_data = pgd)
  nm <- unique(pgd[!is.na(player_name), c("player_id", "player_name")])
  imp <- merge(imp, nm, by = "player_id", all.x = TRUE)
  imp <- imp[imp$n_matches >= ANCHOR_MIN_MATCHES, ]
  skip_if(nrow(imp) < 30, "too few qualifying players to rank")

  imp <- imp[order(-imp$batting_impact), ]
  imp$bat_rank <- seq_len(nrow(imp))
  imp <- imp[order(-imp$bowling_impact), ]
  imp$bowl_rank <- seq_len(nrow(imp))
  .anchor_pool_cache[[format]] <- imp
  imp
}

test_that("ODI batting anchors rank in the top 20", {
  pool <- anchor_pool("odi")

  for (id in names(ANCHOR_BAT)) {
    row <- pool[pool$player_id == id, ]
    expect_equal(nrow(row), 1L,
                 info = paste(ANCHOR_BAT[[id]], "missing from the ODI pool"))
    expect_lte(row$bat_rank, ANCHOR_TOP_N,
               label = sprintf("%s ODI batting rank (%d of %d)",
                               ANCHOR_BAT[[id]], row$bat_rank, nrow(pool)))
  }
})

test_that("ODI bowling anchor ranks in the top 20", {
  pool <- anchor_pool("odi")

  for (id in names(ANCHOR_BOWL)) {
    row <- pool[pool$player_id == id, ]
    expect_equal(nrow(row), 1L)
    expect_lte(row$bowl_rank, ANCHOR_TOP_N,
               label = sprintf("%s ODI bowling rank (%d of %d)",
                               ANCHOR_BOWL[[id]], row$bowl_rank, nrow(pool)))
  }
})

test_that("the ICC soft-reference sweep runs and reports, without hard-failing", {
  # Pete's rule (2026-08-14): ICC rankings are a SOFT reference, not a hard
  # anchor -- their pool, window and weighting all differ from ours, and the
  # impact rating's leverage term (kappa*WPA) legitimately demotes players ICC
  # rates highly (D-P11 accepted that trade-off with eyes open; Theekshana at
  # 92/98 vs ICC #6 is the canonical case, bouncerverse#27). So this sweep
  # PRINTS every ICC top-10 player sitting in the bottom half of our pooled
  # list for a human to read, and only asserts that the sweep itself ran on
  # real data. Harden it into a floor only with a deliberate decision.
  icc_path <- file.path("..", "..", "..", "docs", "reference",
                        "icc-rankings-2026-08.csv")
  skip_if(!file.exists(icc_path), "ICC reference CSV not available")
  icc <- utils::read.csv(icc_path, stringsAsFactors = FALSE)
  surname <- function(x) tolower(vapply(strsplit(x, " "), function(p) p[length(p)], ""))

  checked <- 0L
  for (fmt in c("odi", "t20")) {
    pool <- anchor_pool(fmt)
    pool_sn <- surname(ifelse(is.na(pool$player_name), "", pool$player_name))
    band <- ceiling(nrow(pool) / 2)

    for (disc in c("batting", "bowling")) {
      icc_fmt <- if (fmt == "t20") "t20i" else "odi"
      top10 <- icc[icc$format == icc_fmt & icc$discipline == disc & icc$rank <= 10, ]
      rank_col <- if (disc == "batting") "bat_rank" else "bowl_rank"
      for (nm in top10$player_name) {
        hits <- which(pool_sn == surname(nm))
        if (length(hits) != 1) next  # absent from pool, or ambiguous surname
        checked <- checked + 1L
        if (pool[[rank_col]][hits] > band) {
          cli::cli_inform(
            "ICC soft-reference flag: {nm} ({toupper(fmt)} ICC top-10 {disc}) ranks {pool[[rank_col]][hits]}/{nrow(pool)} on impact."
          )
        }
      }
    }
  }
  expect_gt(checked, 10)  # the sweep matched real players, so it actually ran
})

test_that("the T20 pooled leaderboard is still unstratified -- remove this when #21 lands", {
  # Deliberately asserts the KNOWN state. The pooled T20 list mixes genders
  # and minor leagues, so the named anchors fail there under EVERY engine
  # measured on 2026-08-14 (bouncerverse#18: Kohli 65-116/365 depending on
  # option). The fix is pool stratification (#21), not the metric. When
  # stratification lands and Kohli enters the top 20 of the appropriate pool,
  # this test FAILS -- that is the signal to enable real T20 anchor
  # assertions.
  pool <- anchor_pool("t20")
  row <- pool[pool$player_id == "49752", ]  # Kohli
  skip_if(nrow(row) != 1, "Kohli not in the T20 pool")
  expect_gt(row$bat_rank, ANCHOR_TOP_N)
})
