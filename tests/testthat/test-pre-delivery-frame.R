# Frame-of-reference guards for ball-state features.
#
# Cricsheet's stored ball rows carry state AFTER the delivery: `total_runs` is
# the running innings score including the ball, and `wickets_fallen` includes
# the ball's own wicket. Any feature fed to a ball-outcome model must describe
# the state BEFORE the delivery, or it leaks the target.
#
# This has now failed twice. `wickets_fallen` was fixed and `total_runs` was
# not, so `runs_difference` on the first ball of an innings WAS the target:
# cor(runs_difference, runs off that ball) = 1.000 across 14,129 T20 innings.
# These tests exist so the third time is caught by CI instead of by eye.

#
# SCOPE -- which models need PRE and which legitimately use POST:
#
#   Ball-outcome models (target = THIS ball's runs/wicket) MUST use pre-delivery
#   state. Post-delivery state contains the answer. These are the agnostic and
#   full outcome models, the RAA scorer, and the skill/team/venue index scripts.
#
#   Win-probability models (target = who wins the MATCH) may use post-delivery
#   state, and do. Knowing the score after a ball cannot leak the eventual
#   result, and serving reads cricinfo's post-delivery columns, so POST/POST is
#   the frame that matches what the model is asked at serving time. See the
#   deliberate note at data-raw/models/in-match/08_test_win_probability_v3.R:50.
#   Do not "fix" those to PRE -- it would create a train/serve mismatch.
#
# The guard below is deliberately keyed to `batting_score`, which only the
# ball-outcome family builds, so it cannot fire on the win-probability path.

test_that("no query uses total_runs raw as batting_score", {
  roots <- c(
    testthat::test_path("..", "..", "R"),
    testthat::test_path("..", "..", "data-raw")
  )
  roots <- roots[dir.exists(roots)]
  skip_if(length(roots) == 0, "source tree not available")

  files <- unlist(lapply(roots, list.files,
                         pattern = "[.]R$", recursive = TRUE, full.names = TRUE))
  offenders <- Filter(function(f) {
    any(grepl("total_runs AS batting_score",
            gsub("[[:space:]]+", " ", readLines(f, warn = FALSE)), fixed = TRUE))
  }, files)

  expect_equal(
    offenders, character(0),
    info = paste0(
      "batting_score must be the PRE-delivery score: use\n",
      "  (d.total_runs - (d.runs_batter + d.runs_extras)) AS batting_score\n",
      "Offending files:\n  ", paste(offenders, collapse = "\n  ")
    )
  )
})

test_that("pre-delivery batting score telescopes exactly", {
  # The corrected expression must equal the previous delivery's stored total.
  # Ordered by delivery_id, never by (over, ball): `ball` counts extras and
  # collides past 6, so an (over, ball) sort silently reorders the innings.
  d <- data.frame(
    delivery_id = 1:6,
    total_runs  = c(0L, 0L, 1L, 1L, 6L, 10L),
    runs_batter = c(0L, 0L, 1L, 0L, 1L, 4L),
    runs_extras = c(0L, 0L, 0L, 0L, 4L, 0L)
  )
  pre <- d$total_runs - (d$runs_batter + d$runs_extras)

  expect_equal(pre, c(0L, 0L, 0L, 1L, 1L, 6L))
  expect_equal(pre[1], 0L)                       # innings opens at zero
  expect_equal(pre[-1], d$total_runs[-nrow(d)])  # telescopes onto the prior row
})

test_that("pre-delivery batting score is independent of the ball's own runs", {
  # The leak signature: on the first ball of innings 1 the pre-delivery score is
  # 0 for every innings, so it cannot correlate with what the ball produced.
  first_ball <- data.frame(
    total_runs  = c(0L, 1L, 4L, 6L, 2L),
    runs_batter = c(0L, 1L, 4L, 6L, 0L),
    runs_extras = c(0L, 0L, 0L, 0L, 2L)
  )
  pre <- first_ball$total_runs - (first_ball$runs_batter + first_ball$runs_extras)
  scored <- first_ball$runs_batter + first_ball$runs_extras

  expect_true(all(pre == 0))
  expect_equal(sd(pre), 0)            # no variance => cannot carry the target
  expect_equal(cor(first_ball$total_runs, scored), 1)  # what the bug looked like
})

test_that("wickets_fallen is corrected to the pre-delivery count", {
  d <- data.frame(wickets_fallen = c(0L, 1L, 1L, 2L), is_wicket = c(FALSE, TRUE, FALSE, TRUE))
  expect_equal(d$wickets_fallen - as.integer(d$is_wicket), c(0L, 0L, 1L, 1L))
})
