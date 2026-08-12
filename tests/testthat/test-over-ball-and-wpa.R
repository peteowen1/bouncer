test_that("calculate_over_ball matches the convention stored in cricsheet.deliveries", {
  # All 10,895,339 rows of cricsheet.deliveries satisfy over + ball/10, and
  # every model was trained on that scale. If this test fails, the stored
  # column and the prediction paths have diverged again.
  expect_equal(calculate_over_ball(10, 3), 10.3)
  expect_equal(calculate_over_ball(0, 1), 0.1)
  expect_equal(calculate_over_ball(19, 6), 19.6)
})

test_that("calculate_over_ball is vectorised and recycles", {
  expect_equal(calculate_over_ball(c(1, 2, 3), c(1, 2, 3)), c(1.1, 2.2, 3.3))
  expect_equal(calculate_over_ball(c(1, 2), 1), c(1.1, 2.1))
  expect_length(calculate_over_ball(integer(0), integer(0)), 0)
})

test_that("calculate_over_ball spills past a full over when extras push ball above 9", {
  # Documented defect, deliberately preserved: 233,975 stored deliveries have
  # ball > 6 and 2,637 have ball >= 10, where the value collides with the next
  # over. Pinned so that "fixing" it is a conscious decision with a retrain,
  # not an accident.
  expect_equal(calculate_over_ball(5, 12), 6.2)
  expect_equal(calculate_over_ball(6, 2), 6.2)
})

test_that("every over_ball reconstruction site agrees with the helper", {
  # The bug this guards: prepare_full_features(), prepare_agnostic_features(),
  # prepare_shortform_features() and simulate_delivery() each reconstructed
  # over_ball as over + ball/6 while training read over + ball/10 from the DB.
  src <- c("agnostic_model.R", "expected_outcomes.R", "simulation.R",
           "cricsheet_parser.R")
  for (f in src) {
    path <- testthat::test_path("..", "..", "R", f)
    skip_if_not(file.exists(path), paste("missing", f))
    txt <- readLines(path, warn = FALSE)
    offenders <- grep("over.*\\+.*ball\\s*/\\s*6", txt, value = TRUE)
    offenders <- grep("^\\s*#", offenders, value = TRUE, invert = TRUE)
    expect_equal(offenders, character(0),
                 info = paste(f, "reconstructs over_ball on the /6 scale"))
  }
})


# ---------------------------------------------------------------------------
# add_win_probability
# ---------------------------------------------------------------------------

# Two matches with very different first-innings totals. Under the old code a
# single global target (the max across both) was applied to each chase.
# ODI rather than T20 because odi_stage1/stage2 are the in-match models present
# in bouncerdata/models/. Skips cleanly where they are absent (e.g. CI).
WPA_FORMAT <- "odi"

skip_without_models <- function() {
  skip_if_not(!is.null(load_in_match_models(WPA_FORMAT)),
              "in-match models unavailable")
}

two_match_deliveries <- function() {
  one <- function(mid, inn1_total, inn2_total) {
    rbind(
      data.frame(
        match_id = mid, innings = 1L, over = 0:5, ball = rep(1L, 6),
        total_runs = round(seq(inn1_total / 6, inn1_total, length.out = 6)),
        wickets_fallen = 0L
      ),
      data.frame(
        match_id = mid, innings = 2L, over = 0:5, ball = rep(1L, 6),
        total_runs = round(seq(inn2_total / 6, inn2_total, length.out = 6)),
        wickets_fallen = 0L
      )
    )
  }
  rbind(one("low_scoring", 60, 40), one("high_scoring", 220, 150))
}

test_that("a match scores identically alone and inside a multi-match batch", {
  skip_without_models()
  # The invariant the per-match target fix exists to restore. Previously the
  # batch run applied high_scoring's target (221) to low_scoring's chase.
  all_matches <- two_match_deliveries()
  just_low <- all_matches[all_matches$match_id == "low_scoring", , drop = FALSE]

  batched <- add_win_probability(all_matches, format = WPA_FORMAT)
  alone <- add_win_probability(just_low, format = WPA_FORMAT)

  from_batch <- batched[batched$match_id == "low_scoring", ]
  expect_equal(from_batch$win_prob_before, alone$win_prob_before)
  expect_equal(from_batch$win_prob_after, alone$win_prob_after)
  expect_equal(from_batch$wpa, alone$wpa)
})

test_that("per-match targets differ from a single global target", {
  skip_without_models()
  # Guards against the fix degenerating back to one target for the batch.
  all_matches <- two_match_deliveries()

  per_match <- add_win_probability(all_matches, format = WPA_FORMAT)
  global <- add_win_probability(all_matches, format = WPA_FORMAT, target = 221)

  low_per_match <- per_match[per_match$match_id == "low_scoring" &
                               per_match$innings == 2L, "win_prob_after"]
  low_global <- global[global$match_id == "low_scoring" &
                         global$innings == 2L, "win_prob_after"]

  expect_false(isTRUE(all.equal(low_per_match, low_global)))
})

test_that("win_prob_before telescopes from the previous delivery's after-state", {
  skip_without_models()
  # overs_before used (ball - 1)/10, one ball earlier than the after-state of
  # delivery i-1, so WPA did not sum to the innings win-probability swing.
  res <- add_win_probability(two_match_deliveries(), format = WPA_FORMAT)

  by_innings <- split(res, list(res$match_id, res$innings), drop = TRUE)
  for (grp in by_innings) {
    if (nrow(grp) < 2) next
    expect_equal(grp$win_prob_before[-1],
                 grp$win_prob_after[-nrow(grp)],
                 tolerance = 1e-9)
  }
})

test_that("WPA sums to the total win probability swing across an innings", {
  skip_without_models()
  res <- add_win_probability(two_match_deliveries(), format = WPA_FORMAT)

  by_innings <- split(res, list(res$match_id, res$innings), drop = TRUE)
  for (grp in by_innings) {
    swing <- grp$win_prob_after[nrow(grp)] - grp$win_prob_before[1]
    expect_equal(sum(grp$wpa), swing, tolerance = 1e-9)
  }
})

test_that("a chase with no first innings in the frame fails loudly, not as NA", {
  skip_without_models()
  # target_by_match has no entry for this match. Looking it up with [[ would
  # error "subscript out of bounds"; [ yields NA, which must become NULL so
  # predict_win_probability()'s own guard fires and the rows are counted.
  chase_only <- two_match_deliveries()
  chase_only <- chase_only[chase_only$innings == 2L, , drop = FALSE]

  expect_error(
    add_win_probability(chase_only, format = WPA_FORMAT),
    "Win probability failed"
  )
})

test_that("a vector target is rejected rather than silently recycled", {
  skip_without_models()
  expect_error(
    add_win_probability(two_match_deliveries(), format = WPA_FORMAT,
                        target = c(100, 200)),
    "single value"
  )
})
