# cricinfo_match_outcome() parses the result out of status_text.
#
# This exists because deriving the result from the scores
# (innings2_total <= innings1_total) is 33 points wrong on rain-affected
# matches, where the chase wins on a reduced target and so reads as a
# batting-first win. That single mistake made a correctly-calibrated model look
# badly miscalibrated for most of a session, so the parsing gets tests.
#
# Every string below is a real value from cricinfo.matches.status_text.

test_that("a win by runs is a batting-first win", {
  o <- cricinfo_match_outcome("Australia won by 66 runs")
  expect_identical(o$result, "batting_first")
  expect_identical(o$bf_won, 1L)
  expect_identical(o$margin, 66)
  expect_identical(o$margin_type, "runs")
  expect_false(o$is_dls)
})

test_that("a win by wickets is a chase win", {
  o <- cricinfo_match_outcome("India won by 10 wickets (with 28 balls remaining)")
  expect_identical(o$result, "chasing")
  expect_identical(o$bf_won, 0L)
  expect_identical(o$margin, 10)
  expect_identical(o$margin_type, "wickets")
})

test_that("the balls-remaining clause does not confuse the margin", {
  o <- cricinfo_match_outcome("Nepal won by 6 wickets (with 23 balls remaining)")
  expect_identical(o$margin, 6)          # not 23
  expect_identical(o$margin_type, "wickets")
})

test_that("DLS results parse normally and are flagged", {
  o <- cricinfo_match_outcome(c(
    "Hong Kong won by 3 wickets (with 7 balls remaining) (DLS method)",
    "SA Women won by 150 runs (DLS method)"
  ))
  expect_identical(o$bf_won, c(0L, 1L))
  expect_identical(o$is_dls, c(TRUE, TRUE))
  expect_identical(o$margin, c(3, 150))

  # This is the whole point: a DLS chase win is a CHASE win, where the
  # score-derived label would have called it a batting-first win.
  expect_identical(o$result[1], "chasing")
})

test_that("singular margins parse", {
  o <- cricinfo_match_outcome(c("England won by 1 run", "Pakistan won by 1 wicket"))
  expect_identical(o$bf_won, c(1L, 0L))
  expect_identical(o$margin, c(1, 1))
  expect_identical(o$margin_type, c("runs", "wickets"))
})

test_that("an innings victory is a batting-first win, not a runs margin", {
  o <- cricinfo_match_outcome("England won by an innings and 47 runs")
  expect_identical(o$result, "batting_first")
  expect_identical(o$bf_won, 1L)
  expect_identical(o$margin_type, "innings_and_runs")
  expect_identical(o$margin, 47)
})

test_that("ties are NA even when a Super Over settled them", {
  o <- cricinfo_match_outcome(c(
    "Match tied",
    "Match tied (India won the Super Over)",
    "Match tied (RCB won the one-over eliminator)"
  ))
  expect_identical(o$result, rep("tied", 3))
  expect_true(all(is.na(o$bf_won)))
  expect_identical(o$super_over, c(FALSE, TRUE, TRUE))
})

test_that("draws, no results and abandonments are NA", {
  o <- cricinfo_match_outcome(c(
    "Match drawn",
    "Match drawn (Vidarbha won on 1st innings)",
    "No result",
    "Match abandoned without a ball bowled"
  ))
  expect_identical(o$result, c("drawn", "drawn", "no_result", "no_result"))
  expect_true(all(is.na(o$bf_won)))
})

test_that("a draw decided on first innings is NOT read as a batting-first win", {
  # "won on 1st innings" contains no margin, but a looser matcher could treat
  # the word "won" as a result. A drawn match has no batting-first winner.
  o <- cricinfo_match_outcome("Match drawn (Vidarbha won on 1st innings)")
  expect_identical(o$result, "drawn")
  expect_true(is.na(o$bf_won))
})

test_that("NA and unrecognised text yield NA rather than a guess", {
  o <- cricinfo_match_outcome(c(NA_character_, "", "Some unparseable status"))
  expect_true(all(is.na(o$result)))
  expect_true(all(is.na(o$bf_won)))
  expect_identical(o$is_dls, c(FALSE, FALSE, FALSE))
})

test_that("it is vectorised and preserves order", {
  txt <- c("A won by 5 runs", "Match tied", "B won by 2 wickets", NA_character_,
           "C won by 100 runs (DLS method)")
  o <- cricinfo_match_outcome(txt)
  expect_equal(nrow(o), 5L)
  expect_identical(o$bf_won, c(1L, NA_integer_, 0L, NA_integer_, 1L))
  expect_identical(o$is_dls, c(FALSE, FALSE, FALSE, FALSE, TRUE))
})

test_that("an empty input returns an empty frame, not an error", {
  o <- cricinfo_match_outcome(character(0))
  expect_equal(nrow(o), 0L)
  expect_true(all(c("result", "bf_won", "margin", "margin_type",
                    "is_dls", "super_over") %in% names(o)))
})

test_that("run wins and wicket wins are mutually exclusive across the real corpus shape", {
  # Zero rows in cricinfo.matches match both patterns; a change that made them
  # overlap would silently pick one branch.
  txt <- c("A won by 5 runs", "B won by 5 wickets",
           "C won by 10 wickets (with 28 balls remaining)")
  o <- cricinfo_match_outcome(txt)
  expect_identical(o$result, c("batting_first", "chasing", "chasing"))
})
