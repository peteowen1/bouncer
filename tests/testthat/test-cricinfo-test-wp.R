# Test-format win probability serving: the vectorized feature construction.
#
# The full parity run (300 sampled real states against the scalar path,
# max |diff| 3.3e-3) and the honest 3-way evaluation live in the record for
# bouncerverse#14; pinned here is the feature arithmetic that needs no
# database.

test_that(".test_wp_features reproduces training semantics on a known state", {
  # Innings 3, 120/4 after 34.0 overs; inn1 192 all out in 54.83 overs,
  # inn2 284 all out in 81.5 overs.
  dt <- data.table::data.table(
    innings = 3L, over_number = 35, ball_number = 0, overs_frac = 34,
    score = 120L, wickets = 4L,
    runs_1 = 192, runs_2 = 284, runs_3 = NA_real_,
    wkts_1 = 10, wkts_2 = 10, wkts_3 = NA_real_,
    overs_1 = 54.8333, overs_2 = 81.5, overs_3 = NA_real_,
    venue_avg = 340, venue_result_rate = 0.63
  )
  f <- .test_wp_features(dt)

  # team1 batted innings 1 and 3: lead = 192 + 120 - 284
  expect_equal(f$team1_lead, 28)
  expect_equal(f$batting_is_team1, 1L)
  # cum_overs adds the WHOLE current over (training convention)
  expect_equal(f$cum_overs, 34 + 54.8333 + 81.5, tolerance = 1e-6)
  expect_equal(f$total_wickets_match, 24)
  # rate projection clamps its denominator at one over
  expect_equal(f$projected_innings_total, 120 * 90 / 34, tolerance = 1e-6)
  # not innings 4: chase block zeroed
  expect_equal(f$target, 0)
  expect_equal(f$req_rate, 0)
  # follow-on: inn1 - inn2 = -92, not >= 200
  expect_equal(f$follow_on_possible, 0L)
})

test_that("follow_on_possible never uses the current innings' own total", {
  # An innings-2 ball in a match whose innings 2 FINISHED 250 behind: the
  # leaky training construction says 1; honest serving must say 0 because at
  # this ball that outcome is the future. See bouncerverse#14/#24.
  dt <- data.table::data.table(
    innings = 2L, over_number = 30, ball_number = 3, overs_frac = 29.5,
    score = 80L, wickets = 3L,
    runs_1 = 450, runs_2 = 200, runs_3 = NA_real_,   # runs_2 is inn2's FINAL total
    wkts_1 = 10, wkts_2 = 10, wkts_3 = NA_real_,
    overs_1 = 120, overs_2 = 65, overs_3 = NA_real_,
    venue_avg = 340, venue_result_rate = 0.63
  )
  f <- .test_wp_features(dt)
  expect_equal(f$follow_on_possible, 0L)

  # From innings 3 the deficit is history and counts.
  dt3 <- data.table::copy(dt)[, `:=`(innings = 3L)]
  expect_equal(.test_wp_features(dt3)$follow_on_possible, 1L)
})

test_that("early-innings projection is clamped, not exploded", {
  # 10 runs off the first ball must not project 5,400.
  dt <- data.table::data.table(
    innings = 1L, over_number = 1, ball_number = 1, overs_frac = 1 / 6,
    score = 10L, wickets = 0L,
    runs_1 = NA_real_, runs_2 = NA_real_, runs_3 = NA_real_,
    wkts_1 = NA_real_, wkts_2 = NA_real_, wkts_3 = NA_real_,
    overs_1 = NA_real_, overs_2 = NA_real_, overs_3 = NA_real_,
    venue_avg = 340, venue_result_rate = 0.63
  )
  f <- .test_wp_features(dt)
  expect_equal(f$projected_innings_total, 10 * 90 / 1)
  # and the first over carries no run rate, as training computed it
  expect_equal(f$current_run_rate, 0)
})
