# calculate_unified_margin() had NO test file before this one.
#
# Its docstring's sign convention was wrong until 2026-08-21: it said
# "Positive = team1 won", but measured on 17,636 decided matches the sign
# actually follows "the side BATTING FIRST won" (98.9% agreement, vs 86.4%
# for team1). The two readings agree 87.2% of the time -- because team1 is
# usually the batting-first side -- which is exactly why the wrong reading
# survived undetected. The 13% disagreement tracks the toss, so anything
# fitted against this column while reading the sign as team1-relative
# absorbs a toss-shaped error (bouncerverse#63, same shape as #30).
#
# These tests pin the ACTUAL convention: team1_score is contractually the
# BATTING-FIRST side's score (see the function's own docstring), regardless
# of which team is labelled "team1" in the match schema.

test_that("a runs win by the batting-first side gives a positive margin", {
  # team1_score is, by contract, the batting-first (or only) innings.
  margin <- calculate_unified_margin(
    team1_score = 180, team2_score = 150,
    win_type = "runs", format = "t20"
  )
  expect_equal(margin, 30)
  expect_gt(margin, 0)
})

test_that("a wickets win by the chasing side gives a negative margin", {
  # Won with balls to spare so the chasing side is genuinely credited with
  # more than the raw scoreboard gap (see the projection test below).
  margin <- calculate_unified_margin(
    team1_score = 150, team2_score = 152,
    wickets_remaining = 4, overs_remaining = 6,
    win_type = "wickets", format = "t20"
  )
  expect_lt(margin, 0)
})

test_that("tie, draw, and no-result all return exactly 0", {
  expect_equal(calculate_unified_margin(100, 100, win_type = "tie"), 0)
  expect_equal(calculate_unified_margin(250, 180, win_type = "draw", format = "test"), 0)
  expect_equal(calculate_unified_margin(150, 120, win_type = "no result"), 0)
  expect_equal(calculate_unified_margin(150, 120, win_type = "no_result"), 0)
})

test_that("a wickets win with no balls to spare equals the raw run difference exactly", {
  # balls_remaining = 0 forces resource_remaining = 0 in limited-overs formats
  # (score_projection.R's calculate_projection_resource()), so the chasing
  # side's projected total collapses to its actual score and the margin is
  # exactly team1_score - team2_score -- a fully deterministic, parameter-
  # independent case that pins the sign without depending on the fitted
  # projection RDS files in bouncerdata/models/.
  margin <- calculate_unified_margin(
    team1_score = 150, team2_score = 152,
    wickets_remaining = 4, overs_remaining = 0,
    win_type = "wickets", format = "t20"
  )
  expect_equal(margin, 150 - 152)
  expect_equal(margin, -2)
})

test_that("wickets-win magnitude reflects wickets/balls remaining, not the raw run difference", {
  # Same scoreline as the previous test, but won with 6 overs (36 balls) to
  # spare instead of 0. The chasing side is now projected to have scored
  # MORE than its actual total, so the margin must be more negative than the
  # raw scoreline gap -- a test that would still pass if the function just
  # returned team1_score - team2_score is worthless here.
  raw_diff <- 150 - 152

  margin_with_balls_to_spare <- calculate_unified_margin(
    team1_score = 150, team2_score = 152,
    wickets_remaining = 4, overs_remaining = 6,
    win_type = "wickets", format = "t20"
  )

  expect_false(isTRUE(all.equal(margin_with_balls_to_spare, raw_diff)))
  expect_lt(margin_with_balls_to_spare, raw_diff)

  # Pin the exact value against the real projection composition: wickets
  # fallen = 10 - wickets_remaining, overs bowled = max_balls - balls
  # remaining. Getting either conversion wrong inside calculate_unified_margin
  # would desync this from what calculate_projected_score() itself returns.
  projected_team2 <- calculate_projected_score(
    current_score = 152, wickets = 6, overs = 14.0, format = "t20"
  )
  expect_equal(margin_with_balls_to_spare, 150 - projected_team2)
})

test_that("more wickets in hand makes the chasing side's margin more negative", {
  # Holding balls remaining fixed, more wickets in hand -> larger resource
  # remaining -> larger projected total -> a more negative margin. This
  # is the "reflects wickets remaining" half of the magnitude claim, checked
  # as a direction rather than an exact value so it survives future retrains
  # of the projection parameters.
  margin_few_wickets <- calculate_unified_margin(
    team1_score = 150, team2_score = 152,
    wickets_remaining = 2, overs_remaining = 6,
    win_type = "wickets", format = "t20"
  )
  margin_many_wickets <- calculate_unified_margin(
    team1_score = 150, team2_score = 152,
    wickets_remaining = 8, overs_remaining = 6,
    win_type = "wickets", format = "t20"
  )
  expect_lt(margin_many_wickets, margin_few_wickets)
})

test_that("sign follows batting order, not which team is labelled team1", {
  # This is the exact divergence the wrong docstring produced. Schema's
  # "team1" is India, who bowled first and WON by chasing. Schema's "team2"
  # is Australia, who batted first and lost. calculate_unified_margin's
  # contract requires the BATTING-FIRST score in team1_score regardless of
  # match-schema labels, so the correct call plugs Australia's score into
  # team1_score and India's into team2_score.
  australia_batted_first_and_lost <- 165
  india_chased_and_won <- 168

  margin <- calculate_unified_margin(
    team1_score = australia_batted_first_and_lost,
    team2_score = india_chased_and_won,
    wickets_remaining = 5, overs_remaining = 3,
    win_type = "wickets", format = "t20"
  )

  # Actual convention: negative, because the batting-first side (Australia)
  # lost. Under the OLD docstring's "positive = team1 won" reading, this
  # would be expected POSITIVE, since schema's team1 (India) is the side
  # that actually won the match. It is not -- the sign tracks batting order,
  # not the team1 label.
  expect_lt(margin, 0)
})

test_that("super over margin is subtracted, preserving whichever side it favours", {
  margin_no_super_over <- calculate_unified_margin(
    team1_score = 150, team2_score = 152,
    wickets_remaining = 4, overs_remaining = 0,
    win_type = "wickets", format = "t20"
  )
  margin_with_super_over <- calculate_unified_margin(
    team1_score = 150, team2_score = 152,
    wickets_remaining = 4, overs_remaining = 0,
    win_type = "wickets", format = "t20",
    super_over_margin = 5
  )
  expect_equal(margin_with_super_over, margin_no_super_over - 5)
})

test_that("an invalid runs win (team1 did not actually score more) warns", {
  expect_warning(
    calculate_unified_margin(100, 120, win_type = "runs"),
    "team1_score"
  )
})

test_that("an unknown win_type warns and returns 0", {
  expect_warning(
    margin <- calculate_unified_margin(100, 90, win_type = "bogus"),
    "Unknown win_type"
  )
  expect_equal(margin, 0)
})

test_that("overs_to_balls converts cricket notation, not true decimals", {
  expect_equal(overs_to_balls(18.4), 112L)
  expect_equal(overs_to_balls(0), 0L)
  expect_equal(overs_to_balls(20.0), 120L)
})

test_that("balls_to_overs_cricket round-trips overs_to_balls", {
  expect_equal(balls_to_overs_cricket(82), 13.4)
  expect_equal(balls_to_overs_cricket(84), 14.0)
  expect_equal(balls_to_overs_cricket(120), 20.0)
})
