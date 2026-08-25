# Illegal deliveries in the simulator (#81/D-P50 stage 6): a wide is one of
# the model's own trained OUTCOME_CATEGORIES; a no-ball is NOT (its
# batter-runs distribution mirrors a legal ball's, so the trainer never gave
# it a distinguishing feature -- see NO_BALL_RATE_* in R/constants_skill.R).
# simulate_delivery() draws no-ball occurrence independently of the model.
#
# `stats::runif()` is namespace-qualified in R/simulation.R, so
# local_mocked_bindings(runif = ..., .package = "stats") intercepts it --
# confirmed working before writing these tests.

wide_only_probs <- c(0, 0, 0, 0, 0, 0.05, 0.05, 0.9)   # wicket,0,1,2,3,4,6,wide
noball_target_probs <- c(0, 0, 0, 0, 0, 1, 0, 0)        # deterministic "4"
zero_wide_probs <- c(0.05, 0.35, 0.35, 0.1, 0.02, 0.1, 0.03, 0)  # sums to 1, no wide mass

fixed_state <- function() {
  list(format = "t20", innings = 1, over = 5, ball = 3,
       wickets_fallen = 1, runs_scored = 42)
}
fixed_player <- function() {
  list(batter_scoring_index = 1.25, batter_survival_rate = 0.975,
       bowler_economy_index = 1.25, bowler_strike_rate = 0.025)
}
fixed_team <- function() {
  list(batting_team_runs_skill = 0, batting_team_wicket_skill = 0,
       bowling_team_runs_skill = 0, bowling_team_wicket_skill = 0)
}
fixed_venue <- function() {
  list(venue_run_rate = 0, venue_wicket_rate = 0,
       venue_boundary_rate = 0.15, venue_dot_rate = 0.35)
}

test_that("a drawn WIDE (no-ball not firing) is illegal, wicket-free, no free hit", {
  local_mocked_bindings(
    predict_full_outcome = function(model, delivery_data, format) wide_only_probs,
    .package = "bouncer"
  )
  # no_ball_rate is ~0.5%; forcing runif high guarantees is_noball = FALSE.
  local_mocked_bindings(runif = function(...) 0.99, .package = "stats")

  set.seed(1)
  draws <- replicate(500, simulate_delivery(
    NULL, fixed_state(), fixed_player(), fixed_team(), fixed_venue(),
    mode = "categorical"
  ), simplify = FALSE)

  illegal <- Filter(function(d) d$is_illegal, draws)
  expect_gt(length(illegal), 0)   # wide_only_probs gives it ~90% + 5%/5% mass
  expect_true(all(vapply(illegal, function(d) !d$sets_free_hit, logical(1))))
  expect_true(all(vapply(illegal, function(d) !d$is_wicket, logical(1))))
})

test_that("no-ball firing (model draws a non-wide category) adds the penalty run and sets free hit", {
  local_mocked_bindings(
    predict_full_outcome = function(model, delivery_data, format) noball_target_probs,
    .package = "bouncer"
  )
  # Call 1: is_noball check, forced TRUE (below any positive rate). Call 2:
  # the independent no-ball-wicket check, forced FALSE (above any positive
  # rate) -- a no-ball's wicket occurrence is NOT the drawn category's
  # is_wicket (see the dedicated test below for why).
  call_n <- 0L
  local_mocked_bindings(runif = function(...) {
    call_n <<- call_n + 1L
    if (call_n == 1L) 0 else 0.99
  }, .package = "stats")

  result <- simulate_delivery(NULL, fixed_state(), fixed_player(), fixed_team(),
                               fixed_venue(), mode = "categorical")

  expect_true(result$is_illegal)
  expect_true(result$sets_free_hit)
  expect_equal(result$runs, 4 + 1)   # "4" category's run value, plus the no-ball penalty
  expect_false(result$is_wicket)
})

test_that("no-ball wicket occurrence uses the measured run-out-only rate, not the model's draw", {
  # The drawn category IS "wicket" (all probability mass there), which would
  # give is_wicket = TRUE if the no-ball branch reused it directly -- but
  # only a run-out is legal on a no-ball, and the model's unconditional
  # P(wicket) overstates that by 9-45x (NO_BALL_WICKET_RATE_* measurement).
  # Forcing the independent wicket draw's runif call above the (tiny)
  # no-ball-wicket rate must produce is_wicket = FALSE despite the model
  # having drawn "wicket".
  wicket_only_probs <- c(1, 0, 0, 0, 0, 0, 0, 0)
  local_mocked_bindings(
    predict_full_outcome = function(model, delivery_data, format) wicket_only_probs,
    .package = "bouncer"
  )
  call_n <- 0L
  local_mocked_bindings(runif = function(...) {
    call_n <<- call_n + 1L
    if (call_n == 1L) 0 else 0.99   # call 1: is_noball fires; call 2: no-ball-wicket does not
  }, .package = "stats")

  result <- simulate_delivery(NULL, fixed_state(), fixed_player(), fixed_team(),
                               fixed_venue(), mode = "categorical")

  expect_true(result$is_illegal)
  expect_true(result$sets_free_hit)
  expect_false(result$is_wicket)   # NOT the model's drawn "wicket" category

  # And forcing the second runif call BELOW the no-ball-wicket rate must
  # produce is_wicket = TRUE, confirming the independent draw is actually
  # wired in (not just always FALSE).
  call_n <- 0L
  local_mocked_bindings(runif = function(...) {
    call_n <<- call_n + 1L
    0   # both calls fire
  }, .package = "stats")
  result2 <- simulate_delivery(NULL, fixed_state(), fixed_player(), fixed_team(),
                                fixed_venue(), mode = "categorical")
  expect_true(result2$is_wicket)
})

test_that("a no-ball/wide collision resolves as a no-ball, never as a wide", {
  # Wide gets almost all the mass; the model would draw wide on its own, but
  # the independent no-ball draw (forced to always fire) must win. wicket
  # gets zero mass throughout (including after the wide-excluding redraw),
  # so is_wicket would be FALSE via the drawn category too -- but each
  # delivery also makes a second runif call for the independent no-ball
  # wicket draw, forced above the (tiny) no-ball-wicket rate here so that
  # call doesn't itself introduce a wicket and confound this test's focus
  # (collision resolution, not the wicket-rate fix covered by its own test
  # above). Odd calls = is_noball check (always fires); even calls = the
  # no-ball-wicket check (never fires).
  local_mocked_bindings(
    predict_full_outcome = function(model, delivery_data, format) wide_only_probs,
    .package = "bouncer"
  )
  call_n <- 0L
  local_mocked_bindings(runif = function(...) {
    call_n <<- call_n + 1L
    if (call_n %% 2L == 1L) 0 else 0.99
  }, .package = "stats")

  set.seed(2)
  draws <- replicate(300, simulate_delivery(
    NULL, fixed_state(), fixed_player(), fixed_team(), fixed_venue(),
    mode = "categorical"
  ), simplify = FALSE)

  # Every draw is illegal (no-ball always fires) and every one sets a free
  # hit (never resolves as a bare wide, which never sets one).
  expect_true(all(vapply(draws, function(d) d$is_illegal, logical(1))))
  expect_true(all(vapply(draws, function(d) d$sets_free_hit, logical(1))))
  # The redraw excludes wide's category, so runs is always a non-wide value
  # (0-6) plus the no-ball penalty -- never wide's own 1.217 run value.
  runs_seen <- vapply(draws, function(d) d$runs, numeric(1))
  expect_true(all(runs_seen != 1.217))
  expect_true(all(vapply(draws, function(d) !d$is_wicket, logical(1))))
})

test_that("no illegal delivery occurs when the model has zero wide mass and no-ball never fires", {
  local_mocked_bindings(
    predict_full_outcome = function(model, delivery_data, format) zero_wide_probs,
    .package = "bouncer"
  )
  local_mocked_bindings(runif = function(...) 0.99, .package = "stats")

  set.seed(3)
  draws <- replicate(200, simulate_delivery(
    NULL, fixed_state(), fixed_player(), fixed_team(), fixed_venue(),
    mode = "categorical"
  ), simplify = FALSE)

  expect_true(all(vapply(draws, function(d) !d$is_illegal, logical(1))))
  expect_true(all(vapply(draws, function(d) !d$sets_free_hit, logical(1))))
})

test_that("expected mode mirrors categorical mode's illegal-delivery handling", {
  local_mocked_bindings(
    predict_full_outcome = function(model, delivery_data, format) noball_target_probs,
    .package = "bouncer"
  )
  local_mocked_bindings(runif = function(...) 0, .package = "stats")  # noball fires; wicket draw uses same stream

  result <- simulate_delivery(NULL, fixed_state(), fixed_player(), fixed_team(),
                               fixed_venue(), mode = "expected")

  expect_true(result$is_illegal)
  expect_true(result$sets_free_hit)
  # Conditional (non-wide) expected runs for noball_target_probs is exactly
  # 4 (all mass on "4"), plus the no-ball penalty run.
  expect_equal(result$runs, 4 + 1)
})

test_that("expected mode: a wide draw (via P(wide), no-ball not firing) never blends into every ball's runs", {
  local_mocked_bindings(
    predict_full_outcome = function(model, delivery_data, format) wide_only_probs,
    .package = "bouncer"
  )
  # First runif call decides is_noball (forced high = FALSE); the second
  # decides the wide Bernoulli draw (forced low = fires, since p_wide=0.9).
  calls <- 0L
  local_mocked_bindings(runif = function(...) {
    calls <<- calls + 1L
    if (calls == 1L) 0.99 else 0.01
  }, .package = "stats")

  result <- simulate_delivery(NULL, fixed_state(), fixed_player(), fixed_team(),
                               fixed_venue(), mode = "expected")

  expect_true(result$is_illegal)
  expect_false(result$sets_free_hit)
  expect_equal(result$runs, 1.217)   # OUTCOME_RUN_VALUES's wide value, deterministic
  expect_false(result$is_wicket)
})

test_that("an old 7-column model (no wide category) can still draw a no-ball, never a wide", {
  seven_col_probs <- c(0.05, 0.35, 0.35, 0.1, 0.02, 0.1, 0.03)  # no 8th (wide) column
  local_mocked_bindings(
    predict_full_outcome = function(model, delivery_data, format) seven_col_probs,
    .package = "bouncer"
  )
  local_mocked_bindings(runif = function(...) 0, .package = "stats")  # always a no-ball

  result <- simulate_delivery(NULL, fixed_state(), fixed_player(), fixed_team(),
                               fixed_venue(), mode = "categorical")

  expect_true(result$is_illegal)
  expect_true(result$sets_free_hit)
})

# simulate_innings() integration --------------------------------------------

test_that("an illegal delivery does not advance balls/over, and a legal one after it does", {
  # First call: forced no-ball (runif low). All subsequent calls: forced
  # legal (runif high). One-shot counter avoids ever forcing every ball
  # illegal, which would loop forever against MAX_CONSECUTIVE_ILLEGAL.
  call_n <- 0L
  local_mocked_bindings(runif = function(...) {
    call_n <<- call_n + 1L
    if (call_n == 1L) 0 else 0.99
  }, .package = "stats")
  local_mocked_bindings(
    predict_full_outcome = function(model, delivery_data, format) noball_target_probs,
    .package = "bouncer"
  )

  batters <- create_default_batters(11, "t20")
  bowlers <- create_default_bowlers(6, "t20")
  bat_skills <- list(runs_skill = 0, wicket_skill = 0)
  bowl_skills <- list(runs_skill = 0, wicket_skill = 0)
  venue <- fixed_venue()

  set.seed(4)
  result <- simulate_innings(
    model = NULL, format = "t20", innings = 1, target = NULL,
    batting_team_skills = bat_skills, bowling_team_skills = bowl_skills,
    venue_skills = venue, batters = batters, bowlers = bowlers,
    mode = "categorical", max_overs_override = 1
  )

  bb <- result$ball_by_ball
  expect_true(bb$is_illegal[1])
  expect_false(bb$is_illegal[2])
  # The illegal retry and the legal ball that follows occupy the SAME
  # over/ball slot -- balls does not advance for the illegal one.
  expect_equal(bb$over[1], bb$over[2])
  expect_equal(bb$ball[1], bb$ball[2])
  # balls_faced (legal balls only) reflects 6 legal deliveries for a 1-over
  # innings that completed normally, not 7 (the illegal retry doesn't count).
  expect_equal(result$balls_faced, 6L)
  expect_equal(nrow(bb), 7L)   # 6 legal + 1 illegal retry
})

test_that("a pending free hit propagates into the next legal delivery's feature vector", {
  call_n <- 0L
  local_mocked_bindings(runif = function(...) {
    call_n <<- call_n + 1L
    if (call_n == 1L) 0 else 0.99
  }, .package = "stats")

  captured <- list()
  local_mocked_bindings(
    predict_full_outcome = function(model, delivery_data, format) {
      captured[[length(captured) + 1L]] <<- delivery_data$is_free_hit
      noball_target_probs
    },
    .package = "bouncer"
  )

  batters <- create_default_batters(11, "t20")
  bowlers <- create_default_bowlers(6, "t20")
  bat_skills <- list(runs_skill = 0, wicket_skill = 0)
  bowl_skills <- list(runs_skill = 0, wicket_skill = 0)

  set.seed(5)
  simulate_innings(
    model = NULL, format = "t20", innings = 1, target = NULL,
    batting_team_skills = bat_skills, bowling_team_skills = bowl_skills,
    venue_skills = fixed_venue(), batters = batters, bowlers = bowlers,
    mode = "categorical", max_overs_override = 1
  )

  # First delivery (the no-ball itself) is NOT a free hit; the second
  # (the legal ball resolving the over) IS.
  expect_false(captured[[1]])
  expect_true(captured[[2]])
  # Free hit is consumed by the third (already-legal) delivery.
  expect_false(captured[[3]])
})
