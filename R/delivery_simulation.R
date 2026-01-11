# Delivery Simulation (Ball-by-Ball)
#
# Functions for simulating cricket matches ball-by-ball using the full model.
# The full model predicts 7-class outcome probabilities, which are used to
# either draw categorical outcomes or calculate expected values.
#
# Two simulation modes:
#   - categorical: Draw from P(wicket), P(0), P(1), ..., P(6) distribution
#   - expected: Use E[runs] = sum(P(i) * runs(i)) directly (faster)


#' Simulate Single Delivery
#'
#' Simulates a single delivery outcome using the full prediction model.
#'
#' @param model XGBoost model from load_full_model()
#' @param match_state List containing current match state:
#'   - format: "t20", "odi", or "test"
#'   - innings: 1 or 2
#'   - over: current over (0-indexed)
#'   - ball: ball within over (1-6)
#'   - wickets_fallen: wickets lost so far
#'   - runs_scored: runs scored so far in innings
#'   - target: target to chase (innings 2 only, NULL for innings 1)
#'   - gender: "male" or "female"
#'   - is_knockout: 0 or 1
#'   - event_tier: 1, 2, or 3
#' @param player_skills List with batter and bowler skills:
#'   - batter_scoring_index, batter_survival_rate, batter_balls_faced
#'   - bowler_economy_index, bowler_strike_rate, bowler_balls_bowled
#' @param team_skills List with team skills:
#'   - batting_team_runs_skill, batting_team_wicket_skill
#'   - bowling_team_runs_skill, bowling_team_wicket_skill
#' @param venue_skills List with venue skills:
#'   - venue_run_rate, venue_wicket_rate, venue_boundary_rate, venue_dot_rate
#' @param mode Character. "categorical" (draw outcome) or "expected" (use expected runs)
#'
#' @return List with outcome details:
#'   - runs: runs scored (0-6, or fractional if mode = "expected")
#'   - is_wicket: TRUE if wicket fell
#'   - probs: 7-class probability vector (if categorical mode)
#'
#' @export
simulate_delivery <- function(model, match_state, player_skills, team_skills,
                               venue_skills, mode = c("categorical", "expected")) {
  mode <- match.arg(mode)
  format <- match_state$format

  # Build delivery data frame for prediction
  delivery_data <- data.frame(
    # Context features
    innings = match_state$innings,
    over = match_state$over,
    ball = match_state$ball,
    over_ball = match_state$over + match_state$ball / 6,
    wickets_fallen = match_state$wickets_fallen,
    runs_difference = calculate_runs_diff(match_state),
    gender = match_state$gender %||% "male",
    is_knockout = match_state$is_knockout %||% 0,
    event_tier = match_state$event_tier %||% 2,

    # Player skills
    batter_scoring_index = player_skills$batter_scoring_index %||% 1.25,
    batter_survival_rate = player_skills$batter_survival_rate %||% 0.975,
    bowler_economy_index = player_skills$bowler_economy_index %||% 1.25,
    bowler_strike_rate = player_skills$bowler_strike_rate %||% 0.025,
    batter_balls_faced = player_skills$batter_balls_faced %||% 0,
    bowler_balls_bowled = player_skills$bowler_balls_bowled %||% 0,

    # Team skills
    batting_team_runs_skill = team_skills$batting_team_runs_skill %||% 0,
    batting_team_wicket_skill = team_skills$batting_team_wicket_skill %||% 0,
    bowling_team_runs_skill = team_skills$bowling_team_runs_skill %||% 0,
    bowling_team_wicket_skill = team_skills$bowling_team_wicket_skill %||% 0,

    # Venue skills
    venue_run_rate = venue_skills$venue_run_rate %||% 0,
    venue_wicket_rate = venue_skills$venue_wicket_rate %||% 0,
    venue_boundary_rate = venue_skills$venue_boundary_rate %||% 0.15,
    venue_dot_rate = venue_skills$venue_dot_rate %||% 0.35,

    stringsAsFactors = FALSE
  )

  # Get probability predictions
  probs <- predict_full_outcome(model, delivery_data, format)

  # Ensure probs is a vector
  if (is.matrix(probs)) {
    probs <- probs[1, ]
  }

  # Outcome: [1]=wicket, [2]=0runs, [3]=1run, [4]=2runs, [5]=3runs, [6]=4runs, [7]=6runs
  if (mode == "categorical") {
    # Draw from categorical distribution
    outcome_idx <- sample(1:7, 1, prob = probs)

    is_wicket <- outcome_idx == 1
    runs <- switch(outcome_idx,
      0L,  # wicket - 0 runs
      0L,  # dot ball
      1L,  # single
      2L,  # double
      3L,  # three
      4L,  # boundary
      6L   # six
    )

    list(runs = runs, is_wicket = is_wicket, probs = probs)

  } else {
    # Expected value mode
    exp_runs <- sum(probs * c(0, 0, 1, 2, 3, 4, 6))
    exp_wicket <- probs[1]

    list(runs = exp_runs, is_wicket = exp_wicket > 0.5, exp_wicket = exp_wicket, probs = probs)
  }
}


#' Simulate Innings
#'
#' Simulates a complete innings ball-by-ball.
#'
#' @param model XGBoost model from load_full_model()
#' @param format Character. Format: "t20", "odi", or "test"
#' @param innings Integer. Innings number (1 or 2)
#' @param target Integer. Target to chase (innings 2 only, NULL for innings 1)
#' @param batting_team_skills List. Team-level skills for batting team
#' @param bowling_team_skills List. Team-level skills for bowling team
#' @param venue_skills List. Venue skills
#' @param batters List. List of batter skill objects (at least 11)
#' @param bowlers List. List of bowler skill objects (at least 5-6)
#' @param mode Character. "categorical" or "expected"
#' @param gender Character. "male" or "female"
#' @param is_knockout Integer. 0 or 1
#' @param event_tier Integer. 1, 2, or 3
#'
#' @return List with innings summary:
#'   - total_runs: final score
#'   - wickets_lost: wickets that fell
#'   - overs_faced: overs used
#'   - ball_by_ball: data frame of each delivery
#'   - result: "completed", "all_out", or "target_reached"
#'
#' @export
simulate_innings <- function(model, format = "t20", innings = 1, target = NULL,
                              batting_team_skills, bowling_team_skills, venue_skills,
                              batters, bowlers, mode = "categorical",
                              gender = "male", is_knockout = 0, event_tier = 2) {

  # Determine max overs
  max_overs <- switch(format, "t20" = 20, "odi" = 50, "test" = NULL)
  max_balls <- if (!is.null(max_overs)) max_overs * 6 else 999999  # Test effectively unlimited

  # Initialize state
  runs <- 0L
  wickets <- 0L
  balls <- 0L
  current_batter_idx <- 1
  current_bowler_idx <- 1
  deliveries <- list()

  # Track batter/bowler balls faced/bowled
  batter_balls <- rep(0L, length(batters))
  bowler_balls <- rep(0L, length(bowlers))

  # Main simulation loop
  while (balls < max_balls && wickets < 10) {
    over <- balls %/% 6
    ball <- (balls %% 6) + 1

    # Get current batter/bowler skills
    current_batter <- batters[[current_batter_idx]]
    current_batter$batter_balls_faced <- batter_balls[current_batter_idx]

    current_bowler <- bowlers[[current_bowler_idx]]
    current_bowler$bowler_balls_bowled <- bowler_balls[current_bowler_idx]

    # Build match state
    match_state <- list(
      format = format,
      innings = innings,
      over = over,
      ball = ball,
      wickets_fallen = wickets,
      runs_scored = runs,
      target = target,
      gender = gender,
      is_knockout = is_knockout,
      event_tier = event_tier
    )

    # Combine team skills
    team_skills <- list(
      batting_team_runs_skill = batting_team_skills$runs_skill %||% 0,
      batting_team_wicket_skill = batting_team_skills$wicket_skill %||% 0,
      bowling_team_runs_skill = bowling_team_skills$runs_skill %||% 0,
      bowling_team_wicket_skill = bowling_team_skills$wicket_skill %||% 0
    )

    # Simulate delivery
    result <- simulate_delivery(model, match_state, current_batter, team_skills,
                                 venue_skills, mode)

    # Update state
    runs <- runs + as.integer(result$runs)
    balls <- balls + 1
    batter_balls[current_batter_idx] <- batter_balls[current_batter_idx] + 1
    bowler_balls[current_bowler_idx] <- bowler_balls[current_bowler_idx] + 1

    if (result$is_wicket) {
      wickets <- wickets + 1
      current_batter_idx <- min(current_batter_idx + 1, length(batters))
    }

    # Track delivery
    deliveries[[length(deliveries) + 1]] <- data.frame(
      over = over,
      ball = ball,
      runs = result$runs,
      is_wicket = result$is_wicket,
      total_runs = runs,
      wickets = wickets,
      stringsAsFactors = FALSE
    )

    # Check if target reached (innings 2)
    if (!is.null(target) && runs >= target) {
      break
    }

    # Rotate bowler every over
    if (ball == 6) {
      current_bowler_idx <- ((current_bowler_idx) %% length(bowlers)) + 1
    }
  }

  # Determine result
  result_type <- if (!is.null(target) && runs >= target) {
    "target_reached"
  } else if (wickets >= 10) {
    "all_out"
  } else {
    "completed"
  }

  list(
    total_runs = runs,
    wickets_lost = wickets,
    balls_faced = balls,
    overs_faced = balls %/% 6 + (balls %% 6) / 10,
    ball_by_ball = do.call(rbind, deliveries),
    result = result_type
  )
}


#' Simulate Match Ball-by-Ball
#'
#' Simulates a complete match ball-by-ball using the full model.
#'
#' @param model XGBoost model from load_full_model()
#' @param format Character. Format: "t20", "odi"
#' @param team1_batters List. Batter skill objects for team 1 (11 batters)
#' @param team1_bowlers List. Bowler skill objects for team 1 (5+ bowlers)
#' @param team2_batters List. Batter skill objects for team 2 (11 batters)
#' @param team2_bowlers List. Bowler skill objects for team 2 (5+ bowlers)
#' @param team1_skills List. Team-level skills for team 1
#' @param team2_skills List. Team-level skills for team 2
#' @param venue_skills List. Venue skills
#' @param mode Character. "categorical" or "expected"
#' @param gender Character. "male" or "female"
#' @param is_knockout Integer. 0 or 1
#' @param event_tier Integer. 1, 2, or 3
#'
#' @return List with match summary:
#'   - team1_score, team1_wickets, team1_overs
#'   - team2_score, team2_wickets, team2_overs
#'   - winner: "team1", "team2", or "tie"
#'   - margin: description of margin
#'   - innings1, innings2: detailed ball-by-ball results
#'
#' @export
simulate_match_ballbyball <- function(model, format = "t20",
                                       team1_batters, team1_bowlers,
                                       team2_batters, team2_bowlers,
                                       team1_skills, team2_skills,
                                       venue_skills,
                                       mode = "categorical",
                                       gender = "male", is_knockout = 0,
                                       event_tier = 2) {

  # Simulate first innings: Team 1 bats, Team 2 bowls
  innings1 <- simulate_innings(
    model = model,
    format = format,
    innings = 1,
    target = NULL,
    batting_team_skills = team1_skills,
    bowling_team_skills = team2_skills,
    venue_skills = venue_skills,
    batters = team1_batters,
    bowlers = team2_bowlers,
    mode = mode,
    gender = gender,
    is_knockout = is_knockout,
    event_tier = event_tier
  )

  # Target for second innings
  target <- innings1$total_runs + 1

  # Simulate second innings: Team 2 bats, Team 1 bowls
  innings2 <- simulate_innings(
    model = model,
    format = format,
    innings = 2,
    target = target,
    batting_team_skills = team2_skills,
    bowling_team_skills = team1_skills,
    venue_skills = venue_skills,
    batters = team2_batters,
    bowlers = team1_bowlers,
    mode = mode,
    gender = gender,
    is_knockout = is_knockout,
    event_tier = event_tier
  )

  # Determine winner
  if (innings2$total_runs >= target) {
    winner <- "team2"
    wickets_remaining <- 10 - innings2$wickets_lost
    margin <- paste(wickets_remaining, "wickets")
  } else if (innings2$total_runs < innings1$total_runs) {
    winner <- "team1"
    runs_diff <- innings1$total_runs - innings2$total_runs
    margin <- paste(runs_diff, "runs")
  } else {
    winner <- "tie"
    margin <- "tie"
  }

  list(
    team1_score = innings1$total_runs,
    team1_wickets = innings1$wickets_lost,
    team1_overs = innings1$overs_faced,
    team2_score = innings2$total_runs,
    team2_wickets = innings2$wickets_lost,
    team2_overs = innings2$overs_faced,
    winner = winner,
    margin = margin,
    innings1 = innings1,
    innings2 = innings2
  )
}


#' Calculate Runs Difference for Match State
#'
#' Internal helper to calculate runs_difference for model prediction.
#'
#' @param match_state List. Current match state
#'
#' @return Numeric. Runs difference (batting team - bowling team)
#' @keywords internal
calculate_runs_diff <- function(match_state) {
  if (match_state$innings == 1) {
    # First innings: runs scored so far (no opponent score yet)
    match_state$runs_scored %||% 0
  } else {
    # Second innings: current score minus target
    current <- match_state$runs_scored %||% 0
    target <- match_state$target %||% 0
    current - target
  }
}


#' Create Default Batter Skills
#'
#' Creates a list of default batter skill objects for simulation.
#'
#' @param n Integer. Number of batters to create (default 11)
#' @param format Character. Format for default values
#'
#' @return List of batter skill objects
#' @keywords internal
create_default_batters <- function(n = 11, format = "t20") {
  start_vals <- get_skill_start_values(format)

  lapply(seq_len(n), function(i) {
    # Top order batters slightly better, tail-enders worse
    skill_adj <- if (i <= 3) 0.1 else if (i <= 6) 0 else -0.1

    list(
      batter_scoring_index = start_vals$runs_per_ball + skill_adj,
      batter_survival_rate = start_vals$survival_rate + skill_adj * 0.01,
      batter_balls_faced = 0
    )
  })
}


#' Create Default Bowler Skills
#'
#' Creates a list of default bowler skill objects for simulation.
#'
#' @param n Integer. Number of bowlers to create (default 6)
#' @param format Character. Format for default values
#'
#' @return List of bowler skill objects
#' @keywords internal
create_default_bowlers <- function(n = 6, format = "t20") {
  start_vals <- get_skill_start_values(format)

  lapply(seq_len(n), function(i) {
    # First choice bowlers slightly better
    skill_adj <- if (i <= 3) -0.05 else 0.05

    list(
      bowler_economy_index = start_vals$runs_per_ball + skill_adj,
      bowler_strike_rate = (1 - start_vals$survival_rate) - skill_adj * 0.005,
      bowler_balls_bowled = 0
    )
  })
}


#' Quick Match Simulation
#'
#' Simulates a match using default player skills (useful for testing).
#'
#' @param model XGBoost model from load_full_model()
#' @param format Character. Format: "t20", "odi"
#' @param team1_skill Numeric. Overall skill adjustment for team 1 (-1 to 1)
#' @param team2_skill Numeric. Overall skill adjustment for team 2 (-1 to 1)
#' @param mode Character. "categorical" or "expected"
#'
#' @return Match result from simulate_match_ballbyball()
#' @export
quick_match_simulation <- function(model, format = "t20",
                                    team1_skill = 0, team2_skill = 0,
                                    mode = "categorical") {

  # Create batters with skill adjustments
  team1_batters <- create_default_batters(11, format)
  team2_batters <- create_default_batters(11, format)

  # Apply skill adjustments
  for (i in seq_along(team1_batters)) {
    team1_batters[[i]]$batter_scoring_index <-
      team1_batters[[i]]$batter_scoring_index + team1_skill * 0.2
  }
  for (i in seq_along(team2_batters)) {
    team2_batters[[i]]$batter_scoring_index <-
      team2_batters[[i]]$batter_scoring_index + team2_skill * 0.2
  }

  # Create bowlers
  team1_bowlers <- create_default_bowlers(6, format)
  team2_bowlers <- create_default_bowlers(6, format)

  # Team skills (based on adjustment)
  team1_skills <- list(runs_skill = team1_skill * 0.1, wicket_skill = 0)
  team2_skills <- list(runs_skill = team2_skill * 0.1, wicket_skill = 0)

  # Default venue skills
  venue_skills <- list(
    venue_run_rate = 0,
    venue_wicket_rate = 0,
    venue_boundary_rate = 0.15,
    venue_dot_rate = 0.35
  )

  simulate_match_ballbyball(
    model = model,
    format = format,
    team1_batters = team1_batters,
    team1_bowlers = team1_bowlers,
    team2_batters = team2_batters,
    team2_bowlers = team2_bowlers,
    team1_skills = team1_skills,
    team2_skills = team2_skills,
    venue_skills = venue_skills,
    mode = mode
  )
}
