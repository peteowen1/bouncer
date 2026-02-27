# Simulation Framework
#
# Core infrastructure for cricket match and season simulations.
# Includes:
#   - Delivery simulation primitives (ball-by-ball simulation using full model)
#   - Configuration, random seed management, result aggregation
#   - Database storage utilities


# Delivery Simulation Primitives ===========================================
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
#' @seealso
#' \code{\link{simulate_innings}} to simulate a full innings,
#' \code{\link{simulate_match_ballbyball}} to simulate a full match,
#' \code{\link{load_full_model}} to load the prediction model
#'
#' @examples
#' \dontrun{
#' model <- load_full_model("shortform")
#' state <- list(format = "t20", innings = 1, over = 5, ball = 3,
#'               wickets_fallen = 1, runs_scored = 42)
#' player <- list(batter_scoring_index = 1.3, batter_survival_rate = 0.97,
#'                bowler_economy_index = 1.2, bowler_strike_rate = 0.03)
#' team <- list(batting_team_runs_skill = 0.05, batting_team_wicket_skill = 0,
#'              bowling_team_runs_skill = -0.02, bowling_team_wicket_skill = 0)
#' venue <- list(venue_run_rate = 0, venue_wicket_rate = 0,
#'               venue_boundary_rate = 0.15, venue_dot_rate = 0.35)
#' result <- simulate_delivery(model, state, player, team, venue)
#' }
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
#' @param max_overs_override Integer. Override max overs (used for super overs).
#'
#' @return List with innings summary:
#'   - total_runs: final score
#'   - wickets_lost: wickets that fell
#'   - overs_faced: overs used
#'   - ball_by_ball: data frame of each delivery
#'   - result: "completed", "all_out", or "target_reached"
#'
#' @seealso
#' \code{\link{simulate_delivery}} for single-ball simulation,
#' \code{\link{simulate_match_ballbyball}} to simulate a full match
#'
#' @examples
#' \dontrun{
#' model <- load_full_model("shortform")
#' batters <- create_default_batters(11, "t20")
#' bowlers <- create_default_bowlers(6, "t20")
#' bat_skills <- list(runs_skill = 0.05, wicket_skill = 0)
#' bowl_skills <- list(runs_skill = -0.02, wicket_skill = 0)
#' venue <- list(venue_run_rate = 0, venue_wicket_rate = 0,
#'               venue_boundary_rate = 0.15, venue_dot_rate = 0.35)
#' result <- simulate_innings(model, format = "t20", innings = 1,
#'                            batting_team_skills = bat_skills,
#'                            bowling_team_skills = bowl_skills,
#'                            venue_skills = venue,
#'                            batters = batters, bowlers = bowlers)
#' }
#'
#' @export
simulate_innings <- function(model, format = "t20", innings = 1, target = NULL,
                              batting_team_skills, bowling_team_skills, venue_skills,
                              batters, bowlers, mode = "categorical",
                              gender = "male", is_knockout = 0, event_tier = 2,
                              max_overs_override = NULL) {

  # Determine max overs (override for super overs, otherwise use format lookup)
  max_overs <- if (!is.null(max_overs_override)) max_overs_override else get_max_overs(format)
  max_balls <- if (!is.null(max_overs)) max_overs * 6 else 999999L  # Test effectively unlimited

  # OPTIMIZED: Pre-allocate vectors for max possible deliveries
  # This avoids expensive list appending in the while loop
  max_deliveries <- if (!is.null(max_overs)) as.integer(max_overs * 6) else 1000L
  result_over <- integer(max_deliveries)
  result_ball <- integer(max_deliveries)
  result_runs <- numeric(max_deliveries)
  result_is_wicket <- logical(max_deliveries)
  result_total_runs <- numeric(max_deliveries)
  result_wickets <- integer(max_deliveries)

  # Initialize state
  runs <- 0
  wickets <- 0L
  balls <- 0L
  delivery_count <- 0L
  current_batter_idx <- 1L
  current_bowler_idx <- 1L

  # Track batter/bowler balls faced/bowled
  batter_balls <- rep(0L, length(batters))
  bowler_balls <- rep(0L, length(bowlers))

  # Pre-compute team skills (avoid repeated %||% in loop)
  team_skills <- list(
    batting_team_runs_skill = batting_team_skills$runs_skill %||% 0,
    batting_team_wicket_skill = batting_team_skills$wicket_skill %||% 0,
    bowling_team_runs_skill = bowling_team_skills$runs_skill %||% 0,
    bowling_team_wicket_skill = bowling_team_skills$wicket_skill %||% 0
  )

  # Main simulation loop
  while (balls < max_balls && wickets < 10L) {
    over <- balls %/% 6L
    ball <- (balls %% 6L) + 1L

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

    # Simulate delivery
    sim_result <- simulate_delivery(model, match_state, current_batter, team_skills,
                                     venue_skills, mode)

    # Update state
    runs <- runs + sim_result$runs
    balls <- balls + 1L
    batter_balls[current_batter_idx] <- batter_balls[current_batter_idx] + 1L
    bowler_balls[current_bowler_idx] <- bowler_balls[current_bowler_idx] + 1L

    if (sim_result$is_wicket) {
      wickets <- wickets + 1L
      current_batter_idx <- min(current_batter_idx + 1L, length(batters))
    }

    # Store result in pre-allocated vectors (no list appending)
    delivery_count <- delivery_count + 1L
    result_over[delivery_count] <- over
    result_ball[delivery_count] <- ball
    result_runs[delivery_count] <- sim_result$runs
    result_is_wicket[delivery_count] <- sim_result$is_wicket
    result_total_runs[delivery_count] <- runs
    result_wickets[delivery_count] <- wickets

    # Check if target reached (innings 2)
    if (!is.null(target) && runs >= target) {
      break
    }

    # Rotate bowler every over
    if (ball == 6L) {
      current_bowler_idx <- ((current_bowler_idx) %% length(bowlers)) + 1L
    }
  }

  # Determine result
  result_type <- if (!is.null(target) && runs >= target) {
    "target_reached"
  } else if (wickets >= 10L) {
    "all_out"
  } else {
    "completed"
  }

  # Create single data.table at end (trimmed to actual size)
  ball_by_ball <- data.table::data.table(
    over = result_over[seq_len(delivery_count)],
    ball = result_ball[seq_len(delivery_count)],
    runs = result_runs[seq_len(delivery_count)],
    is_wicket = result_is_wicket[seq_len(delivery_count)],
    total_runs = result_total_runs[seq_len(delivery_count)],
    wickets = result_wickets[seq_len(delivery_count)]
  )

  list(
    total_runs = runs,
    wickets_lost = wickets,
    balls_faced = balls,
    overs_faced = balls %/% 6L + (balls %% 6L) / 10,
    ball_by_ball = ball_by_ball,
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
#' @seealso
#' \code{\link{simulate_innings}} for single-innings simulation,
#' \code{\link{simulate_delivery}} for ball-by-ball control
#'
#' @examples
#' \dontrun{
#' model <- load_full_model("shortform")
#' result <- quick_match_simulation(model, format = "t20",
#'                                  team1_skill = 0.3, team2_skill = -0.1)
#' result$winner
#' result$margin
#' }
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
  } else if (is_knockout == 1) {
    # Super over tiebreaker for knockout matches
    so_result <- simulate_super_over(
      model = model, format = format,
      team1_skills = team1_skills, team2_skills = team2_skills,
      venue_skills = venue_skills,
      team1_batters = team1_batters, team1_bowlers = team1_bowlers,
      team2_batters = team2_batters, team2_bowlers = team2_bowlers,
      mode = mode, gender = gender, event_tier = event_tier
    )
    winner <- so_result$winner
    margin <- "super over"
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


#' Simulate Super Over Tiebreaker
#'
#' Resolves a tied knockout match with a super over (1 over per side).
#' If still tied after one super over, resolves by coin flip.
#'
#' @param model Model object for predictions
#' @param format Character. Match format
#' @param team1_skills,team2_skills Team skill lists
#' @param venue_skills Venue skill list
#' @param team1_batters,team1_bowlers,team2_batters,team2_bowlers Player vectors
#' @param mode Character. Simulation mode
#' @param gender Character. Gender
#' @param event_tier Integer. Event tier
#' @return List with winner ("team1" or "team2") and scores
#' @keywords internal
simulate_super_over <- function(model, format,
                                team1_skills, team2_skills, venue_skills,
                                team1_batters, team1_bowlers,
                                team2_batters, team2_bowlers,
                                mode, gender, event_tier) {

  # Team1 bats first in super over — 1 over max
  so_inn1 <- simulate_innings(
    model = model, format = format, innings = 1, target = NULL,
    batting_team_skills = team1_skills,
    bowling_team_skills = team2_skills,
    venue_skills = venue_skills,
    batters = team1_batters,
    bowlers = team2_bowlers,
    mode = mode, gender = gender,
    is_knockout = 1, event_tier = event_tier,
    max_overs_override = 1
  )

  # Team2 chases super over target — 1 over max
  so_target <- so_inn1$total_runs + 1
  so_inn2 <- simulate_innings(
    model = model, format = format, innings = 2, target = so_target,
    batting_team_skills = team2_skills,
    bowling_team_skills = team1_skills,
    venue_skills = venue_skills,
    batters = team2_batters,
    bowlers = team1_bowlers,
    mode = mode, gender = gender,
    is_knockout = 1, event_tier = event_tier,
    max_overs_override = 1
  )

  if (so_inn2$total_runs >= so_target) {
    winner <- "team2"
  } else if (so_inn2$total_runs < so_inn1$total_runs) {
    winner <- "team1"
  } else {
    # Still tied after super over — coin flip (boundary count rules are complex)
    winner <- sample(c("team1", "team2"), 1)
  }

  list(
    winner = winner,
    so_team1_score = so_inn1$total_runs,
    so_team2_score = so_inn2$total_runs
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
      batter_scoring_index = start_vals$scoring_index + skill_adj,
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
      bowler_economy_index = start_vals$economy_index + skill_adj,
      bowler_strike_rate = start_vals$strike_rate - skill_adj * 0.005,
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
#'
#' @examples
#' \dontrun{
#' model <- load_full_model("shortform")
#' result <- quick_match_simulation(model, format = "t20")
#' result <- quick_match_simulation(model, format = "odi",
#'                                  team1_skill = 0.5, team2_skill = -0.3)
#' }
#'
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


# Simulation Framework =====================================================
#
# Configuration, random seed management, result aggregation,
# and database storage utilities.

# Configuration ============================================================

#' Create Simulation Configuration
#'
#' Creates a configuration object for simulations.
#'
#' @param simulation_type Character. Type: "match", "season", "playoffs"
#' @param event_name Character. Event to simulate (e.g., "Indian Premier League")
#' @param season Character. Season to simulate (e.g., "2024")
#' @param n_simulations Integer. Number of Monte Carlo iterations (default 10000)
#' @param random_seed Integer. Random seed for reproducibility (default 42)
#' @param use_model Logical. Use trained model vs simple ELO (default TRUE)
#' @param model_version Character. Model version to use (default "v1.0")
#'
#' @return List with simulation configuration
#'
#' @examples
#' create_simulation_config()
#' create_simulation_config(
#'   simulation_type = "match",
#'   event_name = "Big Bash League",
#'   season = "2025",
#'   n_simulations = 100
#' )
#'
#' @export
create_simulation_config <- function(
    simulation_type = "season",
    event_name = "Indian Premier League",
    season = "2024",
    n_simulations = 10000,
    random_seed = 42,
    use_model = TRUE,
    model_version = "v1.0"
) {
  config <- list(
    simulation_id = paste0(
      simulation_type, "_",
      gsub(" ", "_", tolower(event_name)), "_",
      season, "_",
      format(Sys.time(), "%Y%m%d%H%M%S")
    ),
    simulation_type = simulation_type,
    event_name = event_name,
    season = season,
    n_simulations = n_simulations,
    random_seed = random_seed,
    use_model = use_model,
    model_version = model_version,
    created_at = Sys.time()
  )

  return(config)
}


# Random Seed Management ===================================================

#' Set Simulation Seed
#'
#' Sets the random seed for reproducible simulations.
#'
#' @param seed Integer. Random seed
#'
#' @return Invisibly returns the seed
#' @keywords internal
set_simulation_seed <- function(seed) {
  set.seed(seed)
  invisible(seed)
}


#' Get Simulation Seeds
#'
#' Generates a vector of seeds for parallel simulations.
#'
#' @param n Integer. Number of seeds to generate
#' @param base_seed Integer. Base seed (default 42)
#'
#' @return Integer vector of seeds
#' @keywords internal
get_simulation_seeds <- function(n, base_seed = 42) {
  set.seed(base_seed)
  sample.int(.Machine$integer.max, n)
}


# Match Outcome Simulation =================================================

#' Simulate Match Outcome
#'
#' Simulates a single match outcome based on win probabilities.
#' Important: team1 is assumed to bat first. This determines margin type
#' (team batting first wins by runs, team batting second wins by wickets).
#' Callers must ensure team order reflects batting order.
#'
#' @param team1_win_prob Numeric. Probability that team1 wins (0-1)
#' @param team1 Character. Team 1 name (batting first)
#' @param team2 Character. Team 2 name (batting second)
#'
#' @return List with winner, loser, margin, and team1_won flag
#' @keywords internal
simulate_match_outcome <- function(team1_win_prob, team1, team2) {
  # Simulate outcome
  team1_wins <- stats::runif(1) < team1_win_prob

  winner <- if (team1_wins) team1 else team2
  loser <- if (team1_wins) team2 else team1

  # Margin type is deterministic in cricket:
  # Team batting first wins by runs, team batting second wins by wickets.
  # Convention: team1 bats first.
  if (team1_wins) {
    margin <- sample(1:50, 1, prob = stats::dnorm(1:50, mean = 20, sd = 15))
    margin_str <- paste(margin, "runs")
  } else {
    margin <- sample(1:10, 1, prob = stats::dnorm(1:10, mean = 5, sd = 2))
    margin_str <- paste(margin, "wickets")
  }

  list(
    winner = winner,
    loser = loser,
    margin = margin_str,
    team1_won = team1_wins
  )
}


# Result Aggregation =======================================================

#' Aggregate Match Simulation Results
#'
#' Aggregates results from multiple match simulations.
#'
#' @param results List. List of simulation results from simulate_match_outcome
#' @param team1 Character. Team 1 name
#' @param team2 Character. Team 2 name
#'
#' @return List with aggregated statistics
#' @keywords internal
aggregate_match_results <- function(results, team1, team2) {
  n <- length(results)
  team1_wins <- sum(vapply(results, function(r) r$team1_won, logical(1)))
  team2_wins <- n - team1_wins

  list(
    n_simulations = n,
    team1 = team1,
    team2 = team2,
    team1_wins = team1_wins,
    team2_wins = team2_wins,
    team1_win_pct = team1_wins / n,
    team2_win_pct = team2_wins / n
  )
}


#' Aggregate Season Simulation Results
#'
#' Aggregates results from multiple season simulations.
#'
#' @param standings_list List. List of final standings from each simulation.
#'   Each element should be a data frame with columns: team, wins, losses,
#'   points, makes_playoffs, position.
#'
#' @return Data frame with team probabilities
#' @importFrom dplyr group_by summarise mutate arrange desc
#' @keywords internal
aggregate_season_results <- function(standings_list) {
  n <- length(standings_list)

  # Combine all standings
  all_standings <- fast_rbind(lapply(seq_along(standings_list), function(i) {
    df <- standings_list[[i]]
    df$simulation <- i
    df
  }))

  # Calculate probabilities
  results <- all_standings |>
    dplyr::group_by(.data$team) |>
    dplyr::summarise(
      avg_wins = mean(.data$wins),
      avg_losses = mean(.data$losses),
      avg_points = mean(.data$points),
      playoff_appearances = sum(.data$makes_playoffs),
      playoff_pct = mean(.data$makes_playoffs) * 100,
      top2_pct = mean(.data$position <= 2) * 100,
      championship_pct = mean(.data$position == 1) * 100,
      .groups = "drop"
    ) |>
    dplyr::arrange(dplyr::desc(.data$playoff_pct), dplyr::desc(.data$avg_points))

  return(results)
}


# Database Storage =========================================================

#' Store Simulation Results
#'
#' Stores simulation results in the database.
#'
#' @param config List. Simulation configuration from create_simulation_config
#' @param team_results Data frame or list. Per-team results
#' @param match_results Data frame or list. Per-match results (optional)
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns TRUE on success
#' @importFrom DBI dbExecute
#' @keywords internal
store_simulation_results <- function(config, team_results, match_results = NULL, conn) {
  # Convert results to JSON
  team_results_json <- jsonlite::toJSON(team_results, auto_unbox = TRUE)
  match_results_json <- if (!is.null(match_results)) {
    jsonlite::toJSON(match_results, auto_unbox = TRUE)
  } else {
    NA_character_
  }
  params_json <- jsonlite::toJSON(config, auto_unbox = TRUE)

  DBI::dbExecute(conn, "
    INSERT INTO simulation_results (
      simulation_id, simulation_type, event_name, season,
      simulation_date, n_simulations, parameters,
      team_results, match_results, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (simulation_id) DO UPDATE SET
      team_results = EXCLUDED.team_results,
      match_results = EXCLUDED.match_results,
      created_at = EXCLUDED.created_at
  ", params = list(
    config$simulation_id,
    config$simulation_type,
    config$event_name,
    config$season,
    config$created_at,
    config$n_simulations,
    params_json,
    team_results_json,
    match_results_json,
    Sys.time()
  ))

  cli::cli_alert_success("Stored simulation results: {config$simulation_id}")
  invisible(TRUE)
}


#' Get Simulation Results
#'
#' Retrieves simulation results from the database.
#'
#' @param simulation_id Character. Simulation ID (optional)
#' @param simulation_type Character. Filter by type (optional)
#' @param event_name Character. Filter by event (optional)
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with simulation results
#' @importFrom DBI dbGetQuery
#' @keywords internal
get_simulation_results <- function(simulation_id = NULL, simulation_type = NULL,
                                   event_name = NULL, conn) {
  query <- "SELECT * FROM simulation_results WHERE 1=1"
  params <- list()

  if (!is.null(simulation_id)) {
    query <- paste0(query, " AND simulation_id = ?")
    params <- c(params, simulation_id)
  }

  if (!is.null(simulation_type)) {
    query <- paste0(query, " AND simulation_type = ?")
    params <- c(params, simulation_type)
  }

  if (!is.null(event_name)) {
    query <- paste0(query, " AND event_name LIKE ?")
    params <- c(params, paste0("%", event_name, "%"))
  }

  query <- paste0(query, " ORDER BY created_at DESC")

  if (length(params) > 0) {
    DBI::dbGetQuery(conn, query, params = params)
  } else {
    DBI::dbGetQuery(conn, query)
  }
}


# Progress Utilities =======================================================

#' Run Simulations with Progress
#'
#' Runs a simulation function multiple times with progress bar.
#'
#' @param n Integer. Number of simulations
#' @param sim_fn Function. Simulation function to call
#' @param ... Additional arguments passed to sim_fn
#' @param progress Logical. Show progress bar (default TRUE)
#'
#' @return List of simulation results
#' @keywords internal
run_simulations <- function(n, sim_fn, ..., progress = TRUE) {
  if (progress) {
    cli::cli_progress_bar("Simulating", total = n)
  }

  results <- vector("list", n)

  for (i in seq_len(n)) {
    results[[i]] <- sim_fn(...)

    if (progress) {
      cli::cli_progress_update()
    }
  }

  if (progress) {
    cli::cli_progress_done()
  }

  return(results)
}


# Data Loading =============================================================

#' Get Season Fixtures
#'
#' Retrieves all matches for a specific event and season from the database.
#'
#' @param event_name Character. Event name (e.g., "Indian Premier League")
#' @param season Character. Season (e.g., "2024", "2023/24")
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with match fixtures including team ELOs and actual outcomes
#' @importFrom DBI dbGetQuery
#' @keywords internal
get_season_fixtures <- function(event_name, season, conn) {
  query <- "
    SELECT
      m.match_id,
      m.match_date,
      m.team1,
      m.team2,
      m.venue,
      m.outcome_winner,
      m.outcome_by_runs,
      m.outcome_by_wickets,
      m.event_match_number,
      t1.elo_result AS team1_elo,
      t2.elo_result AS team2_elo
    FROM cricsheet.matches m
    LEFT JOIN team_elo t1 ON m.match_id = t1.match_id AND m.team1 = t1.team_id
    LEFT JOIN team_elo t2 ON m.match_id = t2.match_id AND m.team2 = t2.team_id
    WHERE m.event_name LIKE ?
      AND m.season = ?
      AND m.outcome_winner IS NOT NULL
    ORDER BY m.match_date, m.event_match_number
  "

  fixtures <- DBI::dbGetQuery(conn, query, params = list(
    paste0("%", event_name, "%"),
    season
  ))

  # Fill missing ELOs with starting value
  fixtures$team1_elo[is.na(fixtures$team1_elo)] <- 1500
  fixtures$team2_elo[is.na(fixtures$team2_elo)] <- 1500

  # Calculate win probabilities from ELO
  fixtures$team1_win_prob <- elo_win_probability(
    fixtures$team1_elo,
    fixtures$team2_elo
  )

  # Add actual outcome flag
  fixtures$team1_won <- fixtures$outcome_winner == fixtures$team1

  return(fixtures)
}


#' Get Available Seasons
#'
#' Lists available event/season combinations in the database.
#'
#' @param event_name Character. Filter by event name (optional, uses LIKE)
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with event_name, season, and match count
#' @importFrom DBI dbGetQuery
#' @keywords internal
get_available_seasons <- function(event_name = NULL, conn) {
  query <- "
    SELECT
      event_name,
      season,
      COUNT(*) as n_matches,
      MIN(match_date) as start_date,
      MAX(match_date) as end_date
    FROM cricsheet.matches
    WHERE outcome_winner IS NOT NULL
  "

  if (!is.null(event_name)) {
    query <- paste0(query, " AND event_name LIKE ?")
    query <- paste0(query, " GROUP BY event_name, season ORDER BY event_name, season DESC")
    DBI::dbGetQuery(conn, query, params = list(paste0("%", event_name, "%")))
  } else {
    query <- paste0(query, " GROUP BY event_name, season ORDER BY n_matches DESC")
    DBI::dbGetQuery(conn, query)
  }
}


# ELO Utilities ============================================================

#' Calculate Win Probability from ELO
#'
#' Converts ELO ratings to win probability using the standard formula.
#'
#' @param team1_elo Numeric. Team 1 ELO rating
#' @param team2_elo Numeric. Team 2 ELO rating
#' @param divisor Numeric. ELO divisor (default 400)
#'
#' @return Numeric. Probability that team1 wins (0-1)
#' @keywords internal
elo_win_probability <- function(team1_elo, team2_elo, divisor = 400) {
  1 / (1 + 10^((team2_elo - team1_elo) / divisor))
}


# Season Simulation ========================================================

#' Simulate Season
#'
#' Simulates a complete season using pre-match win probabilities.
#' For each match, simulates the outcome based on the ELO-derived win probability.
#'
#' @param fixtures Data frame. Season fixtures from get_season_fixtures()
#' @param points_for_win Integer. Points awarded for a win (default 2)
#' @param points_for_loss Integer. Points awarded for a loss (default 0)
#'
#' @return Data frame with simulated standings
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection(read_only = TRUE)
#' fixtures <- get_season_fixtures("Indian Premier League", "2024", conn)
#' standings <- simulate_season(fixtures)
#' DBI::dbDisconnect(conn, shutdown = TRUE)
#' }
#'
#' @export
simulate_season <- function(fixtures, points_for_win = 2, points_for_loss = 0) {
  # Get all teams
  all_teams <- unique(c(fixtures$team1, fixtures$team2))

  # Initialize standings
  standings <- data.frame(
    team = all_teams,
    wins = 0L,
    losses = 0L,
    points = 0L,
    stringsAsFactors = FALSE
  )
  rownames(standings) <- standings$team

  # Simulate each match
  for (i in seq_len(nrow(fixtures))) {
    team1 <- fixtures$team1[i]
    team2 <- fixtures$team2[i]
    team1_win_prob <- fixtures$team1_win_prob[i]

    # Simulate outcome
    team1_wins <- stats::runif(1) < team1_win_prob

    if (team1_wins) {
      standings[team1, "wins"] <- standings[team1, "wins"] + 1L
      standings[team1, "points"] <- standings[team1, "points"] + points_for_win
      standings[team2, "losses"] <- standings[team2, "losses"] + 1L
      standings[team2, "points"] <- standings[team2, "points"] + points_for_loss
    } else {
      standings[team2, "wins"] <- standings[team2, "wins"] + 1L
      standings[team2, "points"] <- standings[team2, "points"] + points_for_win
      standings[team1, "losses"] <- standings[team1, "losses"] + 1L
      standings[team1, "points"] <- standings[team1, "points"] + points_for_loss
    }
  }

  # Sort by points, then wins
  standings <- standings[order(-standings$points, -standings$wins), ]
  standings$position <- seq_len(nrow(standings))
  standings$makes_playoffs <- standings$position <= 4

  rownames(standings) <- NULL
  return(standings)
}


#' Simulate Season Multiple Times
#'
#' Runs multiple season simulations and aggregates the results.
#'
#' @param fixtures Data frame. Season fixtures from get_season_fixtures()
#' @param n_simulations Integer. Number of simulations to run
#' @param seed Integer. Random seed for reproducibility (optional)
#' @param progress Logical. Show progress bar (default TRUE)
#'
#' @return Data frame with aggregated probabilities per team
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection(read_only = TRUE)
#' fixtures <- get_season_fixtures("Indian Premier League", "2024", conn)
#' results <- simulate_season_n(fixtures, n_simulations = 1000, seed = 42)
#' DBI::dbDisconnect(conn, shutdown = TRUE)
#' }
#'
#' @export
simulate_season_n <- function(fixtures, n_simulations = 1000, seed = NULL,
                               progress = TRUE) {
  if (!is.null(seed)) {
    set.seed(seed)
  }

  if (progress) {
    cli::cli_progress_bar("Simulating seasons", total = n_simulations)
  }

  standings_list <- vector("list", n_simulations)

  for (i in seq_len(n_simulations)) {
    standings_list[[i]] <- simulate_season(fixtures)

    if (progress) {
      cli::cli_progress_update()
    }
  }

  if (progress) {
    cli::cli_progress_done()
  }

  aggregate_season_results(standings_list)
}


#' Get Actual Season Standings
#'
#' Calculates the actual final standings from completed fixtures.
#'
#' @param fixtures Data frame. Season fixtures from get_season_fixtures()
#' @param points_for_win Integer. Points awarded for a win (default 2)
#'
#' @return Data frame with actual standings
#' @keywords internal
get_actual_standings <- function(fixtures, points_for_win = 2) {
  # Get all teams
  all_teams <- unique(c(fixtures$team1, fixtures$team2))

  # Initialize standings
  standings <- data.frame(
    team = all_teams,
    wins = 0L,
    losses = 0L,
    points = 0L,
    stringsAsFactors = FALSE
  )
  rownames(standings) <- standings$team

  # Count actual results
  for (i in seq_len(nrow(fixtures))) {
    winner <- fixtures$outcome_winner[i]
    loser <- if (fixtures$team1[i] == winner) fixtures$team2[i] else fixtures$team1[i]

    if (!is.na(winner) && winner != "") {
      standings[winner, "wins"] <- standings[winner, "wins"] + 1L
      standings[winner, "points"] <- standings[winner, "points"] + points_for_win
      standings[loser, "losses"] <- standings[loser, "losses"] + 1L
    }
  }

  # Sort by points, then wins
  standings <- standings[order(-standings$points, -standings$wins), ]
  standings$position <- seq_len(nrow(standings))
  standings$makes_playoffs <- standings$position <= 4

  rownames(standings) <- NULL
  return(standings)
}


# Playoff Simulation =======================================================

#' Get Playoff Teams
#'
#' Extracts the top 4 teams from standings for playoff simulation.
#'
#' @param standings Data frame. Season standings
#' @param fixtures Data frame. Season fixtures (for ELO lookup)
#'
#' @return Data frame with top 4 teams and their average ELOs
#' @keywords internal
get_playoff_teams <- function(standings, fixtures) {
  top4 <- standings[1:4, ]

  # Calculate average ELO for each team across the season
  team_elos <- sapply(top4$team, function(team) {
    team1_matches <- fixtures$team1 == team
    team2_matches <- fixtures$team2 == team
    elos <- c(fixtures$team1_elo[team1_matches], fixtures$team2_elo[team2_matches])
    if (length(elos) > 0) mean(elos, na.rm = TRUE) else 1500
  })

  top4$elo <- team_elos
  top4
}


#' Simulate IPL Playoffs
#'
#' Simulates the IPL playoff bracket (Qualifier 1, Eliminator, Qualifier 2, Final).
#'
#' @param teams Data frame. Top 4 teams with columns: team, elo, position
#'
#' @return List with winner and path
#'
#' @examples
#' \dontrun{
#' teams <- data.frame(
#'   team = c("Team A", "Team B", "Team C", "Team D"),
#'   elo = c(1600, 1550, 1520, 1480),
#'   position = 1:4
#' )
#' result <- simulate_ipl_playoffs(teams)
#' result$champion
#' }
#'
#' @export
simulate_ipl_playoffs <- function(teams) {
  # Ensure teams are ordered by position
  teams <- teams[order(teams$position), ]

  team1 <- teams$team[1]
  team2 <- teams$team[2]
  team3 <- teams$team[3]
  team4 <- teams$team[4]

  elo1 <- teams$elo[1]
  elo2 <- teams$elo[2]
  elo3 <- teams$elo[3]
  elo4 <- teams$elo[4]

  # Qualifier 1: Team 1 vs Team 2
  q1_prob <- elo_win_probability(elo1, elo2)
  q1_winner <- if (stats::runif(1) < q1_prob) team1 else team2
  q1_loser <- if (q1_winner == team1) team2 else team1
  q1_loser_elo <- if (q1_winner == team1) elo2 else elo1

  # Eliminator: Team 3 vs Team 4
  elim_prob <- elo_win_probability(elo3, elo4)
  elim_winner <- if (stats::runif(1) < elim_prob) team3 else team4
  elim_winner_elo <- if (elim_winner == team3) elo3 else elo4

  # Qualifier 2: Loser Q1 vs Winner Eliminator
  q2_prob <- elo_win_probability(q1_loser_elo, elim_winner_elo)
  q2_winner <- if (stats::runif(1) < q2_prob) q1_loser else elim_winner
  q2_winner_elo <- if (q2_winner == q1_loser) q1_loser_elo else elim_winner_elo

  # Final: Winner Q1 vs Winner Q2
  q1_winner_elo <- if (q1_winner == team1) elo1 else elo2
  final_prob <- elo_win_probability(q1_winner_elo, q2_winner_elo)
  champion <- if (stats::runif(1) < final_prob) q1_winner else q2_winner

  list(
    champion = champion,
    finalist_q1 = q1_winner,
    finalist_q2 = q2_winner,
    q1_winner = q1_winner,
    elim_winner = elim_winner,
    q2_winner = q2_winner
  )
}


#' Simulate Playoffs Multiple Times
#'
#' Runs multiple playoff simulations and aggregates championship probabilities.
#'
#' @param teams Data frame. Top 4 teams from get_playoff_teams()
#' @param n_simulations Integer. Number of simulations
#' @param seed Integer. Random seed (optional)
#' @param progress Logical. Show progress bar (default TRUE)
#'
#' @return Data frame with championship and final appearance probabilities
#' @keywords internal
simulate_playoffs_n <- function(teams, n_simulations = 10000, seed = NULL,
                                 progress = TRUE) {
  if (!is.null(seed)) {
    set.seed(seed)
  }

  if (progress) {
    cli::cli_progress_bar("Simulating playoffs", total = n_simulations)
  }

  # Track results
  championships <- setNames(rep(0L, 4), teams$team[1:4])
  finals <- setNames(rep(0L, 4), teams$team[1:4])

  for (i in seq_len(n_simulations)) {
    result <- simulate_ipl_playoffs(teams)
    championships[result$champion] <- championships[result$champion] + 1L
    finals[result$finalist_q1] <- finals[result$finalist_q1] + 1L
    finals[result$finalist_q2] <- finals[result$finalist_q2] + 1L

    if (progress) {
      cli::cli_progress_update()
    }
  }

  if (progress) {
    cli::cli_progress_done()
  }

  data.frame(
    team = names(championships),
    elo = teams$elo[match(names(championships), teams$team)],
    final_appearances = as.integer(finals),
    final_pct = finals / n_simulations * 100,
    championships = as.integer(championships),
    championship_pct = championships / n_simulations * 100,
    stringsAsFactors = FALSE
  )
}
