# ELO-Based Prediction Functions

#' Predict Expected Runs
#'
#' Predicts the expected runs for a batter vs bowler matchup based on ELO ratings.
#'
#' @param batter_id Character. Batter identifier
#' @param bowler_id Character. Bowler identifier
#' @param context List with match context:
#'   \itemize{
#'     \item match_type: "test", "odi", or "t20"
#'     \item venue: Stadium name (optional)
#'     \item over: Over number (optional)
#'     \item phase: Match phase - "powerplay", "middle", "death" (optional)
#'   }
#' @param batter_elo Numeric. If provided, uses this instead of looking up
#' @param bowler_elo Numeric. If provided, uses this instead of looking up
#' @param db_path Character. Database path
#'
#' @return Numeric. Expected runs for this delivery
#' @export
#'
#' @examples
#' \dontrun{
#' # Basic prediction
#' expected_runs <- predict_expected_runs(
#'   "V Kohli",
#'   "J Bumrah",
#'   context = list(match_type = "t20")
#' )
#'
#' # With context
#' expected_runs <- predict_expected_runs(
#'   "V Kohli",
#'   "J Bumrah",
#'   context = list(
#'     match_type = "t20",
#'     venue = "Wankhede Stadium",
#'     over = 18,
#'     phase = "death"
#'   )
#' )
#' }
predict_expected_runs <- function(batter_id,
                                   bowler_id,
                                   context = list(match_type = "t20"),
                                   batter_elo = NULL,
                                   bowler_elo = NULL,
                                   db_path = NULL) {

  # Get ELO ratings if not provided
  if (is.null(batter_elo)) {
    match_type <- context$match_type %||% "t20"
    batter_elo <- get_batting_elo(batter_id, match_type, db_path = db_path)
  }

  if (is.null(bowler_elo)) {
    match_type <- context$match_type %||% "t20"
    bowler_elo <- get_bowling_elo(bowler_id, match_type, db_path = db_path)
  }

  # Calculate expected outcome (probability batter succeeds)
  p_batter_success <- calculate_expected_outcome(batter_elo, bowler_elo)

  # Get average runs per ball for this format
  match_type <- context$match_type %||% "t20"
  avg_runs <- get_avg_runs_per_ball(match_type)

  # Base expected runs
  expected <- p_batter_success * avg_runs

  # Apply context adjustments
  if (!is.null(context$phase)) {
    expected <- adjust_for_phase(expected, context$phase, match_type)
  }

  if (!is.null(context$over)) {
    expected <- adjust_for_over(expected, context$over, match_type)
  }

  return(expected)
}


#' Predict Wicket Probability
#'
#' Predicts the probability of wicket for a batter vs bowler matchup.
#'
#' @inheritParams predict_expected_runs
#'
#' @return Numeric. Probability of wicket (0 to 1)
#' @export
#'
#' @examples
#' \dontrun{
#' # Basic prediction
#' wicket_prob <- predict_wicket_probability(
#'   "V Kohli",
#'   "J Bumrah",
#'   context = list(match_type = "t20")
#' )
#'
#' # Death overs (higher pressure)
#' wicket_prob <- predict_wicket_probability(
#'   "V Kohli",
#'   "J Bumrah",
#'   context = list(match_type = "t20", over = 19, phase = "death")
#' )
#' }
predict_wicket_probability <- function(batter_id,
                                        bowler_id,
                                        context = list(match_type = "t20"),
                                        batter_elo = NULL,
                                        bowler_elo = NULL,
                                        db_path = NULL) {

  # Get ELO ratings if not provided
  if (is.null(batter_elo)) {
    match_type <- context$match_type %||% "t20"
    batter_elo <- get_batting_elo(batter_id, match_type, db_path = db_path)
  }

  if (is.null(bowler_elo)) {
    match_type <- context$match_type %||% "t20"
    bowler_elo <- get_bowling_elo(bowler_id, match_type, db_path = db_path)
  }

  # Calculate expected outcome (probability bowler succeeds)
  p_bowler_success <- 1 - calculate_expected_outcome(batter_elo, bowler_elo)

  # Get base wicket probability for this format
  match_type <- context$match_type %||% "t20"
  base_prob <- get_base_wicket_prob(match_type)

  # Scale by ELO difference
  # If bowler favored (p_bowler_success > 0.5), increase wicket probability
  # If batter favored (p_bowler_success < 0.5), decrease wicket probability
  wicket_prob <- base_prob * (0.5 + p_bowler_success)

  # Apply context adjustments
  if (!is.null(context$phase)) {
    wicket_prob <- adjust_wicket_for_phase(wicket_prob, context$phase, match_type)
  }

  if (!is.null(context$over)) {
    wicket_prob <- adjust_wicket_for_over(wicket_prob, context$over, match_type)
  }

  # Ensure valid probability
  wicket_prob <- max(0, min(1, wicket_prob))

  return(wicket_prob)
}


#' Predict Matchup Outcome
#'
#' Comprehensive prediction for a batter vs bowler matchup.
#'
#' @inheritParams predict_expected_runs
#'
#' @return List with predictions:
#'   \itemize{
#'     \item expected_runs: Expected runs this ball
#'     \item wicket_probability: Probability of dismissal
#'     \item batter_elo: Batter's ELO rating
#'     \item bowler_elo: Bowler's ELO rating
#'     \item batter_advantage: How much batter is favored (-1 to 1)
#'   }
#' @export
#'
#' @examples
#' \dontrun{
#' # Full matchup prediction
#' matchup <- predict_matchup_outcome(
#'   "V Kohli",
#'   "J Bumrah",
#'   context = list(match_type = "t20", over = 18, phase = "death")
#' )
#'
#' print(matchup$expected_runs)
#' print(matchup$wicket_probability)
#' print(matchup$batter_advantage)
#' }
predict_matchup_outcome <- function(batter_id,
                                     bowler_id,
                                     context = list(match_type = "t20"),
                                     batter_elo = NULL,
                                     bowler_elo = NULL,
                                     db_path = NULL) {

  # Get ELO ratings
  match_type <- context$match_type %||% "t20"

  if (is.null(batter_elo)) {
    batter_elo <- get_batting_elo(batter_id, match_type, db_path = db_path)
  }

  if (is.null(bowler_elo)) {
    bowler_elo <- get_bowling_elo(bowler_id, match_type, db_path = db_path)
  }

  # Calculate expected runs
  expected_runs <- predict_expected_runs(
    batter_id, bowler_id, context,
    batter_elo = batter_elo,
    bowler_elo = bowler_elo,
    db_path = db_path
  )

  # Calculate wicket probability
  wicket_prob <- predict_wicket_probability(
    batter_id, bowler_id, context,
    batter_elo = batter_elo,
    bowler_elo = bowler_elo,
    db_path = db_path
  )

  # Calculate advantage
  # Positive = batter favored, Negative = bowler favored
  elo_diff <- batter_elo - bowler_elo
  batter_advantage <- tanh(elo_diff / 200)  # Scale to roughly -1 to 1

  list(
    batter_id = batter_id,
    bowler_id = bowler_id,
    expected_runs = expected_runs,
    wicket_probability = wicket_prob,
    batter_elo = batter_elo,
    bowler_elo = bowler_elo,
    elo_difference = elo_diff,
    batter_advantage = batter_advantage,
    context = context
  )
}


# Helper functions for context adjustments

#' Adjust for Match Phase
#' @keywords internal
adjust_for_phase <- function(expected_runs, phase, match_type) {
  # Adjust expected runs based on match phase
  if (match_type == "t20") {
    if (phase == "powerplay") {
      # Slightly higher scoring in powerplay
      expected_runs <- expected_runs * 1.1
    } else if (phase == "death") {
      # Much higher scoring in death overs
      expected_runs <- expected_runs * 1.3
    }
  } else if (match_type == "odi") {
    if (phase == "powerplay") {
      expected_runs <- expected_runs * 1.05
    } else if (phase == "death") {
      expected_runs <- expected_runs * 1.25
    }
  }

  return(expected_runs)
}

#' Adjust Wicket Probability for Phase
#' @keywords internal
adjust_wicket_for_phase <- function(wicket_prob, phase, match_type) {
  # Wicket probability tends to be higher when batters attack
  if (phase == "powerplay") {
    # Batters attack, slightly more wickets
    wicket_prob <- wicket_prob * 1.1
  } else if (phase == "death") {
    # Batters take risks, more wickets
    wicket_prob <- wicket_prob * 1.3
  }

  return(wicket_prob)
}

#' Adjust for Over Number
#' @keywords internal
adjust_for_over <- function(expected_runs, over_num, match_type) {
  # Similar to phase but more granular
  if (match_type == "t20") {
    if (over_num >= 16) {
      expected_runs <- expected_runs * 1.3
    } else if (over_num <= 6) {
      expected_runs <- expected_runs * 1.1
    }
  } else if (match_type == "odi") {
    if (over_num >= 40) {
      expected_runs <- expected_runs * 1.25
    } else if (over_num <= 10) {
      expected_runs <- expected_runs * 1.05
    }
  }

  return(expected_runs)
}

#' Adjust Wicket Probability for Over
#' @keywords internal
adjust_wicket_for_over <- function(wicket_prob, over_num, match_type) {
  if (match_type == "t20") {
    if (over_num >= 16) {
      wicket_prob <- wicket_prob * 1.3
    } else if (over_num <= 6) {
      wicket_prob <- wicket_prob * 1.1
    }
  } else if (match_type == "odi") {
    if (over_num >= 40) {
      wicket_prob <- wicket_prob * 1.25
    } else if (over_num <= 10) {
      wicket_prob <- wicket_prob * 1.05
    }
  }

  return(wicket_prob)
}

#' Null-coalescing operator
#' @keywords internal
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}
