# Core ELO Rating System for Cricket

#' Calculate Expected Outcome
#'
#' Calculates the expected outcome for a player matchup using ELO ratings.
#' This is the standard ELO formula: E = 1 / (1 + 10^((R_opponent - R_player) / 400))
#'
#' @param player_elo Numeric. Player's current ELO rating
#' @param opponent_elo Numeric. Opponent's current ELO rating
#' @param divisor Numeric. ELO divisor constant (default 400)
#'
#' @return Numeric value between 0 and 1 representing expected outcome
#' @export
#'
#' @examples
#' # Batter (1600) vs Bowler (1500)
#' calculate_expected_outcome(1600, 1500)
#' # Returns ~0.64 (batter favored)
#'
#' # Batter (1400) vs Bowler (1600)
#' calculate_expected_outcome(1400, 1600)
#' # Returns ~0.24 (bowler favored)
calculate_expected_outcome <- function(player_elo, opponent_elo, divisor = ELO_DIVISOR) {
  1 / (1 + 10^((opponent_elo - player_elo) / divisor))
}


#' Calculate K-Factor
#'
#' Calculates the K-factor (learning rate) for ELO updates based on match type
#' and player experience.
#'
#' @param match_type Character. Type of match ("test", "odi", "t20", etc.)
#' @param player_matches Numeric. Number of matches player has played
#'
#' @return Numeric K-factor value
#' @export
#'
#' @examples
#' # New player in Test match
#' calculate_k_factor("test", player_matches = 5)
#'
#' # Experienced player in T20
#' calculate_k_factor("t20", player_matches = 100)
calculate_k_factor <- function(match_type, player_matches = 0) {
  # Base K-factor by match type
  match_type <- tolower(match_type)

  base_k <- if (match_type %in% c("test", "tests")) {
    K_FACTOR_TEST
  } else if (match_type %in% c("odi", "odis", "mdm")) {
    K_FACTOR_ODI
  } else if (match_type %in% c("t20", "t20i", "it20", "t20s")) {
    K_FACTOR_T20
  } else {
    K_FACTOR_DOMESTIC
  }

  # Adjust for player experience (newer players have higher K)
  # Formula: K * (0.5 + 0.5 / log10(matches + 2))
  # This gradually reduces K as player gains experience
  if (player_matches > 0) {
    experience_factor <- 0.5 + 0.5 / log10(player_matches + 2)
    base_k <- base_k * experience_factor
  }

  return(base_k)
}


#' Calculate ELO Update
#'
#' Calculates the new ELO rating after a performance.
#'
#' @param current_elo Numeric. Player's current ELO rating
#' @param expected Numeric. Expected outcome (from \code{calculate_expected_outcome})
#' @param actual Numeric. Actual outcome score (0 to 1)
#' @param k_factor Numeric. K-factor for this update
#'
#' @return Numeric. New ELO rating
#' @export
#'
#' @examples
#' # Player performed better than expected
#' calculate_elo_update(
#'   current_elo = 1500,
#'   expected = 0.5,
#'   actual = 0.8,
#'   k_factor = 24
#' )
#' # Returns 1507.2 (rating increases)
#'
#' # Player performed worse than expected
#' calculate_elo_update(
#'   current_elo = 1500,
#'   expected = 0.6,
#'   actual = 0.2,
#'   k_factor = 24
#' )
#' # Returns 1490.4 (rating decreases)
calculate_elo_update <- function(current_elo, expected, actual, k_factor) {
  current_elo + k_factor * (actual - expected)
}


#' Initialize Player ELO
#'
#' Returns the starting ELO rating for a new player.
#'
#' @param rating_type Character. Type of rating ("batting" or "bowling")
#'
#' @return Numeric. Starting ELO rating (default 1500)
#' @export
#'
#' @examples
#' initialize_player_elo("batting")
#' initialize_player_elo("bowling")
initialize_player_elo <- function(rating_type = "batting") {
  ELO_START_RATING
}


#' Calculate Actual Outcome Score from Delivery
#'
#' Converts a delivery outcome (runs, wicket) into a score between 0 and 1
#' for ELO calculation.
#'
#' @param runs_batter Integer. Runs scored by batter (not including extras)
#' @param is_wicket Logical. Whether batter was dismissed
#' @param is_boundary Logical. Whether it was a boundary
#'
#' @return Numeric value between 0 and 1
#' @export
#'
#' @examples
#' # Wicket - worst outcome for batter
#' calculate_delivery_outcome_score(0, is_wicket = TRUE, is_boundary = FALSE)
#' # Returns 0.0
#'
#' # Dot ball
#' calculate_delivery_outcome_score(0, is_wicket = FALSE, is_boundary = FALSE)
#' # Returns 0.2
#'
#' # Four runs
#' calculate_delivery_outcome_score(4, is_wicket = FALSE, is_boundary = TRUE)
#' # Returns ~0.73
#'
#' # Six runs
#' calculate_delivery_outcome_score(6, is_wicket = FALSE, is_boundary = TRUE)
#' # Returns 1.0
calculate_delivery_outcome_score <- function(runs_batter, is_wicket, is_boundary = FALSE) {
  # Wicket is complete failure for batter (0.0)
  if (is_wicket) {
    return(0.0)
  }

  # Base score from runs (normalized to 0-6 range)
  # 0 runs = 0.2, 1 run = 0.33, 2 = 0.47, 3 = 0.6, 4 = 0.73, 6 = 1.0
  base_score <- (runs_batter / 6)

  # Bonus for boundaries (dot balls get small credit)
  if (runs_batter == 0) {
    score <- 0.2  # Survived the ball
  } else {
    score <- 0.2 + (base_score * 0.8)
  }

  # Boundary bonus (4s and 6s are premium)
  if (is_boundary) {
    score <- min(score * 1.1, 1.0)
  }

  # Ensure in [0, 1] range
  score <- max(0, min(1, score))

  return(score)
}


#' Normalize Match Type
#'
#' Normalizes match type strings to standard format.
#'
#' @param match_type Character. Raw match type string
#'
#' @return Character. Normalized match type ("test", "odi", or "t20")
#' @keywords internal
normalize_match_type <- function(match_type) {
  match_type <- tolower(trimws(match_type))

  if (match_type %in% c("test", "tests")) {
    return("test")
  } else if (match_type %in% c("odi", "odis", "mdm")) {
    return("odi")
  } else if (match_type %in% c("t20", "t20i", "it20", "t20s", "twenty20")) {
    return("t20")
  } else {
    # Default to T20 for domestic leagues
    return("t20")
  }
}


#' Get Average Runs Per Ball
#'
#' Returns the average runs per ball for a given match format.
#'
#' @param match_type Character. Match type
#'
#' @return Numeric. Average runs per ball
#' @keywords internal
get_avg_runs_per_ball <- function(match_type) {
  match_type <- normalize_match_type(match_type)

  if (match_type == "test") {
    return(AVG_RUNS_PER_BALL_TEST)
  } else if (match_type == "odi") {
    return(AVG_RUNS_PER_BALL_ODI)
  } else {
    return(AVG_RUNS_PER_BALL_T20)
  }
}


#' Get Base Wicket Probability
#'
#' Returns the base probability of wicket per delivery for a given format.
#'
#' @param match_type Character. Match type
#'
#' @return Numeric. Base wicket probability
#' @keywords internal
get_base_wicket_prob <- function(match_type) {
  match_type <- normalize_match_type(match_type)

  if (match_type == "test") {
    return(BASE_WICKET_PROB_TEST)
  } else if (match_type == "odi") {
    return(BASE_WICKET_PROB_ODI)
  } else {
    return(BASE_WICKET_PROB_T20)
  }
}
