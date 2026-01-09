# Margin of Victory Calculation
#
# Functions to calculate unified margin of victory for cricket matches.
# Converts wickets-wins to runs-equivalent using DLS-style resource projection.
#
# Key concept:
# - Runs wins: margin = team1_score - team2_score (straightforward)
# - Wickets wins: project what chasing team "would have scored" using remaining resources
#
# Resource Model:
# Each combination of (balls remaining, wickets remaining) represents a % of
# scoring potential. Uses balls (not overs) to avoid cricket notation confusion.
#
# Power Parameter Logic:
# - balls_power <= 1: Later balls worth MORE (scarcity - each remaining ball precious)
# - wickets_power >= 1: Early wickets worth MORE (quality - top order > tailenders)


#' Convert Cricket Notation Overs to Balls
#'
#' Converts overs in cricket notation (e.g., 18.4 = 18 overs + 4 balls)
#' to total balls. Cricket notation uses .1-.5 for balls, not true decimals.
#'
#' @param overs Numeric. Overs in cricket notation (e.g., 18.4)
#'
#' @return Integer. Total balls
#' @export
#'
#' @examples
#' overs_to_balls(18.4)  # Returns 112 (18*6 + 4)
#' overs_to_balls(20.0)  # Returns 120
overs_to_balls <- function(overs) {
  complete_overs <- floor(overs)
  partial_balls <- round((overs - complete_overs) * 10)  # .4 -> 4 balls
  total_balls <- complete_overs * 6 + partial_balls
  return(as.integer(total_balls))
}


#' Convert Balls to True Decimal Overs
#'
#' Converts balls to true decimal overs (not cricket notation).
#'
#' @param balls Integer. Number of balls
#'
#' @return Numeric. True decimal overs (e.g., 112 balls = 18.667 overs)
#' @export
balls_to_overs <- function(balls) {
  return(balls / 6)
}


#' Calculate Resources Remaining
#'
#' Calculates the percentage of batting resources remaining based on
#' balls and wickets in hand. Uses a simplified DLS-style model.
#'
#' @param balls_remaining Integer. Balls remaining in innings
#' @param wickets_remaining Integer. Wickets in hand (0-10)
#' @param format Character. Match format: "t20", "odi", or "test"
#'
#' @return Numeric. Resource percentage remaining (0-1)
#' @export
#'
#' @examples
#' # T20: 30 balls (5 overs) left, 8 wickets in hand
#' calculate_resource_remaining(30, 8, "t20")
#'
#' # ODI: 120 balls (20 overs) left, 5 wickets in hand
#' calculate_resource_remaining(120, 5, "odi")
calculate_resource_remaining <- function(balls_remaining, wickets_remaining, format = "t20") {

  # Get format-specific parameters
  format_lower <- tolower(format)
  max_balls <- switch(format_lower,
    "t20" = 120,
    "it20" = 120,
    "odi" = 300,
    "odm" = 300,
    "test" = 540,   # Approximate for a day's play (90 overs)
    "mdm" = 540,
    120  # Default to T20
  )

  # Ensure valid inputs (vectorized)
  balls_remaining <- pmax(0, balls_remaining)
  wickets_remaining <- pmax(0, pmin(10, wickets_remaining))

  # Resource calculation calibrated against actual match data
  # with cricket-logical constraints:
  #   - balls_power <= 1: Later balls worth MORE (scarcity)
  #   - wickets_power >= 1: Early wickets worth MORE (quality)

  balls_pct <- balls_remaining / max_balls

  # Balls power: optimized with constraint <= 1.0
  # Lower power = remaining balls more valuable when scarce
  balls_power <- switch(format_lower,
    "t20" = 0.94,
    "it20" = 0.94,
    "odi" = 0.90,
    "odm" = 0.90,
    "test" = 1.00,
    "mdm" = 1.00,
    0.94
  )

  balls_factor <- balls_pct ^ balls_power

  # Wickets power: optimized with constraint >= 1.0
  # Higher power = early wickets (top order) more valuable
  wickets_pct <- wickets_remaining / 10

  wickets_power <- switch(format_lower,
    "t20" = 1.00,
    "it20" = 1.00,
    "odi" = 1.00,
    "odm" = 1.00,
    "test" = 1.50,
    "mdm" = 1.50,
    1.00
  )

  wickets_factor <- wickets_pct ^ wickets_power

  # Combined resource using multiplicative model
  resource_pct <- balls_factor * wickets_factor

  # For Tests, balls are less constraining (can declare, multiple days)
  # Wickets matter more, time/balls matter less
  if (format_lower %in% c("test", "mdm")) {
    resource_pct <- wickets_factor * (0.4 + 0.6 * balls_factor)
  }

  # CRITICAL: 0 wickets = 0% resources (ALL OUT - can't score!)
  # CRITICAL: 0 balls = 0% resources in limited overs (can't bat!)
  resource_pct <- ifelse(wickets_remaining == 0, 0, resource_pct)

  if (format_lower %in% c("t20", "it20", "odi", "odm")) {
    resource_pct <- ifelse(balls_remaining == 0, 0, resource_pct)
  }

  # Ensure bounds
  resource_pct <- pmax(0, pmin(1, resource_pct))

  return(resource_pct)
}


#' Project Chaser Final Score
#'
#' Projects what the chasing team would have scored if they continued
#' batting after reaching the target. Used to convert wickets-wins to
#' runs-equivalent margin.
#'
#' @param score_achieved Integer. Runs scored when target was reached
#' @param balls_remaining Integer. Balls remaining when target reached
#' @param wickets_remaining Integer. Wickets in hand when target reached
#' @param format Character. Match format
#'
#' @return Numeric. Projected final score
#' @export
#'
#' @examples
#' # Team scored 165 to chase 160, with 12 balls (2 overs) and 6 wickets left
#' project_chaser_final_score(165, 12, 6, "t20")
project_chaser_final_score <- function(score_achieved, balls_remaining,
                                        wickets_remaining, format = "t20") {

  # Calculate resources remaining at point of win
  resource_remaining <- calculate_resource_remaining(
    balls_remaining, wickets_remaining, format
  )

  # Resources used
  resource_used <- 1 - resource_remaining

  # Prevent division by zero (if somehow no resources used)
  if (resource_used <= 0.01) {
    return(score_achieved)
  }

  # Project: if they scored X with Y% resources, they would score X / Y with 100%
  projected_final <- score_achieved / resource_used

  return(projected_final)
}


#' Project Chaser Final Score (Advanced)
#'
#' Enhanced version of project_chaser_final_score() that uses the
#' optimized score projection formula with format-specific parameters.
#'
#' Formula:
#'   projected = cs + a * eis * resource_remaining + b * cs * resource_remaining / resource_used
#'
#' @param score_achieved Integer. Runs scored when target was reached.
#' @param balls_remaining Integer. Balls remaining when target reached.
#' @param wickets_remaining Integer. Wickets in hand when target reached.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param gender Character. "male" or "female" (for loading correct parameters).
#' @param team_type Character. "international" or "club" (for loading correct parameters).
#' @param expected_initial_score Numeric. Expected initial score (NULL = use agnostic).
#' @param params Named list. Pre-loaded parameters (NULL = load from file).
#'
#' @return Numeric. Projected final score.
#' @export
#'
#' @examples
#' # Team scored 165 to chase 160, with 12 balls (2 overs) and 6 wickets left
#' project_chaser_final_score_advanced(165, 12, 6, "t20", "male", "club")
project_chaser_final_score_advanced <- function(score_achieved,
                                                 balls_remaining,
                                                 wickets_remaining,
                                                 format = "t20",
                                                 gender = "male",
                                                 team_type = "international",
                                                 expected_initial_score = NULL,
                                                 params = NULL) {

  # Use the main score projection function
  calculate_projected_score(
    current_score = score_achieved,
    balls_remaining = balls_remaining,
    wickets_remaining = wickets_remaining,
    expected_initial_score = expected_initial_score,
    format = format,
    params = params,
    gender = gender,
    team_type = team_type
  )
}


#' Calculate Unified Margin
#'
#' Converts all match outcomes to a runs-equivalent margin.
#' Positive margin = team1 won, negative margin = team2 won.
#'
#' For runs wins: margin = team1_score - team2_score
#' For wickets wins: project team2's final score, then calculate difference
#'
#' @param team1_score Integer. Team 1's total (batting first or only innings)
#' @param team2_score Integer. Team 2's total when match ended
#' @param balls_remaining Integer. Balls remaining when match ended (0 for runs wins)
#' @param wickets_remaining Integer. Wickets in hand when match ended (0 for runs wins)
#' @param win_type Character. "runs", "wickets", "tie", or "draw"
#' @param format Character. Match format
#' @param super_over_margin Integer. If match had Super Over, the run difference (optional)
#' @param use_advanced_projection Logical. If TRUE, uses the optimized score projection
#'   formula. If FALSE (default), uses simple resource-based projection.
#' @param gender Character. "male" or "female" (for advanced projection).
#' @param team_type Character. "international" or "club" (for advanced projection).
#'
#' @return Numeric. Unified margin in runs-equivalent.
#'   Positive = team1 won, Negative = team2 won, 0 = tie/draw
#' @export
#'
#' @examples
#' # Team 1 won by 20 runs
#' calculate_unified_margin(180, 160, 0, 0, "runs", "t20")
#'
#' # Team 2 won by 6 wickets with 12 balls (2 overs) left
#' calculate_unified_margin(160, 165, 12, 6, "wickets", "t20")
#'
#' # Using advanced projection
#' calculate_unified_margin(160, 165, 12, 6, "wickets", "t20",
#'                          use_advanced_projection = TRUE, gender = "male", team_type = "club")
#'
#' # Tie
#' calculate_unified_margin(160, 160, 0, 0, "tie", "t20")
calculate_unified_margin <- function(team1_score, team2_score,
                                      balls_remaining = 0,
                                      wickets_remaining = 0,
                                      win_type = "runs",
                                      format = "t20",
                                      super_over_margin = NULL,
                                      use_advanced_projection = FALSE,
                                      gender = "male",
                                      team_type = "international") {

  win_type <- tolower(win_type)

  # Handle ties and draws

  if (win_type %in% c("tie", "draw", "no result", "no_result")) {
    return(0)
  }

  # Runs win: team batting first won
  if (win_type == "runs") {
    margin <- team1_score - team2_score
    return(margin)
  }

  # Wickets win: team batting second won
  if (win_type == "wickets") {
    # Project what team2 would have scored
    if (use_advanced_projection) {
      projected_team2 <- project_chaser_final_score_advanced(
        team2_score, balls_remaining, wickets_remaining, format,
        gender = gender, team_type = team_type
      )
    } else {
      projected_team2 <- project_chaser_final_score(
        team2_score, balls_remaining, wickets_remaining, format
      )
    }

    # Margin from team1's perspective (negative = team2 won)
    margin <- team1_score - projected_team2

    # Add Super Over margin if applicable
    if (!is.null(super_over_margin) && !is.na(super_over_margin)) {
      # Super Over margin should maintain the sign (who won)
      # If team2 won Super Over, margin becomes more negative
      margin <- margin - super_over_margin
    }

    return(margin)
  }

  # Super Over win (main match was tied)
  if (win_type %in% c("super over", "super_over", "superover")) {
    if (!is.null(super_over_margin) && !is.na(super_over_margin)) {
      return(super_over_margin)
    }
    # If no Super Over margin provided, treat as small margin
    return(0)
  }

  # Unknown win type - return 0
  cli::cli_warn("Unknown win_type: {win_type}, returning margin = 0")
  return(0)
}


#' Calculate Unified Margin for Match Data
#'
#' Convenience function to calculate unified margin from match record data.
#' Handles extraction of scores, wickets, overs from database columns.
#' Converts cricket notation overs to balls internally.
#'
#' @param match_data Data frame or list with match information. Expected columns:
#'   - team1_score, team2_score (or innings totals)
#'   - outcome_by_runs, outcome_by_wickets
#'   - balls_remaining OR overs_remaining (cricket notation, will be converted)
#'   - wickets_remaining (optional, derived from outcome_by_wickets)
#'   - match_type
#'   - outcome_method (optional, for Super Over detection)
#'
#' @return Numeric. Unified margin
#' @export
calculate_match_margin <- function(match_data) {

  # Extract scores
  team1_score <- match_data$team1_score %||%
                 match_data$innings1_total %||%
                 match_data$team1_runs %||%
                 NA_integer_

  team2_score <- match_data$team2_score %||%
                 match_data$innings2_total %||%
                 match_data$team2_runs %||%
                 NA_integer_

  # Determine win type
  outcome_by_runs <- match_data$outcome_by_runs %||% NA_integer_
  outcome_by_wickets <- match_data$outcome_by_wickets %||% NA_integer_

  if (!is.na(outcome_by_runs) && outcome_by_runs > 0) {
    win_type <- "runs"
    balls_remaining <- 0L
    wickets_remaining <- 0
  } else if (!is.na(outcome_by_wickets) && outcome_by_wickets > 0) {
    win_type <- "wickets"
    wickets_remaining <- outcome_by_wickets

    # Get balls remaining - prefer direct balls, else convert cricket notation overs
    if (!is.null(match_data$balls_remaining) && !is.na(match_data$balls_remaining)) {
      balls_remaining <- as.integer(match_data$balls_remaining)
    } else if (!is.null(match_data$overs_remaining) && !is.na(match_data$overs_remaining)) {
      # overs_remaining is in cricket notation (18.4 = 18 overs + 4 balls)
      balls_remaining <- overs_to_balls(match_data$overs_remaining)
    } else {
      balls_remaining <- 0L
    }
  } else {
    # Tie, draw, or no result
    win_type <- match_data$outcome_type %||% "tie"
    balls_remaining <- 0L
    wickets_remaining <- 0
  }

  # Format
  format <- match_data$match_type %||% match_data$format %||% "t20"

  # Super Over handling
  outcome_method <- match_data$outcome_method %||% ""
  super_over_margin <- NULL
  if (grepl("super", tolower(outcome_method))) {
    # If we have Super Over data, include it
    super_over_margin <- match_data$super_over_margin %||% 5  # Default small margin
  }

  calculate_unified_margin(
    team1_score = team1_score,
    team2_score = team2_score,
    balls_remaining = balls_remaining,
    wickets_remaining = wickets_remaining,
    win_type = win_type,
    format = format,
    super_over_margin = super_over_margin
  )
}


#' Normalize Margin by Format
#'
#' Normalizes margin to a 0-1 scale based on typical margins for each format.
#' Useful for comparing margins across formats or for model training.
#'
#' @param margin Numeric. Raw margin in runs
#' @param format Character. Match format
#'
#' @return Numeric. Normalized margin (-1 to 1 scale, 0 = tied)
#' @export
normalize_margin <- function(margin, format = "t20") {

  # Typical maximum margins by format (95th percentile roughly)
  max_margin <- switch(tolower(format),
    "t20" = 80,
    "it20" = 80,
    "odi" = 150,
    "odm" = 150,
    "test" = 300,  # Innings + runs
    "mdm" = 300,
    80  # Default
  )

  # Normalize using tanh-like scaling to bound in [-1, 1]
  normalized <- margin / max_margin

  # Soft clamp using tanh to prevent extreme values
  normalized <- tanh(normalized)

  return(normalized)
}


#' Get Expected Margin from ELO Difference
#'
#' Calculates the expected margin of victory based on the ELO difference
#' between two teams. Based on FiveThirtyEight methodology.
#'
#' @param team1_elo Numeric. Team 1's ELO rating
#' @param team2_elo Numeric. Team 2's ELO rating
#' @param home_advantage Numeric. Home advantage in ELO points (added to team1)
#' @param format Character. Match format
#'
#' @return Numeric. Expected margin in runs (positive = team1 favored)
#' @export
#'
#' @examples
#' # Team with 100 ELO advantage
#' get_expected_margin(1600, 1500, 0, "t20")  # ~6-7 runs
get_expected_margin <- function(team1_elo, team2_elo, home_advantage = 0,
                                 format = "t20") {

  elo_diff <- team1_elo - team2_elo + home_advantage

  # Format-specific divisor
  divisor <- switch(tolower(format),
    "t20" = MARGIN_ELO_DIVISOR_T20,
    "it20" = MARGIN_ELO_DIVISOR_T20,
    "odi" = MARGIN_ELO_DIVISOR_ODI,
    "odm" = MARGIN_ELO_DIVISOR_ODI,
    "test" = MARGIN_ELO_DIVISOR_TEST,
    "mdm" = MARGIN_ELO_DIVISOR_TEST,
    MARGIN_ELO_DIVISOR_T20  # Default
  )

  expected_margin <- elo_diff / divisor

  return(expected_margin)
}
