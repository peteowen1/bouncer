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
#' @keywords internal
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
#' @keywords internal
balls_to_overs <- function(balls) {
  return(balls / 6)
}


#' Convert Balls to Cricket Notation Overs
#'
#' Converts balls to cricket notation overs (e.g., 82 balls = 13.4 overs).
#' In cricket notation, .1-.5 represent balls, not true decimals.
#'
#' @param balls Integer. Number of balls
#'
#' @return Numeric. Overs in cricket notation (e.g., 82 balls = 13.4)
#' @keywords internal
balls_to_overs_cricket <- function(balls) {
  complete_overs <- balls %/% 6
  remaining_balls <- balls %% 6
  return(complete_overs + remaining_balls / 10)
}


#' Calculate Unified Margin
#'
#' Converts all match outcomes to a runs-equivalent margin.
#' Positive margin = team1 won, negative margin = team2 won.
#'
#' For runs wins: margin = team1_score - team2_score
#' For wickets wins: project team2's final score using optimized projection,
#' then calculate difference.
#'
#' @param team1_score Integer. Team 1's total (batting first or only innings).
#' @param team2_score Integer. Team 2's total when match ended.
#' @param wickets_remaining Integer. Wickets in hand when match ended (0 for runs wins).
#'   This is how results are reported: "won by 6 wickets" means wickets_remaining=6.
#' @param overs_remaining Numeric. Overs remaining in cricket notation (e.g., 2.3 = 2 overs + 3 balls).
#'   This is how results are reported: "with 2.3 overs to spare" means overs_remaining=2.3.
#' @param win_type Character. "runs", "wickets", "tie", or "draw".
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param super_over_margin Integer. If match had Super Over, the run difference (optional).
#' @param gender Character. "male" or "female".
#' @param team_type Character. "international" or "club".
#'
#' @return Numeric. Unified margin in runs-equivalent.
#'   Positive = team1 won, Negative = team2 won, 0 = tie/draw
#' @keywords internal
calculate_unified_margin <- function(team1_score, team2_score,
                                      wickets_remaining = 0,
                                      overs_remaining = 0,
                                      win_type = "runs",
                                      format = "t20",
                                      super_over_margin = NULL,
                                      gender = "male",
                                      team_type = "international") {

  win_type <- tolower(win_type)

  # Convert overs remaining to balls remaining
  balls_remaining <- overs_to_balls(overs_remaining)

  # Handle ties and draws
  if (win_type %in% c("tie", "draw", "no result", "no_result")) {
    return(0)
  }

  # Runs win: team batting first won
  if (win_type == "runs") {
    # Validation: runs win means team1 scored more than team2
    if (!is.na(team1_score) && !is.na(team2_score) && team1_score <= team2_score) {
      cli::cli_warn("Runs win but team1_score ({team1_score}) <= team2_score ({team2_score}). This is invalid - team1 must have scored more.")
    }

    margin <- team1_score - team2_score
    return(margin)
  }

  # Wickets win: team batting second won
  if (win_type == "wickets") {
    # Validation: wickets win means team2 passed team1's score
    if (!is.na(team2_score) && !is.na(team1_score) && team2_score <= team1_score) {
      cli::cli_warn("Wickets win but team2_score ({team2_score}) <= team1_score ({team1_score}). This is invalid - team2 must have passed the target.")
    }

    # Convert remaining values to scoreboard format for projection
    # wickets fallen = 10 - wickets remaining
    wickets_fallen <- 10 - wickets_remaining

    # Get max balls for format to calculate overs bowled
    format_lower <- tolower(format)
    max_balls <- switch(format_lower,
      "t20" = 120, "it20" = 120,
      "odi" = 300, "odm" = 300,
      "test" = 540, "mdm" = 540,
      120
    )
    balls_bowled <- max_balls - balls_remaining
    overs_bowled <- balls_to_overs_cricket(balls_bowled)

    # Project what team2 would have scored
    projected_team2 <- calculate_projected_score(
      current_score = team2_score,
      wickets = wickets_fallen,
      overs = overs_bowled,
      format = format,
      gender = gender,
      team_type = team_type
    )

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
#'
#' @param match_data Data frame or list with match information. Expected columns:
#'   - team1_score, team2_score (or innings totals)
#'   - outcome_by_runs, outcome_by_wickets
#'   - overs_remaining (cricket notation like 2.3)
#'   - match_type
#'   - outcome_method (optional, for Super Over detection)
#'   - gender (optional)
#'   - team_type (optional)
#'
#' @return Numeric. Unified margin
#' @keywords internal
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
    overs_remaining <- 0
    wickets_remaining <- 0
  } else if (!is.na(outcome_by_wickets) && outcome_by_wickets > 0) {
    win_type <- "wickets"
    wickets_remaining <- outcome_by_wickets

    # Get overs remaining (prefer direct, else convert from balls if available)
    if (!is.null(match_data$overs_remaining) && !is.na(match_data$overs_remaining)) {
      overs_remaining <- match_data$overs_remaining
    } else if (!is.null(match_data$balls_remaining) && !is.na(match_data$balls_remaining)) {
      # Convert balls to cricket notation overs
      overs_remaining <- balls_to_overs_cricket(as.integer(match_data$balls_remaining))
    } else {
      overs_remaining <- 0
    }
  } else {
    # Tie, draw, or no result
    win_type <- match_data$outcome_type %||% "tie"
    overs_remaining <- 0
    wickets_remaining <- 0
  }

  # Format
  format <- match_data$match_type %||% match_data$format %||% "t20"

  # Gender and team_type for projection
  gender <- match_data$gender %||% "male"
  team_type <- match_data$team_type %||% "international"

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
    wickets_remaining = wickets_remaining,
    overs_remaining = overs_remaining,
    win_type = win_type,
    format = format,
    super_over_margin = super_over_margin,
    gender = gender,
    team_type = team_type
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
#' @keywords internal
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
#' @keywords internal
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
