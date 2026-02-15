# Test Match Score Projection Functions
#
# Additional functions for handling Test match score projections.
# Test matches have unique challenges:
#   1. Unknown exact overs remaining (depends on days, weather, declarations)
#   2. Multiple innings (up to 4)
#   3. 4-day vs 5-day formats with different overs per day
#
# These functions help estimate overs remaining based on match metadata.


#' Get Test Match Overs Per Day
#'
#' Returns the standard number of overs per day based on Test match duration.
#' 4-day Tests have extended playing hours with 98 overs/day.
#' 5-day Tests have standard 90 overs/day.
#'
#' @param days Integer. Number of days in the Test match (4 or 5).
#'
#' @return Integer. Overs scheduled per day.
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' get_test_overs_per_day(5)  # 90
#' get_test_overs_per_day(4)  # 98
#' }
get_test_overs_per_day <- function(days = 5) {
  if (days == 4) {
    return(TEST_OVERS_PER_DAY_4DAY)
  } else {
    return(TEST_OVERS_PER_DAY_5DAY)
  }
}


#' Estimate Test Match Total Overs
#'
#' Estimates the total overs available in a Test match based on duration.
#'
#' @param days Integer. Number of days in the Test match (4 or 5).
#'
#' @return Integer. Total scheduled overs for the match.
#' @export
#'
#' @examples
#' estimate_test_total_overs(5)  # 450 (5 * 90)
#' estimate_test_total_overs(4)  # 392 (4 * 98)
estimate_test_total_overs <- function(days = 5) {
  overs_per_day <- get_test_overs_per_day(days)
  return(days * overs_per_day)
}


#' Calculate Test Match Overs Remaining
#'
#' Estimates overs remaining in a Test match based on days played,
#' current day's progress, and match duration.
#'
#' @param match_days Integer. Total days scheduled for the match (4 or 5).
#' @param day_number Integer. Current day number (1-5).
#' @param overs_bowled_today Numeric. Overs bowled so far today.
#' @param session Integer. Current session (1, 2, or 3). Optional.
#'
#' @return Numeric. Estimated overs remaining in the match.
#' @export
#'
#' @examples
#' # Day 3 of 5-day Test, 45 overs bowled today
#' calculate_test_overs_remaining(
#'   match_days = 5,
#'   day_number = 3,
#'   overs_bowled_today = 45
#' )
calculate_test_overs_remaining <- function(match_days = 5,
                                           day_number = 1,
                                           overs_bowled_today = 0,
                                           session = NULL) {

  # Validate inputs
  match_days <- as.integer(match_days)
  if (!match_days %in% c(4, 5)) {
    cli::cli_warn("Invalid match_days {match_days}, defaulting to 5")
    match_days <- 5L
  }

  day_number <- as.integer(day_number)
  if (day_number < 1) day_number <- 1L
  if (day_number > match_days) day_number <- match_days

  overs_per_day <- get_test_overs_per_day(match_days)

  # Days completed (before today)
  days_completed <- day_number - 1

  # Overs completed in previous days
  overs_completed_previous <- days_completed * overs_per_day

  # Total overs bowled
  total_overs_bowled <- overs_completed_previous + overs_bowled_today

  # Total scheduled overs
  total_scheduled <- estimate_test_total_overs(match_days)

  # Overs remaining
  overs_remaining <- total_scheduled - total_overs_bowled

  # Ensure non-negative
  overs_remaining <- max(0, overs_remaining)

  return(overs_remaining)
}


#' Convert Test Overs Remaining to Balls
#'
#' Converts overs remaining to balls for use in projection functions.
#'
#' @param overs_remaining Numeric. Overs remaining in match.
#'
#' @return Integer. Balls remaining.
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' test_overs_to_balls(45.3)  # 273 balls (45*6 + 3)
#' }
test_overs_to_balls <- function(overs_remaining) {
  complete_overs <- floor(overs_remaining)
  partial_balls <- round((overs_remaining - complete_overs) * 10)

  # Cricket notation: .1-.6 for balls, but some data uses decimal
  # Handle both cases
  if (partial_balls > 6) {
    # True decimal (e.g., 45.5 = 45.5 overs, not 45 overs 5 balls)
    total_balls <- round(overs_remaining * 6)
  } else {
    # Cricket notation (e.g., 45.3 = 45 overs 3 balls)
    total_balls <- complete_overs * 6 + partial_balls
  }

  return(as.integer(total_balls))
}


#' Estimate Overs Remaining in Current Innings (Test)
#'
#' For Test matches, estimates how many overs might be bowled in the current
#' innings based on typical innings lengths and declarations.
#' This is inherently uncertain and uses heuristics.
#'
#' @param wickets_fallen Integer. Wickets lost so far in innings.
#' @param current_score Integer. Runs scored in innings.
#' @param overs_bowled Numeric. Overs bowled in innings.
#' @param innings_number Integer. Which innings (1-4).
#' @param lead Integer. Run lead/deficit (positive = batting team ahead).
#' @param match_overs_remaining Numeric. Overs remaining in match.
#'
#' @return Numeric. Estimated overs remaining in innings.
#'
#' @examples
#' # First innings, early in the match
#' estimate_test_innings_overs_remaining(
#'   wickets_fallen = 2, current_score = 85,
#'   overs_bowled = 30, innings_number = 1
#' )
#'
#' # Third innings with large lead (likely declaration)
#' estimate_test_innings_overs_remaining(
#'   wickets_fallen = 3, current_score = 350,
#'   overs_bowled = 80, innings_number = 3, lead = 320
#' )
#'
#' @export
estimate_test_innings_overs_remaining <- function(wickets_fallen,
                                                  current_score,
                                                  overs_bowled,
                                                  innings_number = 1,
                                                  lead = 0,
                                                  match_overs_remaining = 270) {

  wickets_remaining <- 10 - wickets_fallen

  # If all out, 0 overs remaining

  if (wickets_remaining <= 0) {
    return(0)
  }

  # Base estimate: typical Test innings lengths
  # First innings tend to be longer, 4th innings shorter
  typical_innings_overs <- switch(as.character(innings_number),
    "1" = 120,  # ~2 days batting
    "2" = 100,
    "3" = 80,
    "4" = 60,   # Chasing innings often shorter
    90
  )

  # Remaining overs based on typical length
  overs_based_on_typical <- max(0, typical_innings_overs - overs_bowled)

  # Remaining overs based on match time
  # Can't bat more overs than remain in match
  overs_based_on_match <- match_overs_remaining

  # Take minimum
  estimated_remaining <- min(overs_based_on_typical, overs_based_on_match)

  # Adjust for declarations (rough heuristics)
  # If batting team has big lead in 3rd innings, likely to declare
  if (innings_number == 3 && lead > 300) {
    # Likely to declare soon
    estimated_remaining <- min(estimated_remaining, 30)
  } else if (innings_number == 3 && lead > 200) {
    estimated_remaining <- min(estimated_remaining, 60)
  }

  # 4th innings: if chasing small target, might not need many overs
  if (innings_number == 4 && lead < 0) {
    target <- abs(lead) + 1
    # Rough estimate: need ~3 runs per over in Tests
    overs_needed_to_win <- ceiling(target / 3)
    estimated_remaining <- min(estimated_remaining, overs_needed_to_win + 20)
  }

  return(estimated_remaining)
}


#' Calculate Test Match Projected Score
#'
#' Wrapper for calculate_projected_score() with Test-specific handling.
#' Estimates overs remaining from match metadata if not provided.
#'
#' @param current_score Integer. Runs scored in innings.
#' @param wickets_remaining Integer. Wickets in hand.
#' @param overs_bowled Numeric. Overs bowled in innings.
#' @param match_days Integer. Total days in match (4 or 5).
#' @param day_number Integer. Current day.
#' @param overs_bowled_today Numeric. Overs bowled today (all innings).
#' @param expected_initial_score Numeric. EIS (NULL = use default).
#' @param params Named list. Projection parameters (NULL = use defaults).
#' @param gender Character. "male" or "female".
#' @param team_type Character. "international" or "club".
#'
#' @return Numeric. Projected innings total.
#'
#' @examples
#' \dontrun{
#' # Day 2 of a 5-day Test, 8 wickets in hand, 50 overs bowled
#' calculate_test_projected_score(
#'   current_score = 180, wickets_remaining = 8,
#'   overs_bowled = 50, match_days = 5,
#'   day_number = 2, overs_bowled_today = 50
#' )
#' }
#'
#' @export
calculate_test_projected_score <- function(current_score,
                                           wickets_remaining,
                                           overs_bowled,
                                           match_days = 5,
                                           day_number = 1,
                                           overs_bowled_today = 0,
                                           expected_initial_score = NULL,
                                           params = NULL,
                                           gender = "male",
                                           team_type = "international") {

  # Estimate match overs remaining
  match_overs_remaining <- calculate_test_overs_remaining(
    match_days = match_days,
    day_number = day_number,
    overs_bowled_today = overs_bowled_today
  )

  # For Test innings, use a simplified resource calculation
  # based on wickets and estimated innings overs remaining
  # (rather than match-level balls remaining)

  # Estimate innings overs remaining
  innings_overs_remaining <- estimate_test_innings_overs_remaining(
    wickets_fallen = 10 - wickets_remaining,
    current_score = current_score,
    overs_bowled = overs_bowled,
    innings_number = 1,  # Simplified - could be passed in
    match_overs_remaining = match_overs_remaining
  )

  # Convert wickets remaining to wickets fallen for new interface
  wickets_fallen <- 10 - wickets_remaining

  # Use general projection function with new interface
  # overs_bowled is the innings progress (already provided as input)
  calculate_projected_score(
    current_score = current_score,
    wickets = wickets_fallen,
    overs = overs_bowled,
    expected_initial_score = expected_initial_score,
    format = "test",
    params = params,
    gender = gender,
    team_type = team_type
  )
}
