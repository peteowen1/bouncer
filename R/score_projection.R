# Score Projection Functions
#
# Functions to project final innings totals from any game state.
# Uses a parameterized resource-based formula with optimizable parameters.
#
# Formula:
#   projected = cs + a * eis * resource_remaining + b * cs * resource_remaining / resource_used
#
# Where:
#   cs = current score
#   eis = expected initial score (format average or model-predicted)
#   resource_remaining = (balls_remaining/max_balls)^z * (wickets_remaining/10)^y
#   resource_used = 1 - resource_remaining
#
# Two models:
#   1. Agnostic: eis = format average (no team/venue identity)
#   2. Full: eis = model-predicted (with team skills, venue skills)
#
# NOTE: Parameter optimization and batch calculation functions are in
# score_projection_optimization.R for better maintainability.


#' Calculate Projection Resource Remaining
#'
#' Calculates the percentage of batting resources remaining based on
#' wickets and balls in hand using parameterized power formula.
#'
#' @param wickets_remaining Integer. Wickets in hand (0-10).
#' @param balls_remaining Integer. Balls remaining in innings.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param z Numeric. Power parameter for balls (default from constants).
#' @param y Numeric. Power parameter for wickets (default from constants).
#' @param max_balls Integer. Maximum balls in format (NULL = use default).
#'
#' @return Numeric. Resource percentage remaining (0-1).
#' @export
#'
#' @examples
#' # T20: 8 wickets in hand, 30 balls (5 overs) left
#' calculate_projection_resource(8, 30, "t20")
#'
#' # ODI: 5 wickets in hand, 120 balls (20 overs) left, custom parameters
#' calculate_projection_resource(5, 120, "odi", z = 0.85, y = 1.1)
calculate_projection_resource <- function(wickets_remaining,
                                          balls_remaining,
                                          format = "t20",
                                          z = NULL,
                                          y = NULL,
                                          max_balls = NULL) {

  # Get max balls for format
  if (is.null(max_balls)) {
    max_balls <- get_max_balls(format)
  }

  # Use default parameters if not provided
  if (is.null(z)) z <- PROJ_DEFAULT_Z
  if (is.null(y)) y <- PROJ_DEFAULT_Y

  # Ensure valid inputs (vectorized)
  balls_remaining <- pmax(0, balls_remaining)
  wickets_remaining <- pmax(0, pmin(10, wickets_remaining))

  # Calculate resource components
  balls_pct <- balls_remaining / max_balls
  wickets_pct <- wickets_remaining / 10

  # Apply power parameters
  balls_factor <- balls_pct ^ z
  wickets_factor <- wickets_pct ^ y

  # Combined resource (multiplicative)
  resource_remaining <- balls_factor * wickets_factor

  # Edge cases: 0 wickets or 0 balls (in limited overs) = 0 resources
  resource_remaining <- ifelse(wickets_remaining == 0, 0, resource_remaining)

  # In limited overs formats, 0 balls remaining = 0 resources
  if (normalize_format(format) %in% c("t20", "odi")) {
    resource_remaining <- ifelse(balls_remaining == 0, 0, resource_remaining)
  }

  # Ensure bounds
  resource_remaining <- pmax(0, pmin(1, resource_remaining))

  return(resource_remaining)
}


#' Get Agnostic Expected Score (Format Average)
#'
#' Returns the format/gender/team_type average innings total for agnostic projections.
#' This is the expected initial score (EIS) when we don't consider team identity.
#'
#' @param format Character. Format: "t20", "odi", or "test".
#' @param gender Character. "male" or "female".
#' @param team_type Character. "international" or "club" (domestic/franchise).
#'
#' @return Numeric. Expected initial score (average first innings total).
#' @keywords internal
get_agnostic_expected_score <- function(format = "t20",
                                        gender = "male",
                                        team_type = "international") {

  gender_lower <- tolower(gender)
  team_type_lower <- tolower(team_type)

  # Normalize format using central helper
  format_norm <- normalize_format(format)

  # Normalize team_type (club = domestic/franchise)
  is_international <- team_type_lower %in% c("international", "intl", "int")

  # Look up EIS from constants
  if (format_norm == "t20") {
    if (gender_lower == "male") {
      eis <- if (is_international) EIS_T20_MALE_INTL else EIS_T20_MALE_CLUB
    } else {
      eis <- if (is_international) EIS_T20_FEMALE_INTL else EIS_T20_FEMALE_CLUB
    }
  } else if (format_norm == "odi") {
    if (gender_lower == "male") {
      eis <- if (is_international) EIS_ODI_MALE_INTL else EIS_ODI_MALE_CLUB
    } else {
      eis <- if (is_international) EIS_ODI_FEMALE_INTL else EIS_ODI_FEMALE_CLUB
    }
  } else {  # test
    if (gender_lower == "male") {
      eis <- if (is_international) EIS_TEST_MALE_INTL else EIS_TEST_MALE_CLUB
    } else {
      eis <- if (is_international) EIS_TEST_FEMALE_INTL else EIS_TEST_FEMALE_CLUB
    }
  }

  return(eis)
}


#' Load Projection Parameters
#'
#' Loads optimized projection parameters for a format/gender/team_type segment.
#' Parameters are loaded from RDS files or database if available,
#' otherwise returns defaults.
#'
#' @param format Character. Format: "t20", "odi", "test".
#' @param gender Character. "male" or "female".
#' @param team_type Character. "international" or "club".
#' @param params_dir Character. Directory where parameter files are stored.
#'   NULL uses default location (../bouncerdata/models/).
#' @param conn DBI connection. Optional database connection to load from DB.
#'
#' @return Named list with parameters: a, b, z, y, eis_agnostic.
#' @keywords internal
load_projection_params <- function(format = "t20",
                                   gender = "male",
                                   team_type = "international",
                                   params_dir = NULL,
                                   conn = NULL) {

  gender_lower <- tolower(gender)
  team_type_lower <- tolower(team_type)

  # Normalize format using central helper
  format_norm <- normalize_format(format)

  # Normalize team_type
  team_type_norm <- if (team_type_lower %in% c("international", "intl", "int")) {
    "international"
  } else {
    "club"
  }

  # Construct segment ID
  segment_id <- paste(format_norm, gender_lower, team_type_norm, sep = "_")

  # Try to load from RDS file
  if (is.null(params_dir)) {
    params_dir <- "../bouncerdata/models"
  }

  params_file <- file.path(params_dir, paste0("projection_params_", segment_id, ".rds"))

  if (file.exists(params_file)) {
    params <- readRDS(params_file)
    return(params)
  }

  # Try to load from database
  if (!is.null(conn)) {
    query <- "SELECT param_a, param_b, param_z, param_y, eis_agnostic
       FROM projection_params
       WHERE segment_id = ?"

    result <- tryCatch(
      DBI::dbGetQuery(conn, query, params = list(segment_id)),
      error = function(e) NULL
    )

    if (!is.null(result) && nrow(result) > 0) {
      return(list(
        a = result$param_a[1],
        b = result$param_b[1],
        z = result$param_z[1],
        y = result$param_y[1],
        eis_agnostic = result$eis_agnostic[1]
      ))
    }
  }

  # Return defaults
  list(
    a = PROJ_DEFAULT_A,
    b = PROJ_DEFAULT_B,
    z = PROJ_DEFAULT_Z,
    y = PROJ_DEFAULT_Y,
    eis_agnostic = get_agnostic_expected_score(format_norm, gender_lower, team_type_norm)
  )
}


#' Calculate Projected Score
#'
#' Projects the final innings score from current game state using the
#' parameterized resource-based formula. Input matches scoreboard display.
#'
#' Formula:
#'   projected = cs + a * eis * resource_remaining + b * cs * resource_remaining / resource_used
#'
#' @param current_score Integer. Runs scored so far in the innings.
#' @param wickets Integer. Wickets fallen (0-10). Matches scoreboard: "120/3" means wickets=3.
#' @param overs Numeric. Overs bowled in cricket notation (e.g., 13.4 = 13 overs + 4 balls).
#'   Matches scoreboard: "(13.4 overs)" means overs=13.4.
#' @param format Character. Format: "t20", "odi", or "test".
#' @param expected_initial_score Numeric. Expected innings total at start (EIS).
#'   If NULL, uses agnostic expected score for the format.
#' @param params Named list. Optimized parameters (a, b, z, y).
#'   If NULL, loads default parameters.
#' @param gender Character. "male" or "female" (used for defaults).
#' @param team_type Character. "international" or "club" (used for defaults).
#' @param min_resource_used Numeric. Minimum resource_used to prevent division issues.
#' @param apply_bounds Logical. Whether to apply min/max bounds on projection.
#' @param max_balls Integer. Override max balls for format (used by Test match innings projection).
#'
#' @return Numeric. Projected final score.
#' @export
#'
#' @examples
#' # Scoreboard shows: "India 120/3 (13.4 overs)" in T20
#' calculate_projected_score(120, 3, 13.4, "t20")
#'
#' # With named parameters
#' calculate_projected_score(
#'   current_score = 80,
#'   wickets = 3,
#'   overs = 10.0,
#'   format = "t20"
#' )
#'
#' # With custom EIS (e.g., from full model)
#' calculate_projected_score(80, 3, 10.0, "t20", expected_initial_score = 175)
calculate_projected_score <- function(current_score,
                                      wickets,
                                      overs,
                                      format = "t20",
                                      expected_initial_score = NULL,
                                      params = NULL,
                                      gender = "male",
                                      team_type = "international",
                                      min_resource_used = NULL,
                                      apply_bounds = TRUE,
                                      max_balls = NULL) {

  format_lower <- tolower(format)

  # Input validation
  if (any(current_score < 0, na.rm = TRUE)) {
    cli::cli_warn("current_score contains negative values, setting to 0")
    current_score <- pmax(0, current_score)
  }

  # Validate wickets (0-10)
  if (any(wickets < 0 | wickets > 10, na.rm = TRUE)) {
    cli::cli_warn("wickets should be 0-10 (wickets fallen), clamping")
    wickets <- pmax(0, pmin(10, wickets))
  }

  # Convert wickets fallen to wickets remaining
  wickets_remaining <- 10 - wickets

  # Get max balls for format (override allows Test match innings-specific limits)
  max_balls_format <- if (!is.null(max_balls)) max_balls else get_max_balls(format)

  # Convert overs bowled (cricket notation) to balls remaining
  balls_bowled <- overs_to_balls(overs)
  balls_remaining <- max_balls_format - balls_bowled

  # Validate balls remaining
  if (any(balls_remaining < 0, na.rm = TRUE)) {
    cli::cli_warn("overs exceeds max for {format}, clamping")
    balls_remaining <- pmax(0, balls_remaining)
  }

  # Load parameters if not provided
  if (is.null(params)) {
    params <- load_projection_params(format, gender, team_type)
  }

  # Get expected initial score if not provided
  if (is.null(expected_initial_score)) {
    expected_initial_score <- params$eis_agnostic %||%
      get_agnostic_expected_score(format, gender, team_type)
  }

  # Extract parameters
  a <- params$a %||% PROJ_DEFAULT_A
  b <- params$b %||% PROJ_DEFAULT_B
  z <- params$z %||% PROJ_DEFAULT_Z
  y <- params$y %||% PROJ_DEFAULT_Y

  if (is.null(min_resource_used)) {
    min_resource_used <- PROJ_MIN_RESOURCE_USED
  }

  # Calculate resource remaining
  resource_remaining <- calculate_projection_resource(
    wickets_remaining = wickets_remaining,
    balls_remaining = balls_remaining,
    format = format,
    z = z,
    y = y
  )

  # Calculate resource used (with minimum to prevent division issues)
  resource_used <- pmax(1 - resource_remaining, min_resource_used)

  # Calculate balls bowled for early innings handling
  max_balls <- get_max_balls(format)
  balls_bowled <- max_balls - balls_remaining

  # Apply formula: projected = cs + a*eis*rr + b*cs*rr/ru
  eis_component <- a * expected_initial_score * resource_remaining
  current_rate_component <- b * current_score * resource_remaining / resource_used

  projected <- current_score + eis_component + current_rate_component

  # Early innings blending: for first over, blend with simple projection
  if (balls_bowled < PROJ_EARLY_INNINGS_BALLS && balls_bowled > 0) {
    simple_projection <- expected_initial_score
    blend_weight <- balls_bowled / PROJ_EARLY_INNINGS_BALLS
    projected <- blend_weight * projected + (1 - blend_weight) * simple_projection
  } else if (balls_bowled == 0) {
    # At very start, just return EIS
    projected <- expected_initial_score
  }

  # Apply bounds if requested
  if (apply_bounds) {
    bounds <- get_projection_bounds(format)

    # Projected score should be at least current score
    projected <- pmax(projected, current_score)
    # Apply format bounds
    projected <- pmax(bounds[1], pmin(bounds[2], projected))
  }

  return(projected)
}


#' Calculate Full Model Expected Score
#'
#' Predicts expected initial score (EIS) using team skills, venue skills,
#' and match context. Used for the "full" projection model.
#'
#' @param batting_team_skills Named list. Batting team's skill indices:
#'   - runs_skill: Team batting runs skill index
#'   - wicket_skill: Team batting wicket skill index (optional)
#' @param bowling_team_skills Named list. Bowling team's skill indices:
#'   - runs_skill: Team bowling runs skill index (conceding runs)
#'   - wicket_skill: Team bowling wicket skill index (optional)
#' @param venue_skills Named list. Venue skill indices (optional):
#'   - run_rate: Venue run rate deviation
#'   - wicket_rate: Venue wicket rate deviation (optional)
#' @param format Character. Format: "t20", "odi", or "test".
#' @param gender Character. "male" or "female".
#' @param team_type Character. "international" or "club".
#' @param innings Integer. 1 or 2 (second innings may adjust for target).
#' @param target Integer. Target to chase (innings 2 only, NULL for innings 1).
#'
#' @return Numeric. Predicted expected initial score.
#' @keywords internal
calculate_full_expected_score <- function(batting_team_skills = NULL,
                                          bowling_team_skills = NULL,
                                          venue_skills = NULL,
                                          format = "t20",
                                          gender = "male",
                                          team_type = "international",
                                          innings = 1,
                                          target = NULL) {

  # Start with agnostic baseline
  eis <- get_agnostic_expected_score(format, gender, team_type)

  # Get max balls for scaling using central helper
  max_balls <- get_max_balls(format)

  # Adjust for batting team skills
  # runs_skill is runs/ball deviation, so multiply by balls for total runs adjustment
  if (!is.null(batting_team_skills) && !is.null(batting_team_skills$runs_skill)) {
    batting_adjustment <- batting_team_skills$runs_skill * max_balls
    eis <- eis + batting_adjustment
  }

  # Adjust for bowling team skills
  # Positive bowling runs_skill = team concedes more (bad bowling), increases EIS
  if (!is.null(bowling_team_skills) && !is.null(bowling_team_skills$runs_skill)) {
    bowling_adjustment <- bowling_team_skills$runs_skill * max_balls
    eis <- eis + bowling_adjustment
  }

  # Adjust for venue
  if (!is.null(venue_skills) && !is.null(venue_skills$run_rate)) {
    venue_adjustment <- venue_skills$run_rate * max_balls
    eis <- eis + venue_adjustment
  }

  # Get bounds for format using central helper
  bounds <- get_projection_bounds(format)

  # Apply bounds
  eis <- pmax(bounds[1], pmin(bounds[2], eis))

  return(eis)
}


#' Calculate Projection Change from Delivery
#'
#' Calculates how a single delivery changes the projected score.
#' Used for player attribution (did the player increase/decrease projected score?).
#'
#' @param projected_before Numeric. Projected score before delivery.
#' @param projected_after Numeric. Projected score after delivery.
#' @param runs_scored Integer. Runs scored on delivery.
#' @param is_wicket Logical. Whether wicket fell on delivery.
#' @param expected_runs Numeric. Expected runs for this delivery context (optional).
#'
#' @return Named list with:
#'   - projection_change: Actual change in projection
#'   - runs_scored: Runs scored
#'   - is_wicket: Whether wicket fell
#'   - above_expectation: Whether change was better than expected (optional)
#' @keywords internal
calculate_projection_change <- function(projected_before,
                                        projected_after,
                                        runs_scored,
                                        is_wicket,
                                        expected_runs = NULL) {

  projection_change <- projected_after - projected_before

  result <- list(
    projection_change = projection_change,
    runs_scored = runs_scored,
    is_wicket = is_wicket
  )

  # If we have expected runs, calculate above/below expectation
  if (!is.null(expected_runs)) {
    # Expected change would be just adding expected runs (no resource change)
    # But actually a wicket has different impact than runs
    # For simplicity, compare projection change to runs scored
    # Positive projection change beyond runs = good resource preservation
    result$expected_change <- expected_runs
    result$above_expectation <- projection_change > expected_runs
  }

  return(result)
}


#' Vectorized Projected Score Calculation
#'
#' Calculates projected scores for multiple deliveries efficiently.
#' Used in parameter optimization and batch processing.
#'
#' @param current_score Integer vector. Runs scored so far for each delivery.
#' @param wickets_remaining Integer vector. Wickets in hand for each delivery.
#' @param balls_remaining Integer vector. Balls remaining for each delivery.
#' @param expected_initial_score Numeric vector or scalar. EIS for each delivery.
#' @param a Numeric. Parameter a.
#' @param b Numeric. Parameter b.
#' @param z Numeric. Parameter z (balls power).
#' @param y Numeric. Parameter y (wickets power).
#' @param max_balls Integer. Maximum balls in format.
#' @param min_resource_used Numeric. Minimum resource used.
#'
#' @return Numeric vector. Projected scores.
#' @keywords internal
calculate_projected_scores_vectorized <- function(current_score,
                                                  wickets_remaining,
                                                  balls_remaining,
                                                  expected_initial_score,
                                                  a,
                                                  b,
                                                  z,
                                                  y,
                                                  max_balls,
                                                  min_resource_used = 0.01) {

  # Ensure expected_initial_score matches vector length
  expected_initial_score <- rep_len(expected_initial_score, length(current_score))

  # Ensure valid inputs
  balls_remaining <- pmax(0, balls_remaining)
  wickets_remaining <- pmax(0, pmin(10, wickets_remaining))

  # Calculate resource
  balls_pct <- balls_remaining / max_balls
  wickets_pct <- wickets_remaining / 10

  resource_remaining <- (balls_pct ^ z) * (wickets_pct ^ y)

  # Handle edge cases
  resource_remaining <- ifelse(wickets_remaining == 0, 0, resource_remaining)
  resource_remaining <- ifelse(balls_remaining == 0, 0, resource_remaining)
  resource_remaining <- pmax(0, pmin(1, resource_remaining))

  # Resource used with minimum
  resource_used <- pmax(1 - resource_remaining, min_resource_used)

  # Calculate projection
  eis_component <- a * expected_initial_score * resource_remaining
  current_rate_component <- b * current_score * resource_remaining / resource_used

  projected <- current_score + eis_component + current_rate_component

  # Early innings handling (vectorized)
  balls_bowled <- max_balls - balls_remaining
  early_innings_mask <- balls_bowled < PROJ_EARLY_INNINGS_BALLS & balls_bowled > 0
  start_mask <- balls_bowled == 0

  if (any(early_innings_mask)) {
    blend_weight <- balls_bowled[early_innings_mask] / PROJ_EARLY_INNINGS_BALLS
    projected[early_innings_mask] <- blend_weight * projected[early_innings_mask] +
      (1 - blend_weight) * expected_initial_score[early_innings_mask]
  }

  if (any(start_mask)) {
    projected[start_mask] <- expected_initial_score[start_mask]
  }

  # Ensure projected >= current_score
  projected <- pmax(projected, current_score)

  return(projected)
}


#' Get Segment ID
#'
#' Constructs the segment identifier for projection parameters.
#'
#' @param format Character. Format: "t20", "odi", "test".
#' @param gender Character. "male" or "female".
#' @param team_type Character. "international" or "club".
#'
#' @return Character. Segment ID (e.g., "t20_male_international").
#' @keywords internal
get_projection_segment_id <- function(format, gender, team_type) {

  gender_lower <- tolower(gender)
  team_type_lower <- tolower(team_type)

  # Normalize format using central helper
  format_norm <- normalize_format(format)

  # Normalize team_type
  team_type_norm <- if (team_type_lower %in% c("international", "intl", "int")) {
    "international"
  } else {
    "club"
  }

  paste(format_norm, gender_lower, team_type_norm, sep = "_")
}


#' Save Projection Parameters
#'
#' Saves optimized projection parameters to RDS file.
#'
#' @param params Named list. Parameters to save (a, b, z, y, eis_agnostic).
#' @param format Character. Format: "t20", "odi", "test".
#' @param gender Character. "male" or "female".
#' @param team_type Character. "international" or "club".
#' @param params_dir Character. Directory to save to.
#' @param metrics Named list. Optional optimization metrics (train_rmse, etc.).
#'
#' @return Invisible NULL.
#' @keywords internal
save_projection_params <- function(params,
                                   format,
                                   gender,
                                   team_type,
                                   params_dir = "../bouncerdata/models",
                                   metrics = NULL) {

  segment_id <- get_projection_segment_id(format, gender, team_type)

  # Add metadata
  params$segment_id <- segment_id
  params$format <- format
  params$gender <- gender
  params$team_type <- team_type
  params$optimized_at <- Sys.time()

  if (!is.null(metrics)) {
    params$train_rmse <- metrics$train_rmse
    params$validation_rmse <- metrics$validation_rmse
    params$n_innings <- metrics$n_innings
  }

  # Ensure directory exists
  if (!dir.exists(params_dir)) {
    dir.create(params_dir, recursive = TRUE)
  }

  # Save
  params_file <- file.path(params_dir, paste0("projection_params_", segment_id, ".rds"))
  saveRDS(params, params_file)

  cli::cli_alert_success("Saved projection parameters to {.file {params_file}}")

  invisible(NULL)
}
