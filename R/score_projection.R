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

  format_lower <- tolower(format)

  # Get max balls for format
  if (is.null(max_balls)) {
    max_balls <- switch(format_lower,
      "t20" = MAX_BALLS_T20,
      "it20" = MAX_BALLS_T20,
      "odi" = MAX_BALLS_ODI,
      "odm" = MAX_BALLS_ODI,
      "test" = MAX_BALLS_TEST,
      "mdm" = MAX_BALLS_TEST,
      MAX_BALLS_T20  # Default
    )
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

  if (format_lower %in% c("t20", "it20", "odi", "odm")) {
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

  format_lower <- tolower(format)
  gender_lower <- tolower(gender)
  team_type_lower <- tolower(team_type)

  # Normalize format
  format_norm <- switch(format_lower,
    "t20" = "t20",
    "it20" = "t20",
    "odi" = "odi",
    "odm" = "odi",
    "test" = "test",
    "mdm" = "test",
    "t20"  # Default
  )

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

  format_lower <- tolower(format)
  gender_lower <- tolower(gender)
  team_type_lower <- tolower(team_type)

  # Normalize format
  format_norm <- switch(format_lower,
    "t20" = "t20", "it20" = "t20",
    "odi" = "odi", "odm" = "odi",
    "test" = "test", "mdm" = "test",
    "t20"
  )

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
    query <- sprintf(
      "SELECT param_a, param_b, param_z, param_y, eis_agnostic
       FROM projection_params
       WHERE segment_id = '%s'",
      segment_id
    )

    result <- tryCatch(
      DBI::dbGetQuery(conn, query),
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
                                      apply_bounds = TRUE) {

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

  # Get max balls for format
  max_balls_format <- switch(format_lower,
    "t20" = MAX_BALLS_T20, "it20" = MAX_BALLS_T20,
    "odi" = MAX_BALLS_ODI, "odm" = MAX_BALLS_ODI,
    "test" = MAX_BALLS_TEST, "mdm" = MAX_BALLS_TEST,
    MAX_BALLS_T20
  )

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
  max_balls <- switch(format_lower,
    "t20" = MAX_BALLS_T20, "it20" = MAX_BALLS_T20,
    "odi" = MAX_BALLS_ODI, "odm" = MAX_BALLS_ODI,
    "test" = MAX_BALLS_TEST, "mdm" = MAX_BALLS_TEST,
    MAX_BALLS_T20
  )
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
    bounds <- switch(format_lower,
      "t20" = c(PROJ_MIN_SCORE_T20, PROJ_MAX_SCORE_T20),
      "it20" = c(PROJ_MIN_SCORE_T20, PROJ_MAX_SCORE_T20),
      "odi" = c(PROJ_MIN_SCORE_ODI, PROJ_MAX_SCORE_ODI),
      "odm" = c(PROJ_MIN_SCORE_ODI, PROJ_MAX_SCORE_ODI),
      "test" = c(PROJ_MIN_SCORE_TEST, PROJ_MAX_SCORE_TEST),
      "mdm" = c(PROJ_MIN_SCORE_TEST, PROJ_MAX_SCORE_TEST),
      c(60, 500)  # Default
    )

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

  format_lower <- tolower(format)

  # Start with agnostic baseline
  eis <- get_agnostic_expected_score(format, gender, team_type)

  # Get max balls for scaling
  max_balls <- switch(format_lower,
    "t20" = MAX_BALLS_T20, "it20" = MAX_BALLS_T20,
    "odi" = MAX_BALLS_ODI, "odm" = MAX_BALLS_ODI,
    "test" = MAX_BALLS_TEST, "mdm" = MAX_BALLS_TEST,
    MAX_BALLS_T20
  )

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

  # Get bounds for format
  bounds <- switch(format_lower,
    "t20" = c(PROJ_MIN_SCORE_T20, PROJ_MAX_SCORE_T20),
    "it20" = c(PROJ_MIN_SCORE_T20, PROJ_MAX_SCORE_T20),
    "odi" = c(PROJ_MIN_SCORE_ODI, PROJ_MAX_SCORE_ODI),
    "odm" = c(PROJ_MIN_SCORE_ODI, PROJ_MAX_SCORE_ODI),
    "test" = c(PROJ_MIN_SCORE_TEST, PROJ_MAX_SCORE_TEST),
    "mdm" = c(PROJ_MIN_SCORE_TEST, PROJ_MAX_SCORE_TEST),
    c(60, 500)
  )

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

  format_lower <- tolower(format)
  gender_lower <- tolower(gender)
  team_type_lower <- tolower(team_type)

  # Normalize format
  format_norm <- switch(format_lower,
    "t20" = "t20", "it20" = "t20",
    "odi" = "odi", "odm" = "odi",
    "test" = "test", "mdm" = "test",
    "t20"
  )

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


# ============================================================================
# PARAMETER OPTIMIZATION FUNCTIONS
# ============================================================================

#' Load Training Data for Projection Optimization
#'
#' Loads deliveries with final innings totals for a specific segment.
#'
#' @param conn DBI connection to database.
#' @param format Character. Format: "t20", "odi", "test".
#' @param gender Character. "male" or "female".
#' @param team_type Character. "international" or "club".
#' @param sample_frac Numeric. Fraction of data to sample (for speed).
#'
#' @return Data frame with columns: current_score, balls_remaining, wickets_remaining,
#'   final_innings_total, match_id, innings.
#' @keywords internal
load_projection_training_data <- function(conn, format, gender, team_type,
                                          sample_frac = 1.0) {

  format_lower <- tolower(format)
  gender_lower <- tolower(gender)

 # Match types lowercase for LOWER() comparison
  match_types <- switch(format_lower,
    "t20" = c("t20", "it20"),
    "odi" = c("odi", "odm"),
    "test" = c("test", "mdm")
  )

  # Team type filter (handle NULL for club matches)
  if (team_type == "international") {
    team_type_filter <- "m.team_type = 'international'"
  } else {
    team_type_filter <- "(m.team_type IS NULL OR m.team_type != 'international')"
  }

  match_types_sql <- paste0("'", match_types, "'", collapse = ", ")

  # Get max balls for format
  max_balls <- switch(format_lower,
    "t20" = MAX_BALLS_T20,
    "odi" = MAX_BALLS_ODI,
    "test" = MAX_BALLS_TEST,
    MAX_BALLS_T20
  )

  # Query to get deliveries with final innings totals
  query <- sprintf("
    WITH innings_totals AS (
      SELECT
        match_id,
        innings,
        MAX(total_runs) as final_innings_total
      FROM deliveries
      WHERE LOWER(match_type) IN (%s)
        AND LOWER(gender) = '%s'
      GROUP BY match_id, innings
    )
    SELECT
      d.match_id,
      d.innings,
      d.total_runs as current_score,
      ((d.over * 6) + d.ball) as balls_bowled,
      %d - ((d.over * 6) + d.ball) as balls_remaining,
      (10 - d.wickets_fallen) as wickets_remaining,
      it.final_innings_total
    FROM deliveries d
    JOIN innings_totals it ON d.match_id = it.match_id AND d.innings = it.innings
    JOIN matches m ON d.match_id = m.match_id
    WHERE LOWER(d.match_type) IN (%s)
      AND LOWER(d.gender) = '%s'
      AND %s
      AND it.final_innings_total IS NOT NULL
    ORDER BY RANDOM()
  ", match_types_sql, gender_lower, max_balls, match_types_sql, gender_lower, team_type_filter)

  data <- DBI::dbGetQuery(conn, query)

  # Sample if requested
  if (sample_frac < 1.0 && nrow(data) > 1000) {
    n_sample <- ceiling(nrow(data) * sample_frac)
    data <- data[sample(nrow(data), n_sample), ]
  }

  # Ensure valid values
  data$balls_remaining <- pmax(0, data$balls_remaining)
  data$wickets_remaining <- pmax(0, pmin(10, data$wickets_remaining))

  return(data)
}


#' Calculate RMSE for Projection Parameters
#'
#' Objective function for parameter optimization.
#'
#' @param params Numeric vector c(a, b, z, y).
#' @param data Data frame with columns: current_score, balls_remaining,
#'   wickets_remaining, final_innings_total.
#' @param eis Numeric. Expected initial score.
#' @param max_balls Integer. Maximum balls in format.
#'
#' @return Numeric. RMSE.
#' @keywords internal
calculate_projection_rmse <- function(params, data, eis, max_balls) {
  a <- params[1]
  b <- params[2]
  z <- params[3]
  y <- params[4]

  # Calculate projections
  projected <- calculate_projected_scores_vectorized(
    current_score = data$current_score,
    wickets_remaining = data$wickets_remaining,
    balls_remaining = data$balls_remaining,
    expected_initial_score = rep(eis, nrow(data)),
    a = a, b = b, z = z, y = y,
    max_balls = max_balls
  )

  # Calculate RMSE
  errors <- projected - data$final_innings_total
  rmse <- sqrt(mean(errors^2, na.rm = TRUE))

  return(rmse)
}


#' Grid Search for Projection Parameters
#'
#' Searches parameter space for good starting values.
#'
#' @param data Data frame with training data.
#' @param eis Numeric. Expected initial score.
#' @param max_balls Integer. Maximum balls in format.
#' @param grid Named list with parameter ranges (a, b, z, y).
#'
#' @return Named list with best parameters and RMSE.
#' @keywords internal
grid_search_projection_params <- function(data, eis, max_balls, grid = NULL) {

  # Default grid
  if (is.null(grid)) {
    grid <- list(
      a = seq(0.5, 1.0, by = 0.1),
      b = seq(0.0, 0.5, by = 0.1),
      z = seq(0.7, 1.1, by = 0.1),
      y = seq(0.8, 1.4, by = 0.1)
    )
  }

  # Generate all combinations
  combinations <- expand.grid(grid)

  best_rmse <- Inf
  best_params <- NULL

  cli::cli_progress_bar("Grid search", total = nrow(combinations))

  for (i in seq_len(nrow(combinations))) {
    params <- as.numeric(combinations[i, ])

    rmse <- calculate_projection_rmse(params, data, eis, max_balls)

    if (rmse < best_rmse) {
      best_rmse <- rmse
      best_params <- params
    }

    cli::cli_progress_update()
  }

  cli::cli_progress_done()

  list(
    a = best_params[1],
    b = best_params[2],
    z = best_params[3],
    y = best_params[4],
    rmse = best_rmse
  )
}


#' Refine Projection Parameters with Nelder-Mead
#'
#' Fine-tunes parameters using optimization.
#'
#' @param data Data frame with training data.
#' @param eis Numeric. Expected initial score.
#' @param max_balls Integer. Maximum balls in format.
#' @param initial Named list with starting parameters.
#'
#' @return Named list with optimized parameters and RMSE.
#' @keywords internal
refine_projection_params <- function(data, eis, max_balls, initial) {

  # Objective function for optim
  objective <- function(params) {
    # Enforce constraints
    if (any(params <= 0)) return(1e10)
    if (params[1] + params[2] > 2) return(1e10)

    calculate_projection_rmse(params, data, eis, max_balls)
  }

  # Initial values
  start <- c(initial$a, initial$b, initial$z, initial$y)

  # Optimize
  result <- stats::optim(
    par = start,
    fn = objective,
    method = "Nelder-Mead",
    control = list(maxit = 500, trace = 0)
  )

  list(
    a = result$par[1],
    b = result$par[2],
    z = result$par[3],
    y = result$par[4],
    rmse = result$value,
    convergence = result$convergence
  )
}


#' Calculate Actual EIS from Database
#'
#' Computes the actual average first innings score for a segment.
#'
#' @param conn DBI connection.
#' @param format Character. Format: "t20", "odi", "test".
#' @param gender Character. "male" or "female".
#' @param team_type Character. "international" or "club".
#'
#' @return Numeric. Average first innings total.
#' @keywords internal
calculate_actual_eis <- function(conn, format, gender, team_type) {

  format_lower <- tolower(format)
  gender_lower <- tolower(gender)

  # Match types lowercase for LOWER() comparison
  match_types <- switch(format_lower,
    "t20" = c("t20", "it20"),
    "odi" = c("odi", "odm"),
    "test" = c("test", "mdm")
  )

  # Team type filter (handle NULL for club matches)
  if (team_type == "international") {
    team_type_filter <- "m.team_type = 'international'"
  } else {
    team_type_filter <- "(m.team_type IS NULL OR m.team_type != 'international')"
  }

  match_types_sql <- paste0("'", match_types, "'", collapse = ", ")

  query <- sprintf("
    SELECT AVG(mi.total_runs) as avg_score
    FROM match_innings mi
    JOIN matches m ON mi.match_id = m.match_id
    WHERE mi.innings = 1
      AND LOWER(m.match_type) IN (%s)
      AND LOWER(m.gender) = '%s'
      AND %s
      AND mi.total_runs IS NOT NULL
  ", match_types_sql, gender_lower, team_type_filter)

  result <- DBI::dbGetQuery(conn, query)

  if (nrow(result) == 0 || is.na(result$avg_score[1])) {
    # Fall back to default
    return(get_agnostic_expected_score(format, gender, team_type))
  }

  return(result$avg_score[1])
}


#' Optimize Projection Parameters for One Segment
#'
#' Runs full optimization (grid search + refinement) for a single segment.
#'
#' @param conn DBI connection.
#' @param format Character. Format: "t20", "odi", "test".
#' @param gender Character. "male" or "female".
#' @param team_type Character. "international" or "club".
#' @param sample_frac Numeric. Fraction of data to sample.
#' @param validation_split Numeric. Fraction for validation.
#' @param output_dir Character. Directory to save parameters.
#'
#' @return Named list with optimized parameters and metrics, or NULL if insufficient data.
#' @keywords internal
optimize_projection_segment <- function(conn, format, gender, team_type,
                                        sample_frac = 0.5,
                                        validation_split = 0.2,
                                        output_dir = "../bouncerdata/models") {

  segment_id <- get_projection_segment_id(format, gender, team_type)

  cli::cli_h2("Optimizing: {segment_id}")

  # Load data
  cli::cli_alert_info("Loading data...")
  data <- tryCatch(
    load_projection_training_data(conn, format, gender, team_type,
                                  sample_frac = sample_frac),
    error = function(e) {
      cli::cli_alert_warning("Error loading data: {e$message}")
      NULL
    }
  )

  if (is.null(data) || nrow(data) < 100) {
    cli::cli_alert_warning("Insufficient data for {segment_id}, skipping...")
    return(NULL)
  }

  cli::cli_alert_success("Loaded {nrow(data)} deliveries")

  # Split train/validation by match
  unique_matches <- unique(data$match_id)
  n_val <- ceiling(length(unique_matches) * validation_split)
  val_matches <- sample(unique_matches, n_val)

  train_data <- data[!data$match_id %in% val_matches, ]
  val_data <- data[data$match_id %in% val_matches, ]

  cli::cli_alert_info("Train: {nrow(train_data)} rows, Validation: {nrow(val_data)} rows")

  # Calculate actual EIS from data
  eis <- calculate_actual_eis(conn, format, gender, team_type)
  cli::cli_alert_info("Calculated EIS: {round(eis, 1)}")

  # Get max balls
  format_lower <- tolower(format)
  max_balls <- switch(format_lower,
    "t20" = MAX_BALLS_T20,
    "odi" = MAX_BALLS_ODI,
    "test" = MAX_BALLS_TEST,
    MAX_BALLS_T20
  )

  # Grid search
  cli::cli_alert_info("Running grid search...")
  grid_result <- grid_search_projection_params(train_data, eis, max_balls)
  cli::cli_alert_success("Grid search RMSE: {round(grid_result$rmse, 2)}")

  # Refine with Nelder-Mead
  cli::cli_alert_info("Refining with Nelder-Mead...")
  refined <- refine_projection_params(train_data, eis, max_balls, grid_result)
  cli::cli_alert_success("Refined RMSE: {round(refined$rmse, 2)}")

  # Validate
  val_rmse <- calculate_projection_rmse(
    c(refined$a, refined$b, refined$z, refined$y),
    val_data, eis, max_balls
  )
  cli::cli_alert_info("Validation RMSE: {round(val_rmse, 2)}")

  # Build result
  params <- list(
    a = refined$a,
    b = refined$b,
    z = refined$z,
    y = refined$y,
    eis_agnostic = eis
  )

  # Save parameters
  save_projection_params(
    params,
    format, gender, team_type,
    params_dir = output_dir,
    metrics = list(
      train_rmse = refined$rmse,
      validation_rmse = val_rmse,
      n_innings = length(unique_matches)
    )
  )

  # Return full result
  params$segment_id <- segment_id
  params$format <- format
  params$gender <- gender
  params$team_type <- team_type
  params$train_rmse <- refined$rmse
  params$validation_rmse <- val_rmse
  params$n_innings <- length(unique_matches)

  cli::cli_alert_info("  a={round(refined$a, 3)}, b={round(refined$b, 3)}, z={round(refined$z, 3)}, y={round(refined$y, 3)}")

  return(params)
}


#' Optimize All Projection Segments
#'
#' Runs optimization for all format x gender x team_type combinations.
#'
#' @param db_path Character. Path to database.
#' @param output_dir Character. Directory to save parameters.
#' @param sample_frac Numeric. Fraction of data to sample.
#' @param formats Character vector. Formats to optimize (default: all).
#' @param genders Character vector. Genders to optimize (default: all).
#' @param team_types Character vector. Team types to optimize (default: all).
#'
#' @return Named list of results for each segment.
#' @keywords internal
optimize_all_projection_segments <- function(db_path = "../bouncerdata/bouncer.duckdb",
                                             output_dir = "../bouncerdata/models",
                                             sample_frac = 0.5,
                                             formats = c("t20", "odi", "test"),
                                             genders = c("male", "female"),
                                             team_types = c("international", "club")) {

  # Generate segments
  segments <- expand.grid(
    format = formats,
    gender = genders,
    team_type = team_types,
    stringsAsFactors = FALSE
  )

  # Connect to database
  cli::cli_h1("Score Projection Parameter Optimization")
  cli::cli_alert_info("Connecting to database: {.file {db_path}}")

  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Ensure output directory exists
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  results <- list()

  for (i in seq_len(nrow(segments))) {
    segment <- segments[i, ]
    segment_id <- get_projection_segment_id(segment$format, segment$gender, segment$team_type)

    result <- optimize_projection_segment(
      conn = conn,
      format = segment$format,
      gender = segment$gender,
      team_type = segment$team_type,
      sample_frac = sample_frac,
      output_dir = output_dir
    )

    if (!is.null(result)) {
      results[[segment_id]] <- result
    }
  }

  # Summary
  cli::cli_h1("Optimization Complete")

  if (length(results) > 0) {
    summary_df <- do.call(rbind, lapply(results, function(r) {
      data.frame(
        segment = r$segment_id,
        a = round(r$a, 3),
        b = round(r$b, 3),
        z = round(r$z, 3),
        y = round(r$y, 3),
        eis = round(r$eis_agnostic, 1),
        train_rmse = round(r$train_rmse, 2),
        val_rmse = round(r$validation_rmse, 2)
      )
    }))

    print(summary_df)
  }

  invisible(results)
}


# ============================================================================
# PER-DELIVERY PROJECTION CALCULATION
# ============================================================================

#' Calculate Projections for All Deliveries
#'
#' Calculates and stores per-delivery score projections for a format.
#' Both agnostic (format average EIS) and full (team/venue adjusted) projections.
#'
#' @param conn DBI connection to database (write access required).
#' @param format Character. Format: "t20", "odi", or "test".
#' @param batch_size Integer. Number of deliveries to process per batch.
#' @param params_dir Character. Directory with parameter files.
#'
#' @return Invisible NULL.
#' @keywords internal
calculate_all_delivery_projections <- function(conn, format,
                                               batch_size = 50000,
                                               params_dir = "../bouncerdata/models") {

  format_lower <- tolower(format)

  # Match types for this format
  match_types <- switch(format_lower,
    "t20" = c("t20", "it20"),
    "odi" = c("odi", "odm"),
    "test" = c("test", "mdm")
  )

  match_types_sql <- paste0("'", match_types, "'", collapse = ", ")

  # Get max balls
  max_balls <- switch(format_lower,
    "t20" = MAX_BALLS_T20,
    "odi" = MAX_BALLS_ODI,
    "test" = MAX_BALLS_TEST
  )

  # Table name
  table_name <- paste0(format_lower, "_score_projection")

  cli::cli_h2("Calculating {format_lower} projections")

 # Create table if it doesn't exist
  create_table_sql <- sprintf("
    CREATE TABLE IF NOT EXISTS %s (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      innings INTEGER,
      batting_team_id VARCHAR,
      current_score INTEGER,
      balls_remaining INTEGER,
      wickets_remaining INTEGER,
      resource_remaining DOUBLE,
      resource_used DOUBLE,
      eis_agnostic DOUBLE,
      eis_full DOUBLE,
      projected_agnostic DOUBLE,
      projected_full DOUBLE,
      final_innings_total INTEGER,
      projection_change_agnostic DOUBLE,
      projection_change_full DOUBLE
    )
  ", table_name)

  DBI::dbExecute(conn, create_table_sql)

  # Count deliveries
  count_query <- sprintf("
    SELECT COUNT(*) as n
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    WHERE LOWER(d.match_type) IN (%s)
  ", match_types_sql)

  total_count <- DBI::dbGetQuery(conn, count_query)$n

  cli::cli_alert_info("Total deliveries to process: {total_count}")

  if (total_count == 0) {
    cli::cli_alert_warning("No deliveries found for {format_lower}")
    return(invisible(NULL))
  }

  # Clear existing projections
  cli::cli_alert_info("Clearing existing projections...")
  DBI::dbExecute(conn, sprintf("DELETE FROM %s", table_name))

  # Pre-load parameters for all segments
  cli::cli_alert_info("Loading optimized parameters...")
  params_cache <- list()
  for (gender in c("male", "female")) {
    for (team_type in c("international", "club")) {
      key <- paste(gender, team_type, sep = "_")
      params_cache[[key]] <- load_projection_params(
        format, gender, team_type, params_dir = params_dir
      )
      cli::cli_alert_success("  {key}: a={round(params_cache[[key]]$a, 3)}, b={round(params_cache[[key]]$b, 3)}, EIS={round(params_cache[[key]]$eis_agnostic, 1)}")
    }
  }

  # Process in batches
  n_batches <- ceiling(total_count / batch_size)
  rows_inserted <- 0
  start_time <- Sys.time()

  cli::cli_alert_info("Processing {n_batches} batches of {batch_size} deliveries each")

  for (batch_idx in seq_len(n_batches)) {
    offset <- (batch_idx - 1) * batch_size

    if (batch_idx <= 3 || batch_idx %% 10 == 0) {
      cli::cli_alert("Batch {batch_idx}/{n_batches}: querying rows {offset + 1} to {min(offset + batch_size, total_count)}...")
    }

    # Query batch of deliveries
    query <- sprintf("
      WITH innings_totals AS (
        SELECT
          match_id,
          innings,
          MAX(total_runs) as final_innings_total
        FROM deliveries
        WHERE LOWER(match_type) IN (%s)
        GROUP BY match_id, innings
      )
      SELECT
        d.delivery_id,
        d.match_id,
        m.match_date,
        d.innings,
        d.batting_team as batting_team_id,
        d.total_runs as current_score,
        %d - ((d.over * 6) + d.ball) as balls_remaining,
        (10 - d.wickets_fallen) as wickets_remaining,
        LOWER(m.gender) as gender,
        CASE WHEN m.team_type = 'international' THEN 'international' ELSE 'club' END as team_type,
        it.final_innings_total
      FROM deliveries d
      JOIN matches m ON d.match_id = m.match_id
      JOIN innings_totals it ON d.match_id = it.match_id AND d.innings = it.innings
      WHERE LOWER(d.match_type) IN (%s)
      ORDER BY m.match_date, d.match_id, d.delivery_id
      LIMIT %d OFFSET %d
    ", match_types_sql, max_balls, match_types_sql, batch_size, offset)

    batch_data <- DBI::dbGetQuery(conn, query)

    if (nrow(batch_data) == 0) {
      cli::cli_alert_warning("Batch {batch_idx} returned 0 rows, stopping")
      break
    }

    if (batch_idx <= 3) {
      cli::cli_alert_info("  Got {nrow(batch_data)} rows, calculating projections...")
    }

    # Calculate projections for each segment
    batch_data$projected_agnostic <- NA_real_
    batch_data$projected_full <- NA_real_
    batch_data$eis_agnostic <- NA_real_
    batch_data$resource_remaining <- NA_real_
    batch_data$resource_used <- NA_real_

    for (gender in c("male", "female")) {
      for (team_type in c("international", "club")) {
        key <- paste(gender, team_type, sep = "_")
        params <- params_cache[[key]]

        mask <- batch_data$gender == gender & batch_data$team_type == team_type

        if (sum(mask) > 0) {
          # Get EIS
          eis <- params$eis_agnostic %||%
            get_agnostic_expected_score(format, gender, team_type)

          # Calculate resource remaining
          rr <- calculate_projection_resource(
            wickets_remaining = batch_data$wickets_remaining[mask],
            balls_remaining = batch_data$balls_remaining[mask],
            format = format,
            z = params$z,
            y = params$y
          )

          ru <- pmax(1 - rr, PROJ_MIN_RESOURCE_USED)

          # Calculate agnostic projection
          proj_agnostic <- calculate_projected_scores_vectorized(
            current_score = batch_data$current_score[mask],
            wickets_remaining = batch_data$wickets_remaining[mask],
            balls_remaining = batch_data$balls_remaining[mask],
            expected_initial_score = rep(eis, sum(mask)),
            a = params$a, b = params$b, z = params$z, y = params$y,
            max_balls = max_balls
          )

          batch_data$projected_agnostic[mask] <- proj_agnostic
          batch_data$eis_agnostic[mask] <- eis
          batch_data$resource_remaining[mask] <- rr
          batch_data$resource_used[mask] <- ru

          # For full projection, would need team/venue skills
          # For now, just use agnostic
          batch_data$projected_full[mask] <- proj_agnostic
          batch_data$eis_full <- eis
        }
      }
    }

    # Calculate projection changes (requires ordering within innings)
    batch_data <- batch_data[order(batch_data$match_id, batch_data$innings, batch_data$delivery_id), ]

    batch_data$projection_change_agnostic <- NA_real_
    batch_data$projection_change_full <- NA_real_

    # Group by match_id + innings and calculate deltas
    for (mid in unique(batch_data$match_id)) {
      for (inn in unique(batch_data$innings[batch_data$match_id == mid])) {
        mask <- batch_data$match_id == mid & batch_data$innings == inn

        if (sum(mask) > 1) {
          projs <- batch_data$projected_agnostic[mask]
          changes <- c(NA, diff(projs))
          batch_data$projection_change_agnostic[mask] <- changes

          projs_full <- batch_data$projected_full[mask]
          changes_full <- c(NA, diff(projs_full))
          batch_data$projection_change_full[mask] <- changes_full
        }
      }
    }

    # Prepare for insertion
    insert_data <- data.frame(
      delivery_id = batch_data$delivery_id,
      match_id = batch_data$match_id,
      match_date = batch_data$match_date,
      innings = batch_data$innings,
      batting_team_id = batch_data$batting_team_id,
      current_score = batch_data$current_score,
      balls_remaining = batch_data$balls_remaining,
      wickets_remaining = batch_data$wickets_remaining,
      resource_remaining = round(batch_data$resource_remaining, 4),
      resource_used = round(batch_data$resource_used, 4),
      eis_agnostic = round(batch_data$eis_agnostic, 2),
      eis_full = round(batch_data$eis_full, 2),
      projected_agnostic = round(batch_data$projected_agnostic, 2),
      projected_full = round(batch_data$projected_full, 2),
      final_innings_total = batch_data$final_innings_total,
      projection_change_agnostic = round(batch_data$projection_change_agnostic, 3),
      projection_change_full = round(batch_data$projection_change_full, 3)
    )

    # Write to database
    DBI::dbWriteTable(conn, table_name, insert_data, append = TRUE, row.names = FALSE)
    rows_inserted <- rows_inserted + nrow(insert_data)

    if (batch_idx <= 3) {
      cli::cli_alert_success("  Batch {batch_idx} complete: inserted {nrow(insert_data)} rows")
    }

    # Show progress every 10 batches or on last batch
    if (batch_idx %% 10 == 0 || batch_idx == n_batches) {
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
      rate <- rows_inserted / elapsed
      remaining <- (total_count - rows_inserted) / rate
      cli::cli_alert_info("Progress: {rows_inserted}/{total_count} rows ({round(100 * rows_inserted/total_count, 1)}%) - {round(elapsed, 1)} min elapsed, ~{round(remaining, 1)} min remaining")
    }
  }

  # Get final count
  final_count <- DBI::dbGetQuery(
    conn,
    sprintf("SELECT COUNT(*) as n FROM %s", table_name)
  )$n

  elapsed_total <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
  cli::cli_alert_success("Stored {final_count} projections in {table_name} ({round(elapsed_total, 1)} minutes)")

  invisible(NULL)
}


#' Calculate Projections for All Formats
#'
#' Convenience function to calculate projections for T20, ODI, and Test formats.
#'
#' @param db_path Character. Path to database.
#' @param formats Character vector. Formats to process.
#' @param batch_size Integer. Deliveries per batch.
#' @param params_dir Character. Directory with parameter files.
#'
#' @return Invisible NULL.
#' @keywords internal
calculate_all_format_projections <- function(db_path = "../bouncerdata/bouncer.duckdb",
                                             formats = c("t20", "odi", "test"),
                                             batch_size = 50000,
                                             params_dir = "../bouncerdata/models") {

  cli::cli_h1("Score Projection Calculation")
  cli::cli_alert_info("Connecting to database: {.file {db_path}}")

  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  for (format in formats) {
    calculate_all_delivery_projections(
      conn = conn,
      format = format,
      batch_size = batch_size,
      params_dir = params_dir
    )
  }

  cli::cli_h1("Projection Calculation Complete")

  invisible(NULL)
}
