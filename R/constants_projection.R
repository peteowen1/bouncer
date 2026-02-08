# Score Projection Constants
#
# Constants for projecting final innings totals from any game state.
# Formula: projected = cs + a*eis*resource_remaining + b*cs*resource_remaining/resource_used
#
# Split from constants.R for better maintainability.

# ============================================================================
# SCORE PROJECTION CONSTANTS
# ============================================================================
# For projecting final innings totals from any game state.
# Formula: projected = cs + a*eis*resource_remaining + b*cs*resource_remaining/resource_used
# Where:
#   cs = current score
#   eis = expected initial score (format average or model-predicted)
#   resource_remaining = (balls_remaining/max_balls)^z * (wickets_remaining/10)^y
#   resource_used = 1 - resource_remaining

# Expected Initial Score (EIS) by format/gender/team_type
# Calibrated from actual data (January 2026 optimization)
EIS_T20_MALE_INTL <- 146.5     # Actual T20I first innings average
EIS_T20_MALE_CLUB <- 160.7     # Actual franchise T20 (IPL, BBL, etc.)
EIS_T20_FEMALE_INTL <- 115.2   # Actual WT20I first innings
EIS_T20_FEMALE_CLUB <- 136.0   # Actual women's franchise T20

EIS_ODI_MALE_INTL <- 238.7     # Actual ODI first innings
EIS_ODI_MALE_CLUB <- 253.8     # Actual domestic ODI
EIS_ODI_FEMALE_INTL <- 215.2   # Actual WODI first innings
EIS_ODI_FEMALE_CLUB <- 225.1   # Actual women's domestic ODI

EIS_TEST_MALE_INTL <- 342.4    # Actual Test first innings
EIS_TEST_MALE_CLUB <- 305.9    # Actual first-class innings
EIS_TEST_FEMALE_INTL <- 287.0  # Actual Women's Test innings
EIS_TEST_FEMALE_CLUB <- 250.0  # Estimated (insufficient data)

# Default projection parameters (before optimization)
# These are reasonable starting points for the formula
PROJ_DEFAULT_A <- 0.8          # Weight on EIS-based component
PROJ_DEFAULT_B <- 0.2          # Weight on current-rate-based component
PROJ_DEFAULT_Z <- 0.9          # Balls power (<1 = later balls more valuable)
PROJ_DEFAULT_Y <- 1.2          # Wickets power (>1 = early wickets more valuable)

# Minimum resource used to prevent division by zero
PROJ_MIN_RESOURCE_USED <- 0.01

# Maximum balls by format (used in resource calculation)
MAX_BALLS_T20 <- 120
MAX_BALLS_ODI <- 300
MAX_BALLS_TEST <- 540          # Approximate for a day's play (90 overs)

# Test match overs per day
TEST_OVERS_PER_DAY_5DAY <- 90
TEST_OVERS_PER_DAY_4DAY <- 98

# Early innings blending (first N balls use simpler projection)
PROJ_EARLY_INNINGS_BALLS <- 6  # Blend in first over

# Projection bounds (sanity checks)
PROJ_MIN_SCORE_T20 <- 60       # Minimum reasonable T20 projection
PROJ_MAX_SCORE_T20 <- 280      # Maximum reasonable T20 projection
PROJ_MIN_SCORE_ODI <- 100      # Minimum reasonable ODI projection
PROJ_MAX_SCORE_ODI <- 450      # Maximum reasonable ODI projection
PROJ_MIN_SCORE_TEST <- 80      # Minimum reasonable Test innings projection
PROJ_MAX_SCORE_TEST <- 700     # Maximum reasonable Test innings projection


# ============================================================================
# PHASE BOUNDARY CONSTANTS
# ============================================================================
# Defines innings phases for different formats.
# Used by outcome models and feature engineering.

# T20 phases (20 overs total)
PHASE_T20_POWERPLAY_END <- 6   # Overs 1-6: Powerplay
PHASE_T20_MIDDLE_END <- 16     # Overs 7-16: Middle overs
# Overs 17-20: Death overs

# ODI phases (50 overs total)
PHASE_ODI_POWERPLAY_END <- 10  # Overs 1-10: Powerplay
PHASE_ODI_MIDDLE_END <- 40     # Overs 11-40: Middle overs
# Overs 41-50: Death overs

# Test phases (unlimited overs)
PHASE_TEST_NEW_BALL_END <- 20  # Overs 1-20: New ball
PHASE_TEST_MIDDLE_END <- 80    # Overs 21-80: Middle overs
# Overs 81+: Old ball


#' Get Phase Boundaries for a Format
#'
#' Returns the over boundaries that define innings phases for a given format.
#' Phases help capture different tactical contexts (powerplay restrictions,
#' new ball movement, death overs pressure).
#'
#' @param format Character. Cricket format: "t20", "odi", or "test".
#'   Also accepts uppercase and full names like "T20I", "ODI", "Test".
#'
#' @return Named list with phase boundary overs:
#'   \itemize{
#'     \item For T20/ODI: \code{powerplay_end}, \code{middle_end}
#'     \item For Test: \code{new_ball_end}, \code{middle_end}
#'   }
#'
#' @examples
#' get_phase_boundaries("t20")
#' # Returns: list(powerplay_end = 6, middle_end = 16)
#'
#' get_phase_boundaries("test")
#' # Returns: list(new_ball_end = 20, middle_end = 80)
#'
#' @export
get_phase_boundaries <- function(format) {
  format <- tolower(format)

  # Normalize format names
  if (format %in% c("t20", "t20i", "it20", "t20s")) {
    list(
      powerplay_end = PHASE_T20_POWERPLAY_END,
      middle_end = PHASE_T20_MIDDLE_END
    )
  } else if (format %in% c("odi", "odis", "odm")) {
    list(
      powerplay_end = PHASE_ODI_POWERPLAY_END,
      middle_end = PHASE_ODI_MIDDLE_END
    )
  } else if (format %in% c("test", "tests", "mdm")) {
    list(
      new_ball_end = PHASE_TEST_NEW_BALL_END,
      middle_end = PHASE_TEST_MIDDLE_END
    )
  } else {
    cli::cli_warn("Unknown format '{format}', defaulting to T20 phase boundaries")
    list(
      powerplay_end = PHASE_T20_POWERPLAY_END,
      middle_end = PHASE_T20_MIDDLE_END
    )
  }
}


#' Get Projection Score Bounds
#'
#' Returns the minimum and maximum reasonable projected scores for a format.
#' Used to sanity-check projections.
#'
#' @param format Character. Cricket format: "t20", "odi", or "test".
#'
#' @return Numeric vector of length 2: c(min, max).
#'
#' @examples
#' get_projection_bounds("t20")   # c(60, 280)
#' get_projection_bounds("odi")   # c(100, 450)
#' get_projection_bounds("test")  # c(80, 700)
#'
#' @export
get_projection_bounds <- function(format) {
  canonical <- normalize_format(format)

  switch(canonical,
    "t20" = c(PROJ_MIN_SCORE_T20, PROJ_MAX_SCORE_T20),
    "odi" = c(PROJ_MIN_SCORE_ODI, PROJ_MAX_SCORE_ODI),
    "test" = c(PROJ_MIN_SCORE_TEST, PROJ_MAX_SCORE_TEST),
    c(60, 500)  # Default fallback
  )
}


#' Classify Over into Phase
#'
#' Determines the phase of an innings based on the current over and format.
#'
#' @param over Numeric. Current over number (0-indexed, so over 0 = first over).
#' @param format Character. Cricket format: "t20", "odi", or "test".
#'
#' @return Character. Phase name:
#'   \itemize{
#'     \item T20/ODI: "powerplay", "middle", or "death"
#'     \item Test: "new_ball", "middle", or "old_ball"
#'   }
#'
#' @examples
#' get_phase_from_over(3, "t20")   # "powerplay"
#' get_phase_from_over(10, "t20")  # "middle"
#' get_phase_from_over(18, "t20")  # "death"
#' get_phase_from_over(50, "test") # "middle"
#' get_phase_from_over(85, "test") # "old_ball"
#'
#' @export
get_phase_from_over <- function(over, format) {
  format <- tolower(format)
  bounds <- get_phase_boundaries(format)

  if (format %in% c("test", "tests", "mdm")) {
    # Test match phases
    if (over < bounds$new_ball_end) {
      "new_ball"
    } else if (over < bounds$middle_end) {
      "middle"
    } else {
      "old_ball"
    }
  } else {
    # Limited overs phases (T20, ODI)
    if (over < bounds$powerplay_end) {
      "powerplay"
    } else if (over < bounds$middle_end) {
      "middle"
    } else {
      "death"
    }
  }
}
