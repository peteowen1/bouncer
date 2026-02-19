# Skill Index Constants
#
# Constants for skill index systems including:
# - Player skill indices (EMA-based scoring, economy, survival, strike)
# - Venue skill indices (run rate, wicket rate, boundary rate, dot rate)
# - Format mappings
#
# Split from constants.R for better maintainability.

# ============================================================================
# PLAYER SKILL INDEX CONSTANTS (EMA-based, drift-proof)
# ============================================================================

# EMA decay factor (alpha) - how much weight on new observation
# Higher alpha = faster adaptation, shorter memory
# alpha = 0.01 gives half-life of ~69 balls (ln(2)/0.01)
# alpha = 0.02 gives half-life of ~35 balls
# alpha = 0.005 gives half-life of ~139 balls
SKILL_ALPHA_T20 <- 0.01
SKILL_ALPHA_ODI <- 0.008
SKILL_ALPHA_TEST <- 0.005

# Starting values for skill indices
# INDICES (residual-based): Start at 0 = neutral deviation from expected
# RATES (absolute): Start at format average probability

# Scoring/Economy INDEX: deviation from context-expected runs/ball
# Neutral start = player performs as expected until we have data
SKILL_START_SCORING_INDEX <- 0    # Neutral scoring deviation
SKILL_START_ECONOMY_INDEX <- 0    # Neutral economy deviation

# Survival RATE: actual survival probability (absolute EMA)
SKILL_START_SURVIVAL_T20 <- 0.946   # 94.6% survival (5.4% wicket rate from data)
SKILL_START_SURVIVAL_ODI <- 0.972   # 97.2% survival (2.8% wicket rate from data)
SKILL_START_SURVIVAL_TEST <- 0.983  # 98.3% survival (1.7% wicket rate from data)

# Strike RATE: actual wicket probability (absolute EMA)
SKILL_START_STRIKE_T20 <- 0.054     # 5.4% wicket rate from T20 data
SKILL_START_STRIKE_ODI <- 0.028     # 2.8% wicket rate from ODI data
SKILL_START_STRIKE_TEST <- 0.017    # 1.7% wicket rate from Test data

# Expected values (fallback when agnostic model unavailable)
# These are the actual format averages, used for calculating residuals
EXPECTED_RUNS_T20 <- 1.138          # Actual T20 average runs/ball
EXPECTED_RUNS_ODI <- 0.782          # Actual ODI average runs/ball
EXPECTED_RUNS_TEST <- 0.518         # Actual Test average runs/ball
EXPECTED_WICKET_T20 <- 0.054        # Actual T20 wicket rate per ball
EXPECTED_WICKET_ODI <- 0.028        # Actual ODI wicket rate per ball
EXPECTED_WICKET_TEST <- 0.017       # Actual Test wicket rate per ball

# Minimum deliveries for reliable skill index
SKILL_MIN_BALLS_BATTER <- 30
SKILL_MIN_BALLS_BOWLER <- 60

# ============================================================================
# VENUE SKILL INDEX CONSTANTS (EMA-based venue characteristics)
# ============================================================================

# EMA decay factor - LOWER than player alpha (venues change slowly)
# alpha = 0.002 gives half-life of ~346 balls (~3 T20 matches)
# alpha = 0.001 gives half-life of ~693 balls (~2 ODI matches)
# alpha = 0.0005 gives half-life of ~1386 balls (~1 Test match)
VENUE_ALPHA_T20 <- 0.002
VENUE_ALPHA_ODI <- 0.001
VENUE_ALPHA_TEST <- 0.0005

# Starting values (calibrated from actual data - Dec 2025)
VENUE_START_RUN_RATE_T20 <- 1.138       # Actual: 1.138 runs/ball
VENUE_START_RUN_RATE_ODI <- 0.782       # Actual: 0.782 runs/ball
VENUE_START_RUN_RATE_TEST <- 0.518      # Actual: 0.518 runs/ball

VENUE_START_WICKET_RATE_T20 <- 0.0543   # Actual: 5.43% wicket rate per ball
VENUE_START_WICKET_RATE_ODI <- 0.028    # Actual: 2.8% wicket rate per ball
VENUE_START_WICKET_RATE_TEST <- 0.0173  # Actual: 1.73% wicket rate per ball

VENUE_START_BOUNDARY_RATE_T20 <- 0.139  # Actual: 13.9% boundaries in T20
VENUE_START_BOUNDARY_RATE_ODI <- 0.086  # Actual: 8.6% in ODI
VENUE_START_BOUNDARY_RATE_TEST <- 0.068 # Actual: 6.8% in Test

VENUE_START_DOT_RATE_T20 <- 0.38        # Actual: 38% dot balls in T20
VENUE_START_DOT_RATE_ODI <- 0.537       # Actual: 53.7% in ODI
VENUE_START_DOT_RATE_TEST <- 0.73       # Actual: 73% in Test

# Minimum balls for reliable venue index
VENUE_MIN_BALLS_T20 <- 500              # About 4-5 T20 matches
VENUE_MIN_BALLS_ODI <- 1000             # About 3-4 ODI matches
VENUE_MIN_BALLS_TEST <- 2000            # About 2 Test matches

# ============================================================================
# FORMAT MAPPINGS
# ============================================================================

# Match type mappings (for internal model format)
MATCH_TYPE_MAP <- c(
  "Test" = "test",
  "ODI" = "odi",
  "T20" = "t20",
  "T20I" = "t20",
  "MDM" = "test",  # Multi-day Match (first-class) - changed to test
  "IT20" = "t20",
  "ODM" = "odi"   # One-day Match (domestic)
)

# Format classification (internal model formats)
FORMAT_TEST <- c("test", "Test", "MDM")
FORMAT_ODI <- c("odi", "ODI", "ODM")
FORMAT_T20 <- c("t20", "T20", "T20I", "IT20")

# ============================================================================
# ADDITIVE SKILL INDEX SYSTEM CONSTANTS
# ============================================================================
# A unified skill system where skills are directly interpretable:
#   - Skill = deviation from baseline in natural units (runs, probability)
#   - Batter run skill of +0.30 means +0.30 runs/ball above expected
#   - Bowler wicket skill of +0.02 means +2% wicket probability above expected
#
# Two dimensions:
#   - Run skill: Deviation from expected runs per ball
#   - Wicket skill: Deviation from expected wicket probability per ball
#
# Key features:
#   - Start at 0 (neutral) instead of 1400 (arbitrary ELO)
#   - Continuous decay toward 0 (Bayesian prior to neutral)
#   - Experience-based alpha decay (like current K-factor system)
#   - Dual venue component (permanent + session)
#   - No conversion factor needed (directly interpretable)

# ============================================================================
# SKILL INDEX STARTING VALUES AND BOUNDS
# ============================================================================

# Starting skill value (neutral = 0 deviation from expected)
SKILL_INDEX_START <- 0

# Soft bounds to prevent extreme skill values
# Elite players typically fall within these ranges
# Clipping applied after each update to prevent drift
SKILL_INDEX_RUN_MAX <- 0.50      # +0.50 runs/ball (elite batter or terrible bowler)
SKILL_INDEX_RUN_MIN <- -0.50     # -0.50 runs/ball (struggling batter or elite bowler)
SKILL_INDEX_WICKET_MAX <- 0.05   # +5% wicket probability
SKILL_INDEX_WICKET_MIN <- -0.05  # -5% wicket probability

# ============================================================================
# LEARNING RATE (ALPHA) PARAMETERS
# ============================================================================
# Alpha = learning rate for skill updates (like K-factor in ELO)
# Formula: alpha = ALPHA_MIN + (ALPHA_MAX - ALPHA_MIN) * exp(-deliveries / ALPHA_HALFLIFE)
#
# New players (0 balls): alpha = ALPHA_MAX (learn fast from every delivery)
# Experienced (halflife balls): alpha = midpoint between max and min
# Veterans (many balls): alpha approaches ALPHA_MIN (stable ratings)

# Men's T20
SKILL_ALPHA_RUN_MAX_MENS_T20 <- 0.05      # New player learns fast
SKILL_ALPHA_RUN_MIN_MENS_T20 <- 0.01      # Experienced player is stable
SKILL_ALPHA_RUN_HALFLIFE_MENS_T20 <- 300  # ~25 T20 innings until halfway

SKILL_ALPHA_WICKET_MAX_MENS_T20 <- 0.04
SKILL_ALPHA_WICKET_MIN_MENS_T20 <- 0.008
SKILL_ALPHA_WICKET_HALFLIFE_MENS_T20 <- 350

# Men's ODI
SKILL_ALPHA_RUN_MAX_MENS_ODI <- 0.04
SKILL_ALPHA_RUN_MIN_MENS_ODI <- 0.008
SKILL_ALPHA_RUN_HALFLIFE_MENS_ODI <- 500  # ~10 ODI innings

SKILL_ALPHA_WICKET_MAX_MENS_ODI <- 0.03
SKILL_ALPHA_WICKET_MIN_MENS_ODI <- 0.006
SKILL_ALPHA_WICKET_HALFLIFE_MENS_ODI <- 600

# Men's Test
SKILL_ALPHA_RUN_MAX_MENS_TEST <- 0.03
SKILL_ALPHA_RUN_MIN_MENS_TEST <- 0.005
SKILL_ALPHA_RUN_HALFLIFE_MENS_TEST <- 800  # ~8 Test innings

SKILL_ALPHA_WICKET_MAX_MENS_TEST <- 0.025
SKILL_ALPHA_WICKET_MIN_MENS_TEST <- 0.004
SKILL_ALPHA_WICKET_HALFLIFE_MENS_TEST <- 900

# Women's T20
SKILL_ALPHA_RUN_MAX_WOMENS_T20 <- 0.06
SKILL_ALPHA_RUN_MIN_WOMENS_T20 <- 0.012
SKILL_ALPHA_RUN_HALFLIFE_WOMENS_T20 <- 250

SKILL_ALPHA_WICKET_MAX_WOMENS_T20 <- 0.05
SKILL_ALPHA_WICKET_MIN_WOMENS_T20 <- 0.010
SKILL_ALPHA_WICKET_HALFLIFE_WOMENS_T20 <- 300

# Women's ODI
SKILL_ALPHA_RUN_MAX_WOMENS_ODI <- 0.05
SKILL_ALPHA_RUN_MIN_WOMENS_ODI <- 0.010
SKILL_ALPHA_RUN_HALFLIFE_WOMENS_ODI <- 400

SKILL_ALPHA_WICKET_MAX_WOMENS_ODI <- 0.04
SKILL_ALPHA_WICKET_MIN_WOMENS_ODI <- 0.008
SKILL_ALPHA_WICKET_HALFLIFE_WOMENS_ODI <- 500

# Women's Test
SKILL_ALPHA_RUN_MAX_WOMENS_TEST <- 0.035
SKILL_ALPHA_RUN_MIN_WOMENS_TEST <- 0.006
SKILL_ALPHA_RUN_HALFLIFE_WOMENS_TEST <- 700

SKILL_ALPHA_WICKET_MAX_WOMENS_TEST <- 0.030
SKILL_ALPHA_WICKET_MIN_WOMENS_TEST <- 0.005
SKILL_ALPHA_WICKET_HALFLIFE_WOMENS_TEST <- 800

# ============================================================================
# CONTINUOUS DECAY (BAYESIAN PRIOR TOWARD NEUTRAL)
# ============================================================================
# Every delivery, all skills decay slightly toward 0 (neutral)
# This provides:
#   - Automatic mean reversion (prevents infinite drift)
#   - Implicit handling of inactive players (decay happens regardless)
#   - Bayesian interpretation: prior centered at 0
#
# Formula: new_skill = (1 - decay) * old_skill + alpha * residual
#
# After 1000 balls with no skill change:
#   skill × (1 - 0.0005)^1000 ≈ skill × 0.61 (39% decay)

SKILL_DECAY_PER_DELIVERY <- 0.0005    # ~0.05% pull toward 0 per ball

# Format-specific decay rates (longer formats = slower decay)
SKILL_DECAY_T20 <- 0.0005
SKILL_DECAY_ODI <- 0.0003
SKILL_DECAY_TEST <- 0.0002

# ============================================================================
# VENUE SKILL PARAMETERS
# ============================================================================
# Venues have both permanent (ground characteristics) and session (current conditions)

# Venue alpha (learning rate) - lower than players (venues change slowly)
SKILL_VENUE_ALPHA_PERM_T20 <- 0.002
SKILL_VENUE_ALPHA_PERM_ODI <- 0.001
SKILL_VENUE_ALPHA_PERM_TEST <- 0.0005

SKILL_VENUE_ALPHA_SESSION_MAX <- 0.05   # Session learns fast within match
SKILL_VENUE_ALPHA_SESSION_MIN <- 0.01
SKILL_VENUE_ALPHA_SESSION_HALFLIFE <- 150

# Venue decay rates (slower than players)
SKILL_VENUE_DECAY_PERM <- 0.0001   # Long-term venue characteristics
SKILL_VENUE_DECAY_SESSION <- 0.001 # Session resets anyway

# Venue skill bounds (tighter than players - venues are more stable)
SKILL_VENUE_RUN_MAX <- 0.30
SKILL_VENUE_RUN_MIN <- -0.30
SKILL_VENUE_WICKET_MAX <- 0.03
SKILL_VENUE_WICKET_MIN <- -0.03

# ============================================================================
# ATTRIBUTION WEIGHTS (Placeholder - to be optimized)
# ============================================================================
# How much each entity contributes to the combined skill effect
# Weights should sum to 1.0 for run skill, and separately for wicket skill
#
# These will be re-optimized after implementing the skill index system
# Initial values are approximations based on ELO weight patterns

# Run skill attribution weights (Men's T20)
SKILL_W_BATTER_RUN_MENS_T20 <- 0.55
SKILL_W_BOWLER_RUN_MENS_T20 <- 0.30
SKILL_W_VENUE_SESSION_RUN_MENS_T20 <- 0.12
SKILL_W_VENUE_PERM_RUN_MENS_T20 <- 0.03

# Wicket skill attribution weights (Men's T20)
SKILL_W_BATTER_WICKET_MENS_T20 <- 0.45
SKILL_W_BOWLER_WICKET_MENS_T20 <- 0.25
SKILL_W_VENUE_SESSION_WICKET_MENS_T20 <- 0.24
SKILL_W_VENUE_PERM_WICKET_MENS_T20 <- 0.06

# Run skill attribution weights (Men's ODI)
SKILL_W_BATTER_RUN_MENS_ODI <- 0.52
SKILL_W_BOWLER_RUN_MENS_ODI <- 0.28
SKILL_W_VENUE_SESSION_RUN_MENS_ODI <- 0.16
SKILL_W_VENUE_PERM_RUN_MENS_ODI <- 0.04

# Wicket skill attribution weights (Men's ODI)
SKILL_W_BATTER_WICKET_MENS_ODI <- 0.50
SKILL_W_BOWLER_WICKET_MENS_ODI <- 0.32
SKILL_W_VENUE_SESSION_WICKET_MENS_ODI <- 0.14
SKILL_W_VENUE_PERM_WICKET_MENS_ODI <- 0.04

# Run skill attribution weights (Men's Test)
SKILL_W_BATTER_RUN_MENS_TEST <- 0.55
SKILL_W_BOWLER_RUN_MENS_TEST <- 0.30
SKILL_W_VENUE_SESSION_RUN_MENS_TEST <- 0.12
SKILL_W_VENUE_PERM_RUN_MENS_TEST <- 0.03

# Wicket skill attribution weights (Men's Test)
SKILL_W_BATTER_WICKET_MENS_TEST <- 0.52
SKILL_W_BOWLER_WICKET_MENS_TEST <- 0.32
SKILL_W_VENUE_SESSION_WICKET_MENS_TEST <- 0.13
SKILL_W_VENUE_PERM_WICKET_MENS_TEST <- 0.03

# Women's formats (placeholder - same as men's until optimized)
SKILL_W_BATTER_RUN_WOMENS_T20 <- 0.50
SKILL_W_BOWLER_RUN_WOMENS_T20 <- 0.20
SKILL_W_VENUE_SESSION_RUN_WOMENS_T20 <- 0.24
SKILL_W_VENUE_PERM_RUN_WOMENS_T20 <- 0.06

SKILL_W_BATTER_WICKET_WOMENS_T20 <- 0.55
SKILL_W_BOWLER_WICKET_WOMENS_T20 <- 0.30
SKILL_W_VENUE_SESSION_WICKET_WOMENS_T20 <- 0.12
SKILL_W_VENUE_PERM_WICKET_WOMENS_T20 <- 0.03

SKILL_W_BATTER_RUN_WOMENS_ODI <- 0.52
SKILL_W_BOWLER_RUN_WOMENS_ODI <- 0.25
SKILL_W_VENUE_SESSION_RUN_WOMENS_ODI <- 0.18
SKILL_W_VENUE_PERM_RUN_WOMENS_ODI <- 0.05

SKILL_W_BATTER_WICKET_WOMENS_ODI <- 0.54
SKILL_W_BOWLER_WICKET_WOMENS_ODI <- 0.15
SKILL_W_VENUE_SESSION_WICKET_WOMENS_ODI <- 0.25
SKILL_W_VENUE_PERM_WICKET_WOMENS_ODI <- 0.06

SKILL_W_BATTER_RUN_WOMENS_TEST <- 0.50
SKILL_W_BOWLER_RUN_WOMENS_TEST <- 0.32
SKILL_W_VENUE_SESSION_RUN_WOMENS_TEST <- 0.14
SKILL_W_VENUE_PERM_RUN_WOMENS_TEST <- 0.04

SKILL_W_BATTER_WICKET_WOMENS_TEST <- 0.52
SKILL_W_BOWLER_WICKET_WOMENS_TEST <- 0.32
SKILL_W_VENUE_SESSION_WICKET_WOMENS_TEST <- 0.13
SKILL_W_VENUE_PERM_WICKET_WOMENS_TEST <- 0.03

# Legacy aliases for backward compatibility (point to Men's T20)
SKILL_W_BATTER_RUN <- SKILL_W_BATTER_RUN_MENS_T20
SKILL_W_BOWLER_RUN <- SKILL_W_BOWLER_RUN_MENS_T20
SKILL_W_VENUE_SESSION_RUN <- SKILL_W_VENUE_SESSION_RUN_MENS_T20
SKILL_W_VENUE_PERM_RUN <- SKILL_W_VENUE_PERM_RUN_MENS_T20

SKILL_W_BATTER_WICKET <- SKILL_W_BATTER_WICKET_MENS_T20
SKILL_W_BOWLER_WICKET <- SKILL_W_BOWLER_WICKET_MENS_T20
SKILL_W_VENUE_SESSION_WICKET <- SKILL_W_VENUE_SESSION_WICKET_MENS_T20
SKILL_W_VENUE_PERM_WICKET <- SKILL_W_VENUE_PERM_WICKET_MENS_T20

# ============================================================================
# EXPECTED SKILL RANGES BY ENTITY TYPE (Reference values for validation)
# ============================================================================
# These are approximate 5th and 95th percentile expectations for skilled players
# Used for sanity-checking recovered skills in simulation

# Run skill (runs per ball deviation from expected)
SKILL_BATTER_RUN_ELITE <- 0.30      # 95th percentile (e.g., Kohli, Buttler)
SKILL_BATTER_RUN_POOR <- -0.30      # 5th percentile (tailenders)
SKILL_BOWLER_RUN_ELITE <- 0.20      # 95th percentile (restricts runs - Bumrah)
SKILL_BOWLER_RUN_POOR <- -0.20      # 5th percentile (leaks runs)
SKILL_VENUE_RUN_HIGH <- 0.15        # High-scoring venue
SKILL_VENUE_RUN_LOW <- -0.15        # Low-scoring venue

# Wicket skill (probability deviation from expected)
SKILL_BATTER_WICKET_ELITE <- -0.02  # 95th percentile (survives more - negative!)
SKILL_BATTER_WICKET_POOR <- 0.03    # 5th percentile (gets out more)
SKILL_BOWLER_WICKET_ELITE <- 0.02   # 95th percentile (takes wickets)
SKILL_BOWLER_WICKET_POOR <- -0.02   # 5th percentile (poor strike rate)
SKILL_VENUE_WICKET_HIGH <- 0.01     # Bowler-friendly venue
SKILL_VENUE_WICKET_LOW <- -0.01     # Batter-friendly venue

# ============================================================================
# PARAMETER LOOKUP HELPERS (Skill Index Version)
# ============================================================================

#' Get Skill Index Alpha (Learning Rate) Parameters
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @param skill_type "run" or "wicket"
#' @return Named list with alpha_max, alpha_min, halflife
#'
#' @examples
#' get_skill_alpha_params("t20")
#' get_skill_alpha_params("odi", skill_type = "wicket")
#'
#' @export
get_skill_alpha_params <- function(format, gender = "male", skill_type = "run") {
  suffix <- get_format_gender_suffix(format, gender)
  skill_upper <- toupper(skill_type)

  list(
    alpha_max = get(paste0("SKILL_ALPHA_", skill_upper, "_MAX_", suffix)),
    alpha_min = get(paste0("SKILL_ALPHA_", skill_upper, "_MIN_", suffix)),
    halflife = get(paste0("SKILL_ALPHA_", skill_upper, "_HALFLIFE_", suffix))
  )
}

#' Get Skill Index Attribution Weights
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @param skill_type "run" or "wicket"
#' @return Named list with w_batter, w_bowler, w_venue_session, w_venue_perm
#'
#' @examples
#' get_skill_weights("t20")
#' get_skill_weights("test", skill_type = "wicket")
#'
#' @export
get_skill_weights <- function(format, gender = "male", skill_type = "run") {
  suffix <- get_format_gender_suffix(format, gender)
  skill_upper <- toupper(skill_type)

  list(
    w_batter = get(paste0("SKILL_W_BATTER_", skill_upper, "_", suffix)),
    w_bowler = get(paste0("SKILL_W_BOWLER_", skill_upper, "_", suffix)),
    w_venue_session = get(paste0("SKILL_W_VENUE_SESSION_", skill_upper, "_", suffix)),
    w_venue_perm = get(paste0("SKILL_W_VENUE_PERM_", skill_upper, "_", suffix))
  )
}

#' Get Skill Decay Rate for Format
#'
#' @param format Format (T20, ODI, TEST)
#' @return Numeric decay rate per delivery
#'
#' @examples
#' get_skill_decay("t20")
#' get_skill_decay("test")
#'
#' @export
get_skill_decay <- function(format) {
  format_clean <- toupper(format)
  if (format_clean %in% c("IT20", "T20I", "WT20I", "T20")) {
    return(SKILL_DECAY_T20)
  } else if (format_clean %in% c("ODI", "WODI", "ODM")) {
    return(SKILL_DECAY_ODI)
  } else {
    return(SKILL_DECAY_TEST)
  }
}


# ============================================================================
# SCORE PROJECTION CONSTANTS (from constants_projection.R)
# ============================================================================

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
#' \dontrun{
#' get_phase_boundaries("t20")
#' # Returns: list(powerplay_end = 6, middle_end = 16)
#'
#' get_phase_boundaries("test")
#' # Returns: list(new_ball_end = 20, middle_end = 80)
#' }
#'
#' @keywords internal
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
#' \dontrun{
#' get_projection_bounds("t20")   # c(60, 280)
#' get_projection_bounds("odi")   # c(100, 450)
#' get_projection_bounds("test")  # c(80, 700)
#' }
#'
#' @keywords internal
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
#' \dontrun{
#' get_phase_from_over(3, "t20")   # "powerplay"
#' get_phase_from_over(10, "t20")  # "middle"
#' get_phase_from_over(18, "t20")  # "death"
#' get_phase_from_over(50, "test") # "middle"
#' get_phase_from_over(85, "test") # "old_ball"
#' }
#'
#' @keywords internal
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


# ============================================================================
# NETWORK CENTRALITY CONSTANTS (from constants_centrality.R)
# ============================================================================

# Quality tier percentile thresholds for centrality/PageRank classification
QUALITY_TIER_ELITE <- 95
QUALITY_TIER_VERY_GOOD <- 80
QUALITY_TIER_ABOVE_AVERAGE <- 60
QUALITY_TIER_AVERAGE <- 40
QUALITY_TIER_BELOW_AVERAGE <- 20

CENTRALITY_MEAN_TYPE <- "arithmetic"

# Alpha parameter: controls balance between breadth and quality
# - alpha = 0: pure degree centrality (just count unique opponents)
# - alpha = 0.5: balanced (default for geometric)
# - alpha = 0.8: quality-weighted (default for arithmetic) [RECOMMENDED]
# - alpha = 1: pure neighbor degree (only opponent quality matters)
CENTRALITY_ALPHA <- 0.8

# Minimum deliveries to include player in network analysis
# Feb 2026: Lowered to 1 to include ALL players - low-ball players
# benefit most from centrality-based K adjustments and regression
CENTRALITY_MIN_DELIVERIES <- 1

# ============================================================================
# MARGIN OF VICTORY CONSTANTS
# ============================================================================
# For converting wickets-wins to runs-equivalent margin and for
# margin-adjusted ELO updates (FiveThirtyEight methodology).

# Resource calculation: balls worth per wicket remaining
# Each wicket in hand represents additional scoring potential
# These values calibrated from historical scoring patterns
RESOURCE_WICKET_VALUE_T20 <- 7    # ~7 balls of scoring per wicket in T20
RESOURCE_WICKET_VALUE_ODI <- 9    # ~9 balls per wicket in ODI
RESOURCE_WICKET_VALUE_TEST <- 12  # ~12 balls per wicket in Test

# Margin multiplier (G-factor) for ELO updates
# Formula: G = (abs(margin) + MOV_BASE_OFFSET)^MOV_EXPONENT /
#              (MOV_DENOMINATOR_BASE + MOV_ELO_FACTOR * elo_diff)
# Based on FiveThirtyEight NBA/NFL methodology
MOV_EXPONENT <- 0.7              # How fast G grows with margin (sublinear)
MOV_BASE_OFFSET <- 5             # Prevents G=0 for very close games
MOV_DENOMINATOR_BASE <- 10       # Base denominator
MOV_ELO_FACTOR <- 0.004          # Adjusts for favorite bias
MOV_MIN <- 0.5                   # Floor: close games still matter
MOV_MAX <- 2.5                   # Cap: prevent extreme ELO swings

# Expected margin from ELO difference
# Formula: expected_margin = elo_diff / MARGIN_ELO_DIVISOR
# 100 ELO points difference = this many runs expected margin
MARGIN_ELO_DIVISOR_T20 <- 15     # 100 ELO diff ~ 6-7 runs margin
MARGIN_ELO_DIVISOR_ODI <- 12     # 100 ELO diff ~ 8 runs margin
MARGIN_ELO_DIVISOR_TEST <- 10    # 100 ELO diff ~ 10 runs margin

# Draw handling (Test matches)
DRAW_ACTUAL_SCORE <- 0.5         # Treat draw as 0.5 result (like chess)
DRAW_MARGIN <- 0                 # Draws have zero margin
DRAW_MOV_MULTIPLIER <- 1.0       # No margin adjustment for draws

# ============================================================================
# CENTRALITY INTEGRATION CONSTANTS (ELO + Centrality Two-Pronged Defense)
# ============================================================================
# Integrates network centrality quality scores into the 3-Way ELO system to combat
# isolated cluster inflation. Two complementary mechanisms:
#   Option A: Centrality-weighted K-factors (preventive - reduces new inflation)
#   Option D: Periodic centrality correction (corrective - fixes existing inflation)
#
# See data-raw/ratings/player/00_compute_centrality_snapshots.R for snapshot generation.

# -----------------------------------------------------------------------------
# SNAPSHOT GENERATION PARAMETERS
# -----------------------------------------------------------------------------
# Centrality must be computed from data BEFORE each match to prevent data leakage.
# We store dated snapshots and look up the most recent one before each match date.

CENTRALITY_SNAPSHOT_INTERVAL_GAMES <- 50   # Games between snapshots (per format)
CENTRALITY_SNAPSHOT_KEEP_MONTHS <- 999     # Keep ALL history (needed for ELO recalculation from 2005)

# -----------------------------------------------------------------------------
# K-FACTOR MODULATION (Option A: Preventive)
# -----------------------------------------------------------------------------
# Scale K-factor by opponent's centrality percentile.
# Learn more from elite opponents, less from weak ones.
#
# Formula: cent_factor = FLOOR + (CEILING - FLOOR) / (1 + exp(-STEEPNESS * (percentile - MIDPOINT)))
#
# Examples:
#   - Facing 99th percentile opponent (Kohli, Narine) -> K x 1.5
#   - Facing 50th percentile opponent (average)       -> K x 1.0
#   - Facing 10th percentile opponent (weak league)   -> K x 0.5

CENTRALITY_K_FLOOR <- 0.5           # Min K multiplier for low centrality opponents
CENTRALITY_K_CEILING <- 1.5         # Max K multiplier for high centrality opponents
CENTRALITY_K_MIDPOINT <- 50         # Percentile for neutral K (1.0x)
CENTRALITY_K_STEEPNESS <- 0.08      # Sigmoid steepness (higher = sharper transition)

# -----------------------------------------------------------------------------
# PERIODIC CORRECTION (Option D: Corrective)
# -----------------------------------------------------------------------------
# After each delivery, apply small correction toward centrality-implied ELO.
# This gradually pulls inflated ratings back to globally-informed levels.
#
# Formula:
#   cent_implied_elo = THREE_WAY_ELO_START + (centrality_percentile - 50) * ELO_PER_PERCENTILE
#   elo_cent_gap = raw_elo - cent_implied_elo
#   correction = CORRECTION_RATE * sign(gap) * sqrt(|gap|)
#   corrected_elo = raw_elo - correction
#
# Examples:
#   - 1800 ELO, 30th percentile centrality -> implied ELO = 1320, correction toward 1320
#   - 1500 ELO, 50th percentile centrality -> implied ELO = 1400, minimal correction

CENTRALITY_CORRECTION_RATE <- 0.01  # Base correction per delivery (scales with gap)
CENTRALITY_ELO_PER_PERCENTILE <- 6  # ELO points per percentile (50th = start, +/-300 range)

# -----------------------------------------------------------------------------
# CONTINUOUS REGRESSION TO CENTRALITY-IMPLIED ELO (Stronger Bayesian Prior)
# -----------------------------------------------------------------------------
# After each delivery, applies a small "gravity" pull toward the player's
# centrality-implied ELO. This prevents low-centrality players from accumulating
# inflated ratings by dominating weak opponents in isolated ecosystems.
#
# Formula: correction = REGRESSION_STRENGTH * (implied_elo - current_elo)
# Where:   implied_elo = ELO_START + (centrality - 50) * ELO_PER_PERCENTILE
#
# Effect examples (with REGRESSION_STRENGTH = 0.005, ELO_PER_PERCENTILE = 6):
#   - Player at 2400 ELO with 5% centrality:
#     implied_elo = 1400 + (5-50)*6 = 1130
#     correction = 0.005 * (1130 - 2400) = -6.35 per delivery
#     Over 300 balls: ~1900 point regression toward implied level
#
#   - Player at 1800 ELO with 90% centrality:
#     implied_elo = 1400 + (90-50)*6 = 1640
#     correction = 0.005 * (1640 - 1800) = -0.80 per delivery
#     Over 300 balls: ~240 point regression (much smaller since closer to implied)

CENTRALITY_REGRESSION_STRENGTH <- 0.005  # Pull strength per delivery toward implied ELO

# -----------------------------------------------------------------------------
# LEAGUE-ADJUSTED BASELINE (Environment-Aware Expected Runs)
# -----------------------------------------------------------------------------
# Instead of using a single global average for expected runs, blend the league's
# historical average with the global average based on sample size.
#
# Formula: expected_baseline = w * league_avg + (1-w) * global_avg
# Where:   w = league_deliveries / (league_deliveries + HALFLIFE)
#
# This prevents ELO inflation in high-scoring leagues (IPL ~1.35 runs/ball)
# and unfair penalties in low-scoring leagues (qualifiers ~1.05 runs/ball).
#
# Examples (with HALFLIFE = 5000):
#   - New league (500 deliveries): 9% league weight -> mostly global average
#   - Established league (5000 deliveries): 50% league weight -> balanced blend
#   - Mature league (20000 deliveries): 80% league weight -> mostly league average

LEAGUE_BASELINE_BLEND_HALFLIFE <- 5000  # Deliveries until 50% weight on league average

# -----------------------------------------------------------------------------
# COLD START DEFAULTS (New Players Without Centrality History)
# -----------------------------------------------------------------------------
# When a player has no centrality snapshot before their match date, use a
# tier-based default percentile. Higher tiers = higher starting percentile
# because debuts in elite events suggest the player is likely decent.

CENTRALITY_COLD_START_TIER_1 <- 60  # IPL/BBL/ICC debut -> 60th percentile
CENTRALITY_COLD_START_TIER_2 <- 50  # Strong domestic     -> 50th percentile
CENTRALITY_COLD_START_TIER_3 <- 40  # Regional/qualifier  -> 40th percentile
CENTRALITY_COLD_START_TIER_4 <- 30  # Development         -> 30th percentile
