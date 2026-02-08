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
