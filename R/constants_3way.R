# 3-Way ELO System Constants
#
# Constants for the 3-Way ELO rating system:
# - Batter + Bowler + Venue with dual session/permanent venue
# - Format-gender specific parameters (optimized Feb 2026)
#
# Split from constants.R for better maintainability.
# Format-gender parameters consolidated into THREE_WAY_PARAMS list (Mar 2026).

# ============================================================================
# PREDICTION CALCULATION CONSTANTS
# ============================================================================

# Placeholder for "effectively infinite" values in features
# Used when calculations would produce Inf (e.g., runs_needed = 0)
INF_FEATURE_PLACEHOLDER <- 999

# Probability bounds for win probability calculations
# Prevents extreme probabilities that cause numerical issues
MIN_WIN_PROBABILITY <- 0.01
MAX_WIN_PROBABILITY <- 0.99

# Maximum possible required run rate (6 runs per ball)
MAX_REQUIRED_RUN_RATE <- 36

# ============================================================================
# 3-WAY ELO SYSTEM CONSTANTS (Batter + Bowler + Venue)
# ============================================================================
# A unified ELO system where Batter, Bowler, and Venue all participate in
# rating updates based on delivery outcomes.
#
# Two dimensions:
#   - Run ELO: Expected runs vs actual runs (continuous scale 0-1)
#   - Wicket ELO: Expected wicket probability vs actual wicket (binary 0/1)
#
# Expected values use agnostic model baseline + ELO-based adjustments.

# Starting/Target ELO (optimized via IPL Poisson grid search, Jan 2026)
THREE_WAY_ELO_START <- 1400
THREE_WAY_ELO_TARGET_MEAN <- 1500
THREE_WAY_ELO_DIVISOR <- 400
THREE_WAY_REPLACEMENT_LEVEL <- 1500  # Players/venues decay towards this when inactive

# ============================================================================
# ELO BOUNDS (Feb 2026 - Prevent Extreme Drift)
# ============================================================================
# Hard bounds to prevent extreme ELO values that produce unrealistic predictions.
# Venue bounds are tighter since venue characteristics should be more stable.
#
# Player bounds: +/-400 from start (1000-1800)
# Venue bounds: +/-200 from start (1200-1600) - more conservative
#
# These bounds are applied AFTER each ELO update to prevent drift.

THREE_WAY_PLAYER_ELO_MIN <- 1000  # Hard floor for player ELOs
THREE_WAY_PLAYER_ELO_MAX <- 1800  # Hard ceiling for player ELOs
THREE_WAY_VENUE_ELO_MIN <- 1200   # Tighter floor for venue ELOs
THREE_WAY_VENUE_ELO_MAX <- 1600   # Tighter ceiling for venue ELOs

# Whether to apply bounds (set FALSE to compare with unbounded)
THREE_WAY_APPLY_BOUNDS <- TRUE

# ============================================================================
# LOGISTIC ELO CONVERSION (Feb 2026 - Alternative to Linear)
# ============================================================================
# The standard linear conversion can produce unrealistic predictions at extreme ELOs.
# Logistic conversion naturally bounds the output range.
#
# Linear:   expected = baseline + elo_diff * scale (can go negative/infinite)
# Logistic: expected = baseline * 2 / (1 + exp(-elo_diff / sensitivity))
#           Range: [0, 2*baseline] regardless of elo_diff
#
# Set USE_LOGISTIC_CONVERSION = TRUE to use logistic instead of linear.

THREE_WAY_USE_LOGISTIC_CONVERSION <- FALSE  # Default: use original linear

# Logistic sensitivity: ELO difference for ~76% of max effect
# Higher = more gradual transition, lower = more sensitive to ELO differences
THREE_WAY_LOGISTIC_SENSITIVITY <- 200  # ELO points

# Logistic multiplier range: output = baseline * [1/max_mult, max_mult]
# At extreme positive ELO diff: multiplier approaches max_mult
# At extreme negative ELO diff: multiplier approaches 1/max_mult
THREE_WAY_LOGISTIC_MAX_MULTIPLIER <- 1.5  # Range: 0.67x to 1.5x baseline

# ============================================================================
# LEAGUE-SPECIFIC STARTING ELOs (Feb 2026)
# ============================================================================
# New players start at their league's tier-based ELO rather than a fixed value.
# This reflects that a player debuting in IPL is likely stronger than one debuting
# in a development league, providing better prior estimation.
#
# Tier assignment based on event quality and player pool strength:
#   Tier 1: Elite leagues (IPL, T20 World Cup, major international events)
#   Tier 2: Strong franchise leagues (BBL, PSL, CPL, The Hundred, SA20)
#   Tier 3: Domestic leagues and qualifiers
#   Tier 4: Development and associate events

THREE_WAY_LEAGUE_START_TIER_1 <- 1550  # IPL, T20 World Cup, international T20
THREE_WAY_LEAGUE_START_TIER_2 <- 1525  # BBL, PSL, CPL, The Hundred, SA20, MLC
THREE_WAY_LEAGUE_START_TIER_3 <- 1475  # Domestic T20 leagues
THREE_WAY_LEAGUE_START_TIER_4 <- 1425  # Development, associate events

# Tier 1 events (elite)
THREE_WAY_TIER_1_EVENTS <- c(
  "Indian Premier League",
  "ICC Men's T20 World Cup",
  "ICC Women's T20 World Cup",
  "T20 World Cup",
  "T20I",  # International T20s
  "IT20"   # International T20s
)

# Tier 2 events (strong franchise leagues)
THREE_WAY_TIER_2_EVENTS <- c(
  "Big Bash League",
  "Pakistan Super League",
  "Caribbean Premier League",
  "The Hundred Men's Competition",
  "The Hundred Women's Competition",
  "SA20",
  "Major League Cricket",
  "Women's Big Bash League",
  "Lanka Premier League"
)

# Tier 3 events (domestic leagues)
THREE_WAY_TIER_3_EVENTS <- c(
  "Vitality Blast",
  "Syed Mushtaq Ali Trophy",
  "Bangladesh Premier League",
  "Super Smash",
  "CSA T20 Challenge",
  "Sheffield Shield",  # For Test/FC equivalent
  "County Championship"
)

# Update rule: "standard" = k*delta, "sqrt" = k*sign(delta)*sqrt(|delta|)
# Poisson optimization (Jan 2026) found linear (power=1.0) optimal for all formats
THREE_WAY_UPDATE_RULE <- "linear"

# ============================================================================
# FORMAT-GENDER SPECIFIC PARAMETERS (Consolidated Lookup Table)
# ============================================================================
# All format-gender-specific constants in a single nested list.
# Keys match the old variable name prefixes for backward compatibility
# with get_3way_constant().
#
# Optimized separately for each format-gender combination (Feb 2026):
# - Run ELO: Poisson loss (see 02_optimize_run_elo.R)
# - Wicket ELO: Binary log-loss (see 03_optimize_wicket_elo.R)
# Results saved in bouncerdata/models/{run|wicket}_elo_params_{gender}_{format}.rds

THREE_WAY_PARAMS <- list(

  # ---- Men's T20 (baseline) ----
  MENS_T20 = list(
    # Run ELO attribution weights (w_batter + w_bowler + w_venue = 1.0)
    THREE_WAY_W_BATTER         = 0.612,
    THREE_WAY_W_BOWLER         = 0.311,
    THREE_WAY_W_VENUE_SESSION  = 0.062,  # 0.077 * 0.8
    THREE_WAY_W_VENUE_PERM     = 0.015,  # 0.077 * 0.2
    # Wicket ELO attribution weights (Feb 2026: re-optimized with raised bounds)
    THREE_WAY_W_BATTER_WICKET         = 0.411,
    THREE_WAY_W_BOWLER_WICKET         = 0.200,
    THREE_WAY_W_VENUE_SESSION_WICKET  = 0.311,  # 0.389 * 0.8
    THREE_WAY_W_VENUE_PERM_WICKET     = 0.078,  # 0.389 * 0.2
    # Runs per 100 ELO points
    THREE_WAY_RUNS_PER_100_ELO_POINTS = 0.0745,
    # Run K-factors: K = K_MIN + (K_MAX - K_MIN) * exp(-deliveries / K_HALFLIFE)
    THREE_WAY_K_RUN_MAX      = 11.0,
    THREE_WAY_K_RUN_MIN      = 7.0,
    THREE_WAY_K_RUN_HALFLIFE = 150,
    # Wicket K-factors
    THREE_WAY_K_WICKET_MAX      = 12.0,
    THREE_WAY_K_WICKET_MIN      = 4.0,
    THREE_WAY_K_WICKET_HALFLIFE = 150,
    # Wicket ELO divisor
    THREE_WAY_WICKET_ELO_DIVISOR = 400,
    # Venue K-factors (permanent + session)
    THREE_WAY_K_VENUE_PERM_MAX      = 6.15,
    THREE_WAY_K_VENUE_PERM_MIN      = 1.0,
    THREE_WAY_K_VENUE_PERM_HALFLIFE = 4000,
    THREE_WAY_K_VENUE_SESSION_MAX      = 15.0,
    THREE_WAY_K_VENUE_SESSION_MIN      = 2.0,
    THREE_WAY_K_VENUE_SESSION_HALFLIFE = 150
  ),

  # ---- Men's ODI ----
  MENS_ODI = list(
    THREE_WAY_W_BATTER         = 0.563,
    THREE_WAY_W_BOWLER         = 0.279,
    THREE_WAY_W_VENUE_SESSION  = 0.126,  # 0.158 * 0.8
    THREE_WAY_W_VENUE_PERM     = 0.032,  # 0.158 * 0.2
    THREE_WAY_W_BATTER_WICKET         = 0.612,
    THREE_WAY_W_BOWLER_WICKET         = 0.312,
    THREE_WAY_W_VENUE_SESSION_WICKET  = 0.061,  # 0.076 * 0.8
    THREE_WAY_W_VENUE_PERM_WICKET     = 0.015,  # 0.076 * 0.2
    THREE_WAY_RUNS_PER_100_ELO_POINTS = 0.0826,
    THREE_WAY_K_RUN_MAX      = 12.5,
    THREE_WAY_K_RUN_MIN      = 5.1,
    THREE_WAY_K_RUN_HALFLIFE = 150,
    THREE_WAY_K_WICKET_MAX      = 12.0,
    THREE_WAY_K_WICKET_MIN      = 4.0,
    THREE_WAY_K_WICKET_HALFLIFE = 150,
    THREE_WAY_WICKET_ELO_DIVISOR = 400,
    THREE_WAY_K_VENUE_PERM_MAX      = 7.22,
    THREE_WAY_K_VENUE_PERM_MIN      = 1.27,
    THREE_WAY_K_VENUE_PERM_HALFLIFE = 4000,
    THREE_WAY_K_VENUE_SESSION_MAX      = 13.97,
    THREE_WAY_K_VENUE_SESSION_MIN      = 5.0,
    THREE_WAY_K_VENUE_SESSION_HALFLIFE = 150
  ),

  # ---- Men's Test ----
  MENS_TEST = list(
    THREE_WAY_W_BATTER         = 0.628,
    THREE_WAY_W_BOWLER         = 0.290,
    THREE_WAY_W_VENUE_SESSION  = 0.066,  # 0.082 * 0.8
    THREE_WAY_W_VENUE_PERM     = 0.016,  # 0.082 * 0.2
    THREE_WAY_W_BATTER_WICKET         = 0.612,
    THREE_WAY_W_BOWLER_WICKET         = 0.312,
    THREE_WAY_W_VENUE_SESSION_WICKET  = 0.061,  # 0.076 * 0.8
    THREE_WAY_W_VENUE_PERM_WICKET     = 0.015,  # 0.076 * 0.2
    THREE_WAY_RUNS_PER_100_ELO_POINTS = 0.0932,
    THREE_WAY_K_RUN_MAX      = 12.8,
    THREE_WAY_K_RUN_MIN      = 4.7,
    THREE_WAY_K_RUN_HALFLIFE = 150,
    THREE_WAY_K_WICKET_MAX      = 12.0,
    THREE_WAY_K_WICKET_MIN      = 4.0,
    THREE_WAY_K_WICKET_HALFLIFE = 150,
    THREE_WAY_WICKET_ELO_DIVISOR = 400,
    THREE_WAY_K_VENUE_PERM_MAX      = 6.25,
    THREE_WAY_K_VENUE_PERM_MIN      = 1.14,
    THREE_WAY_K_VENUE_PERM_HALFLIFE = 4000,
    THREE_WAY_K_VENUE_SESSION_MAX      = 14.41,
    THREE_WAY_K_VENUE_SESSION_MIN      = 5.0,
    THREE_WAY_K_VENUE_SESSION_HALFLIFE = 150
  ),

  # ---- Women's T20 (venue importance 28.9% - much higher than men's) ----
  WOMENS_T20 = list(
    THREE_WAY_W_BATTER         = 0.542,
    THREE_WAY_W_BOWLER         = 0.169,
    THREE_WAY_W_VENUE_SESSION  = 0.231,  # 0.289 * 0.8
    THREE_WAY_W_VENUE_PERM     = 0.058,  # 0.289 * 0.2
    # Zero venue effect for wickets, all player skill
    THREE_WAY_W_BATTER_WICKET         = 0.650,
    THREE_WAY_W_BOWLER_WICKET         = 0.350,
    THREE_WAY_W_VENUE_SESSION_WICKET  = 0.000,
    THREE_WAY_W_VENUE_PERM_WICKET     = 0.000,
    THREE_WAY_RUNS_PER_100_ELO_POINTS = 0.128,  # +70% vs men's T20
    THREE_WAY_K_RUN_MAX      = 13.6,
    THREE_WAY_K_RUN_MIN      = 3.5,
    THREE_WAY_K_RUN_HALFLIFE = 150,
    THREE_WAY_K_WICKET_MAX      = 12.0,
    THREE_WAY_K_WICKET_MIN      = 4.0,
    THREE_WAY_K_WICKET_HALFLIFE = 443,  # Much slower decay
    THREE_WAY_WICKET_ELO_DIVISOR = 200,  # Faster response
    THREE_WAY_K_VENUE_PERM_MAX      = 10.0,  # Higher: venue characteristics matter more
    THREE_WAY_K_VENUE_PERM_MIN      = 0.98,
    THREE_WAY_K_VENUE_PERM_HALFLIFE = 4000,
    THREE_WAY_K_VENUE_SESSION_MAX      = 13.2,
    THREE_WAY_K_VENUE_SESSION_MIN      = 2.0,
    THREE_WAY_K_VENUE_SESSION_HALFLIFE = 151
  ),

  # ---- Women's ODI ----
  WOMENS_ODI = list(
    THREE_WAY_W_BATTER         = 0.597,
    THREE_WAY_W_BOWLER         = 0.236,
    THREE_WAY_W_VENUE_SESSION  = 0.134,  # 0.167 * 0.8
    THREE_WAY_W_VENUE_PERM     = 0.033,  # 0.167 * 0.2
    THREE_WAY_W_BATTER_WICKET         = 0.641,
    THREE_WAY_W_BOWLER_WICKET         = 0.100,
    THREE_WAY_W_VENUE_SESSION_WICKET  = 0.207,  # 0.259 * 0.8
    THREE_WAY_W_VENUE_PERM_WICKET     = 0.052,  # 0.259 * 0.2
    THREE_WAY_RUNS_PER_100_ELO_POINTS = 0.0894,
    THREE_WAY_K_RUN_MAX      = 13.5,
    THREE_WAY_K_RUN_MIN      = 3.4,
    THREE_WAY_K_RUN_HALFLIFE = 150,
    THREE_WAY_K_WICKET_MAX      = 12.0,
    THREE_WAY_K_WICKET_MIN      = 4.0,
    THREE_WAY_K_WICKET_HALFLIFE = 296,  # Slower decay than men's
    THREE_WAY_WICKET_ELO_DIVISOR = 200,  # Faster response
    THREE_WAY_K_VENUE_PERM_MAX      = 6.26,
    THREE_WAY_K_VENUE_PERM_MIN      = 1.07,
    THREE_WAY_K_VENUE_PERM_HALFLIFE = 4000,
    THREE_WAY_K_VENUE_SESSION_MAX      = 14.4,
    THREE_WAY_K_VENUE_SESSION_MIN      = 5.0,
    THREE_WAY_K_VENUE_SESSION_HALFLIFE = 150
  ),

  # ---- Women's Test ----
  WOMENS_TEST = list(
    THREE_WAY_W_BATTER         = 0.541,
    THREE_WAY_W_BOWLER         = 0.315,
    THREE_WAY_W_VENUE_SESSION  = 0.115,  # 0.144 * 0.8
    THREE_WAY_W_VENUE_PERM     = 0.029,  # 0.144 * 0.2
    THREE_WAY_W_BATTER_WICKET         = 0.613,
    THREE_WAY_W_BOWLER_WICKET         = 0.311,
    THREE_WAY_W_VENUE_SESSION_WICKET  = 0.061,  # 0.076 * 0.8
    THREE_WAY_W_VENUE_PERM_WICKET     = 0.015,  # 0.076 * 0.2
    THREE_WAY_RUNS_PER_100_ELO_POINTS = 0.0798,
    THREE_WAY_K_RUN_MAX      = 10.2,
    THREE_WAY_K_RUN_MIN      = 8.0,
    THREE_WAY_K_RUN_HALFLIFE = 150,
    THREE_WAY_K_WICKET_MAX      = 12.0,
    THREE_WAY_K_WICKET_MIN      = 4.0,
    THREE_WAY_K_WICKET_HALFLIFE = 150,
    THREE_WAY_WICKET_ELO_DIVISOR = 400,
    THREE_WAY_K_VENUE_PERM_MAX      = 6.07,
    THREE_WAY_K_VENUE_PERM_MIN      = 1.30,
    THREE_WAY_K_VENUE_PERM_HALFLIFE = 4000,
    THREE_WAY_K_VENUE_SESSION_MAX      = 14.6,
    THREE_WAY_K_VENUE_SESSION_MIN      = 5.0,
    THREE_WAY_K_VENUE_SESSION_HALFLIFE = 150
  )
)

# ============================================================================
# VENUE K-FACTOR REDUCTION (Feb 2026 - Prevent Venue ELO Drift)
# ============================================================================
# Venue K-factors can be globally reduced to prevent excessive drift.
# Set to 1.0 for original values, 0.1 for 10x reduction.
# This affects PERMANENT venue K only (session resets each match anyway).

THREE_WAY_VENUE_PERM_K_MULTIPLIER <- 0.1  # 10x reduction to prevent excessive venue drift

# ============================================================================
# VENUE DECAY HALFLIVES (Time-based decay toward replacement)
# ============================================================================
# Optimized via IPL Poisson grid search (Jan 2026)
# See data-raw/debug/debug_elo_venue_weight_split.R for methodology
#
# Session (short-term): Captures recent pitch prep, dew, weather conditions
# Permanent (long-term): Captures inherent ground characteristics (size, typical pitch)

THREE_WAY_VENUE_SESSION_DECAY_HALFLIFE <- 3     # 3 days - very short-term
THREE_WAY_VENUE_PERM_DECAY_HALFLIFE <- 1095     # 3 years - long-term characteristics

# ============================================================================
# PLAYER INACTIVITY DECAY
# ============================================================================
# Players who haven't played recently decay towards replacement level
# Optimized via IPL Poisson grid search (Jan 2026)

THREE_WAY_INACTIVITY_THRESHOLD_DAYS <- 90   # Start decay after this many days
THREE_WAY_INACTIVITY_HALFLIFE <- 500        # ~16 months half-life (slow decay)
THREE_WAY_INACTIVITY_K_BOOST_FACTOR <- 0.00 # No K boost when returning (didn't help)

# ============================================================================
# SITUATIONAL K-FACTOR MODIFIERS
# ============================================================================

# High chase (innings 2, required run rate > 10)
# Wickets less informative (risk-taking), runs more informative (intent)
THREE_WAY_HIGH_CHASE_WICKET_MULT <- 0.6
THREE_WAY_HIGH_CHASE_RUN_MULT <- 1.3

# Phase multipliers (restrictions/pressure make outcomes more informative)
THREE_WAY_PHASE_POWERPLAY_MULT <- 1.1
THREE_WAY_PHASE_MIDDLE_MULT <- 1.0
THREE_WAY_PHASE_DEATH_MULT <- 1.2

# Knockout/pressure K modifier (high-stakes = more informative)
THREE_WAY_KNOCKOUT_MULT <- 1.25

# Event tier K multipliers (higher tier = stronger opponents = more informative)
# Uses existing tier detection from get_event_tier()
THREE_WAY_TIER_1_MULT <- 1.2   # IPL, BBL, ICC events, Tests, ODI/T20I
THREE_WAY_TIER_2_MULT <- 1.0   # Strong domestic leagues
THREE_WAY_TIER_3_MULT <- 0.85  # Regional/qualifier events
THREE_WAY_TIER_4_MULT <- 0.7   # Development/associate events

# ============================================================================
# NORMALIZATION PARAMETERS
# ============================================================================

# Apply normalization after every match (user preference)
THREE_WAY_NORMALIZE_AFTER_MATCH <- TRUE
THREE_WAY_NORMALIZATION_CORRECTION_RATE <- 0.5  # Correct 50% of drift per match
THREE_WAY_NORMALIZATION_MIN_DRIFT <- 0.1        # Only if drift > this

# Drift alert threshold
THREE_WAY_DRIFT_ALERT_THRESHOLD <- 10  # Alert if mean drifts > 10 points

# ============================================================================
# SAMPLE-SIZE BAYESIAN BLENDING
# ============================================================================
# Blend raw ELO toward replacement level based on sample size.
# Formula: effective_elo = raw_elo * reliability + REPLACEMENT_LEVEL * (1 - reliability)
#          reliability = balls / (balls + RELIABILITY_HALFLIFE)
# At RELIABILITY_HALFLIFE balls, player has 50% weight on raw ELO.

THREE_WAY_RELIABILITY_HALFLIFE <- 500  # Balls until 50% weight on raw ELO

# ============================================================================
# ANCHOR-BASED CALIBRATION
# ============================================================================
# Scale K-factors by player calibration score to propagate anchor ratings.
# Low calibration = higher K (learns faster from calibrated opponents).
# High calibration = normal K (trusts existing rating).
#
# Formula (in get_3way_player_k):
#   calibration_mult = 1 + (1 - calibration_score / 100) * CALIBRATION_K_BOOST
#   effective_k = base_k * calibration_mult

THREE_WAY_CALIBRATION_K_BOOST <- 0.5   # Max K boost for uncalibrated players
THREE_WAY_ANCHOR_THRESHOLD <- 50       # Calibration score for full trust

# Anchor leagues for calibration scoring (Tier 1 events)
# These are the premier franchise leagues + main ICC events
THREE_WAY_ANCHOR_EVENTS <- c(
  "Indian Premier League",
  "Big Bash League",
  "Pakistan Super League",
  "Caribbean Premier League",
  "SA20",
  "The Hundred Men's Competition",
  "The Hundred Women's Competition",
  "Major League Cricket"
)

# Event-tier K multipliers (player-level, for isolated ecosystem calibration)
# Lower K in weak leagues = ratings change slowly = less inflation
THREE_WAY_PLAYER_TIER_K_MULT_1 <- 1.0  # IPL, BBL, etc. (baseline)
THREE_WAY_PLAYER_TIER_K_MULT_2 <- 0.8  # Vitality Blast, etc.
THREE_WAY_PLAYER_TIER_K_MULT_3 <- 0.6  # Qualifiers
THREE_WAY_PLAYER_TIER_K_MULT_4 <- 0.4  # Development

# ============================================================================
# PARAMETER LOOKUP HELPERS
# ============================================================================
# Functions to retrieve format-gender-specific constants dynamically

#' Get format-gender suffix for constant lookup
#'
#' @param format Format (T20, ODI, TEST, IT20, etc.)
#' @param gender Gender (male or female)
#' @return Suffix like "MENS_T20" or "WOMENS_ODI"
#' @keywords internal
get_format_gender_suffix <- function(format, gender = "male") {
  # Normalize format
  format_clean <- toupper(format)
  if (format_clean %in% c("IT20", "T20I", "WT20I", "T20")) {
    format_clean <- "T20"
  } else if (format_clean %in% c("ODI", "WODI", "ODM")) {
    format_clean <- "ODI"
  } else if (format_clean %in% c("TEST", "MDM")) {
    format_clean <- "TEST"
  } else {
    cli::cli_abort(c(
      "Unrecognized cricket format: {.val {format}}",
      "i" = "Expected one of: T20, IT20, T20I, ODI, ODM, TEST, MDM"
    ), call = NULL)
  }

  # Normalize gender
  gender_clean <- if (tolower(gender) == "female") "WOMENS" else "MENS"

  paste0(gender_clean, "_", format_clean)
}

#' Look up a format-gender-specific 3-way ELO constant
#'
#' Retrieves a constant from the THREE_WAY_PARAMS lookup table.
#' The prefix should match the key name within each format-gender entry
#' (e.g., "THREE_WAY_W_BATTER", "THREE_WAY_K_RUN_MAX").
#'
#' @param prefix Constant prefix (e.g., "THREE_WAY_W_BATTER")
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return The constant value
#' @keywords internal
get_3way_constant <- function(prefix, format, gender = "male") {
  suffix <- get_format_gender_suffix(format, gender)
  params <- THREE_WAY_PARAMS[[suffix]]
  if (is.null(params)) {
    cli::cli_abort("Unknown format-gender combo: {suffix}")
  }
  value <- params[[prefix]]
  if (is.null(value)) {
    cli::cli_abort("Unknown 3-way constant: {prefix} for {suffix}")
  }
  value
}

#' Get Run ELO attribution weights for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Named list with w_batter, w_bowler, w_venue_session, w_venue_perm
#'
#' @examples
#' get_run_elo_weights("t20")
#' get_run_elo_weights("odi", gender = "female")
#'
#' @export
get_run_elo_weights <- function(format, gender = "male") {
  suffix <- get_format_gender_suffix(format, gender)
  p <- THREE_WAY_PARAMS[[suffix]]
  list(
    w_batter = p$THREE_WAY_W_BATTER,
    w_bowler = p$THREE_WAY_W_BOWLER,
    w_venue_session = p$THREE_WAY_W_VENUE_SESSION,
    w_venue_perm = p$THREE_WAY_W_VENUE_PERM
  )
}

#' Get Wicket ELO attribution weights for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Named list with w_batter, w_bowler, w_venue_session, w_venue_perm
#'
#' @examples
#' get_wicket_elo_weights("t20")
#' get_wicket_elo_weights("test", gender = "female")
#'
#' @export
get_wicket_elo_weights <- function(format, gender = "male") {
  suffix <- get_format_gender_suffix(format, gender)
  p <- THREE_WAY_PARAMS[[suffix]]
  list(
    w_batter = p$THREE_WAY_W_BATTER_WICKET,
    w_bowler = p$THREE_WAY_W_BOWLER_WICKET,
    w_venue_session = p$THREE_WAY_W_VENUE_SESSION_WICKET,
    w_venue_perm = p$THREE_WAY_W_VENUE_PERM_WICKET
  )
}

#' Get runs per 100 ELO points for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Numeric value
#'
#' @examples
#' get_runs_per_100_elo("t20")
#' get_runs_per_100_elo("odi")
#'
#' @export
get_runs_per_100_elo <- function(format, gender = "male") {
  suffix <- get_format_gender_suffix(format, gender)
  THREE_WAY_PARAMS[[suffix]]$THREE_WAY_RUNS_PER_100_ELO_POINTS
}

#' Get player Run K-factors for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Named list with k_max, k_min, halflife
#'
#' @examples
#' get_run_k_factors("t20")
#' get_run_k_factors("test", gender = "female")
#'
#' @export
get_run_k_factors <- function(format, gender = "male") {
  suffix <- get_format_gender_suffix(format, gender)
  p <- THREE_WAY_PARAMS[[suffix]]
  list(
    k_max = p$THREE_WAY_K_RUN_MAX,
    k_min = p$THREE_WAY_K_RUN_MIN,
    halflife = p$THREE_WAY_K_RUN_HALFLIFE
  )
}

#' Get player Wicket K-factors for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Named list with k_max, k_min, halflife
#'
#' @examples
#' get_wicket_k_factors("t20")
#' get_wicket_k_factors("odi", gender = "female")
#'
#' @export
get_wicket_k_factors <- function(format, gender = "male") {
  suffix <- get_format_gender_suffix(format, gender)
  p <- THREE_WAY_PARAMS[[suffix]]
  list(
    k_max = p$THREE_WAY_K_WICKET_MAX,
    k_min = p$THREE_WAY_K_WICKET_MIN,
    halflife = p$THREE_WAY_K_WICKET_HALFLIFE
  )
}

#' Get venue K-factors for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Named list with perm and session k-factor parameters
#'
#' @examples
#' get_venue_k_factors("t20")
#' get_venue_k_factors("test")
#'
#' @export
get_venue_k_factors <- function(format, gender = "male") {
  suffix <- get_format_gender_suffix(format, gender)
  p <- THREE_WAY_PARAMS[[suffix]]
  list(
    perm_max = p$THREE_WAY_K_VENUE_PERM_MAX,
    perm_min = p$THREE_WAY_K_VENUE_PERM_MIN,
    perm_halflife = p$THREE_WAY_K_VENUE_PERM_HALFLIFE,
    session_max = p$THREE_WAY_K_VENUE_SESSION_MAX,
    session_min = p$THREE_WAY_K_VENUE_SESSION_MIN,
    session_halflife = p$THREE_WAY_K_VENUE_SESSION_HALFLIFE
  )
}

#' Get wicket ELO divisor for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Numeric value (400 or 200)
#'
#' @examples
#' get_wicket_elo_divisor("t20")
#' get_wicket_elo_divisor("t20", gender = "female")
#'
#' @export
get_wicket_elo_divisor <- function(format, gender = "male") {
  suffix <- get_format_gender_suffix(format, gender)
  THREE_WAY_PARAMS[[suffix]]$THREE_WAY_WICKET_ELO_DIVISOR
}

NULL
