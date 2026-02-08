# 3-Way ELO System Constants
#
# Constants for the 3-Way ELO rating system:
# - Batter + Bowler + Venue with dual session/permanent venue
# - Format-gender specific parameters (optimized Feb 2026)
#
# Split from constants.R for better maintainability.

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
# Player bounds: ±400 from start (1000-1800)
# Venue bounds: ±200 from start (1200-1600) - more conservative
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

# ============================================================================
# FORMAT-GENDER SPECIFIC ATTRIBUTION WEIGHTS (Feb 2026 Optimization)
# ============================================================================
# Optimized separately for each format-gender combination using:
# - Run ELO: Poisson loss (see 02_optimize_run_elo.R)
# - Wicket ELO: Binary log-loss (see 03_optimize_wicket_elo.R)
# Results saved in bouncerdata/models/{run|wicket}_elo_params_{gender}_{format}.rds

# Update rule: "standard" = k*delta, "sqrt" = k*sign(delta)*sqrt(|delta|)
# Poisson optimization (Jan 2026) found linear (power=1.0) optimal for all formats
THREE_WAY_UPDATE_RULE <- "linear"

# ----------------------------------------------------------------------------
# RUN ELO: Attribution Weights (w_batter + w_bowler + w_venue = 1.0)
# Venue split 80% session / 20% permanent
# ----------------------------------------------------------------------------

# Men's T20 (baseline, unchanged from prior optimization)
THREE_WAY_W_BATTER_MENS_T20 <- 0.612
THREE_WAY_W_BOWLER_MENS_T20 <- 0.311
THREE_WAY_W_VENUE_SESSION_MENS_T20 <- 0.062  # 0.077 * 0.8
THREE_WAY_W_VENUE_PERM_MENS_T20 <- 0.015     # 0.077 * 0.2

# Men's ODI (venue importance: 15.8%)
THREE_WAY_W_BATTER_MENS_ODI <- 0.563
THREE_WAY_W_BOWLER_MENS_ODI <- 0.279
THREE_WAY_W_VENUE_SESSION_MENS_ODI <- 0.126  # 0.158 * 0.8
THREE_WAY_W_VENUE_PERM_MENS_ODI <- 0.032     # 0.158 * 0.2

# Men's Test (batter-dominant: 62.8%)
THREE_WAY_W_BATTER_MENS_TEST <- 0.628
THREE_WAY_W_BOWLER_MENS_TEST <- 0.290
THREE_WAY_W_VENUE_SESSION_MENS_TEST <- 0.066  # 0.082 * 0.8
THREE_WAY_W_VENUE_PERM_MENS_TEST <- 0.016     # 0.082 * 0.2

# Women's T20 (venue importance: 28.9% - much higher than men's!)
THREE_WAY_W_BATTER_WOMENS_T20 <- 0.542
THREE_WAY_W_BOWLER_WOMENS_T20 <- 0.169
THREE_WAY_W_VENUE_SESSION_WOMENS_T20 <- 0.231  # 0.289 * 0.8
THREE_WAY_W_VENUE_PERM_WOMENS_T20 <- 0.058     # 0.289 * 0.2

# Women's ODI (venue importance: 16.7%)
THREE_WAY_W_BATTER_WOMENS_ODI <- 0.597
THREE_WAY_W_BOWLER_WOMENS_ODI <- 0.236
THREE_WAY_W_VENUE_SESSION_WOMENS_ODI <- 0.134  # 0.167 * 0.8
THREE_WAY_W_VENUE_PERM_WOMENS_ODI <- 0.033     # 0.167 * 0.2

# Women's Test (venue importance: 14.4%)
THREE_WAY_W_BATTER_WOMENS_TEST <- 0.541
THREE_WAY_W_BOWLER_WOMENS_TEST <- 0.315
THREE_WAY_W_VENUE_SESSION_WOMENS_TEST <- 0.115  # 0.144 * 0.8
THREE_WAY_W_VENUE_PERM_WOMENS_TEST <- 0.029     # 0.144 * 0.2


# ----------------------------------------------------------------------------
# WICKET ELO: Attribution Weights
# Note: Men's T20 rebalanced Feb 2026 (see comment below)
# ----------------------------------------------------------------------------

# Men's T20 (Feb 2026: re-optimized with raised bounds)
# Optimizer found: w_batter=0.411, w_bowler=0.200 (hit lower bound), w_venue=0.389
# Shows 3.85% improvement over null model in internal simulation
# New weights: 41% batter, 20% bowler, 39% venue (31% session + 8% perm)
THREE_WAY_W_BATTER_WICKET_MENS_T20 <- 0.411
THREE_WAY_W_BOWLER_WICKET_MENS_T20 <- 0.200
THREE_WAY_W_VENUE_SESSION_WICKET_MENS_T20 <- 0.311   # 0.389 * 0.8
THREE_WAY_W_VENUE_PERM_WICKET_MENS_T20 <- 0.078      # 0.389 * 0.2

# Men's ODI (player-dominant for wickets)
THREE_WAY_W_BATTER_WICKET_MENS_ODI <- 0.612
THREE_WAY_W_BOWLER_WICKET_MENS_ODI <- 0.312
THREE_WAY_W_VENUE_SESSION_WICKET_MENS_ODI <- 0.061  # 0.076 * 0.8
THREE_WAY_W_VENUE_PERM_WICKET_MENS_ODI <- 0.015     # 0.076 * 0.2

# Men's Test (player-dominant for wickets)
THREE_WAY_W_BATTER_WICKET_MENS_TEST <- 0.612
THREE_WAY_W_BOWLER_WICKET_MENS_TEST <- 0.312
THREE_WAY_W_VENUE_SESSION_WICKET_MENS_TEST <- 0.061  # 0.076 * 0.8
THREE_WAY_W_VENUE_PERM_WICKET_MENS_TEST <- 0.015     # 0.076 * 0.2

# Women's T20 (zero venue effect, all player skill)
THREE_WAY_W_BATTER_WICKET_WOMENS_T20 <- 0.650
THREE_WAY_W_BOWLER_WICKET_WOMENS_T20 <- 0.350
THREE_WAY_W_VENUE_SESSION_WICKET_WOMENS_T20 <- 0.000
THREE_WAY_W_VENUE_PERM_WICKET_WOMENS_T20 <- 0.000

# Women's ODI (venue importance: 25.9%)
THREE_WAY_W_BATTER_WICKET_WOMENS_ODI <- 0.641
THREE_WAY_W_BOWLER_WICKET_WOMENS_ODI <- 0.100
THREE_WAY_W_VENUE_SESSION_WICKET_WOMENS_ODI <- 0.207  # 0.259 * 0.8
THREE_WAY_W_VENUE_PERM_WICKET_WOMENS_ODI <- 0.052     # 0.259 * 0.2

# Women's Test (player-dominant)
THREE_WAY_W_BATTER_WICKET_WOMENS_TEST <- 0.613
THREE_WAY_W_BOWLER_WICKET_WOMENS_TEST <- 0.311
THREE_WAY_W_VENUE_SESSION_WICKET_WOMENS_TEST <- 0.061  # 0.076 * 0.8
THREE_WAY_W_VENUE_PERM_WICKET_WOMENS_TEST <- 0.015     # 0.076 * 0.2


# ----------------------------------------------------------------------------
# RUNS_PER_100_ELO: How much each 100 ELO points affects run prediction
# ----------------------------------------------------------------------------

# Men's formats
THREE_WAY_RUNS_PER_100_ELO_POINTS_MENS_T20 <- 0.0745   # Unchanged
THREE_WAY_RUNS_PER_100_ELO_POINTS_MENS_ODI <- 0.0826   # Optimized
THREE_WAY_RUNS_PER_100_ELO_POINTS_MENS_TEST <- 0.0932  # Optimized (+25% vs baseline)

# Women's formats
THREE_WAY_RUNS_PER_100_ELO_POINTS_WOMENS_T20 <- 0.128   # +70% vs men's T20
THREE_WAY_RUNS_PER_100_ELO_POINTS_WOMENS_ODI <- 0.0894
THREE_WAY_RUNS_PER_100_ELO_POINTS_WOMENS_TEST <- 0.0798


# ============================================================================
# 3-WAY ELO DYNAMIC K-FACTORS (Format-Gender Specific)
# ============================================================================
# K = K_MIN + (K_MAX - K_MIN) * exp(-deliveries / K_HALFLIFE)
# Optimized separately for each format-gender combination (Feb 2026)

# ----------------------------------------------------------------------------
# RUN K-FACTORS: Men's formats
# ----------------------------------------------------------------------------

THREE_WAY_K_RUN_MAX_MENS_T20 <- 11.0
THREE_WAY_K_RUN_MIN_MENS_T20 <- 7.0
THREE_WAY_K_RUN_HALFLIFE_MENS_T20 <- 150

THREE_WAY_K_RUN_MAX_MENS_ODI <- 12.5    # Higher max for ODI
THREE_WAY_K_RUN_MIN_MENS_ODI <- 5.1
THREE_WAY_K_RUN_HALFLIFE_MENS_ODI <- 150

THREE_WAY_K_RUN_MAX_MENS_TEST <- 12.8
THREE_WAY_K_RUN_MIN_MENS_TEST <- 4.7
THREE_WAY_K_RUN_HALFLIFE_MENS_TEST <- 150

# ----------------------------------------------------------------------------
# RUN K-FACTORS: Women's formats
# ----------------------------------------------------------------------------

THREE_WAY_K_RUN_MAX_WOMENS_T20 <- 13.6   # Higher max for women's
THREE_WAY_K_RUN_MIN_WOMENS_T20 <- 3.5
THREE_WAY_K_RUN_HALFLIFE_WOMENS_T20 <- 150

THREE_WAY_K_RUN_MAX_WOMENS_ODI <- 13.5
THREE_WAY_K_RUN_MIN_WOMENS_ODI <- 3.4
THREE_WAY_K_RUN_HALFLIFE_WOMENS_ODI <- 150

THREE_WAY_K_RUN_MAX_WOMENS_TEST <- 10.2  # Lower for Test
THREE_WAY_K_RUN_MIN_WOMENS_TEST <- 8.0
THREE_WAY_K_RUN_HALFLIFE_WOMENS_TEST <- 150


# ----------------------------------------------------------------------------
# WICKET K-FACTORS: Men's formats
# Wicket halflives vary more by gender than format
# ----------------------------------------------------------------------------

THREE_WAY_K_WICKET_MAX_MENS_T20 <- 12.0
THREE_WAY_K_WICKET_MIN_MENS_T20 <- 4.0
THREE_WAY_K_WICKET_HALFLIFE_MENS_T20 <- 150

THREE_WAY_K_WICKET_MAX_MENS_ODI <- 12.0
THREE_WAY_K_WICKET_MIN_MENS_ODI <- 4.0
THREE_WAY_K_WICKET_HALFLIFE_MENS_ODI <- 150

THREE_WAY_K_WICKET_MAX_MENS_TEST <- 12.0
THREE_WAY_K_WICKET_MIN_MENS_TEST <- 4.0
THREE_WAY_K_WICKET_HALFLIFE_MENS_TEST <- 150

# ----------------------------------------------------------------------------
# WICKET K-FACTORS: Women's formats
# Note: Much longer halflives in women's T20/ODI (443/296 balls)
# ----------------------------------------------------------------------------

THREE_WAY_K_WICKET_MAX_WOMENS_T20 <- 12.0
THREE_WAY_K_WICKET_MIN_WOMENS_T20 <- 4.0
THREE_WAY_K_WICKET_HALFLIFE_WOMENS_T20 <- 443  # Much slower decay

THREE_WAY_K_WICKET_MAX_WOMENS_ODI <- 12.0
THREE_WAY_K_WICKET_MIN_WOMENS_ODI <- 4.0
THREE_WAY_K_WICKET_HALFLIFE_WOMENS_ODI <- 296  # Slower decay than men's

THREE_WAY_K_WICKET_MAX_WOMENS_TEST <- 12.0
THREE_WAY_K_WICKET_MIN_WOMENS_TEST <- 4.0
THREE_WAY_K_WICKET_HALFLIFE_WOMENS_TEST <- 150


# ----------------------------------------------------------------------------
# WICKET ELO DIVISOR (Format-Gender Specific)
# Women's T20/ODI use 200 (faster response), others use 400
# ----------------------------------------------------------------------------

THREE_WAY_WICKET_ELO_DIVISOR_MENS_T20 <- 400
THREE_WAY_WICKET_ELO_DIVISOR_MENS_ODI <- 400
THREE_WAY_WICKET_ELO_DIVISOR_MENS_TEST <- 400
THREE_WAY_WICKET_ELO_DIVISOR_WOMENS_T20 <- 200  # Faster response
THREE_WAY_WICKET_ELO_DIVISOR_WOMENS_ODI <- 200  # Faster response
THREE_WAY_WICKET_ELO_DIVISOR_WOMENS_TEST <- 400

# ============================================================================
# VENUE K-FACTOR REDUCTION (Feb 2026 - Prevent Venue ELO Drift)
# ============================================================================
# Venue K-factors can be globally reduced to prevent excessive drift.
# Set to 1.0 for original values, 0.1 for 10x reduction.
# This affects PERMANENT venue K only (session resets each match anyway).

THREE_WAY_VENUE_PERM_K_MULTIPLIER <- 0.1  # 10x reduction (was 1.0)

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
# VENUE K-FACTORS (Dual Component System - Format-Gender Specific)
# ============================================================================
# Optimized via Poisson loss for each format-gender (Feb 2026)
# See data-raw/ratings/player/3way-elo/optimization/02_optimize_run_elo.R
#
# Two components:
#   - PERMANENT: Slow-learning venue characteristics (pitch type, boundaries, altitude)
#   - SESSION: Fast-learning current conditions (resets each match)
#
# K = K_MIN + (K_MAX - K_MIN) * exp(-balls / K_HALFLIFE)

# ----------------------------------------------------------------------------
# VENUE K-FACTORS: Men's formats
# ----------------------------------------------------------------------------

# Men's T20
THREE_WAY_K_VENUE_PERM_MAX_MENS_T20 <- 6.15
THREE_WAY_K_VENUE_PERM_MIN_MENS_T20 <- 1.0
THREE_WAY_K_VENUE_PERM_HALFLIFE_MENS_T20 <- 4000

THREE_WAY_K_VENUE_SESSION_MAX_MENS_T20 <- 15.0
THREE_WAY_K_VENUE_SESSION_MIN_MENS_T20 <- 2.0
THREE_WAY_K_VENUE_SESSION_HALFLIFE_MENS_T20 <- 150

# Men's ODI (higher venue session min = venue matters more consistently)
THREE_WAY_K_VENUE_PERM_MAX_MENS_ODI <- 7.22
THREE_WAY_K_VENUE_PERM_MIN_MENS_ODI <- 1.27
THREE_WAY_K_VENUE_PERM_HALFLIFE_MENS_ODI <- 4000

THREE_WAY_K_VENUE_SESSION_MAX_MENS_ODI <- 13.97
THREE_WAY_K_VENUE_SESSION_MIN_MENS_ODI <- 5.0   # Much higher floor
THREE_WAY_K_VENUE_SESSION_HALFLIFE_MENS_ODI <- 150

# Men's Test
THREE_WAY_K_VENUE_PERM_MAX_MENS_TEST <- 6.25
THREE_WAY_K_VENUE_PERM_MIN_MENS_TEST <- 1.14
THREE_WAY_K_VENUE_PERM_HALFLIFE_MENS_TEST <- 4000

THREE_WAY_K_VENUE_SESSION_MAX_MENS_TEST <- 14.41
THREE_WAY_K_VENUE_SESSION_MIN_MENS_TEST <- 5.0
THREE_WAY_K_VENUE_SESSION_HALFLIFE_MENS_TEST <- 150

# ----------------------------------------------------------------------------
# VENUE K-FACTORS: Women's formats
# Note: Women's T20 has higher venue_perm_max (10 vs 6.15 for men's)
# ----------------------------------------------------------------------------

# Women's T20 (venue_perm_max=10 - venue characteristics matter more)
THREE_WAY_K_VENUE_PERM_MAX_WOMENS_T20 <- 10.0
THREE_WAY_K_VENUE_PERM_MIN_WOMENS_T20 <- 0.98
THREE_WAY_K_VENUE_PERM_HALFLIFE_WOMENS_T20 <- 4000

THREE_WAY_K_VENUE_SESSION_MAX_WOMENS_T20 <- 13.2
THREE_WAY_K_VENUE_SESSION_MIN_WOMENS_T20 <- 2.0
THREE_WAY_K_VENUE_SESSION_HALFLIFE_WOMENS_T20 <- 151

# Women's ODI
THREE_WAY_K_VENUE_PERM_MAX_WOMENS_ODI <- 6.26
THREE_WAY_K_VENUE_PERM_MIN_WOMENS_ODI <- 1.07
THREE_WAY_K_VENUE_PERM_HALFLIFE_WOMENS_ODI <- 4000

THREE_WAY_K_VENUE_SESSION_MAX_WOMENS_ODI <- 14.4
THREE_WAY_K_VENUE_SESSION_MIN_WOMENS_ODI <- 5.0
THREE_WAY_K_VENUE_SESSION_HALFLIFE_WOMENS_ODI <- 150

# Women's Test
THREE_WAY_K_VENUE_PERM_MAX_WOMENS_TEST <- 6.07
THREE_WAY_K_VENUE_PERM_MIN_WOMENS_TEST <- 1.30
THREE_WAY_K_VENUE_PERM_HALFLIFE_WOMENS_TEST <- 4000

THREE_WAY_K_VENUE_SESSION_MAX_WOMENS_TEST <- 14.6
THREE_WAY_K_VENUE_SESSION_MIN_WOMENS_TEST <- 5.0
THREE_WAY_K_VENUE_SESSION_HALFLIFE_WOMENS_TEST <- 150


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
  }

  # Normalize gender
  gender_clean <- if (tolower(gender) == "female") "WOMENS" else "MENS"


  paste0(gender_clean, "_", format_clean)
}

#' Look up a format-gender-specific 3-way ELO constant
#'
#' @param prefix Constant prefix (e.g., "THREE_WAY_W_BATTER")
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return The constant value
#' @keywords internal
get_3way_constant <- function(prefix, format, gender = "male") {
  suffix <- get_format_gender_suffix(format, gender)
  var_name <- paste0(prefix, "_", suffix)
  if (!exists(var_name)) cli::cli_abort("Unknown 3-way constant: {var_name}")
  get(var_name)
}

#' Get Run ELO attribution weights for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Named list with w_batter, w_bowler, w_venue_session, w_venue_perm
#' @export
get_run_elo_weights <- function(format, gender = "male") {
  list(
    w_batter = get_3way_constant("THREE_WAY_W_BATTER", format, gender),
    w_bowler = get_3way_constant("THREE_WAY_W_BOWLER", format, gender),
    w_venue_session = get_3way_constant("THREE_WAY_W_VENUE_SESSION", format, gender),
    w_venue_perm = get_3way_constant("THREE_WAY_W_VENUE_PERM", format, gender)
  )
}

#' Get Wicket ELO attribution weights for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Named list with w_batter, w_bowler, w_venue_session, w_venue_perm
#' @export
get_wicket_elo_weights <- function(format, gender = "male") {
  list(
    w_batter = get_3way_constant("THREE_WAY_W_BATTER_WICKET", format, gender),
    w_bowler = get_3way_constant("THREE_WAY_W_BOWLER_WICKET", format, gender),
    w_venue_session = get_3way_constant("THREE_WAY_W_VENUE_SESSION_WICKET", format, gender),
    w_venue_perm = get_3way_constant("THREE_WAY_W_VENUE_PERM_WICKET", format, gender)
  )
}

#' Get runs per 100 ELO points for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Numeric value
#' @export
get_runs_per_100_elo <- function(format, gender = "male") {
  get_3way_constant("THREE_WAY_RUNS_PER_100_ELO_POINTS", format, gender)
}

#' Get player Run K-factors for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Named list with k_max, k_min, halflife
#' @export
get_run_k_factors <- function(format, gender = "male") {
  list(
    k_max = get_3way_constant("THREE_WAY_K_RUN_MAX", format, gender),
    k_min = get_3way_constant("THREE_WAY_K_RUN_MIN", format, gender),
    halflife = get_3way_constant("THREE_WAY_K_RUN_HALFLIFE", format, gender)
  )
}

#' Get player Wicket K-factors for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Named list with k_max, k_min, halflife
#' @export
get_wicket_k_factors <- function(format, gender = "male") {
  list(
    k_max = get_3way_constant("THREE_WAY_K_WICKET_MAX", format, gender),
    k_min = get_3way_constant("THREE_WAY_K_WICKET_MIN", format, gender),
    halflife = get_3way_constant("THREE_WAY_K_WICKET_HALFLIFE", format, gender)
  )
}

#' Get venue K-factors for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Named list with perm and session k-factor parameters
#' @export
get_venue_k_factors <- function(format, gender = "male") {
  list(
    perm_max = get_3way_constant("THREE_WAY_K_VENUE_PERM_MAX", format, gender),
    perm_min = get_3way_constant("THREE_WAY_K_VENUE_PERM_MIN", format, gender),
    perm_halflife = get_3way_constant("THREE_WAY_K_VENUE_PERM_HALFLIFE", format, gender),
    session_max = get_3way_constant("THREE_WAY_K_VENUE_SESSION_MAX", format, gender),
    session_min = get_3way_constant("THREE_WAY_K_VENUE_SESSION_MIN", format, gender),
    session_halflife = get_3way_constant("THREE_WAY_K_VENUE_SESSION_HALFLIFE", format, gender)
  )
}

#' Get wicket ELO divisor for format-gender
#'
#' @param format Format (T20, ODI, TEST)
#' @param gender Gender (male or female)
#' @return Numeric value (400 or 200)
#' @export
get_wicket_elo_divisor <- function(format, gender = "male") {
  get_3way_constant("THREE_WAY_WICKET_ELO_DIVISOR", format, gender)
}

NULL
