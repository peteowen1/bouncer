# Package Constants for Bouncer

#' ELO Rating Constants
#'
#' @keywords internal
#' @name elo_constants

# Starting ELO rating for all players
ELO_START_RATING <- 1500

# K-factors by match type (learning rate)
K_FACTOR_TEST <- 32
K_FACTOR_ODI <- 24
K_FACTOR_T20 <- 20
K_FACTOR_DOMESTIC <- 16

# Average runs per ball by format (used in predictions)
AVG_RUNS_PER_BALL_TEST <- 3.0
AVG_RUNS_PER_BALL_ODI <- 5.0
AVG_RUNS_PER_BALL_T20 <- 7.5

# Base wicket probability per delivery by format
BASE_WICKET_PROB_TEST <- 0.035   # ~1 wicket per 30 balls
BASE_WICKET_PROB_ODI <- 0.030    # ~1 wicket per 33 balls
BASE_WICKET_PROB_T20 <- 0.025    # ~1 wicket per 40 balls

# ELO calculation constant (standard chess-style)
ELO_DIVISOR <- 400

# ============================================================================
# NEW DUAL ELO SYSTEM CONSTANTS (Run + Wicket dimensions)
# ============================================================================

# Starting/Target ELO
DUAL_ELO_START <- 1500
DUAL_ELO_TARGET_MEAN <- 1500

# ELO divisor (standard)
DUAL_ELO_DIVISOR <- 400

# K-factors for Run ELO (scoring ability)
K_RUN_T20 <- 20
K_RUN_ODI <- 16
K_RUN_TEST <- 12

# K-factors for Wicket ELO
# IMPORTANT: K_WICKET and K_WICKET_SURVIVAL must be EQUAL for zero-sum ELO.
# Asymmetric K-factors cause systematic drift because:
#   E[update] = P(survive)*k_survival*exp - P(wicket)*k_wicket*(1-exp)
# This only equals zero when k_survival = k_wicket.
# The natural rarity of wickets (1.6% - 5.4%) provides sufficient weighting.
K_WICKET_T20 <- 8
K_WICKET_ODI <- 6
K_WICKET_TEST <- 4
# Survival K must equal wicket K for zero-sum
K_WICKET_SURVIVAL_T20 <- 8
K_WICKET_SURVIVAL_ODI <- 6
K_WICKET_SURVIVAL_TEST <- 4

# Run ELO scoring weights (batter outcome)
RUN_SCORE_WICKET <- 0.0    # Worst outcome
RUN_SCORE_DOT <- 0.15      # Slight credit for survival
RUN_SCORE_SINGLE <- 0.35
RUN_SCORE_DOUBLE <- 0.45
RUN_SCORE_THREE <- 0.55
RUN_SCORE_FOUR <- 0.75
RUN_SCORE_SIX <- 1.0       # Best outcome


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
# DATA ORGANIZATION FORMAT CATEGORIES
# ============================================================================
# For organizing data into folders and releases
# Uses long_form/short_form instead of test/odi/t20

# Long form = day-limited matches (Tests, First-class, multi-day)
FORMAT_LONG_FORM <- c("Test", "MDM")

# Short form = over/ball-limited matches (everything else)
FORMAT_SHORT_FORM <- c("ODI", "ODM", "T20", "IT20", "T10")

# International match types (for international vs club classification)
MATCH_TYPE_INTERNATIONAL <- c("Test", "ODI", "IT20")

# All data partition folders (match_type × gender × team_type)
# Based on actual partitions created by daily scraper
DATA_FOLDERS <- c(
  # Test format
  "Test_male_international",
  "Test_female_international",
  # ODI format
  "ODI_male_international",
  "ODI_female_international",
  # T20 format (includes franchise leagues)
  "T20_male_international",
  "T20_male_club",
  "T20_female_international",
  "T20_female_club",
  # IT20 (domestic T20 internationals)
  "IT20_male_international",
  "IT20_female_international",
  # MDM (multi-day matches / first-class)
  "MDM_male_international",
  "MDM_male_club",
  "MDM_female_international",
  "MDM_female_club",
  # ODM (domestic one-day)
  "ODM_male_international",
  "ODM_male_club"
)

# ============================================================================
# PLAYER ELO DYNAMIC K-FACTOR SYSTEM
# ============================================================================
# Addresses "weak vs weak" ELO inflation by:
#   1. Dynamic K based on experience (deliveries faced/bowled)
#   2. Opponent strength adjustment to K-factor
#   3. Tier-based starting ELOs for new players

# Dynamic K decay: K = K_MIN + (K_MAX - K_MIN) * exp(-deliveries / K_HALFLIFE)
# New players learn faster, experienced players are more stable

# Run ELO K-factors (max for new players, min for experienced)
PLAYER_K_RUN_MAX_T20 <- 40    # New player K-factor
PLAYER_K_RUN_MIN_T20 <- 12    # Established player K-factor
PLAYER_K_RUN_HALFLIFE_T20 <- 300  # Deliveries until K decays halfway (~25 T20 innings)

PLAYER_K_RUN_MAX_ODI <- 32
PLAYER_K_RUN_MIN_ODI <- 10
PLAYER_K_RUN_HALFLIFE_ODI <- 500  # ~10 ODI innings

PLAYER_K_RUN_MAX_TEST <- 24
PLAYER_K_RUN_MIN_TEST <- 8
PLAYER_K_RUN_HALFLIFE_TEST <- 800  # ~8 Test innings

# Wicket ELO K-factors (same max/min/halflife structure)
PLAYER_K_WICKET_MAX_T20 <- 16
PLAYER_K_WICKET_MIN_T20 <- 5
PLAYER_K_WICKET_HALFLIFE_T20 <- 300

PLAYER_K_WICKET_MAX_ODI <- 12
PLAYER_K_WICKET_MIN_ODI <- 4
PLAYER_K_WICKET_HALFLIFE_ODI <- 500

PLAYER_K_WICKET_MAX_TEST <- 8
PLAYER_K_WICKET_MIN_TEST <- 3
PLAYER_K_WICKET_HALFLIFE_TEST <- 800

# Opponent strength K-factor modifiers
# When facing a stronger opponent (>avg ELO), K is boosted for good performance
# When facing a weaker opponent, K is reduced for losses (less penalty for losing to good players)
# Increased from 0.3/0.2 to 0.4/0.3 to better penalize weak-vs-weak matchups
PLAYER_OPP_K_BOOST <- 0.4    # K boost when beating strong opponent (per 400 ELO diff)
PLAYER_OPP_K_REDUCE <- 0.3   # K penalty reduction when losing to strong opponent

# Tier-based starting ELOs for players (based on first event they appear in)
PLAYER_TIER_1_START_ELO <- 1525  # Elite events (IPL, BBL, ICC World events)
PLAYER_TIER_2_START_ELO <- 1500  # Strong domestic leagues
PLAYER_TIER_3_START_ELO <- 1450  # Regional/qualifier events
PLAYER_TIER_4_START_ELO <- 1400  # Development events

# ============================================================================
# PLAYER ELO CORRELATION-BASED PROPAGATION
# ============================================================================
# Addresses isolated player pools (e.g., Kenya vs Nepal) by propagating
# ELO adjustments through correlation matrix based on:
#   1. Shared opponents (batters who faced same bowlers, etc.)
#   2. Same events (players in same tournaments)
#   3. Direct matchups (head-to-head encounters)
#
# This anchors isolated pools to the main player pool.

# Correlation weights (must sum to 1.0)
PLAYER_CORR_W_OPPONENTS <- 0.5  # Weight for shared opponents
PLAYER_CORR_W_EVENTS <- 0.3     # Weight for same events
PLAYER_CORR_W_DIRECT <- 0.2     # Weight for direct matches

# Propagation settings
PLAYER_CORR_THRESHOLD <- 0.1    # Min correlation to propagate (skip weak links)
PLAYER_PROPAGATION_FACTOR <- 0.25  # How much ELO change propagates (was 0.15, increased for visibility)

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
MARGIN_ELO_DIVISOR_T20 <- 15     # 100 ELO diff ≈ 6-7 runs margin
MARGIN_ELO_DIVISOR_ODI <- 12     # 100 ELO diff ≈ 8 runs margin
MARGIN_ELO_DIVISOR_TEST <- 10    # 100 ELO diff ≈ 10 runs margin

# Draw handling (Test matches)
DRAW_ACTUAL_SCORE <- 0.5         # Treat draw as 0.5 result (like chess)
DRAW_MARGIN <- 0                 # Draws have zero margin
DRAW_MOV_MULTIPLIER <- 1.0       # No margin adjustment for draws

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

NULL
