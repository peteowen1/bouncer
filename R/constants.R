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
# PAGERANK PLAYER QUALITY CONSTANTS
# ============================================================================
# PageRank-style algorithm for global player quality assessment.
# Addresses isolated cluster inflation by propagating "authority" through
# the batter↔bowler matchup network.
#
# Core concept:
#   - A batter is good if they score well against good bowlers
#   - A bowler is good if they restrict good batters
#   - Quality flows through the bipartite matchup graph

PAGERANK_DAMPING <- 0.85           # Standard damping factor (probability of following links)
PAGERANK_MAX_ITER <- 100           # Maximum iterations for convergence
PAGERANK_TOLERANCE <- 1e-6         # Convergence threshold (max change in scores)
PAGERANK_MIN_DELIVERIES <- 100     # Minimum deliveries to include player in graph
PAGERANK_PERFORMANCE_SCALE <- 6.0  # Max runs/ball for normalization (typical T20 max)

# Performance weights: how runs/ball translates to batter "win"
# 0.0 = neutral (avg performance), 1.0 = maximum batter dominance
# Uses logistic scaling centered on format average
PAGERANK_PERFORMANCE_CENTER_T20 <- 1.2   # Runs/ball considered neutral in T20
PAGERANK_PERFORMANCE_CENTER_ODI <- 0.8   # Runs/ball considered neutral in ODI
PAGERANK_PERFORMANCE_CENTER_TEST <- 0.5  # Runs/ball considered neutral in Test

# Wicket impact: how much a wicket shifts the performance score
# Higher = wickets matter more for quality propagation
PAGERANK_WICKET_WEIGHT <- 0.3  # Wicket contribution to bowler quality

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

# ============================================================================
# PREDICTION CALCULATION CONSTANTS
# ============================================================================

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

# How much each ELO point affects runs prediction
# 100 ELO diff ≈ RUNS_PER_100_ELO_POINTS runs/ball difference
# Optimized via IPL Poisson grid search (Jan 2026): 0.12 with linear scores
THREE_WAY_RUNS_PER_100_ELO_POINTS_T20 <- 0.12

# Update rule: "standard" = k*delta, "sqrt" = k*sign(delta)*sqrt(|delta|)
# Poisson optimization (Jan 2026) found linear (power=1.0) optimal for all formats
THREE_WAY_UPDATE_RULE <- "linear"

# ODI/Test runs_per_100 (Poisson-optimized Jan 2026)
# See data-raw/debug/debug_poisson_fast_optimization.R
THREE_WAY_RUNS_PER_100_ELO_POINTS_ODI <- 0.12   # 1.13% improvement
THREE_WAY_RUNS_PER_100_ELO_POINTS_TEST <- 0.10  # 1.51% improvement

# Attribution weights (how much each entity contributes to expected value)
# Optimized via IPL Poisson grid search (Jan 2026)
# See data-raw/debug/debug_elo_4way_venue_decay.R for methodology
# Player weight: 74% (batter 52%, bowler 22%), Venue weight: 26% (split 80/20)
# Must sum to 1.0
THREE_WAY_W_BATTER <- 0.52
THREE_WAY_W_BOWLER <- 0.22
THREE_WAY_W_VENUE_SESSION <- 0.208  # Short-term venue conditions (80% of venue)
THREE_WAY_W_VENUE_PERM <- 0.052     # Permanent venue characteristics (20% of venue)

# ============================================================================
# 3-WAY ELO DYNAMIC K-FACTORS
# ============================================================================
# K = K_MIN + (K_MAX - K_MIN) * exp(-deliveries / K_HALFLIFE)

# Player Run K-factors (optimized via IPL Poisson grid search, Jan 2026)
# See data-raw/debug/debug_elo_4way_venue_decay.R for methodology
# Poisson improvement: 1.63% over constant-mean baseline with 4-way system
THREE_WAY_K_RUN_MAX_T20 <- 22
THREE_WAY_K_RUN_MIN_T20 <- 7
THREE_WAY_K_RUN_HALFLIFE_T20 <- 150  # ~2.5 T20 innings to half-decay

# ODI K-factors (Poisson-optimized via international ODI data, Jan 2026)
# See data-raw/debug/debug_poisson_fast_optimization.R
# Poisson improvement: 1.13% over constant-mean baseline
THREE_WAY_K_RUN_MAX_ODI <- 16
THREE_WAY_K_RUN_MIN_ODI <- 2
THREE_WAY_K_RUN_HALFLIFE_ODI <- 400  # ~6-7 ODI innings to half-decay

# Test K-factors (Poisson-optimized via international Test data, Jan 2026)
# Poisson improvement: 1.51% over constant-mean baseline (best of all formats)
THREE_WAY_K_RUN_MAX_TEST <- 16
THREE_WAY_K_RUN_MIN_TEST <- 2
THREE_WAY_K_RUN_HALFLIFE_TEST <- 800  # ~8 Test innings to half-decay

# Player Wicket K-factors
# Using same halflives as Run ELO (Poisson-optimized), lower K values for rarer events
THREE_WAY_K_WICKET_MAX_T20 <- 6
THREE_WAY_K_WICKET_MIN_T20 <- 1
THREE_WAY_K_WICKET_HALFLIFE_T20 <- 150   # Match Run ELO halflife

THREE_WAY_K_WICKET_MAX_ODI <- 8
THREE_WAY_K_WICKET_MIN_ODI <- 1
THREE_WAY_K_WICKET_HALFLIFE_ODI <- 400   # Match Run ELO halflife

THREE_WAY_K_WICKET_MAX_TEST <- 8
THREE_WAY_K_WICKET_MIN_TEST <- 1
THREE_WAY_K_WICKET_HALFLIFE_TEST <- 800  # Match Run ELO halflife

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
# VENUE K-FACTORS (Dual Component System - Format-Specific)
# ============================================================================
# Optimized via Poisson grid search across T20/ODI/Test (Jan 2026)
# See data-raw/debug/debug_venue_dual_*.R for methodology
#
# Two components:
#   - PERMANENT: Slow-learning venue characteristics (pitch type, boundaries, altitude)
#   - SESSION: Fast-learning current conditions (resets each match)
#
# K = K_MIN + (K_MAX - K_MIN) * exp(-balls / K_HALFLIFE)

# T20 Venue K-factors (optimized from IPL data)
# Improvement: 1.67% vs baseline with dual venue
THREE_WAY_K_VENUE_PERM_MAX_T20 <- 8
THREE_WAY_K_VENUE_PERM_MIN_T20 <- 1
THREE_WAY_K_VENUE_PERM_HALFLIFE_T20 <- 4000   # ~30+ T20 matches

THREE_WAY_K_VENUE_SESSION_MAX_T20 <- 20
THREE_WAY_K_VENUE_SESSION_MIN_T20 <- 2
THREE_WAY_K_VENUE_SESSION_HALFLIFE_T20 <- 150  # ~1 T20 innings

# ODI Venue K-factors (optimized from international ODI data)
# Improvement: 1.43% vs baseline with dual venue
THREE_WAY_K_VENUE_PERM_MAX_ODI <- 8
THREE_WAY_K_VENUE_PERM_MIN_ODI <- 1
THREE_WAY_K_VENUE_PERM_HALFLIFE_ODI <- 8000   # ~25+ ODI matches

THREE_WAY_K_VENUE_SESSION_MAX_ODI <- 20
THREE_WAY_K_VENUE_SESSION_MIN_ODI <- 2
THREE_WAY_K_VENUE_SESSION_HALFLIFE_ODI <- 300  # ~1 ODI innings

# Test Venue K-factors (optimized from international Test data)
# Improvement: 1.56% vs baseline with dual venue
THREE_WAY_K_VENUE_PERM_MAX_TEST <- 4
THREE_WAY_K_VENUE_PERM_MIN_TEST <- 1
THREE_WAY_K_VENUE_PERM_HALFLIFE_TEST <- 15000  # ~10+ Test matches (very stable)

THREE_WAY_K_VENUE_SESSION_MAX_TEST <- 12
THREE_WAY_K_VENUE_SESSION_MIN_TEST <- 2
THREE_WAY_K_VENUE_SESSION_HALFLIFE_TEST <- 600  # ~1 Test session

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

# Maximum possible required run rate (6 runs per ball)
MAX_REQUIRED_RUN_RATE <- 36

# ============================================================================
# 3-WAY GLICKO SYSTEM CONSTANTS (Poisson-Glicko Rating with Explicit RD)
# ============================================================================
# A Glicko-style extension of 3-Way ELO that adds:
#   - Rating Deviation (RD): Explicit uncertainty tracking
#   - g-function weighting: Updates weighted by opponent uncertainty
#   - Composite RD: Combining uncertainties from batter, bowler, venue
#   - Poisson loss function: Variance scales with expected runs
#
# Key mathematical difference from standard ELO:
#   - ELO: K-factor = K_MIN + (K_MAX - K_MIN) * exp(-balls/halflife)
#   - Glicko: Effective K derived from 1/RD², automatically adapts
#
# Glicko constant Q = ln(10)/400 ≈ 0.00576
GLICKO_Q <- log(10) / 400

# Starting ratings and RDs
GLICKO_R_START <- 1500            # Initial rating (same as ELO)
GLICKO_RD_START <- 350            # Initial RD (standard Glicko)
GLICKO_RD_MIN <- 30               # RD floor (very confident)
GLICKO_RD_MAX <- 500              # RD cap for inactive players

# Venue-specific RD parameters (venues more stable than players)
GLICKO_VENUE_RD_START <- 200      # Venues start with less uncertainty
GLICKO_VENUE_RD_MIN <- 50         # Venues can be very well-characterized
GLICKO_VENUE_RD_MAX <- 400        # Max venue uncertainty

# Poisson dispersion parameter (φ)
# φ > 1 indicates overdispersion (variance > mean), typical for cricket
# Cricket run distributions are typically overdispersed due to boundaries
# Optimized via IPL Poisson grid search (Jan 2026): φ=2.5 gave best results
GLICKO_PHI_T20 <- 2.5             # T20: high variance (boundaries, optimized)
GLICKO_PHI_ODI <- 2.0             # ODI: moderate variance
GLICKO_PHI_TEST <- 1.5            # Test: lower variance (defensive play)

# RD decay when inactive (uncertainty grows over time)
# Formula: new_RD = sqrt(old_RD² + decay² * days)
GLICKO_RD_DECAY_PER_DAY <- 5      # Player RD grows by ~5 per day inactive
GLICKO_VENUE_RD_DECAY_PER_DAY <- 1  # Venues decay slower

# Base expected runs (format-specific, used in exponential link)
# These are the intercepts before ELO adjustments
GLICKO_BASE_RUNS_T20 <- 1.28      # IPL T20 average runs per ball
GLICKO_BASE_RUNS_ODI <- 0.82      # ODI average runs per ball
GLICKO_BASE_RUNS_TEST <- 0.52     # Test average runs per ball

# Attribution weights for combining ratings
# (batter + venue) - bowler in the combined rating
GLICKO_W_BATTER <- 0.40           # Batter contribution
GLICKO_W_BOWLER <- 0.40           # Bowler contribution
GLICKO_W_VENUE <- 0.20            # Venue contribution

# Placeholder for "effectively infinite" values in features
# Used when calculations would produce Inf (e.g., runs_needed = 0)
INF_FEATURE_PLACEHOLDER <- 999

# Probability bounds for win probability calculations
# Prevents extreme probabilities that cause numerical issues
MIN_WIN_PROBABILITY <- 0.01
MAX_WIN_PROBABILITY <- 0.99

NULL


# ============================================================================
# GLOBAL VARIABLE DECLARATIONS
# ============================================================================
# R CMD check requires declaring global variables used in dplyr pipelines,
# data.table operations, etc. to avoid "no visible binding" NOTEs.


# ELO System Variables -------------------------------------------------------

utils::globalVariables(c(
  # Basic ELO ratings by format
  "elo_batting",
  "elo_bowling",
  "elo_batting_test",
  "elo_batting_odi",
  "elo_batting_t20",
  "elo_bowling_test",
  "elo_bowling_odi",
  "elo_bowling_t20",
  # Dual ELO system (scoring vs survival)
  "batter_survival_elo",
  "batter_scoring_elo",
  "bowler_strike_elo",
  "bowler_economy_elo",
  "exp_survival",
  "exp_scoring",
  # Dual ELO constant
  "DUAL_ELO_START",
  # Team ELO
  "elo_result"
))


# Event Tier Variables -------------------------------------------------------

utils::globalVariables(c(
  "TIER_1_STARTING_ELO",
  "TIER_2_STARTING_ELO",
  "TIER_3_STARTING_ELO",
  "TIER_4_STARTING_ELO",
  "CROSS_TIER_K_BOOST",
  "CROSS_TIER_K_MAX_MULTIPLIER",
  "TEAM_TIER_EMA_ALPHA",
  "TIER_1_EVENTS",
  "TIER_2_EVENTS",
  "TIER_3_PATTERNS",
  "TIER_4_EVENTS",
  "event_tier",
  "team_tier"
))


# Team Correlation Variables -------------------------------------------------

utils::globalVariables(c(
  "CORRELATION_W_OPPONENTS",
  "CORRELATION_W_EVENTS",
  "CORRELATION_W_DIRECT",
  "CORRELATION_THRESHOLD",
  "PROPAGATION_FACTOR",
  "team_opponents",
  "team_events",
  "team_matchups"
))


# Player Skill Index Variables -----------------------------------------------

utils::globalVariables(c(
  # Batter skills
  "batter_scoring_index",
  "batter_survival_rate",
  "batter_balls_faced",
  "batter_experience",
  # Bowler skills
  "bowler_economy_index",
  "bowler_strike_rate",
  "bowler_balls_bowled",
  "bowler_experience"
))


# Team Skill Index Variables -------------------------------------------------

utils::globalVariables(c(
  "batting_team_runs_skill",
  "batting_team_wicket_skill",
  "bowling_team_runs_skill",
  "bowling_team_wicket_skill",
  "batting_team_balls",
  "bowling_team_balls",
  "team1_team_runs_skill",
  "team2_team_runs_skill",
  "team1_team_wicket_skill",
  "team2_team_wicket_skill"
))


# Venue Skill Index Variables ------------------------------------------------

utils::globalVariables(c(
  "venue_run_rate",
  "venue_wicket_rate",
  "venue_boundary_rate",
  "venue_dot_rate",
  "venue_balls",
  "venue_canonical",
  "venue_reliable",
  "canonical_venue",
  "is_boundary",
  "is_dot",
  "alias",
  "venue_run_rate_skill",
  "venue_wicket_rate_skill"
))


# Player Metrics Variables ---------------------------------------------------

utils::globalVariables(c(
  "player_out_id",
  "balls_faced",
  "runs_scored",
  "wickets",
  "balls_bowled"
))


# 3-Way ELO Variables --------------------------------------------------------

utils::globalVariables(c(
  # 3-Way ELO Constants
  "THREE_WAY_ELO_START",
  "THREE_WAY_ELO_TARGET_MEAN",
  "THREE_WAY_ELO_DIVISOR",
  "THREE_WAY_REPLACEMENT_LEVEL",
  "THREE_WAY_UPDATE_RULE",
  # Sample-size blending
  "THREE_WAY_RELIABILITY_HALFLIFE",
  # Anchor calibration
  "THREE_WAY_CALIBRATION_K_BOOST",
  "THREE_WAY_ANCHOR_THRESHOLD",
  "THREE_WAY_ANCHOR_EVENTS",
  "THREE_WAY_PLAYER_TIER_K_MULT_1",
  "THREE_WAY_PLAYER_TIER_K_MULT_2",
  "THREE_WAY_PLAYER_TIER_K_MULT_3",
  "THREE_WAY_PLAYER_TIER_K_MULT_4",
  # Player Run ELOs
  "batter_run_elo_before",
  "batter_run_elo_after",
  "bowler_run_elo_before",
  "bowler_run_elo_after",
  # Player Wicket ELOs
  "batter_wicket_elo_before",
  "batter_wicket_elo_after",
  "bowler_wicket_elo_before",
  "bowler_wicket_elo_after",
  # Venue Permanent ELOs
  "venue_perm_run_elo_before",
  "venue_perm_run_elo_after",
  "venue_perm_wicket_elo_before",
  "venue_perm_wicket_elo_after",
  # Venue Session ELOs
  "venue_session_run_elo_before",
  "venue_session_run_elo_after",
  "venue_session_wicket_elo_before",
  "venue_session_wicket_elo_after",
  # Venue Weights
  "THREE_WAY_W_VENUE_PERM",
  "THREE_WAY_W_VENUE_SESSION",
  # Venue Decay Halflives
  "THREE_WAY_VENUE_SESSION_DECAY_HALFLIFE",
  "THREE_WAY_VENUE_PERM_DECAY_HALFLIFE",
  # Venue K-factors by format
  "THREE_WAY_K_VENUE_PERM_MAX_T20",
  "THREE_WAY_K_VENUE_PERM_MIN_T20",
  "THREE_WAY_K_VENUE_PERM_HALFLIFE_T20",
  "THREE_WAY_K_VENUE_SESSION_MAX_T20",
  "THREE_WAY_K_VENUE_SESSION_MIN_T20",
  "THREE_WAY_K_VENUE_SESSION_HALFLIFE_T20",
  "THREE_WAY_K_VENUE_PERM_MAX_ODI",
  "THREE_WAY_K_VENUE_PERM_MIN_ODI",
  "THREE_WAY_K_VENUE_PERM_HALFLIFE_ODI",
  "THREE_WAY_K_VENUE_SESSION_MAX_ODI",
  "THREE_WAY_K_VENUE_SESSION_MIN_ODI",
  "THREE_WAY_K_VENUE_SESSION_HALFLIFE_ODI",
  "THREE_WAY_K_VENUE_PERM_MAX_TEST",
  "THREE_WAY_K_VENUE_PERM_MIN_TEST",
  "THREE_WAY_K_VENUE_PERM_HALFLIFE_TEST",
  "THREE_WAY_K_VENUE_SESSION_MAX_TEST",
  "THREE_WAY_K_VENUE_SESSION_MIN_TEST",
  "THREE_WAY_K_VENUE_SESSION_HALFLIFE_TEST",
  # K-Factors
  "k_batter_run",
  "k_bowler_run",
  "k_venue_perm_run",
  "k_venue_session_run",
  "k_batter_wicket",
  "k_bowler_wicket",
  "k_venue_perm_wicket",
  "k_venue_session_wicket",
  # Context variables
  "batter_balls",
  "bowler_balls",
  "venue_balls",
  "balls_in_match",
  "days_inactive_batter",
  "days_inactive_bowler",
  "is_knockout",
  "phase",
  # Expected values
  "exp_runs",
  "exp_wicket",
  "actual_runs",
  "agnostic_runs",
  "agnostic_wicket"
))


# WPA/ERA Attribution Variables ----------------------------------------------

utils::globalVariables(c(
  "predicted_before",
  "predicted_after",
  "wpa",
  "batter_wpa",
  "bowler_wpa",
  "expected_runs",
  "era",
  "batter_era",
  "bowler_era",
  "total_wpa",
  "total_era",
  "wpa_per_delivery",
  "era_per_delivery",
  "key_moment_wpa",
  "clutch_moments",
  "positive_wpa_pct",
  "role"
))


# Feature Engineering Variables ----------------------------------------------

utils::globalVariables(c(
  # Phase of play
  "phase_powerplay",
  "phase_middle",
  "phase_death",
  "phase_new_ball",
  "phase_old_ball",
  # Match context
  "gender",
  "gender_male",
  "format_t20",
  "format_odi",
  "match_type_t20",
  "match_type_odi",
  "innings_num",
  # Game state
  "runs_difference",
  "overs_left",
  # Ball outcomes
  "is_four",
  "is_six",
  "over_runs",
  "over_wickets"
))


# Outcome Classification Variables -------------------------------------------

utils::globalVariables(c(
  "event_match_number",
  "event_group",
  "outcome_method_lower",
  "outcome_type_lower",
  "is_super_over",
  "event_match_number_lower",
  "event_group_lower",
  "is_pure_tie",
  "is_no_result",
  "is_dls_match",
  "normal_results",
  "dls_matches",
  "super_over_matches",
  "pure_ties",
  "no_results"
))


# Chase Calibration Variables ------------------------------------------------

utils::globalVariables(c(
  "chase_completed",
  "chase_impossible",
  "runs_per_ball_needed",
  "balls_per_run_available",
  "resources_per_run",
  "chase_buffer",
  "chase_buffer_ratio",
  "is_easy_chase",
  "is_difficult_chase",
  "theoretical_min_balls",
  "balls_surplus"
))


# Pre-Match Prediction Variables ---------------------------------------------

utils::globalVariables(c(
  # Team 1 features
  "team1_elo_result",
  "team1_elo_roster",
  "team1_form_last5",
  "team1_h2h_total",
  "team1_h2h_wins",
  "team1_bat_scoring_avg",
  "team1_bat_scoring_top5",
  "team1_bat_survival_avg",
  "team1_bowl_economy_avg",
  "team1_bowl_economy_top5",
  "team1_bowl_strike_avg",
  "team1_won_toss",
  # Team 2 features
  "team2_elo_result",
  "team2_elo_roster",
  "team2_form_last5",
  "team2_bat_scoring_avg",
  "team2_bat_scoring_top5",
  "team2_bat_survival_avg",
  "team2_bowl_economy_avg",
  "team2_bowl_economy_top5",
  "team2_bowl_strike_avg",
  # Derived features (differences)
  "elo_diff_result",
  "form_diff",
  "bat_scoring_diff",
  "bat_scoring_top5_diff",
  "bowl_economy_diff",
  "bowl_economy_top5_diff",
  # Match context
  "toss_elect_bat",
  "venue_avg_score",
  "venue_chase_success_rate"
))


# JSON Parser Enhanced Variables ---------------------------------------------

utils::globalVariables(c(
  # Match-level
  "reserve_umpire",
  "missing_data",
  # Innings-level
  "target_runs",
  "target_overs",
  "absent_hurt",
  # Delivery-level: DRS review
  "has_review",
  "review_by",
  "review_umpire",
  "review_batter",
  "review_decision",
  # Delivery-level: substitute fielder flags
  "fielder1_is_sub",
  "fielder2_is_sub",
  # Delivery-level: player replacement
  "has_replacement",
  "replacement_in",
  "replacement_out",
  "replacement_reason",
  "replacement_role",
  # Powerplays table
  "powerplay_id",
  "from_over",
  "to_over",
  "powerplay_type"
))


# Pipeline State Variables ---------------------------------------------------

utils::globalVariables(c(
  "step_name",
  "last_run_at",
  "last_match_count",
  "last_delivery_count"
))


# Fox Sports Scraper Variables -----------------------------------------------

utils::globalVariables(c(
  "running_over"
))


# Score Projection Variables -------------------------------------------------

utils::globalVariables(c(
  # EIS Constants
  "EIS_T20_MALE_INTL",
  "EIS_T20_MALE_CLUB",
  "EIS_T20_FEMALE_INTL",
  "EIS_T20_FEMALE_CLUB",
  "EIS_ODI_MALE_INTL",
  "EIS_ODI_MALE_CLUB",
  "EIS_ODI_FEMALE_INTL",
  "EIS_ODI_FEMALE_CLUB",
  "EIS_TEST_MALE_INTL",
  "EIS_TEST_MALE_CLUB",
  "EIS_TEST_FEMALE_INTL",
  "EIS_TEST_FEMALE_CLUB",
  # Projection parameters
  "PROJ_DEFAULT_A",
  "PROJ_DEFAULT_B",
  "PROJ_DEFAULT_Z",
  "PROJ_DEFAULT_Y",
  "PROJ_MIN_RESOURCE_USED",
  "PROJ_EARLY_INNINGS_BALLS",
  # Projection bounds
  "PROJ_MIN_SCORE_T20",
  "PROJ_MAX_SCORE_T20",
  "PROJ_MIN_SCORE_ODI",
  "PROJ_MAX_SCORE_ODI",
  "PROJ_MIN_SCORE_TEST",
  "PROJ_MAX_SCORE_TEST",
  # Format-specific constants
  "MAX_BALLS_T20",
  "MAX_BALLS_ODI",
  "MAX_BALLS_TEST",
  "TEST_OVERS_PER_DAY_5DAY",
  "TEST_OVERS_PER_DAY_4DAY",
  # Projection table columns
  "current_score",
  "balls_remaining",
  "balls_bowled",
  "wickets_remaining",
  "resource_remaining",
  "resource_used",
  "eis_agnostic",
  "eis_full",
  "projected_agnostic",
  "projected_full",
  "final_innings_total",
  "projection_change_agnostic",
  "projection_change_full",
  "avg_score"
))


# Visualization Variables ----------------------------------------------------

utils::globalVariables(c(
  # plot_player_comparison
  "Metric",
  "Value",
  "NormValue",
  "Player",
  # plot_score_progression
  "ball_number",
  "cumulative_runs",
  # plot_skill_progression
  "value",
  "match_num",
  "skill_type",
  # plot_team_strength
  "Skill",
  "Category",
  # plot_win_probability
  "match_ball",
  "win_prob_after"
))


# 3-Way Glicko Variables -----------------------------------------------------

utils::globalVariables(c(
  # Glicko constants
  "GLICKO_Q",
  "GLICKO_R_START",
  "GLICKO_RD_START",
  "GLICKO_RD_MIN",
  "GLICKO_RD_MAX",
  "GLICKO_VENUE_RD_START",
  "GLICKO_VENUE_RD_MIN",
  "GLICKO_VENUE_RD_MAX",
  "GLICKO_PHI_T20",
  "GLICKO_PHI_ODI",
  "GLICKO_PHI_TEST",
  "GLICKO_RD_DECAY_PER_DAY",
  "GLICKO_VENUE_RD_DECAY_PER_DAY",
  "GLICKO_BASE_RUNS_T20",
  "GLICKO_BASE_RUNS_ODI",
  "GLICKO_BASE_RUNS_TEST",
  "GLICKO_W_BATTER",
  "GLICKO_W_BOWLER",
  "GLICKO_W_VENUE",
  # Player Glicko ratings and RDs
  "batter_glicko_r",
  "batter_glicko_rd",
  "bowler_glicko_r",
  "bowler_glicko_rd",
  "batter_glicko_r_after",
  "batter_glicko_rd_after",
  "bowler_glicko_r_after",
  "bowler_glicko_rd_after",
  # Venue Glicko ratings and RDs
  "venue_glicko_r",
  "venue_glicko_rd",
  "venue_glicko_r_after",
  "venue_glicko_rd_after",
  # Combined/expected values
  "glicko_theta",
  "glicko_expected_runs",
  "glicko_composite_rd"
))


# PageRank Player Quality Variables ------------------------------------------

utils::globalVariables(c(
  # PageRank constants
  "PAGERANK_DAMPING",
  "PAGERANK_MAX_ITER",
  "PAGERANK_TOLERANCE",
  "PAGERANK_MIN_DELIVERIES",
  "PAGERANK_PERFORMANCE_SCALE",
  "PAGERANK_PERFORMANCE_CENTER_T20",
  "PAGERANK_PERFORMANCE_CENTER_ODI",
  "PAGERANK_PERFORMANCE_CENTER_TEST",
  "PAGERANK_WICKET_WEIGHT",
  # PageRank outputs
  "batter_pagerank",
  "bowler_pagerank",
  "pagerank_quality_tier",
  "pagerank_percentile"
))
