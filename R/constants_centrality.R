# Network Centrality and Margin of Victory Constants
#
# Constants for:
# - Network centrality player quality assessment
# - Margin of victory calculations
# - Centrality integration with ELO system
#
# Split from constants.R for better maintainability.

# ============================================================================
# NETWORK CENTRALITY PLAYER QUALITY CONSTANTS
# ============================================================================
# Network centrality for global player quality assessment.
# Addresses isolated cluster inflation by measuring position in the
# batter-bowler matchup network using Opsahl's weighted degree centrality.
#
# Core concept:
#   - A player is well-connected if they face many unique opponents
#   - A player faces quality opposition if those opponents are also well-connected
#   - Centrality = sqrt(unique_opponents * avg_opponent_degree)
#
# Reference: Opsahl, Agneessens & Skvoretz (2010) "Node centrality in weighted networks"

# Centrality mean type: "arithmetic" or "geometric"
# - arithmetic: (1-alpha)*degree + alpha*avg_opp_degree [RECOMMENDED]
#   Gives immediate boost to players in elite leagues even with few appearances
# - geometric: degree^(1-alpha) * avg_opp_degree^alpha (Opsahl's original)
#   Penalizes low breadth too heavily for new players
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
CENTRALITY_ELO_PER_PERCENTILE <- 6  # ELO points per percentile (50th = start, +/-300 range) (was 4)

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
# Effect examples (with REGRESSION_STRENGTH = 0.002):
#   - Player at 2400 ELO with 5% centrality:
#     implied_elo = 1400 + (5-50)*4 = 1220
#     correction = 0.002 * (1220 - 2400) = -2.36 per delivery
#     Over 300 balls: ~700 point regression toward implied level
#
#   - Player at 1800 ELO with 90% centrality:
#     implied_elo = 1400 + (90-50)*4 = 1560
#     correction = 0.002 * (1560 - 1800) = -0.48 per delivery
#     Over 300 balls: ~144 point regression (much smaller since closer to implied)

CENTRALITY_REGRESSION_STRENGTH <- 0.005  # Pull strength per delivery toward implied ELO (was 0.002)

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
