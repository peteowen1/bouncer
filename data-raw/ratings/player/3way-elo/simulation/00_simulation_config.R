# Simulation Configuration ----
#
# Configuration constants for the 3-Way ELO simulation validation framework.
# This framework generates synthetic cricket deliveries with KNOWN TRUE SKILL VALUES,
# runs them through the ELO system, and validates whether the system recovers
# those true skills.
#
# Purpose: Validate that the ELO formulas and update rules correctly attribute
# runs/wickets to the right entities (batter, bowler, venue).

# ============================================================================
# ENTITY COUNTS
# ============================================================================
# Number of synthetic entities to generate
# Smaller than real datasets for faster iteration, but large enough for stable estimation

SIM_N_BATTERS <- 40           # Number of batters to simulate
SIM_N_BOWLERS <- 40           # Number of bowlers to simulate
SIM_N_VENUES <- 15            # Number of venues to simulate
SIM_N_TRAIN_DELIVERIES <- 100000  # Training deliveries
SIM_N_TEST_DELIVERIES <- 50000    # Test deliveries (for out-of-sample evaluation)
SIM_DELIVERIES_PER_MATCH <- 240   # ~120 balls x 2 innings (typical T20 match)

# ============================================================================
# TRUE SKILL DISTRIBUTIONS
# ============================================================================
# These define the "ground truth" skill values for synthetic players.
# Based on Men's T20 statistics for realistic ranges.

# --- Batter Skills ---
# Strike rate: runs per ball (higher = better batter)
SIM_BATTER_SR_MEAN <- 1.138       # T20 format average (~1.14 runs/ball)
SIM_BATTER_SR_SD <- 0.25          # Range: ~0.7-1.8 (covers tailenders to power hitters)

# Wicket rate: probability of dismissal per ball (lower = better batter)
SIM_BATTER_WICKET_MEAN <- 0.054   # T20 average: ~5.4% wicket rate per ball
SIM_BATTER_WICKET_SD <- 0.02      # Range: ~2-12% (stable batters to tailenders)

# --- Bowler Skills ---
# Economy: runs conceded per ball (lower = better bowler)
SIM_BOWLER_ECON_MEAN <- 1.138     # Same as format average (neutral baseline)
SIM_BOWLER_ECON_SD <- 0.15        # Range: ~0.9-1.5 (economical to expensive)

# Wicket rate: probability of taking wicket per ball (higher = better bowler)
SIM_BOWLER_WICKET_MEAN <- 0.054   # T20 average
SIM_BOWLER_WICKET_SD <- 0.015     # Range: ~3-8% (defensive to strike bowlers)

# --- Venue Effects ---
# How much the venue adds/subtracts from expected outcomes
SIM_VENUE_RUN_EFFECT_SD <- 0.1    # Effect on runs (±0.2 typical, high-scoring vs low-scoring)
SIM_VENUE_WICKET_EFFECT_SD <- 0.01  # Effect on wicket probability (±2% typical)

# ============================================================================
# BASELINE VALUES
# ============================================================================
# These are the "neutral" expected values before any skill adjustments

SIM_BASELINE_RUNS <- 1.138        # T20 average runs per delivery
SIM_BASELINE_WICKET <- 0.054      # T20 average wicket probability

# ============================================================================
# SIMULATION PARAMETERS
# ============================================================================

SIM_SEED <- 42                    # Random seed for reproducibility

# Run outcome probabilities for discretization
# Given expected runs, sample from this distribution
# Based on T20 empirical distribution
SIM_RUN_PROBS <- c(
  "0" = 0.35,   # Dot balls
  "1" = 0.35,   # Singles
  "2" = 0.08,   # Twos
  "3" = 0.02,   # Threes
  "4" = 0.12,   # Fours
  "6" = 0.08    # Sixes
)

# ============================================================================
# ATTRIBUTION WEIGHTS (from constants_3way.R for reference)
# ============================================================================
# These should match the ELO system's attribution weights for proper validation

SIM_W_BATTER <- 0.612             # Batter contribution to runs
SIM_W_BOWLER <- 0.311             # Bowler contribution to runs
SIM_W_VENUE_SESSION <- 0.062      # Session venue contribution
SIM_W_VENUE_PERM <- 0.015         # Permanent venue contribution

# For wickets
SIM_W_BATTER_WICKET <- 0.411
SIM_W_BOWLER_WICKET <- 0.200
SIM_W_VENUE_SESSION_WICKET <- 0.311
SIM_W_VENUE_PERM_WICKET <- 0.078

# ============================================================================
# VALIDATION THRESHOLDS
# ============================================================================
# Minimum correlation thresholds for "successful" recovery

SIM_MIN_CORRELATION <- 0.5        # Minimum correlation for success
SIM_MIN_VENUE_CORRELATION <- 0.3  # Lower threshold for venue (fewer samples)

# ============================================================================
# HELPER FUNCTION
# ============================================================================

#' Get simulation configuration as a list
#'
#' Returns all configuration values as a named list for easy access.
#'
#' @return Named list of all simulation configuration values.
get_simulation_config <- function() {
  list(
    # Entity counts
    n_batters = SIM_N_BATTERS,
    n_bowlers = SIM_N_BOWLERS,
    n_venues = SIM_N_VENUES,
    n_train_deliveries = SIM_N_TRAIN_DELIVERIES,
    n_test_deliveries = SIM_N_TEST_DELIVERIES,
    deliveries_per_match = SIM_DELIVERIES_PER_MATCH,

    # Batter skills
    batter_sr_mean = SIM_BATTER_SR_MEAN,
    batter_sr_sd = SIM_BATTER_SR_SD,
    batter_wicket_mean = SIM_BATTER_WICKET_MEAN,
    batter_wicket_sd = SIM_BATTER_WICKET_SD,

    # Bowler skills
    bowler_econ_mean = SIM_BOWLER_ECON_MEAN,
    bowler_econ_sd = SIM_BOWLER_ECON_SD,
    bowler_wicket_mean = SIM_BOWLER_WICKET_MEAN,
    bowler_wicket_sd = SIM_BOWLER_WICKET_SD,

    # Venue effects
    venue_run_effect_sd = SIM_VENUE_RUN_EFFECT_SD,
    venue_wicket_effect_sd = SIM_VENUE_WICKET_EFFECT_SD,

    # Baselines
    baseline_runs = SIM_BASELINE_RUNS,
    baseline_wicket = SIM_BASELINE_WICKET,

    # Attribution weights
    w_batter = SIM_W_BATTER,
    w_bowler = SIM_W_BOWLER,
    w_venue_session = SIM_W_VENUE_SESSION,
    w_venue_perm = SIM_W_VENUE_PERM,

    # Validation
    min_correlation = SIM_MIN_CORRELATION,
    min_venue_correlation = SIM_MIN_VENUE_CORRELATION,

    # Seed
    seed = SIM_SEED
  )
}

NULL
