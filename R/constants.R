# Package Constants for Bouncer
#
# Main entry point for package constants.
#
#   - constants_skill.R: Player/venue skill indices, format mappings, projections, centrality
#   - constants_skill.R: Player/venue skill indices, format mappings
#   - constants_3way.R: 3-Way ELO system constants
#   - globals.R: All globalVariables() declarations
#
# This file contains only shared data organization categories.

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

# All data partition folders (match_type x gender x team_type)
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

NULL


# ============================================================================
# LEGACY ELO CONSTANTS (from constants_elo.R)
# ============================================================================

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
# DUAL ELO SYSTEM CONSTANTS (Run + Wicket dimensions)
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
# MODEL FILE CONSTANTS (from constants_models.R)
# ============================================================================

MODEL_AGNOSTIC_PATTERN <- "agnostic_outcome_%s.ubj"

# Full outcome models (includes player skill features)
# Format: full_outcome_{format}.ubj where format = shortform|longform
MODEL_FULL_PATTERN <- "full_outcome_%s.ubj"

# Pre-match margin prediction models
# Format: {format}_margin_model.ubj where format = t20|odi|test
MODEL_MARGIN_PATTERN <- "%s_margin_model.ubj"

# In-match win probability models (2-stage)
# Format: {format}_stage1_results.rds, {format}_stage2_results.rds
MODEL_STAGE1_PATTERN <- "%s_stage1_results.rds"
MODEL_STAGE2_PATTERN <- "%s_stage2_results.rds"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

#' Get Model Filename
#'
#' Returns the standardized filename for a model type and format.
#'
#' @param model_type Character. One of: "agnostic", "full", "margin",
#'   "stage1", "stage2"
#' @param format Character. Model format:
#'   - For outcome models: "shortform" or "longform"
#'   - For margin/stage models: "t20", "odi", or "test"
#'
#' @return Character. The model filename (not full path).
#'
#' @examples
#' get_model_filename("agnostic", "shortform")
#' # Returns: "agnostic_outcome_shortform.ubj"
#'
#' get_model_filename("margin", "t20")
#' # Returns: "t20_margin_model.ubj"
#'
#' @export
get_model_filename <- function(model_type, format) {
  model_type <- tolower(model_type)
  format <- tolower(format)

  switch(model_type,
    "agnostic" = sprintf(MODEL_AGNOSTIC_PATTERN, format),
    "full" = sprintf(MODEL_FULL_PATTERN, format),
    "margin" = sprintf(MODEL_MARGIN_PATTERN, format),
    "stage1" = sprintf(MODEL_STAGE1_PATTERN, format),
    "stage2" = sprintf(MODEL_STAGE2_PATTERN, format),
    cli::cli_abort("Unknown model_type: {model_type}. Expected: agnostic, full, margin, stage1, stage2")
  )
}


#' Get Full Model Path
#'
#' Returns the full path to a model file in the models directory.
#'
#' @param model_type Character. One of: "agnostic", "full", "margin",
#'   "stage1", "stage2"
#' @param format Character. Model format (see \code{get_model_filename}).
#' @param models_dir Character. Optional models directory. If NULL, uses
#'   \code{get_models_dir()}.
#'
#' @return Character. Full path to the model file.
#'
#' @examples
#' \dontrun{
#' get_model_path("agnostic", "shortform")
#' # Returns: "/path/to/bouncerdata/models/agnostic_outcome_shortform.ubj"
#' }
#'
#' @export
get_model_path <- function(model_type, format, models_dir = NULL) {
  if (is.null(models_dir)) {
    models_dir <- get_models_dir(create = FALSE)
  }

  filename <- get_model_filename(model_type, format)
  file.path(models_dir, filename)
}


#' Check if Model Exists
#'
#' Checks if a model file exists in the models directory.
#'
#' @param model_type Character. Model type (see \code{get_model_filename}).
#' @param format Character. Model format.
#' @param models_dir Character. Optional models directory.
#'
#' @return Logical. TRUE if model file exists.
#'
#' @export
model_exists <- function(model_type, format, models_dir = NULL) {
  path <- get_model_path(model_type, format, models_dir)
  file.exists(path)
}
