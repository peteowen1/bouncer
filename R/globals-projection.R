# Score Projection and Visualization Global Variables
#
# This file contains global variable declarations for:
# - Score projection constants and parameters
# - Expected Innings Score (EIS) constants
# - Visualization variables

# ============================================================================
# Expected Innings Score (EIS) Constants
# ============================================================================
# Base expected scores by format, gender, and level

utils::globalVariables(c(
  # T20 EIS
  "EIS_T20_MALE_INTL",
  "EIS_T20_MALE_CLUB",
  "EIS_T20_FEMALE_INTL",
  "EIS_T20_FEMALE_CLUB",
  # ODI EIS
  "EIS_ODI_MALE_INTL",
  "EIS_ODI_MALE_CLUB",
  "EIS_ODI_FEMALE_INTL",
  "EIS_ODI_FEMALE_CLUB",
  # Test EIS
  "EIS_TEST_MALE_INTL",
  "EIS_TEST_MALE_CLUB",
  "EIS_TEST_FEMALE_INTL",
  "EIS_TEST_FEMALE_CLUB"
))

# ============================================================================
# Projection Parameters
# ============================================================================
# Model parameters for score projection

utils::globalVariables(c(
  # Default curve parameters
  "PROJ_DEFAULT_A",
  "PROJ_DEFAULT_B",
  "PROJ_DEFAULT_Z",
  "PROJ_DEFAULT_Y",
  # Thresholds
  "PROJ_MIN_RESOURCE_USED",
  "PROJ_EARLY_INNINGS_BALLS"
))

# ============================================================================
# Projection Bounds
# ============================================================================
# Min/max score constraints by format

utils::globalVariables(c(
  # T20 bounds
  "PROJ_MIN_SCORE_T20",
  "PROJ_MAX_SCORE_T20",
  # ODI bounds
  "PROJ_MIN_SCORE_ODI",
  "PROJ_MAX_SCORE_ODI",
  # Test bounds
  "PROJ_MIN_SCORE_TEST",
  "PROJ_MAX_SCORE_TEST"
))

# ============================================================================
# Format-Specific Constants
# ============================================================================
# Maximum balls and overs per format

utils::globalVariables(c(
  # Max balls per innings
  "MAX_BALLS_T20",
  "MAX_BALLS_ODI",
  "MAX_BALLS_TEST",
  # Test overs per day
  "TEST_OVERS_PER_DAY_5DAY",
  "TEST_OVERS_PER_DAY_4DAY"
))

# ============================================================================
# Score Projection Table Columns
# ============================================================================
# Variables in the projection output tables

utils::globalVariables(c(
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

# ============================================================================
# Visualization Variables
# ============================================================================
# Variables used in plotting functions

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
