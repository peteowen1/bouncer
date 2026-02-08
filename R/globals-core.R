# Core Global Variable Declarations
#
# This file contains:
# - Package imports
# - Pipe operator export
# - Null-coalescing operator
# - All global variable declarations for R CMD check compliance
#
# All globalVariables() moved here from constants.R for centralized management.

#' @importFrom stats predict setNames runif
#' @importFrom utils head tail
#' @importFrom dplyr %>%
#' @importFrom rlang .data
#' @importFrom slider slide_dbl
#' @importFrom Matrix sparseMatrix rowSums colSums crossprod
NULL

# Null coalescing operator (like %||% in purrr)
# Returns y if x is NULL, otherwise returns x
`%||%` <- function(x, y) if (is.null(x)) y else x

#' Pipe operator
#'
#' See \code{dplyr::\link[dplyr:reexports]{\%>\%}} for details.
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom dplyr %>%
#' @usage lhs \%>\% rhs
#' @param lhs A value or the dplyr/magrittr placeholder.
#' @param rhs A function call using the dplyr/magrittr semantics.
#' @return The result of calling \code{rhs(lhs)}.
NULL

# ============================================================================
# Core Data Variables
# ============================================================================
# Variables used throughout the package for deliveries, matches, and players

utils::globalVariables(c(
  # Tables
  "deliveries",
  "matches",
  # Match identifiers
  "match_id",
  "match_type",
  "match_date",
  "season",
  # Player identifiers
  "player_id",
  "batter_id",
  "bowler_id",
  # Teams
  "team1",
  "team2",
  "batting_team",
  "bowling_team",
  # Venue and context
  "venue",
  "innings",
  "over",
  "ball",
  "delivery_id",
  # Runs and wickets
  "runs_batter",
  "runs_total",
  "total_runs",
  "is_wicket",
  "wicket_kind",
  "wickets_fallen",
  # Match outcome
  "outcome_winner",
  "outcome_method",
  "outcome_type",
  # Extras
  "is_extra",
  "is_wide",
  "is_noball",
  # Count helper
  "n",
  # data.table symbols
  ".",
  ":="
))


# ============================================================================
# ELO System Variables
# ============================================================================

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


# ============================================================================
# Event Tier Variables
# ============================================================================

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


# ============================================================================
# Team Correlation Variables
# ============================================================================

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


# ============================================================================
# Player Skill Index Variables
# ============================================================================

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


# ============================================================================
# Team Skill Index Variables
# ============================================================================

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


# ============================================================================
# Venue Skill Index Variables
# ============================================================================

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
  "is_wicket_int",  # Helper for rolling feature calculations
  "alias",
  "venue_run_rate_skill",
  "venue_wicket_rate_skill"
))


# ============================================================================
# Player Metrics Variables
# ============================================================================

utils::globalVariables(c(
  "player_out_id",
  "balls_faced",
  "runs_scored",
  "wickets",
  "balls_bowled"
))


# ============================================================================
# 3-Way ELO Variables
# ============================================================================

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


# ============================================================================
# WPA/ERA Attribution Variables
# ============================================================================

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


# ============================================================================
# Feature Engineering Variables
# ============================================================================

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


# ============================================================================
# Outcome Classification Variables
# ============================================================================

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


# ============================================================================
# Chase Calibration Variables
# ============================================================================

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


# ============================================================================
# Pre-Match Prediction Variables
# ============================================================================

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


# ============================================================================
# JSON Parser Enhanced Variables
# ============================================================================

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


# ============================================================================
# Pipeline State Variables
# ============================================================================

utils::globalVariables(c(
  "step_name",
  "last_run_at",
  "last_match_count",
  "last_delivery_count"
))


# ============================================================================
# Fox Sports Scraper Variables
# ============================================================================

utils::globalVariables(c(
  "running_over"
))


# ============================================================================
# Score Projection Variables
# ============================================================================

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


# ============================================================================
# Visualization Variables
# ============================================================================

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


# ============================================================================
# 3-Way Glicko Variables (DEPRECATED)
# ============================================================================
# NOTE: Glicko system moved to data-raw/_deprecated/three_way_glicko.R
# These variables are kept for backward compatibility but the system is no longer active.

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


# ============================================================================
# Network Centrality Player Quality Variables
# ============================================================================

utils::globalVariables(c(
  # Network centrality constants
  "CENTRALITY_ALPHA",
  "CENTRALITY_MEAN_TYPE",
  "CENTRALITY_MIN_DELIVERIES",
  # Centrality outputs
  "batter_centrality",
  "bowler_centrality",
  "centrality_quality_tier",
  "centrality_percentile",
  "unique_opponents",
  "avg_opponent_degree",
  # Matchup matrix building
  "is_wicket_val",
  "is_wicket_delivery",
  "count",
  "total"
))


# ============================================================================
# Centrality Integration Variables
# ============================================================================

utils::globalVariables(c(
  # Snapshot generation
  "CENTRALITY_SNAPSHOT_INTERVAL_GAMES",
  "CENTRALITY_SNAPSHOT_KEEP_MONTHS",
  # K-factor modulation (Option A)
  "CENTRALITY_K_FLOOR",
  "CENTRALITY_K_CEILING",
  "CENTRALITY_K_MIDPOINT",
  "CENTRALITY_K_STEEPNESS",
  # Periodic correction (Option D)
  "CENTRALITY_CORRECTION_RATE",
  "CENTRALITY_ELO_PER_PERCENTILE",
  # Continuous regression (Stronger Bayesian prior)
  "CENTRALITY_REGRESSION_STRENGTH",
  # League-adjusted baseline
  "LEAGUE_BASELINE_BLEND_HALFLIFE",
  # Cold start defaults
  "CENTRALITY_COLD_START_TIER_1",
  "CENTRALITY_COLD_START_TIER_2",
  "CENTRALITY_COLD_START_TIER_3",
  "CENTRALITY_COLD_START_TIER_4",
  # Snapshot lookup results
  "opponent_centrality_percentile",
  "centrality_snapshot_date"
))


# ============================================================================
# Utility Functions
# ============================================================================

#' Check for Parallel Support
#'
#' Checks if the required packages for parallel file parsing are available.
#'
#' @return Logical. TRUE if future and furrr are available.
#' @keywords internal
has_parallel_support <- function() {
  requireNamespace("future", quietly = TRUE) &&
    requireNamespace("furrr", quietly = TRUE)
}
