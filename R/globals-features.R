# Feature Engineering Global Variables
#
# This file contains global variable declarations for:
# - Feature engineering variables
# - Outcome classification
# - Pre-match prediction features
# - JSON parser enhanced fields
# - WPA/ERA attribution

# ============================================================================
# WPA/ERA Attribution Variables
# ============================================================================
# Win Probability Added and Expected Runs Added

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
# Context features for modeling

utils::globalVariables(c(
  # Phase of play
  "phase",
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
# Match result types and special cases

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
# Chase Calibration Features
# ============================================================================
# Features for second innings modeling

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
# Pre-Match Prediction Features
# ============================================================================
# Features for pre-game win probability models

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
  "venue_chase_success_rate",
  "is_knockout"
))

# ============================================================================
# JSON Parser Enhanced Fields
# ============================================================================
# Additional fields from Cricsheet JSON parsing

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
# For tracking pipeline execution

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
