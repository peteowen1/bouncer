# Global Variable Declarations for R CMD check

#' @importFrom stats predict setNames runif
#' @importFrom utils head tail
#' @importFrom dplyr %>%
#' @importFrom rlang .data
#' @importFrom slider slide_dbl
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

# Declare global variables used in dplyr/tidyr operations
utils::globalVariables(c(
  "deliveries",
  "matches",
  "match_id",
  "match_type",
  "match_date",
  "player_id",
  "batter_id",
  "bowler_id",
  "batting_team",
  "bowling_team",
  "venue",
  "innings",
  "over",
  "ball",
  "runs_batter",
  "runs_total",
  "is_wicket",
  "wicket_kind",
  "elo_batting",
  "elo_bowling",
  "elo_batting_test",
  "elo_batting_odi",
  "elo_batting_t20",
  "elo_bowling_test",
  "elo_bowling_odi",
  "elo_bowling_t20",
  "delivery_id",
  "season",
  "team1",
  "team2",
  "outcome_winner",
  "total_runs",
  "wickets_fallen",
  "n",
  # Dual ELO system variables
  "batter_survival_elo",
  "batter_scoring_elo",
  "bowler_strike_elo",
  "bowler_economy_elo",
  "exp_survival",
  "exp_scoring",
  # WPA/ERA variables
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
  "role",
  "is_extra",
  "is_wide",
  "is_noball",
  # data.table symbols
  ".",
  ":=",
  # Column names from feature engineering and outcome classification
  "phase",
  "gender",
  "match_type_t20",
  "match_type_odi",
  "innings_num",
  "runs_difference",
  "overs_left",
  "phase_powerplay",
  "phase_middle",
  "phase_death",
  "phase_new_ball",
  "phase_old_ball",
  "gender_male",
  "is_four",
  "is_six",
  "over_runs",
  "over_wickets",
  # Outcome classification variables
  "outcome_method",
  "outcome_type",
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
  "no_results",
  # Tail calibration features
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

# Dual ELO constants (declared for package use)
utils::globalVariables(c(
  "DUAL_ELO_START"
))

# Event tier constants (from event_tiers.R)
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

# Team correlation constants (from team_correlation.R)
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

# Skill index variables (from player_skill_index.R)
utils::globalVariables(c(
  "batter_scoring_index",
  "batter_survival_rate",
  "bowler_economy_index",
  "bowler_strike_rate",
  "batter_balls_faced",
  "bowler_balls_bowled"
))

# Venue skill index variables (from venue_skill_index.R)
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
  "alias"
))

# Pre-match features variables (from pre_match_features.R)
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
  # Derived features
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

# Fox Sports scraper variables (from fox_scraper.R)
utils::globalVariables(c(
  "running_over"
))

# Pipeline state variables (from pipeline_state.R)
utils::globalVariables(c(
  "step_name",
  "last_run_at",
  "last_match_count",
  "last_delivery_count"
))

# Team skill variables (from team_skill_index.R and agnostic_model.R)
utils::globalVariables(c(
  "batting_team_runs_skill",
  "batting_team_wicket_skill",
  "bowling_team_runs_skill",
  "bowling_team_wicket_skill",
  "batting_team_balls",
  "bowling_team_balls",
  "format_t20",
  "format_odi",
  "batter_experience",
  "bowler_experience",
  "team1_team_runs_skill",
  "team2_team_runs_skill",
  "team1_team_wicket_skill",
  "team2_team_wicket_skill",
  "venue_run_rate_skill",
  "venue_wicket_rate_skill"
))

# Visualization variables (from plotting functions)
utils::globalVariables(c(
  # plot_elo_history
  "elo_result",
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

# Score projection constants (from constants.R and score_projection.R)
utils::globalVariables(c(
  # EIS constants
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
  # Max balls
  "MAX_BALLS_T20",
  "MAX_BALLS_ODI",
  "MAX_BALLS_TEST",
  # Test overs per day
  "TEST_OVERS_PER_DAY_5DAY",
  "TEST_OVERS_PER_DAY_4DAY",
  # Score projection table columns
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
