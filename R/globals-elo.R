# ELO and Skill Index Global Variables
#
# This file contains global variable declarations for:
# - ELO rating system variables
# - Skill index variables (player, team, venue)
# - Event tier constants
# - Team correlation constants

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
# Event Tier Constants
# ============================================================================
# Used for K-factor adjustments based on competition importance

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
# Team Correlation Constants
# ============================================================================
# Used for cross-event rating propagation

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
# EMA-based skill tracking per delivery

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
# Aggregate team skills from player indices

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
# Venue-specific characteristics

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

# ============================================================================
# Player Metrics Variables
# ============================================================================
# Aggregated player statistics

utils::globalVariables(c(
  "player_out_id",
  "balls_faced",
  "runs_scored",
  "wickets",
  "balls_bowled"
))
