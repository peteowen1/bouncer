# Database Schema Functions
#
# Functions for creating the database schema (table definitions).
# Split from database_setup.R for better maintainability.

#' Create Database Schema
#'
#' Creates all tables needed for cricket data storage.
#'
#' @param conn A DuckDB connection object
#' @param verbose Logical. If TRUE, shows progress for each table. Default TRUE.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
create_schema <- function(conn, verbose = TRUE) {
  if (verbose) cli::cli_h2("Creating database schema")

  # Helper to log table creation
  log_table <- function(name) {
    if (verbose) cli::cli_alert_info("Creating {name} table...")
  }

  # Create matches table
  log_table("matches")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS matches (
      match_id VARCHAR PRIMARY KEY,
      season VARCHAR,
      match_type VARCHAR,
      match_type_number INTEGER,
      match_date DATE,
      venue VARCHAR,
      city VARCHAR,
      gender VARCHAR,
      team_type VARCHAR,

      -- Teams
      team1 VARCHAR,
      team2 VARCHAR,

      -- Match Details
      balls_per_over INTEGER,
      overs_per_innings INTEGER,

      -- Toss
      toss_winner VARCHAR,
      toss_decision VARCHAR,

      -- Outcome
      outcome_type VARCHAR,
      outcome_winner VARCHAR,
      outcome_by_runs INTEGER,
      outcome_by_wickets INTEGER,
      outcome_method VARCHAR,

      -- Match Officials
      umpire1 VARCHAR,
      umpire2 VARCHAR,
      tv_umpire VARCHAR,
      referee VARCHAR,
      reserve_umpire VARCHAR,

      -- Player of Match
      player_of_match_id VARCHAR,

      -- Data Quality
      missing_data VARCHAR,

      -- Event Info
      event_name VARCHAR,
      event_match_number INTEGER,
      event_group VARCHAR,

      -- Meta (from JSON)
      data_version VARCHAR,
      data_created DATE,
      data_revision INTEGER
    )
  ")

  # Create deliveries table
  log_table("deliveries")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS deliveries (
      -- Primary Keys
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,

      -- Match Context
      season VARCHAR,
      match_type VARCHAR,
      match_date DATE,
      venue VARCHAR,
      city VARCHAR,
      gender VARCHAR,

      -- Teams
      batting_team VARCHAR,
      bowling_team VARCHAR,

      -- Innings Context
      innings INTEGER,
      over INTEGER,
      ball INTEGER,
      over_ball DECIMAL(5,1),  -- Supports up to 9999.9 (Test matches can exceed 100 overs)

      -- Players
      batter_id VARCHAR,
      bowler_id VARCHAR,
      non_striker_id VARCHAR,

      -- Outcomes
      runs_batter INTEGER,
      runs_extras INTEGER,
      runs_total INTEGER,
      is_boundary BOOLEAN,
      is_four BOOLEAN,
      is_six BOOLEAN,

      -- Extras Detail
      wides INTEGER,
      noballs INTEGER,
      byes INTEGER,
      legbyes INTEGER,
      penalty INTEGER,

      -- Wicket Information
      is_wicket BOOLEAN,
      wicket_kind VARCHAR,
      player_out_id VARCHAR,
      fielder1_id VARCHAR,
      fielder2_id VARCHAR,
      fielder1_is_sub BOOLEAN,
      fielder2_is_sub BOOLEAN,

      -- DRS Review Information
      has_review BOOLEAN,
      review_by VARCHAR,
      review_umpire VARCHAR,
      review_batter VARCHAR,
      review_decision VARCHAR,

      -- Player Replacement (concussion sub, impact player)
      has_replacement BOOLEAN,
      replacement_in VARCHAR,
      replacement_out VARCHAR,
      replacement_reason VARCHAR,
      replacement_role VARCHAR,

      -- Match State (running totals)
      total_runs INTEGER,
      wickets_fallen INTEGER
    )
  ")

  # Create players table
  log_table("players")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS players (
      player_id VARCHAR PRIMARY KEY,
      player_name VARCHAR,
      country VARCHAR,
      dob DATE,
      batting_style VARCHAR,
      bowling_style VARCHAR
    )
  ")

  # Create match_innings table
  log_table("match_innings")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS match_innings (
      match_id VARCHAR,
      innings INTEGER,
      batting_team VARCHAR,
      bowling_team VARCHAR,
      total_runs INTEGER,
      total_wickets INTEGER,
      total_overs DECIMAL(5,1),  -- Supports up to 9999.9 (Test innings can exceed 100 overs)
      declared BOOLEAN,
      forfeited BOOLEAN,
      target_runs INTEGER,       -- Chase target (2nd innings only in limited overs)
      target_overs INTEGER,      -- Overs available for chase (may differ from match overs due to DLS)
      is_super_over BOOLEAN,     -- TRUE for super over innings
      absent_hurt VARCHAR,       -- JSON array of player names who couldn't bat
      PRIMARY KEY (match_id, innings)
    )
  ")

  # Create innings_powerplays table
  log_table("innings_powerplays")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS innings_powerplays (
      powerplay_id VARCHAR PRIMARY KEY,  -- {match_id}_{innings}_{seq}
      match_id VARCHAR NOT NULL,
      innings INTEGER NOT NULL,
      from_over REAL NOT NULL,           -- e.g., 0.1
      to_over REAL NOT NULL,             -- e.g., 9.6
      powerplay_type VARCHAR NOT NULL    -- 'mandatory', 'batting', 'bowling'
    )
  ")

  # Create match_metrics table (derived/calculated match data, separate from raw data)
  log_table("match_metrics")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS match_metrics (
      match_id VARCHAR PRIMARY KEY,

      -- Margin of Victory (calculated from outcome)
      unified_margin DOUBLE,

      -- Future: other calculated match-level metrics can go here
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP
    )
  ")

  # Create player_elo_history table
  log_table("player_elo_history")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS player_elo_history (
      player_id VARCHAR,
      match_id VARCHAR,
      match_date DATE,
      match_type VARCHAR,

      -- Overall ELO
      elo_batting DOUBLE,
      elo_bowling DOUBLE,

      -- Format-specific
      elo_batting_test DOUBLE,
      elo_batting_odi DOUBLE,
      elo_batting_t20 DOUBLE,
      elo_bowling_test DOUBLE,
      elo_bowling_odi DOUBLE,
      elo_bowling_t20 DOUBLE,

      -- Opposition-adjusted (stored as JSON for flexibility)
      elo_batting_vs_opposition VARCHAR,
      elo_bowling_vs_opposition VARCHAR,

      PRIMARY KEY (player_id, match_id)
    )
  ")

  # Create team_elo table
  log_table("team_elo")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS team_elo (
      team_id VARCHAR,
      match_id VARCHAR,
      match_date DATE,
      match_type VARCHAR,
      event_name VARCHAR,

      -- Result-based ELO (traditional, updates on wins/losses)
      elo_result DOUBLE,

      -- Roster-based ELO (aggregated from player ELOs)
      elo_roster_batting DOUBLE,
      elo_roster_bowling DOUBLE,
      elo_roster_combined DOUBLE,

      -- Context
      matches_played INTEGER,

      PRIMARY KEY (team_id, match_id)
    )
  ")

  # Create pre_match_features table
  log_table("pre_match_features")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS pre_match_features (
      match_id VARCHAR PRIMARY KEY,
      match_date DATE,
      match_type VARCHAR,
      event_name VARCHAR,
      team1 VARCHAR,
      team2 VARCHAR,

      -- Team 1 ELO Features
      team1_elo_result DOUBLE,
      team1_elo_roster DOUBLE,
      team1_form_last5 DOUBLE,
      team1_h2h_wins INTEGER,
      team1_h2h_total INTEGER,

      -- Team 1 Skill Index Features (from player skill indices)
      team1_bat_scoring_avg DOUBLE,     -- Avg BSI (runs/ball) for batters
      team1_bat_scoring_top5 DOUBLE,    -- Avg BSI for top 5 batters
      team1_bat_survival_avg DOUBLE,    -- Avg survival rate
      team1_bowl_economy_avg DOUBLE,    -- Avg runs conceded/ball
      team1_bowl_economy_top5 DOUBLE,   -- Avg economy for top 5 bowlers
      team1_bowl_strike_avg DOUBLE,     -- Avg wickets/ball

      -- Team 2 ELO Features
      team2_elo_result DOUBLE,
      team2_elo_roster DOUBLE,
      team2_form_last5 DOUBLE,
      team2_h2h_wins INTEGER,
      team2_h2h_total INTEGER,

      -- Team 2 Skill Index Features
      team2_bat_scoring_avg DOUBLE,
      team2_bat_scoring_top5 DOUBLE,
      team2_bat_survival_avg DOUBLE,
      team2_bowl_economy_avg DOUBLE,
      team2_bowl_economy_top5 DOUBLE,
      team2_bowl_strike_avg DOUBLE,

      -- Venue Features
      venue VARCHAR,
      venue_avg_score DOUBLE,
      venue_chase_success_rate DOUBLE,
      venue_matches INTEGER,

      -- Match Context
      is_knockout BOOLEAN,
      is_neutral_venue BOOLEAN,

      -- Margin of Victory Features
      expected_margin DOUBLE,  -- Predicted margin from ELO difference
      actual_margin DOUBLE,    -- Unified margin (backfilled from matches)

      created_at TIMESTAMP
    )
  ")

  # Create pre_match_predictions table
  log_table("pre_match_predictions")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS pre_match_predictions (
      prediction_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      model_version VARCHAR,
      model_type VARCHAR,
      prediction_date TIMESTAMP,

      team1_win_prob DOUBLE,
      team2_win_prob DOUBLE,
      predicted_winner VARCHAR,
      confidence DOUBLE,

      -- Actual outcomes (filled post-match)
      actual_winner VARCHAR,
      prediction_correct BOOLEAN
    )
  ")

  # Create simulation_results table
  log_table("simulation_results")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS simulation_results (
      simulation_id VARCHAR PRIMARY KEY,
      simulation_type VARCHAR,
      event_name VARCHAR,
      season VARCHAR,
      simulation_date TIMESTAMP,
      n_simulations INTEGER,
      parameters VARCHAR,
      team_results VARCHAR,
      match_results VARCHAR,
      created_at TIMESTAMP
    )
  ")

  # Create format-specific ELO tables (T20)
  log_table("t20_player_elo")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS t20_player_elo (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      batter_id VARCHAR,
      bowler_id VARCHAR,

      -- Run-based ELO (scoring ability)
      batter_run_elo_before DOUBLE,
      batter_run_elo_after DOUBLE,
      bowler_run_elo_before DOUBLE,
      bowler_run_elo_after DOUBLE,

      -- Wicket-based ELO (survival/strike ability)
      batter_wicket_elo_before DOUBLE,
      batter_wicket_elo_after DOUBLE,
      bowler_wicket_elo_before DOUBLE,
      bowler_wicket_elo_after DOUBLE,

      -- For calibration validation
      exp_runs DOUBLE,
      exp_wicket DOUBLE,
      actual_runs INTEGER,
      is_wicket BOOLEAN
    )
  ")

  # Create ELO calibration metrics table
  log_table("elo_calibration_metrics")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS elo_calibration_metrics (
      format VARCHAR,
      metric_type VARCHAR,
      metric_key VARCHAR,
      metric_value DOUBLE,
      sample_size INTEGER,
      calculated_date DATE,
      PRIMARY KEY (format, metric_type, metric_key)
    )
  ")

  # Create ELO normalization log table
  log_table("elo_normalization_log")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS elo_normalization_log (
      normalization_id VARCHAR PRIMARY KEY,
      format VARCHAR,
      elo_type VARCHAR,
      normalization_date DATE,
      mean_before DOUBLE,
      mean_after DOUBLE,
      adjustment DOUBLE,
      players_affected INTEGER,
      created_at TIMESTAMP
    )
  ")

  # Create ELO calculation params table (for incremental updates)
  log_table("elo_calculation_params")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS elo_calculation_params (
      format VARCHAR PRIMARY KEY,
      k_run DOUBLE,
      k_wicket DOUBLE,
      k_wicket_survival DOUBLE,
      run_score_wicket DOUBLE,
      run_score_dot DOUBLE,
      run_score_single DOUBLE,
      run_score_two DOUBLE,
      run_score_three DOUBLE,
      run_score_four DOUBLE,
      run_score_six DOUBLE,
      elo_start DOUBLE,
      elo_divisor DOUBLE,
      last_delivery_id VARCHAR,
      last_match_date DATE,
      total_deliveries INTEGER,
      calculated_at TIMESTAMP
    )
  ")

  # Create T20 player skill table (EMA-based, drift-proof)
  log_table("t20_player_skill")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS t20_player_skill (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      batter_id VARCHAR,
      bowler_id VARCHAR,

      -- Batter skill indices (before this delivery)
      batter_scoring_index DOUBLE,    -- EMA of runs per ball
      batter_survival_rate DOUBLE,    -- EMA of survival (1 - wicket rate)

      -- Bowler skill indices (before this delivery)
      bowler_economy_index DOUBLE,    -- EMA of runs conceded per ball
      bowler_strike_rate DOUBLE,      -- EMA of wickets per ball

      -- Expected values (from skill indices)
      exp_runs DOUBLE,
      exp_wicket DOUBLE,

      -- Actual outcomes
      actual_runs INTEGER,
      is_wicket BOOLEAN,

      -- Ball counts (for reliability)
      batter_balls_faced INTEGER,
      bowler_balls_bowled INTEGER
    )
  ")

  # Create skill calculation params table
  log_table("skill_calculation_params")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS skill_calculation_params (
      format VARCHAR PRIMARY KEY,
      alpha DOUBLE,
      start_runs DOUBLE,
      start_survival DOUBLE,
      last_delivery_id VARCHAR,
      last_match_date DATE,
      total_deliveries INTEGER,
      calculated_at TIMESTAMP
    )
  ")

  # Create venue_aliases table for venue name normalization
  log_table("venue_aliases")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS venue_aliases (
      alias VARCHAR PRIMARY KEY,
      canonical_venue VARCHAR NOT NULL,
      country VARCHAR,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  ")

  # Create T20 venue skill table (EMA-based venue characteristics)
  log_table("t20_venue_skill")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS t20_venue_skill (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      venue VARCHAR,

      -- Venue skill indices (before this delivery)
      venue_run_rate DOUBLE,        -- EMA of runs per ball at venue
      venue_wicket_rate DOUBLE,     -- EMA of wickets per ball at venue
      venue_boundary_rate DOUBLE,   -- EMA of boundaries (4s + 6s) per ball
      venue_dot_rate DOUBLE,        -- EMA of dot balls per ball

      -- Ball count (for reliability)
      venue_balls INTEGER,

      -- Actual outcomes
      actual_runs INTEGER,
      is_wicket BOOLEAN,
      is_boundary BOOLEAN,
      is_dot BOOLEAN
    )
  ")

  # Create ODI venue skill table
  log_table("odi_venue_skill")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS odi_venue_skill (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      venue VARCHAR,

      -- Venue skill indices (before this delivery)
      venue_run_rate DOUBLE,
      venue_wicket_rate DOUBLE,
      venue_boundary_rate DOUBLE,
      venue_dot_rate DOUBLE,

      -- Ball count (for reliability)
      venue_balls INTEGER,

      -- Actual outcomes
      actual_runs INTEGER,
      is_wicket BOOLEAN,
      is_boundary BOOLEAN,
      is_dot BOOLEAN
    )
  ")

  # Create Test venue skill table
  log_table("test_venue_skill")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS test_venue_skill (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      venue VARCHAR,

      -- Venue skill indices (before this delivery)
      venue_run_rate DOUBLE,
      venue_wicket_rate DOUBLE,
      venue_boundary_rate DOUBLE,
      venue_dot_rate DOUBLE,

      -- Ball count (for reliability)
      venue_balls INTEGER,

      -- Actual outcomes
      actual_runs INTEGER,
      is_wicket BOOLEAN,
      is_boundary BOOLEAN,
      is_dot BOOLEAN
    )
  ")

  # Create venue skill calculation params table
  log_table("venue_skill_calculation_params")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS venue_skill_calculation_params (
      format VARCHAR PRIMARY KEY,
      alpha DOUBLE,
      start_run_rate DOUBLE,
      start_wicket_rate DOUBLE,
      start_boundary_rate DOUBLE,
      start_dot_rate DOUBLE,
      last_delivery_id VARCHAR,
      last_match_date DATE,
      total_deliveries INTEGER,
      calculated_at TIMESTAMP
    )
  ")

  # Create T20 team skill table (per-delivery team skill indices)
  log_table("t20_team_skill")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS t20_team_skill (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      batting_team_id VARCHAR,
      bowling_team_id VARCHAR,

      -- Batting team skill indices (before this delivery)
      batting_team_runs_skill DOUBLE,    -- EMA of runs residual when batting
      batting_team_wicket_skill DOUBLE,  -- EMA of wicket residual when batting

      -- Bowling team skill indices (before this delivery)
      bowling_team_runs_skill DOUBLE,    -- EMA of runs residual when bowling
      bowling_team_wicket_skill DOUBLE,  -- EMA of wicket residual when bowling

      -- Agnostic model expectations
      exp_runs_agnostic DOUBLE,
      exp_wicket_agnostic DOUBLE,

      -- Actual outcomes
      actual_runs INTEGER,
      is_wicket BOOLEAN,

      -- Ball counts (for reliability)
      batting_team_balls INTEGER,
      bowling_team_balls INTEGER
    )
  ")

  # Create ODI team skill table
  log_table("odi_team_skill")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS odi_team_skill (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      batting_team_id VARCHAR,
      bowling_team_id VARCHAR,

      batting_team_runs_skill DOUBLE,
      batting_team_wicket_skill DOUBLE,
      bowling_team_runs_skill DOUBLE,
      bowling_team_wicket_skill DOUBLE,

      exp_runs_agnostic DOUBLE,
      exp_wicket_agnostic DOUBLE,

      actual_runs INTEGER,
      is_wicket BOOLEAN,

      batting_team_balls INTEGER,
      bowling_team_balls INTEGER
    )
  ")

  # Create Test team skill table
  log_table("test_team_skill")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS test_team_skill (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      batting_team_id VARCHAR,
      bowling_team_id VARCHAR,

      batting_team_runs_skill DOUBLE,
      batting_team_wicket_skill DOUBLE,
      bowling_team_runs_skill DOUBLE,
      bowling_team_wicket_skill DOUBLE,

      exp_runs_agnostic DOUBLE,
      exp_wicket_agnostic DOUBLE,

      actual_runs INTEGER,
      is_wicket BOOLEAN,

      batting_team_balls INTEGER,
      bowling_team_balls INTEGER
    )
  ")

  # Create team skill calculation params table
  log_table("team_skill_calculation_params")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS team_skill_calculation_params (
      format VARCHAR PRIMARY KEY,
      alpha DOUBLE,
      start_runs_skill DOUBLE,
      start_wicket_skill DOUBLE,
      last_delivery_id VARCHAR,
      last_match_date DATE,
      total_deliveries INTEGER,
      calculated_at TIMESTAMP
    )
  ")

  # Create projection parameters table
  log_table("projection_params")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS projection_params (
      segment_id VARCHAR PRIMARY KEY,
      format VARCHAR,
      gender VARCHAR,
      team_type VARCHAR,

      -- Optimized parameters
      param_a DOUBLE,
      param_b DOUBLE,
      param_z DOUBLE,
      param_y DOUBLE,

      -- Agnostic EIS (format average)
      eis_agnostic DOUBLE,

      -- Optimization metrics
      train_rmse DOUBLE,
      validation_rmse DOUBLE,
      n_innings INTEGER,

      -- Metadata
      optimized_at TIMESTAMP,
      version VARCHAR
    )
  ")

  # Create T20 score projection table (per-delivery projections)
  log_table("t20_score_projection")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS t20_score_projection (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      innings INTEGER,
      batting_team_id VARCHAR,

      -- Current state
      current_score INTEGER,
      balls_remaining INTEGER,
      wickets_remaining INTEGER,

      -- Resource values
      resource_remaining DOUBLE,
      resource_used DOUBLE,

      -- Expected initial scores
      eis_agnostic DOUBLE,
      eis_full DOUBLE,

      -- Projected scores
      projected_agnostic DOUBLE,
      projected_full DOUBLE,

      -- Actual outcome (for validation)
      final_innings_total INTEGER,

      -- Change from previous delivery (for attribution)
      projection_change_agnostic DOUBLE,
      projection_change_full DOUBLE
    )
  ")

  # Create ODI score projection table
  log_table("odi_score_projection")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS odi_score_projection (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      innings INTEGER,
      batting_team_id VARCHAR,

      current_score INTEGER,
      balls_remaining INTEGER,
      wickets_remaining INTEGER,

      resource_remaining DOUBLE,
      resource_used DOUBLE,

      eis_agnostic DOUBLE,
      eis_full DOUBLE,

      projected_agnostic DOUBLE,
      projected_full DOUBLE,

      final_innings_total INTEGER,

      projection_change_agnostic DOUBLE,
      projection_change_full DOUBLE
    )
  ")

  # Create Test score projection table
  log_table("test_score_projection")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS test_score_projection (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      innings INTEGER,
      batting_team_id VARCHAR,

      current_score INTEGER,
      balls_remaining INTEGER,
      wickets_remaining INTEGER,

      resource_remaining DOUBLE,
      resource_used DOUBLE,

      eis_agnostic DOUBLE,
      eis_full DOUBLE,

      projected_agnostic DOUBLE,
      projected_full DOUBLE,

      final_innings_total INTEGER,

      projection_change_agnostic DOUBLE,
      projection_change_full DOUBLE
    )
  ")

  # Create pipeline state table (for smart caching)
  log_table("pipeline_state")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS pipeline_state (
      step_name VARCHAR PRIMARY KEY,
      last_run_at TIMESTAMP,
      last_match_date DATE,
      last_match_count INTEGER,
      last_delivery_count INTEGER,
      status VARCHAR
    )
  ")

  # Create 3-way ELO tables (delegated to database_3way_elo.R)
  create_3way_elo_schema_tables(conn, verbose)

  n_tables <- length(DBI::dbListTables(conn))
  cli::cli_alert_success("Schema created successfully ({n_tables} tables)")
  invisible(TRUE)
}


#' Create 3-Way ELO Schema Tables
#'
#' Creates the format-specific 3-way ELO tables and params table.
#' Called by create_schema().
#'
#' @param conn A DuckDB connection object
#' @param verbose Logical. If TRUE, shows progress.
#'
#' @return Invisibly returns TRUE
#' @keywords internal
create_3way_elo_schema_tables <- function(conn, verbose = TRUE) {
  log_table <- function(name) {
    if (verbose) cli::cli_alert_info("Creating {name} table...")
  }

  # Create T20 3-way ELO table
  log_table("t20_3way_elo")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS t20_3way_elo (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      batter_id VARCHAR,
      bowler_id VARCHAR,
      venue VARCHAR,

      -- Player Run ELOs (before/after)
      batter_run_elo_before DOUBLE,
      batter_run_elo_after DOUBLE,
      bowler_run_elo_before DOUBLE,
      bowler_run_elo_after DOUBLE,

      -- Player Wicket ELOs (before/after)
      batter_wicket_elo_before DOUBLE,
      batter_wicket_elo_after DOUBLE,
      bowler_wicket_elo_before DOUBLE,
      bowler_wicket_elo_after DOUBLE,

      -- Venue PERMANENT ELOs (slow-changing ground characteristics)
      venue_perm_run_elo_before DOUBLE,
      venue_perm_run_elo_after DOUBLE,
      venue_perm_wicket_elo_before DOUBLE,
      venue_perm_wicket_elo_after DOUBLE,

      -- Venue SESSION ELOs (day-specific conditions, resets each match)
      venue_session_run_elo_before DOUBLE,
      venue_session_run_elo_after DOUBLE,
      venue_session_wicket_elo_before DOUBLE,
      venue_session_wicket_elo_after DOUBLE,

      -- K-factors used (for analysis/debugging)
      k_batter_run DOUBLE,
      k_bowler_run DOUBLE,
      k_venue_perm_run DOUBLE,
      k_venue_session_run DOUBLE,
      k_batter_wicket DOUBLE,
      k_bowler_wicket DOUBLE,
      k_venue_perm_wicket DOUBLE,
      k_venue_session_wicket DOUBLE,

      -- Predictions vs actuals
      exp_runs DOUBLE,
      exp_wicket DOUBLE,
      actual_runs INTEGER,
      is_wicket BOOLEAN,

      -- Ball counts & context
      batter_balls INTEGER,
      bowler_balls INTEGER,
      venue_balls INTEGER,
      balls_in_match INTEGER,
      days_inactive_batter INTEGER,
      days_inactive_bowler INTEGER,

      -- Situational flags (for analysis)
      is_knockout BOOLEAN,
      event_tier INTEGER,
      phase VARCHAR
    )
  ")

  # Create ODI 3-way ELO table
  log_table("odi_3way_elo")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS odi_3way_elo (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      batter_id VARCHAR,
      bowler_id VARCHAR,
      venue VARCHAR,

      batter_run_elo_before DOUBLE,
      batter_run_elo_after DOUBLE,
      bowler_run_elo_before DOUBLE,
      bowler_run_elo_after DOUBLE,

      batter_wicket_elo_before DOUBLE,
      batter_wicket_elo_after DOUBLE,
      bowler_wicket_elo_before DOUBLE,
      bowler_wicket_elo_after DOUBLE,

      venue_perm_run_elo_before DOUBLE,
      venue_perm_run_elo_after DOUBLE,
      venue_perm_wicket_elo_before DOUBLE,
      venue_perm_wicket_elo_after DOUBLE,

      venue_session_run_elo_before DOUBLE,
      venue_session_run_elo_after DOUBLE,
      venue_session_wicket_elo_before DOUBLE,
      venue_session_wicket_elo_after DOUBLE,

      k_batter_run DOUBLE,
      k_bowler_run DOUBLE,
      k_venue_perm_run DOUBLE,
      k_venue_session_run DOUBLE,
      k_batter_wicket DOUBLE,
      k_bowler_wicket DOUBLE,
      k_venue_perm_wicket DOUBLE,
      k_venue_session_wicket DOUBLE,

      exp_runs DOUBLE,
      exp_wicket DOUBLE,
      actual_runs INTEGER,
      is_wicket BOOLEAN,

      batter_balls INTEGER,
      bowler_balls INTEGER,
      venue_balls INTEGER,
      balls_in_match INTEGER,
      days_inactive_batter INTEGER,
      days_inactive_bowler INTEGER,

      is_knockout BOOLEAN,
      event_tier INTEGER,
      phase VARCHAR
    )
  ")

  # Create Test 3-way ELO table
  log_table("test_3way_elo")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS test_3way_elo (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      batter_id VARCHAR,
      bowler_id VARCHAR,
      venue VARCHAR,

      batter_run_elo_before DOUBLE,
      batter_run_elo_after DOUBLE,
      bowler_run_elo_before DOUBLE,
      bowler_run_elo_after DOUBLE,

      batter_wicket_elo_before DOUBLE,
      batter_wicket_elo_after DOUBLE,
      bowler_wicket_elo_before DOUBLE,
      bowler_wicket_elo_after DOUBLE,

      venue_perm_run_elo_before DOUBLE,
      venue_perm_run_elo_after DOUBLE,
      venue_perm_wicket_elo_before DOUBLE,
      venue_perm_wicket_elo_after DOUBLE,

      venue_session_run_elo_before DOUBLE,
      venue_session_run_elo_after DOUBLE,
      venue_session_wicket_elo_before DOUBLE,
      venue_session_wicket_elo_after DOUBLE,

      k_batter_run DOUBLE,
      k_bowler_run DOUBLE,
      k_venue_perm_run DOUBLE,
      k_venue_session_run DOUBLE,
      k_batter_wicket DOUBLE,
      k_bowler_wicket DOUBLE,
      k_venue_perm_wicket DOUBLE,
      k_venue_session_wicket DOUBLE,

      exp_runs DOUBLE,
      exp_wicket DOUBLE,
      actual_runs INTEGER,
      is_wicket BOOLEAN,

      batter_balls INTEGER,
      bowler_balls INTEGER,
      venue_balls INTEGER,
      balls_in_match INTEGER,
      days_inactive_batter INTEGER,
      days_inactive_bowler INTEGER,

      is_knockout BOOLEAN,
      event_tier INTEGER,
      phase VARCHAR
    )
  ")

  # Create 3-way ELO params table (for storing optimized parameters)
  log_table("three_way_elo_params")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS three_way_elo_params (
      format VARCHAR PRIMARY KEY,

      -- Player K-factor parameters
      k_run_max DOUBLE,
      k_run_min DOUBLE,
      k_run_halflife DOUBLE,
      k_wicket_max DOUBLE,
      k_wicket_min DOUBLE,
      k_wicket_halflife DOUBLE,

      -- Venue K-factor parameters
      k_venue_perm_max DOUBLE,
      k_venue_perm_min DOUBLE,
      k_venue_perm_halflife DOUBLE,
      k_venue_session_base DOUBLE,
      k_venue_session_min DOUBLE,
      k_venue_session_halflife DOUBLE,

      -- Attribution weights
      w_batter DOUBLE,
      w_bowler DOUBLE,
      w_venue DOUBLE,
      venue_w_permanent DOUBLE,
      venue_w_session DOUBLE,

      -- Other parameters
      runs_per_100_elo DOUBLE,
      inactivity_halflife DOUBLE,
      replacement_level DOUBLE,

      -- Tracking
      last_delivery_id VARCHAR,
      last_match_date DATE,
      total_deliveries INTEGER,
      calculated_at TIMESTAMP
    )
  ")

  # Create 3-way ELO drift metrics table (for monitoring)
  log_table("three_way_elo_drift_metrics")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS three_way_elo_drift_metrics (
      metric_date DATE,
      format VARCHAR,
      dimension VARCHAR,
      entity_type VARCHAR,
      mean_elo DOUBLE,
      std_elo DOUBLE,
      PRIMARY KEY (metric_date, format, dimension, entity_type)
    )
  ")

 invisible(TRUE)
}
