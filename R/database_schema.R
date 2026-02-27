# Database Schema Functions
#
# Functions for creating the database schema (table definitions).
# Split from database_setup.R for better maintainability.

#' Create DuckDB Schemas
#'
#' Creates the cricsheet and cricinfo schemas for namespace isolation.
#'
#' @param conn A DuckDB connection object
#'
#' @return Invisibly returns TRUE
#' @keywords internal
create_schemas <- function(conn) {
  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS cricsheet")
  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS cricinfo")
  invisible(TRUE)
}


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

  # Create schemas for namespacing
  create_schemas(conn)

  # Helper to log table creation
  log_table <- function(name) {
    if (verbose) cli::cli_alert_info("Creating {name} table...")
  }

  # Create matches table
  log_table("cricsheet.matches")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS cricsheet.matches (
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
  log_table("cricsheet.deliveries")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS cricsheet.deliveries (
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
  log_table("cricsheet.players")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS cricsheet.players (
      player_id VARCHAR PRIMARY KEY,
      player_name VARCHAR,
      country VARCHAR,
      dob DATE,
      batting_style VARCHAR,
      bowling_style VARCHAR
    )
  ")

  # Create match_innings table
  log_table("cricsheet.match_innings")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS cricsheet.match_innings (
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
  log_table("cricsheet.innings_powerplays")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS cricsheet.innings_powerplays (
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

  # Create centrality/PageRank history tables (per format)
  create_centrality_schema_tables(conn, verbose)

  # Create Cricinfo tables (rich ball-by-ball, matches, innings, fixtures)
  create_cricinfo_tables(conn, verbose)

  n_tables <- length(DBI::dbListTables(conn))
  cli::cli_alert_success("Schema created successfully ({n_tables} tables)")
  invisible(TRUE)
}


#' Create Centrality Schema Tables
#'
#' Creates format-specific centrality history tables for each format and gender.
#' Called by create_schema().
#'
#' @param conn A DuckDB connection object
#' @param verbose Logical. If TRUE, shows progress.
#'
#' @return Invisibly returns TRUE
#' @keywords internal
create_centrality_schema_tables <- function(conn, verbose = TRUE) {
  log_table <- function(name) {
    if (verbose) cli::cli_alert_info("Creating {name} table...")
  }

  formats <- c("t20", "odi", "test")
  genders <- c("mens", "womens")

  for (gender in genders) {
    for (format in formats) {
      table_name <- paste0(gender, "_", format, "_player_centrality_history")
      log_table(table_name)

      DBI::dbExecute(conn, sprintf("
        CREATE TABLE IF NOT EXISTS %s (
          snapshot_date DATE NOT NULL,
          player_id VARCHAR NOT NULL,
          role VARCHAR NOT NULL,
          centrality DOUBLE,
          percentile DOUBLE,
          quality_tier VARCHAR,
          deliveries INTEGER,
          unique_opponents INTEGER,
          avg_opponent_degree DOUBLE,
          PRIMARY KEY (snapshot_date, player_id, role)
        )
      ", table_name))

      DBI::dbExecute(conn, sprintf("
        CREATE INDEX IF NOT EXISTS idx_%s_player_date
        ON %s (player_id, role, snapshot_date DESC)
      ", table_name, table_name))
    }
  }

  invisible(TRUE)
}


#' Create Cricinfo Tables
#'
#' Creates tables for Cricinfo rich ball-by-ball data, match metadata,
#' batting scorecards, and fixtures index. These are separate from the
#' Cricsheet-based tables because Cricinfo data includes Hawkeye fields
#' (wagon wheel, pitch map, shot type) not available in Cricsheet.
#'
#' @param conn A DuckDB connection object
#' @param verbose Logical. If TRUE, shows progress. Default TRUE.
#'
#' @return Invisibly returns TRUE
#' @keywords internal
create_cricinfo_tables <- function(conn, verbose = TRUE) {
  # Ensure cricinfo schema exists (idempotent)
  create_schemas(conn)

  log_table <- function(name) {
    if (verbose) cli::cli_alert_info("Creating {name} table...")
  }

  # Ball-by-ball with Hawkeye rich fields

  log_table("cricinfo.balls")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS cricinfo.balls (
      id VARCHAR,
      match_id VARCHAR,
      innings_number INTEGER,
      over_number DOUBLE,
      ball_number INTEGER,
      overs_actual DOUBLE,
      overs_unique DOUBLE,

      -- Runs/outcome
      total_runs INTEGER,
      batsman_runs INTEGER,
      is_four BOOLEAN,
      is_six BOOLEAN,
      is_wicket BOOLEAN,
      dismissal_type VARCHAR,
      dismissal_text VARCHAR,

      -- Extras
      wides INTEGER,
      noballs INTEGER,
      byes INTEGER,
      legbyes INTEGER,
      penalties INTEGER,

      -- Hawkeye rich fields
      wagon_x DOUBLE,
      wagon_y DOUBLE,
      wagon_zone VARCHAR,
      pitch_line VARCHAR,
      pitch_length VARCHAR,
      shot_type VARCHAR,
      shot_control VARCHAR,

      -- Players
      batsman_player_id VARCHAR,
      bowler_player_id VARCHAR,
      non_striker_player_id VARCHAR,
      out_player_id VARCHAR,

      -- Running totals
      total_innings_runs INTEGER,
      total_innings_wickets INTEGER,

      -- Win probability (T20I/ODI only)
      predicted_score INTEGER,
      win_probability DOUBLE,

      -- DRS / events
      event_type VARCHAR,
      drs_successful BOOLEAN,

      -- Commentary
      title VARCHAR,
      timestamp VARCHAR,

      PRIMARY KEY (match_id, id)
    )
  ")

  # Match metadata
  log_table("cricinfo.matches")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS cricinfo.matches (
      match_id VARCHAR PRIMARY KEY,
      title VARCHAR,
      series_id VARCHAR,
      series_name VARCHAR,
      format VARCHAR,
      international_class_id INTEGER,
      gender VARCHAR,
      start_date VARCHAR,
      end_date VARCHAR,
      start_time VARCHAR,
      status VARCHAR,
      status_text VARCHAR,
      slug VARCHAR,

      -- Venue
      ground_id VARCHAR,
      ground_name VARCHAR,
      ground_long_name VARCHAR,
      country_name VARCHAR,
      city_name VARCHAR,

      -- Toss/result
      toss_winner_team_id VARCHAR,
      toss_winner_choice VARCHAR,
      winner_team_id VARCHAR,
      scheduled_overs INTEGER,
      hawkeye_source VARCHAR,
      ball_by_ball_source VARCHAR,

      -- Teams
      team1_id VARCHAR,
      team1_name VARCHAR,
      team1_abbreviation VARCHAR,
      team1_captain_id VARCHAR,
      team1_is_home BOOLEAN,
      team2_id VARCHAR,
      team2_name VARCHAR,
      team2_abbreviation VARCHAR,
      team2_captain_id VARCHAR,
      team2_is_home BOOLEAN,

      -- Officials
      umpire1_id VARCHAR, umpire1_name VARCHAR,
      umpire2_id VARCHAR, umpire2_name VARCHAR,
      tv_umpire_id VARCHAR, tv_umpire_name VARCHAR,
      match_referee_id VARCHAR, match_referee_name VARCHAR,

      -- Awards
      potm_player_id VARCHAR,
      potm_player_name VARCHAR
    )
  ")

  # Batting scorecards (one row per batsman per innings)
  log_table("cricinfo.innings")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS cricinfo.innings (
      match_id VARCHAR,
      innings_number INTEGER,
      team_id VARCHAR,
      team_name VARCHAR,
      total_runs INTEGER,
      total_wickets INTEGER,
      total_overs DOUBLE,

      -- Per-batsman
      player_id VARCHAR,
      player_name VARCHAR,
      player_dob VARCHAR,
      batting_style VARCHAR,
      bowling_style VARCHAR,
      playing_role VARCHAR,
      runs INTEGER,
      balls_faced INTEGER,
      fours INTEGER,
      sixes INTEGER,
      strike_rate DOUBLE,
      is_not_out BOOLEAN,
      batting_position INTEGER,

      PRIMARY KEY (match_id, innings_number, player_id)
    )
  ")

  # Fixtures index (schedule + results for all discovered matches)
  log_table("cricinfo.fixtures")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS cricinfo.fixtures (
      match_id VARCHAR PRIMARY KEY,
      series_id VARCHAR,
      series_name VARCHAR,
      format VARCHAR,
      gender VARCHAR,
      status VARCHAR,
      start_date VARCHAR,
      start_time VARCHAR,
      title VARCHAR,
      team1 VARCHAR,
      team1_abbrev VARCHAR,
      team2 VARCHAR,
      team2_abbrev VARCHAR,
      venue VARCHAR,
      country VARCHAR,
      status_text VARCHAR,
      winner_team_id VARCHAR,
      has_ball_by_ball BOOLEAN
    )
  ")

  # Indexes for common query patterns
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_cricinfo_balls_match
    ON cricinfo.balls(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_cricinfo_balls_innings
    ON cricinfo.balls(match_id, innings_number)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_cricinfo_matches_format
    ON cricinfo.matches(format, gender)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_cricinfo_innings_match
    ON cricinfo.innings(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_cricinfo_fixtures_format
    ON cricinfo.fixtures(format, gender)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_cricinfo_fixtures_status
    ON cricinfo.fixtures(status)")

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


# ============================================================================
# 3-WAY ELO TABLE SCHEMA (from database_3way_elo.R)
# ============================================================================

create_3way_elo_table <- function(format, conn, overwrite = FALSE) {
  # Keep gender prefix in table name to avoid overwrites between men's and women's
  # e.g., "mens_t20" -> "mens_t20_3way_elo", "womens_t20" -> "womens_t20_3way_elo"
  table_name <- paste0(tolower(format), "_3way_elo")

  if (overwrite) {
    DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", table_name))
    cli::cli_alert_info("Dropped existing table: {table_name}")
  }

  # Check if table exists
  if (table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_info("Table {table_name} already exists")
    return(invisible(TRUE))
  }

  # Create the table
  DBI::dbExecute(conn, sprintf("
    CREATE TABLE IF NOT EXISTS %s (
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
  ", table_name))

  # Create indexes
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_match ON %s(match_id)",
                               gsub("_3way_elo", "_3way", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_batter ON %s(batter_id)",
                               gsub("_3way_elo", "_3way", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_bowler ON %s(bowler_id)",
                               gsub("_3way_elo", "_3way", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_venue ON %s(venue)",
                               gsub("_3way_elo", "_3way", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_date ON %s(match_date)",
                               gsub("_3way_elo", "_3way", table_name), table_name))

  cli::cli_alert_success("Created table: {table_name}")
  invisible(TRUE)
}


#' Insert 3-Way ELO Data
#'
#' Inserts 3-way ELO data into the format-specific table.
#'
#' @param df data.frame/data.table with 3-way ELO columns.
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#'
#' @return Invisibly returns TRUE on success.
#' @keywords internal
insert_3way_elos <- function(df, format, conn) {
  # Keep gender prefix in table name
  table_name <- paste0(tolower(format), "_3way_elo")

  # Check table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_danger("Table {table_name} does not exist")
    return(invisible(FALSE))
  }

  # Use DBI::dbWriteTable with append mode
  DBI::dbWriteTable(conn, table_name, df, append = TRUE, row.names = FALSE)

  invisible(TRUE)
}


#' Get 3-Way ELO Statistics
#'
#' Returns summary statistics for the 3-way ELO table.
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#'
#' @return List with statistics or NULL if table doesn't exist.
#' @keywords internal
get_3way_elo_stats <- function(format, conn) {
  # Keep gender prefix in table name
  table_name <- paste0(tolower(format), "_3way_elo")

  # Check table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  stats <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(*) as total_records,
      COUNT(DISTINCT batter_id) as unique_batters,
      COUNT(DISTINCT bowler_id) as unique_bowlers,
      COUNT(DISTINCT venue) as unique_venues,
      MIN(match_date) as first_date,
      MAX(match_date) as last_date,
      AVG(batter_run_elo_after) as mean_batter_run_elo,
      AVG(bowler_run_elo_after) as mean_bowler_run_elo,
      AVG(batter_wicket_elo_after) as mean_batter_wicket_elo,
      AVG(bowler_wicket_elo_after) as mean_bowler_wicket_elo,
      AVG(venue_perm_run_elo_after) as mean_venue_perm_run_elo,
      AVG(venue_session_run_elo_after) as mean_venue_session_run_elo
    FROM %s
  ", table_name))

  as.list(stats)
}


#' Delete 3-Way ELO Data for Matches
#'
#' Deletes 3-way ELO data for specified matches.
#'
#' @param match_ids Character vector of match IDs to delete.
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return Invisibly returns number of rows deleted.
#' @keywords internal
delete_3way_elo_matches <- function(match_ids, format, conn, verbose = TRUE) {
  if (length(match_ids) == 0) {
    return(invisible(0))
  }

  table_name <- paste0(tolower(format), "_3way_elo")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(invisible(0))
  }

  match_ids_sql <- paste0("'", escape_sql_quotes(match_ids), "'", collapse = ", ")
  sql <- sprintf("DELETE FROM %s WHERE match_id IN (%s)", table_name, match_ids_sql)

  n_deleted <- tryCatch({
    DBI::dbExecute(conn, sql)
  }, error = function(e) {
    if (verbose) cli::cli_alert_warning("Could not delete from {table_name}: {e$message}")
    0
  })

  if (n_deleted > 0 && verbose) {
    cli::cli_alert_success("Deleted {n_deleted} rows from {table_name}")
  }

  invisible(n_deleted)
}


#' Get Latest 3-Way ELO State for Players
#'
#' Returns the most recent ELO values for all players in a format.
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#' @param elo_type Character. "run" or "wicket". Default "run".
#'
#' @return data.frame with player_id, role (batter/bowler), and latest ELO.
#' @keywords internal
get_latest_3way_player_elos <- function(format, conn, elo_type = "run") {
  table_name <- paste0(tolower(format), "_3way_elo")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(data.frame())
  }

  elo_col <- if (elo_type == "run") "run_elo_after" else "wicket_elo_after"

  # Get latest batter ELOs
  batters <- DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        batter_id as player_id,
        batter_%s as elo,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT player_id, elo, match_date, 'batter' as role
    FROM ranked
    WHERE rn = 1
  ", elo_col, table_name))

  # Get latest bowler ELOs
  bowlers <- DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        bowler_id as player_id,
        bowler_%s as elo,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT player_id, elo, match_date, 'bowler' as role
    FROM ranked
    WHERE rn = 1
  ", elo_col, table_name))

  rbind(batters, bowlers)
}


#' Get Latest 3-Way ELO State for Venues
#'
#' Returns the most recent ELO values for all venues in a format.
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#'
#' @return data.frame with venue, permanent ELOs, and latest match date.
#' @keywords internal
get_latest_3way_venue_elos <- function(format, conn) {
  table_name <- paste0(tolower(format), "_3way_elo")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(data.frame())
  }

  DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        venue,
        venue_perm_run_elo_after,
        venue_perm_wicket_elo_after,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY venue ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT
      venue,
      venue_perm_run_elo_after as perm_run_elo,
      venue_perm_wicket_elo_after as perm_wicket_elo,
      match_date as last_match_date
    FROM ranked
    WHERE rn = 1
  ", table_name))
}


# ============================================================================
# SKILL INDICES TABLE SCHEMA (from database_skill_indices.R)
# ============================================================================

create_3way_skill_table <- function(format, conn, overwrite = FALSE) {
  # Keep gender prefix in table name to avoid overwrites between men's and women's
  # e.g., "mens_t20" -> "mens_t20_3way_skill", "womens_t20" -> "womens_t20_3way_skill"
  table_name <- paste0(tolower(format), "_3way_skill")

  if (overwrite) {
    DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", table_name))
    cli::cli_alert_info("Dropped existing table: {table_name}")
  }

  # Check if table exists
  if (table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_info("Table {table_name} already exists")
    return(invisible(TRUE))
  }

  # Create the table - columns use "skill" suffix instead of "elo"
  # Values are now centered at 0 (neutral) with typical range ±0.5
  DBI::dbExecute(conn, sprintf("
    CREATE TABLE IF NOT EXISTS %s (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      batter_id VARCHAR,
      bowler_id VARCHAR,
      venue VARCHAR,

      -- Player Run Skills (deviation from expected runs/ball)
      batter_run_skill_before DOUBLE,
      batter_run_skill_after DOUBLE,
      bowler_run_skill_before DOUBLE,
      bowler_run_skill_after DOUBLE,

      -- Player Wicket Skills (deviation from expected wicket probability)
      batter_wicket_skill_before DOUBLE,
      batter_wicket_skill_after DOUBLE,
      bowler_wicket_skill_before DOUBLE,
      bowler_wicket_skill_after DOUBLE,

      -- Venue Permanent Skills (long-term venue characteristics)
      venue_perm_run_skill_before DOUBLE,
      venue_perm_run_skill_after DOUBLE,
      venue_perm_wicket_skill_before DOUBLE,
      venue_perm_wicket_skill_after DOUBLE,

      -- Venue Session Skills (current match conditions, resets each match)
      venue_session_run_skill_before DOUBLE,
      venue_session_run_skill_after DOUBLE,
      venue_session_wicket_skill_before DOUBLE,
      venue_session_wicket_skill_after DOUBLE,

      -- Learning Rates (alpha, equivalent to K-factors)
      alpha_batter_run DOUBLE,
      alpha_bowler_run DOUBLE,
      alpha_venue_perm_run DOUBLE,
      alpha_venue_session_run DOUBLE,
      alpha_batter_wicket DOUBLE,
      alpha_bowler_wicket DOUBLE,
      alpha_venue_perm_wicket DOUBLE,
      alpha_venue_session_wicket DOUBLE,

      -- Predictions/actuals (from agnostic model)
      exp_runs DOUBLE,
      exp_wicket DOUBLE,
      actual_runs INTEGER,
      is_wicket BOOLEAN,

      -- Context
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
  ", table_name))

  # Create indexes
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_match ON %s(match_id)",
                               gsub("_3way_skill", "_3way_sk", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_batter ON %s(batter_id)",
                               gsub("_3way_skill", "_3way_sk", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_bowler ON %s(bowler_id)",
                               gsub("_3way_skill", "_3way_sk", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_venue ON %s(venue)",
                               gsub("_3way_skill", "_3way_sk", table_name), table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_date ON %s(match_date)",
                               gsub("_3way_skill", "_3way_sk", table_name), table_name))

  cli::cli_alert_success("Created table: {table_name}")
  invisible(TRUE)
}


#' Insert 3-Way Skill Index Data
#'
#' Inserts 3-way skill index data into the format-specific table.
#'
#' @param df data.frame/data.table with 3-way skill columns.
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#'
#' @return Invisibly returns TRUE on success.
#' @keywords internal
insert_3way_skills <- function(df, format, conn) {
  # Keep gender prefix in table name
  table_name <- paste0(tolower(format), "_3way_skill")

  # Check table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_danger("Table {table_name} does not exist")
    return(invisible(FALSE))
  }

  # Use DBI::dbWriteTable with append mode
  DBI::dbWriteTable(conn, table_name, df, append = TRUE, row.names = FALSE)

  invisible(TRUE)
}


#' Get 3-Way Skill Index Statistics
#'
#' Returns summary statistics for the 3-way skill index table.
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#'
#' @return List with statistics or NULL if table doesn't exist.
#' @keywords internal
get_3way_skill_stats <- function(format, conn) {
  # Keep gender prefix in table name
  table_name <- paste0(tolower(format), "_3way_skill")

  # Check table exists
  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  stats <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(*) as total_records,
      COUNT(DISTINCT batter_id) as unique_batters,
      COUNT(DISTINCT bowler_id) as unique_bowlers,
      COUNT(DISTINCT venue) as unique_venues,
      MIN(match_date) as first_date,
      MAX(match_date) as last_date,
      AVG(batter_run_skill_after) as mean_batter_run_skill,
      AVG(bowler_run_skill_after) as mean_bowler_run_skill,
      AVG(batter_wicket_skill_after) as mean_batter_wicket_skill,
      AVG(bowler_wicket_skill_after) as mean_bowler_wicket_skill,
      AVG(venue_perm_run_skill_after) as mean_venue_perm_run_skill,
      AVG(venue_session_run_skill_after) as mean_venue_session_run_skill,
      STDDEV(batter_run_skill_after) as sd_batter_run_skill,
      STDDEV(bowler_run_skill_after) as sd_bowler_run_skill,
      AVG(exp_runs) as mean_exp_runs,
      AVG(actual_runs) as mean_actual_runs,
      AVG(exp_wicket) as mean_exp_wicket,
      AVG(CAST(is_wicket AS DOUBLE)) as actual_wicket_rate
    FROM %s
  ", table_name))

  as.list(stats)
}


#' Delete 3-Way Skill Index Data for Matches
#'
#' Deletes 3-way skill index data for specified matches.
#'
#' @param match_ids Character vector of match IDs to delete.
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return Invisibly returns number of rows deleted.
#' @keywords internal
delete_3way_skill_matches <- function(match_ids, format, conn, verbose = TRUE) {
  if (length(match_ids) == 0) {
    return(invisible(0))
  }

  table_name <- paste0(tolower(format), "_3way_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(invisible(0))
  }

  match_ids_sql <- paste0("'", escape_sql_quotes(match_ids), "'", collapse = ", ")
  sql <- sprintf("DELETE FROM %s WHERE match_id IN (%s)", table_name, match_ids_sql)

  n_deleted <- tryCatch({
    DBI::dbExecute(conn, sql)
  }, error = function(e) {
    if (verbose) cli::cli_alert_warning("Could not delete from {table_name}: {e$message}")
    0
  })

  if (n_deleted > 0 && verbose) {
    cli::cli_alert_success("Deleted {n_deleted} rows from {table_name}")
  }

  invisible(n_deleted)
}


#' Get Latest 3-Way Skill Index State for Players
#'
#' Returns the most recent skill values for all players in a format.
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#' @param skill_type Character. "run" or "wicket". Default "run".
#'
#' @return data.frame with player_id, role (batter/bowler), and latest skill.
#' @keywords internal
get_latest_3way_player_skills <- function(format, conn, skill_type = "run") {
  table_name <- paste0(tolower(format), "_3way_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(data.frame())
  }

  skill_col <- if (skill_type == "run") "run_skill_after" else "wicket_skill_after"

  # Get latest batter skills
  batters <- DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        batter_id as player_id,
        batter_%s as skill,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT player_id, skill, match_date, 'batter' as role
    FROM ranked
    WHERE rn = 1
  ", skill_col, table_name))

  # Get latest bowler skills
  bowlers <- DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        bowler_id as player_id,
        bowler_%s as skill,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT player_id, skill, match_date, 'bowler' as role
    FROM ranked
    WHERE rn = 1
  ", skill_col, table_name))

  rbind(batters, bowlers)
}


#' Get Latest 3-Way Skill Index State for Venues
#'
#' Returns the most recent skill values for all venues in a format.
#'
#' @param format Character. Format identifier (e.g., "t20", "mens_t20").
#' @param conn A DuckDB connection object.
#'
#' @return data.frame with venue, permanent skills, and latest match date.
#' @keywords internal
get_latest_3way_venue_skills <- function(format, conn) {
  table_name <- paste0(tolower(format), "_3way_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(data.frame())
  }

  DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        venue,
        venue_perm_run_skill_after,
        venue_perm_wicket_skill_after,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY venue ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT
      venue,
      venue_perm_run_skill_after as perm_run_skill,
      venue_perm_wicket_skill_after as perm_wicket_skill,
      match_date as last_match_date
    FROM ranked
    WHERE rn = 1
  ", table_name))
}


#' Store 3-Way Skill Index Parameters
#'
#' Stores the parameters used for skill index calculation for reproducibility.
#'
#' @param params List of parameters used for calculation.
#' @param last_delivery_id Character. ID of last processed delivery.
#' @param last_match_date Date. Date of last processed match.
#' @param total_deliveries Integer. Total deliveries processed.
#' @param conn A DuckDB connection object.
#'
#' @return Invisibly returns TRUE on success.
#' @keywords internal
store_3way_skill_params <- function(params, last_delivery_id, last_match_date,
                                     total_deliveries, conn) {
  # Ensure table exists
  if (!"skill_index_params" %in% DBI::dbListTables(conn)) {
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS skill_index_params (
        format VARCHAR PRIMARY KEY,
        alpha_run_max DOUBLE,
        alpha_run_min DOUBLE,
        alpha_run_halflife DOUBLE,
        alpha_wicket_max DOUBLE,
        alpha_wicket_min DOUBLE,
        alpha_wicket_halflife DOUBLE,
        decay_rate DOUBLE,
        w_batter_run DOUBLE,
        w_bowler_run DOUBLE,
        w_venue_session_run DOUBLE,
        w_venue_perm_run DOUBLE,
        last_delivery_id VARCHAR,
        last_match_date DATE,
        total_deliveries INTEGER,
        calculated_at TIMESTAMP
      )
    ")
  }

  # Delete existing entry for this format
  DBI::dbExecute(conn, "DELETE FROM skill_index_params WHERE format = ?",
                 params = list(params$format))

  # Insert new parameters
  DBI::dbExecute(conn, "
    INSERT INTO skill_index_params
    (format, alpha_run_max, alpha_run_min, alpha_run_halflife,
     alpha_wicket_max, alpha_wicket_min, alpha_wicket_halflife,
     decay_rate, w_batter_run, w_bowler_run, w_venue_session_run, w_venue_perm_run,
     last_delivery_id, last_match_date, total_deliveries, calculated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ", params = list(
    params$format,
    params$alpha_run_max, params$alpha_run_min, params$alpha_run_halflife,
    params$alpha_wicket_max, params$alpha_wicket_min, params$alpha_wicket_halflife,
    params$decay_rate,
    params$w_batter_run, params$w_bowler_run,
    params$w_venue_session_run, params$w_venue_perm_run,
    last_delivery_id, as.character(last_match_date),
    total_deliveries, Sys.time()
  ))

  invisible(TRUE)
}


NULL
