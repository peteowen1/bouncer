# Database Setup Functions for Bouncer

#' Initialize Bouncer Database
#'
#' Creates a new DuckDB database file with the schema for cricket data storage.
#'
#' @param path Character string specifying the database file path. If NULL,
#'   uses the default system data directory.
#' @param overwrite Logical. If TRUE, overwrites existing database. Default FALSE.
#' @param skip_indexes Logical. If TRUE, skips index creation. Useful when
#'   bulk loading data (indexes are faster to create after data is loaded).
#'   Default FALSE.
#' @param verbose Logical. If TRUE, shows progress for each table/index creation.
#'   Default FALSE for cleaner output.
#'
#' @return Invisibly returns the database path
#' @export
#'
#' @examples
#' \dontrun{
#' # Initialize database in default location
#' initialize_bouncer_database()
#'
#' # Initialize in custom location
#' initialize_bouncer_database("~/cricket_data/bouncer.duckdb")
#'
#' # Initialize without indexes (for bulk loading)
#' initialize_bouncer_database(skip_indexes = TRUE)
#'
#' # Initialize with detailed progress
#' initialize_bouncer_database(verbose = TRUE)
#' }
initialize_bouncer_database <- function(path = NULL, overwrite = FALSE, skip_indexes = FALSE, verbose = FALSE) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  # Create directory if it doesn't exist
  db_dir <- dirname(path)
  if (!dir.exists(db_dir)) {
    dir.create(db_dir, recursive = TRUE)
    cli::cli_alert_success("Created directory: {.file {db_dir}}")
  }

  # Check if database already exists
  if (file.exists(path) && !overwrite) {
    cli::cli_alert_warning("Database already exists at {.file {path}}")
    cli::cli_alert_info("Use overwrite = TRUE to replace it")
    return(invisible(path))
  }

  if (file.exists(path) && overwrite) {
    cli::cli_alert_warning("Overwriting existing database at {.file {path}}")
    file.remove(path)
  }

  # Create database and schema
  cli::cli_alert_info("Initializing DuckDB database...")

  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
  on.exit(DBI::dbDisconnect(conn, shutdown = FALSE))  # Don't shutdown - allows subsequent connections

  # Create schema
  create_schema(conn, verbose = verbose)

  # Create indexes (skip if bulk loading - they'll be created after data load)
  if (!skip_indexes) {
    create_indexes(conn, verbose = verbose)
  }

  cli::cli_alert_success("Database initialized at {.file {path}}")

  invisible(path)
}


#' Find Bouncerdata Directory
#'
#' Finds or creates the bouncerdata directory for storing data files.
#' Prefers project-local directory (bouncerverse/bouncerdata) if it exists,
#' otherwise falls back to system data directory.
#'
#' @param create Logical. Whether to create directory if not found. Default TRUE.
#'
#' @return Character string with directory path
#' @export
find_bouncerdata_dir <- function(create = TRUE) {
  cwd <- normalizePath(getwd(), winslash = "/")

  # Walk up the directory tree looking for bouncerdata as a sibling

  # This handles cases where we're in bouncer/, bouncer/data-raw/, etc.
  current <- cwd
  for (i in 1:10) {  # Max 10 levels up
    parent <- dirname(current)
    if (parent == current) break  # Reached root

    # Check for bouncerdata sibling
    sibling_path <- file.path(parent, "bouncerdata")
    if (dir.exists(sibling_path)) {
      return(normalizePath(sibling_path, winslash = "/"))
    }

    # Also check if bouncerdata is a child of current (if we're in bouncerverse/)
    child_path <- file.path(current, "bouncerdata")
    if (dir.exists(child_path)) {
      return(normalizePath(child_path, winslash = "/"))
    }

    current <- parent
  }

  if (!create) {
    return(NULL)
  }

  # If not found, create as sibling to 'bouncer' directory
  # Walk up until we find 'bouncer' folder, then create sibling
  current <- cwd
  for (i in 1:10) {
    if (basename(current) == "bouncer") {
      parent_bouncerdata <- file.path(dirname(current), "bouncerdata")
      dir.create(parent_bouncerdata, recursive = TRUE)
      cli::cli_alert_info("Created data directory: {.file {parent_bouncerdata}}")
      return(normalizePath(parent_bouncerdata, winslash = "/"))
    }
    parent <- dirname(current)
    if (parent == current) break
    current <- parent
  }

  # Fallback to system directory
  data_dir <- tools::R_user_dir("bouncerdata", which = "data")
  if (!dir.exists(data_dir)) {
    dir.create(data_dir, recursive = TRUE)
  }
  return(data_dir)
}

#' Get Default Database Path
#'
#' Returns the default path for the Bouncer DuckDB database.
#' Prefers project-local directory (bouncerverse/bouncerdata) if it exists,
#' otherwise falls back to system data directory.
#'
#' @return Character string with database path
#' @keywords internal
get_default_db_path <- function() {
  data_dir <- find_bouncerdata_dir(create = TRUE)
  file.path(data_dir, "bouncer.duckdb")
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

  # Helper to log table creation

  log_table <- function(name) {
    if (verbose) cli::cli_alert_info("Creating {name} table...")
  }

  table_count <- 0

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

  n_tables <- length(DBI::dbListTables(conn))
  cli::cli_alert_success("Schema created successfully ({n_tables} tables)")
  invisible(TRUE)
}


#' Drop Bulk Load Indexes
#'
#' Drops indexes on core tables (matches, deliveries, players, match_innings)
#' to speed up bulk data loading. Call create_indexes() after loading to restore.
#'
#' @param conn A DuckDB connection object
#' @param verbose Logical. If TRUE, shows progress messages. Default TRUE.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
drop_bulk_load_indexes <- function(conn, verbose = TRUE) {
  if (verbose) cli::cli_alert_info("Dropping indexes for bulk loading...")


  # Get all existing indexes
  indexes <- tryCatch({
    DBI::dbGetQuery(conn, "SELECT index_name FROM duckdb_indexes()")
  }, error = function(e) {
    data.frame(index_name = character(0))
  })

  # Indexes on core tables that slow down bulk inserts
  core_index_patterns <- c(
    "idx_matches_",
    "idx_deliveries_",
    "idx_players_"
  )

  dropped_count <- 0
  for (idx_name in indexes$index_name) {
    # Check if this is a core table index
    is_core <- any(sapply(core_index_patterns, function(p) grepl(p, idx_name)))

    if (is_core) {
      tryCatch({
        DBI::dbExecute(conn, sprintf("DROP INDEX IF EXISTS %s", idx_name))
        dropped_count <- dropped_count + 1
      }, error = function(e) {
        if (verbose) cli::cli_alert_warning("Could not drop index {idx_name}: {e$message}")
      })
    }
  }

  if (verbose) cli::cli_alert_success("Dropped {dropped_count} indexes")
  invisible(TRUE)
}


#' Create Database Indexes
#'
#' Creates indexes on key columns for query performance.
#'
#' @param conn A DuckDB connection object
#' @param core_only Logical. If TRUE, only creates indexes on core tables
#'   (matches, deliveries, players). Default FALSE creates all indexes.
#' @param verbose Logical. If TRUE, shows progress for each index group. Default TRUE.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
create_indexes <- function(conn, core_only = FALSE, verbose = TRUE) {
  if (verbose) cli::cli_h2("Creating indexes")

  # Helper to log index creation
  log_index <- function(name) {
    if (verbose) cli::cli_alert_info("Creating {name} indexes...")
  }

  # Matches indexes
  log_index("matches")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_matches_venue ON matches(venue)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_matches_type ON matches(match_type)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_matches_season ON matches(season)")

  # Deliveries indexes
  log_index("deliveries")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_deliveries_match ON deliveries(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_deliveries_batter ON deliveries(batter_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_deliveries_bowler ON deliveries(bowler_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_deliveries_type ON deliveries(match_type)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_deliveries_date ON deliveries(match_date)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_deliveries_venue ON deliveries(venue)")

  # Players indexes
  log_index("players")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_players_name ON players(player_name)")

  # Innings powerplays indexes
  log_index("innings_powerplays")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_powerplays_match ON innings_powerplays(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_powerplays_innings ON innings_powerplays(match_id, innings)")

  # Player ELO indexes
  log_index("player_elo_history")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_elo_player_date ON player_elo_history(player_id, match_date)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_elo_match_type ON player_elo_history(match_type)")

  # Team ELO indexes
  log_index("team_elo")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_team_elo_date ON team_elo(team_id, match_date)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_team_elo_event ON team_elo(event_name)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_team_elo_type ON team_elo(match_type)")

  # Pre-match features indexes
  log_index("pre_match_features")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_prematch_date ON pre_match_features(match_date)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_prematch_event ON pre_match_features(event_name)")

  # Pre-match predictions indexes
  log_index("pre_match_predictions")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_predictions_match ON pre_match_predictions(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_predictions_date ON pre_match_predictions(prediction_date)")

  # Simulation results indexes
  log_index("simulation_results")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_simulation_type ON simulation_results(simulation_type)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_simulation_event ON simulation_results(event_name)")

  # T20 player ELO indexes
  log_index("t20_player_elo")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_elo_match ON t20_player_elo(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_elo_batter ON t20_player_elo(batter_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_elo_bowler ON t20_player_elo(bowler_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_elo_date ON t20_player_elo(match_date)")

  # T20 player skill indexes
  log_index("t20_player_skill")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_skill_match ON t20_player_skill(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_skill_batter ON t20_player_skill(batter_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_skill_bowler ON t20_player_skill(bowler_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_skill_date ON t20_player_skill(match_date)")

  # Venue aliases index
  log_index("venue_aliases")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_venue_aliases_canonical ON venue_aliases(canonical_venue)")

  # T20 venue skill indexes
  log_index("t20_venue_skill")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_venue_skill_match ON t20_venue_skill(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_venue_skill_venue ON t20_venue_skill(venue)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_venue_skill_date ON t20_venue_skill(match_date)")

  # ODI venue skill indexes
  log_index("odi_venue_skill")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_venue_skill_match ON odi_venue_skill(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_venue_skill_venue ON odi_venue_skill(venue)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_venue_skill_date ON odi_venue_skill(match_date)")

  # Test venue skill indexes
  log_index("test_venue_skill")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_venue_skill_match ON test_venue_skill(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_venue_skill_venue ON test_venue_skill(venue)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_venue_skill_date ON test_venue_skill(match_date)")

  # T20 team skill indexes
  log_index("t20_team_skill")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_team_skill_match ON t20_team_skill(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_team_skill_batting ON t20_team_skill(batting_team_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_team_skill_bowling ON t20_team_skill(bowling_team_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_team_skill_date ON t20_team_skill(match_date)")

  # ODI team skill indexes
  log_index("odi_team_skill")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_team_skill_match ON odi_team_skill(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_team_skill_batting ON odi_team_skill(batting_team_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_team_skill_bowling ON odi_team_skill(bowling_team_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_team_skill_date ON odi_team_skill(match_date)")

  # Test team skill indexes
  log_index("test_team_skill")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_team_skill_match ON test_team_skill(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_team_skill_batting ON test_team_skill(batting_team_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_team_skill_bowling ON test_team_skill(bowling_team_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_team_skill_date ON test_team_skill(match_date)")

  # Projection params indexes
  log_index("projection_params")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_projection_params_format ON projection_params(format)")

  # T20 score projection indexes
  log_index("t20_score_projection")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_proj_match ON t20_score_projection(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_proj_date ON t20_score_projection(match_date)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_proj_team ON t20_score_projection(batting_team_id)")

  # ODI score projection indexes
  log_index("odi_score_projection")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_proj_match ON odi_score_projection(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_proj_date ON odi_score_projection(match_date)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_proj_team ON odi_score_projection(batting_team_id)")

  # Test score projection indexes
  log_index("test_score_projection")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_proj_match ON test_score_projection(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_proj_date ON test_score_projection(match_date)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_proj_team ON test_score_projection(batting_team_id)")

  cli::cli_alert_success("Indexes created successfully")
  invisible(TRUE)
}


#' Verify Database
#'
#' Checks that the database exists and has the correct schema.
#'
#' @param path Character string specifying the database file path. If NULL,
#'   uses the default system data directory.
#' @param detailed Logical. If TRUE, returns detailed information about tables.
#'
#' @return Invisibly returns a list with database info
#' @export
#'
#' @examples
#' \dontrun{
#' # Quick verification
#' verify_database()
#'
#' # Detailed information
#' verify_database(detailed = TRUE)
#' }
verify_database <- function(path = NULL, detailed = FALSE) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_alert_danger("Database not found at {.file {path}}")
    cli::cli_alert_info("Run {.fn initialize_bouncer_database} to create it")
    return(invisible(NULL))
  }

  cli::cli_alert_success("Database found at {.file {path}}")

  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Get list of tables
  tables <- DBI::dbListTables(conn)
  expected_tables <- c("matches", "deliveries", "players", "match_innings", "innings_powerplays",
                       "match_metrics", "player_elo_history", "team_elo", "pre_match_features",
                       "pre_match_predictions", "simulation_results", "t20_player_elo",
                       "elo_calibration_metrics", "elo_normalization_log", "elo_calculation_params",
                       "t20_player_skill", "skill_calculation_params",
                       "venue_aliases", "t20_venue_skill", "odi_venue_skill", "test_venue_skill",
                       "venue_skill_calculation_params", "t20_team_skill", "odi_team_skill",
                       "test_team_skill", "team_skill_calculation_params",
                       "projection_params", "t20_score_projection", "odi_score_projection",
                       "test_score_projection")

  cli::cli_h2("Tables")
  for (tbl in expected_tables) {
    if (tbl %in% tables) {
      if (detailed) {
        row_count <- DBI::dbGetQuery(conn, paste0("SELECT COUNT(*) as n FROM ", tbl))$n
        cli::cli_alert_success("{tbl}: {row_count} rows")
      } else {
        cli::cli_alert_success("{tbl}")
      }
    } else {
      cli::cli_alert_danger("{tbl}: MISSING")
    }
  }

  info <- list(
    path = path,
    tables = tables,
    valid = all(expected_tables %in% tables)
  )

  if (detailed) {
    info$row_counts <- lapply(setNames(expected_tables, expected_tables), function(tbl) {
      if (tbl %in% tables) {
        DBI::dbGetQuery(conn, paste0("SELECT COUNT(*) as n FROM ", tbl))$n
      } else {
        0
      }
    })
  }

  invisible(info)
}


# ============================================================================
# DUAL ELO TABLE
# ============================================================================

#' Create Dual ELO Table
#'
#' Creates a table to store dual ELO ratings (survival/scoring for batters,
#' strike/economy for bowlers). Can be joined to deliveries via delivery_id.
#'
#' @param path Character. Database file path. If NULL, uses default.
#' @param overwrite Logical. If TRUE, drops and recreates the table. Default FALSE.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
create_dual_elo_table <- function(path = NULL, overwrite = FALSE) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_alert_danger("Database not found at {.file {path}}")
    cli::cli_alert_info("Run {.fn initialize_bouncer_database} first")
    return(invisible(FALSE))
  }

  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Check if table exists
  tables <- DBI::dbListTables(conn)

  if ("dual_elos" %in% tables) {
    if (!overwrite) {
      cli::cli_alert_warning("Table 'dual_elos' already exists")
      cli::cli_alert_info("Use overwrite = TRUE to replace it")
      return(invisible(FALSE))
    }
    cli::cli_alert_warning("Dropping existing 'dual_elos' table")
    DBI::dbExecute(conn, "DROP TABLE dual_elos")
  }

  # Create dual_elos table
  cli::cli_alert_info("Creating 'dual_elos' table...")

  DBI::dbExecute(conn, "
    CREATE TABLE dual_elos (
      delivery_id VARCHAR PRIMARY KEY,
      batter_survival_elo DOUBLE,
      batter_scoring_elo DOUBLE,
      bowler_strike_elo DOUBLE,
      bowler_economy_elo DOUBLE
    )
  ")

  # Create index on delivery_id for fast joins
  DBI::dbExecute(conn, "CREATE INDEX idx_dual_elos_delivery ON dual_elos(delivery_id)")

  cli::cli_alert_success("Created 'dual_elos' table")
  cli::cli_alert_info("Join to deliveries: SELECT * FROM deliveries d JOIN dual_elos e ON d.delivery_id = e.delivery_id")


  invisible(TRUE)
}

#' Insert Dual ELO Data
#'
#' Inserts dual ELO ratings into the dual_elos table.
#'
#' @param dt data.table/data.frame with columns: delivery_id, batter_survival_elo,
#'           batter_scoring_elo, bowler_strike_elo, bowler_economy_elo
#' @param path Character. Database file path. If NULL, uses default.
#' @param batch_size Integer. Number of rows to insert per transaction.
#'
#' @return Invisibly returns the number of rows inserted
#' @keywords internal
insert_dual_elos <- function(dt, path = NULL, batch_size = 10000) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Check table exists
  if (!"dual_elos" %in% DBI::dbListTables(conn)) {
    cli::cli_alert_danger("Table 'dual_elos' does not exist")
    cli::cli_alert_info("Run {.fn create_dual_elo_table} first")
    return(invisible(0))
  }

  # Required columns
  required_cols <- c("delivery_id", "batter_survival_elo", "batter_scoring_elo",
                     "bowler_strike_elo", "bowler_economy_elo")
  missing_cols <- setdiff(required_cols, names(dt))
  if (length(missing_cols) > 0) {
    cli::cli_alert_danger("Missing columns: {paste(missing_cols, collapse = ', ')}")
    return(invisible(0))
  }

  # Select only required columns
  dt_insert <- dt[, required_cols, with = FALSE]

  # Insert in batches
  n_total <- nrow(dt_insert)
  n_batches <- ceiling(n_total / batch_size)

  cli::cli_progress_bar("Inserting dual ELOs", total = n_batches)

  rows_inserted <- 0
  for (i in seq_len(n_batches)) {
    start_idx <- (i - 1) * batch_size + 1
    end_idx <- min(i * batch_size, n_total)

    batch <- dt_insert[start_idx:end_idx, ]

    DBI::dbWriteTable(conn, "dual_elos", batch, append = TRUE)
    rows_inserted <- rows_inserted + nrow(batch)

    cli::cli_progress_update()
  }

  cli::cli_progress_done()
  cli::cli_alert_success("Inserted {rows_inserted} rows into 'dual_elos'")

  invisible(rows_inserted)
}


#' Ensure Match Metrics Table Exists
#'
#' Ensures the match_metrics table exists for storing calculated match data
#' like unified_margin. Also ensures pre_match_features has margin columns.
#'
#' @param conn DBI connection. If provided, uses this connection (does not close it).
#' @param path Character. Database file path. If NULL, uses default. Ignored if conn is provided.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
ensure_match_metrics_table <- function(conn = NULL, path = NULL) {
  # If connection provided, use it (caller is responsible for closing)
  own_conn <- is.null(conn)

  if (own_conn) {
    if (is.null(path)) {
      path <- get_default_db_path()
    }

    if (!file.exists(path)) {
      cli::cli_alert_danger("Database not found at {.file {path}}")
      cli::cli_alert_info("Run {.fn initialize_bouncer_database} first")
      return(invisible(FALSE))
    }

    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))
  }

  cli::cli_h2("Ensuring match_metrics table exists")

  # Check if match_metrics table exists
  tables <- DBI::dbListTables(conn)

  if (!"match_metrics" %in% tables) {
    cli::cli_alert_info("Creating match_metrics table...")
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS match_metrics (
        match_id VARCHAR PRIMARY KEY,
        unified_margin DOUBLE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP
      )
    ")
    cli::cli_alert_success("Created match_metrics table")
  } else {
    cli::cli_alert_info("match_metrics table already exists")
  }

  # Check if pre_match_features table has margin columns
  pmf_cols <- DBI::dbGetQuery(conn, "SELECT column_name FROM information_schema.columns WHERE table_name = 'pre_match_features'")$column_name

  if (!"expected_margin" %in% pmf_cols) {
    cli::cli_alert_info("Adding expected_margin column to pre_match_features table...")
    DBI::dbExecute(conn, "ALTER TABLE pre_match_features ADD COLUMN expected_margin DOUBLE")
    cli::cli_alert_success("Added expected_margin column to pre_match_features")
  }

  if (!"actual_margin" %in% pmf_cols) {
    cli::cli_alert_info("Adding actual_margin column to pre_match_features table...")
    DBI::dbExecute(conn, "ALTER TABLE pre_match_features ADD COLUMN actual_margin DOUBLE")
    cli::cli_alert_success("Added actual_margin column to pre_match_features")
  }

  cli::cli_alert_success("Match metrics tables ready")
  invisible(TRUE)
}


#' Add Margin of Victory Columns to Existing Database
#'
#' @description
#' `r lifecycle::badge("deprecated")`
#'
#' This function is deprecated. Use ensure_match_metrics_table() instead.
#' Margin data is now stored in the match_metrics table, not in matches.
#'
#' @param conn DBI connection.
#' @param path Character. Database file path.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
add_margin_columns <- function(conn = NULL, path = NULL) {
  cli::cli_alert_warning("add_margin_columns() is deprecated - use ensure_match_metrics_table()")
  ensure_match_metrics_table(conn = conn, path = path)
}


#' Delete Matches from Database
#'
#' Deletes matches and all related data from the database.
#' Used for incremental updates when match data has changed (e.g., Test match updates).
#'
#' This function removes data from all related tables:
#' - Core: matches, deliveries, match_innings
#' - Skill indices: t20/odi/test_player_skill, t20/odi/test_team_skill, t20/odi/test_venue_skill
#' - Ratings: team_elo, t20_player_elo
#' - Predictions: pre_match_features, pre_match_predictions
#' - Simulations: simulation_results (for matches)
#'
#' @param match_ids Character vector of match IDs to delete
#' @param conn DuckDB connection (must have write access)
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return Invisibly returns the number of matches deleted
#' @keywords internal
delete_matches_from_db <- function(match_ids, conn, verbose = TRUE) {
  if (length(match_ids) == 0) {
    if (verbose) cli::cli_alert_info("No matches to delete")
    return(invisible(0))
  }

  n_matches <- length(match_ids)
  if (verbose) cli::cli_alert_info("Deleting {n_matches} matches and related data...")

  # Build SQL IN clause
  match_ids_sql <- paste0("'", match_ids, "'", collapse = ", ")

  # Tables to delete from, in order (to handle foreign keys if any)
  tables_with_match_id <- c(
    # Core tables
    "deliveries",
    "match_innings",
    "match_metrics",
    # Skill index tables
    "t20_player_skill",
    "odi_player_skill",
    "test_player_skill",
    "t20_team_skill",
    "odi_team_skill",
    "test_team_skill",
    "t20_venue_skill",
    "odi_venue_skill",
    "test_venue_skill",
    # ELO tables
    "t20_player_elo",
    "team_elo",
    # Prediction tables
    "pre_match_features",
    "pre_match_predictions",
    # Main matches table (last)
    "matches"
  )

  # Get list of existing tables
  existing_tables <- DBI::dbListTables(conn)

  total_deleted <- 0

  for (tbl in tables_with_match_id) {
    if (!tbl %in% existing_tables) {
      next
    }

    sql <- sprintf("DELETE FROM %s WHERE match_id IN (%s)", tbl, match_ids_sql)
    n_deleted <- tryCatch({
      DBI::dbExecute(conn, sql)
    }, error = function(e) {
      if (verbose) cli::cli_alert_warning("Could not delete from {tbl}: {e$message}")
      0
    })

    if (n_deleted > 0 && verbose) {
      cli::cli_alert_success("Deleted {n_deleted} rows from {tbl}")
    }
    total_deleted <- total_deleted + n_deleted
  }

  if (verbose) {
    cli::cli_alert_success("Deleted {n_matches} matches ({total_deleted} total rows)")
  }

  invisible(n_matches)
}
