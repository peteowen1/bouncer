# Database Migration Functions
#
# Functions to add new tables to existing databases without losing data.


#' Add Prediction Tables to Database
#'
#' Adds the new prediction and simulation tables to an existing database.
#' This is a non-destructive migration that preserves all existing data.
#'
#' @param path Character. Database file path. If NULL, uses default.
#'
#' @return Invisibly returns TRUE on success
#' @export
#'
#' @examples
#' \dontrun{
#' # Add new tables to existing database
#' add_prediction_tables()
#' }
add_prediction_tables <- function(path = NULL) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_alert_danger("Database not found at {.file {path}}")
    return(invisible(FALSE))
  }

  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  existing_tables <- DBI::dbListTables(conn)

  cli::cli_h2("Adding prediction tables to database")

  # Add team_elo table
  if (!"team_elo" %in% existing_tables) {
    cli::cli_alert_info("Creating team_elo table...")
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS team_elo (
        team_id VARCHAR,
        match_id VARCHAR,
        match_date DATE,
        match_type VARCHAR,
        event_name VARCHAR,
        elo_result DOUBLE,
        elo_roster_batting DOUBLE,
        elo_roster_bowling DOUBLE,
        elo_roster_combined DOUBLE,
        matches_played INTEGER,
        PRIMARY KEY (team_id, match_id)
      )
    ")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_team_elo_date ON team_elo(team_id, match_date)")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_team_elo_event ON team_elo(event_name)")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_team_elo_type ON team_elo(match_type)")
    cli::cli_alert_success("Created team_elo table")
  } else {
    cli::cli_alert_info("team_elo table already exists")
  }

  # Add pre_match_features table
  if (!"pre_match_features" %in% existing_tables) {
    cli::cli_alert_info("Creating pre_match_features table...")
    DBI::dbExecute(conn, "
      CREATE TABLE IF NOT EXISTS pre_match_features (
        match_id VARCHAR PRIMARY KEY,
        match_date DATE,
        match_type VARCHAR,
        event_name VARCHAR,
        team1 VARCHAR,
        team2 VARCHAR,
        team1_elo_result DOUBLE,
        team1_elo_roster DOUBLE,
        team1_form_last5 DOUBLE,
        team1_h2h_wins INTEGER,
        team1_h2h_total INTEGER,
        team2_elo_result DOUBLE,
        team2_elo_roster DOUBLE,
        team2_form_last5 DOUBLE,
        team2_h2h_wins INTEGER,
        team2_h2h_total INTEGER,
        venue VARCHAR,
        venue_avg_score DOUBLE,
        venue_chase_success_rate DOUBLE,
        venue_matches INTEGER,
        is_knockout BOOLEAN,
        is_neutral_venue BOOLEAN,
        created_at TIMESTAMP
      )
    ")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_prematch_date ON pre_match_features(match_date)")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_prematch_event ON pre_match_features(event_name)")
    cli::cli_alert_success("Created pre_match_features table")
  } else {
    cli::cli_alert_info("pre_match_features table already exists")
  }

  # Add pre_match_predictions table
  if (!"pre_match_predictions" %in% existing_tables) {
    cli::cli_alert_info("Creating pre_match_predictions table...")
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
        actual_winner VARCHAR,
        prediction_correct BOOLEAN
      )
    ")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_predictions_match ON pre_match_predictions(match_id)")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_predictions_date ON pre_match_predictions(prediction_date)")
    cli::cli_alert_success("Created pre_match_predictions table")
  } else {
    cli::cli_alert_info("pre_match_predictions table already exists")
  }

  # Add simulation_results table
  if (!"simulation_results" %in% existing_tables) {
    cli::cli_alert_info("Creating simulation_results table...")
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
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_simulation_type ON simulation_results(simulation_type)")
    DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_simulation_event ON simulation_results(event_name)")
    cli::cli_alert_success("Created simulation_results table")
  } else {
    cli::cli_alert_info("simulation_results table already exists")
  }

  cli::cli_alert_success("Migration complete!")

  # Verify
  new_tables <- DBI::dbListTables(conn)
  cli::cli_h3("Database tables")
  for (tbl in new_tables) {
    cli::cli_alert_success("{tbl}")
  }

  invisible(TRUE)
}


#' Add Skill Index Columns to Pre-Match Features
#'
#' Adds the new skill index columns to an existing pre_match_features table.
#' This is a non-destructive migration that preserves all existing data.
#'
#' @param conn DBI connection. Optional existing database connection.
#'   If NULL, opens a new connection using path.
#' @param path Character. Database file path. If NULL, uses default.
#'   Ignored if conn is provided.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
add_skill_columns_to_features <- function(conn = NULL, path = NULL) {
  # Handle connection - use provided or create new

  own_connection <- FALSE
  if (is.null(conn)) {
    if (is.null(path)) {
      path <- get_default_db_path()
    }

    if (!file.exists(path)) {
      cli::cli_alert_danger("Database not found at {.file {path}}")
      return(invisible(FALSE))
    }

    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
    own_connection <- TRUE
  }

  # Only disconnect if we created the connection

  if (own_connection) {
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))
  }

  existing_tables <- DBI::dbListTables(conn)

  if (!"pre_match_features" %in% existing_tables) {
    cli::cli_alert_danger("pre_match_features table does not exist")
    cli::cli_alert_info("Run add_prediction_tables() first")
    return(invisible(FALSE))
  }

  cli::cli_h2("Adding skill index columns to pre_match_features")

  # Get existing columns
  existing_cols <- DBI::dbGetQuery(conn, "
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'pre_match_features'
  ")$column_name

  # Define new skill columns
  skill_columns <- c(
    "team1_bat_scoring_avg DOUBLE",
    "team1_bat_scoring_top5 DOUBLE",
    "team1_bat_survival_avg DOUBLE",
    "team1_bowl_economy_avg DOUBLE",
    "team1_bowl_economy_top5 DOUBLE",
    "team1_bowl_strike_avg DOUBLE",
    "team2_bat_scoring_avg DOUBLE",
    "team2_bat_scoring_top5 DOUBLE",
    "team2_bat_survival_avg DOUBLE",
    "team2_bowl_economy_avg DOUBLE",
    "team2_bowl_economy_top5 DOUBLE",
    "team2_bowl_strike_avg DOUBLE"
  )

  columns_added <- 0

  for (col_def in skill_columns) {
    col_name <- strsplit(col_def, " ")[[1]][1]

    if (!col_name %in% existing_cols) {
      tryCatch({
        DBI::dbExecute(conn, sprintf(
          "ALTER TABLE pre_match_features ADD COLUMN %s", col_def
        ))
        cli::cli_alert_success("Added column: {col_name}")
        columns_added <- columns_added + 1
      }, error = function(e) {
        cli::cli_alert_warning("Failed to add {col_name}: {e$message}")
      })
    } else {
      cli::cli_alert_info("Column {col_name} already exists")
    }
  }

  if (columns_added > 0) {
    cli::cli_alert_success("Added {columns_added} new skill columns")
  } else {
    cli::cli_alert_info("All skill columns already exist")
  }

  # Also add toss columns if missing
  toss_columns <- c(
    "team1_won_toss INTEGER",
    "toss_elect_bat INTEGER"
  )

  for (col_def in toss_columns) {
    col_name <- strsplit(col_def, " ")[[1]][1]

    if (!col_name %in% existing_cols) {
      tryCatch({
        DBI::dbExecute(conn, sprintf(
          "ALTER TABLE pre_match_features ADD COLUMN %s", col_def
        ))
        cli::cli_alert_success("Added column: {col_name}")
      }, error = function(e) {
        cli::cli_alert_warning("Failed to add {col_name}: {e$message}")
      })
    }
  }

  # Add team skill columns (per-delivery residual-based team skills)
  cli::cli_h2("Adding team skill columns to pre_match_features")

  team_skill_columns <- c(
    "team1_team_runs_skill DOUBLE",
    "team1_team_wicket_skill DOUBLE",
    "team2_team_runs_skill DOUBLE",
    "team2_team_wicket_skill DOUBLE"
  )

  for (col_def in team_skill_columns) {
    col_name <- strsplit(col_def, " ")[[1]][1]

    if (!col_name %in% existing_cols) {
      tryCatch({
        DBI::dbExecute(conn, sprintf(
          "ALTER TABLE pre_match_features ADD COLUMN %s", col_def
        ))
        cli::cli_alert_success("Added column: {col_name}")
        columns_added <- columns_added + 1
      }, error = function(e) {
        cli::cli_alert_warning("Failed to add {col_name}: {e$message}")
      })
    } else {
      cli::cli_alert_info("Column {col_name} already exists")
    }
  }

  # Add venue skill columns (residual-based venue skills)
  cli::cli_h2("Adding venue skill columns to pre_match_features")

  venue_skill_columns <- c(
    "venue_run_rate_skill DOUBLE",
    "venue_wicket_rate_skill DOUBLE",
    "venue_boundary_rate DOUBLE",
    "venue_dot_rate DOUBLE"
  )

  for (col_def in venue_skill_columns) {
    col_name <- strsplit(col_def, " ")[[1]][1]

    if (!col_name %in% existing_cols) {
      tryCatch({
        DBI::dbExecute(conn, sprintf(
          "ALTER TABLE pre_match_features ADD COLUMN %s", col_def
        ))
        cli::cli_alert_success("Added column: {col_name}")
        columns_added <- columns_added + 1
      }, error = function(e) {
        cli::cli_alert_warning("Failed to add {col_name}: {e$message}")
      })
    } else {
      cli::cli_alert_info("Column {col_name} already exists")
    }
  }

  invisible(TRUE)
}
