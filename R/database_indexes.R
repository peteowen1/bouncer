# Database Index Functions
#
# Functions for creating and managing database indexes.
# Split from database_setup.R for better maintainability.

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

  # T20 3-way ELO indexes
  log_index("t20_3way_elo")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_3way_match ON t20_3way_elo(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_3way_batter ON t20_3way_elo(batter_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_3way_bowler ON t20_3way_elo(bowler_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_3way_venue ON t20_3way_elo(venue)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_t20_3way_date ON t20_3way_elo(match_date)")

  # ODI 3-way ELO indexes
  log_index("odi_3way_elo")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_3way_match ON odi_3way_elo(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_3way_batter ON odi_3way_elo(batter_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_3way_bowler ON odi_3way_elo(bowler_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_3way_venue ON odi_3way_elo(venue)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_odi_3way_date ON odi_3way_elo(match_date)")

  # Test 3-way ELO indexes
  log_index("test_3way_elo")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_3way_match ON test_3way_elo(match_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_3way_batter ON test_3way_elo(batter_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_3way_bowler ON test_3way_elo(bowler_id)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_3way_venue ON test_3way_elo(venue)")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_test_3way_date ON test_3way_elo(match_date)")

  cli::cli_alert_success("Indexes created successfully")
  invisible(TRUE)
}
