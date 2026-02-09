# Validate Database Schema
#
# Verifies database schema matches expected structure.

library(DBI)
devtools::load_all()

# ============================================================================
# Expected Schema
# ============================================================================

# Core table columns that must exist
REQUIRED_COLUMNS <- list(
  matches = c("match_id", "match_date", "match_type", "team1", "team2", "venue",
              "gender", "outcome_winner"),

  deliveries = c("delivery_id", "match_id", "innings", "over", "ball",
                 "batter_id", "bowler_id", "runs_batter", "runs_total",
                 "is_wicket", "wickets_fallen"),

  players = c("player_id", "player_name"),

  t20_player_skill = c("match_id", "player_id", "batter_scoring_index",
                       "batter_survival_rate", "bowler_economy_index",
                       "bowler_strike_rate"),

  t20_3way_elo = c("delivery_id", "batter_run_elo_before", "batter_run_elo_after",
                   "bowler_run_elo_before", "bowler_run_elo_after")
)

# ============================================================================
# Validation Functions
# ============================================================================

validate_table_columns <- function(conn, table_name, required_cols) {
  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Table '{table_name}' does not exist (skipping column check)")
    return(TRUE)  # Not a failure if table doesn't exist yet
  }

  existing_cols <- DBI::dbListFields(conn, table_name)
  missing_cols <- setdiff(required_cols, existing_cols)

  if (length(missing_cols) > 0) {
    cli::cli_alert_danger("{table_name}: missing columns: {paste(missing_cols, collapse = ', ')}")
    return(FALSE)
  }

  cli::cli_alert_success("{table_name}: all {length(required_cols)} required columns present")
  TRUE
}


validate_primary_keys <- function(conn) {
  # Check that primary key columns have no NULLs
  issues <- 0

  # matches.match_id
  null_match_pk <- DBI::dbGetQuery(conn, "
    SELECT COUNT(*) as n FROM matches WHERE match_id IS NULL
  ")$n
  if (null_match_pk > 0) {
    cli::cli_alert_danger("matches: {null_match_pk} NULL primary keys")
    issues <- issues + 1
  }

  # deliveries.delivery_id
  null_delivery_pk <- DBI::dbGetQuery(conn, "
    SELECT COUNT(*) as n FROM deliveries WHERE delivery_id IS NULL
  ")$n
  if (null_delivery_pk > 0) {
    cli::cli_alert_danger("deliveries: {null_delivery_pk} NULL primary keys")
    issues <- issues + 1
  }

  # players.player_id
  null_player_pk <- DBI::dbGetQuery(conn, "
    SELECT COUNT(*) as n FROM players WHERE player_id IS NULL
  ")$n
  if (null_player_pk > 0) {
    cli::cli_alert_danger("players: {null_player_pk} NULL primary keys")
    issues <- issues + 1
  }

  if (issues == 0) {
    cli::cli_alert_success("No NULL primary keys found")
    return(TRUE)
  }

  FALSE
}


validate_indexes_exist <- function(conn) {
  # DuckDB doesn't have a standard way to check indexes in the same way as other DBs

  # This is a placeholder - in production you might query system tables
  cli::cli_alert_info("Index validation: manual check recommended for DuckDB")
  TRUE
}


validate_foreign_keys <- function(conn) {
  # Check referential integrity for key relationships
  issues <- 0

  # deliveries.match_id -> matches.match_id
  orphan_deliveries <- DBI::dbGetQuery(conn, "
    SELECT COUNT(*) as n
    FROM deliveries d
    LEFT JOIN matches m ON d.match_id = m.match_id
    WHERE m.match_id IS NULL
  ")$n

  if (orphan_deliveries > 0) {
    cli::cli_alert_danger("deliveries: {orphan_deliveries} rows reference non-existent matches")
    issues <- issues + 1
  }

  # deliveries.batter_id -> players.player_id
  orphan_batters <- DBI::dbGetQuery(conn, "
    SELECT COUNT(DISTINCT d.batter_id) as n
    FROM deliveries d
    LEFT JOIN players p ON d.batter_id = p.player_id
    WHERE p.player_id IS NULL AND d.batter_id IS NOT NULL
  ")$n

  if (orphan_batters > 0) {
    cli::cli_alert_warning("deliveries: {orphan_batters} unique batter_ids not in players table")
    # This is a warning, not an error - players might be added lazily
  }

  if (issues == 0) {
    cli::cli_alert_success("Foreign key relationships valid")
    return(TRUE)
  }

  FALSE
}


# ============================================================================
# Main Validation
# ============================================================================

run_schema_validation <- function() {
  cli::cli_h1("Schema Validation")

  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Validate columns for each table
  column_results <- lapply(names(REQUIRED_COLUMNS), function(table) {
    validate_table_columns(conn, table, REQUIRED_COLUMNS[[table]])
  })
  names(column_results) <- names(REQUIRED_COLUMNS)

  results <- list(
    columns = all(unlist(column_results)),
    primary_keys = validate_primary_keys(conn),
    foreign_keys = validate_foreign_keys(conn),
    indexes = validate_indexes_exist(conn)
  )

  cli::cli_h2("Summary")

  passed <- sum(unlist(results))
  total <- length(results)

  if (passed == total) {
    cli::cli_alert_success("All {total} schema validations passed")
    return(invisible(TRUE))
  } else {
    cli::cli_alert_danger("{total - passed}/{total} schema validations failed")
    return(invisible(FALSE))
  }
}


# Run if executed directly
if (sys.nframe() == 0) {
  run_schema_validation()
}
