# Validate Pipeline State
#
# Verifies that all pipeline steps have completed and tables are populated.
# Run after pipeline execution to confirm success.

library(DBI)
devtools::load_all()

validate_pipeline_state <- function() {
  cli::cli_h1("Pipeline State Validation")

  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  all_passed <- TRUE

  # ============================================================================
  # Core Tables
  # ============================================================================
  cli::cli_h2("Core Tables")

  core_tables <- c("matches", "deliveries", "players", "match_innings")
  for (tbl in core_tables) {
    count <- DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) AS n FROM %s", tbl))$n
    if (count > 0) {
      cli::cli_alert_success("{tbl}: {format(count, big.mark=',')} rows")
    } else {
      cli::cli_alert_danger("{tbl}: EMPTY")
      all_passed <- FALSE
    }
  }

  # ============================================================================
  # Skill Tables (per format)
  # ============================================================================
  cli::cli_h2("Skill Tables")

  formats <- c("t20", "odi", "test")
  skill_tables <- c("player_skill", "team_skill", "venue_skill", "3way_player_elo")

  for (fmt in formats) {
    for (skill in skill_tables) {
      tbl_name <- paste0(fmt, "_", skill)
      exists <- DBI::dbExistsTable(conn, tbl_name)
      if (exists) {
        count <- DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) AS n FROM %s", tbl_name))$n
        if (count > 0) {
          cli::cli_alert_success("{tbl_name}: {format(count, big.mark=',')} rows")
        } else {
          cli::cli_alert_warning("{tbl_name}: exists but empty")
        }
      } else {
        cli::cli_alert_danger("{tbl_name}: MISSING")
        all_passed <- FALSE
      }
    }
  }

  # ============================================================================
  # Team ELO
  # ============================================================================
  cli::cli_h2("Team ELO")

  if (DBI::dbExistsTable(conn, "team_elo")) {
    team_elo_count <- DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM team_elo")$n
    latest_date <- DBI::dbGetQuery(conn, "SELECT MAX(match_date) AS d FROM team_elo")$d
    cli::cli_alert_success("team_elo: {format(team_elo_count, big.mark=',')} rows, latest: {latest_date}")
  } else {
    cli::cli_alert_danger("team_elo: MISSING")
    all_passed <- FALSE
  }

  # ============================================================================
  # Summary
  # ============================================================================
  cli::cli_rule()
  if (all_passed) {
    cli::cli_alert_success("All pipeline state checks PASSED")
  } else {
    cli::cli_alert_danger("Some pipeline state checks FAILED")
  }

  invisible(all_passed)
}

# Run if executed directly
if (sys.nframe() == 0) {
  validate_pipeline_state()
}
