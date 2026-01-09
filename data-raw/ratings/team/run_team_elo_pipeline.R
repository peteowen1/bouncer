# Run Team ELO Pipeline ----
#
# This script runs the complete Team ELO rating pipeline.
#
# Team ELOs are calculated by category:
#   - mens_club: Male club/franchise leagues (IPL, BBL, etc.)
#   - mens_international: Male international matches
#   - womens_club: Female club/franchise leagues
#   - womens_international: Female international matches
#
# Prerequisites:
#   - Player ELOs should be calculated first (for roster-based ELO features)
#   - Run either run_dual_elo_pipeline.R or run_skill_index_pipeline.R first
#
# Pipeline steps:
#   1. Calculate team ELOs (01_calculate_team_elos.R)
#
# Output:
#   - team_elo table in DuckDB
#   - team_elo_history_{format}_{category}.rds files in bouncerdata/models/
#
# Usage:
#   source("data-raw/ratings/team/run_team_elo_pipeline.R")

# 1. Configuration ----

# Set working directory to bouncer package root if needed
if (!file.exists("DESCRIPTION")) {
  if (file.exists("bouncer/DESCRIPTION")) {
    setwd("bouncer")
  } else {
    stop("Please run from the bouncer package root directory")
  }
}

# Timing
start_time <- Sys.time()

cat("\n")
cli::cli_h1("Team ELO Pipeline")
cli::cli_alert_info("Started at: {format(start_time, '%Y-%m-%d %H:%M:%S')}")
cat("\n")

# 2. Check Prerequisites ----
cli::cli_rule("Checking Prerequisites")

devtools::load_all()
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

tables <- DBI::dbListTables(conn)
has_player_elo <- any(grepl("_player_elo$", tables))
has_player_skill <- any(grepl("_player_skill$", tables))

if (has_player_elo) {
  cli::cli_alert_success("Player ELO tables found")
} else if (has_player_skill) {
  cli::cli_alert_info("Player skill tables found (will use for roster ELO)")
} else {
  cli::cli_alert_warning("No player rating tables found - team ELO will use defaults for roster strength")
  cli::cli_alert_info("Consider running player ratings first for better accuracy")
}

DBI::dbDisconnect(conn, shutdown = TRUE)
rm(conn)

cat("\n")

# 3. Run Team ELO Calculation ----
cli::cli_rule("Team ELO Calculation")
step_start <- Sys.time()

tryCatch({
  source("data-raw/ratings/team/01_calculate_team_elos.R", local = new.env())
  calc_time <- difftime(Sys.time(), step_start, units = "mins")
  cli::cli_alert_success("Team ELO calculation complete ({round(calc_time, 1)} mins)")
}, error = function(e) {
  cli::cli_alert_danger("Team ELO calculation failed: {e$message}")
  stop(e)
})

cat("\n")

# 4. Summary ----
end_time <- Sys.time()
total_time <- difftime(end_time, start_time, units = "mins")

cli::cli_rule("Pipeline Complete")
cat("\n")
cli::cli_alert_success("Team ELO Pipeline finished successfully!")
cli::cli_alert_info("Total time: {round(total_time, 1)} minutes")
cat("\n")

cli::cli_h3("Output")
cli::cli_bullets(c(
  "*" = "team_elo table in DuckDB",
  "*" = "team_elo_history_*.rds files in bouncerdata/models/"
))

cat("\n")
cli::cli_h3("Categories Processed")
cli::cli_bullets(c(
  "i" = "mens_club: IPL, BBL, PSL, CPL, etc.",
  "i" = "mens_international: Test, ODI, T20I",
  "i" = "womens_club: WBBL, WPL, etc.",
  "i" = "womens_international: Women's internationals"
))
cat("\n")
