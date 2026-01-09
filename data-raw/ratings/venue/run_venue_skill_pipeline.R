# Run Venue Skill Index Pipeline ----
#
# This script runs the complete Venue Skill Index pipeline.
#
# Venue skill indices track per-delivery venue characteristics:
#   - Run Rate: EMA of runs per ball at venue
#   - Wicket Rate: EMA of wicket probability per ball
#   - Boundary Rate: EMA of boundary (4s + 6s) probability per ball
#   - Dot Rate: EMA of dot ball probability per ball
#
# Key features:
#   - Lower alpha (0.002) than player skills because venues change slower
#   - Venue aliases normalize different names for the same ground
#   - Supports T20, ODI, and Test formats
#
# Pipeline steps:
#   1. Calculate venue skill indices (01_calculate_venue_skill_indices.R)
#
# Output:
#   - {format}_venue_skill tables in DuckDB (t20_venue_skill, odi_venue_skill, test_venue_skill)
#   - venue_aliases table for name normalization
#
# Usage:
#   source("data-raw/ratings/venue/run_venue_skill_pipeline.R")

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
cli::cli_h1("Venue Skill Index Pipeline")
cli::cli_alert_info("Started at: {format(start_time, '%Y-%m-%d %H:%M:%S')}")
cat("\n")

# 2. Run Venue Skill Calculation ----
cli::cli_rule("Venue Skill Index Calculation")
step_start <- Sys.time()

tryCatch({
  source("data-raw/ratings/venue/01_calculate_venue_skill_indices.R", local = new.env())
  calc_time <- difftime(Sys.time(), step_start, units = "mins")
  cli::cli_alert_success("Venue skill calculation complete ({round(calc_time, 1)} mins)")
}, error = function(e) {
  cli::cli_alert_danger("Venue skill calculation failed: {e$message}")
  stop(e)
})

cat("\n")

# 3. Summary ----
end_time <- Sys.time()
total_time <- difftime(end_time, start_time, units = "mins")

cli::cli_rule("Pipeline Complete")
cat("\n")
cli::cli_alert_success("Venue Skill Index Pipeline finished successfully!")
cli::cli_alert_info("Total time: {round(total_time, 1)} minutes")
cat("\n")

cli::cli_h3("Output Tables")
cli::cli_bullets(c(
  "*" = "t20_venue_skill - T20 venue skill indices",
  "*" = "odi_venue_skill - ODI venue skill indices",
  "*" = "test_venue_skill - Test venue skill indices",
  "*" = "venue_aliases - Venue name normalization"
))

cat("\n")
cli::cli_h3("Venue Index Interpretation")
cli::cli_bullets(c(
  "i" = "Run Rate: average runs per ball (higher = batting-friendly)",
  "i" = "Wicket Rate: probability of wicket per ball (higher = bowling-friendly)",
  "i" = "Boundary Rate: probability of 4 or 6 (higher = big-hitting venue)",
  "i" = "Dot Rate: probability of dot ball (higher = restrictive venue)"
))
cat("\n")
