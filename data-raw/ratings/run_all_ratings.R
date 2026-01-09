# Run All Rating Pipelines ----
#
# Master script to run all rating calculations end-to-end.
#
# This runs all rating systems in the recommended order:
#   1. Player ratings (choose ELO or Skill Index - skill index recommended)
#   2. Venue skill indices
#   3. Team ELOs (uses player ratings for roster strength)
#
# Configuration:
#   Set the options below to control which pipelines run.
#
# Usage:
#   source("data-raw/ratings/run_all_ratings.R")

# 1. Configuration ----

# Set working directory to bouncer package root if needed
if (!file.exists("DESCRIPTION")) {
  if (file.exists("bouncer/DESCRIPTION")) {
    setwd("bouncer")
  } else {
    stop("Please run from the bouncer package root directory")
  }
}

# Choose which pipelines to run
RUN_PLAYER_DUAL_ELO <- FALSE     # Traditional ELO system (can drift)
RUN_PLAYER_SKILL_INDEX <- TRUE   # Skill indices (recommended, drift-proof)
RUN_VENUE_SKILL_INDEX <- TRUE    # Venue characteristics
RUN_TEAM_ELO <- TRUE             # Team-level ELOs

# Note: If both player systems are TRUE, both will run.
# For new projects, SKILL_INDEX is recommended.

# Timing
start_time <- Sys.time()
pipeline_times <- list()

cat("\n")
cli::cli_h1("Complete Rating Pipeline")
cli::cli_alert_info("Started at: {format(start_time, '%Y-%m-%d %H:%M:%S')}")
cat("\n")

# Summary of what will run
cli::cli_h3("Pipelines to Run")
cli::cli_bullets(c(
  if (RUN_PLAYER_DUAL_ELO) "v" else "x" = "Player Dual ELO",
  if (RUN_PLAYER_SKILL_INDEX) "v" else "x" = "Player Skill Index",
  if (RUN_VENUE_SKILL_INDEX) "v" else "x" = "Venue Skill Index",
  if (RUN_TEAM_ELO) "v" else "x" = "Team ELO"
))
cat("\n")

# 2. Player Dual ELO Pipeline ----
if (RUN_PLAYER_DUAL_ELO) {
  cli::cli_rule("Player Dual ELO Pipeline")
  pipeline_start <- Sys.time()

  tryCatch({
    source("data-raw/ratings/player/run_dual_elo_pipeline.R", local = new.env())
    pipeline_times$player_dual_elo <- difftime(Sys.time(), pipeline_start, units = "mins")
    cli::cli_alert_success("Player Dual ELO complete ({round(pipeline_times$player_dual_elo, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Player Dual ELO failed: {e$message}")
    pipeline_times$player_dual_elo <- NA
  })

  cat("\n\n")
}

# 3. Player Skill Index Pipeline ----
if (RUN_PLAYER_SKILL_INDEX) {
  cli::cli_rule("Player Skill Index Pipeline")
  pipeline_start <- Sys.time()

  tryCatch({
    source("data-raw/ratings/player/run_skill_index_pipeline.R", local = new.env())
    pipeline_times$player_skill_index <- difftime(Sys.time(), pipeline_start, units = "mins")
    cli::cli_alert_success("Player Skill Index complete ({round(pipeline_times$player_skill_index, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Player Skill Index failed: {e$message}")
    pipeline_times$player_skill_index <- NA
  })

  cat("\n\n")
}

# 4. Venue Skill Index Pipeline ----
if (RUN_VENUE_SKILL_INDEX) {
  cli::cli_rule("Venue Skill Index Pipeline")
  pipeline_start <- Sys.time()

  tryCatch({
    source("data-raw/ratings/venue/run_venue_skill_pipeline.R", local = new.env())
    pipeline_times$venue_skill_index <- difftime(Sys.time(), pipeline_start, units = "mins")
    cli::cli_alert_success("Venue Skill Index complete ({round(pipeline_times$venue_skill_index, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Venue Skill Index failed: {e$message}")
    pipeline_times$venue_skill_index <- NA
  })

  cat("\n\n")
}

# 5. Team ELO Pipeline ----
if (RUN_TEAM_ELO) {
  cli::cli_rule("Team ELO Pipeline")
  pipeline_start <- Sys.time()

  tryCatch({
    source("data-raw/ratings/team/run_team_elo_pipeline.R", local = new.env())
    pipeline_times$team_elo <- difftime(Sys.time(), pipeline_start, units = "mins")
    cli::cli_alert_success("Team ELO complete ({round(pipeline_times$team_elo, 1)} mins)")
  }, error = function(e) {
    cli::cli_alert_danger("Team ELO failed: {e$message}")
    pipeline_times$team_elo <- NA
  })

  cat("\n\n")
}

# 6. Final Summary ----
end_time <- Sys.time()
total_time <- difftime(end_time, start_time, units = "mins")

cli::cli_rule("All Pipelines Complete")
cat("\n")

# Pipeline timing summary
cli::cli_h3("Pipeline Timings")
for (pipeline_name in names(pipeline_times)) {
  time_val <- pipeline_times[[pipeline_name]]
  if (is.na(time_val)) {
    cli::cli_alert_danger("{pipeline_name}: FAILED")
  } else {
    cli::cli_alert_success("{pipeline_name}: {round(time_val, 1)} mins")
  }
}
cat("\n")

# Overall stats
successful <- sum(!sapply(pipeline_times, is.na))
total_pipelines <- length(pipeline_times)

cli::cli_alert_info("Pipelines completed: {successful}/{total_pipelines}")
cli::cli_alert_info("Total time: {round(total_time, 1)} minutes")

if (successful == total_pipelines) {
  cli::cli_alert_success("All rating pipelines completed successfully!")
} else {
  cli::cli_alert_warning("Some pipelines failed - check output above")
}

cat("\n")
cli::cli_h3("Database Tables Updated")
cli::cli_bullets(c(
  "i" = "Player: {format}_player_elo, {format}_player_skill",
  "i" = "Venue: {format}_venue_skill",
  "i" = "Team: team_elo"
))
cat("\n")
