# Download Cricket Data from Cricsheet
#
# This script downloads cricket data and loads it into the DuckDB database.
# Run this ONCE to set up your data, then use update scripts to keep it current.
#
# Usage:
#   source("data-raw/01_download_cricket_data.R")

library(bouncer)

# Configuration
# Modify these to control what data you download

FORMATS_TO_DOWNLOAD <- c("odi", "t20i")  # Options: "test", "odi", "t20i"
LEAGUES_TO_DOWNLOAD <- c("ipl")          # Options: "ipl", "bbl", "cpl", "psl", "wbbl", etc.
GENDER <- "male"                          # Options: "male", "female", "both"
START_SEASON <- 2018                      # Only download from this year onwards (NULL for all)

# You can specify a custom database path, or leave NULL to use default
DB_PATH <- NULL  # NULL = uses default location in system data dir
# DB_PATH <- "~/cricket_data/bouncer.duckdb"  # Or specify custom path

# Keep downloaded JSON files? (useful for debugging, but takes up space)
KEEP_DOWNLOADS <- FALSE

# ============================================================================
# Download and Install Data
# ============================================================================

cli::cli_h1("Bouncer Data Download Script")

cli::cli_alert_info("This will download cricket data from Cricsheet")
cli::cli_alert_info("Formats: {paste(FORMATS_TO_DOWNLOAD, collapse = ', ')}")
cli::cli_alert_info("Leagues: {paste(LEAGUES_TO_DOWNLOAD, collapse = ', ')}")
cli::cli_alert_info("Gender: {GENDER}")
if (!is.null(START_SEASON)) {
  cli::cli_alert_info("Starting from season: {START_SEASON}")
}

# Confirm before proceeding (comment out if running non-interactively)
if (interactive()) {
  response <- readline(prompt = "Continue? (y/n): ")
  if (tolower(response) != "y") {
    cli::cli_alert_warning("Download cancelled")
    stop("User cancelled download", call. = FALSE)
  }
}

# Run the installation
install_bouncer_data(
  formats = FORMATS_TO_DOWNLOAD,
  leagues = LEAGUES_TO_DOWNLOAD,
  gender = GENDER,
  start_season = START_SEASON,
  db_path = DB_PATH,
  download_path = NULL,  # Uses temp directory
  keep_downloads = KEEP_DOWNLOADS
)

# Show what we got
cli::cli_h2("Download Complete!")
get_data_info(path = DB_PATH)

cli::cli_alert_success("Data is ready!")
cli::cli_alert_info("Next step: Run 02_calculate_player_elos.R to calculate ELO ratings")

# Save the database path for later scripts
if (is.null(DB_PATH)) {
  DB_PATH <- get_db_path()
}
saveRDS(DB_PATH, "data-raw/.db_path.rds")
cli::cli_alert_info("Database path saved for other scripts")
