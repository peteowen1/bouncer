# Download Cricket Data from Cricsheet ----
#
# This script downloads ALL cricket data from Cricsheet and loads it into DuckDB.
# Uses the optimized batch loading system for maximum performance.
#
# TWO MODES:
#   fresh = TRUE  (default): Deletes existing database, loads everything from scratch
#   fresh = FALSE          : Skips already-loaded matches (incremental/update mode)
#
# Usage:
#   source("data-raw/download-data/01_download_cricsheet.R")

# Setup ----
library(DBI)
devtools::load_all()

# Configuration ----

# Fresh start or incremental update?
# - TRUE:  Delete existing database, load everything (use for first setup or schema changes)
# - FALSE: Keep existing data, only load new matches (use for updates or error recovery)
FRESH_START <- TRUE

# Database path (NULL = use default in bouncerdata/ directory)
DB_PATH <- NULL
# DB_PATH <- "C:/cricket_data/bouncer.duckdb"  # Or specify custom path

# Keep the downloaded all_json.zip file? (useful for debugging, but ~400MB)
KEEP_ZIP <- FALSE

# Download and Load Data ----

cli::cli_h1("Bouncer Data Download & Installation")

## Close existing connections ----
# Close any existing connections
tryCatch({
  duckdb::duckdb_shutdown(duckdb::duckdb())
  Sys.sleep(0.5)
}, error = function(e) {
  # Ignore if no connections to close
})

## Show configuration ----
if (FRESH_START) {
  cli::cli_alert_warning("Mode: FRESH START (will delete existing database)")
} else {
  cli::cli_alert_info("Mode: INCREMENTAL (will skip already-loaded matches)")
}

if (is.null(DB_PATH)) {
  cli::cli_alert_info("Database: Default location (bouncerdata/bouncer.duckdb)")
} else {
  cli::cli_alert_info("Database: {.file {DB_PATH}}")
}

## Run installation ----
# This function:
# 1. Downloads all_json.zip from Cricsheet (~400MB)
# 2. Extracts ~20,000 JSON files
# 3. Batch loads with transactions (100 files per commit)
# 4. Shows detailed progress with counts
install_all_bouncer_data(
  fresh = FRESH_START,
  db_path = DB_PATH,
  download_path = NULL,  # Uses bouncerdata/ directory
  keep_zip = KEEP_ZIP
)

# Summary ----

cli::cli_h2("Installation Complete!")

## Show data info ----
if (is.null(DB_PATH)) {
  DB_PATH <- get_db_path()
}

get_data_info(path = DB_PATH)

## Save database path ----
saveRDS(DB_PATH, "data-raw/download-data/.db_path.rds")
cli::cli_alert_info("Database path saved for use in other scripts")

cli::cli_alert_success("Data is ready!")
cli::cli_alert_info("Next step: Run player-modelling/02_calculate_dual_elos.R to calculate ELO ratings")

## Usage tips ----
cli::cli_h3("Usage Tips")
cli::cli_bullets(c(
  "i" = "To add new matches later: Set FRESH_START = FALSE and re-run this script",
  "i" = "To rebuild from scratch: Set FRESH_START = TRUE and re-run",
  "i" = "Check data quality: Run get_data_info() or check_database_integrity()"
))
