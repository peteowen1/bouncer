# =============================================================================
# FOX SPORTS CRICKET SCRAPER - MULTI-FORMAT DOWNLOAD
# Downloads ball-by-ball data for multiple cricket formats from Fox Sports
#
# This script extends 03_download_foxsports_bulk.R to support all formats:
# TEST, T20I, WT20I, ODI, WODI, BBL, WBBL, WNCL, WPL
#
# Outputs parquet files organized by format:
#   bouncerdata/fox_cricket/test/TEST2025-260604.parquet
#   bouncerdata/fox_cricket/t20i/T20I2025-262001.parquet
#   etc.
#
# Usage:
#   source("data-raw/data-acquisition/04_download_foxsports_formats.R")
#
# Or download specific formats by modifying FORMATS_TO_DOWNLOAD below.
# =============================================================================

library(chromote)
library(tidyverse)
devtools::load_all()  # Load bouncer package functions

# --- CONFIGURATION ---
OUTPUT_DIR <- "../bouncerdata/fox_cricket"
YEARS_TO_SCAN <- 2010:2025  # Adjust as needed

# Rescrape options:
# - FALSE: Skip matches that already have all requested data (default, fastest)
# - TRUE: Re-download everything, overwriting existing files
FORCE_RESCRAPE <- TRUE

# What data to fetch for each match
INCLUDE_PLAYERS <- TRUE   # Squad data (batting/bowling arms, positions, countries)
INCLUDE_DETAILS <- TRUE   # Match metadata (venue, weather, toss, result, umpires)

# Which formats to download (see fox_list_formats() for all options)
# Comment out formats you don't need
FORMATS_TO_DOWNLOAD <- c(
  "TEST",
  "T20I",
  "WT20I",
  "ODI",
  "WODI",
  "BBL",
  "WBBL"
  # "WNCL",
  # "WPL"
)

# Create output directory
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

# =============================================================================
# STEP 1: Launch browser and get userkey
# =============================================================================

cli::cli_h1("Fox Sports Multi-Format Download")
cli::cli_alert_info("Formats to download: {paste(FORMATS_TO_DOWNLOAD, collapse = ', ')}")
cli::cli_alert_info("Years to scan: {paste(YEARS_TO_SCAN, collapse = ', ')}")
if (FORCE_RESCRAPE) {
  cli::cli_alert_warning("FORCE_RESCRAPE is ON - will re-download ALL matches!")
} else {
  cli::cli_alert_info("Incremental mode - existing matches will be skipped/updated")
}

cli::cli_alert_info("Step 1: Launching browser...")
browser <- chromote::ChromoteSession$new()
browser$Network$enable()

# Use a known T20I match to get userkey (these are more common/recent)
cli::cli_alert_info("Step 2: Getting userkey from a known match...")
userkey <- fox_get_userkey(browser, "T20I2025-262001")

# Fallback to Test match if T20I fails
if (is.null(userkey)) {
  cli::cli_alert_warning("T20I userkey failed, trying Test match...")
  userkey <- fox_get_userkey(browser, "TEST2025-260604")
}

if (is.null(userkey)) {
  browser$close()
  cli::cli_alert_danger("Failed to get userkey. Try running again.")
  stop("Failed to get userkey")
}

cli::cli_alert_success("Got userkey: {userkey}")

# =============================================================================
# STEP 2: Process each format
# =============================================================================

all_results <- list()

for (fmt in FORMATS_TO_DOWNLOAD) {
  cli::cli_h2("Processing format: {fmt}")

  # --- Discovery ---
  cli::cli_alert_info("Discovering {fmt} matches...")
  valid_matches <- fox_discover_matches(
    browser = browser,
    userkey = userkey,
    format = fmt,
    years = YEARS_TO_SCAN,
    output_dir = OUTPUT_DIR
  )

  if (length(valid_matches) == 0) {
    cli::cli_alert_warning("No {fmt} matches found for years {paste(YEARS_TO_SCAN, collapse = ', ')}")
    next
  }

  cli::cli_alert_success("Found {length(valid_matches)} {fmt} matches")

  # --- Download ---
  cli::cli_alert_info("Downloading {fmt} match data...")
  format_data <- fox_fetch_matches(
    browser = browser,
    match_ids = valid_matches,
    userkey = userkey,
    format = fmt,
    output_dir = OUTPUT_DIR,
    use_parquet = TRUE,              # Output as parquet
    include_players = INCLUDE_PLAYERS,
    include_details = INCLUDE_DETAILS,
    skip_existing = !FORCE_RESCRAPE, # FALSE = re-download everything
    refresh_key_every = 10,          # Get new userkey every 10 matches
    delay_between = 8                # 8+ seconds between matches
  )

  # --- Combine ---
  if (!is.null(format_data)) {
    cli::cli_alert_info("Combining {fmt} match files...")
    combined_data <- fox_combine_matches(
      format = fmt,
      output_dir = OUTPUT_DIR,
      use_parquet = TRUE,
      include_players = INCLUDE_PLAYERS,
      include_details = INCLUDE_DETAILS
    )
    all_results[[fmt]] <- combined_data
  }

  # Brief pause between formats to be polite
  if (fmt != FORMATS_TO_DOWNLOAD[length(FORMATS_TO_DOWNLOAD)]) {
    cli::cli_alert_info("Pausing before next format...")
    Sys.sleep(5)
  }
}

# =============================================================================
# STEP 3: Summary
# =============================================================================

cli::cli_h2("Download Summary")

for (fmt in names(all_results)) {
  data <- all_results[[fmt]]
  if (!is.null(data)) {
    n_matches <- length(unique(data$match_id))
    n_balls <- nrow(data)
    cli::cli_alert_success("{fmt}: {n_matches} matches, {n_balls} balls")
  }
}

# =============================================================================
# CLEANUP
# =============================================================================

cli::cli_alert_info("Closing browser...")
browser$close()

cli::cli_alert_success("Done! Data saved to: {OUTPUT_DIR}")

# =============================================================================
# NEXT STEPS
# =============================================================================

cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "To add more formats: Add to FORMATS_TO_DOWNLOAD and re-run",
  "i" = "To scan more years: Change YEARS_TO_SCAN and re-run",
  "i" = "To force re-download all: Set FORCE_RESCRAPE <- TRUE",
  "i" = "To backfill players/details for existing matches: Just re-run (smart caching)",
  "i" = "Data location: {OUTPUT_DIR}/{format}/",
  "i" = "Combined files: all_{format}_matches.parquet, all_{format}_players.parquet, all_{format}_details.parquet"
))

# Show directory structure
cli::cli_h3("Directory Structure")
if (dir.exists(OUTPUT_DIR)) {
  subdirs <- list.dirs(OUTPUT_DIR, recursive = FALSE, full.names = FALSE)
  for (d in subdirs) {
    dir_path <- file.path(OUTPUT_DIR, d)
    files <- list.files(dir_path, pattern = "\\.(rds|parquet)$")
    cli::cli_alert("  {d}/: {length(files)} files")
  }
}
