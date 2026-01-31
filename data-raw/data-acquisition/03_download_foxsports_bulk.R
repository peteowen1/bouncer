# Fox Sports Bulk Download (TEST only) ----
#
# Downloads ball-by-ball data for Test matches from Fox Sports
#
# This script:
# 1. Discovers valid match IDs by trying both URL patterns
# 2. Downloads each match with rate limiting to avoid blocks
# 3. Saves individual .rds files and a combined dataset
#
# NOTE: For multi-format downloads, use 04_download_foxsports_formats.R instead
#
# Usage:
#   source("data-raw/data-acquisition/03_download_foxsports_bulk.R")

library(chromote)
library(tidyverse)
devtools::load_all()  # Load bouncer package functions

# Configuration ----

OUTPUT_DIR <- "../bouncerdata/fox_cricket"
YEARS_TO_SCAN <- 2010:2025  # Start with recent years, expand later

# Create output directory
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

# Launch Browser ----

cli::cli_h1("Fox Sports Bulk Download")

cli::cli_alert_info("Step 1: Launching browser...")
browser <- chromote::ChromoteSession$new()
browser$Network$enable()

cli::cli_alert_info("Step 2: Getting userkey from a known match...")
userkey <- fox_get_userkey(browser, "TEST2025-260604")

if (is.null(userkey)) {
  browser$close()
  cli::cli_alert_danger("Failed to get userkey. Try running again.")
  stop("Failed to get userkey")
}

cli::cli_alert_success("Got userkey: {userkey}")

# Discover Matches ----

cli::cli_h2("Step 3: Discovering valid Test match IDs")
cli::cli_alert_info("Scanning years: {paste(YEARS_TO_SCAN, collapse = ', ')}")

valid_matches <- fox_discover_matches(browser, userkey, years = YEARS_TO_SCAN, output_dir = OUTPUT_DIR)

cli::cli_alert_success("Found {length(valid_matches)} matches to download")

# Download Matches ----

if (length(valid_matches) > 0) {
  cli::cli_h2("Step 4: Downloading match data")

  all_data <- fox_fetch_matches(
    browser = browser,
    match_ids = valid_matches,
    userkey = userkey,
    output_dir = OUTPUT_DIR,
    skip_existing = TRUE,       # Won't re-download existing matches
    refresh_key_every = 10,     # Get new userkey every 10 matches
    delay_between = 8           # 8+ seconds between matches
  )

  # Combine Data ----

  cli::cli_h2("Step 5: Combining all downloaded matches")
  combined_data <- fox_combine_matches(OUTPUT_DIR)

  # Summary ----

  cli::cli_h2("Summary")
  if (!is.null(combined_data)) {
    cli::cli_alert_success("Total balls: {nrow(combined_data)}")
    cli::cli_alert_success("Total matches: {length(unique(combined_data$match_id))}")

    # Show breakdown by team
    team_summary <- combined_data %>%
      group_by(team_name) %>%
      summarise(balls = n(), matches = n_distinct(match_id), .groups = "drop") %>%
      arrange(desc(balls))
    print(team_summary)
  }

} else {
  cli::cli_alert_warning("No matches found to download.")
}

# Cleanup ----

cli::cli_alert_info("Closing browser...")
browser$close()

cli::cli_alert_success("Done! Data saved to: {OUTPUT_DIR}")

# Next Steps ----

cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "To scan more years: Change YEARS_TO_SCAN and re-run (existing matches are skipped)",
  "i" = "Data location: {OUTPUT_DIR}",
  "i" = "Combined file: {file.path(OUTPUT_DIR, 'all_test_matches.rds')}"
))
