# Fox Sports Single Match Download ----
#
# Downloads ball-by-ball data for a single Test match from Fox Sports
#
# Usage:
#   source("data-raw/data-acquisition/02_download_foxsports_single.R")

library(chromote)
library(tidyverse)
devtools::load_all()  # Load bouncer package functions

# Configuration ----

MATCH_ID <- "TEST2025-260604"  # Boxing Day Test
OUTPUT_DIR <- "../bouncerdata/fox_cricket"

# Create output directory
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

# Launch Browser ----

cli::cli_h1("Fox Sports Single Match Download")

cli::cli_alert_info("Launching browser...")
browser <- chromote::ChromoteSession$new()
browser$Network$enable()

cli::cli_alert_info("Getting userkey...")
userkey <- fox_get_userkey(browser, MATCH_ID)

if (is.null(userkey)) {
  browser$close()
  cli::cli_alert_danger("Failed to get userkey. Try running again.")
  stop("Failed to get userkey")
}

cli::cli_alert_success("Got userkey: {userkey}")

# Fetch Match Data ----

cli::cli_alert_info("Fetching ball-by-ball data for {MATCH_ID}...")
match_data <- fox_fetch_match(browser, MATCH_ID, userkey)

# Display Results ----

if (!is.null(match_data)) {
  cli::cli_alert_success("Captured {nrow(match_data)} balls")

  # Show summary by innings
  innings_summary <- match_data %>%
    group_by(innings, team_name) %>%
    summarise(balls = n(), overs = max(running_over), .groups = "drop")
  print(innings_summary)

  # Save to file
  output_file <- file.path(OUTPUT_DIR, paste0(MATCH_ID, ".rds"))
  saveRDS(match_data, output_file)
  cli::cli_alert_success("Saved to {output_file}")

  # Also save to global environment for inspection
  fox_data_full <- match_data

} else {
  cli::cli_alert_warning("No data found for this match.")
}

# Cleanup ----

cli::cli_alert_info("Closing browser...")
browser$close()

cli::cli_alert_success("Done!")
