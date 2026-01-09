# =============================================================================
# FOX SPORTS CRICKET SCRAPER - BULK DOWNLOAD
# Downloads ball-by-ball data for Test matches from Fox Sports
# =============================================================================

library(chromote)
library(tidyverse)
devtools::load_all()  # Load bouncer package functions

# --- CONFIGURATION ---
OUTPUT_DIR <- "../bouncerdata/fox_cricket"
YEARS_TO_SCAN <- 2010:2025  # Start with recent years, expand later

# Create output directory
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

# =============================================================================
# STEP 1: Launch browser and get userkey
# =============================================================================

message("Step 1: Launching browser...")
browser <- chromote::ChromoteSession$new()
browser$Network$enable()

message("Step 2: Getting userkey from a known match...")
userkey <- fox_get_userkey(browser, "TEST2025-260604")

if (is.null(userkey)) {
  browser$close()
  stop("Failed to get userkey. Try running again.")
}

message("Got userkey: ", userkey)

# =============================================================================
# STEP 2: Discover valid match IDs
# =============================================================================

message("\nStep 3: Discovering valid Test match IDs...")
valid_matches <- fox_discover_matches(browser, userkey, years = YEARS_TO_SCAN, output_dir = OUTPUT_DIR)

message(sprintf("\nFound %d matches to download", length(valid_matches)))

# =============================================================================
# STEP 3: Fetch all matches
# =============================================================================

if (length(valid_matches) > 0) {
  message("\nStep 4: Downloading match data...")

  all_data <- fox_fetch_matches(
    browser = browser,
    match_ids = valid_matches,
    userkey = userkey,
    output_dir = OUTPUT_DIR,
    skip_existing = TRUE,
    refresh_key_every = 10,
    delay_between = 8
  )

  # =============================================================================
  # STEP 4: Combine all downloaded data
  # =============================================================================

  message("\nStep 5: Combining all downloaded matches...")
  combined_data <- fox_combine_matches(OUTPUT_DIR)

  message("\n=== SUMMARY ===")
  if (!is.null(combined_data)) {
    message(sprintf("Total balls: %d", nrow(combined_data)))
    message(sprintf("Total matches: %d", length(unique(combined_data$match_id))))

    # Show breakdown by team
    combined_data %>%
      group_by(team_name) %>%
      summarise(balls = n(), matches = n_distinct(match_id)) %>%
      print()
  }

} else {
  message("No matches found to download.")
}

# =============================================================================
# CLEANUP
# =============================================================================

message("\nClosing browser...")
browser$close()

message("\nDone! Data saved to: ", OUTPUT_DIR)
