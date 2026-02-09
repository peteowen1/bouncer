# =============================================================================
# Clean corrupted entries from Fox Sports discovery cache files
# =============================================================================
# Problem: Some cache files contain match IDs with "_details" or "_players"
# suffixes, which are not valid match IDs and cause download failures.
# =============================================================================

library(cli)

cache_dir <- "../bouncerdata"
cache_files <- list.files(cache_dir, pattern = "^fox_discovered_.*\\.rds$", full.names = TRUE)

cli_h1("Fox Cache Cleanup")

total_removed <- 0

for (f in cache_files) {
  matches <- readRDS(f)
  format_name <- gsub("fox_discovered_|_matches\\.rds", "", basename(f))

  # Find corrupted entries (ending in _details or _players)
  bad_pattern <- "_(details|players)$"
  bad_entries <- matches[grepl(bad_pattern, matches)]
  good_entries <- matches[!grepl(bad_pattern, matches)]

  cli_h2("{toupper(format_name)}: {basename(f)}")
  cli_alert_info("Total entries: {length(matches)}")

  if (length(bad_entries) > 0) {
    cli_alert_warning("Found {length(bad_entries)} corrupted entries:")
    for (b in head(bad_entries, 10)) {
      cli_bullets(c("x" = b))
    }
    if (length(bad_entries) > 10) {
      cli_alert_info("... and {length(bad_entries) - 10} more")
    }

    # Create backup
    backup_file <- paste0(f, ".bak")
    saveRDS(matches, backup_file)
    cli_alert_info("Backup saved to: {basename(backup_file)}")

    # Save cleaned version
    saveRDS(good_entries, f)
    cli_alert_success("Cleaned! Removed {length(bad_entries)} bad entries, {length(good_entries)} remain")

    total_removed <- total_removed + length(bad_entries)
  } else {
    cli_alert_success("No corrupted entries found")
  }
}

cli_h2("Summary")
if (total_removed > 0) {
  cli_alert_success("Removed {total_removed} corrupted entries total")
  cli_alert_info("Backup files created with .bak extension")
} else {
  cli_alert_success("All cache files are clean!")
}
