# Validate Event Tier Assignments ----
#
# Compares manual tier assignments to observed mean ELOs in each event.
# Helps identify:
#   - Events that may be misclassified (observed mean far from tier expected)
#   - Unknown events that should be explicitly classified
#   - Tier boundaries that may need adjustment
#
# Usage: Run this script after calculating team ELOs to validate tier system

# 1. Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

cat("\n")
cli::cli_h1("Event Tier Validation")
cat("\n")

# 2. Database Connection ----
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

# 3. Load Event Statistics ----
cli::cli_h2("Loading event statistics")

event_stats <- DBI::dbGetQuery(conn, "
  SELECT
    event_name,
    COUNT(DISTINCT match_id) as n_matches,
    COUNT(DISTINCT team_id) as n_teams,
    AVG(elo_result) as mean_elo,
    MIN(elo_result) as min_elo,
    MAX(elo_result) as max_elo,
    STDDEV(elo_result) as sd_elo
  FROM team_elo
  WHERE event_name IS NOT NULL
    AND event_name != ''
  GROUP BY event_name
  HAVING COUNT(DISTINCT match_id) >= 5
  ORDER BY n_matches DESC
")

cli::cli_alert_success("Loaded stats for {nrow(event_stats)} events")

# 4. Add Tier Assignments ----
cli::cli_h2("Adding tier assignments")

event_stats <- event_stats %>%
  mutate(
    assigned_tier = sapply(event_name, get_event_tier),
    tier_label = sapply(assigned_tier, get_tier_label),
    expected_mean = case_when(
      assigned_tier == 1 ~ TIER_1_STARTING_ELO,
      assigned_tier == 2 ~ TIER_2_STARTING_ELO,
      assigned_tier == 3 ~ TIER_3_STARTING_ELO,
      assigned_tier == 4 ~ TIER_4_STARTING_ELO
    ),
    deviation = mean_elo - expected_mean,
    deviation_pct = (mean_elo - expected_mean) / expected_mean * 100
  )

# 5. Summary by Tier ----
cli::cli_h2("Summary by Tier")

tier_summary <- event_stats %>%
  group_by(assigned_tier, tier_label) %>%
  summarise(
    n_events = n(),
    total_matches = sum(n_matches),
    avg_mean_elo = weighted.mean(mean_elo, n_matches),
    avg_deviation = weighted.mean(deviation, n_matches),
    .groups = "drop"
  )

cat("\n")
cat(sprintf("%-5s %-12s %8s %10s %12s %12s\n",
            "Tier", "Label", "Events", "Matches", "Avg ELO", "Deviation"))
cat(paste(rep("-", 65), collapse = ""), "\n")

for (i in seq_len(nrow(tier_summary))) {
  row <- tier_summary[i, ]
  cat(sprintf("%-5d %-12s %8d %10d %12.0f %+12.0f\n",
              row$assigned_tier, row$tier_label, row$n_events,
              row$total_matches, row$avg_mean_elo, row$avg_deviation))
}

# 6. Events with Large Deviations ----
cli::cli_h2("Events with Large Tier Deviations (>100 ELO)")

large_deviations <- event_stats %>%
  filter(abs(deviation) > 100) %>%
  arrange(desc(abs(deviation)))

if (nrow(large_deviations) > 0) {
  cat("\n")
  cat(sprintf("%-45s %5s %8s %8s %10s\n",
              "Event", "Tier", "Matches", "Mean ELO", "Deviation"))
  cat(paste(rep("-", 80), collapse = ""), "\n")

  for (i in seq_len(min(20, nrow(large_deviations)))) {
    row <- large_deviations[i, ]
    cat(sprintf("%-45s %5d %8d %8.0f %+10.0f\n",
                substr(row$event_name, 1, 45), row$assigned_tier,
                row$n_matches, row$mean_elo, row$deviation))
  }

  if (nrow(large_deviations) > 20) {
    cli::cli_alert_info("... and {nrow(large_deviations) - 20} more")
  }
} else {
  cli::cli_alert_success("No events with large deviations")
}

# 7. Potential Misclassifications ----
cli::cli_h2("Potential Misclassifications")

cli::cli_h3("Tier 3/4 events with high mean ELO (>1500)")
high_elo_low_tier <- event_stats %>%
  filter(assigned_tier >= 3, mean_elo > 1500, n_matches >= 10) %>%
  arrange(desc(mean_elo))

if (nrow(high_elo_low_tier) > 0) {
  cat("\n")
  for (i in seq_len(min(15, nrow(high_elo_low_tier)))) {
    row <- high_elo_low_tier[i, ]
    cli::cli_alert_warning("{row$event_name}: Tier {row$assigned_tier} but mean ELO = {round(row$mean_elo)} ({row$n_matches} matches)")
  }
} else {
  cli::cli_alert_success("No low-tier events with suspiciously high ELO")
}

cli::cli_h3("Tier 1/2 events with low mean ELO (<1450)")
low_elo_high_tier <- event_stats %>%
  filter(assigned_tier <= 2, mean_elo < 1450, n_matches >= 10) %>%
  arrange(mean_elo)

if (nrow(low_elo_high_tier) > 0) {
  cat("\n")
  for (i in seq_len(min(15, nrow(low_elo_high_tier)))) {
    row <- low_elo_high_tier[i, ]
    cli::cli_alert_warning("{row$event_name}: Tier {row$assigned_tier} but mean ELO = {round(row$mean_elo)} ({row$n_matches} matches)")
  }
} else {
  cli::cli_alert_success("No high-tier events with suspiciously low ELO")
}

# 8. Unknown Events (defaulted to Tier 3) ----
cli::cli_h2("Large Events Defaulting to Tier 3")

# Find events that matched the default tier (not in any explicit list)
default_tier_events <- event_stats %>%
  filter(
    assigned_tier == 3,
    n_matches >= 20,
    !grepl("Qualifier|Region|Continental|ACC |Africa|Americas|Europe|Asia|Pacific",
           event_name, ignore.case = TRUE)
  ) %>%
  arrange(desc(n_matches))

if (nrow(default_tier_events) > 0) {
  cli::cli_alert_info("These large events defaulted to Tier 3 - consider explicit classification:")
  cat("\n")

  for (i in seq_len(min(20, nrow(default_tier_events)))) {
    row <- default_tier_events[i, ]
    suggested_tier <- case_when(
      row$mean_elo >= 1550 ~ 1,
      row$mean_elo >= 1480 ~ 2,
      row$mean_elo >= 1380 ~ 3,
      TRUE ~ 4
    )
    cat(sprintf("  %-50s %4d matches, ELO=%.0f -> suggest Tier %d\n",
                substr(row$event_name, 1, 50), row$n_matches, row$mean_elo, suggested_tier))
  }
} else {
  cli::cli_alert_success("No large events defaulting to Tier 3")
}

# 9. Top Events by Tier ----
cli::cli_h2("Top Events by Tier (by match count)")

for (tier in 1:4) {
  cli::cli_h3("Tier {tier} - {get_tier_label(tier)}")

  tier_events <- event_stats %>%
    filter(assigned_tier == tier) %>%
    arrange(desc(n_matches)) %>%
    head(10)

  if (nrow(tier_events) > 0) {
    cat("\n")
    for (i in seq_len(nrow(tier_events))) {
      row <- tier_events[i, ]
      cat(sprintf("  %-50s %5d matches, mean ELO = %.0f\n",
                  substr(row$event_name, 1, 50), row$n_matches, row$mean_elo))
    }
  } else {
    cli::cli_alert_info("No events in this tier")
  }
}

# 10. Cross-Tier Match Analysis ----
cli::cli_h2("Cross-Tier Match Statistics")

# Get team primary tiers
team_tiers <- DBI::dbGetQuery(conn, "
  WITH latest_elo AS (
    SELECT team_id, elo_result, matches_played,
           ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY match_date DESC, match_id DESC) as rn
    FROM team_elo
  )
  SELECT team_id, elo_result, matches_played
  FROM latest_elo
  WHERE rn = 1
")

# Calculate tier based on ELO (approximation)
team_tiers <- team_tiers %>%
  mutate(
    approx_tier = case_when(
      elo_result >= 1600 ~ 1,
      elo_result >= 1500 ~ 2,
      elo_result >= 1400 ~ 3,
      TRUE ~ 4
    )
  )

tier_counts <- table(team_tiers$approx_tier)
cli::cli_alert_info("Teams by approximate tier (based on current ELO):")
cat(sprintf("  Tier 1 (ELO >= 1600): %d teams\n", tier_counts["1"] %||% 0))
cat(sprintf("  Tier 2 (1500-1599):   %d teams\n", tier_counts["2"] %||% 0))
cat(sprintf("  Tier 3 (1400-1499):   %d teams\n", tier_counts["3"] %||% 0))
cat(sprintf("  Tier 4 (< 1400):      %d teams\n", tier_counts["4"] %||% 0))

# 11. Summary ----
cat("\n")
cli::cli_h2("Validation Summary")

n_well_calibrated <- sum(abs(event_stats$deviation) <= 50)
n_minor_deviation <- sum(abs(event_stats$deviation) > 50 & abs(event_stats$deviation) <= 100)
n_major_deviation <- sum(abs(event_stats$deviation) > 100)

cli::cli_alert_info("Events well-calibrated (within 50 ELO): {n_well_calibrated}")
cli::cli_alert_info("Events with minor deviation (50-100 ELO): {n_minor_deviation}")
cli::cli_alert_warning("Events with major deviation (>100 ELO): {n_major_deviation}")

cat("\n")
cli::cli_alert_success("Validation complete!")
cat("\n")
