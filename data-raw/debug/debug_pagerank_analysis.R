# Debug Script: PageRank Player Quality Analysis
#
# This script validates the Cricket PageRank implementation by:
# 1. Running PageRank on T20 data (men's only)
# 2. Comparing top/bottom rankings to known quality players
# 3. Checking cluster differentiation (elite vs domestic players)
# 4. Comparing PageRank with existing ELO ratings
#
# Key algorithm insight:
#   PageRank = 70% opponent quality + 30% performance bonus
#   This ensures players who face elite opponents rank higher,
#   regardless of raw performance (which would inflate weak-league players)
#
# Run with: source("data-raw/debug/debug_pagerank_analysis.R")

library(DBI)
library(dplyr)
library(ggplot2)
devtools::load_all()

# ============================================================================
# 1. LOAD DATA AND COMPUTE PAGERANK
# ============================================================================

cat("\n")
cli::cli_h1("Cricket PageRank Analysis")
cli::cli_rule()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

# Compute PageRank for Men's T20 format
cat("\n")
cli::cli_h2("Computing PageRank for Men's T20 format")

pr_t20 <- calculate_player_pagerank(
  conn,
  format = "t20",
  gender = "male",
  min_deliveries = 500,  # At least 500 balls for stable rankings
  verbose = TRUE
)

# Print summary
print_pagerank_summary(pr_t20, n_top = 15)

# ============================================================================
# 2. ANALYZE TOP AND BOTTOM PLAYERS
# ============================================================================

cat("\n")
cli::cli_h2("Quality Distribution Analysis")

# Get player names from database
players <- DBI::dbGetQuery(conn, "SELECT player_id, player_name FROM players")

# Merge names with PageRank results
batters_with_names <- merge(pr_t20$batters, players, by = "player_id", all.x = TRUE)
bowlers_with_names <- merge(pr_t20$bowlers, players, by = "player_id", all.x = TRUE)

cat("\n")
cli::cli_h3("Top 20 Batters by PageRank")
top_batters <- batters_with_names %>%
  arrange(desc(pagerank)) %>%
  head(20) %>%
  mutate(rank = row_number())

for (i in 1:nrow(top_batters)) {
  cat(sprintf("%2d. %-25s  PR: %.6f  Tier: %-12s  Balls: %5d\n",
              top_batters$rank[i],
              substr(top_batters$player_name[i], 1, 25),
              top_batters$pagerank[i],
              top_batters$quality_tier[i],
              top_batters$deliveries[i]))
}

cat("\n")
cli::cli_h3("Top 20 Bowlers by PageRank")
top_bowlers <- bowlers_with_names %>%
  arrange(desc(pagerank)) %>%
  head(20) %>%
  mutate(rank = row_number())

for (i in 1:nrow(top_bowlers)) {
  cat(sprintf("%2d. %-25s  PR: %.6f  Tier: %-12s  Balls: %5d\n",
              top_bowlers$rank[i],
              substr(top_bowlers$player_name[i], 1, 25),
              top_bowlers$pagerank[i],
              top_bowlers$quality_tier[i],
              top_bowlers$deliveries[i]))
}

# ============================================================================
# 3. TIER DISTRIBUTION
# ============================================================================

cat("\n")
cli::cli_h2("Quality Tier Distribution")

cat("\nBatters:\n")
print(table(pr_t20$batters$quality_tier))

cat("\nBowlers:\n")
print(table(pr_t20$bowlers$quality_tier))

# ============================================================================
# 4. EVENT-BASED CLUSTER ANALYSIS
# ============================================================================

cat("\n")
cli::cli_h2("Event-Based Cluster Analysis")

# Get events each player has played in
event_query <- "
  SELECT DISTINCT
    d.batter_id as player_id,
    m.event_name
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  WHERE LOWER(m.match_type) IN ('t20', 'it20')
    AND d.batter_id IS NOT NULL
    AND m.event_name IS NOT NULL
"

player_events <- DBI::dbGetQuery(conn, event_query)

# Define elite events
elite_events <- c(
 "Indian Premier League",
 "Big Bash League",
 "Pakistan Super League",
 "The Hundred Men's Competition",
 "SA20",
 "Major League Cricket",
 "Caribbean Premier League"
)

# Flag players who have played in elite events
players_in_elite <- player_events %>%
  filter(event_name %in% elite_events) %>%
  distinct(player_id) %>%
  mutate(in_elite_event = TRUE)

# Merge with PageRank results
batters_elite <- merge(
  pr_t20$batters,
  players_in_elite,
  by = "player_id",
  all.x = TRUE
)
batters_elite$in_elite_event[is.na(batters_elite$in_elite_event)] <- FALSE

cat("\n")
cli::cli_h3("PageRank by Elite Event Participation (Batters)")

elite_summary <- batters_elite %>%
  group_by(in_elite_event) %>%
  summarise(
    n_players = n(),
    mean_pagerank = mean(pagerank),
    median_pagerank = median(pagerank),
    mean_percentile = mean(percentile),
    .groups = "drop"
  )

print(elite_summary)

cat("\nElite vs Non-Elite PageRank ratio:",
    round(elite_summary$mean_pagerank[elite_summary$in_elite_event] /
          elite_summary$mean_pagerank[!elite_summary$in_elite_event], 2), "\n")

# ============================================================================
# 5. COMPARE WITH ELO (if available)
# ============================================================================

cat("\n")
cli::cli_h2("PageRank vs ELO Comparison")

# Check if 3-way ELO table exists
tables <- DBI::dbListTables(conn)
elo_table <- if ("t20_3way_player_elo" %in% tables) {
  "t20_3way_player_elo"
} else if ("t20_player_skill" %in% tables) {
  "t20_player_skill"
} else {
  NULL
}

if (!is.null(elo_table)) {
  cat("Loading ELO from:", elo_table, "\n")

  # Get latest ELO for each player
  elo_query <- sprintf("
    SELECT
      batter_id as player_id,
      MAX(batter_run_elo_after) as run_elo
    FROM %s
    WHERE batter_run_elo_after IS NOT NULL
    GROUP BY batter_id
  ", elo_table)

  elo_data <- tryCatch(
    DBI::dbGetQuery(conn, elo_query),
    error = function(e) {
      cli::cli_alert_warning("Could not load ELO data: {e$message}")
      NULL
    }
  )

  if (!is.null(elo_data) && nrow(elo_data) > 0) {
    # Compare
    comparison <- compare_pagerank_elo(pr_t20, elo_data, role = "batter")

    if ("elo_pr_discrepancy" %in% names(comparison)) {
      cat("\n")
      cli::cli_h3("Players with Highest ELO-PageRank Discrepancy")
      cat("(Positive = ELO higher than PageRank suggests, potentially inflated)\n\n")

      # Merge with names
      comparison <- merge(comparison, players, by = "player_id", all.x = TRUE)

      # Top discrepancies (potentially inflated)
      top_discrepancy <- comparison %>%
        filter(!is.na(elo_pr_discrepancy)) %>%
        arrange(desc(elo_pr_discrepancy)) %>%
        head(15)

      for (i in 1:nrow(top_discrepancy)) {
        cat(sprintf("%-25s  ELO: %4.0f  PR-Pct: %5.1f%%  Disc: %+.3f  %s\n",
                    substr(top_discrepancy$player_name[i], 1, 25),
                    top_discrepancy$run_elo[i],
                    top_discrepancy$percentile[i],
                    top_discrepancy$elo_pr_discrepancy[i],
                    if (top_discrepancy$potentially_inflated[i]) "⚠️ INFLATED?" else ""))
      }

      cat("\n")
      cli::cli_h3("Players with Lowest ELO-PageRank Discrepancy")
      cat("(Negative = ELO lower than PageRank suggests, potentially underrated)\n\n")

      bottom_discrepancy <- comparison %>%
        filter(!is.na(elo_pr_discrepancy)) %>%
        arrange(elo_pr_discrepancy) %>%
        head(15)

      for (i in 1:nrow(bottom_discrepancy)) {
        cat(sprintf("%-25s  ELO: %4.0f  PR-Pct: %5.1f%%  Disc: %+.3f\n",
                    substr(bottom_discrepancy$player_name[i], 1, 25),
                    bottom_discrepancy$run_elo[i],
                    bottom_discrepancy$percentile[i],
                    bottom_discrepancy$elo_pr_discrepancy[i]))
      }
    }
  }
} else {
  cli::cli_alert_info("No ELO table found - skipping ELO comparison")
}

# ============================================================================
# 6. VISUALIZATION
# ============================================================================

cat("\n")
cli::cli_h2("Generating Visualizations")

# PageRank distribution
p1 <- ggplot(batters_elite, aes(x = pagerank, fill = in_elite_event)) +
  geom_density(alpha = 0.5) +
  scale_fill_manual(
    values = c("FALSE" = "gray60", "TRUE" = "steelblue"),
    labels = c("FALSE" = "Non-Elite Events Only", "TRUE" = "Played in Elite Events")
  ) +
  labs(
    title = "Batter PageRank Distribution by Elite Event Participation",
    x = "PageRank Score",
    y = "Density",
    fill = "Player Type"
  ) +
  theme_minimal() +
  theme(legend.position = "bottom")

print(p1)

# PageRank percentile by tier
p2 <- ggplot(batters_with_names, aes(x = reorder(quality_tier, percentile), y = percentile)) +
  geom_boxplot(fill = "steelblue", alpha = 0.7) +
  labs(
    title = "PageRank Percentile by Quality Tier",
    x = "Quality Tier",
    y = "Percentile"
  ) +
  theme_minimal() +
  coord_flip()

print(p2)

# ============================================================================
# 7. SUMMARY STATISTICS
# ============================================================================

cat("\n")
cli::cli_h2("Summary Statistics")

cat("\n=== Algorithm Performance ===\n")
cat("Iterations:", pr_t20$algorithm$iterations, "\n")
cat("Converged:", pr_t20$algorithm$converged, "\n")

cat("\n=== Matrix Dimensions ===\n")
cat("Batters:", nrow(pr_t20$matrices$matchup_matrix), "\n")
cat("Bowlers:", ncol(pr_t20$matrices$matchup_matrix), "\n")
cat("Total matchups:", sum(pr_t20$matrices$matchup_matrix > 0), "\n")
cat("Total deliveries:", sum(pr_t20$matrices$matchup_matrix), "\n")

cat("\n=== PageRank Statistics ===\n")
cat("Batter PR range:",
    signif(min(pr_t20$batters$pagerank), 3), "-",
    signif(max(pr_t20$batters$pagerank), 3), "\n")
cat("Bowler PR range:",
    signif(min(pr_t20$bowlers$pagerank), 3), "-",
    signif(max(pr_t20$bowlers$pagerank), 3), "\n")

cat("\n")
cli::cli_alert_success("PageRank analysis complete!")
