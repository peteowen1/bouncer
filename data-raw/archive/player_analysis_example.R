# Player Analysis Example Script ----
#
# Analyzes a player's match results and performance statistics
# Change the PLAYER_NAME variable to analyze different players

# Setup ----
library(dplyr)
library(DBI)
devtools::load_all()

# Configuration ----

PLAYER_NAME <- "JC Archer"  # Jofra Archer's ID in Cricsheet

# Get Player's Match Results ----

cat("\n=== Match Results for", PLAYER_NAME, "===\n\n")

## Connect to database ----
conn <- get_db_connection()
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

## Find all matches where the player participated ----
player_matches <- dbGetQuery(conn, "
  SELECT DISTINCT
    m.match_id,
    m.match_type,
    m.match_date,
    m.venue,
    m.team1,
    m.team2,
    m.outcome_winner,
    m.outcome_type,
    m.event_name
  FROM matches m
  WHERE m.match_id IN (
    SELECT DISTINCT match_id
    FROM deliveries
    WHERE batter_id = ? OR bowler_id = ?
  )
  ORDER BY m.match_date
", params = list(PLAYER_NAME, PLAYER_NAME))

cat("Total matches found:", nrow(player_matches), "\n\n")

## Determine which team the player was on for each match ----
player_team_info <- dbGetQuery(conn, "
  SELECT DISTINCT
    match_id,
    CASE
      WHEN batter_id = ? THEN batting_team
      WHEN bowler_id = ? THEN bowling_team
    END as player_team
  FROM deliveries
  WHERE batter_id = ? OR bowler_id = ?
", params = list(PLAYER_NAME, PLAYER_NAME, PLAYER_NAME, PLAYER_NAME))

# Get the player's team for each match (take first occurrence)
player_team_info <- player_team_info %>%
  group_by(match_id) %>%
  slice(1) %>%
  ungroup()

# Join with match results
player_matches <- player_matches %>%
  left_join(player_team_info, by = "match_id") %>%
  mutate(
    result = case_when(
      is.na(outcome_winner) ~ "No Result",
      outcome_type == "tie" ~ "Tie",
      outcome_winner == player_team ~ "Won",
      TRUE ~ "Lost"
    )
  )

# Group by match type and result
results_by_type <- player_matches %>%
  group_by(match_type, result) %>%
  summarise(count = n(), .groups = "drop") %>%
  arrange(match_type, result)

print(results_by_type)

cat("\n")

# Summary table
summary_table <- results_by_type %>%
  tidyr::pivot_wider(names_from = result, values_from = count, values_fill = 0) %>%
  mutate(
    total = rowSums(across(where(is.numeric))),
    win_pct = if_else(total > 0, round(100 * Won / total, 1), 0)
  )

cat("Win/Loss Summary:\n")
print(summary_table)

# Batting Statistics ----

cat("\n\n=== Batting Statistics for", PLAYER_NAME, "===\n\n")

batting_stats_all <- dbGetQuery(conn, "
  SELECT
    match_type,
    COUNT(DISTINCT match_id) as matches_batted,
    COUNT(DISTINCT CASE WHEN runs_batter > 0 OR is_wicket THEN match_id END) as innings,
    SUM(runs_batter) as total_runs,
    SUM(CASE WHEN is_wicket AND player_out_id = ? THEN 1 ELSE 0 END) as dismissals,
    MAX(
      (SELECT SUM(d2.runs_batter)
       FROM deliveries d2
       WHERE d2.match_id = d.match_id
         AND d2.innings = d.innings
         AND d2.batter_id = d.batter_id
       GROUP BY d2.match_id, d2.innings, d2.batter_id)
    ) as highest_score,
    COUNT(*) as balls_faced,
    SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END) as fours,
    SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END) as sixes
  FROM deliveries d
  WHERE batter_id = ?
  GROUP BY match_type
  ORDER BY match_type
", params = list(PLAYER_NAME, PLAYER_NAME))

# Calculate derived stats
batting_stats_all <- batting_stats_all %>%
  mutate(
    average = if_else(dismissals > 0, round(total_runs / dismissals, 2), NA_real_),
    strike_rate = if_else(balls_faced > 0, round(100 * total_runs / balls_faced, 2), NA_real_),
    boundary_pct = if_else(balls_faced > 0, round(100 * (fours + sixes) / balls_faced, 2), NA_real_)
  )

print(batting_stats_all)

# Bowling Statistics ----

cat("\n\n=== Bowling Statistics for", PLAYER_NAME, "===\n\n")

bowling_stats_all <- dbGetQuery(conn, "
  SELECT
    match_type,
    COUNT(DISTINCT match_id) as matches_bowled,
    COUNT(*) as balls_bowled,
    CAST(COUNT(*) AS REAL) / 6.0 as overs_bowled,
    SUM(runs_batter + runs_extras) as runs_conceded,
    SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets,
    SUM(CASE WHEN runs_batter = 0 AND runs_extras = 0 THEN 1 ELSE 0 END) as dots,
    SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END) as fours_conceded,
    SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END) as sixes_conceded
  FROM deliveries
  WHERE bowler_id = ?
  GROUP BY match_type
  ORDER BY match_type
", params = list(PLAYER_NAME))

# Calculate derived stats
bowling_stats_all <- bowling_stats_all %>%
  mutate(
    economy = if_else(overs_bowled > 0, round(runs_conceded / overs_bowled, 2), NA_real_),
    average = if_else(wickets > 0, round(runs_conceded / wickets, 2), NA_real_),
    strike_rate = if_else(wickets > 0, round(balls_bowled / wickets, 2), NA_real_),
    dot_ball_pct = if_else(balls_bowled > 0, round(100 * dots / balls_bowled, 2), NA_real_)
  )

print(bowling_stats_all)

# Summary ----

cat("\n\n=== Summary for", PLAYER_NAME, "===\n\n")
cat("Total matches:", nrow(player_matches), "\n")
cat("Formats played:", paste(unique(player_matches$match_type), collapse = ", "), "\n")
cat("Date range:", min(player_matches$match_date), "to", max(player_matches$match_date), "\n")

if (nrow(batting_stats_all) > 0) {
  cat("\nCareer batting aggregate:\n")
  cat("  Runs:", sum(batting_stats_all$total_runs), "\n")
  cat("  Average:", round(sum(batting_stats_all$total_runs) / sum(batting_stats_all$dismissals), 2), "\n")
  cat("  Strike rate:", round(100 * sum(batting_stats_all$total_runs) / sum(batting_stats_all$balls_faced), 2), "\n")
}

if (nrow(bowling_stats_all) > 0) {
  cat("\nCareer bowling aggregate:\n")
  cat("  Wickets:", sum(bowling_stats_all$wickets), "\n")
  cat("  Average:", round(sum(bowling_stats_all$runs_conceded) / sum(bowling_stats_all$wickets), 2), "\n")
  cat("  Economy:", round(sum(bowling_stats_all$runs_conceded) / sum(bowling_stats_all$overs_bowled), 2), "\n")
}

cat("\n=== Analysis Complete ===\n")
