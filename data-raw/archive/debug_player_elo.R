# Debug Player ELO ----
#
# Run this after 02_calculate_dual_elos.R to investigate
# why certain players have unexpectedly high/low ELOs

# 1. Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

# 2. Configuration ----
conn <- get_db_connection(read_only = TRUE)
PLAYER_NAME <- "FH Allen"  # Change this to investigate other players

cat("\n")
cli::cli_h1("Investigating: {PLAYER_NAME}")
cat("\n")

# 3. Score Mapping Function ----
map_runs_to_score <- function(runs, is_wicket) {
  ifelse(is_wicket, RUN_SCORE_WICKET,
    ifelse(runs == 0, RUN_SCORE_DOT,
    ifelse(runs == 1, RUN_SCORE_SINGLE,
    ifelse(runs == 2, RUN_SCORE_DOUBLE,
    ifelse(runs == 3, RUN_SCORE_THREE,
    ifelse(runs == 4, RUN_SCORE_FOUR,
    ifelse(runs == 6, RUN_SCORE_SIX, runs / 6)))))))  # fallback for other values
}

# 4. Get all deliveries for this player as batter ----
cli::cli_h2("Delivery History (as batter)")

# Get deliveries with team ELOs
# team_elo uses format: {team_snake_case}_{gender}_{format}_{type}
# We join by matching the snake_case team name within the team_id

player_deliveries <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    e.delivery_id,
    e.match_date,
    e.batter_id,
    e.bowler_id,
    e.batter_run_elo_before,
    e.batter_run_elo_after,
    e.bowler_run_elo_before,
    e.bowler_run_elo_after,
    e.exp_runs,
    e.actual_runs,
    e.is_wicket,
    d.match_id,
    d.batting_team,
    d.bowling_team,
    m.event_name,
    te_bat.elo_result as batting_team_elo,
    te_bowl.elo_result as bowling_team_elo
  FROM mens_t20_player_elo e
  JOIN deliveries d ON e.delivery_id = d.delivery_id
  JOIN matches m ON d.match_id = m.match_id
  LEFT JOIN team_elo te_bat ON d.match_id = te_bat.match_id
    AND te_bat.team_id LIKE CONCAT(REPLACE(REPLACE(LOWER(d.batting_team), ' ', '_'), '''', ''), '%%')
  LEFT JOIN team_elo te_bowl ON d.match_id = te_bowl.match_id
    AND te_bowl.team_id LIKE CONCAT(REPLACE(REPLACE(LOWER(d.bowling_team), ' ', '_'), '''', ''), '%%')
  WHERE e.batter_id = '%s'
  ORDER BY e.match_date, e.delivery_id
", PLAYER_NAME))

cat(sprintf("Total deliveries as batter: %d\n", nrow(player_deliveries)))
cat(sprintf("Date range: %s to %s\n",
            min(player_deliveries$match_date),
            max(player_deliveries$match_date)))

# 5. Player Stats ----
cli::cli_h2("Player Stats")

total_runs <- sum(player_deliveries$actual_runs)
total_balls <- nrow(player_deliveries)
dismissals <- sum(player_deliveries$is_wicket)
strike_rate <- (total_runs / total_balls) * 100
batting_avg <- if (dismissals > 0) total_runs / dismissals else total_runs

cat(sprintf("Total runs: %d\n", total_runs))
cat(sprintf("Total balls: %d\n", total_balls))
cat(sprintf("Dismissals: %d\n", dismissals))
cat(sprintf("Strike rate: %.1f\n", strike_rate))
cat(sprintf("Batting average: %.1f\n", batting_avg))

# 6. ELO progression ----
cli::cli_h2("ELO Progression")

first_elo <- player_deliveries$batter_run_elo_before[1]
last_elo <- player_deliveries$batter_run_elo_after[nrow(player_deliveries)]
total_change <- last_elo - first_elo

cat(sprintf("Starting ELO: %.1f\n", first_elo))
cat(sprintf("Final ELO: %.1f\n", last_elo))
cat(sprintf("Total change: %+.1f\n", total_change))

# 7. Per-delivery updates with mapped scores ----
player_deliveries <- player_deliveries %>%
  mutate(
    actual_score = map_runs_to_score(actual_runs, is_wicket),
    elo_update = batter_run_elo_after - batter_run_elo_before,
    performance = actual_score - exp_runs
  )

avg_update <- mean(player_deliveries$elo_update)
sum_updates <- sum(player_deliveries$elo_update)
avg_exp <- mean(player_deliveries$exp_runs)
avg_actual <- mean(player_deliveries$actual_score)

cat(sprintf("\nAverage expected score: %.3f\n", avg_exp))
cat(sprintf("Average actual score:   %.3f\n", avg_actual))
cat(sprintf("Average performance:    %+.3f (actual - expected)\n", avg_actual - avg_exp))
cat(sprintf("Average ELO update:     %+.3f per delivery\n", avg_update))
cat(sprintf("Sum of all updates:     %+.1f\n", sum_updates))

# 8. Score mapping reference ----
cli::cli_h2("Score Mapping Reference")
cat("  Wicket  → 0.00\n")
cat("  0 (dot) → 0.15\n")
cat("  1 run   → 0.35\n")
cat("  2 runs  → 0.45\n")
cat("  3 runs  → 0.55\n")
cat("  4 runs  → 0.75\n")
cat("  6 runs  → 1.00\n")

# 9. Performance by outcome type ----
cli::cli_h2("Performance by Outcome")

outcome_summary <- player_deliveries %>%
  mutate(outcome = case_when(
    is_wicket ~ "Wicket",
    actual_runs == 0 ~ "Dot",
    actual_runs == 1 ~ "Single",
    actual_runs == 2 ~ "Double",
    actual_runs == 3 ~ "Three",
    actual_runs == 4 ~ "Four",
    actual_runs == 6 ~ "Six",
    TRUE ~ "Other"
  )) %>%
  group_by(outcome) %>%
  summarise(
    count = n(),
    pct = n() / nrow(player_deliveries) * 100,
    mapped_score = first(actual_score),
    avg_exp = mean(exp_runs),
    avg_elo_update = mean(elo_update),
    total_elo_change = sum(elo_update),
    .groups = "drop"
  ) %>%
  arrange(mapped_score)

cat(sprintf("  %-8s %5s %6s  %6s  %6s  %8s  %10s\n",
            "Outcome", "Count", "Pct", "Score", "AvgExp", "AvgUpd", "TotalChg"))
cat(sprintf("  %-8s %5s %6s  %6s  %6s  %8s  %10s\n",
            "--------", "-----", "------", "------", "------", "--------", "----------"))
for (i in seq_len(nrow(outcome_summary))) {
  cat(sprintf("  %-8s %5d %5.1f%%  %6.2f  %6.3f  %+8.2f  %+10.1f\n",
              outcome_summary$outcome[i],
              outcome_summary$count[i],
              outcome_summary$pct[i],
              outcome_summary$mapped_score[i],
              outcome_summary$avg_exp[i],
              outcome_summary$avg_elo_update[i],
              outcome_summary$total_elo_change[i]))
}

# 10. Opponents faced ----
cli::cli_h2("Opponent Analysis")

opp_summary <- player_deliveries %>%
  summarise(
    avg_opp_elo = mean(bowler_run_elo_before),
    min_opp_elo = min(bowler_run_elo_before),
    max_opp_elo = max(bowler_run_elo_before),
    unique_bowlers = n_distinct(bowler_id),
    unique_teams = n_distinct(bowling_team),
    avg_bat_team_elo = mean(batting_team_elo, na.rm = TRUE),
    avg_bowl_team_elo = mean(bowling_team_elo, na.rm = TRUE),
    min_bowl_team_elo = min(bowling_team_elo, na.rm = TRUE),
    max_bowl_team_elo = max(bowling_team_elo, na.rm = TRUE)
  )

cat(sprintf("Unique bowlers faced: %d\n", opp_summary$unique_bowlers))
cat(sprintf("Unique teams faced: %d\n", opp_summary$unique_teams))
cat("\nOpponent Bowler ELO:\n")
cat(sprintf("  Average: %.1f\n", opp_summary$avg_opp_elo))
cat(sprintf("  Range:   %.1f to %.1f\n", opp_summary$min_opp_elo, opp_summary$max_opp_elo))
cat("\nTeam ELO:\n")
cat(sprintf("  Player's team avg:  %.1f\n", opp_summary$avg_bat_team_elo))
cat(sprintf("  Opponent team avg:  %.1f\n", opp_summary$avg_bowl_team_elo))
cat(sprintf("  Opponent team range: %.1f to %.1f\n", opp_summary$min_bowl_team_elo, opp_summary$max_bowl_team_elo))

# 11. Team affiliations ----
cli::cli_h2("Team Affiliations")

team_counts <- player_deliveries %>%
  count(batting_team) %>%
  arrange(desc(n))

for (i in seq_len(min(5, nrow(team_counts)))) {
  cat(sprintf("  %s: %d deliveries\n", team_counts$batting_team[i], team_counts$n[i]))
}

# 12. Events played ----
cli::cli_h2("Events Played")

event_counts <- player_deliveries %>%
  count(event_name) %>%
  arrange(desc(n))

for (i in seq_len(min(10, nrow(event_counts)))) {
  cat(sprintf("  %s: %d deliveries\n", event_counts$event_name[i], event_counts$n[i]))
}

# 13. Sample of deliveries ----
cli::cli_h2("Sample Deliveries (first 20)")

sample_dels <- player_deliveries %>%
  head(20) %>%
  select(match_date, bowler_id, bowler_run_elo_before, actual_runs, actual_score, exp_runs, elo_update)

cat(sprintf("  %-10s %-18s %7s %4s %6s %6s %8s\n",
            "Date", "Bowler", "OppELO", "Runs", "Score", "Exp", "Update"))
cat(sprintf("  %-10s %-18s %7s %4s %6s %6s %8s\n",
            "----------", "------------------", "-------", "----", "------", "------", "--------"))
for (i in seq_len(nrow(sample_dels))) {
  cat(sprintf("  %-10s %-18s %7.0f %4d %6.2f %6.3f %+8.2f\n",
              sample_dels$match_date[i],
              substr(sample_dels$bowler_id[i], 1, 18),
              sample_dels$bowler_run_elo_before[i],
              sample_dels$actual_runs[i],
              sample_dels$actual_score[i],
              sample_dels$exp_runs[i],
              sample_dels$elo_update[i]))
}

# 14. Biggest ELO gains ----
cli::cli_h2("Biggest ELO Gains (Top 10)")

top_gains <- player_deliveries %>%
  arrange(desc(elo_update)) %>%
  head(10) %>%
  select(match_date, bowler_id, batter_run_elo_before, bowler_run_elo_before, bowling_team, batting_team_elo, bowling_team_elo, actual_runs, actual_score, exp_runs, elo_update, event_name)

cat(sprintf("  %-10s %-15s %-12s %6s %6s %6s %4s %5s %7s\n",
            "Date", "Bowler", "Opp Team", "BwlELO", "BatTm", "BwlTm", "Runs", "Scor", "Update"))
cat(sprintf("  %-10s %-15s %-12s %6s %6s %6s %4s %5s %7s\n",
            "----------", "---------------", "------------", "------", "------", "------", "----", "-----", "-------"))
for (i in seq_len(nrow(top_gains))) {
  bat_tm_elo <- if (!is.na(top_gains$batting_team_elo[i])) sprintf("%6.0f", top_gains$batting_team_elo[i]) else "    NA"
  bwl_tm_elo <- if (!is.na(top_gains$bowling_team_elo[i])) sprintf("%6.0f", top_gains$bowling_team_elo[i]) else "    NA"
  cat(sprintf("  %-10s %-15s %-12s %6.0f %s %s %4d %5.2f %+7.2f\n",
              top_gains$match_date[i],
              substr(top_gains$bowler_id[i], 1, 15),
              substr(top_gains$bowling_team[i], 1, 12),
              top_gains$bowler_run_elo_before[i],
              bat_tm_elo,
              bwl_tm_elo,
              top_gains$actual_runs[i],
              top_gains$actual_score[i],
              top_gains$elo_update[i]))
}

# Summarize where gains come from
cat("\n  Summary of top gains:\n")
event_gain_summary <- top_gains %>% count(event_name) %>% arrange(desc(n))
for (i in seq_len(nrow(event_gain_summary))) {
  cat(sprintf("    %s: %d of top 10\n", event_gain_summary$event_name[i], event_gain_summary$n[i]))
}
team_gain_summary <- top_gains %>% count(bowling_team) %>% arrange(desc(n))
cat("  Opponent teams in top gains:\n")
for (i in seq_len(nrow(team_gain_summary))) {
  cat(sprintf("    %s: %d\n", team_gain_summary$bowling_team[i], team_gain_summary$n[i]))
}

# 15. Biggest ELO losses ----
cli::cli_h2("Biggest ELO Losses (Top 10)")

top_losses <- player_deliveries %>%
  arrange(elo_update) %>%
  head(10) %>%
  select(match_date, bowler_id, batter_run_elo_before, bowler_run_elo_before, bowling_team, batting_team_elo, bowling_team_elo, actual_runs, actual_score, exp_runs, elo_update, event_name)

cat(sprintf("  %-10s %-15s %-12s %6s %6s %6s %4s %5s %7s\n",
            "Date", "Bowler", "Opp Team", "BwlELO", "BatTm", "BwlTm", "Runs", "Scor", "Update"))
cat(sprintf("  %-10s %-15s %-12s %6s %6s %6s %4s %5s %7s\n",
            "----------", "---------------", "------------", "------", "------", "------", "----", "-----", "-------"))
for (i in seq_len(nrow(top_losses))) {
  bat_tm_elo <- if (!is.na(top_losses$batting_team_elo[i])) sprintf("%6.0f", top_losses$batting_team_elo[i]) else "    NA"
  bwl_tm_elo <- if (!is.na(top_losses$bowling_team_elo[i])) sprintf("%6.0f", top_losses$bowling_team_elo[i]) else "    NA"
  cat(sprintf("  %-10s %-15s %-12s %6.0f %s %s %4d %5.2f %+7.2f\n",
              top_losses$match_date[i],
              substr(top_losses$bowler_id[i], 1, 15),
              substr(top_losses$bowling_team[i], 1, 12),
              top_losses$bowler_run_elo_before[i],
              bat_tm_elo,
              bwl_tm_elo,
              top_losses$actual_runs[i],
              top_losses$actual_score[i],
              top_losses$elo_update[i]))
}
cat(sprintf("\n  Events: %s\n", paste(unique(top_losses$event_name), collapse = ", ")))

# 16. Inflation Check ----
cli::cli_h2("Inflation Check")

# If player has high ELO but opponents have low ELO, that's a sign of isolated pool
cat(sprintf("Player final ELO:       %.1f\n", last_elo))
cat(sprintf("Avg opponent ELO:       %.1f\n", opp_summary$avg_opp_elo))
cat(sprintf("Player vs Opponents:    %+.1f\n", last_elo - opp_summary$avg_opp_elo))

cat(sprintf("\nAvg opponent team ELO:  %.1f\n", opp_summary$avg_bowl_team_elo))
cat(sprintf("Team ELO vs 1500:       %+.1f\n", opp_summary$avg_bowl_team_elo - 1500))

# Flag potential issues
issues <- 0
if (last_elo - opp_summary$avg_opp_elo > 300) {
  cli::cli_alert_warning("Player ELO is 300+ points above average opponent - possible inflation!")
  issues <- issues + 1
}
if (!is.na(opp_summary$avg_bowl_team_elo) && opp_summary$avg_bowl_team_elo < 1450) {
  cli::cli_alert_warning("Average opponent team ELO < 1450 - playing against weak teams!")
  issues <- issues + 1
}
if (!is.na(opp_summary$avg_bowl_team_elo) && opp_summary$avg_bowl_team_elo < 1400) {
  cli::cli_alert_danger("Average opponent team ELO < 1400 - VERY weak opposition!")
  issues <- issues + 1
}
if (issues == 0) {
  cli::cli_alert_success("Player ELO and opponent quality appear reasonable")
}

# 17. Full Delivery Log ----
cli::cli_h2("Full Delivery Log")
cat("Run: player_deliveries %>% select(match_date, bowler_id, actual_runs, actual_score, exp_runs, elo_update) %>% print(n=Inf)\n")

# Cleanup
DBI::dbDisconnect(conn, shutdown = TRUE)

cat("\n")
cli::cli_alert_info("Debug complete!")
cat("\n")
