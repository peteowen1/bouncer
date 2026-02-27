# Analyze Team Match History ----
#
# Investigates a team's ELO progression by looking at:
#   - Who they played against
#   - Opponent ELO at time of match
#   - Win/loss record by opponent strength
#
# Usage: Set TARGET_TEAM below

# 1. Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

# 2. Configuration ----
TARGET_TEAM <- "Uganda_male_t20"  # Change this to analyze different teams

cat("\n")
cli::cli_h1("Team Match History Analysis: {TARGET_TEAM}")
cat("\n")

# 3. Database Connection ----
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

# 4. Get Team's ELO History ----
cli::cli_h2("Loading data")

team_elo_history <- DBI::dbGetQuery(conn, "
  SELECT *
  FROM team_elo
  WHERE team_id = ?
  ORDER BY match_date, match_id
", params = list(TARGET_TEAM))

if (nrow(team_elo_history) == 0) {
  cli::cli_alert_danger("No matches found for {TARGET_TEAM}")
  cli::cli_alert_info("Available teams (sample):")
  sample_teams <- DBI::dbGetQuery(conn, "SELECT DISTINCT team_id FROM team_elo LIMIT 20")
  print(sample_teams)
  stop("Team not found")
}

cli::cli_alert_success("Found {nrow(team_elo_history)} matches for {TARGET_TEAM}")

# 5. Get All Matches With Opponent Info ----
matches <- DBI::dbGetQuery(conn, "
  SELECT
  *
    --m.match_id,
    --m.match_date,
    --m.team1,
    --m.team2,
    --m.gender,
    --m.outcome_winner,
    --m.event_name
  FROM cricsheet.matches m
  WHERE 1=1
  --AND (m.team1 = ? OR m.team2 = ?)
  AND LOWER(m.match_type) IN ('t20', 'it20')
  AND m.outcome_winner IS NOT NULL
  AND m.outcome_winner != ''
  ORDER BY m.match_date
"
  # ,params = list(
  # gsub("_male_t20|_female_t20", "", TARGET_TEAM),
  # gsub("_male_t20|_female_t20", "", TARGET_TEAM))
  )

# Determine opponent and result for each match
gender <- ifelse(grepl("female", TARGET_TEAM), "female", "male")
team_name <- gsub("_male_t20|_female_t20", "", TARGET_TEAM)

matches <- matches %>%
  filter(gender == !!gender) %>%
  mutate(
    opponent = ifelse(team1 == team_name, team2, team1),
    opponent_id = paste(opponent, !!gender, "t20", sep = "_"),
    won = outcome_winner == team_name,
    year = as.integer(format(as.Date(match_date), "%Y"))
  )

cli::cli_alert_success("Processed {nrow(matches)} matches")

# 6. Get Opponent ELO At Time of Each Match ----
cli::cli_h2("Fetching opponent ELOs")

all_team_elos <- DBI::dbGetQuery(conn, "
  SELECT team_id, match_id, match_date, elo_result
  FROM team_elo
  ORDER BY team_id, match_date
")

# For each match, get the opponent's ELO before that match
get_opponent_elo <- function(opp_id, match_date, elo_data) {
  opp_elos <- elo_data %>%
    filter(team_id == opp_id, match_date < !!match_date) %>%
    arrange(desc(match_date)) %>%
    slice(1)
  if (nrow(opp_elos) == 0) return(1500)
  opp_elos$elo_result
}

matches$opponent_elo <- sapply(seq_len(nrow(matches)), function(i) {
  get_opponent_elo(matches$opponent_id[i], matches$match_date[i], all_team_elos)
})

# Also get team's own ELO before each match
matches$team_elo <- sapply(seq_len(nrow(matches)), function(i) {
  team_elos <- all_team_elos %>%
    filter(team_id == TARGET_TEAM, match_date < matches$match_date[i]) %>%
    arrange(desc(match_date)) %>%
    slice(1)
  if (nrow(team_elos) == 0) return(1500)
  team_elos$elo_result
})

# 7. Summary Statistics ----
cli::cli_h2("Match Summary")

cat(sprintf("\nTotal matches: %d\n", nrow(matches)))
cat(sprintf("Wins: %d (%.1f%%)\n", sum(matches$won), mean(matches$won) * 100))
cat(sprintf("Losses: %d (%.1f%%)\n", sum(!matches$won), mean(!matches$won) * 100))

# Opponent strength breakdown
matches <- matches %>%
  mutate(
    opp_strength = case_when(
      opponent_elo >= 1700 ~ "Elite (1700+)",
      opponent_elo >= 1600 ~ "Strong (1600-1699)",
      opponent_elo >= 1500 ~ "Average (1500-1599)",
      opponent_elo >= 1400 ~ "Below Avg (1400-1499)",
      TRUE ~ "Weak (<1400)"
    ),
    opp_strength = factor(opp_strength, levels = c(
      "Elite (1700+)", "Strong (1600-1699)", "Average (1500-1599)",
      "Below Avg (1400-1499)", "Weak (<1400)"
    ))
  )

# 8. Record by Opponent Strength ----
cli::cli_h2("Record by Opponent Strength")

strength_summary <- matches %>%
  group_by(opp_strength) %>%
  summarise(
    matches = n(),
    wins = sum(won),
    losses = sum(!won),
    win_pct = mean(won) * 100,
    avg_opp_elo = mean(opponent_elo),
    .groups = "drop"
  )

cat("\n")
cat(sprintf("%-20s %8s %6s %6s %8s %10s\n",
            "Opponent Strength", "Matches", "Wins", "Losses", "Win%", "Avg Opp ELO"))
cat(paste(rep("-", 65), collapse = ""), "\n")

for (i in seq_len(nrow(strength_summary))) {
  row <- strength_summary[i, ]
  cat(sprintf("%-20s %8d %6d %6d %7.1f%% %10.0f\n",
              as.character(row$opp_strength), row$matches, row$wins, row$losses,
              row$win_pct, row$avg_opp_elo))
}

# 9. Most Frequent Opponents ----
cli::cli_h2("Most Frequent Opponents")

opponent_summary <- matches %>%
  group_by(opponent) %>%
  summarise(
    matches = n(),
    wins = sum(won),
    losses = sum(!won),
    win_pct = mean(won) * 100,
    avg_opp_elo = mean(opponent_elo),
    .groups = "drop"
  ) %>%
  arrange(desc(matches)) %>%
  head(15)

cat("\n")
cat(sprintf("%-25s %8s %6s %6s %8s %10s\n",
            "Opponent", "Matches", "Wins", "Losses", "Win%", "Avg ELO"))
cat(paste(rep("-", 70), collapse = ""), "\n")

for (i in seq_len(nrow(opponent_summary))) {
  row <- opponent_summary[i, ]
  cat(sprintf("%-25s %8d %6d %6d %7.1f%% %10.0f\n",
              substr(row$opponent, 1, 25), row$matches, row$wins, row$losses,
              row$win_pct, row$avg_opp_elo))
}

# 10. ELO Progression by Year ----
cli::cli_h2("ELO Progression by Year")

yearly_summary <- matches %>%
  group_by(year) %>%
  summarise(
    matches = n(),
    wins = sum(won),
    win_pct = mean(won) * 100,
    avg_opp_elo = mean(opponent_elo),
    end_elo = last(team_elo),
    .groups = "drop"
  )

cat("\n")
cat(sprintf("%-6s %8s %6s %8s %12s %10s\n",
            "Year", "Matches", "Wins", "Win%", "Avg Opp ELO", "End ELO"))
cat(paste(rep("-", 55), collapse = ""), "\n")

for (i in seq_len(nrow(yearly_summary))) {
  row <- yearly_summary[i, ]
  cat(sprintf("%-6d %8d %6d %7.1f%% %12.0f %10.0f\n",
              row$year, row$matches, row$wins, row$win_pct,
              row$avg_opp_elo, row$end_elo))
}

# 11. Recent Matches Detail ----
cli::cli_h2("Recent Matches (Last 20)")

recent <- matches %>%
  arrange(desc(match_date)) %>%
  head(20)

cat("\n")
cat(sprintf("%-12s %-25s %6s %10s %10s\n",
            "Date", "Opponent", "Result", "Opp ELO", "Team ELO"))
cat(paste(rep("-", 70), collapse = ""), "\n")

for (i in seq_len(nrow(recent))) {
  row <- recent[i, ]
  result <- ifelse(row$won, "Won", "Lost")
  cat(sprintf("%-12s %-25s %6s %10.0f %10.0f\n",
              as.character(row$match_date),
              substr(row$opponent, 1, 25),
              result,
              row$opponent_elo,
              row$team_elo))
}

# 12. Key Insight ----
cli::cli_h2("Key Insight")

avg_opp_elo <- mean(matches$opponent_elo)
pct_weak <- mean(matches$opponent_elo < 1500) * 100

cat(sprintf("\n%s has played %d matches with average opponent ELO of %.0f\n",
            TARGET_TEAM, nrow(matches), avg_opp_elo))
cat(sprintf("%.1f%% of matches were against teams below 1500 ELO (new/weak teams)\n", pct_weak))

if (avg_opp_elo < 1450) {
  cli::cli_alert_warning("This team's high ELO may be inflated due to weak competition")
} else if (avg_opp_elo > 1550) {
  cli::cli_alert_success("This team has faced strong competition - ELO is well-tested")
}

cat("\n")
cli::cli_alert_success("Analysis complete!")
cat("\n")
