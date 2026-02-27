# Analyze Team ELO Ratings ----
#
# This script analyzes team ELO ratings over time:
#   - Average ELO by year
#   - Top 10 teams at end of each year
#   - ELO distribution statistics

# Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

cat("\n")
cli::cli_h1("Team ELO Analysis")
cat("\n")

# Database Connection ----
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

# Load team_elo data ----
cli::cli_h2("Loading team ELO data")

team_elo <- DBI::dbGetQuery(conn, "
  SELECT
    team_id,
    match_id,
    match_date,
    match_type,
    event_name,
    elo_result,
    elo_roster_combined,
    matches_played
  FROM team_elo
  ORDER BY match_date, match_id
")


matches <- DBI::dbGetQuery(conn, "
  SELECT
  *
  FROM cricsheet.matches
  ORDER BY match_date, match_id
")

cli::cli_alert_success("Loaded {nrow(team_elo)} team-match ELO records")

# Add year column
team_elo <- team_elo %>%
  mutate(year = as.integer(format(as.Date(match_date), "%Y")))

# Average ELO by Year ----
cli::cli_h2("Average Team ELO by Year")

yearly_avg <- team_elo %>%
  group_by(year) %>%
  summarise(
    n_matches = n_distinct(match_id),
    n_teams = n_distinct(team_id),
    avg_elo = mean(elo_result, na.rm = TRUE),
    sd_elo = sd(elo_result, na.rm = TRUE),
    min_elo = min(elo_result, na.rm = TRUE),
    max_elo = max(elo_result, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(year)

cat("\n")
cat(sprintf(
  "%-6s %8s %8s %10s %10s %10s %10s\n",
  "Year", "Matches", "Teams", "Avg ELO", "Std Dev", "Min", "Max"
))
cat(paste(rep("-", 72), collapse = ""), "\n")

for (i in seq_len(nrow(yearly_avg))) {
  row <- yearly_avg[i, ]
  cat(sprintf(
    "%-6d %8d %8d %10.1f %10.1f %10.1f %10.1f\n",
    row$year, row$n_matches, row$n_teams,
    row$avg_elo, row$sd_elo, row$min_elo, row$max_elo
  ))
}

# Top 10 Teams at End of Each Year ----
cli::cli_h2("Top 10 Teams at End of Each Year")

# Get the last ELO for each team in each year
year_end_elos <- team_elo %>%
  group_by(year, team_id) %>%
  filter(match_date == max(match_date)) %>%
  slice_tail(n = 1) %>% # In case of multiple matches on same day
  ungroup() %>%
  select(year, team_id, elo_result, matches_played)

# Get top 10 for each year
years <- sort(unique(year_end_elos$year))

for (yr in years) {
  top10 <- year_end_elos %>%
    filter(year == yr) %>%
    arrange(desc(elo_result)) %>%
    head(10)

  if (nrow(top10) > 0) {
    cat("\n")
    cli::cli_h3("End of {yr}")
    cat(sprintf("%-4s %-35s %10s %10s\n", "Rank", "Team", "ELO", "Matches"))
    cat(paste(rep("-", 65), collapse = ""), "\n")

    for (j in seq_len(nrow(top10))) {
      row <- top10[j, ]
      cat(sprintf(
        "%-4d %-35s %10.1f %10d\n",
        j, substr(row$team_id, 1, 35), row$elo_result, row$matches_played
      ))
    }
  }
}

# Current Top 20 ----
cli::cli_h2("Current Top 20 Teams (All Time)")

current_elos <- year_end_elos %>%
  filter(year == max(year)) %>%
  arrange(desc(elo_result)) %>%
  head(20)

cat("\n")
cat(sprintf("%-4s %-35s %10s %10s\n", "Rank", "Team", "ELO", "Matches"))
cat(paste(rep("-", 65), collapse = ""), "\n")

for (j in seq_len(nrow(current_elos))) {
  row <- current_elos[j, ]
  cat(sprintf(
    "%-4d %-35s %10.1f %10d\n",
    j, substr(row$team_id, 1, 35), row$elo_result, row$matches_played
  ))
}

# Summary ----
cat("\n")
cli::cli_alert_success("Analysis complete!")
cat("\n")
