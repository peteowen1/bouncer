# Baseline Projected Score Model (Team-Agnostic) ----
#
# This script builds a simple baseline model for projected score that is
# team-agnostic. It predicts what an "average team" would score given:
#   - Venue characteristics
#   - Home/away status
#   - Toss decision
#   - Match context (knockout vs league)
#
# This baseline is used in the innings 1 win probability model to determine
# if a team is scoring "above par" or "below par" for the venue.

# Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

cat("\n")
cli::cli_h1("Baseline Projected Score Model (Team-Agnostic)")
cat("\n")

# Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cli::cli_alert_success("Connected to database")

# Configuration ----
EVENT_FILTER <- "Indian Premier League"
MATCH_TYPE <- "t20"
MIN_VENUE_MATCHES <- 10  # Need enough matches for reliable venue stats

# Load Historical Match Data ----
cli::cli_h2("Loading historical match data")

matches_query <- "
  SELECT
    m.match_id,
    m.season,
    m.match_date,
    m.venue,
    m.city,
    m.team1,
    m.team2,
    m.toss_winner,
    m.toss_decision,
    m.outcome_winner,
    m.event_match_number,
    m.event_group
  FROM cricsheet.matches m
  WHERE m.event_name LIKE ?
    AND LOWER(m.match_type) = ?
    AND m.outcome_winner IS NOT NULL
    AND m.outcome_winner != ''
  ORDER BY m.match_date
"

matches_df <- DBI::dbGetQuery(conn, matches_query, params = list(
  paste0("%", EVENT_FILTER, "%"),
  MATCH_TYPE
))

cli::cli_alert_success("Loaded {nrow(matches_df)} matches")

# Load Innings Totals ----
cli::cli_h2("Loading innings totals")

innings_query <- "
  SELECT
    mi.match_id,
    mi.innings,
    mi.batting_team,
    mi.total_runs,
    mi.total_wickets,
    mi.total_overs
  FROM cricsheet.match_innings mi
  WHERE mi.match_id IN (
    SELECT match_id FROM cricsheet.matches
    WHERE event_name LIKE ?
      AND LOWER(match_type) = ?
  )
  ORDER BY mi.match_id, mi.innings
"

innings_df <- DBI::dbGetQuery(conn, innings_query, params = list(
  paste0("%", EVENT_FILTER, "%"),
  MATCH_TYPE
))

cli::cli_alert_success("Loaded innings data for {length(unique(innings_df$match_id))} matches")

# Calculate Venue Statistics ----
cli::cli_h2("Calculating venue statistics")

# Get first innings scores by venue
first_innings <- innings_df %>%
  filter(innings == 1) %>%
  left_join(matches_df %>% select(match_id, venue, season), by = "match_id")

venue_stats <- first_innings %>%
  group_by(venue) %>%
  summarise(
    n_matches = n(),
    venue_avg_score = mean(total_runs, na.rm = TRUE),
    venue_sd_score = sd(total_runs, na.rm = TRUE),
    venue_median_score = median(total_runs, na.rm = TRUE),
    venue_min_score = min(total_runs, na.rm = TRUE),
    venue_max_score = max(total_runs, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(n_matches >= MIN_VENUE_MATCHES)

cli::cli_alert_success("Calculated stats for {nrow(venue_stats)} venues (min {MIN_VENUE_MATCHES} matches)")

# Overall league average (for venues with insufficient data)
overall_avg_score <- mean(first_innings$total_runs, na.rm = TRUE)
overall_sd_score <- sd(first_innings$total_runs, na.rm = TRUE)

cli::cli_alert_info("Overall IPL 1st innings average: {round(overall_avg_score, 1)} (SD: {round(overall_sd_score, 1)})")

# Build Match-Level Dataset ----
cli::cli_h2("Building match-level dataset")

# Join matches with innings and venue stats
match_data <- matches_df %>%
  # Add first innings info
  left_join(
    innings_df %>%
      filter(innings == 1) %>%
      select(match_id, batting_team_1 = batting_team, innings1_total = total_runs),
    by = "match_id"
  ) %>%
  # Add second innings info
  left_join(
    innings_df %>%
      filter(innings == 2) %>%
      select(match_id, batting_team_2 = batting_team, innings2_total = total_runs),
    by = "match_id"
  ) %>%
  # Add venue stats
  left_join(venue_stats, by = "venue") %>%
  # Fill missing venue stats with overall average

  mutate(
    venue_avg_score = coalesce(venue_avg_score, overall_avg_score),
    venue_sd_score = coalesce(venue_sd_score, overall_sd_score)
  ) %>%
  # Calculate derived features
  mutate(
    # Home team indicator (team1 is usually home in IPL)
    is_home_team1 = 1,  # In IPL, team1 is typically the home team

    # Toss features
    toss_winner_batted_first = as.integer(
      (toss_winner == batting_team_1 & toss_decision == "bat") |
      (toss_winner != batting_team_1 & toss_decision == "field")
    ),
    chose_to_bat = as.integer(toss_decision == "bat"),

    # Knockout indicator (finals, eliminators, qualifiers)
    is_knockout = as.integer(
      grepl("final|eliminator|qualifier", tolower(event_match_number)) |
      grepl("final|eliminator|qualifier", tolower(event_group))
    ),

    # Season trend (IPL scores have generally increased over time)
    season_numeric = as.numeric(gsub("/.*", "", season)),

    # Did batting first team win?
    batting_first_won = as.integer(outcome_winner == batting_team_1),

    # Score relative to venue average
    innings1_vs_venue_avg = innings1_total - venue_avg_score,
    innings1_vs_venue_zscore = (innings1_total - venue_avg_score) / venue_sd_score
  ) %>%
  filter(!is.na(innings1_total), !is.na(innings2_total))

cli::cli_alert_success("Built dataset with {nrow(match_data)} complete matches")

# Analyze Baseline Factors ----
cli::cli_h2("Analyzing baseline factors")

# Venue effect
cli::cli_h3("Score by Venue (top 10 by matches)")
venue_summary <- match_data %>%
  group_by(venue) %>%
  summarise(
    matches = n(),
    avg_1st_innings = mean(innings1_total),
    avg_2nd_innings = mean(innings2_total),
    batting_first_win_rate = mean(batting_first_won),
    .groups = "drop"
  ) %>%
  arrange(desc(matches)) %>%
  head(10)

print(venue_summary)
cat("\n")

# Toss effect
cli::cli_h3("Toss Decision Impact")
toss_summary <- match_data %>%
  group_by(chose_to_bat) %>%
  summarise(
    matches = n(),
    avg_1st_innings = mean(innings1_total),
    batting_first_win_rate = mean(batting_first_won),
    .groups = "drop"
  ) %>%
  mutate(decision = ifelse(chose_to_bat == 1, "Bat First", "Field First"))

print(toss_summary)
cat("\n")

# Knockout effect
cli::cli_h3("Knockout vs League Matches")
knockout_summary <- match_data %>%
  group_by(is_knockout) %>%
  summarise(
    matches = n(),
    avg_1st_innings = mean(innings1_total),
    avg_2nd_innings = mean(innings2_total),
    batting_first_win_rate = mean(batting_first_won),
    .groups = "drop"
  ) %>%
  mutate(match_type = ifelse(is_knockout == 1, "Knockout", "League"))

print(knockout_summary)
cat("\n")

# Season trend
cli::cli_h3("Score Trend by Season")
season_summary <- match_data %>%
  group_by(season_numeric) %>%
  summarise(
    matches = n(),
    avg_1st_innings = mean(innings1_total),
    .groups = "drop"
  ) %>%
  filter(matches >= 10) %>%
  arrange(season_numeric)

print(season_summary)
cat("\n")

# Build Simple Baseline Model ----
cli::cli_h2("Building baseline projected score model")

# For a simple baseline, we use:
# baseline_projected_score = venue_avg + toss_adjustment + knockout_adjustment + season_trend

# Calculate adjustments from data
toss_bat_filtered <- toss_summary %>% filter(chose_to_bat == 1)
toss_bat_adjustment <- if (nrow(toss_bat_filtered) > 0) {
  toss_bat_filtered$avg_1st_innings - overall_avg_score
} else {
  0  # No adjustment if no data
}

knockout_filtered <- knockout_summary %>% filter(is_knockout == 1)
knockout_adjustment <- if (nrow(knockout_filtered) > 0) {
  knockout_filtered$avg_1st_innings - overall_avg_score
} else {
  0  # No adjustment if no knockout matches
}

# Season trend: simple linear trend
season_model <- lm(avg_1st_innings ~ season_numeric, data = season_summary)
season_slope <- coef(season_model)[2]

cli::cli_alert_info("Toss bat-first adjustment: {round(toss_bat_adjustment, 2)} runs")
cli::cli_alert_info("Knockout adjustment: {round(knockout_adjustment, 2)} runs")
cli::cli_alert_info("Season trend: {round(season_slope, 2)} runs/year")

# Create baseline model function
baseline_model <- list(
  overall_avg = overall_avg_score,
  overall_sd = overall_sd_score,
  venue_stats = venue_stats,
  toss_bat_adjustment = toss_bat_adjustment,
  knockout_adjustment = knockout_adjustment,
  season_slope = season_slope,
  season_baseline = min(season_summary$season_numeric),
  created_at = Sys.time()
)

# Apply baseline model to match data
match_data <- match_data %>%
  mutate(
    baseline_projected_score = venue_avg_score +
      (chose_to_bat * toss_bat_adjustment) +
      (is_knockout * knockout_adjustment) +
      ((season_numeric - baseline_model$season_baseline) * season_slope),

    # How much above/below baseline is the actual score
    score_vs_baseline = innings1_total - baseline_projected_score
  )

# Evaluate baseline model
baseline_rmse <- sqrt(mean((match_data$innings1_total - match_data$baseline_projected_score)^2))
baseline_mae <- mean(abs(match_data$innings1_total - match_data$baseline_projected_score))
baseline_r2 <- cor(match_data$innings1_total, match_data$baseline_projected_score)^2

cli::cli_h3("Baseline Model Performance")
cli::cli_alert_info("RMSE: {round(baseline_rmse, 2)} runs")
cli::cli_alert_info("MAE: {round(baseline_mae, 2)} runs")
cli::cli_alert_info("R-squared: {round(baseline_r2, 4)}")
cat("\n")

# This is expected to be modest - team strength and match-specific factors matter!
cli::cli_alert_info("Note: Low R-squared is expected - this is a team-agnostic baseline")
cli::cli_alert_info("The baseline captures venue/context effects, not team quality")

# Save Baseline Model ----
cli::cli_h2("Saving baseline model")

output_dir <- file.path("..", "bouncerdata", "models")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

baseline_path <- file.path(output_dir, "ipl_baseline_projected_score.rds")
saveRDS(baseline_model, baseline_path)
cli::cli_alert_success("Saved baseline model to {baseline_path}")

# Save venue stats separately for easy lookup
venue_stats_path <- file.path(output_dir, "ipl_venue_stats.rds")
saveRDS(venue_stats, venue_stats_path)
cli::cli_alert_success("Saved venue stats to {venue_stats_path}")

# Done ----
cat("\n")
cli::cli_alert_success("Baseline projected score model complete!")
cat("\n")

cli::cli_h3("Model Summary")
cat(sprintf("  Overall IPL average: %.1f runs\n", overall_avg_score))
cat(sprintf("  Venues with stats: %d\n", nrow(venue_stats)))
cat(sprintf("  Toss bat-first effect: %+.1f runs\n", toss_bat_adjustment))
cat(sprintf("  Knockout effect: %+.1f runs\n", knockout_adjustment))
cat(sprintf("  Season trend: %+.2f runs/year\n", season_slope))
cat(sprintf("  Baseline RMSE: %.1f runs\n", baseline_rmse))
cat("\n")

cli::cli_h3("Usage")
cli::cli_bullets(c(
 "i" = "Use baseline_projected_score as the 'par score' for a venue",
 "i" = "Compare actual/projected score to baseline to see if team is above/below par",
 "i" = "This feeds into innings 1 win probability model"
))
cat("\n")
