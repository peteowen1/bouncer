# 02 Match Simulation ----
#
# Simulates individual cricket matches using real data and ELO-based probabilities.
# Compares simulated win rates to actual outcomes.

# 1. Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

cli::cli_h1("Match Simulation")

# 2. Configuration ----
EVENT_NAME <- "Indian Premier League"
TARGET_SEASON <- "2023"  # Use a completed season
N_SIMULATIONS <- 10000
RANDOM_SEED <- 42

cli::cli_h2("Configuration")
cli::cli_alert_info("Event: {EVENT_NAME}")
cli::cli_alert_info("Season: {TARGET_SEASON}")
cli::cli_alert_info("Simulations per match: {format(N_SIMULATIONS, big.mark = ',')}")

# 3. Database Connection ----
cli::cli_h2("Loading Data")

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

fixtures <- get_season_fixtures(EVENT_NAME, TARGET_SEASON, conn)

if (nrow(fixtures) == 0) {
  cli::cli_alert_danger("No fixtures found for {EVENT_NAME} {TARGET_SEASON}")
  cli::cli_alert_info("Run team ELO calculations first, or try a different season")
  stop("No data available")
}

cli::cli_alert_success("Loaded {nrow(fixtures)} matches")

# 4. Select Match to Simulate ----
cli::cli_h2("Match Selection")

# Pick a competitive match (close to 50/50)
fixtures$competitiveness <- abs(fixtures$team1_win_prob - 0.5)
competitive_idx <- which.min(fixtures$competitiveness)
match <- fixtures[competitive_idx, ]

cli::cli_alert_info("Selected most competitive match:")
cli::cli_alert(" {match$team1} vs {match$team2}")
cli::cli_alert(" Date: {match$match_date}")
cli::cli_alert(" Venue: {match$venue}")
cli::cli_alert(" ELO: {round(match$team1_elo)} vs {round(match$team2_elo)}")
cli::cli_alert(" Pre-match probability: {round(match$team1_win_prob * 100, 1)}% vs {round((1 - match$team1_win_prob) * 100, 1)}%")
cli::cli_alert(" Actual winner: {match$outcome_winner}")

# 5. Run Simulation ----
cli::cli_h2("Running Match Simulation")

set_simulation_seed(RANDOM_SEED)

results <- run_simulations(
  n = N_SIMULATIONS,
  sim_fn = simulate_match_outcome,
  team1_win_prob = match$team1_win_prob,
  team1 = match$team1,
  team2 = match$team2,
  progress = TRUE
)

# 6. Results ----
cli::cli_h2("Simulation Results")

summary <- aggregate_match_results(results, match$team1, match$team2)

cli::cli_alert_info("{match$team1}: {round(summary$team1_win_pct * 100, 1)}% ({format(summary$team1_wins, big.mark = ',')} wins)")
cli::cli_alert_info("{match$team2}: {round(summary$team2_win_pct * 100, 1)}% ({format(summary$team2_wins, big.mark = ',')} wins)")

# Confidence interval
ci <- binom.test(summary$team1_wins, N_SIMULATIONS)$conf.int
cli::cli_alert_info("95% CI for {match$team1}: {round(ci[1] * 100, 1)}% - {round(ci[2] * 100, 1)}%")

# Compare to actual
actual_winner_was_team1 <- match$outcome_winner == match$team1
simulated_favorite <- summary$team1_win_pct > 0.5

if (actual_winner_was_team1 == simulated_favorite) {
  cli::cli_alert_success("Simulation correctly favored the actual winner")
} else {
  cli::cli_alert_warning("Actual winner was the simulation underdog")
}

# 7. Batch Analysis ----
cli::cli_h2("Full Season Accuracy")

# Simulate all matches and check accuracy
correct <- 0
total <- nrow(fixtures)

set_simulation_seed(RANDOM_SEED)
for (i in seq_len(total)) {
  f <- fixtures[i, ]
  predicted_team1 <- f$team1_win_prob > 0.5
  actual_team1 <- f$outcome_winner == f$team1
  if (predicted_team1 == actual_team1) correct <- correct + 1
}

accuracy <- correct / total * 100
cli::cli_alert_info("ELO prediction accuracy: {round(accuracy, 1)}% ({correct}/{total} correct)")

# 8. Summary ----
cli::cli_h2("Summary")
cli::cli_alert_success("Match simulation complete")
