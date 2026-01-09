# 03 Season Simulation ----
#
# Simulates a full cricket season using real fixtures and ELO-based probabilities.
# Runs Monte Carlo simulations to calculate playoff and championship probabilities.
# Compares simulated standings to actual final standings.

# 1. Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

cli::cli_h1("Season Simulation")

# 2. Configuration ----
EVENT_NAME <- "Indian Premier League"
TARGET_SEASON <- "2023"  # Use a completed season to compare results
N_SIMULATIONS <- 1000
RANDOM_SEED <- 42

cli::cli_h2("Configuration")
cli::cli_alert_info("Event: {EVENT_NAME}")
cli::cli_alert_info("Season: {TARGET_SEASON}")
cli::cli_alert_info("Simulations: {format(N_SIMULATIONS, big.mark = ',')}")

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

teams <- unique(c(fixtures$team1, fixtures$team2))
cli::cli_alert_info("Teams: {length(teams)}")

# 4. Actual Standings ----
cli::cli_h2("Actual Final Standings")

actual_standings <- get_actual_standings(fixtures)

for (i in seq_len(nrow(actual_standings))) {
  s <- actual_standings[i, ]
  playoff_marker <- if (s$makes_playoffs) "*" else " "
  cli::cli_alert("{playoff_marker} {i}. {s$team}: {s$wins}W-{s$losses}L ({s$points} pts)")
}

cli::cli_alert_info("* = Made playoffs")

# 5. Run Season Simulations ----
cli::cli_h2("Running Season Simulations")

simulated_results <- simulate_season_n(
  fixtures = fixtures,
  n_simulations = N_SIMULATIONS,
  seed = RANDOM_SEED,
  progress = TRUE
)

# 6. Simulated Probabilities ----
cli::cli_h2("Simulated Season Probabilities")

# Sort by playoff probability
simulated_results <- simulated_results |>
  arrange(desc(playoff_pct))

cli::cli_alert_info("Team probabilities after {format(N_SIMULATIONS, big.mark = ',')} simulations:")

for (i in seq_len(nrow(simulated_results))) {
  s <- simulated_results[i, ]
  cli::cli_alert(" {s$team}: {round(s$avg_wins, 1)} avg wins, {round(s$playoff_pct, 1)}% playoffs, {round(s$championship_pct, 1)}% #1")
}

# 7. Compare to Actual ----
cli::cli_h2("Comparison: Simulated vs Actual")

# Merge actual and simulated
comparison <- actual_standings |>
  select(team, actual_wins = wins, actual_position = position, actual_playoffs = makes_playoffs) |>
  left_join(
    simulated_results |>
      select(team, sim_avg_wins = avg_wins, sim_playoff_pct = playoff_pct),
    by = "team"
  ) |>
  arrange(actual_position)

cli::cli_alert_info("Comparing predicted vs actual performance:")

for (i in seq_len(nrow(comparison))) {
  c <- comparison[i, ]
  diff <- c$sim_avg_wins - c$actual_wins
  direction <- if (diff > 0.5) "overrated" else if (diff < -0.5) "underrated" else "accurate"
  cli::cli_alert(" {c$team}: Predicted {round(c$sim_avg_wins, 1)}W, Actual {c$actual_wins}W ({direction})")
}

# Check playoff prediction accuracy
actual_playoff_teams <- actual_standings$team[actual_standings$makes_playoffs]
predicted_playoff_teams <- simulated_results$team[simulated_results$playoff_pct > 50]

correct_playoff_predictions <- sum(actual_playoff_teams %in% predicted_playoff_teams)
cli::cli_alert_success("Playoff prediction: {correct_playoff_predictions}/4 teams correctly identified as favorites")

# 8. Summary ----
cli::cli_h2("Summary")

cli::cli_alert_success("Season simulation complete")
cli::cli_bullets(c(
  " " = "Simulated {format(N_SIMULATIONS, big.mark = ',')} hypothetical seasons",
  " " = "Based on pre-match ELO probabilities",
  " " = "Actual champion: {actual_standings$team[1]}"
))
