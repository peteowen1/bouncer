# 01 Simulation Framework Demo ----
#
# Demonstrates the core simulation infrastructure using real data.
# Loads fixtures from a completed season and shows how simulation functions work.
#
# All functions are defined in R/simulation.R

# 1. Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

cli::cli_h1("Simulation Framework Demo")

# 2. Database Connection ----
cli::cli_h2("Connecting to Database")

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

cli::cli_alert_success("Connected to database")

# 3. Available Seasons ----
cli::cli_h2("Available Seasons")

seasons <- get_available_seasons("Indian Premier League", conn)

if (nrow(seasons) > 0) {
  cli::cli_alert_info("Found {nrow(seasons)} IPL seasons:")
  for (i in seq_len(min(5, nrow(seasons)))) {
    cli::cli_alert(" {seasons$season[i]}: {seasons$n_matches[i]} matches")
  }
} else {
  cli::cli_alert_warning("No IPL seasons found - run team ELO calculations first")
  cli::cli_alert_info("Showing demo with mock data instead")
}

# 4. Load Real Fixtures ----
cli::cli_h2("Loading Season Fixtures")

# Use most recent complete IPL season
target_season <- if (nrow(seasons) > 0) seasons$season[1] else "2023"

fixtures <- get_season_fixtures("Indian Premier League", target_season, conn)

if (nrow(fixtures) > 0) {
  cli::cli_alert_success("Loaded {nrow(fixtures)} fixtures from IPL {target_season}")

  # Show sample fixtures
  cli::cli_h3("Sample Fixtures")
  sample_fixtures <- head(fixtures, 5)
  for (i in seq_len(nrow(sample_fixtures))) {
    f <- sample_fixtures[i, ]
    cli::cli_alert(" {f$team1} vs {f$team2}: {f$team1_win_prob * 100:.1f}% vs {(1 - f$team1_win_prob) * 100:.1f}%")
  }
} else {
  cli::cli_alert_warning("No fixtures found for IPL {target_season}")
}

# 5. ELO Win Probability Demo ----
cli::cli_h2("ELO Win Probability")

# Demo with sample ELOs
demo_elos <- data.frame(
  team1_elo = c(1550, 1500, 1600, 1450),
  team2_elo = c(1500, 1500, 1400, 1550)
)

demo_elos$win_prob <- elo_win_probability(demo_elos$team1_elo, demo_elos$team2_elo)

cli::cli_alert_info("ELO to win probability examples:")
for (i in seq_len(nrow(demo_elos))) {
  cli::cli_alert(" {demo_elos$team1_elo[i]} vs {demo_elos$team2_elo[i]} -> {demo_elos$win_prob[i] * 100:.1f}%")
}

# 6. Single Match Simulation ----
cli::cli_h2("Single Match Simulation")

if (nrow(fixtures) > 0) {
  # Pick a real match
  match <- fixtures[1, ]
  cli::cli_alert_info("Match: {match$team1} vs {match$team2}")
  cli::cli_alert_info("Pre-match probability: {match$team1_win_prob * 100:.1f}% vs {(1 - match$team1_win_prob) * 100:.1f}%")
  cli::cli_alert_info("Actual winner: {match$outcome_winner}")

  # Simulate 100 times
  set_simulation_seed(42)
  results <- run_simulations(
    n = 100,
    sim_fn = simulate_match_outcome,
    team1_win_prob = match$team1_win_prob,
    team1 = match$team1,
    team2 = match$team2,
    progress = FALSE
  )

  summary <- aggregate_match_results(results, match$team1, match$team2)
  cli::cli_alert_success("Simulated {summary$team1}: {summary$team1_win_pct * 100:.1f}%")
}

# 7. Summary ----
cli::cli_h2("Framework Summary")

cli::cli_alert_success("Simulation framework demo complete")
cli::cli_bullets(c(
  " " = "See 02_match_simulation.R for detailed match simulation",
  " " = "See 03_season_simulation.R for full season Monte Carlo",
  " " = "See 04_playoff_simulation.R for playoff bracket simulation",
  " " = "See 05_simulation_analysis.R for results analysis"
))
