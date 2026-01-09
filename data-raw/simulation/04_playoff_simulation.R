# 04 Playoff Simulation ----
#
# Simulates playoff brackets using real data from completed seasons.
# Uses actual playoff qualifiers and their ELO ratings to calculate
# championship probabilities via Monte Carlo simulation.

# 1. Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

cli::cli_h1("Playoff Simulation")

# 2. Configuration ----
EVENT_NAME <- "Indian Premier League"
TARGET_SEASON <- "2023"  # Use a completed season
N_SIMULATIONS <- 10000
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
  stop("No data available")
}

cli::cli_alert_success("Loaded {nrow(fixtures)} matches")

# 4. Get Actual Standings ----
cli::cli_h2("League Stage Results")

actual_standings <- get_actual_standings(fixtures)

cli::cli_alert_info("Final league standings:")
for (i in seq_len(nrow(actual_standings))) {
  s <- actual_standings[i, ]
  marker <- if (s$makes_playoffs) "*" else " "
  cli::cli_alert("{marker} {i}. {s$team}: {s$wins}W-{s$losses}L")
}

# 5. IPL Playoff Format ----
cli::cli_h2("IPL Playoff Format")

cat("
  +-------------------------------------------------------------+
  |                                                             |
  |   QUALIFIER 1                                               |
  |   Team 1 vs Team 2  ----------------------+                 |
  |                                           |                 |
  |                                           v                 |
  |                                        FINAL                |
  |                                        Winner Q1            |
  |   ELIMINATOR                              vs                |
  |   Team 3 vs Team 4  --+                Winner Q2            |
  |                       |                   |                 |
  |                       v                   |                 |
  |                   QUALIFIER 2             |                 |
  |                   Loser Q1                |                 |
  |                      vs     --------------+                 |
  |                   Winner E                                  |
  |                                                             |
  +-------------------------------------------------------------+
")

# 6. Get Playoff Teams ----
cli::cli_h2("Playoff Qualifiers")

playoff_teams <- get_playoff_teams(actual_standings, fixtures)

cli::cli_alert_info("Teams entering playoffs:")
for (i in 1:4) {
  t <- playoff_teams[i, ]
  cli::cli_alert(" {i}. {t$team} (ELO: {round(t$elo)})")
}

# 7. Run Playoff Simulations ----
cli::cli_h2("Running Playoff Simulations")

playoff_results <- simulate_playoffs_n(
  teams = playoff_teams,
  n_simulations = N_SIMULATIONS,
  seed = RANDOM_SEED,
  progress = TRUE
)

# 8. Results ----
cli::cli_h2("Playoff Simulation Results")

# Sort by championship probability
playoff_results <- playoff_results |>
  arrange(desc(championship_pct))

cli::cli_alert_info("Championship probabilities after {format(N_SIMULATIONS, big.mark = ',')} simulations:")

for (i in seq_len(nrow(playoff_results))) {
  r <- playoff_results[i, ]
  cli::cli_alert(" {r$team}: {round(r$final_pct, 1)}% final, {round(r$championship_pct, 1)}% champion")
}

# 9. Match-by-Match Probabilities ----
cli::cli_h2("Match-by-Match Probabilities")

# Calculate probabilities for each playoff match
team1 <- playoff_teams$team[1]
team2 <- playoff_teams$team[2]
team3 <- playoff_teams$team[3]
team4 <- playoff_teams$team[4]

elo1 <- playoff_teams$elo[1]
elo2 <- playoff_teams$elo[2]
elo3 <- playoff_teams$elo[3]
elo4 <- playoff_teams$elo[4]

q1_prob <- elo_win_probability(elo1, elo2)
elim_prob <- elo_win_probability(elo3, elo4)

cli::cli_alert_info("Qualifier 1: {team1} ({round(q1_prob * 100)}%) vs {team2} ({round((1 - q1_prob) * 100)}%)")
cli::cli_alert_info("Eliminator: {team3} ({round(elim_prob * 100)}%) vs {team4} ({round((1 - elim_prob) * 100)}%)")

# 10. Summary ----
cli::cli_h2("Summary")

champion_favorite <- playoff_results$team[1]
champion_prob <- playoff_results$championship_pct[1]

cli::cli_alert_success("Playoff simulation complete")
cli::cli_bullets(c(
  " " = "Championship favorite: {champion_favorite} ({round(champion_prob, 1)}%)",
  " " = "Based on {format(N_SIMULATIONS, big.mark = ',')} playoff bracket simulations",
  " " = "Using pre-playoff ELO ratings"
))
