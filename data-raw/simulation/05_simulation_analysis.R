# 05 Simulation Analysis ----
#
# Analyzes simulation results and validates against actual outcomes.
# Generates visualizations comparing predicted vs actual standings.

# 1. Setup ----
library(DBI)
library(dplyr)
library(ggplot2)
devtools::load_all()

cli::cli_h1("Simulation Analysis")

# 2. Configuration ----
EVENT_NAME <- "Indian Premier League"
TARGET_SEASON <- "2023"
N_SIMULATIONS <- 1000
RANDOM_SEED <- 42

cli::cli_h2("Configuration")
cli::cli_alert_info("Event: {EVENT_NAME}")
cli::cli_alert_info("Season: {TARGET_SEASON}")

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

# 4. Generate Simulation Data ----
cli::cli_h2("Running Simulations")

# Get actual standings
actual_standings <- get_actual_standings(fixtures)

# Run season simulations
simulated_results <- simulate_season_n(
  fixtures = fixtures,
  n_simulations = N_SIMULATIONS,
  seed = RANDOM_SEED,
  progress = TRUE
)

# 5. Prepare Analysis Data ----
cli::cli_h2("Preparing Analysis")

# Merge actual and simulated
analysis_data <- actual_standings |>
  select(team, actual_wins = wins, actual_position = position) |>
  left_join(
    simulated_results |>
      select(team, sim_avg_wins = avg_wins, playoff_pct, championship_pct),
    by = "team"
  ) |>
  mutate(
    win_diff = sim_avg_wins - actual_wins,
    prediction_error = abs(win_diff)
  )

# 6. Summary Statistics ----
cli::cli_h2("Summary Statistics")

mae <- mean(analysis_data$prediction_error)
rmse <- sqrt(mean(analysis_data$prediction_error^2))
correlation <- cor(analysis_data$actual_wins, analysis_data$sim_avg_wins)

cli::cli_alert_info("Mean Absolute Error: {round(mae, 2)} wins")
cli::cli_alert_info("RMSE: {round(rmse, 2)} wins")
cli::cli_alert_info("Correlation (actual vs predicted): {round(correlation, 3)}")

# Teams most over/under-rated
most_overrated <- analysis_data |> arrange(desc(win_diff)) |> slice(1)
most_underrated <- analysis_data |> arrange(win_diff) |> slice(1)

cli::cli_alert_info("Most overrated: {most_overrated$team} (+{round(most_overrated$win_diff, 1)} predicted wins)")
cli::cli_alert_info("Most underrated: {most_underrated$team} ({round(most_underrated$win_diff, 1)} predicted wins)")

# 7. Visualization: Predicted vs Actual Wins ----
cli::cli_h2("Visualization: Predicted vs Actual")

p1 <- ggplot(analysis_data, aes(x = actual_wins, y = sim_avg_wins)) +
  geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "gray50") +
  geom_point(aes(color = prediction_error), size = 4) +
  geom_text(aes(label = team), hjust = -0.1, vjust = -0.5, size = 3) +
  scale_color_gradient(low = "darkgreen", high = "red", name = "Error") +
  labs(
    title = paste("IPL", TARGET_SEASON, "- Predicted vs Actual Wins"),
    subtitle = paste("Based on", format(N_SIMULATIONS, big.mark = ","), "season simulations"),
    x = "Actual Wins",
    y = "Predicted Average Wins"
  ) +
  theme_minimal() +
  theme(plot.title = element_text(face = "bold")) +
  coord_equal(xlim = c(0, 15), ylim = c(0, 15))

print(p1)

# 8. Visualization: Championship Probability ----
cli::cli_h2("Visualization: Championship Odds")

# Order by championship probability
analysis_data <- analysis_data |>
  mutate(team = factor(team, levels = team[order(championship_pct)]))

p2 <- ggplot(analysis_data, aes(x = team, y = championship_pct, fill = playoff_pct)) +
  geom_col(alpha = 0.8) +
  geom_text(aes(label = sprintf("%.1f%%", championship_pct)),
            hjust = -0.1, size = 3.5) +
  coord_flip() +
  scale_fill_gradient(low = "lightblue", high = "darkblue",
                      name = "Playoff %") +
  scale_y_continuous(limits = c(0, max(analysis_data$championship_pct) * 1.3)) +
  labs(
    title = paste("IPL", TARGET_SEASON, "- Simulated Championship Probability"),
    subtitle = paste("Based on", format(N_SIMULATIONS, big.mark = ","), "season simulations"),
    x = NULL,
    y = "Championship Probability (%)"
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(face = "bold"),
    axis.text.y = element_text(size = 10)
  )

print(p2)

# 9. Match-Level Analysis ----
cli::cli_h2("Match Prediction Accuracy")

# Calculate prediction accuracy
fixtures <- fixtures |>
  mutate(
    predicted_winner = if_else(team1_win_prob > 0.5, team1, team2),
    prediction_correct = predicted_winner == outcome_winner,
    confidence = abs(team1_win_prob - 0.5) + 0.5
  )

overall_accuracy <- mean(fixtures$prediction_correct) * 100
high_conf_matches <- fixtures |> filter(confidence > 0.6)
high_conf_accuracy <- mean(high_conf_matches$prediction_correct) * 100

cli::cli_alert_info("Overall prediction accuracy: {round(overall_accuracy, 1)}%")
cli::cli_alert_info("High-confidence (>60%) accuracy: {round(high_conf_accuracy, 1)}% ({nrow(high_conf_matches)} matches)")

# Calibration check
cli::cli_h3("Calibration by Confidence Bucket")

fixtures <- fixtures |>
  mutate(
    conf_bucket = cut(confidence,
                      breaks = c(0.5, 0.55, 0.6, 0.65, 0.7, 1.0),
                      labels = c("50-55%", "55-60%", "60-65%", "65-70%", "70%+"))
  )

calibration <- fixtures |>
  group_by(conf_bucket) |>
  summarise(
    n_matches = n(),
    accuracy = mean(prediction_correct) * 100,
    avg_confidence = mean(confidence) * 100,
    .groups = "drop"
  )

for (i in seq_len(nrow(calibration))) {
  c <- calibration[i, ]
  cli::cli_alert(" {c$conf_bucket}: {round(c$accuracy, 1)}% accurate ({c$n_matches} matches)")
}

# 10. Summary ----
cli::cli_h2("Summary")

cli::cli_alert_success("Simulation analysis complete")
cli::cli_bullets(c(
  " " = "MAE: {round(mae, 2)} wins per team",
  " " = "Correlation: {round(correlation, 3)}",
  " " = "Match accuracy: {round(overall_accuracy, 1)}%"
))
