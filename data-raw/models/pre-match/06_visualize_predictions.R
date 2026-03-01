# Visualize Match Predictions ----
#
# This script creates visualizations for prediction model performance,
# team ELO progressions, and calibration analysis.
# Supports all formats (T20, ODI, Test) - set FORMAT below.
#
# Output:
#   - Plots displayed in RStudio
#   - Optional: saved to bouncerdata/figures/

# 1. Setup ----
library(DBI)
library(dplyr)
library(ggplot2)
devtools::load_all()

# 2. Configuration ----
FORMAT <- "t20"  # Options: "t20", "odi", "test"
SAVE_PLOTS <- FALSE  # Set to TRUE to save plots as PNG files

# Output directories (use package helper to find the correct bouncerdata path)
output_dir <- file.path(find_bouncerdata_dir(), "models")
figure_dir <- file.path(find_bouncerdata_dir(), "figures")
if (!dir.exists(figure_dir)) {
  dir.create(figure_dir, recursive = TRUE)
}

cat("\n")
cli::cli_h1("{toupper(FORMAT)} Prediction Visualization")
cat("\n")

# 3. Load Data ----
cli::cli_h2("Loading data")

# Team ELO history
elo_path <- file.path(output_dir, "team_elo_history.rds")
if (file.exists(elo_path)) {
  elo_data <- readRDS(elo_path)
  cli::cli_alert_success("Loaded team ELO history")
} else {
  cli::cli_alert_warning("Team ELO history not found")
  elo_data <- NULL
}

# Evaluation results
eval_path <- file.path(output_dir, paste0(FORMAT, "_prediction_evaluation.rds"))
if (file.exists(eval_path)) {
  eval_data <- readRDS(eval_path)
  cli::cli_alert_success("Loaded evaluation results")
} else {
  cli::cli_alert_warning("Evaluation results not found at {eval_path}")
  eval_data <- NULL
}

# Training results
train_path <- file.path(output_dir, paste0(FORMAT, "_prediction_training.rds"))
if (file.exists(train_path)) {
  train_data <- readRDS(train_path)
  cli::cli_alert_success("Loaded training results")
} else {
  cli::cli_alert_warning("Training results not found at {train_path}")
  train_data <- NULL
}

# 4. Plot 1: Team ELO Progression ----
if (!is.null(elo_data)) {
  cli::cli_h2("Team ELO Progression")

  # Get top teams by final ELO
  top_teams <- elo_data$final_standings %>%
    head(8) %>%
    pull(team_id)

  elo_progression <- elo_data$history %>%
    filter(team_id %in% top_teams) %>%
    arrange(match_date)

  p1 <- ggplot(elo_progression, aes(x = match_date, y = elo_result, color = team_id)) +
    geom_line(linewidth = 0.8, alpha = 0.8) +
    geom_hline(yintercept = 1500, linetype = "dashed", color = "gray50", alpha = 0.5) +
    labs(
      title = "Team ELO Progression (Top 8 Teams)",
      subtitle = "Result-based ELO over time",
      x = "Date",
      y = "ELO Rating",
      color = "Team"
    ) +
    theme_minimal() +
    theme(
      legend.position = "right",
      plot.title = element_text(face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    ) +
    scale_color_brewer(palette = "Set2")

  print(p1)

  if (SAVE_PLOTS) {
    ggsave(file.path(figure_dir, "team_elo_progression.png"), p1,
           width = 12, height = 6, dpi = 150)
    cli::cli_alert_success("Saved team_elo_progression.png")
  }
}

# 5. Plot 2: Calibration ----
if (!is.null(eval_data) && !is.null(eval_data$calibration)) {
  cli::cli_h2("Calibration Plot")

  calibration <- eval_data$calibration

  p2 <- ggplot(calibration, aes(x = mean_pred, y = actual_rate)) +
    geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "gray50") +
    geom_point(aes(size = n), color = "steelblue", alpha = 0.7) +
    geom_line(color = "steelblue", alpha = 0.5) +
    scale_size_continuous(range = c(3, 10), name = "N Matches") +
    labs(
      title = "Model Calibration",
      subtitle = "Predicted probability vs actual win rate",
      x = "Predicted Probability (Team 1 Wins)",
      y = "Actual Win Rate"
    ) +
    coord_fixed(xlim = c(0, 1), ylim = c(0, 1)) +
    theme_minimal() +
    theme(
      plot.title = element_text(face = "bold")
    )

  print(p2)

  if (SAVE_PLOTS) {
    ggsave(file.path(figure_dir, "calibration_plot.png"), p2,
           width = 8, height = 8, dpi = 150)
    cli::cli_alert_success("Saved calibration_plot.png")
  }
}

# 6. Plot 3: Feature Importance ----
if (!is.null(train_data) && !is.null(train_data$importance)) {
  cli::cli_h2("Feature Importance")

  importance <- train_data$importance %>%
    head(12) %>%
    mutate(Feature = factor(Feature, levels = rev(Feature)))

  p3 <- ggplot(importance, aes(x = Gain, y = Feature)) +
    geom_col(fill = "steelblue", alpha = 0.8) +
    labs(
      title = "Feature Importance",
      subtitle = "XGBoost gain scores",
      x = "Importance (Gain)",
      y = NULL
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(face = "bold")
    )

  print(p3)

  if (SAVE_PLOTS) {
    ggsave(file.path(figure_dir, "feature_importance.png"), p3,
           width = 10, height = 6, dpi = 150)
    cli::cli_alert_success("Saved feature_importance.png")
  }
}

# 7. Plot 4: Accuracy by Confidence ----
if (!is.null(eval_data) && !is.null(eval_data$by_confidence)) {
  cli::cli_h2("Accuracy by Confidence")

  conf_data <- eval_data$by_confidence %>%
    filter(!is.na(confidence_bin))

  p4 <- ggplot(conf_data, aes(x = confidence_bin, y = accuracy)) +
    geom_col(aes(fill = accuracy), alpha = 0.8) +
    geom_text(aes(label = sprintf("%.1f%%", accuracy * 100)),
              vjust = -0.5, size = 3.5) +
    geom_text(aes(label = sprintf("n=%d", n_matches), y = 0.05),
              size = 3, color = "white") +
    scale_fill_gradient(low = "coral", high = "forestgreen", guide = "none") +
    labs(
      title = "Prediction Accuracy by Confidence Level",
      subtitle = "Higher confidence predictions should be more accurate",
      x = "Model Confidence",
      y = "Accuracy"
    ) +
    ylim(0, 1) +
    theme_minimal() +
    theme(
      plot.title = element_text(face = "bold")
    )

  print(p4)

  if (SAVE_PLOTS) {
    ggsave(file.path(figure_dir, "accuracy_by_confidence.png"), p4,
           width = 10, height = 6, dpi = 150)
    cli::cli_alert_success("Saved accuracy_by_confidence.png")
  }
}

# 8. Plot 5: Prediction Distribution ----
if (!is.null(eval_data) && !is.null(eval_data$predictions)) {
  cli::cli_h2("Prediction Distribution")

  predictions <- eval_data$predictions

  p5 <- ggplot(predictions, aes(x = pred_prob, fill = factor(team1_wins))) +
    geom_histogram(bins = 20, alpha = 0.7, position = "identity") +
    scale_fill_manual(
      values = c("0" = "coral", "1" = "forestgreen"),
      labels = c("Team 2 Won", "Team 1 Won"),
      name = "Actual Outcome"
    ) +
    labs(
      title = "Distribution of Predicted Probabilities",
      subtitle = "By actual match outcome",
      x = "Predicted Probability (Team 1 Wins)",
      y = "Count"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(face = "bold"),
      legend.position = "top"
    )

  print(p5)

  if (SAVE_PLOTS) {
    ggsave(file.path(figure_dir, "prediction_distribution.png"), p5,
           width = 10, height = 6, dpi = 150)
    cli::cli_alert_success("Saved prediction_distribution.png")
  }
}

# 9. Plot 6: Season Performance ----
if (!is.null(eval_data) && !is.null(eval_data$by_season)) {
  cli::cli_h2("Performance by Season")

  season_data <- eval_data$by_season

  p6 <- ggplot(season_data, aes(x = season, y = accuracy)) +
    geom_col(fill = "steelblue", alpha = 0.8) +
    geom_text(aes(label = sprintf("%.1f%%", accuracy * 100)),
              vjust = -0.5, size = 3.5) +
    geom_hline(yintercept = 0.5, linetype = "dashed", color = "gray50", alpha = 0.5) +
    labs(
      title = "Prediction Accuracy by Season",
      subtitle = "Test set performance",
      x = "Season",
      y = "Accuracy"
    ) +
    ylim(0, 1) +
    theme_minimal() +
    theme(
      plot.title = element_text(face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )

  print(p6)

  if (SAVE_PLOTS) {
    ggsave(file.path(figure_dir, "accuracy_by_season.png"), p6,
           width = 8, height = 6, dpi = 150)
    cli::cli_alert_success("Saved accuracy_by_season.png")
  }
}

# 10. Summary ----
cat("\n")
cli::cli_alert_success("Visualization complete!")
cat("\n")

if (SAVE_PLOTS) {
  cli::cli_alert_info("Plots saved to: {figure_dir}")
} else {
  cli::cli_alert_info("Set SAVE_PLOTS <- TRUE to save plots as PNG files")
}
cat("\n")
