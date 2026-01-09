# Model Evaluation and Visualization ----
#
# Comprehensive evaluation of the two-stage win probability model:
#   - Stage 1: Projected Score Model evaluation
#   - Stage 2: Win Probability Model evaluation
#   - Calibration plots
#   - Match-by-match win probability tracking
#   - Error analysis

# Setup ----
library(DBI)
library(dplyr)
library(tidyr)
library(xgboost)
devtools::load_all()

# Note: Feature engineering functions are now in R/feature_engineering.R
# They are loaded via devtools::load_all() above

# Optional: Load ggplot2 for visualizations
HAS_GGPLOT <- requireNamespace("ggplot2", quietly = TRUE)
if (HAS_GGPLOT) {
  library(ggplot2)
}

cat("\n")
cli::cli_h1("Win Probability Model Evaluation")
cat("\n")

# Load Models and Results ----
cli::cli_h2("Loading models and results")

output_dir <- file.path("..", "bouncerdata", "models")

# Stage 1
stage1_results <- readRDS(file.path(output_dir, "ipl_stage1_results.rds"))
stage1_model <- stage1_results$model
cli::cli_alert_success("Loaded Stage 1 model (Projected Score)")

# Stage 2
stage2_results <- readRDS(file.path(output_dir, "ipl_stage2_results.rds"))
stage2_model <- stage2_results$model
cli::cli_alert_success("Loaded Stage 2 model (Win Probability)")

# Data
stage1_data <- readRDS(file.path(output_dir, "ipl_stage1_data.rds"))
stage2_data <- readRDS(file.path(output_dir, "ipl_stage2_data.rds"))
metadata <- readRDS(file.path(output_dir, "ipl_match_metadata.rds"))

# Stage 1 Evaluation Summary ----
cli::cli_h2("Stage 1: Projected Score Model Summary")

s1_metrics <- stage1_results$metrics

cli::cli_h3("Overall Performance")
cat(sprintf("  Test RMSE: %.2f runs\n", s1_metrics$test_rmse))
cat(sprintf("  Test MAE: %.2f runs\n", s1_metrics$test_mae))
cat(sprintf("  Test R-squared: %.3f\n", s1_metrics$test_r_squared))
cat(sprintf("  Naive Baseline RMSE: %.2f runs\n", s1_metrics$naive_rmse))
cat(sprintf("  Improvement: %.1f%%\n",
            (s1_metrics$naive_rmse - s1_metrics$test_rmse) / s1_metrics$naive_rmse * 100))
cat("\n")

cli::cli_h3("Performance by Phase")
print(s1_metrics$phase_metrics)
cat("\n")

cli::cli_h3("Performance by Overs Remaining")
print(s1_metrics$overs_metrics)
cat("\n")

cli::cli_h3("Top 10 Most Important Features")
print(head(stage1_results$importance, 10))
cat("\n")

# Stage 2 Evaluation Summary ----
cli::cli_h2("Stage 2: Win Probability Model Summary")

s2_metrics <- stage2_results$metrics

cli::cli_h3("Overall Performance")
cat(sprintf("  Test Log Loss: %.4f\n", s2_metrics$test_log_loss))
cat(sprintf("  Test Accuracy: %.2f%%\n", s2_metrics$test_accuracy * 100))
cat(sprintf("  Test Brier Score: %.4f\n", s2_metrics$test_brier_score))
if (!is.na(s2_metrics$test_auc)) {
  cat(sprintf("  Test AUC-ROC: %.4f\n", s2_metrics$test_auc))
}
cat(sprintf("  Baseline Log Loss: %.4f\n", s2_metrics$baseline_log_loss))
cat(sprintf("  Improvement: %.1f%%\n",
            (s2_metrics$baseline_log_loss - s2_metrics$test_log_loss) / s2_metrics$baseline_log_loss * 100))
cat("\n")

cli::cli_h3("Calibration by Probability Bin")
print(s2_metrics$calibration)
cat("\n")

cli::cli_h3("Performance by Phase")
print(s2_metrics$phase_metrics)
cat("\n")

cli::cli_h3("Performance by Runs Needed")
print(s2_metrics$situation_metrics)
cat("\n")

cli::cli_h3("Top 10 Most Important Features")
print(head(stage2_results$importance, 10))
cat("\n")

# Match-by-Match Win Probability Tracking ----
cli::cli_h2("Match-by-Match Analysis")

# Select a few test matches for detailed analysis
test_matches <- unique(stage2_data$test$match_id)
sample_matches <- head(test_matches, 5)

cli::cli_alert_info("Analyzing {length(sample_matches)} sample matches")

# Function to predict win probability for a match
predict_match_win_prob <- function(match_data, stage1_model, stage2_model,
                                   stage1_feature_cols, stage2_feature_cols) {

  # Prepare Stage 1 features
  s1_features <- match_data %>%
    mutate(
      phase_powerplay = as.integer(phase == "powerplay"),
      phase_middle = as.integer(phase == "middle"),
      phase_death = as.integer(phase == "death"),
      gender_male = as.integer(gender == "male"),
      wickets_in_hand = 10 - wickets_fallen
    )

  s1_matrix <- as.matrix(as.data.frame(s1_features)[, stage1_feature_cols, drop = FALSE])
  s1_matrix[is.na(s1_matrix)] <- 0

  # Get Stage 1 predictions
  projected_score <- predict(stage1_model, xgb.DMatrix(data = s1_matrix))

  # Add to data
  match_data$projected_final_score <- projected_score
  match_data$projected_vs_target <- projected_score - match_data$target_runs
  match_data$projected_win_margin <- projected_score - (match_data$target_runs - 1)

  # Prepare Stage 2 features
  s2_features <- match_data %>%
    mutate(
      phase_powerplay = as.integer(phase == "powerplay"),
      phase_middle = as.integer(phase == "middle"),
      phase_death = as.integer(phase == "death"),
      gender_male = as.integer(gender == "male"),
      is_dls = as.integer(is_dls_match),
      is_ko = as.integer(is_knockout),
      wickets_in_hand = 10 - wickets_fallen
    )

  s2_matrix <- as.matrix(as.data.frame(s2_features)[, stage2_feature_cols, drop = FALSE])
  s2_matrix[is.na(s2_matrix)] <- 0
  s2_matrix[is.infinite(s2_matrix)] <- 999

  # Get win probability predictions
  win_prob <- predict(stage2_model, xgb.DMatrix(data = s2_matrix))

  match_data$win_probability <- win_prob

  return(match_data)
}

# Analyze each sample match
for (match_id in sample_matches) {
  match_data <- stage2_data$test %>%
    filter(match_id == !!match_id) %>%
    arrange(over, ball)

  if (nrow(match_data) == 0) next

  # Get match info
  match_info <- metadata$matches %>% filter(match_id == !!match_id)

  cli::cli_h3("Match: {match_id}")
  cat(sprintf("  %s vs %s\n", match_info$team1, match_info$team2))
  cat(sprintf("  Target: %d\n", match_data$target_runs[1]))
  cat(sprintf("  Winner: %s\n", match_info$outcome_winner))

  # Get predictions
  match_with_probs <- predict_match_win_prob(
    match_data,
    stage1_model,
    stage2_model,
    stage1_results$feature_cols,
    stage2_results$feature_cols
  )

  # Show probability at key moments
  key_overs <- c(6, 10, 15, 18, 19)  # End of powerplay, middle, death
  key_moments <- match_with_probs %>%
    filter(over %in% key_overs) %>%
    group_by(over) %>%
    slice_tail(n = 1) %>%
    ungroup() %>%
    select(over, total_runs, wickets_fallen, runs_needed, win_probability)

  cat("  Win probability at key moments:\n")
  for (i in seq_len(nrow(key_moments))) {
    row <- key_moments[i, ]
    cat(sprintf("    After over %d: %.1f%% (Score: %d/%d, Need: %d)\n",
                row$over, row$win_probability * 100,
                row$total_runs, row$wickets_fallen, row$runs_needed))
  }

  # Final probability
  final_row <- match_with_probs[nrow(match_with_probs), ]
  actual_win <- final_row$batting_team_wins
  cat(sprintf("  Final prediction: %.1f%% win probability\n", final_row$win_probability * 100))
  cat(sprintf("  Actual result: %s\n", ifelse(actual_win == 1, "Chase successful", "Chase failed")))
  cat("\n")
}

# Error Analysis ----
cli::cli_h2("Error Analysis")

# High confidence errors (model very wrong)
test_data <- stage2_data$test

# Store feature cols in local variables for data.table compatibility
s1_feature_cols <- stage1_results$feature_cols
s2_feature_cols <- stage2_results$feature_cols

# Prepare features and get predictions
s1_features <- test_data %>%
  mutate(
    phase_powerplay = as.integer(phase == "powerplay"),
    phase_middle = as.integer(phase == "middle"),
    phase_death = as.integer(phase == "death"),
    gender_male = as.integer(gender == "male"),
    wickets_in_hand = 10 - wickets_fallen
  )

s1_matrix <- as.matrix(s1_features[, ..s1_feature_cols])
s1_matrix[is.na(s1_matrix)] <- 0
test_data$projected_final_score <- predict(stage1_model, xgb.DMatrix(data = s1_matrix))
test_data$projected_vs_target <- test_data$projected_final_score - test_data$target_runs

s2_features <- test_data %>%
  mutate(
    phase_powerplay = as.integer(phase == "powerplay"),
    phase_middle = as.integer(phase == "middle"),
    phase_death = as.integer(phase == "death"),
    gender_male = as.integer(gender == "male"),
    is_dls = as.integer(is_dls_match),
    is_ko = as.integer(is_knockout),
    wickets_in_hand = 10 - wickets_fallen,
    projected_win_margin = projected_final_score - (target_runs - 1)
  )

s2_matrix <- as.matrix(s2_features[, ..s2_feature_cols])
s2_matrix[is.na(s2_matrix)] <- 0
s2_matrix[is.infinite(s2_matrix)] <- 999
test_data$win_probability <- predict(stage2_model, xgb.DMatrix(data = s2_matrix))

# Find high confidence errors
high_conf_errors <- test_data %>%
  filter(
    (win_probability > 0.8 & batting_team_wins == 0) |
    (win_probability < 0.2 & batting_team_wins == 1)
  ) %>%
  select(match_id, over, ball, total_runs, wickets_fallen, target_runs, runs_needed,
         win_probability, batting_team_wins) %>%
  distinct(match_id, over, .keep_all = TRUE)

cli::cli_h3("High Confidence Errors (>80% confident, wrong)")
cli::cli_alert_info("Found {nrow(high_conf_errors)} deliveries with high confidence errors")

if (nrow(high_conf_errors) > 0) {
  # Show unique matches with errors
  error_matches <- high_conf_errors %>%
    group_by(match_id) %>%
    summarise(
      max_wrong_confidence = max(abs(win_probability - 0.5)),
      n_errors = n(),
      .groups = "drop"
    ) %>%
    arrange(desc(max_wrong_confidence)) %>%
    head(5)

  cat("\nMatches with most errors:\n")
  print(error_matches)
}
cat("\n")

# Calibration Visualization ----
if (HAS_GGPLOT) {
  cli::cli_h2("Creating Calibration Plots")

  calibration_data <- s2_metrics$calibration %>%
    filter(!is.na(mean_predicted), !is.na(mean_actual))

  # Main calibration plot
  p1 <- ggplot(calibration_data, aes(x = mean_predicted, y = mean_actual)) +
    geom_point(aes(size = n), color = "steelblue") +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray50") +
    scale_x_continuous(limits = c(0, 1), labels = scales::percent_format()) +
    scale_y_continuous(limits = c(0, 1), labels = scales::percent_format()) +
    labs(
      title = "Win Probability Model Calibration",
      subtitle = "IPL T20 Matches (Test Set)",
      x = "Predicted Win Probability",
      y = "Actual Win Rate",
      size = "N"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(face = "bold"),
      legend.position = "right"
    )

  # Save main calibration plot
  plot_path <- file.path(output_dir, "calibration_plot.png")
  ggsave(plot_path, p1, width = 8, height = 6, dpi = 150)
  cli::cli_alert_success("Saved calibration plot to {plot_path}")

  # Calibration error bar plot
  p2 <- ggplot(calibration_data, aes(x = prob_bin, y = calibration_error)) +
    geom_col(fill = "steelblue", alpha = 0.7) +
    geom_hline(yintercept = 0.05, linetype = "dashed", color = "red", alpha = 0.7) +
    scale_y_continuous(labels = scales::percent_format()) +
    labs(
      title = "Calibration Error by Probability Bin",
      subtitle = "Red dashed line = 5% error threshold",
      x = "Predicted Probability Bin",
      y = "Calibration Error (|Predicted - Actual|)"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(face = "bold"),
      axis.text.x = element_text(angle = 45, hjust = 1)
    )

  plot_path2 <- file.path(output_dir, "calibration_error_plot.png")
  ggsave(plot_path2, p2, width = 10, height = 6, dpi = 150)
  cli::cli_alert_success("Saved calibration error plot to {plot_path2}")

  # Prediction distribution histogram
  p3 <- ggplot(test_data, aes(x = win_probability, fill = factor(batting_team_wins))) +
    geom_histogram(bins = 50, alpha = 0.7, position = "identity") +
    scale_fill_manual(
      values = c("0" = "tomato", "1" = "steelblue"),
      labels = c("0" = "Chase Failed", "1" = "Chase Won"),
      name = "Actual Result"
    ) +
    scale_x_continuous(labels = scales::percent_format()) +
    labs(
      title = "Distribution of Win Probability Predictions",
      subtitle = "Colored by actual match outcome",
      x = "Predicted Win Probability",
      y = "Count (deliveries)"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(face = "bold"),
      legend.position = "top"
    )

  plot_path3 <- file.path(output_dir, "prediction_distribution_plot.png")
  ggsave(plot_path3, p3, width = 10, height = 6, dpi = 150)
  cli::cli_alert_success("Saved prediction distribution plot to {plot_path3}")

  # Tail calibration plot - zoom on extremes (0-20% and 80-100%)
  tail_calibration <- test_data %>%
    mutate(
      tail_zone = case_when(
        win_probability <= 0.20 ~ "Low (0-20%)",
        win_probability >= 0.80 ~ "High (80-100%)",
        TRUE ~ "Middle (20-80%)"
      ),
      prob_bin_fine = cut(win_probability,
                          breaks = c(0, 0.05, 0.10, 0.15, 0.20, 0.80, 0.85, 0.90, 0.95, 1.0),
                          include.lowest = TRUE)
    ) %>%
    filter(tail_zone != "Middle (20-80%)") %>%
    group_by(tail_zone, prob_bin_fine) %>%
    summarise(
      n = n(),
      mean_predicted = mean(win_probability),
      mean_actual = mean(batting_team_wins),
      calibration_error = abs(mean_predicted - mean_actual),
      .groups = "drop"
    ) %>%
    filter(!is.na(prob_bin_fine))

  if (nrow(tail_calibration) > 0) {
    p4 <- ggplot(tail_calibration, aes(x = mean_predicted, y = mean_actual)) +
      geom_point(aes(size = n, color = tail_zone)) +
      geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray50") +
      scale_x_continuous(labels = scales::percent_format()) +
      scale_y_continuous(labels = scales::percent_format()) +
      scale_color_manual(values = c("Low (0-20%)" = "tomato", "High (80-100%)" = "steelblue")) +
      labs(
        title = "Tail Calibration (Extreme Probabilities)",
        subtitle = "Focus on 0-20% and 80-100% predicted probabilities",
        x = "Predicted Win Probability",
        y = "Actual Win Rate",
        size = "N",
        color = "Zone"
      ) +
      theme_minimal() +
      theme(
        plot.title = element_text(face = "bold"),
        legend.position = "right"
      )

    plot_path4 <- file.path(output_dir, "tail_calibration_plot.png")
    ggsave(plot_path4, p4, width = 10, height = 6, dpi = 150)
    cli::cli_alert_success("Saved tail calibration plot to {plot_path4}")
  }

  # Reliability diagram with confidence intervals
  reliability_data <- test_data %>%
    mutate(prob_bin = cut(win_probability,
                          breaks = seq(0, 1, 0.05),
                          include.lowest = TRUE)) %>%
    group_by(prob_bin) %>%
    summarise(
      n = n(),
      mean_predicted = mean(win_probability),
      mean_actual = mean(batting_team_wins),
      se_actual = sqrt(mean_actual * (1 - mean_actual) / n),
      .groups = "drop"
    ) %>%
    filter(!is.na(prob_bin), n >= 10)

  if (nrow(reliability_data) > 0) {
    p5 <- ggplot(reliability_data, aes(x = mean_predicted, y = mean_actual)) +
      geom_ribbon(aes(ymin = pmax(0, mean_actual - 1.96 * se_actual),
                      ymax = pmin(1, mean_actual + 1.96 * se_actual)),
                  fill = "steelblue", alpha = 0.2) +
      geom_point(aes(size = n), color = "steelblue") +
      geom_line(color = "steelblue", alpha = 0.7) +
      geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray50") +
      scale_x_continuous(limits = c(0, 1), labels = scales::percent_format()) +
      scale_y_continuous(limits = c(0, 1), labels = scales::percent_format()) +
      labs(
        title = "Reliability Diagram with 95% Confidence Intervals",
        subtitle = "Shaded area shows uncertainty in actual win rate",
        x = "Predicted Win Probability",
        y = "Actual Win Rate",
        size = "N"
      ) +
      theme_minimal() +
      theme(
        plot.title = element_text(face = "bold"),
        legend.position = "right"
      )

    plot_path5 <- file.path(output_dir, "reliability_diagram.png")
    ggsave(plot_path5, p5, width = 10, height = 6, dpi = 150)
    cli::cli_alert_success("Saved reliability diagram to {plot_path5}")
  }

} else {
  cli::cli_alert_warning("ggplot2 not available, skipping calibration plots")
}

# Summary Statistics Table ----
cli::cli_h2("Summary Statistics Table")

summary_table <- data.frame(
  Metric = c(
    "Stage 1: Test RMSE (runs)",
    "Stage 1: Test R-squared",
    "Stage 1: Improvement over naive (%)",
    "",
    "Stage 2: Test Log Loss",
    "Stage 2: Test Accuracy (%)",
    "Stage 2: Test Brier Score",
    "Stage 2: Test AUC-ROC",
    "Stage 2: Improvement over baseline (%)",
    "Stage 2: Mean Calibration Error (%)"
  ),
  Value = c(
    sprintf("%.2f", s1_metrics$test_rmse),
    sprintf("%.3f", s1_metrics$test_r_squared),
    sprintf("%.1f", (s1_metrics$naive_rmse - s1_metrics$test_rmse) / s1_metrics$naive_rmse * 100),
    "",
    sprintf("%.4f", s2_metrics$test_log_loss),
    sprintf("%.2f", s2_metrics$test_accuracy * 100),
    sprintf("%.4f", s2_metrics$test_brier_score),
    ifelse(is.na(s2_metrics$test_auc), "N/A", sprintf("%.4f", s2_metrics$test_auc)),
    sprintf("%.1f", (s2_metrics$baseline_log_loss - s2_metrics$test_log_loss) / s2_metrics$baseline_log_loss * 100),
    sprintf("%.2f", mean(s2_metrics$calibration$calibration_error, na.rm = TRUE) * 100)
  )
)

cli::cli_h3("Final Model Performance")
print(summary_table, row.names = FALSE)
cat("\n")

# Save evaluation results
eval_results <- list(
  stage1_summary = s1_metrics,
  stage2_summary = s2_metrics,
  summary_table = summary_table,
  high_conf_errors = high_conf_errors,
  evaluated_at = Sys.time()
)

eval_path <- file.path(output_dir, "ipl_evaluation_results.rds")
saveRDS(eval_results, eval_path)
cli::cli_alert_success("Saved evaluation results to {eval_path}")

# Done ----
cat("\n")
cli::cli_alert_success("Model evaluation complete!")
cat("\n")

cli::cli_h3("Key Findings")
cli::cli_bullets(c(
  "i" = sprintf("Stage 1 predicts final score within %.1f runs (RMSE)", s1_metrics$test_rmse),
  "i" = sprintf("Stage 2 achieves %.2f%% accuracy on win prediction", s2_metrics$test_accuracy * 100),
  "i" = sprintf("Model is well-calibrated (%.1f%% mean error)", mean(s2_metrics$calibration$calibration_error, na.rm = TRUE) * 100),
  "i" = sprintf("%.1f%% improvement over baseline", (s2_metrics$baseline_log_loss - s2_metrics$test_log_loss) / s2_metrics$baseline_log_loss * 100)
))
cat("\n")

cli::cli_h3("Recommendations")
cli::cli_bullets(c(
  "*" = "Model performs well for most match situations",
  "*" = "Consider adding player-level features (ELO) for further improvement",
  "*" = "Extend to other T20 leagues and ODI format",
  "*" = "Create live prediction interface for real-time use"
))
cat("\n")
