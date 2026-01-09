# Evaluate Match Prediction Model (Two-Stage) ----
#
# This script performs detailed evaluation of the trained two-stage prediction model.
# Primary: Margin model evaluation (MAE, RMSE vs ELO baseline)
# Secondary: Win probability evaluation (calibration, accuracy)
# Supports all formats (T20, ODI, Test) - set FORMAT below.
#
# Output:
#   - Console output with evaluation metrics
#   - bouncerdata/models/{format}_prediction_evaluation.rds

# 1. Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

if (!requireNamespace("xgboost", quietly = TRUE)) {
  stop("xgboost package required. Install with: install.packages('xgboost')")
}
library(xgboost)

# 2. Configuration ----
FORMAT <- "t20"  # Options: "t20", "odi", "test"

# Output directory
output_dir <- file.path("..", "bouncerdata", "models")

cat("\n")
cli::cli_h1("{toupper(FORMAT)} Match Prediction Model Evaluation")
cat("\n")

# 3. Load Models and Data ----
cli::cli_h2("Loading models and data")

# Load margin model (Stage 1)
margin_model_path <- file.path(output_dir, paste0(FORMAT, "_margin_model.ubj"))
if (!file.exists(margin_model_path)) {
  cli::cli_alert_warning("Margin model not found at {margin_model_path}")
  margin_model <- NULL
} else {
  margin_model <- xgb.load(margin_model_path)
  cli::cli_alert_success("Loaded margin model from {margin_model_path}")
}

# Load win probability model (Stage 2)
model_path <- file.path(output_dir, paste0(FORMAT, "_prediction_model.ubj"))
if (!file.exists(model_path)) {
  stop("Win probability model not found at ", model_path, ". Run 03_train_prediction_model.R first.")
}
model <- xgb.load(model_path)
cli::cli_alert_success("Loaded win probability model from {model_path}")

training_results <- readRDS(file.path(output_dir, paste0(FORMAT, "_prediction_training.rds")))
feature_cols <- training_results$feature_cols

features_data <- readRDS(file.path(output_dir, paste0(FORMAT, "_prediction_features.rds")))
test_data <- features_data$test

cli::cli_alert_success("Loaded {nrow(test_data)} test matches")

# 4. Prepare Test Data ----
cli::cli_h2("Preparing test data")

# Use package function for feature preparation (matches script 03)
test_prepared <- prepare_prediction_features(test_data)

# Get base features (without predicted_margin)
base_feature_cols <- get_prediction_feature_cols()
X_test_base <- as.matrix(test_prepared[, base_feature_cols])
X_test_base[is.na(X_test_base)] <- 0

y_test <- test_prepared$team1_wins
y_margin_test <- test_data$actual_margin

# =============================================================================
# 5. MARGIN MODEL EVALUATION (PRIMARY METRIC)
# =============================================================================

cli::cli_h1("Margin Model Evaluation (Primary)")

if (!is.null(margin_model)) {
  # Generate margin predictions
  dtest_base <- xgb.DMatrix(data = X_test_base)
  predicted_margin <- predict(margin_model, dtest_base)

  # Filter to matches with actual margin
  margin_valid <- !is.na(y_margin_test)
  n_valid <- sum(margin_valid)

  if (n_valid > 0) {
    # Model margin metrics
    model_margin_mae <- mean(abs(predicted_margin[margin_valid] - y_margin_test[margin_valid]))
    model_margin_rmse <- sqrt(mean((predicted_margin[margin_valid] - y_margin_test[margin_valid])^2))
    model_margin_cor <- cor(predicted_margin[margin_valid], y_margin_test[margin_valid])

    # Baseline: ELO-only expected margin
    baseline_margin_mae <- mean(abs(test_data$expected_margin[margin_valid] - y_margin_test[margin_valid]))
    baseline_margin_rmse <- sqrt(mean((test_data$expected_margin[margin_valid] - y_margin_test[margin_valid])^2))
    baseline_margin_cor <- cor(test_data$expected_margin[margin_valid], y_margin_test[margin_valid])

    # Improvement
    mae_improvement <- (baseline_margin_mae - model_margin_mae) / baseline_margin_mae * 100
    rmse_improvement <- (baseline_margin_rmse - model_margin_rmse) / baseline_margin_rmse * 100

    cli::cli_h2("Margin Prediction Performance")
    cat(sprintf("\n%-20s %12s %12s %12s\n", "Metric", "Model", "Baseline", "Improvement"))
    cat(paste(rep("-", 60), collapse = ""), "\n")
    cat(sprintf("%-20s %10.1f %12.1f %+11.1f%%\n", "MAE (runs)", model_margin_mae, baseline_margin_mae, mae_improvement))
    cat(sprintf("%-20s %10.1f %12.1f %+11.1f%%\n", "RMSE (runs)", model_margin_rmse, baseline_margin_rmse, rmse_improvement))
    cat(sprintf("%-20s %10.3f %12.3f %12s\n", "Correlation", model_margin_cor, baseline_margin_cor, ""))
    cat("\n")

    cli::cli_alert_success("Margin MAE: {round(model_margin_mae, 1)} runs (vs {round(baseline_margin_mae, 1)} baseline)")
    cli::cli_alert_success("Improvement: {sprintf('%+.1f%%', mae_improvement)}")

    # Analysis by margin size
    cli::cli_h2("Margin Prediction by Game Type")

    margin_analysis <- data.frame(
      predicted = predicted_margin[margin_valid],
      actual = y_margin_test[margin_valid]
    ) %>%
      mutate(
        abs_actual = abs(actual),
        game_type = case_when(
          abs_actual <= 15 ~ "Close (<15 runs)",
          abs_actual <= 40 ~ "Moderate (15-40 runs)",
          TRUE ~ "Blowout (>40 runs)"
        ),
        direction_correct = (sign(predicted) == sign(actual))
      )

    margin_by_type <- margin_analysis %>%
      group_by(game_type) %>%
      summarise(
        n = n(),
        mae = mean(abs(predicted - actual)),
        direction_accuracy = mean(direction_correct),
        .groups = "drop"
      )

    cat("\nMargin prediction accuracy by game type:\n")
    for (i in seq_len(nrow(margin_by_type))) {
      cat(sprintf("  %s: MAE=%.1f runs, Direction=%.1f%% (n=%d)\n",
                  margin_by_type$game_type[i],
                  margin_by_type$mae[i],
                  margin_by_type$direction_accuracy[i] * 100,
                  margin_by_type$n[i]))
    }

    margin_model_metrics <- list(
      model_mae = model_margin_mae,
      model_rmse = model_margin_rmse,
      model_cor = model_margin_cor,
      baseline_mae = baseline_margin_mae,
      baseline_rmse = baseline_margin_rmse,
      baseline_cor = baseline_margin_cor,
      mae_improvement = mae_improvement,
      rmse_improvement = rmse_improvement,
      n_matches = n_valid,
      by_game_type = margin_by_type
    )
  } else {
    cli::cli_alert_warning("No matches with actual margin data")
    margin_model_metrics <- NULL
  }

  # Add predicted margin to features for win prob model
  test_prepared$predicted_margin <- predicted_margin
} else {
  cli::cli_alert_warning("No margin model available - skipping margin evaluation")
  margin_model_metrics <- NULL
  test_prepared$predicted_margin <- 0
}

# =============================================================================
# 6. WIN PROBABILITY MODEL EVALUATION (SECONDARY)
# =============================================================================

cli::cli_h1("Win Probability Evaluation (Secondary)")

# Prepare features with predicted margin
X_test <- as.matrix(test_prepared[, feature_cols])
X_test[is.na(X_test)] <- 0

# Generate predictions
dtest <- xgb.DMatrix(data = X_test)
pred_probs <- predict(model, dtest)
pred_class <- as.integer(pred_probs > 0.5)

test_prepared$pred_prob <- pred_probs
test_prepared$pred_class <- pred_class

# 7. Overall Metrics ----
cli::cli_h2("Win Probability Performance")

accuracy <- mean(pred_class == y_test)
log_loss <- -mean(y_test * log(pmax(pred_probs, 1e-7)) +
                   (1 - y_test) * log(pmax(1 - pred_probs, 1e-7)))
brier <- mean((pred_probs - y_test)^2)

cli::cli_alert_success("Accuracy: {round(accuracy * 100, 1)}%")
cli::cli_alert_success("Log Loss: {round(log_loss, 4)}")
cli::cli_alert_success("Brier Score: {round(brier, 4)}")

# 6. Baseline Comparisons ----
cli::cli_h2("Baseline Comparisons")

# Baseline 1: Always predict higher ELO team
baseline_elo <- as.integer(test_prepared$elo_diff_result > 0)
baseline_elo_acc <- mean(baseline_elo == y_test)
cli::cli_alert_info("Baseline (higher result ELO wins): {round(baseline_elo_acc * 100, 1)}%")

# Baseline 2: Always predict higher roster ELO team
baseline_roster <- as.integer(test_prepared$elo_diff_roster > 0)
baseline_roster_acc <- mean(baseline_roster == y_test)
cli::cli_alert_info("Baseline (higher roster ELO wins): {round(baseline_roster_acc * 100, 1)}%")

# Baseline 3: Random (50%)
cli::cli_alert_info("Baseline (random): 50.0%")

# Baseline 4: Always predict team with better form
baseline_form <- as.integer(test_prepared$form_diff > 0)
baseline_form_acc <- mean(baseline_form == y_test)
cli::cli_alert_info("Baseline (better form wins): {round(baseline_form_acc * 100, 1)}%")

cli::cli_alert_success("Model improvement over best baseline: +{round((accuracy - max(baseline_elo_acc, baseline_roster_acc, baseline_form_acc)) * 100, 1)}%")

# 7. Analysis by Season ----
cli::cli_h2("Performance by Season")

season_results <- test_prepared %>%
  group_by(season) %>%
  summarise(
    n_matches = n(),
    accuracy = mean(pred_class == team1_wins),
    log_loss = -mean(team1_wins * log(pmax(pred_prob, 1e-7)) +
                      (1 - team1_wins) * log(pmax(1 - pred_prob, 1e-7))),
    avg_confidence = mean(ifelse(pred_prob > 0.5, pred_prob, 1 - pred_prob)),
    .groups = "drop"
  )

for (i in seq_len(nrow(season_results))) {
  cli::cli_alert_info(
    "Season {season_results$season[i]}: {round(season_results$accuracy[i] * 100, 1)}% accuracy ({season_results$n_matches[i]} matches)"
  )
}

# 8. Analysis by Knockout Status ----
cli::cli_h2("Performance by Match Type")

knockout_results <- test_prepared %>%
  group_by(is_knockout) %>%
  summarise(
    n_matches = n(),
    accuracy = mean(pred_class == team1_wins),
    avg_confidence = mean(ifelse(pred_prob > 0.5, pred_prob, 1 - pred_prob)),
    .groups = "drop"
  )

cli::cli_alert_info("League matches: {round(knockout_results$accuracy[knockout_results$is_knockout == 0] * 100, 1)}% accuracy")
if (any(knockout_results$is_knockout == 1)) {
  cli::cli_alert_info("Knockout matches: {round(knockout_results$accuracy[knockout_results$is_knockout == 1] * 100, 1)}% accuracy")
}

# 9. Analysis by Confidence Level ----
cli::cli_h2("Performance by Confidence Level")

test_prepared <- test_prepared %>%
  mutate(
    confidence = ifelse(pred_prob > 0.5, pred_prob, 1 - pred_prob),
    confidence_bin = cut(confidence, breaks = c(0.5, 0.55, 0.6, 0.65, 0.7, 1.0),
                         labels = c("50-55%", "55-60%", "60-65%", "65-70%", "70%+"),
                         include.lowest = TRUE)
  )

confidence_results <- test_prepared %>%
  group_by(confidence_bin) %>%
  summarise(
    n_matches = n(),
    accuracy = mean(pred_class == team1_wins),
    .groups = "drop"
  ) %>%
  filter(!is.na(confidence_bin))

for (i in seq_len(nrow(confidence_results))) {
  cli::cli_alert_info(
    "Confidence {confidence_results$confidence_bin[i]}: {round(confidence_results$accuracy[i] * 100, 1)}% accuracy ({confidence_results$n_matches[i]} matches)"
  )
}

# 10. Calibration Analysis ----
cli::cli_h2("Calibration Analysis")

calibration <- test_prepared %>%
  mutate(prob_bin = cut(pred_prob, breaks = seq(0, 1, 0.1), include.lowest = TRUE)) %>%
  group_by(prob_bin) %>%
  summarise(
    n = n(),
    mean_pred = mean(pred_prob),
    actual_rate = mean(team1_wins),
    calibration_error = abs(mean(pred_prob) - mean(team1_wins)),
    .groups = "drop"
  ) %>%
  filter(n >= 3)

cat("\nPredicted Prob vs Actual Win Rate:\n")
cat(sprintf("%-15s %10s %10s %10s %10s\n", "Bin", "N", "Predicted", "Actual", "Error"))
cat(paste(rep("-", 55), collapse = ""), "\n")

for (i in seq_len(nrow(calibration))) {
  cat(sprintf("%-15s %10d %9.1f%% %9.1f%% %9.1f%%\n",
              calibration$prob_bin[i],
              calibration$n[i],
              calibration$mean_pred[i] * 100,
              calibration$actual_rate[i] * 100,
              calibration$calibration_error[i] * 100))
}

# Mean absolute calibration error
mace <- mean(calibration$calibration_error)
cli::cli_alert_info("Mean Absolute Calibration Error: {round(mace * 100, 2)}%")

# 11. Biggest Upsets ----
cli::cli_h2("Notable Predictions")

cli::cli_h3("Biggest Correct Underdogs")
incorrect_underdogs <- test_prepared %>%
  filter(pred_class != team1_wins) %>%  # We predicted wrong
  arrange(confidence) %>%
  tail(10)

for (i in seq_len(nrow(incorrect_underdogs))) {
  match <- incorrect_underdogs[i, ]
  predicted <- ifelse(match$pred_prob > 0.5, match$team1, match$team2)
  actual <- ifelse(match$team1_wins == 1, match$team1, match$team2)
  cli::cli_alert_info(
    "{match$team1} vs {match$team2}: Predicted {predicted} ({round(match$confidence * 100)}%), {actual} won"
  )
}

cli::cli_h3("Biggest Correct Favorites")
correct_favorites <- test_prepared %>%
  filter(pred_class == team1_wins, confidence > 0.65) %>%
  arrange(desc(confidence)) %>%
  head(5)

for (i in seq_len(nrow(correct_favorites))) {
  match <- correct_favorites[i, ]
  winner <- ifelse(match$team1_wins == 1, match$team1, match$team2)
  cli::cli_alert_info(
    "{match$team1} vs {match$team2}: {winner} won (predicted at {round(match$confidence * 100)}%)"
  )
}

# 12. Save Evaluation Results ----
cli::cli_h2("Saving evaluation results")

eval_path <- file.path(output_dir, paste0(FORMAT, "_prediction_evaluation.rds"))
saveRDS(list(
  # Primary: Margin model metrics
  margin_model = margin_model_metrics,
  # Secondary: Win probability metrics
  winprob = list(
    accuracy = accuracy,
    log_loss = log_loss,
    brier_score = brier,
    n_matches = nrow(test_prepared)
  ),
  baselines = list(
    elo_result = baseline_elo_acc,
    elo_roster = baseline_roster_acc,
    form = baseline_form_acc
  ),
  by_season = season_results,
  by_knockout = knockout_results,
  by_confidence = confidence_results,
  calibration = calibration,
  predictions = test_prepared %>% select(
    match_id, team1, team2, pred_prob, pred_class, team1_wins,
    confidence, season, is_knockout, predicted_margin
  ),
  created_at = Sys.time()
), eval_path)

cli::cli_alert_success("Saved evaluation results to {eval_path}")

# 13. Summary ----
cat("\n")
cli::cli_h1("Evaluation Summary")

# Primary: Margin Model
if (!is.null(margin_model_metrics)) {
  cat("\n  MARGIN MODEL (Primary):\n")
  cat(sprintf("    MAE: %.1f runs (baseline: %.1f)\n",
              margin_model_metrics$model_mae, margin_model_metrics$baseline_mae))
  cat(sprintf("    Improvement: %+.1f%%\n", margin_model_metrics$mae_improvement))
  cat(sprintf("    Correlation: %.3f\n", margin_model_metrics$model_cor))
}

# Secondary: Win Probability
cat("\n  WIN PROBABILITY (Secondary):\n")
cat(sprintf("    Accuracy: %.1f%%\n", accuracy * 100))
cat(sprintf("    Log Loss: %.4f\n", log_loss))
cat(sprintf("    vs ELO baseline: %+.1f%%\n", (accuracy - baseline_elo_acc) * 100))

cat("\n  Calibration:\n")
cat(sprintf("    Mean Abs Error: %.2f%%\n", mace * 100))

cat("\n")
cli::cli_alert_success("Two-stage model evaluation complete!")
cat("\n")
