# Stage 2: Win Probability Model (2nd Innings) ----
#
# XGBoost binary classification model to predict win probability
# for the team batting second (chasing team).
#
# Integrates Stage 1 projected score model predictions as features.
#
# Target: batting_team_wins (binary: 1 = chase successful, 0 = chase failed)
# Features: target, current state, pressure metrics, momentum, Stage 1 predictions

# Setup ----
library(DBI)
library(dplyr)
library(xgboost)
devtools::load_all()

# Note: Feature engineering functions are now in R/feature_engineering.R
# They are loaded via devtools::load_all() above

# Configuration ----
RANDOM_SEED <- 42
CV_FOLDS <- 5
MAX_ROUNDS <- 2000
EARLY_STOPPING <- 20
PRINT_EVERY_N <- 20

cat("\n")
cli::cli_h1("Stage 2: Win Probability Model (XGBoost)")
cat("\n")

# Load Prepared Data ----
cli::cli_h2("Loading prepared data")

# Stage 2 data
stage2_path <- file.path("..", "bouncerdata", "models", "ipl_stage2_data.rds")
if (!file.exists(stage2_path)) {
  cli::cli_alert_danger("Data not found at {stage2_path}")
  cli::cli_alert_info("Run 01_prepare_data.R first")
  stop("Data file not found")
}

stage2_data <- readRDS(stage2_path)
train_data <- stage2_data$train
test_data <- stage2_data$test

cli::cli_alert_success("Loaded {nrow(train_data)} training deliveries")
cli::cli_alert_success("Loaded {nrow(test_data)} test deliveries")

# Load Stage 1 model for predictions
stage1_results_path <- file.path("..", "bouncerdata", "models", "ipl_stage1_results.rds")
if (!file.exists(stage1_results_path)) {
  cli::cli_alert_danger("Stage 1 model not found at {stage1_results_path}")
  cli::cli_alert_info("Run 02_stage1_projected_score_model.R first")
  stop("Stage 1 model not found")
}

stage1_results <- readRDS(stage1_results_path)
stage1_model <- stage1_results$model
stage1_feature_cols <- stage1_results$feature_cols

cli::cli_alert_success("Loaded Stage 1 model")

# Generate Stage 1 Predictions ----
cli::cli_h2("Generating Stage 1 predictions for Stage 2 data")

# Prepare Stage 1 features for Stage 2 data
# We need to create the same features that Stage 1 uses
prepare_stage1_features_for_stage2 <- function(data, feature_cols) {

  # Calculate any missing features needed for Stage 1
  data <- data %>%
    mutate(
      # Phase one-hot encoding
      phase_powerplay = as.integer(phase == "powerplay"),
      phase_middle = as.integer(phase == "middle"),
      phase_death = as.integer(phase == "death"),

      # Gender
      gender_male = as.integer(gender == "male"),

      # Wickets in hand (if not already present)
      wickets_in_hand = 10 - wickets_fallen
    )

  # Select only the features needed for Stage 1 model
  # Handle missing columns gracefully
  available_cols <- intersect(feature_cols, names(data))
  missing_cols <- setdiff(feature_cols, names(data))

  if (length(missing_cols) > 0) {
    cli::cli_alert_warning("Missing columns for Stage 1: {paste(missing_cols, collapse = ', ')}")
    # Add missing columns as 0
    for (col in missing_cols) {
      data[[col]] <- 0
    }
  }

  # Return as data.frame with required columns
  as.data.frame(data)[, feature_cols, drop = FALSE]
}

# Generate predictions for train and test
train_stage1_features <- prepare_stage1_features_for_stage2(train_data, stage1_feature_cols)
test_stage1_features <- prepare_stage1_features_for_stage2(test_data, stage1_feature_cols)

# Fill NAs in rolling features
train_stage1_features[is.na(train_stage1_features)] <- 0
test_stage1_features[is.na(test_stage1_features)] <- 0

# Create DMatrix and predict
dtrain_s1 <- xgb.DMatrix(data = as.matrix(train_stage1_features[, stage1_feature_cols]))
dtest_s1 <- xgb.DMatrix(data = as.matrix(test_stage1_features[, stage1_feature_cols]))

train_data$projected_final_score <- predict(stage1_model, dtrain_s1)
test_data$projected_final_score <- predict(stage1_model, dtest_s1)

# Calculate derived features from Stage 1 predictions
train_data <- train_data %>%
  mutate(
    projected_vs_target = projected_final_score - target_runs,
    projected_win_margin = projected_final_score - (target_runs - 1)  # Projected margin if current trajectory
  )

test_data <- test_data %>%
  mutate(
    projected_vs_target = projected_final_score - target_runs,
    projected_win_margin = projected_final_score - (target_runs - 1)
  )

cli::cli_alert_success("Generated Stage 1 predictions")
cli::cli_alert_info("Mean projected score (train): {round(mean(train_data$projected_final_score), 1)}")
cli::cli_alert_info("Mean projected vs target (train): {round(mean(train_data$projected_vs_target), 1)}")

# Prepare Stage 2 Features ----
cli::cli_h2("Preparing Stage 2 features")

prepare_stage2_features <- function(data) {

  # Calculate tail calibration features first
  tail_features <- calculate_tail_calibration_features(
    runs_needed = data$runs_needed,
    balls_remaining = data$balls_remaining,
    wickets_in_hand = 10 - data$wickets_fallen
  )

  # Bind tail features to data
  data <- cbind(data, tail_features)

  features <- data %>%
    mutate(
      # One-hot encode phase
      phase_powerplay = as.integer(phase == "powerplay"),
      phase_middle = as.integer(phase == "middle"),
      phase_death = as.integer(phase == "death"),

      # Gender
      gender_male = as.integer(gender == "male"),

      # DLS and knockout flags
      is_dls = as.integer(is_dls_match),
      is_ko = as.integer(is_knockout),

      # Calculate wickets_in_hand fresh (avoid duplicate column issues)
      wickets_in_hand = 10 - wickets_fallen
    )

  # Base columns to select
  base_cols <- c(
    # Target label
    "batting_team_wins",

    # Target tracking
    "target_runs",
    "runs_needed",
    "balls_remaining",
    "required_run_rate",

    # Current state
    "total_runs",
    "wickets_fallen",
    "wickets_in_hand",
    "current_run_rate",

    # Pressure metrics
    "rr_differential",
    "balls_per_run_needed",
    "balls_per_wicket_available",
    "is_death_chase",

    # Tail calibration features (help with extreme probabilities)
    "chase_completed",
    "chase_impossible",
    "runs_per_ball_needed",
    "balls_per_run_available",
    "resources_per_run",
    "chase_buffer",
    "chase_buffer_ratio",
    "is_easy_chase",
    "is_difficult_chase",
    "theoretical_min_balls",
    "balls_surplus",

    # Phase
    "phase_powerplay",
    "phase_middle",
    "phase_death",
    "overs_into_phase",

    # 1st innings context
    "innings1_total",
    "innings1_run_rate",
    "innings1_wickets",

    # Stage 1 predictions (key integration!)
    "projected_final_score",
    "projected_vs_target",
    "projected_win_margin",

    # Rolling ball features
    "runs_last_12_balls",
    "runs_last_24_balls",
    "dots_last_12_balls",
    "dots_last_24_balls",
    "boundaries_last_12_balls",
    "boundaries_last_24_balls",
    "wickets_last_12_balls",
    "wickets_last_24_balls",

    # Rolling over features
    "runs_last_3_overs",
    "runs_last_6_overs",
    "wickets_last_3_overs",
    "wickets_last_6_overs",
    "rr_last_3_overs",
    "rr_last_6_overs",

    # Venue
    "venue_avg_score",
    "venue_chase_success_rate",

    # Match context
    "gender_male",
    "is_dls",
    "is_ko",

    # Keep match_id for grouping
    "match_id"
  )

  # Add expected outcome features if available
  exp_outcome_cols <- c("exp_runs", "exp_wicket_prob")
  available_exp_cols <- exp_outcome_cols[exp_outcome_cols %in% names(data)]
  if (length(available_exp_cols) > 0) {
    cli::cli_alert_info("Including expected outcome features: {paste(available_exp_cols, collapse = ', ')}")
    base_cols <- c(base_cols, available_exp_cols)
  }

  features <- features %>%
    select(all_of(base_cols))

  return(features)
}

train_features <- prepare_stage2_features(train_data)
test_features <- prepare_stage2_features(test_data)

# Handle NA and Inf values
train_features[is.na(train_features)] <- 0
test_features[is.na(test_features)] <- 0

# Replace Inf values in required_run_rate and related
train_features <- train_features %>%
  mutate(across(where(is.numeric), ~ifelse(is.infinite(.), 999, .)))
test_features <- test_features %>%
  mutate(across(where(is.numeric), ~ifelse(is.infinite(.), 999, .)))

cli::cli_alert_success("Prepared {ncol(train_features) - 2} features")

# Feature list
feature_cols <- setdiff(names(train_features), c("batting_team_wins", "match_id"))

cli::cli_h3("Features Used")
cat(paste(" -", feature_cols, collapse = "\n"))
cat("\n")

# Check class balance
win_rate <- mean(train_features$batting_team_wins)
cli::cli_alert_info("Training win rate (chase success): {round(win_rate * 100, 1)}%")

# Create Grouped CV Folds ----
cli::cli_h2("Creating grouped CV folds")

folds <- create_grouped_folds(train_features, n_folds = CV_FOLDS, seed = RANDOM_SEED)

cli::cli_alert_success("Created {CV_FOLDS} grouped folds by match_id")
cli::cli_alert_info("Fold sizes: {paste(sapply(folds, length), collapse = ', ')} deliveries")

# Create XGBoost DMatrix ----
cli::cli_h2("Creating XGBoost matrices")

# Convert to data.frame to avoid data.table syntax issues
train_features <- as.data.frame(train_features)
test_features <- as.data.frame(test_features)

dtrain <- xgb.DMatrix(
  data = as.matrix(train_features[, feature_cols]),
  label = train_features$batting_team_wins
)

dtest <- xgb.DMatrix(
  data = as.matrix(test_features[, feature_cols]),
  label = test_features$batting_team_wins
)

cli::cli_alert_success("XGBoost matrices created")

# Cross-Validation ----
cli::cli_h2("Cross-validation to find optimal rounds")

# Adjust scale_pos_weight if class imbalance (optional)
scale_pos_weight <- (1 - win_rate) / win_rate

params <- list(
  objective = "binary:logistic",
  max_depth = 6,
  eta = 0.05,
  subsample = 0.8,
  colsample_bytree = 0.8,
  min_child_weight = 3,
  gamma = 0,
  # scale_pos_weight = 1,  # Set to 1 for balanced, or scale_pos_weight for adjustment
  eval_metric = "logloss"
)

cli::cli_alert_info("Parameters:")
cli::cli_alert_info("  objective: {params$objective}")
cli::cli_alert_info("  max_depth: {params$max_depth}")
cli::cli_alert_info("  eta (learning rate): {params$eta}")
cli::cli_alert_info("  subsample: {params$subsample}")
cli::cli_alert_info("  colsample_bytree: {params$colsample_bytree}")
cli::cli_alert_info("  min_child_weight: {params$min_child_weight}")
cli::cli_alert_info("  scale_pos_weight: {params$scale_pos_weight}")
cat("\n")

cli::cli_alert_info("Running {MAX_ROUNDS} rounds with early stopping at {EARLY_STOPPING}...")
cat("\n")

set.seed(RANDOM_SEED)
cv_model <- xgb.cv(
  params = params,
  data = dtrain,
  nrounds = MAX_ROUNDS,
  folds = folds,
  early_stopping_rounds = EARLY_STOPPING,
  verbose = 1,
  print_every_n = PRINT_EVERY_N
)

# Handle different xgboost versions - best_iteration may be NULL or in different locations
best_nrounds <- cv_model$early_stop$best_iteration %||%
                cv_model$best_iteration %||%
                cv_model$best_ntreelimit %||%
                which.min(cv_model$evaluation_log$test_logloss_mean)
best_cv_logloss <- cv_model$evaluation_log$test_logloss_mean[best_nrounds]
best_cv_logloss_std <- cv_model$evaluation_log$test_logloss_std[best_nrounds]

cat("\n")
cli::cli_h3("Cross-Validation Results")
cli::cli_alert_success("Best iteration: {best_nrounds}")
cli::cli_alert_success("Best CV Log Loss: {round(best_cv_logloss, 4)} (+/- {round(best_cv_logloss_std, 4)})")
cat("\n")

# Train Final Model ----
cli::cli_h2("Training final model")

set.seed(RANDOM_SEED)
final_model <- xgb.train(
  params = params,
  data = dtrain,
  nrounds = best_nrounds,
  evals = list(train = dtrain, test = dtest),
  verbose = 1,
  print_every_n = 100
)

cli::cli_alert_success("Final model trained with {best_nrounds} rounds")

# Feature Importance ----
cli::cli_h2("Feature Importance")

importance_matrix <- xgb.importance(
  feature_names = feature_cols,
  model = final_model
)

cli::cli_h3("Top 15 Most Important Features")
print(head(importance_matrix, 15))
cat("\n")

# Evaluation on Test Set ----
cli::cli_h2("Evaluation on Test Set")

# Generate predictions (probabilities)
test_probs <- predict(final_model, dtest)
test_actuals <- test_features$batting_team_wins

# Binary predictions (threshold = 0.5)
test_predictions <- as.integer(test_probs >= 0.5)

# Log Loss
log_loss <- -mean(test_actuals * log(pmax(test_probs, 1e-15)) +
                  (1 - test_actuals) * log(pmax(1 - test_probs, 1e-15)))

# Accuracy
accuracy <- mean(test_predictions == test_actuals)

# Brier Score
brier_score <- mean((test_probs - test_actuals)^2)

# Confusion Matrix
conf_matrix <- table(
  Actual = factor(test_actuals, levels = c(0, 1), labels = c("Loss", "Win")),
  Predicted = factor(test_predictions, levels = c(0, 1), labels = c("Loss", "Win"))
)

cli::cli_h3("Overall Metrics")
cli::cli_alert_info("Log Loss: {round(log_loss, 4)}")
cli::cli_alert_info("Accuracy: {round(accuracy * 100, 2)}%")
cli::cli_alert_info("Brier Score: {round(brier_score, 4)}")
cat("\n")

cli::cli_h3("Confusion Matrix")
print(conf_matrix)
cat("\n")

# ROC-AUC (if pROC available)
if (requireNamespace("pROC", quietly = TRUE)) {
  roc_obj <- pROC::roc(test_actuals, test_probs, quiet = TRUE)
  auc_value <- pROC::auc(roc_obj)
  cli::cli_alert_info("AUC-ROC: {round(auc_value, 4)}")
} else {
  auc_value <- NA
  cli::cli_alert_warning("pROC package not available for AUC calculation")
}
cat("\n")

# Calibration Analysis ----
cli::cli_h2("Calibration Analysis")

# Bin predictions and compare to actual win rates
test_features$predicted_prob <- test_probs
test_features$actual_win <- test_actuals

calibration <- test_features %>%
  mutate(prob_bin = cut(predicted_prob,
                        breaks = seq(0, 1, 0.1),
                        include.lowest = TRUE)) %>%
  group_by(prob_bin) %>%
  summarise(
    n = n(),
    mean_predicted = mean(predicted_prob),
    mean_actual = mean(actual_win),
    .groups = "drop"
  ) %>%
  mutate(
    calibration_error = abs(mean_predicted - mean_actual)
  )

cli::cli_h3("Calibration by Probability Bin")
print(calibration)
cat("\n")

# Overall calibration error
mean_calibration_error <- mean(calibration$calibration_error, na.rm = TRUE)
cli::cli_alert_info("Mean Calibration Error: {round(mean_calibration_error * 100, 2)}%")

# Metrics by Match Situation ----
cli::cli_h2("Performance by Match Situation")

# By runs needed bins
test_features <- test_features %>%
  mutate(
    runs_needed_bin = cut(runs_needed,
                          breaks = c(-Inf, 10, 30, 60, 100, Inf),
                          labels = c("<10", "10-30", "30-60", "60-100", ">100"))
  )

situation_metrics <- test_features %>%
  group_by(runs_needed_bin) %>%
  summarise(
    n = n(),
    actual_win_rate = mean(actual_win),
    mean_predicted = mean(predicted_prob),
    accuracy = mean((predicted_prob >= 0.5) == actual_win),
    .groups = "drop"
  )

cli::cli_h3("Metrics by Runs Needed")
print(situation_metrics)
cat("\n")

# By phase
phase_situation <- test_features %>%
  mutate(phase_name = case_when(
    phase_powerplay == 1 ~ "powerplay",
    phase_death == 1 ~ "death",
    TRUE ~ "middle"
  )) %>%
  group_by(phase_name) %>%
  summarise(
    n = n(),
    actual_win_rate = mean(actual_win),
    mean_predicted = mean(predicted_prob),
    accuracy = mean((predicted_prob >= 0.5) == actual_win),
    log_loss = -mean(actual_win * log(pmax(predicted_prob, 1e-15)) +
                     (1 - actual_win) * log(pmax(1 - predicted_prob, 1e-15))),
    .groups = "drop"
  ) %>%
  arrange(match(phase_name, c("powerplay", "middle", "death")))

cli::cli_h3("Metrics by Phase")
print(phase_situation)
cat("\n")

# Baseline Comparison ----
cli::cli_h2("Baseline Comparison")

# Naive baseline: always predict the average win rate
baseline_prob <- win_rate
baseline_log_loss <- -mean(test_actuals * log(baseline_prob) +
                           (1 - test_actuals) * log(1 - baseline_prob))
baseline_brier <- mean((baseline_prob - test_actuals)^2)

cli::cli_alert_info("Baseline (always predict {round(win_rate * 100, 1)}%):")
cli::cli_alert_info("  Log Loss: {round(baseline_log_loss, 4)}")
cli::cli_alert_info("  Brier Score: {round(baseline_brier, 4)}")
cat("\n")
cli::cli_alert_info("XGBoost Model:")
cli::cli_alert_info("  Log Loss: {round(log_loss, 4)} (improvement: {round((baseline_log_loss - log_loss) / baseline_log_loss * 100, 1)}%)")
cli::cli_alert_info("  Brier Score: {round(brier_score, 4)} (improvement: {round((baseline_brier - brier_score) / baseline_brier * 100, 1)}%)")
cat("\n")

# Save Model and Results ----
cli::cli_h2("Saving model and results")

output_dir <- file.path("..", "bouncerdata", "models")

# Save XGBoost model
model_path <- file.path(output_dir, "ipl_stage2_win_probability.json")
xgb.save(final_model, model_path)
cli::cli_alert_success("Model saved to {model_path}")

# Save full results object
results <- list(
  model = final_model,
  params = params,
  best_nrounds = best_nrounds,
  cv_model = cv_model,
  importance = importance_matrix,
  feature_cols = feature_cols,
  metrics = list(
    test_log_loss = log_loss,
    test_accuracy = accuracy,
    test_brier_score = brier_score,
    test_auc = auc_value,
    best_cv_logloss = best_cv_logloss,
    baseline_log_loss = baseline_log_loss,
    calibration = calibration,
    phase_metrics = phase_situation,
    situation_metrics = situation_metrics
  ),
  config = list(
    random_seed = RANDOM_SEED,
    cv_folds = CV_FOLDS,
    max_rounds = MAX_ROUNDS,
    early_stopping = EARLY_STOPPING,
    train_win_rate = win_rate,
    created_at = Sys.time()
  )
)

results_path <- file.path(output_dir, "ipl_stage2_results.rds")
saveRDS(results, results_path)
cli::cli_alert_success("Results saved to {results_path}")

# Done ----
cat("\n")
cli::cli_alert_success("Stage 2 win probability model training complete!")
cat("\n")

cli::cli_h3("Model Performance Summary")
cat(sprintf("  Test Log Loss: %.4f\n", log_loss))
cat(sprintf("  Test Accuracy: %.2f%%\n", accuracy * 100))
cat(sprintf("  Test Brier Score: %.4f\n", brier_score))
if (!is.na(auc_value)) cat(sprintf("  Test AUC-ROC: %.4f\n", auc_value))
cat(sprintf("  Best CV Log Loss: %.4f\n", best_cv_logloss))
cat(sprintf("  Optimal rounds: %d\n", best_nrounds))
cat(sprintf("  Mean Calibration Error: %.2f%%\n", mean_calibration_error * 100))
cat(sprintf("  Improvement over baseline: %.1f%% (log loss)\n",
            (baseline_log_loss - log_loss) / baseline_log_loss * 100))
cat("\n")

cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "Run 04_evaluate_predictions.R for comprehensive evaluation",
  "i" = "Use predict_win_probability() for live predictions",
  "i" = "Model saved to: {model_path}"
))
cat("\n")
