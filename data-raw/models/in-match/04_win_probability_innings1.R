# Innings 1 Win Probability Model ----
#
# XGBoost binary classification model to predict win probability
# for the team batting FIRST.
#
# Key insight: We compare the team's projected score trajectory against
# the baseline (what an average team would score at this venue) to determine
# if they're "above par" or "below par".
#
# Target: batting_first_wins (binary: 1 = batting first team won)
# Features:
#   - Baseline projected score (team-agnostic)
#   - Current projected score (from Stage 1 model)
#   - Projected vs baseline (above/below par)
#   - Venue chase success rate
#   - Match context, momentum, etc.

# Setup ----
library(DBI)
library(dplyr)
library(xgboost)
devtools::load_all()

# Configuration ----
RANDOM_SEED <- 42
CV_FOLDS <- 5
MAX_ROUNDS <- 2000
EARLY_STOPPING <- 20
PRINT_EVERY_N <- 50

cat("\n")
cli::cli_h1("Innings 1 Win Probability Model (XGBoost)")
cat("\n")

# Load Prepared Data ----
cli::cli_h2("Loading prepared data")

output_dir <- file.path("..", "bouncerdata", "models")

# Stage 1 data (1st innings deliveries with win labels)
stage1_path <- file.path(output_dir, "ipl_stage1_data.rds")
if (!file.exists(stage1_path)) {
  cli::cli_alert_danger("Data not found at {stage1_path}")
  cli::cli_alert_info("Run 01_prepare_data.R first")
  stop("Data file not found")
}

stage1_data <- readRDS(stage1_path)
train_data <- stage1_data$train
test_data <- stage1_data$test

cli::cli_alert_success("Loaded {nrow(train_data)} training deliveries")
cli::cli_alert_success("Loaded {nrow(test_data)} test deliveries")

# Check for win labels
if (!"batting_first_wins" %in% names(train_data)) {
  cli::cli_alert_danger("batting_first_wins column not found")
  cli::cli_alert_info("Re-run 01_prepare_data.R to add win labels")
  stop("Win labels not found")
}

# Load Stage 1 projected score model
stage1_model_path <- file.path(output_dir, "ipl_stage1_results.rds")
if (!file.exists(stage1_model_path)) {
  cli::cli_alert_danger("Stage 1 model not found at {stage1_model_path}")
  cli::cli_alert_info("Run 03_projected_score_model.R first")
  stop("Stage 1 model not found")
}

stage1_results <- readRDS(stage1_model_path)
stage1_model <- stage1_results$model
stage1_feature_cols <- stage1_results$feature_cols

cli::cli_alert_success("Loaded Stage 1 projected score model")

# Load baseline model
baseline_path <- file.path(output_dir, "ipl_baseline_projected_score.rds")
if (file.exists(baseline_path)) {
  baseline_model <- readRDS(baseline_path)
  cli::cli_alert_success("Loaded baseline model")
  HAS_BASELINE <- TRUE
} else {
  cli::cli_alert_warning("Baseline model not found - using venue average as baseline")
  HAS_BASELINE <- FALSE
}

# Generate Stage 1 Predictions ----
cli::cli_h2("Generating projected score predictions")

# Prepare Stage 1 features
prepare_stage1_features_for_innings1 <- function(data, feature_cols) {
  data <- data %>%
    mutate(
      phase_powerplay = as.integer(phase == "powerplay"),
      phase_middle = as.integer(phase == "middle"),
      phase_death = as.integer(phase == "death"),
      gender_male = as.integer(gender == "male"),
      wickets_in_hand = 10 - wickets_fallen
    )

  # Handle missing columns
  for (col in feature_cols) {
    if (!col %in% names(data)) {
      data[[col]] <- 0
    }
  }

  as.data.frame(data)[, feature_cols]
}

train_s1_features <- prepare_stage1_features_for_innings1(train_data, stage1_feature_cols)
test_s1_features <- prepare_stage1_features_for_innings1(test_data, stage1_feature_cols)

train_s1_features[is.na(train_s1_features)] <- 0
test_s1_features[is.na(test_s1_features)] <- 0

# Get projected score predictions
dtrain_s1 <- xgb.DMatrix(data = as.matrix(train_s1_features))
dtest_s1 <- xgb.DMatrix(data = as.matrix(test_s1_features))

train_data$projected_final_score <- predict(stage1_model, dtrain_s1)
test_data$projected_final_score <- predict(stage1_model, dtest_s1)

cli::cli_alert_success("Generated projected score predictions")
cli::cli_alert_info("Mean projected score (train): {round(mean(train_data$projected_final_score), 1)}")

# Prepare Innings 1 Win Probability Features ----
cli::cli_h2("Preparing features")

prepare_innings1_winprob_features <- function(data, baseline_model = NULL) {

  # Calculate baseline projected score if we have the model
  if (!is.null(baseline_model)) {
    # Join venue stats
    data <- data %>%
      left_join(
        baseline_model$venue_stats %>% select(venue, venue_avg_baseline = venue_avg_score),
        by = "venue"
      ) %>%
      mutate(
        venue_avg_baseline = coalesce(venue_avg_baseline, baseline_model$overall_avg)
      )

    # For simplicity, use venue average as baseline (toss/knockout adjustments are match-level)
    data$baseline_projected_score <- data$venue_avg_baseline
  } else {
    # Use venue_avg_score as baseline
    data$baseline_projected_score <- data$venue_avg_score
  }

  features <- data %>%
    mutate(
      # One-hot encode phase
      phase_powerplay = as.integer(phase == "powerplay"),
      phase_middle = as.integer(phase == "middle"),
      phase_death = as.integer(phase == "death"),

      # Gender
      gender_male = as.integer(gender == "male"),

      # Wickets
      wickets_in_hand = 10 - wickets_fallen,

      # Key feature: projected score vs baseline (are we above/below par?)
      projected_vs_baseline = projected_final_score - baseline_projected_score,

      # Normalized version
      projected_vs_baseline_pct = (projected_final_score - baseline_projected_score) / baseline_projected_score,

      # How much of the innings is complete
      innings_progress = balls_bowled / 120,  # T20 has 120 balls

      # Current trajectory vs final projection
      current_vs_projected = total_runs - (projected_final_score * innings_progress),

      # Scoring rate metrics
      projected_run_rate = projected_final_score / 20,  # Projected RPO

      # Venue context
      venue_favors_batting = as.integer(venue_avg_score > 165),  # High-scoring venue
      venue_favors_chasing = as.integer(venue_chase_success_rate > 0.5),

      # Knockout flag (if available)
      is_ko = as.integer(is_knockout)
    )

  # Base columns to select
  base_cols <- c(
    # Target label
    "batting_first_wins",

    # Projected scores (key features)
    "projected_final_score",
    "baseline_projected_score",
    "projected_vs_baseline",
    "projected_vs_baseline_pct",

    # Current state
    "total_runs",
    "wickets_fallen",
    "wickets_in_hand",
    "current_run_rate",
    "balls_bowled",
    "innings_progress",

    # Scoring trajectory
    "current_vs_projected",
    "projected_run_rate",

    # Phase
    "phase_powerplay",
    "phase_middle",
    "phase_death",
    "overs_into_phase",

    # Rolling momentum
    "runs_last_12_balls",
    "runs_last_24_balls",
    "dots_last_12_balls",
    "dots_last_24_balls",
    "boundaries_last_12_balls",
    "boundaries_last_24_balls",
    "wickets_last_12_balls",
    "wickets_last_24_balls",

    # Rolling overs
    "runs_last_3_overs",
    "runs_last_6_overs",
    "wickets_last_3_overs",
    "wickets_last_6_overs",
    "rr_last_3_overs",
    "rr_last_6_overs",

    # Venue context
    "venue_avg_score",
    "venue_chase_success_rate",
    "venue_favors_batting",
    "venue_favors_chasing",

    # Match context
    "gender_male",
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

train_features <- prepare_innings1_winprob_features(train_data, if (HAS_BASELINE) baseline_model else NULL)
test_features <- prepare_innings1_winprob_features(test_data, if (HAS_BASELINE) baseline_model else NULL)

# Handle NA and Inf values
train_features[is.na(train_features)] <- 0
test_features[is.na(test_features)] <- 0

train_features <- train_features %>%
  mutate(across(where(is.numeric), ~ifelse(is.infinite(.), 0, .)))
test_features <- test_features %>%
  mutate(across(where(is.numeric), ~ifelse(is.infinite(.), 0, .)))

# Remove rows with NA in target
train_features <- train_features %>% filter(!is.na(batting_first_wins))
test_features <- test_features %>% filter(!is.na(batting_first_wins))

cli::cli_alert_success("Prepared {ncol(train_features) - 2} features")

# Feature list
feature_cols <- setdiff(names(train_features), c("batting_first_wins", "match_id"))

cli::cli_h3("Features Used")
cat(paste(" -", feature_cols, collapse = "\n"))
cat("\n")

# Check class balance
win_rate <- mean(train_features$batting_first_wins)
cli::cli_alert_info("Training win rate (batting first wins): {round(win_rate * 100, 1)}%")

# Create Grouped CV Folds ----
cli::cli_h2("Creating grouped CV folds")

folds <- create_grouped_folds(train_features, n_folds = CV_FOLDS, seed = RANDOM_SEED)

cli::cli_alert_success("Created {CV_FOLDS} grouped folds by match_id")
cli::cli_alert_info("Fold sizes: {paste(sapply(folds, length), collapse = ', ')} deliveries")

# Create XGBoost DMatrix ----
cli::cli_h2("Creating XGBoost matrices")

train_features <- as.data.frame(train_features)
test_features <- as.data.frame(test_features)

dtrain <- xgb.DMatrix(
  data = as.matrix(train_features[, feature_cols]),
  label = train_features$batting_first_wins
)

dtest <- xgb.DMatrix(
  data = as.matrix(test_features[, feature_cols]),
  label = test_features$batting_first_wins
)

cli::cli_alert_success("XGBoost matrices created")

# Cross-Validation ----
cli::cli_h2("Cross-validation to find optimal rounds")

params <- list(
  objective = "binary:logistic",
  max_depth = 6,
  eta = 0.05,
  subsample = 0.8,
  colsample_bytree = 0.8,
  min_child_weight = 3,
  gamma = 0,
  scale_pos_weight = 1,
  eval_metric = "logloss"
)

cli::cli_alert_info("Parameters:")
cli::cli_alert_info("  objective: {params$objective}")
cli::cli_alert_info("  max_depth: {params$max_depth}")
cli::cli_alert_info("  eta (learning rate): {params$eta}")
cli::cli_alert_info("  subsample: {params$subsample}")
cli::cli_alert_info("  colsample_bytree: {params$colsample_bytree}")
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

# Generate predictions
test_probs <- predict(final_model, dtest)
test_actuals <- test_features$batting_first_wins

# Binary predictions
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
  Actual = factor(test_actuals, levels = c(0, 1), labels = c("Lose", "Win")),
  Predicted = factor(test_predictions, levels = c(0, 1), labels = c("Lose", "Win"))
)

cli::cli_h3("Overall Metrics")
cli::cli_alert_info("Log Loss: {round(log_loss, 4)}")
cli::cli_alert_info("Accuracy: {round(accuracy * 100, 2)}%")
cli::cli_alert_info("Brier Score: {round(brier_score, 4)}")
cat("\n")

cli::cli_h3("Confusion Matrix")
print(conf_matrix)
cat("\n")

# ROC-AUC
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

mean_calibration_error <- mean(calibration$calibration_error, na.rm = TRUE)
cli::cli_alert_info("Mean Calibration Error: {round(mean_calibration_error * 100, 2)}%")

# Performance by Phase ----
cli::cli_h2("Performance by Phase")

phase_metrics <- test_features %>%
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

print(phase_metrics)
cat("\n")

# Performance by Projected vs Baseline ----
cli::cli_h2("Performance by Projected vs Baseline")

situation_metrics <- test_features %>%
  mutate(
    above_below_par = cut(projected_vs_baseline,
                          breaks = c(-Inf, -20, -10, 0, 10, 20, Inf),
                          labels = c("<-20", "-20 to -10", "-10 to 0", "0 to 10", "10 to 20", ">20"))
  ) %>%
  group_by(above_below_par) %>%
  summarise(
    n = n(),
    actual_win_rate = mean(actual_win),
    mean_predicted = mean(predicted_prob),
    .groups = "drop"
  )

cli::cli_h3("Win Rate by Projected vs Baseline (above/below par)")
print(situation_metrics)
cat("\n")

# Baseline Comparison ----
cli::cli_h2("Baseline Comparison")

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

# Save XGBoost model
model_path <- file.path(output_dir, "ipl_innings1_win_probability.json")
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
    phase_metrics = phase_metrics,
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

results_path <- file.path(output_dir, "ipl_innings1_results.rds")
saveRDS(results, results_path)
cli::cli_alert_success("Results saved to {results_path}")

# Done ----
cat("\n")
cli::cli_alert_success("Innings 1 win probability model training complete!")
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

cli::cli_h3("Key Insight")
cli::cli_bullets(c(
  "i" = "This model predicts: 'Given our 1st innings trajectory, how likely are we to win?'",
  "i" = "The key feature is projected_vs_baseline: how far above/below 'par' the team is scoring",
  "i" = "Positive projected_vs_baseline = scoring above par = higher win probability"
))
cat("\n")
