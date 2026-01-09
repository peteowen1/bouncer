# Stage 1: Projected Score Model (1st Innings) ----
#
# XGBoost regression model to predict final 1st innings total
# from current game state. This model powers:
#   - Live projected score during 1st innings
#   - Feature input for Stage 2 win probability model
#
# Target: final_innings_total (continuous, typically 100-250 for T20)
# Features: current runs, wickets, overs, phase, momentum, venue

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
PRINT_EVERY_N <- 50

cat("\n")
cli::cli_h1("Stage 1: Projected Score Model (XGBoost)")
cat("\n")

# Load Prepared Data ----
cli::cli_h2("Loading prepared data")

data_path <- file.path("..", "bouncerdata", "models", "ipl_stage1_data.rds")
if (!file.exists(data_path)) {
  cli::cli_alert_danger("Data not found at {data_path}")
  cli::cli_alert_info("Run 01_prepare_ipl_data.R first to generate feature-engineered data")
  stop("Data file not found")
}

stage1_data <- readRDS(data_path)
train_data <- stage1_data$train
test_data <- stage1_data$test

cli::cli_alert_success("Loaded {nrow(train_data)} training deliveries")
cli::cli_alert_success("Loaded {nrow(test_data)} test deliveries")

# Prepare Features ----
cli::cli_h2("Preparing features")

# Define feature columns for XGBoost
# Note: XGBoost requires numeric features only
prepare_stage1_features <- function(data) {

  features <- data %>%
    mutate(
      # One-hot encode phase
      phase_powerplay = as.integer(phase == "powerplay"),
      phase_middle = as.integer(phase == "middle"),
      phase_death = as.integer(phase == "death"),

      # Gender (mostly male in IPL, but include for consistency)
      gender_male = as.integer(gender == "male")
    )

  # Base feature selection
  base_cols <- c(
    # Target
    "final_innings_total",

    # Current state
    "total_runs",           # Current score
    "wickets_fallen",       # Wickets lost
    "balls_bowled",         # Balls consumed
    "overs_remaining",      # Overs left
    "wickets_in_hand",      # 10 - wickets_fallen
    "current_run_rate",     # Current RR

    # Phase
    "phase_powerplay",
    "phase_middle",
    "phase_death",
    "overs_into_phase",

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

    # Match context
    "gender_male",

    # Keep match_id for grouping (will remove before model)
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

train_features <- prepare_stage1_features(train_data)
test_features <- prepare_stage1_features(test_data)

# Check for NA values
na_counts <- colSums(is.na(train_features))
if (any(na_counts > 0)) {
  cli::cli_alert_warning("NA values found in features:")
  print(na_counts[na_counts > 0])
  cli::cli_alert_info("Filling NA with 0 for rolling features")
  train_features[is.na(train_features)] <- 0
  test_features[is.na(test_features)] <- 0
}

cli::cli_alert_success("Prepared {ncol(train_features) - 2} features (excluding target and match_id)")

# Feature list (for XGBoost matrix)
feature_cols <- setdiff(names(train_features), c("final_innings_total", "match_id"))

cli::cli_h3("Features Used")
cat(paste(" -", feature_cols, collapse = "\n"))
cat("\n")

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
  label = train_features$final_innings_total
)

dtest <- xgb.DMatrix(
  data = as.matrix(test_features[, feature_cols]),
  label = test_features$final_innings_total
)

cli::cli_alert_success("XGBoost matrices created")

# Cross-Validation ----
cli::cli_h2("Cross-validation to find optimal rounds")

params <- list(
  objective = "reg:squarederror",
  max_depth = 6,
  eta = 0.05,
  subsample = 0.8,
  colsample_bytree = 0.8,
  min_child_weight = 3,
  gamma = 0,
  eval_metric = "rmse"
)

cli::cli_alert_info("Parameters:")
cli::cli_alert_info("  objective: {params$objective}")
cli::cli_alert_info("  max_depth: {params$max_depth}")
cli::cli_alert_info("  eta (learning rate): {params$eta}")
cli::cli_alert_info("  subsample: {params$subsample}")
cli::cli_alert_info("  colsample_bytree: {params$colsample_bytree}")
cli::cli_alert_info("  min_child_weight: {params$min_child_weight}")
cat("\n")

cli::cli_alert_info("Running {MAX_ROUNDS} rounds with early stopping at {EARLY_STOPPING}...")
cli::cli_alert_info("Using grouped CV folds (no match leakage across folds)")
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
                which.min(cv_model$evaluation_log$test_rmse_mean)
best_cv_rmse <- cv_model$evaluation_log$test_rmse_mean[best_nrounds]
best_cv_rmse_std <- cv_model$evaluation_log$test_rmse_std[best_nrounds]

cat("\n")
cli::cli_h3("Cross-Validation Results")
cli::cli_alert_success("Best iteration: {best_nrounds}")
cli::cli_alert_success("Best CV RMSE: {round(best_cv_rmse, 2)} (+/- {round(best_cv_rmse_std, 2)})")
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
test_predictions <- predict(final_model, dtest)
test_actuals <- test_features$final_innings_total

# Overall metrics
rmse <- sqrt(mean((test_predictions - test_actuals)^2))
mae <- mean(abs(test_predictions - test_actuals))
r_squared <- 1 - sum((test_actuals - test_predictions)^2) / sum((test_actuals - mean(test_actuals))^2)

cli::cli_h3("Overall Metrics")
cli::cli_alert_info("RMSE: {round(rmse, 2)} runs")
cli::cli_alert_info("MAE: {round(mae, 2)} runs")
cli::cli_alert_info("R-squared: {round(r_squared, 3)}")
cat("\n")

# Metrics by phase
test_features$prediction <- test_predictions
test_features$actual <- test_actuals
test_features$error <- test_predictions - test_actuals
test_features$phase <- test_data$phase

phase_metrics <- test_features %>%
  mutate(phase_name = case_when(
    phase_powerplay == 1 ~ "powerplay",
    phase_death == 1 ~ "death",
    TRUE ~ "middle"
  )) %>%
  group_by(phase_name) %>%
  summarise(
    n = n(),
    rmse = sqrt(mean(error^2)),
    mae = mean(abs(error)),
    mean_error = mean(error),
    .groups = "drop"
  ) %>%
  arrange(match(phase_name, c("powerplay", "middle", "death")))

cli::cli_h3("Metrics by Phase")
print(phase_metrics)
cat("\n")

# Metrics by overs remaining (binned)
test_features <- test_features %>%
  mutate(overs_remaining_bin = cut(overs_remaining,
                                    breaks = c(-Inf, 2, 5, 10, 15, Inf),
                                    labels = c("0-2", "2-5", "5-10", "10-15", "15+")))

overs_metrics <- test_features %>%
  group_by(overs_remaining_bin) %>%
  summarise(
    n = n(),
    rmse = sqrt(mean(error^2)),
    mae = mean(abs(error)),
    mean_error = mean(error),
    .groups = "drop"
  )

cli::cli_h3("Metrics by Overs Remaining")
print(overs_metrics)
cat("\n")

# Naive Baseline Comparison ----
cli::cli_h2("Baseline Comparison")

# Naive baseline: project final score = current_runs * (120 / balls_bowled)
naive_predictions <- test_features$total_runs * (120 / pmax(test_features$balls_bowled, 1))
naive_rmse <- sqrt(mean((naive_predictions - test_actuals)^2))

cli::cli_alert_info("Naive projection RMSE: {round(naive_rmse, 2)} runs")
cli::cli_alert_info("XGBoost model RMSE: {round(rmse, 2)} runs")
cli::cli_alert_success("Improvement: {round((naive_rmse - rmse) / naive_rmse * 100, 1)}%")
cat("\n")

# Save Model and Results ----
cli::cli_h2("Saving model and results")

output_dir <- file.path("..", "bouncerdata", "models")

# Save XGBoost model (native format)
model_path <- file.path(output_dir, "ipl_stage1_projected_score.json")
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
    test_rmse = rmse,
    test_mae = mae,
    test_r_squared = r_squared,
    best_cv_rmse = best_cv_rmse,
    naive_rmse = naive_rmse,
    phase_metrics = phase_metrics,
    overs_metrics = overs_metrics
  ),
  config = list(
    random_seed = RANDOM_SEED,
    cv_folds = CV_FOLDS,
    max_rounds = MAX_ROUNDS,
    early_stopping = EARLY_STOPPING,
    created_at = Sys.time()
  )
)

results_path <- file.path(output_dir, "ipl_stage1_results.rds")
saveRDS(results, results_path)
cli::cli_alert_success("Results saved to {results_path}")

# Done ----
cat("\n")
cli::cli_alert_success("Stage 1 model training complete!")
cat("\n")

cli::cli_h3("Model Performance Summary")
cat(sprintf("  Test RMSE: %.2f runs\n", rmse))
cat(sprintf("  Test MAE: %.2f runs\n", mae))
cat(sprintf("  Test R-squared: %.3f\n", r_squared))
cat(sprintf("  Best CV RMSE: %.2f runs\n", best_cv_rmse))
cat(sprintf("  Optimal rounds: %d\n", best_nrounds))
cat(sprintf("  Improvement over naive: %.1f%%\n", (naive_rmse - rmse) / naive_rmse * 100))
cat("\n")

cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "Run 03_stage2_win_probability_model.R to train the win probability model",
  "i" = "The Stage 1 model predictions will be used as features in Stage 2",
  "i" = "Model saved to: {model_path}"
))
cat("\n")
