# XGBoost Multinomial Outcome Model - LONG-FORM CRICKET ----
#
# Predicts delivery outcome for Test matches
# Uses xgboost with cross-validation for hyperparameter tuning
#
# Target: 7-category multinomial outcome
#   0 = Wicket
#   1 = 0 runs (dot ball)
#   2 = 1 run
#   3 = 2 runs
#   4 = 3 runs
#   5 = 4 runs (boundary)
#   6 = 6 runs (six)

# Setup ----
library(DBI)
library(dplyr)
library(xgboost)
devtools::load_all()

# Configuration ----
RANDOM_SEED <- 42
CV_FOLDS <- 5
MAX_ROUNDS <- 2000
EARLY_STOPPING <- 10

cat("\n=== XGBoost Outcome Model: LONG-FORM CRICKET (Test) ===\n\n")

# Load Pre-Engineered Data ----
cli::cli_h2("Loading pre-engineered data")

# Reuse data from BAM script to ensure consistency
models_dir <- file.path("..", "bouncerdata", "models")
data_splits_path <- file.path(models_dir, "model_data_splits_longform.rds")

if (!file.exists(data_splits_path)) {
  cli::cli_alert_danger("Data splits not found!")
  cli::cli_alert_info("Run model_bam_outcome_longform.R first to generate feature-engineered data")
  stop("Data splits file not found. Run model_bam_outcome_longform.R first.")
}

data_splits <- readRDS(data_splits_path)
train_data <- data_splits$train
test_data <- data_splits$test

cli::cli_alert_success("Loaded {.val {nrow(train_data)}} training deliveries (Test)")
cli::cli_alert_success("Loaded {.val {nrow(test_data)}} test deliveries (Test)")

# Create Grouped Folds by Match ID ----
cli::cli_h2("Creating grouped CV folds")

# Get unique matches and shuffle them
set.seed(RANDOM_SEED)
unique_matches <- unique(train_data$match_id)
shuffled_matches <- sample(unique_matches)

# Split matches into folds (not deliveries!)
fold_assignments <- cut(
  seq_along(shuffled_matches),
  breaks = CV_FOLDS,
  labels = FALSE
)

# Create fold indices for each delivery based on match_id
folds <- list()
for (i in 1:CV_FOLDS) {
  fold_matches <- shuffled_matches[fold_assignments == i]
  folds[[i]] <- which(train_data$match_id %in% fold_matches)
}

cli::cli_alert_success("Created {.val {CV_FOLDS}} grouped folds by match_id")
cli::cli_alert_info("Fold sizes: {paste(sapply(folds, length), collapse = ', ')} deliveries")

# Prepare XGBoost Data Structures ----
cli::cli_h2("Preparing XGBoost matrices")

# XGBoost requires numeric features only
# Convert categoricals to dummy variables
prepare_xgb_features <- function(data) {
  data %>%
    mutate(
      # Convert outcome to 0-6 integer (required for xgboost multiclass)
      outcome_int = as.integer(outcome) - 1,  # 0-indexed for xgboost

      # One-hot encode phase (long-form: new_ball/middle/old_ball)
      phase_new_ball = as.integer(phase == "new_ball"),
      phase_middle = as.integer(phase == "middle"),
      phase_old_ball = as.integer(phase == "old_ball"),

      # Gender
      gender_male = as.integer(gender == "male"),

      # Innings (as numeric 1-4 for Test matches)
      innings_num = as.integer(as.character(innings))
    ) %>%
    select(
      outcome_int,
      innings_num,
      over, ball,
      wickets_fallen,
      runs_difference,
      # NO overs_left for Test matches
      phase_new_ball, phase_middle, phase_old_ball,
      gender_male
    )
}

# Apply feature engineering
train_features <- prepare_xgb_features(train_data)
test_features <- prepare_xgb_features(test_data)

# Create xgb.DMatrix objects
dtrain <- xgb.DMatrix(
  data = as.matrix(train_features %>% select(-outcome_int)),
  label = train_features$outcome_int
)

dtest <- xgb.DMatrix(
  data = as.matrix(test_features %>% select(-outcome_int)),
  label = test_features$outcome_int
)

cli::cli_alert_success("XGBoost matrices created")
cli::cli_alert_info("Features: {.val {ncol(train_features) - 1}}")

# Cross-Validation to Find Optimal Rounds ----
cli::cli_h2("Finding optimal number of rounds via CV")

# Default parameters (good starting point for most problems)
params <- list(
  objective = "multi:softprob",  # Multinomial classification
  num_class = 7,                  # 7 categories (wicket, 0-6 runs)
  max_depth = 6,                  # Tree depth
  eta = 0.1,                     # Learning rate
  subsample = 0.8,                # Row sampling
  colsample_bytree = 0.8,         # Column sampling
  eval_metric = "mlogloss"        # Multiclass log loss
)

cli::cli_alert_info("Using default parameters:")
cli::cli_alert_info("  max_depth: {.val {params$max_depth}}")
cli::cli_alert_info("  eta: {.val {params$eta}}")
cli::cli_alert_info("  subsample: {.val {params$subsample}}")
cli::cli_alert_info("  colsample_bytree: {.val {params$colsample_bytree}}")
cli::cli_alert_info("Running {.val {MAX_ROUNDS}} rounds with early stopping at {.val {EARLY_STOPPING}}")
cli::cli_alert_info("Using grouped CV folds (no match leakage across folds)")

# Cross-validation with grouped folds
set.seed(RANDOM_SEED)
cv_model <- xgb.cv(
  params = params,
  data = dtrain,
  nrounds = MAX_ROUNDS,
  folds = folds,  # Use custom grouped folds instead of nfold
  early_stopping_rounds = EARLY_STOPPING,
  verbose = 1,
  print_every_n = 100
)

# Handle different xgboost versions - best_iteration may be NULL or in different locations
best_nrounds <- cv_model$early_stop$best_iteration %||%
                cv_model$best_iteration %||%
                cv_model$best_ntreelimit %||%
                which.min(cv_model$evaluation_log$test_mlogloss_mean)
best_score <- cv_model$evaluation_log$test_mlogloss_mean[best_nrounds]

cat("\n")
cli::cli_h3("Cross-Validation Results")
cli::cli_alert_success("Best iteration: {.val {best_nrounds}}")
cli::cli_alert_success("Best CV mlogloss: {.val {round(best_score, 4)}}")
cat("\n")

# Train Final Model ----
cli::cli_h2("Training final model")

set.seed(RANDOM_SEED)
xgb_model_lf <- xgb.train(
  params = params,
  data = dtrain,
  nrounds = best_nrounds,
  watchlist = list(train = dtrain, test = dtest),
  verbose = 1,
  print_every_n = 100
)

cli::cli_alert_success("Final model trained")

# Feature Importance ----
cli::cli_h2("Feature Importance")

importance_matrix <- xgb.importance(
  feature_names = colnames(train_features)[-1],  # Exclude outcome_int
  model = xgb_model_lf
)

cli::cli_h3("Top Features")
print(importance_matrix)
cat("\n")

# Predictions and Evaluation ----
cli::cli_h2("Model Evaluation on Test Set")

# Predict on test set (returns probability matrix: nrow × 7 classes)
cli::cli_alert_info("Generating predictions...")
test_probs <- predict(xgb_model_lf, dtest)

# Get predicted class (highest probability)
test_predictions <- max.col(test_probs) - 1  # Convert back to 0-6

# Confusion matrix
outcome_labels <- c("wicket", "0", "1", "2", "3", "4", "6")
conf_matrix <- table(
  Actual = factor(test_features$outcome_int, levels = 0:6, labels = outcome_labels),
  Predicted = factor(test_predictions, levels = 0:6, labels = outcome_labels)
)

cli::cli_h3("Confusion Matrix")
print(conf_matrix)
cat("\n")

# Overall accuracy
accuracy <- sum(diag(conf_matrix)) / sum(conf_matrix)
cli::cli_alert_info("Overall accuracy: {.val {round(accuracy * 100, 2)}}%")

# Category-specific accuracy
category_accuracy <- diag(conf_matrix) / rowSums(conf_matrix)
cli::cli_h3("Category-Specific Accuracy")
category_acc_df <- data.frame(
  Outcome = names(category_accuracy),
  Accuracy = paste0(round(category_accuracy * 100, 2), "%")
)
print(category_acc_df)
cat("\n")

# Calculate multiclass log loss on test set
test_logloss <- mean(-log(pmax(test_probs[cbind(1:nrow(test_probs), test_features$outcome_int + 1)], 1e-15)))
cli::cli_alert_info("Test multiclass logloss: {.val {round(test_logloss, 4)}}")

# Save Model and Results ----
cli::cli_h2("Saving results")

# Create models directory if it doesn't exist
models_dir <- file.path("..", "bouncerdata", "models")
if (!dir.exists(models_dir)) {
  dir.create(models_dir, recursive = TRUE)
  cli::cli_alert_info("Created models directory: {.file {models_dir}}")
}

# Save final model (XGBoost native format)
model_path <- file.path(models_dir, "model_xgb_outcome_longform.json")
xgb.save(xgb_model_lf, model_path)
cli::cli_alert_success("Model saved to {.file {model_path}}")

# Save full results object
results <- list(
  model = xgb_model_lf,
  params = params,
  best_nrounds = best_nrounds,
  cv_model = cv_model,
  importance = importance_matrix,
  test_accuracy = accuracy,
  test_logloss = test_logloss,
  best_cv_score = best_score,
  confusion_matrix = conf_matrix
)

results_path <- file.path(models_dir, "model_xgb_results_longform.rds")
saveRDS(results, results_path)
cli::cli_alert_success("Results saved to {.file {results_path}}")

# Done ----
cat("\n")
cli::cli_alert_success("XGBoost LONG-FORM modeling complete!")
cat("\n")
cat("Model Performance Summary:\n")
cat(sprintf("  - Test Accuracy: %.2f%%\n", accuracy * 100))
cat(sprintf("  - Test Log Loss: %.4f\n", test_logloss))
cat(sprintf("  - Best CV Log Loss: %.4f\n", best_score))
cat(sprintf("  - Optimal Rounds: %d\n", best_nrounds))
cat("\n")
cat("Compare with BAM model:\n")
cat("  - Load model_bam_outcome_longform.rds\n")
cat("  - Compare predictions and feature importance\n")
cat("\n")
