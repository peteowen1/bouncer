# Train Match Prediction Model (Two-Stage) ----
#
# This script trains a TWO-STAGE prediction system:
#   Stage 1: XGBoost regression to predict MARGIN OF VICTORY
#   Stage 2: XGBoost classification using predicted margin + other features
#
# The margin-first approach provides:
#   - More signal than binary win/loss
#   - Better evaluation on small test sets (MAE/RMSE)
#   - Predicted margin as a powerful feature for win probability
#
# Features (37 total):
#   - ELO differences (result + roster)
#   - Form & H2H
#   - Skill index differences (batting/bowling)
#   - Toss features
#   - Venue characteristics
#   - predicted_margin (from Stage 1 model)
#
# Output:
#   - bouncerdata/models/{format}_margin_model.ubj (Stage 1: margin regression)
#   - bouncerdata/models/{format}_prediction_model.ubj (Stage 2: win probability)
#   - bouncerdata/models/{format}_prediction_training.rds (training results)

# 1. Setup ----
library(DBI)
library(dplyr)
if (!("bouncer" %in% loadedNamespaces())) devtools::load_all()

# Check for xgboost
if (!requireNamespace("xgboost", quietly = TRUE)) {
  stop("xgboost package required. Install with: install.packages('xgboost')")
}
library(xgboost)


# 2. Configuration ----
FORMAT <- NULL  # Options: "t20", "odi", "test", or NULL for all formats

# Format groupings
FORMAT_GROUPS <- list(
  t20 = c("T20", "IT20"),
  odi = c("ODI", "ODM"),
  test = c("Test", "MDM")
)

RANDOM_SEED <- 42

# Stage 1: Margin model hyperparameters (regression)
XGB_MARGIN_PARAMS <- list(
  objective = "reg:squarederror",
  eval_metric = "rmse",
  max_depth = 5,           # Slightly shallower for regression
  eta = 0.03,              # Slightly faster learning for regression
  subsample = 0.8,
  colsample_bytree = 0.8,
  min_child_weight = 3     # Slightly more regularization for continuous target
)

# Stage 2: Win probability model hyperparameters (classification)
XGB_PARAMS <- list(
  objective = "binary:logistic",
  eval_metric = "logloss",
  max_depth = 6,           # Deeper trees for more complex patterns
  eta = 0.02,              # Faster learning
  subsample = 0.8,        # Minimal regularization
  colsample_bytree = 0.8, # Minimal feature sampling
  min_child_weight = 1    # Allow finer splits
)

N_ROUNDS <- 500            # More rounds (with early stopping)
EARLY_STOPPING <- 30       # Increased patience
N_FOLDS <- 5               # CV folds for margin stacking

# Output directory
output_dir <- file.path("..", "bouncerdata", "models")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# Determine formats to process
if (is.null(FORMAT)) {
  formats_to_process <- names(FORMAT_GROUPS)
} else {
  formats_to_process <- FORMAT
}

cat("\n")
cli::cli_h1("Match Prediction Model Training")
cli::cli_alert_info("Formats to process: {paste(toupper(formats_to_process), collapse = ', ')}")
cat("\n")


# 3. Margin Model Functions ----

#' Train margin model and generate CV predictions for stacking
#'
#' @param X_train Feature matrix (training data)
#' @param y_margin Margin target (actual_margin)
#' @param X_test Feature matrix (test data)
#' @param xgb_margin_params XGBoost parameters for margin regression
#' @param n_rounds Max rounds
#' @param early_stopping Early stopping rounds
#' @param n_folds Number of CV folds
#' @param random_seed Random seed
#' @return List with cv_predictions (for training), test_predictions, final model, and metrics
train_margin_model <- function(X_train, y_margin, X_test, xgb_margin_params,
                                n_rounds, early_stopping, n_folds, random_seed) {
  set.seed(random_seed)

  cli::cli_h2("Stage 1: Training Margin Model")

  # Filter out rows with NA margin
  valid_idx <- !is.na(y_margin)
  if (sum(valid_idx) < nrow(X_train) * 0.9) {
    cli::cli_alert_warning("Only {sum(valid_idx)}/{nrow(X_train)} matches have valid margin")
  }

  X_train_valid <- X_train[valid_idx, , drop = FALSE]
  y_margin_valid <- y_margin[valid_idx]

  cli::cli_alert_info("Training on {nrow(X_train_valid)} matches with valid margin")
  cli::cli_alert_info("Margin stats - Mean: {round(mean(y_margin_valid), 1)}, SD: {round(sd(y_margin_valid), 1)}")

  # Create folds for CV predictions (stacking approach)
  fold_ids <- sample(rep(1:n_folds, length.out = nrow(X_train_valid)))

  # Generate out-of-fold predictions for training data
  cv_predictions <- rep(NA_real_, nrow(X_train))

  cli::cli_alert_info("Generating {n_folds}-fold CV predictions for stacking...")

  fold_rmses <- numeric(n_folds)

  for (fold in 1:n_folds) {
    train_idx <- fold_ids != fold
    val_idx <- fold_ids == fold

    dtrain_fold <- xgb.DMatrix(data = X_train_valid[train_idx, , drop = FALSE],
                                label = y_margin_valid[train_idx])
    dval_fold <- xgb.DMatrix(data = X_train_valid[val_idx, , drop = FALSE],
                              label = y_margin_valid[val_idx])

    # Train fold model (silently)
    fold_model <- xgb.train(
      params = xgb_margin_params,
      data = dtrain_fold,
      nrounds = n_rounds,
      evals = list(val = dval_fold),
      early_stopping_rounds = early_stopping,
      verbose = 0
    )

    # Generate OOF predictions
    fold_preds <- predict(fold_model, dval_fold)
    fold_rmses[fold] <- sqrt(mean((fold_preds - y_margin_valid[val_idx])^2))

    # Map back to original indices
    original_valid_idx <- which(valid_idx)
    cv_predictions[original_valid_idx[val_idx]] <- fold_preds
  }

  cv_rmse <- mean(fold_rmses)
  cli::cli_alert_success("CV RMSE: {round(cv_rmse, 1)} runs (+/- {round(sd(fold_rmses), 1)})")

  # Train final margin model on all valid data
  cli::cli_alert_info("Training final margin model on all data...")

  dtrain_full <- xgb.DMatrix(data = X_train_valid, label = y_margin_valid)

  # Run CV to find best nrounds
  cv_result <- xgb.cv(
    params = xgb_margin_params,
    data = dtrain_full,
    nrounds = n_rounds,
    nfold = n_folds,
    early_stopping_rounds = early_stopping,
    verbose = 0
  )

  # Handle different xgboost versions (v3.1+ uses early_stop$best_iteration)
  best_nrounds <- cv_result$early_stop$best_iteration %||%
                  cv_result$best_iteration %||%
                  cv_result$best_ntreelimit %||%
                  which.min(cv_result$evaluation_log$test_rmse_mean)

  cli::cli_alert_info("Best margin model iteration: {best_nrounds}")

  final_model <- xgb.train(
    params = xgb_margin_params,
    data = dtrain_full,
    nrounds = best_nrounds,
    verbose = 0
  )

  # Generate test predictions
  dtest <- xgb.DMatrix(data = X_test)
  test_predictions <- predict(final_model, dtest)

  # Fill NA cv_predictions with 0 (for matches without margin)
  cv_predictions[is.na(cv_predictions)] <- 0

  cli::cli_alert_success("Margin model trained ({best_nrounds} rounds)")

  list(
    cv_predictions = cv_predictions,
    test_predictions = test_predictions,
    model = final_model,
    best_nrounds = best_nrounds,
    cv_rmse = cv_rmse,
    cv_rmse_sd = sd(fold_rmses)
  )
}


# 4. Main Training Function ----

train_format_model <- function(format, output_dir, xgb_params, xgb_margin_params,
                                n_rounds, early_stopping, n_folds, random_seed) {

  set.seed(random_seed)

  cli::cli_h1("{toupper(format)} Model Training")

  # Load Feature Data
  cli::cli_h2("Loading feature data")

  features_path <- file.path(output_dir, paste0(format, "_prediction_features.rds"))
  if (!file.exists(features_path)) {
    cli::cli_alert_danger("Feature data not found at {features_path}")
    cli::cli_alert_info("Run 02_calculate_pre_match_features.R first")
    return(NULL)
  }

  features_data <- readRDS(features_path)
  train_data <- features_data$train
  test_data <- features_data$test

  cli::cli_alert_success("Loaded {nrow(train_data)} training matches, {nrow(test_data)} test matches")

  # Prepare Features
  cli::cli_h2("Preparing feature matrix")

  train_prepared <- prepare_prediction_features(train_data)
  test_prepared <- prepare_prediction_features(test_data)

  all_feature_cols <- get_prediction_feature_cols()

  X_train_base <- as.matrix(train_prepared[, all_feature_cols])
  y_train <- train_prepared$team1_wins
  y_margin_train <- train_data$actual_margin

  X_test_base <- as.matrix(test_prepared[, all_feature_cols])
  y_test <- test_prepared$team1_wins
  y_margin_test <- test_data$actual_margin

  cli::cli_alert_info("Base feature matrix: {ncol(X_train_base)} features")
  cli::cli_alert_info("Training samples: {nrow(X_train_base)}")
  cli::cli_alert_info("Test samples: {nrow(X_test_base)}")

  # Check for NA values in base features
  na_counts <- colSums(is.na(X_train_base))
  if (any(na_counts > 0)) {
    cli::cli_alert_warning("NA values found in features - replacing with 0")
    X_train_base[is.na(X_train_base)] <- 0
    X_test_base[is.na(X_test_base)] <- 0
  }

  # -------------------------------------------------------------------------
  # STAGE 1: Train Margin Model
  # -------------------------------------------------------------------------

  margin_result <- train_margin_model(
    X_train = X_train_base,
    y_margin = y_margin_train,
    X_test = X_test_base,
    xgb_margin_params = xgb_margin_params,
    n_rounds = n_rounds,
    early_stopping = early_stopping,
    n_folds = n_folds,
    random_seed = random_seed
  )

  # Evaluate margin model on test set
  margin_test_valid <- !is.na(y_margin_test)
  if (sum(margin_test_valid) > 0) {
    model_margin_mae <- mean(abs(margin_result$test_predictions[margin_test_valid] -
                                  y_margin_test[margin_test_valid]))
    model_margin_rmse <- sqrt(mean((margin_result$test_predictions[margin_test_valid] -
                                     y_margin_test[margin_test_valid])^2))
    model_margin_cor <- cor(margin_result$test_predictions[margin_test_valid],
                            y_margin_test[margin_test_valid], use = "complete.obs")

    # Baseline: ELO-only expected margin
    baseline_margin_mae <- mean(abs(test_data$expected_margin[margin_test_valid] -
                                     y_margin_test[margin_test_valid]))
    baseline_margin_rmse <- sqrt(mean((test_data$expected_margin[margin_test_valid] -
                                        y_margin_test[margin_test_valid])^2))

    cli::cli_h2("Margin Model Evaluation (Primary Metric)")
    cli::cli_alert_info("Model Margin MAE: {round(model_margin_mae, 1)} runs")
    cli::cli_alert_info("Model Margin RMSE: {round(model_margin_rmse, 1)} runs")
    cli::cli_alert_info("Baseline (ELO) MAE: {round(baseline_margin_mae, 1)} runs")
    cli::cli_alert_info("Baseline (ELO) RMSE: {round(baseline_margin_rmse, 1)} runs")

    margin_improvement <- (baseline_margin_mae - model_margin_mae) / baseline_margin_mae * 100
    cli::cli_alert_success("Margin MAE Improvement: {sprintf('%+.1f%%', margin_improvement)}")

    margin_model_metrics <- list(
      model_mae = model_margin_mae,
      model_rmse = model_margin_rmse,
      model_cor = model_margin_cor,
      baseline_mae = baseline_margin_mae,
      baseline_rmse = baseline_margin_rmse,
      improvement_pct = margin_improvement,
      cv_rmse = margin_result$cv_rmse,
      cv_rmse_sd = margin_result$cv_rmse_sd,
      best_nrounds = margin_result$best_nrounds
    )
  } else {
    cli::cli_alert_warning("No valid margin data in test set for evaluation")
    margin_model_metrics <- NULL
  }

  # Save margin model
  margin_model_path <- file.path(output_dir, paste0(format, "_margin_model.ubj"))
  xgb.save(margin_result$model, margin_model_path)
  cli::cli_alert_success("Saved margin model to {margin_model_path}")

  # -------------------------------------------------------------------------
  # STAGE 2: Train Win Probability Model (with predicted margin)
  # -------------------------------------------------------------------------

  cli::cli_h2("Stage 2: Training Win Probability Model")

  # Add predicted_margin to feature matrices
  X_train <- cbind(X_train_base, predicted_margin = margin_result$cv_predictions)
  X_test <- cbind(X_test_base, predicted_margin = margin_result$test_predictions)

  cli::cli_alert_info("Added predicted_margin feature (total: {ncol(X_train)} features)")

  # Create DMatrix for win probability
  dtrain <- xgb.DMatrix(data = X_train, label = y_train)
  dtest <- xgb.DMatrix(data = X_test, label = y_test)

  # Feature Diagnostics
  cli::cli_h3("Feature diagnostics")

  cli::cli_alert_info("ELO diff stats - Mean: {round(mean(X_train[,'elo_diff_result']), 1)}, SD: {round(sd(X_train[,'elo_diff_result']), 1)}")
  cli::cli_alert_info("Predicted margin - Mean: {round(mean(X_train[,'predicted_margin']), 1)}, SD: {round(sd(X_train[,'predicted_margin']), 1)}")
  cli::cli_alert_info("Bat scoring diff - Mean: {round(mean(X_train[,'bat_scoring_diff']), 4)}, SD: {round(sd(X_train[,'bat_scoring_diff']), 4)}")

  # Cross-Validation for win probability
  cli::cli_h3("Running win probability CV")

  cv_params <- c(xgb_params, list(seed = random_seed))

  cv_result <- xgb.cv(
    params = cv_params,
    data = dtrain,
    nrounds = n_rounds,
    nfold = 5,
    early_stopping_rounds = early_stopping,
    print_every_n = 20,
    verbose = 1
  )

  # Handle different xgboost versions (v3.1+ uses early_stop$best_iteration)
  best_nrounds <- cv_result$early_stop$best_iteration %||%
                  cv_result$best_iteration %||%
                  cv_result$best_ntreelimit %||%
                  which.min(cv_result$evaluation_log$test_logloss_mean)

  best_cv_score <- cv_result$evaluation_log$test_logloss_mean[best_nrounds]
  best_cv_std <- cv_result$evaluation_log$test_logloss_std[best_nrounds]

  cli::cli_alert_success("Cross-validation complete")
  cli::cli_alert_info("Best iteration: {best_nrounds}")
  cli::cli_alert_info("Best CV logloss: {round(best_cv_score, 4)} (+/- {round(best_cv_std, 4)})")

  baseline_logloss <- -log(0.5)
  if (best_cv_score > baseline_logloss) {
    cli::cli_alert_warning("CV logloss ({round(best_cv_score, 4)}) is worse than random guessing ({round(baseline_logloss, 4)})")
  }

  # Train Final Model
  cli::cli_h2("Training final model on full training data")

  model <- xgb.train(
    params = xgb_params,
    data = dtrain,
    nrounds = best_nrounds,
    evals = list(train = dtrain),
    print_every_n = 20,
    verbose = 1
  )

  cli::cli_alert_success("Model training complete")

  # Evaluate Model
  cli::cli_h2("Evaluating model")

  pred_probs <- predict(model, dtest)
  pred_class <- as.integer(pred_probs > 0.5)

  accuracy <- mean(pred_class == y_test)
  cli::cli_alert_info("Test Accuracy: {round(accuracy * 100, 1)}%")

  log_loss <- -mean(y_test * log(pmax(pred_probs, 1e-7)) +
                     (1 - y_test) * log(pmax(1 - pred_probs, 1e-7)))
  cli::cli_alert_info("Test Log Loss: {round(log_loss, 4)}")

  brier <- mean((pred_probs - y_test)^2)
  cli::cli_alert_info("Test Brier Score: {round(brier, 4)}")

  baseline_pred <- as.integer(test_prepared$elo_diff_result > 0)
  baseline_acc <- mean(baseline_pred == y_test)
  cli::cli_alert_info("Baseline Accuracy (higher ELO): {round(baseline_acc * 100, 1)}%")

  # Feature Importance
  cli::cli_h2("Feature Importance")

  importance <- xgb.importance(model = model)

  cli::cli_h3("Top Features")
  for (i in seq_len(min(10, nrow(importance)))) {
    cli::cli_alert_info("{i}. {importance$Feature[i]}: {round(importance$Gain[i], 4)}")
  }

  # Calibration
  cli::cli_h2("Calibration Analysis")

  test_prepared$pred_prob <- pred_probs
  test_prepared$actual <- y_test

  calibration <- test_prepared %>%
    mutate(prob_bin = cut(pred_prob, breaks = seq(0, 1, 0.1), include.lowest = TRUE)) %>%
    group_by(prob_bin) %>%
    summarise(
      n = n(),
      mean_pred = mean(pred_prob),
      actual_rate = mean(actual),
      .groups = "drop"
    ) %>%
    filter(n >= 5)

  cat("\nCalibration (predicted prob vs actual win rate):\n")
  for (i in seq_len(nrow(calibration))) {
    cat(sprintf("  %s: Predicted %.1f%%, Actual %.1f%% (n=%d)\n",
                calibration$prob_bin[i],
                calibration$mean_pred[i] * 100,
                calibration$actual_rate[i] * 100,
                calibration$n[i]))
  }

  # Margin-Weighted Accuracy Analysis
  cli::cli_h2("Win Probability by Margin Size")

  # Get margin data from test set (now includes model predictions)
  margin_data <- test_prepared %>%
    select(match_id, pred_prob, actual) %>%
    left_join(
      test_data %>% select(match_id, expected_margin, actual_margin),
      by = "match_id"
    ) %>%
    mutate(predicted_margin = margin_result$test_predictions)

  n_with_margin <- sum(!is.na(margin_data$actual_margin))

  if (n_with_margin >= 20) {
    margin_valid <- margin_data %>% filter(!is.na(actual_margin))

    # Margin-weighted accuracy: correct predictions weighted by margin magnitude
    correct <- (margin_valid$pred_prob > 0.5) == margin_valid$actual
    margin_weights <- abs(margin_valid$actual_margin) / max(abs(margin_valid$actual_margin))
    margin_weighted_acc <- sum(correct * margin_weights) / sum(margin_weights)

    cli::cli_alert_info("Margin-Weighted Accuracy: {round(margin_weighted_acc * 100, 1)}%")

    # Confidence calibration by margin size
    margin_valid <- margin_valid %>%
      mutate(
        abs_margin = abs(actual_margin),
        margin_bucket = case_when(
          abs_margin <= 15 ~ "Close (<15 runs)",
          abs_margin <= 40 ~ "Moderate (15-40 runs)",
          TRUE ~ "Blowout (>40 runs)"
        ),
        confidence = abs(pred_prob - 0.5),
        correct = (pred_prob > 0.5) == actual
      )

    margin_calibration <- margin_valid %>%
      group_by(margin_bucket) %>%
      summarise(
        n = n(),
        accuracy = mean(correct),
        mean_confidence = mean(confidence),
        .groups = "drop"
      )

    cat("\nAccuracy by margin size:\n")
    for (i in seq_len(nrow(margin_calibration))) {
      cat(sprintf("  %s: %.1f%% accuracy (n=%d, avg conf=%.1f%%)\n",
                  margin_calibration$margin_bucket[i],
                  margin_calibration$accuracy[i] * 100,
                  margin_calibration$n[i],
                  margin_calibration$mean_confidence[i] * 100))
    }

    winprob_margin_metrics <- list(
      n_matches = n_with_margin,
      weighted_accuracy = margin_weighted_acc,
      calibration_by_margin = margin_calibration
    )
  } else {
    cli::cli_alert_warning("Only {n_with_margin} matches with margin data - skipping margin analysis")
    winprob_margin_metrics <- NULL
  }

  # Save Model
  cli::cli_h2("Saving model")

  model_path <- file.path(output_dir, paste0(format, "_prediction_model.ubj"))
  xgb.save(model, model_path)
  cli::cli_alert_success("Saved model to {model_path}")

  results_path <- file.path(output_dir, paste0(format, "_prediction_training.rds"))
  saveRDS(list(
    # Stage 1: Margin model metrics (PRIMARY)
    margin_model_metrics = margin_model_metrics,
    # Stage 2: Win probability metrics (secondary)
    winprob_metrics = list(
      accuracy = accuracy,
      log_loss = log_loss,
      brier_score = brier,
      baseline_accuracy = baseline_acc,
      cv_logloss = best_cv_score,
      cv_logloss_std = best_cv_std,
      best_nrounds = best_nrounds
    ),
    winprob_margin_metrics = winprob_margin_metrics,
    cv_result = cv_result$evaluation_log,
    importance = importance,
    calibration = calibration,
    feature_cols = c(all_feature_cols, "predicted_margin"),  # Include new feature
    params = list(
      winprob = xgb_params,
      margin = xgb_margin_params
    ),
    config = list(
      format = format,
      n_rounds = n_rounds,
      n_folds = n_folds,
      early_stopping = early_stopping,
      n_train = nrow(train_prepared),
      n_test = nrow(test_prepared),
      random_seed = random_seed,
      created_at = Sys.time()
    )
  ), results_path)

  cli::cli_alert_success("Saved training results to {results_path}")

  # Return summary (margin model metrics are primary)
  list(
    format = format,
    # Primary: Margin model performance
    margin_mae = if (!is.null(margin_model_metrics)) margin_model_metrics$model_mae else NA,
    margin_baseline_mae = if (!is.null(margin_model_metrics)) margin_model_metrics$baseline_mae else NA,
    margin_improvement = if (!is.null(margin_model_metrics)) margin_model_metrics$improvement_pct else NA,
    # Secondary: Win probability
    accuracy = accuracy,
    baseline_acc = baseline_acc,
    accuracy_improvement = accuracy - baseline_acc,
    log_loss = log_loss,
    margin_weighted_acc = if (!is.null(winprob_margin_metrics)) winprob_margin_metrics$weighted_accuracy else NA,
    n_train = nrow(train_prepared),
    n_test = nrow(test_prepared)
  )
}


# 5. Train Models for Each Format ----

results <- list()
format_times <- list()

for (format in formats_to_process) {
  start_time <- Sys.time()

  result <- tryCatch(
    train_format_model(format, output_dir, XGB_PARAMS, XGB_MARGIN_PARAMS,
                       N_ROUNDS, EARLY_STOPPING, N_FOLDS, RANDOM_SEED),
    error = function(e) {
      cli::cli_alert_danger("Error training {toupper(format)} model: {e$message}")
      NULL
    }
  )

  if (!is.null(result)) {
    results[[format]] <- result
  }

  format_times[[format]] <- difftime(Sys.time(), start_time, units = "mins")
  cat("\n")
}


# 6. Summary ----

cat("\n")
cli::cli_h1("Training Summary")
cat("\n")

if (length(results) > 0) {
  # Primary: Margin Model Performance
  cli::cli_h2("Margin Model (Primary Metric)")
  cat(sprintf("%-8s %12s %12s %12s\n",
              "Format", "Model MAE", "Baseline MAE", "Improvement"))
  cat(paste(rep("-", 50), collapse = ""), "\n")

  for (format in names(results)) {
    r <- results[[format]]
    if (!is.na(r$margin_mae)) {
      cat(sprintf("%-8s %10.1f %12.1f %+11.1f%%\n",
                  toupper(format),
                  r$margin_mae,
                  r$margin_baseline_mae,
                  r$margin_improvement))
    } else {
      cat(sprintf("%-8s %10s %12s %12s\n", toupper(format), "N/A", "N/A", "N/A"))
    }
  }

  # Secondary: Win Probability Performance
  cat("\n")
  cli::cli_h2("Win Probability (Secondary)")
  cat(sprintf("%-8s %10s %10s %12s %10s %10s\n",
              "Format", "Accuracy", "Baseline", "Improvement", "Log Loss", "Time"))
  cat(paste(rep("-", 75), collapse = ""), "\n")

  for (format in names(results)) {
    r <- results[[format]]
    time_mins <- round(as.numeric(format_times[[format]]), 1)
    cat(sprintf("%-8s %9.1f%% %9.1f%% %+11.1f%% %10.4f %8.1f min\n",
                toupper(format),
                r$accuracy * 100,
                r$baseline_acc * 100,
                r$accuracy_improvement * 100,
                r$log_loss,
                time_mins))
  }

  cat("\n")
  cli::cli_alert_success("Two-stage model training complete for {length(results)} format(s)!")
  cli::cli_alert_info("Stage 1: Margin regression (evaluated by MAE vs ELO baseline)")
  cli::cli_alert_info("Stage 2: Win probability using predicted margin as feature")
} else {
  cli::cli_alert_danger("No models were trained successfully")
}

cat("\n")
cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "Run 04_evaluate_model.R for detailed evaluation",
  "i" = "Run 05_generate_predictions.R to predict upcoming matches",
  "i" = "Models saved to: {output_dir}"
))
cat("\n")
