# Model Comparison Script ----
#
# Compares performance of all 4 outcome models:
#   1. BAM Short-form (T20/ODI)
#   2. XGBoost Short-form (T20/ODI)
#   3. BAM Long-form (Test)
#   4. XGBoost Long-form (Test)
#
# Generates:
#   - Performance metrics comparison table
#   - Confusion matrices side-by-side
#   - Category-specific accuracy comparison
#   - Feature importance comparison (XGBoost models)
#   - Prediction distribution analysis

# Setup ----
library(dplyr)
library(tidyr)
library(ggplot2)

cat("\n=== MODEL COMPARISON: BAM vs XGBoost (Short-form & Long-form) ===\n\n")

# Helper Functions ----

# Load model and extract metrics
load_model_results <- function(model_type, format) {
  cat(sprintf("\nLoading %s %s model...\n", toupper(model_type), format))

  # Models directory
  models_dir <- file.path("..", "bouncerdata", "models")

  if (model_type == "bam") {
    # BAM models: Load .rds file and data splits
    model_file <- file.path(models_dir, sprintf("model_bam_outcome_%s.rds", format))
    data_file <- file.path(models_dir, sprintf("model_data_splits_%s.rds", format))

    if (!file.exists(model_file)) {
      stop(sprintf("BAM model file not found: %s\nRun model_bam_outcome_%s.R first",
                   model_file, format))
    }
    if (!file.exists(data_file)) {
      stop(sprintf("Data splits file not found: %s\nRun model_bam_outcome_%s.R first",
                   data_file, format))
    }

    model <- readRDS(model_file)
    data_splits <- readRDS(data_file)
    test_data <- data_splits$test

    # Generate predictions on test set
    cat("  Generating BAM predictions...\n")
    test_probs <- predict(model, newdata = test_data, type = "response")
    predicted_int <- max.col(test_probs)

    # Calculate metrics
    outcome_labels <- c("wicket", "0", "1", "2", "3", "4", "6")
    predicted_label <- factor(predicted_int, levels = 1:7, labels = outcome_labels, ordered = TRUE)
    conf_matrix <- table(Actual = test_data$outcome_label, Predicted = predicted_label)

    accuracy <- sum(diag(conf_matrix)) / sum(conf_matrix)

    # For BAM, calculate log loss manually
    actual_classes <- as.integer(test_data$outcome)
    test_logloss <- mean(-log(pmax(test_probs[cbind(1:nrow(test_probs), actual_classes)], 1e-15)))

    # Deviance explained
    dev_explained <- (1 - model$deviance / model$null.deviance) * 100

    results <- list(
      model_type = "BAM",
      format = format,
      model = model,
      test_data = test_data,
      predictions = predicted_label,
      probabilities = test_probs,
      confusion_matrix = conf_matrix,
      accuracy = accuracy,
      logloss = test_logloss,
      deviance_explained = dev_explained,
      feature_importance = NULL  # BAM doesn't have feature importance in same format
    )

  } else {
    # XGBoost models: Load results .rds
    results_file <- file.path(models_dir, sprintf("model_xgb_results_%s.rds", format))
    data_file <- file.path(models_dir, sprintf("model_data_splits_%s.rds", format))

    if (!file.exists(results_file)) {
      stop(sprintf("XGBoost results file not found: %s\nRun model_xgb_outcome_%s.R first",
                   results_file, format))
    }
    if (!file.exists(data_file)) {
      stop(sprintf("Data splits file not found: %s\nRun model_xgb_outcome_%s.R first",
                   data_file, format))
    }

    xgb_results <- readRDS(results_file)
    data_splits <- readRDS(data_file)

    # Extract predictions from saved confusion matrix
    conf_matrix <- xgb_results$confusion_matrix

    # Regenerate predictions for distribution analysis
    test_data <- data_splits$test

    # Prepare XGBoost features
    if (format == "shortform") {
      test_features <- test_data %>%
        mutate(
          outcome_int = as.integer(outcome) - 1,
          match_type_t20 = as.integer(match_type == "t20"),
          match_type_odi = as.integer(match_type == "odi"),
          phase_powerplay = as.integer(phase == "powerplay"),
          phase_middle = as.integer(phase == "middle"),
          phase_death = as.integer(phase == "death"),
          gender_male = as.integer(gender == "male"),
          innings_num = as.integer(as.character(innings))
        ) %>%
        select(outcome_int, match_type_t20, match_type_odi, innings_num, over, ball,
               wickets_fallen, runs_difference, overs_left,
               phase_powerplay, phase_middle, phase_death, gender_male)
    } else {
      test_features <- test_data %>%
        mutate(
          outcome_int = as.integer(outcome) - 1,
          phase_new_ball = as.integer(phase == "new_ball"),
          phase_middle = as.integer(phase == "middle"),
          phase_old_ball = as.integer(phase == "old_ball"),
          gender_male = as.integer(gender == "male"),
          innings_num = as.integer(as.character(innings))
        ) %>%
        select(outcome_int, innings_num, over, ball, wickets_fallen, runs_difference,
               phase_new_ball, phase_middle, phase_old_ball, gender_male)
    }

    # Generate predictions
    cat("  Generating XGBoost predictions...\n")
    dtest <- xgboost::xgb.DMatrix(data = as.matrix(test_features %>% select(-outcome_int)))
    test_probs <- predict(xgb_results$model, dtest)
    test_predictions <- max.col(test_probs) - 1

    outcome_labels <- c("wicket", "0", "1", "2", "3", "4", "6")
    predicted_label <- factor(test_predictions, levels = 0:6, labels = outcome_labels)

    results <- list(
      model_type = "XGBoost",
      format = format,
      model = xgb_results$model,
      test_data = test_data,
      predictions = predicted_label,
      probabilities = test_probs,
      confusion_matrix = conf_matrix,
      accuracy = xgb_results$test_accuracy,
      logloss = xgb_results$test_logloss,
      cv_logloss = xgb_results$best_cv_score,
      best_nrounds = xgb_results$best_nrounds,
      feature_importance = xgb_results$importance
    )
  }

  cat(sprintf("  ✓ Loaded %s %s model (Accuracy: %.2f%%, LogLoss: %.4f)\n",
              results$model_type, results$format,
              results$accuracy * 100, results$logloss))

  return(results)
}

# Print confusion matrix nicely
print_confusion_matrix <- function(conf_matrix, title) {
  cat(sprintf("\n%s\n", title))
  cat(paste(rep("=", nchar(title)), collapse = ""), "\n")
  print(conf_matrix)
  cat("\n")
}

# Print category accuracy table
print_category_accuracy <- function(conf_matrix, model_name) {
  category_acc <- diag(conf_matrix) / rowSums(conf_matrix)
  cat(sprintf("\n%s - Category-Specific Accuracy:\n", model_name))
  acc_df <- data.frame(
    Outcome = names(category_acc),
    Accuracy = sprintf("%.2f%%", category_acc * 100),
    Count = rowSums(conf_matrix)
  )
  print(acc_df, row.names = FALSE)
  cat("\n")
}

# Load All Models ----
cli::cli_h1("Loading Models")

# Initialize results list
all_results <- list()

# Try to load each model (gracefully handle missing files)
tryCatch({
  all_results$bam_short <- load_model_results("bam", "shortform")
}, error = function(e) {
  cat(sprintf("Warning: Could not load BAM shortform model: %s\n", e$message))
})

tryCatch({
  all_results$xgb_short <- load_model_results("xgb", "shortform")
}, error = function(e) {
  cat(sprintf("Warning: Could not load XGBoost shortform model: %s\n", e$message))
})

tryCatch({
  all_results$bam_long <- load_model_results("bam", "longform")
}, error = function(e) {
  cat(sprintf("Warning: Could not load BAM longform model: %s\n", e$message))
})

tryCatch({
  all_results$xgb_long <- load_model_results("xgb", "longform")
}, error = function(e) {
  cat(sprintf("Warning: Could not load XGBoost longform model: %s\n", e$message))
})

# Check if any models loaded
if (length(all_results) == 0) {
  stop("No models could be loaded. Please run the model training scripts first.")
}

cat(sprintf("\n✓ Successfully loaded %d model(s)\n", length(all_results)))

# Performance Metrics Comparison ----
cli::cli_h1("Performance Metrics Comparison")

metrics_df <- data.frame()
for (name in names(all_results)) {
  res <- all_results[[name]]
  metrics_df <- rbind(metrics_df, data.frame(
    Model = paste(res$model_type, res$format),
    Accuracy = sprintf("%.2f%%", res$accuracy * 100),
    LogLoss = sprintf("%.4f", res$logloss),
    TestDeliveries = nrow(res$test_data),
    stringsAsFactors = FALSE
  ))
}

cat("\nOverall Performance Metrics:\n")
print(metrics_df, row.names = FALSE)
cat("\n")

# Confusion Matrices ----
cli::cli_h1("Confusion Matrices")

for (name in names(all_results)) {
  res <- all_results[[name]]
  title <- sprintf("%s %s - Confusion Matrix", res$model_type, toupper(res$format))
  print_confusion_matrix(res$confusion_matrix, title)
  print_category_accuracy(res$confusion_matrix, paste(res$model_type, res$format))
}

# Category Accuracy Comparison ----
cli::cli_h1("Category-Specific Accuracy Comparison")

# Build comparison table
category_comparison <- data.frame(
  Outcome = c("wicket", "0", "1", "2", "3", "4", "6")
)

for (name in names(all_results)) {
  res <- all_results[[name]]
  category_acc <- diag(res$confusion_matrix) / rowSums(res$confusion_matrix)
  col_name <- paste(res$model_type, res$format)
  category_comparison[[col_name]] <- sprintf("%.2f%%", category_acc * 100)
}

cat("\nCategory Accuracy Across All Models:\n")
print(category_comparison, row.names = FALSE)
cat("\n")

# Short-form Model Comparison ----
if ("bam_short" %in% names(all_results) && "xgb_short" %in% names(all_results)) {
  cli::cli_h1("Short-form Model Comparison (T20/ODI)")

  bam <- all_results$bam_short
  xgb <- all_results$xgb_short

  cat("\nBAM vs XGBoost Short-form:\n")
  cat(sprintf("  BAM Accuracy:     %.2f%%\n", bam$accuracy * 100))
  cat(sprintf("  XGBoost Accuracy: %.2f%%\n", xgb$accuracy * 100))
  cat(sprintf("  Difference:       %.2f pp\n", (xgb$accuracy - bam$accuracy) * 100))
  cat("\n")
  cat(sprintf("  BAM LogLoss:      %.4f\n", bam$logloss))
  cat(sprintf("  XGBoost LogLoss:  %.4f\n", xgb$logloss))
  cat(sprintf("  Difference:       %.4f\n", xgb$logloss - bam$logloss))
  cat("\n")

  # Winner
  if (xgb$accuracy > bam$accuracy) {
    cat("  → XGBoost has higher accuracy\n")
  } else {
    cat("  → BAM has higher accuracy\n")
  }

  if (xgb$logloss < bam$logloss) {
    cat("  → XGBoost has lower log loss (better calibration)\n")
  } else {
    cat("  → BAM has lower log loss (better calibration)\n")
  }
  cat("\n")
}

# Long-form Model Comparison ----
if ("bam_long" %in% names(all_results) && "xgb_long" %in% names(all_results)) {
  cli::cli_h1("Long-form Model Comparison (Test)")

  bam <- all_results$bam_long
  xgb <- all_results$xgb_long

  cat("\nBAM vs XGBoost Long-form:\n")
  cat(sprintf("  BAM Accuracy:     %.2f%%\n", bam$accuracy * 100))
  cat(sprintf("  XGBoost Accuracy: %.2f%%\n", xgb$accuracy * 100))
  cat(sprintf("  Difference:       %.2f pp\n", (xgb$accuracy - bam$accuracy) * 100))
  cat("\n")
  cat(sprintf("  BAM LogLoss:      %.4f\n", bam$logloss))
  cat(sprintf("  XGBoost LogLoss:  %.4f\n", xgb$logloss))
  cat(sprintf("  Difference:       %.4f\n", xgb$logloss - bam$logloss))
  cat("\n")

  # Winner
  if (xgb$accuracy > bam$accuracy) {
    cat("  → XGBoost has higher accuracy\n")
  } else {
    cat("  → BAM has higher accuracy\n")
  }

  if (xgb$logloss < bam$logloss) {
    cat("  → XGBoost has lower log loss (better calibration)\n")
  } else {
    cat("  → BAM has lower log loss (better calibration)\n")
  }
  cat("\n")
}

# Feature Importance (XGBoost only) ----
cli::cli_h1("Feature Importance (XGBoost Models)")

for (name in names(all_results)) {
  res <- all_results[[name]]
  if (res$model_type == "XGBoost" && !is.null(res$feature_importance)) {
    cat(sprintf("\n%s %s - Top 10 Features:\n", res$model_type, toupper(res$format)))
    print(head(res$feature_importance, 10))
    cat("\n")
  }
}

# Prediction Distribution Analysis ----
cli::cli_h1("Prediction Distribution Analysis")

for (name in names(all_results)) {
  res <- all_results[[name]]

  cat(sprintf("\n%s %s - Predicted vs Actual Distribution:\n", res$model_type, toupper(res$format)))

  # Get actual distribution
  actual_dist <- table(res$test_data$outcome_label)
  actual_pct <- round(100 * actual_dist / sum(actual_dist), 2)

  # Get predicted distribution
  pred_dist <- table(res$predictions)
  pred_pct <- round(100 * pred_dist / sum(pred_dist), 2)

  # Compare
  comparison <- data.frame(
    Outcome = names(actual_dist),
    Actual_Count = as.integer(actual_dist),
    Actual_Pct = paste0(actual_pct, "%"),
    Predicted_Count = as.integer(pred_dist),
    Predicted_Pct = paste0(pred_pct, "%"),
    Difference = paste0(sprintf("%+.2f", pred_pct - actual_pct), "pp")
  )

  print(comparison, row.names = FALSE)
  cat("\n")
}

# Model Complexity Comparison ----
cli::cli_h1("Model Complexity")

complexity_df <- data.frame()
for (name in names(all_results)) {
  res <- all_results[[name]]

  if (res$model_type == "BAM") {
    # BAM: report effective degrees of freedom
    edf <- sum(res$model$edf)
    complexity <- sprintf("%.1f edf", edf)
  } else {
    # XGBoost: report number of trees
    complexity <- sprintf("%d trees", res$best_nrounds)
  }

  complexity_df <- rbind(complexity_df, data.frame(
    Model = paste(res$model_type, res$format),
    Complexity = complexity,
    stringsAsFactors = FALSE
  ))
}

cat("\nModel Complexity:\n")
print(complexity_df, row.names = FALSE)
cat("\n")

# Summary and Recommendations ----
cli::cli_h1("Summary and Recommendations")

cat("\nKey Findings:\n\n")

cat("1. MODEL PERFORMANCE:\n")
for (name in names(all_results)) {
  res <- all_results[[name]]
  cat(sprintf("   - %s %s: %.2f%% accuracy, %.4f logloss\n",
              res$model_type, res$format, res$accuracy * 100, res$logloss))
}
cat("\n")

cat("2. STRENGTHS:\n")
cat("   - BAM models: Smooth non-linear relationships, interpretable smooth terms\n")
cat("   - XGBoost models: Better accuracy on complex interactions, feature importance\n")
cat("\n")

cat("3. CALIBRATION:\n")
cat("   - Lower logloss = better probability calibration\n")
cat("   - Check which model has lower logloss for deployment\n")
cat("\n")

cat("4. NEXT STEPS:\n")
cat("   - Use add_predictions_to_deliveries() to add predictions to database\n")
cat("   - Compare predictions on specific scenarios (e.g., death overs, new batters)\n")
cat("   - Ensemble models for potentially better performance\n")
cat("   - Test on recent matches (temporal validation)\n")
cat("\n")

# Done ----
cli::cli_alert_success("Model comparison complete!")
cat("\n")
