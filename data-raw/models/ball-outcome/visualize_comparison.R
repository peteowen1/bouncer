# Model Comparison Visualization Script ----
#
# Creates visualizations comparing all 4 outcome models
# Run this AFTER running 05_compare_models.R
#
# Generates:
#   - Accuracy comparison bar charts
#   - Category-specific performance heatmaps
#   - Confusion matrix heatmaps
#   - Prediction probability distributions
#   - Feature importance plots (BAM & XGBoost)
#   - Model agreement analysis

# Setup ----
library(dplyr)
library(tidyr)
library(ggplot2)

cat("\n=== MODEL COMPARISON VISUALIZATIONS ===\n\n")

# Load Model Results ----
load_model_results <- function(model_type, format) {
  # Models directory
  models_dir <- file.path("..", "bouncerdata", "models")

  if (model_type == "bam") {
    model_file <- file.path(models_dir, sprintf("model_bam_outcome_%s.rds", format))
    data_file <- file.path(models_dir, sprintf("model_data_splits_%s.rds", format))

    if (!file.exists(model_file) || !file.exists(data_file)) {
      return(NULL)
    }

    model <- readRDS(model_file)
    data_splits <- readRDS(data_file)
    test_data <- data_splits$test

    test_probs <- predict(model, newdata = test_data, type = "response")
    predicted_int <- max.col(test_probs)

    outcome_labels <- c("wicket", "0", "1", "2", "3", "4", "6")
    predicted_label <- factor(predicted_int, levels = 1:7, labels = outcome_labels, ordered = TRUE)
    conf_matrix <- table(Actual = test_data$outcome_label, Predicted = predicted_label)
    accuracy <- sum(diag(conf_matrix)) / sum(conf_matrix)

    actual_classes <- as.integer(test_data$outcome)
    test_logloss <- mean(-log(pmax(test_probs[cbind(1:nrow(test_probs), actual_classes)], 1e-15)))

    # Calculate feature importance for BAM models
    feature_importance <- NULL
    if (requireNamespace("gratia", quietly = TRUE)) {
      tryCatch({
        gam_smooth <- gratia::smooth_estimates(model)
        feature_importance <- gam_smooth %>%
          group_by(.smooth, .type) %>%
          summarise(
            Importance = sd(.estimate),
            .groups = "drop"
          ) %>%
          arrange(desc(Importance)) %>%
          rename(Feature = .smooth, Type = .type)
      }, error = function(e) {
        cat("Note: Could not calculate BAM feature importance:", e$message, "\n")
      })
    }

    return(list(
      model_type = "BAM",
      format = format,
      model = model,
      test_data = test_data,
      predictions = predicted_label,
      probabilities = test_probs,
      confusion_matrix = conf_matrix,
      accuracy = accuracy,
      logloss = test_logloss,
      feature_importance = feature_importance
    ))

  } else {
    results_file <- file.path(models_dir, sprintf("model_xgb_results_%s.rds", format))
    data_file <- file.path(models_dir, sprintf("model_data_splits_%s.rds", format))

    if (!file.exists(results_file) || !file.exists(data_file)) {
      return(NULL)
    }

    xgb_results <- readRDS(results_file)
    data_splits <- readRDS(data_file)
    test_data <- data_splits$test

    # Regenerate predictions
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

    dtest <- xgboost::xgb.DMatrix(data = as.matrix(test_features %>% select(-outcome_int)))
    test_probs <- predict(xgb_results$model, dtest)
    test_predictions <- max.col(test_probs) - 1

    outcome_labels <- c("wicket", "0", "1", "2", "3", "4", "6")
    predicted_label <- factor(test_predictions, levels = 0:6, labels = outcome_labels)

    return(list(
      model_type = "XGBoost",
      format = format,
      test_data = test_data,
      predictions = predicted_label,
      probabilities = test_probs,
      confusion_matrix = xgb_results$confusion_matrix,
      accuracy = xgb_results$test_accuracy,
      logloss = xgb_results$test_logloss,
      feature_importance = xgb_results$importance
    ))
  }
}

# Load all models
cli::cli_h2("Loading models")
all_results <- list(
  bam_short = load_model_results("bam", "shortform"),
  xgb_short = load_model_results("xgb", "shortform"),
  bam_long = load_model_results("bam", "longform"),
  xgb_long = load_model_results("xgb", "longform")
)

# Remove NULL entries
all_results <- all_results[!sapply(all_results, is.null)]

if (length(all_results) == 0) {
  stop("No models found. Please run the model training scripts first.")
}

cli::cli_alert_success("Loaded {length(all_results)} model(s)")

# 1. Overall Performance Comparison ----
cli::cli_h2("Plot 1: Overall Performance Comparison")

performance_data <- data.frame()
for (name in names(all_results)) {
  res <- all_results[[name]]
  performance_data <- rbind(performance_data, data.frame(
    Model = paste(res$model_type, res$format),
    ModelType = res$model_type,
    Format = res$format,
    Accuracy = res$accuracy * 100,
    LogLoss = res$logloss
  ))
}

# Accuracy plot
p1 <- ggplot(performance_data, aes(x = Model, y = Accuracy, fill = ModelType)) +
  geom_col() +
  geom_text(aes(label = sprintf("%.2f%%", Accuracy)), vjust = -0.5, size = 3.5) +
  labs(
    title = "Model Accuracy Comparison",
    subtitle = "Higher is better",
    x = NULL,
    y = "Accuracy (%)",
    fill = "Model Type"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  scale_fill_manual(values = c("BAM" = "#4477AA", "XGBoost" = "#EE6677"))

print(p1)
cat("\n")

# LogLoss plot
p2 <- ggplot(performance_data, aes(x = Model, y = LogLoss, fill = ModelType)) +
  geom_col() +
  geom_text(aes(label = sprintf("%.4f", LogLoss)), vjust = -0.5, size = 3.5) +
  labs(
    title = "Model Log Loss Comparison",
    subtitle = "Lower is better (better calibration)",
    x = NULL,
    y = "Multiclass Log Loss",
    fill = "Model Type"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  scale_fill_manual(values = c("BAM" = "#4477AA", "XGBoost" = "#EE6677"))

print(p2)
cat("\n")

# 2. Category-Specific Accuracy Heatmap ----
cli::cli_h2("Plot 2: Category-Specific Accuracy Heatmap")

category_data <- data.frame()
for (name in names(all_results)) {
  res <- all_results[[name]]
  category_acc <- diag(res$confusion_matrix) / rowSums(res$confusion_matrix)

  category_data <- rbind(category_data, data.frame(
    Model = paste(res$model_type, res$format),
    Outcome = names(category_acc),
    Accuracy = category_acc * 100
  ))
}

p3 <- ggplot(category_data, aes(x = Outcome, y = Model, fill = Accuracy)) +
  geom_tile(color = "white") +
  geom_text(aes(label = sprintf("%.1f%%", Accuracy)), color = "white", size = 3) +
  scale_fill_gradient2(
    low = "#D73027",
    mid = "#FEE090",
    high = "#1A9850",
    midpoint = 50,
    name = "Accuracy (%)"
  ) +
  labs(
    title = "Category-Specific Accuracy by Model",
    subtitle = "Darker green = better performance",
    x = "Outcome Category",
    y = NULL
  ) +
  theme_minimal() +
  theme(
    axis.text.x = element_text(size = 10),
    panel.grid = element_blank()
  )

print(p3)
cat("\n")

# 3. Confusion Matrix Heatmaps ----
cli::cli_h2("Plot 3: Confusion Matrix Heatmaps")

for (name in names(all_results)) {
  res <- all_results[[name]]

  # Convert confusion matrix to data frame
  conf_df <- as.data.frame.table(res$confusion_matrix)
  names(conf_df) <- c("Actual", "Predicted", "Count")

  # Calculate percentages for each actual class
  conf_df <- conf_df %>%
    group_by(Actual) %>%
    mutate(Percentage = Count / sum(Count) * 100) %>%
    ungroup()

  p <- ggplot(conf_df, aes(x = Predicted, y = Actual, fill = Percentage)) +
    geom_tile(color = "white") +
    geom_text(aes(label = Count), color = "white", size = 3) +
    scale_fill_gradient2(
      low = "#FFFFFF",
      high = "#2166AC",
      name = "% of Actual"
    ) +
    labs(
      title = sprintf("%s %s - Confusion Matrix", res$model_type, toupper(res$format)),
      subtitle = "Numbers show count, color shows percentage of actual class",
      x = "Predicted Outcome",
      y = "Actual Outcome"
    ) +
    theme_minimal() +
    theme(panel.grid = element_blank())

  print(p)
  cat("\n")
}

# 4. Prediction Probability Distributions ----
cli::cli_h2("Plot 4: Average Predicted Probabilities by Outcome")

prob_data <- data.frame()
for (name in names(all_results)) {
  res <- all_results[[name]]

  # Get actual outcomes
  actual_outcome <- as.integer(res$test_data$outcome)

  # Calculate average predicted probability for each outcome category
  outcome_labels <- c("wicket", "0", "1", "2", "3", "4", "6")

  for (outcome_idx in 1:7) {
    # Get rows where this is the actual outcome
    rows_with_outcome <- which(actual_outcome == outcome_idx)

    if (length(rows_with_outcome) > 0) {
      # Average predicted probabilities for this outcome
      avg_probs <- colMeans(res$probabilities[rows_with_outcome, , drop = FALSE])

      for (pred_idx in 1:7) {
        prob_data <- rbind(prob_data, data.frame(
          Model = paste(res$model_type, res$format),
          ActualOutcome = outcome_labels[outcome_idx],
          PredictedOutcome = outcome_labels[pred_idx],
          AvgProbability = avg_probs[pred_idx]
        ))
      }
    }
  }
}

# Plot for each model
for (model_name in unique(prob_data$Model)) {
  model_data <- prob_data %>% filter(Model == model_name)

  p <- ggplot(model_data, aes(x = PredictedOutcome, y = ActualOutcome, fill = AvgProbability)) +
    geom_tile(color = "white") +
    geom_text(aes(label = sprintf("%.2f", AvgProbability)), color = "white", size = 3) +
    scale_fill_gradient2(
      low = "#FFFFFF",
      high = "#1A9850",
      name = "Avg Prob"
    ) +
    labs(
      title = sprintf("%s - Average Predicted Probabilities", model_name),
      subtitle = "For deliveries where actual outcome was Y-axis, average probability model assigned to X-axis",
      x = "Predicted Outcome",
      y = "Actual Outcome"
    ) +
    theme_minimal() +
    theme(panel.grid = element_blank())

  print(p)
  cat("\n")
}

# 5. Feature Importance (All Models) ----
cli::cli_h2("Plot 5: Feature Importance")

for (name in names(all_results)) {
  res <- all_results[[name]]

  if (!is.null(res$feature_importance)) {
    if (res$model_type == "XGBoost") {
      # XGBoost: Top 15 features by Gain
      top_features <- head(res$feature_importance, 15)

      p <- ggplot(top_features, aes(x = reorder(Feature, Gain), y = Gain)) +
        geom_col(fill = "#EE6677") +
        coord_flip() +
        labs(
          title = sprintf("XGBoost %s - Top 15 Features", toupper(res$format)),
          subtitle = "Feature importance based on gain",
          x = NULL,
          y = "Gain"
        ) +
        theme_minimal()

      print(p)
      cat("\n")

    } else if (res$model_type == "BAM") {
      # BAM: Top 15 smooth terms by importance (SD of estimates)
      top_features <- head(res$feature_importance, 15)

      p <- ggplot(top_features, aes(x = reorder(Feature, Importance), y = Importance)) +
        geom_col(fill = "#4477AA") +
        coord_flip() +
        labs(
          title = sprintf("BAM %s - Top 15 Smooth Terms", toupper(res$format)),
          subtitle = "Importance based on standard deviation of smooth estimates",
          x = NULL,
          y = "Importance (SD of estimates)"
        ) +
        theme_minimal()

      print(p)
      cat("\n")
    }
  }
}

# 6. Model Agreement Analysis ----
cli::cli_h2("Plot 6: Model Agreement Analysis")

# Short-form agreement
if ("bam_short" %in% names(all_results) && "xgb_short" %in% names(all_results)) {
  bam <- all_results$bam_short
  xgb <- all_results$xgb_short

  agreement_data <- data.frame(
    Actual = bam$test_data$outcome_label,
    BAM_Pred = bam$predictions,
    XGB_Pred = xgb$predictions,
    Agreement = as.character(bam$predictions) == as.character(xgb$predictions)
  )

  agreement_summary <- agreement_data %>%
    group_by(Actual) %>%
    summarize(
      Total = n(),
      Agree = sum(Agreement),
      AgreePercent = Agree / Total * 100,
      .groups = "drop"
    )

  p <- ggplot(agreement_summary, aes(x = Actual, y = AgreePercent)) +
    geom_col(fill = "#66C2A5") +
    geom_text(aes(label = sprintf("%.1f%%", AgreePercent)), vjust = -0.5) +
    labs(
      title = "Short-form Models: Prediction Agreement by Outcome",
      subtitle = "Percentage of deliveries where BAM and XGBoost made same prediction",
      x = "Actual Outcome",
      y = "Agreement (%)"
    ) +
    theme_minimal() +
    ylim(0, 100)

  print(p)
  cat("\n")
}

# Long-form agreement
if ("bam_long" %in% names(all_results) && "xgb_long" %in% names(all_results)) {
  bam <- all_results$bam_long
  xgb <- all_results$xgb_long

  agreement_data <- data.frame(
    Actual = bam$test_data$outcome_label,
    BAM_Pred = bam$predictions,
    XGB_Pred = xgb$predictions,
    Agreement = as.character(bam$predictions) == as.character(xgb$predictions)
  )

  agreement_summary <- agreement_data %>%
    group_by(Actual) %>%
    summarize(
      Total = n(),
      Agree = sum(Agreement),
      AgreePercent = Agree / Total * 100,
      .groups = "drop"
    )

  p <- ggplot(agreement_summary, aes(x = Actual, y = AgreePercent)) +
    geom_col(fill = "#66C2A5") +
    geom_text(aes(label = sprintf("%.1f%%", AgreePercent)), vjust = -0.5) +
    labs(
      title = "Long-form Models: Prediction Agreement by Outcome",
      subtitle = "Percentage of deliveries where BAM and XGBoost made same prediction",
      x = "Actual Outcome",
      y = "Agreement (%)"
    ) +
    theme_minimal() +
    ylim(0, 100)

  print(p)
  cat("\n")
}

# Summary ----
cli::cli_alert_success("Visualization complete!")
cat("\nGenerated visualizations:\n")
cat("  1. Overall performance comparison (accuracy & log loss)\n")
cat("  2. Category-specific accuracy heatmap\n")
cat("  3. Confusion matrix heatmaps for each model\n")
cat("  4. Average predicted probabilities by outcome\n")
cat("  5. Feature importance (BAM smooth terms & XGBoost features)\n")
cat("  6. Model agreement analysis\n")
cat("\n")
cat("Note: BAM feature importance requires the 'gratia' package.\n")
cat("      Install with: install.packages('gratia')\n")
cat("\n")
