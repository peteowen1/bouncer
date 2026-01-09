# XGBoost Multinomial Outcome Model - SHORT-FORM CRICKET ----
#
# Predicts delivery outcome for T20 and ODI matches
# Uses xgboost with cross-validation for hyperparameter tuning
#
# This script is STANDALONE - no need to run the BAM script first.
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
MATCH_LIMIT <- NULL  # NULL = all matches, or set to number for testing (e.g., 100)
RANDOM_SEED <- 42
CV_FOLDS <- 5
MAX_ROUNDS <- 2000
EARLY_STOPPING <- 10

cat("\n=== XGBoost Outcome Model: SHORT-FORM CRICKET (T20/ODI) ===\n\n")

# Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cli::cli_alert_success("Connected")

# Load and Engineer Features ----
cli::cli_h2("Loading delivery data with features")

# Build SQL query with runs_difference calculation
# FILTER: Only T20 and ODI matches
query <- "
WITH innings_totals AS (
  SELECT
    match_id,
    innings,
    batting_team,
    MAX(total_runs) AS innings_total
  FROM deliveries
  WHERE LOWER(match_type) IN ('t20', 'odi', 'it20', 'odm')
  GROUP BY match_id, innings, batting_team
),
cumulative_scores AS (
  SELECT
    d.*,
    d.total_runs AS batting_score,
    COALESCE(
      (SELECT SUM(it.innings_total)
       FROM innings_totals it
       WHERE it.match_id = d.match_id
         AND it.batting_team = d.bowling_team
         AND it.innings < d.innings),
      0
    ) AS bowling_score
  FROM deliveries d
  WHERE LOWER(d.match_type) IN ('t20', 'odi', 'it20', 'odm')
)
SELECT
  delivery_id,
  match_id,
  match_type,
  innings,
  over,
  ball,
  over_ball,
  venue,
  gender,
  runs_batter,
  is_wicket,
  wickets_fallen,
  (batting_score - bowling_score) AS runs_difference
FROM cumulative_scores
WHERE runs_batter NOT IN (5)         -- Remove 5 runs (rare)
  AND runs_batter <= 6               -- Remove outliers > 6
  {match_limit_clause}
"

# Add match limit if specified
if (!is.null(MATCH_LIMIT)) {
  cli::cli_alert_info("Using MATCH_LIMIT = {MATCH_LIMIT}")
  match_ids <- DBI::dbGetQuery(conn, sprintf("
    SELECT DISTINCT match_id FROM deliveries
    WHERE LOWER(match_type) IN ('t20', 'odi', 'it20', 'odm')
    ORDER BY match_id
    LIMIT %d
  ", MATCH_LIMIT))$match_id

  match_clause <- sprintf("AND match_id IN ('%s')",
                          paste(match_ids, collapse = "','"))
  query <- gsub("\\{match_limit_clause\\}", match_clause, query)
} else {
  query <- gsub("\\{match_limit_clause\\}", "", query)
}

# Execute query
cli::cli_alert_info("Executing feature engineering query...")
model_data <- DBI::dbGetQuery(conn, query)

cli::cli_alert_success("Loaded {.val {nrow(model_data)}} deliveries (T20/ODI)")

# Add Skill Indices ----
cli::cli_h2("Adding player skill indices")

# Join skill indices from the t20_player_skill table
# This adds: batter_scoring_index, batter_survival_rate, bowler_economy_index,
#            bowler_strike_rate, batter_balls_faced, bowler_balls_bowled
model_data <- add_skill_features(model_data, format = "t20", conn = conn, fill_missing = TRUE)

# Check how many matched
n_with_skills <- sum(!is.na(model_data$batter_scoring_index))
cli::cli_alert_info("{n_with_skills}/{nrow(model_data)} deliveries have skill indices")

# Create Additional Features ----
cli::cli_h2("Engineering derived features")

model_data <- model_data %>%
  mutate(
    # Target variable: outcome (0-6 for XGBoost)
    outcome = case_when(
      is_wicket ~ 0L,        # Category 0: Wicket
      runs_batter == 0 ~ 1L, # Category 1: 0 runs
      runs_batter == 1 ~ 2L, # Category 2: 1 run
      runs_batter == 2 ~ 3L, # Category 3: 2 runs
      runs_batter == 3 ~ 4L, # Category 4: 3 runs
      runs_batter == 4 ~ 5L, # Category 5: 4 runs
      runs_batter == 6 ~ 6L, # Category 6: 6 runs
      TRUE ~ NA_integer_
    ),

    # Also create labeled factor version for display purposes
    outcome_label = factor(
      outcome,
      levels = 0:6,
      labels = c("wicket", "0", "1", "2", "3", "4", "6"),
      ordered = TRUE
    ),

    # Overs left (format-specific)
    overs_left = case_when(
      tolower(match_type) %in% c("t20", "it20") ~ pmax(0, 20 - over_ball),
      tolower(match_type) %in% c("odi", "odm") ~ pmax(0, 50 - over_ball),
      TRUE ~ NA_real_
    ),

    # Phase (short-form specific)
    phase = case_when(
      tolower(match_type) %in% c("t20", "it20") & over < 6 ~ "powerplay",
      tolower(match_type) %in% c("t20", "it20") & over < 16 ~ "middle",
      tolower(match_type) %in% c("t20", "it20") ~ "death",
      tolower(match_type) %in% c("odi", "odm") & over < 10 ~ "powerplay",
      tolower(match_type) %in% c("odi", "odm") & over < 40 ~ "middle",
      tolower(match_type) %in% c("odi", "odm") ~ "death",
      TRUE ~ "middle"
    ),
    phase = factor(phase, levels = c("powerplay", "middle", "death")),

    # Convert categoricals to factors
    match_type = factor(tolower(match_type)),
    innings = factor(innings),
    venue = factor(venue),
    gender = factor(gender)
  )

# Remove any NA outcomes
model_data <- model_data %>% filter(!is.na(outcome))

cli::cli_alert_success("Features engineered")

# Check target distribution
cli::cli_h3("Outcome Distribution")
outcome_table <- table(model_data$outcome_label)
outcome_pct <- round(100 * outcome_table / sum(outcome_table), 2)
outcome_df <- data.frame(
  Outcome = names(outcome_table),
  Count = as.integer(outcome_table),
  Percent = paste0(outcome_pct, "%")
)
print(outcome_df)

# Train-Test Split ----
cli::cli_h2("Creating train-test split")

# Use chronological split: 80% train, 20% test
# Split by match_id to avoid data leakage
set.seed(RANDOM_SEED)

unique_matches <- unique(model_data$match_id)
n_train <- floor(0.8 * length(unique_matches))
train_matches <- unique_matches[1:n_train]
test_matches <- unique_matches[(n_train + 1):length(unique_matches)]

train_data <- model_data %>% filter(match_id %in% train_matches)
test_data <- model_data %>% filter(match_id %in% test_matches)

cli::cli_alert_info("Training: {.val {nrow(train_data)}} deliveries ({.val {length(train_matches)}} matches)")
cli::cli_alert_info("Testing: {.val {nrow(test_data)}} deliveries ({.val {length(test_matches)}} matches)")

# Create Grouped Folds by Match ID ----
cli::cli_h2("Creating grouped CV folds")

# Get unique matches and shuffle them
set.seed(RANDOM_SEED)
unique_train_matches <- unique(train_data$match_id)
shuffled_matches <- sample(unique_train_matches)

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
      # One-hot encode match_type (t20/odi)
      match_type_t20 = as.integer(match_type %in% c("t20", "it20")),
      match_type_odi = as.integer(match_type %in% c("odi", "odm")),

      # One-hot encode phase (short-form: powerplay/middle/death)
      phase_powerplay = as.integer(phase == "powerplay"),
      phase_middle = as.integer(phase == "middle"),
      phase_death = as.integer(phase == "death"),

      # Gender
      gender_male = as.integer(gender == "male"),

      # Innings (as numeric 1-2 for short-form)
      innings_num = as.integer(as.character(innings)),

      # Skill indices (already numeric, ensure no NA)
      batter_scoring_index = coalesce(batter_scoring_index, 1.25),
      batter_survival_rate = coalesce(batter_survival_rate, 0.975),
      bowler_economy_index = coalesce(bowler_economy_index, 1.25),
      bowler_strike_rate = coalesce(bowler_strike_rate, 0.025),

      # Experience indicators (log transform for stability)
      batter_experience = log1p(coalesce(batter_balls_faced, 0)),
      bowler_experience = log1p(coalesce(bowler_balls_bowled, 0))
    ) %>%
    select(
      outcome,
      match_type_t20, match_type_odi,
      innings_num,
      over, ball,
      wickets_fallen,
      runs_difference,
      overs_left,
      phase_powerplay, phase_middle, phase_death,
      gender_male,
      # Skill indices
      batter_scoring_index,
      batter_survival_rate,
      bowler_economy_index,
      bowler_strike_rate,
      batter_experience,
      bowler_experience
    )
}

# Apply feature engineering
train_features <- prepare_xgb_features(train_data)
test_features <- prepare_xgb_features(test_data)

# Create xgb.DMatrix objects
dtrain <- xgb.DMatrix(
  data = as.matrix(train_features %>% select(-outcome)),
  label = train_features$outcome
)

dtest <- xgb.DMatrix(
  data = as.matrix(test_features %>% select(-outcome)),
  label = test_features$outcome
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
  print_every_n = 10
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
xgb_model_sf <- xgb.train(
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
  feature_names = colnames(train_features)[-1],  # Exclude outcome
  model = xgb_model_sf
)

cli::cli_h3("Top 15 Most Important Features")
print(head(importance_matrix, 15))
cat("\n")

# Predictions and Evaluation ----
cli::cli_h2("Model Evaluation on Test Set")

# Predict on test set (returns probability matrix: nrow × 7 classes)
cli::cli_alert_info("Generating predictions...")
test_probs <- predict(xgb_model_sf, dtest)

# Get predicted class (highest probability)
test_predictions <- max.col(test_probs) - 1  # Convert back to 0-6

# Confusion matrix
outcome_labels <- c("wicket", "0", "1", "2", "3", "4", "6")
conf_matrix <- table(
  Actual = factor(test_features$outcome, levels = 0:6, labels = outcome_labels),
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
test_logloss <- mean(-log(pmax(test_probs[cbind(1:nrow(test_probs), test_features$outcome + 1)], 1e-15)))
cli::cli_alert_info("Test multiclass logloss: {.val {round(test_logloss, 4)}}")

# Save Model and Results ----
cli::cli_h2("Saving results")

# Create models directory if it doesn't exist
models_dir <- file.path("..", "bouncerdata", "models")
if (!dir.exists(models_dir)) {
  dir.create(models_dir, recursive = TRUE)
  cli::cli_alert_info("Created models directory: {.file {models_dir}}")
}

# Save final model in UBJ format (used by 01_prepare_data.R)
model_path_ubj <- file.path(models_dir, "xgb_outcome_shortform.ubj")
xgb.save(xgb_model_sf, model_path_ubj)
cli::cli_alert_success("Model saved to {.file {model_path_ubj}}")

# Also save in JSON format for compatibility
model_path_json <- file.path(models_dir, "model_xgb_outcome_shortform.json")
xgb.save(xgb_model_sf, model_path_json)
cli::cli_alert_success("Model also saved to {.file {model_path_json}}")

# Save full results object
results <- list(
  model = xgb_model_sf,
  params = params,
  best_nrounds = best_nrounds,
  cv_model = cv_model,
  importance = importance_matrix,
  test_accuracy = accuracy,
  test_logloss = test_logloss,
  best_cv_score = best_score,
  confusion_matrix = conf_matrix
)

results_path <- file.path(models_dir, "model_xgb_results_shortform.rds")
saveRDS(results, results_path)
cli::cli_alert_success("Results saved to {.file {results_path}}")

# Save data splits for reference (optional, for comparison with other models)
data_splits_path <- file.path(models_dir, "model_data_splits_shortform.rds")
saveRDS(
  list(train = train_data, test = test_data),
  data_splits_path
)
cli::cli_alert_success("Data splits saved to {.file {data_splits_path}}")

# Done ----
cat("\n")
cli::cli_alert_success("XGBoost SHORT-FORM modeling complete!")
cat("\n")
cat("Model Performance Summary:\n")
cat(sprintf("  - Test Accuracy: %.2f%%\n", accuracy * 100))
cat(sprintf("  - Test Log Loss: %.4f\n", test_logloss))
cat(sprintf("  - Best CV Log Loss: %.4f\n", best_score))
cat(sprintf("  - Optimal Rounds: %d\n", best_nrounds))
cat("\n")
cat("Next steps:\n")
cat("  1. Run data-raw/game-modelling/01_prepare_data.R to add expected outcomes\n")
cat("  2. The model is saved as xgb_outcome_shortform.ubj for use in game modelling\n")
cat("\n")
