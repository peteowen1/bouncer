# BAM Ordered Categorical Outcome Model - SHORT-FORM CRICKET ----
#
# Predicts delivery outcome for T20 and ODI matches
# Uses mgcv::bam() for efficient fitting on large datasets
#
# Target: 7-category ordered outcome
#   1 = Wicket (lowest)
#   2 = 0 runs (dot ball)
#   3 = 1 run
#   4 = 2 runs
#   5 = 3 runs
#   6 = 4 runs (boundary)
#   7 = 6 runs (six, highest)

# Setup ----
library(DBI)
library(dplyr)
library(mgcv)
devtools::load_all()

# Configuration ----
MATCH_LIMIT <- NULL  # NULL = all matches, or set to number for testing (e.g., 10)
RANDOM_SEED <- 42

cat("\n=== BAM Outcome Model: SHORT-FORM CRICKET (T20/ODI) ===\n\n")

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
  FROM cricsheet.deliveries
  WHERE LOWER(match_type) IN ('t20', 'odi')
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
  FROM cricsheet.deliveries d
  WHERE LOWER(d.match_type) IN ('t20', 'odi')
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
    SELECT DISTINCT match_id FROM cricsheet.deliveries
    WHERE LOWER(match_type) IN ('t20', 'odi')
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

cli::cli_alert_success("Loaded {.val {nrow(model_data)}} deliveries (T20/ODI only)")

# Create Additional Features ----
cli::cli_h2("Engineering derived features")

model_data <- model_data %>%
  mutate(
    # Target variable: ordered outcome (wicket < 0 runs < 1 run < ... < 6 runs)
    # BAM ocat family requires INTEGER labels starting from 1 (not 0!)
    # 1-7 for 7 categories
    outcome = case_when(
      is_wicket ~ 1L,        # Category 1: Wicket
      runs_batter == 0 ~ 2L, # Category 2: 0 runs
      runs_batter == 1 ~ 3L, # Category 3: 1 run
      runs_batter == 2 ~ 4L, # Category 4: 2 runs
      runs_batter == 3 ~ 5L, # Category 5: 3 runs
      runs_batter == 4 ~ 6L, # Category 6: 4 runs
      runs_batter == 6 ~ 7L, # Category 7: 6 runs
      TRUE ~ NA_integer_
    ),

    # Also create labeled factor version for display purposes
    outcome_label = factor(
      outcome,
      levels = 1:7,
      labels = c("wicket", "0", "1", "2", "3", "4", "6"),
      ordered = TRUE
    ),

    # Overs left (format-specific)
    overs_left = case_when(
      tolower(match_type) == "t20" ~ pmax(0, 20 - over_ball),
      tolower(match_type) == "odi" ~ pmax(0, 50 - over_ball),
      TRUE ~ NA_real_
    ),

    # Phase (short-form specific)
    phase = case_when(
      tolower(match_type) == "t20" & over < 6 ~ "powerplay",
      tolower(match_type) == "t20" & over < 16 ~ "middle",
      tolower(match_type) == "t20" ~ "death",
      tolower(match_type) == "odi" & over < 10 ~ "powerplay",
      tolower(match_type) == "odi" & over < 40 ~ "middle",
      tolower(match_type) == "odi" ~ "death",
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

# Fit BAM Ordered Categorical Model ----
cli::cli_h2("Fitting BAM Model")

# Formula with smooth terms for continuous variables
# SHORT-FORM specific: includes overs_left and interactions
formula <- outcome ~
  match_type +                          # Categorical: t20/odi
  innings +                              # Categorical: 1/2
  phase +                                # Categorical: powerplay/middle/death
  gender +                               # Categorical: male/female
  s(over, bs = "ts") +                  # Smooth: over number
  s(ball, bs = "ts", k = 4) +           # Smooth: ball in over
  s(wickets_fallen, bs = "ts") +        # Smooth: wickets down
  s(runs_difference, bs = "ts") +       # Smooth: match situation
  s(overs_left, bs = "ts") +            # Smooth: overs remaining (key for short-form!)
  ti(runs_difference, overs_left, bs = 'ts') +  # Interaction: pressure situation
  ti(wickets_fallen, overs_left, bs = 'ts') +   # Interaction: wickets vs overs
  s(venue, bs = "re")                    # Random effect: venue

cli::cli_alert_info("Formula: outcome ~ match_type + innings + phase + gender + smooth terms + interactions + random(venue)")
cli::cli_alert_info("Starting BAM fit (this may take 10-30 minutes on full dataset)...")
start_time <- Sys.time()

# Fit model (this can take a while on full dataset)
bam_model_sf <- mgcv::bam(
  formula,
  family = ocat(R = 7),  # Ordered categorical with 7 categories (labeled 1-7)
  data = train_data,
  method = "fREML",      # Fast restricted maximum likelihood
  discrete = TRUE,       # Discretize covariates for speed
  # nthreads = 4,          # Use parallel processing
  select = TRUE          # Perform variable selection
)

end_time <- Sys.time()
duration <- round(difftime(end_time, start_time, units = "mins"), 2)
cli::cli_alert_success("Model fitted in {.val {duration}} minutes")

# Model Summary ----
cli::cli_h2("Model Summary")

cat("\n")
print(summary(bam_model_sf))
cat("\n")

# Check convergence
cli::cli_alert_info("Convergence: {.val {bam_model_sf$converged}}")

# Effective degrees of freedom
edf <- sum(bam_model_sf$edf)
cli::cli_alert_info("Total effective degrees of freedom: {.val {round(edf, 1)}}")

# Deviance explained
dev_explained <- (1 - bam_model_sf$deviance / bam_model_sf$null.deviance) * 100
cli::cli_alert_info("Deviance explained: {.val {round(dev_explained, 2)}}%")

# Predictions and Evaluation ----
cli::cli_h2("Model Evaluation on Test Set")

# Predict on test set (returns probability matrix: nrow × 7 categories)
cli::cli_alert_info("Generating predictions...")
test_probs <- predict(bam_model_sf, newdata = test_data, type = "response")

# Get predicted category (highest probability)
# max.col returns 1-7, which matches our outcome labels
predicted_int <- max.col(test_probs)

# Convert to labeled factors for confusion matrix
predicted_label <- factor(
  predicted_int,
  levels = 1:7,
  labels = c("wicket", "0", "1", "2", "3", "4", "6"),
  ordered = TRUE
)

# Confusion matrix
cli::cli_h3("Confusion Matrix")
conf_matrix <- table(
  Actual = test_data$outcome_label,
  Predicted = predicted_label
)
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

# Save Model and Data ----
cli::cli_h2("Saving results")

# Create models directory if it doesn't exist
models_dir <- file.path("..", "bouncerdata", "models")
if (!dir.exists(models_dir)) {
  dir.create(models_dir, recursive = TRUE)
  cli::cli_alert_info("Created models directory: {.file {models_dir}}")
}

# Save fitted model
model_path <- file.path(models_dir, "model_bam_outcome_shortform.rds")
saveRDS(bam_model_sf, model_path)
cli::cli_alert_success("Model saved to {.file {model_path}}")

# Save feature-engineered test data for comparison with XGBoost
data_splits_path <- file.path(models_dir, "model_data_splits_shortform.rds")
saveRDS(
  list(train = train_data, test = test_data),
  data_splits_path
)
cli::cli_alert_success("Data splits saved to {.file {data_splits_path}}")

# Done ----
cat("\n")
cli::cli_alert_success("BAM SHORT-FORM modeling complete!")
cat("\n")
cat("Next steps:\n")
cat("  1. Review model summary and diagnostics above\n")
cat("  2. Run model_xgb_outcome_shortform.R to compare with XGBoost\n")
cat("  3. Run model_bam_outcome_longform.R for Test cricket\n")
cat("\n")
