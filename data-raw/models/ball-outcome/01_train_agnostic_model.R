# Train Agnostic Outcome Model ----
#
# Trains a context-only outcome prediction model for calculating skill index baselines.
# This model uses ONLY match context features - NO player, team, or venue identity.
#
# Purpose:
#   - Provide baseline expectations for residual-based skill index calculations
#   - residual = actual - agnostic_expected
#   - Skill indices update based on this residual
#
# Features used (context only):
#   - over, ball (match progress)
#   - wickets_fallen (match situation)
#   - runs_difference (score pressure)
#   - overs_left (time pressure, shortform only)
#   - phase (powerplay/middle/death or new_ball/middle/old_ball)
#   - innings (1st or 2nd)
#   - format (t20/odi/test)
#   - gender (male/female)
#   - is_knockout (knockout match flag)
#   - event_tier (competition importance)
#   - league_avg_runs (NEW: historical average runs/ball for this league)
#   - league_avg_wicket (NEW: historical wicket rate for this league)
#
# The league features are continuous values representing historical averages,
# allowing the model to generalize to new leagues rather than one-hot encoding.
#
# EXCLUDES: player identity, team identity, venue identity
#
# Target: 7-category multinomial outcome
#   0 = Wicket, 1 = 0 runs, 2 = 1 run, 3 = 2 runs, 4 = 3 runs, 5 = 4 runs, 6 = 6 runs
#
# Usage:
#   source("data-raw/models/ball-outcome/01_train_agnostic_model.R")

# Setup ----
library(DBI)
library(dplyr)
library(xgboost)
# Note: When called from run_full_pipeline.R, package is already loaded
if (!("bouncer" %in% loadedNamespaces())) {
  devtools::load_all()
}

# Configuration ----
FORMATS_TO_TRAIN <- c("t20", "odi", "test")  # Which formats to train
MATCH_LIMIT <- NULL  # NULL = all matches, or set number for testing
RANDOM_SEED <- 42
CV_FOLDS <- 5
MAX_ROUNDS <- 2000
EARLY_STOPPING <- 20

cat("\n")
cli::cli_h1("Agnostic Outcome Model Training")
cli::cli_alert_info("Training context-only models for skill index baseline calculation")
cli::cli_alert_info("Formats: {paste(FORMATS_TO_TRAIN, collapse = ', ')}")
cat("\n")

# Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = TRUE)
# Note: Don't use on.exit() here - it causes issues when sourced with local=TRUE
# Connection will be closed explicitly at the end of the script
cli::cli_alert_success("Connected")

# Create output directory (use package helper to find the correct bouncerdata path)
models_dir <- file.path(find_bouncerdata_dir(), "models")
if (!dir.exists(models_dir)) {
  dir.create(models_dir, recursive = TRUE)
  cli::cli_alert_info("Created models directory: {.file {models_dir}}")
}

# Store results for each format
all_results <- list()

# Train Model for Each Format ----
for (format in FORMATS_TO_TRAIN) {

  cat("\n")
  cli::cli_rule("{toupper(format)} Format")
  cat("\n")

  # Determine format filters - different prefixes for different CTEs
  if (format == "t20") {
    format_filter_d <- "LOWER(d.match_type) IN ('t20', 'it20')"
    format_filter_m <- "LOWER(m.match_type) IN ('t20', 'it20')"
    format_filter_bare <- "LOWER(match_type) IN ('t20', 'it20')"
  } else if (format == "odi") {
    format_filter_d <- "LOWER(d.match_type) IN ('odi', 'odm')"
    format_filter_m <- "LOWER(m.match_type) IN ('odi', 'odm')"
    format_filter_bare <- "LOWER(match_type) IN ('odi', 'odm')"
  } else {
    format_filter_d <- "LOWER(d.match_type) IN ('test', 'mdm')"
    format_filter_m <- "LOWER(m.match_type) IN ('test', 'mdm')"
    format_filter_bare <- "LOWER(match_type) IN ('test', 'mdm')"
  }

  # Build SQL query with context features including league running averages
  # The league averages are computed from historical data BEFORE each match
  # to prevent data leakage. We use a window function approach.
  query <- sprintf("
    WITH innings_totals AS (
      SELECT
        match_id,
        innings,
        batting_team,
        MAX(total_runs) AS innings_total
      FROM cricsheet.deliveries
      WHERE %s
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
      WHERE %s
    ),
    match_context AS (
      SELECT DISTINCT
        m.match_id,
        m.event_name,
        CASE
          WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%final%%' THEN 1
          WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%qualifier%%' THEN 1
          WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%eliminator%%' THEN 1
          WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%playoff%%' THEN 1
          WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%semi%%' THEN 1
          ELSE 0
        END AS is_knockout,
        CASE
          WHEN LOWER(m.event_name) LIKE '%%world cup%%' THEN 1
          WHEN LOWER(m.event_name) LIKE '%%ipl%%' OR LOWER(m.event_name) LIKE '%%indian premier%%' THEN 1
          WHEN LOWER(m.event_name) LIKE '%%big bash%%' OR LOWER(m.event_name) LIKE '%%bbl%%' THEN 2
          WHEN LOWER(m.event_name) LIKE '%%psl%%' OR LOWER(m.event_name) LIKE '%%super league%%' THEN 2
          WHEN LOWER(m.event_name) LIKE '%%cpl%%' OR LOWER(m.event_name) LIKE '%%caribbean%%' THEN 2
          WHEN LOWER(m.match_type) IN ('test', 'odi', 't20i', 'it20') THEN 1
          ELSE 3
        END AS event_tier
      FROM cricsheet.matches m
    ),
    -- League running averages: compute avg runs/wicket rate for each league
    -- as of each match date (to prevent data leakage, we use LAG approach)
    league_stats AS (
      SELECT
        m.event_name,
        m.match_id,
        m.match_date,
        AVG(d.runs_batter + d.runs_extras) AS match_avg_runs,
        AVG(CAST(d.is_wicket AS DOUBLE)) AS match_wicket_rate
      FROM cricsheet.matches m
      JOIN cricsheet.deliveries d ON m.match_id = d.match_id
      WHERE %s
        AND m.event_name IS NOT NULL
      GROUP BY m.event_name, m.match_id, m.match_date
    ),
    league_running_avg AS (
      SELECT
        event_name,
        match_id,
        -- Running average EXCLUDING current match (lag to prevent data leakage)
        AVG(match_avg_runs) OVER (
          PARTITION BY event_name
          ORDER BY match_date, match_id
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS league_avg_runs,
        AVG(match_wicket_rate) OVER (
          PARTITION BY event_name
          ORDER BY match_date, match_id
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS league_avg_wicket
      FROM league_stats
    )
    SELECT
      cs.delivery_id,
      cs.match_id,
      cs.match_type,
      cs.innings,
      cs.over,
      cs.ball,
      cs.over_ball,
      cs.gender,
      cs.runs_batter,
      cs.is_wicket,
      -- FIX: wickets_fallen in Cricsheet is AFTER the delivery, so subtract is_wicket
      -- to get the count BEFORE this delivery (prevents data leakage)
      (cs.wickets_fallen - CAST(cs.is_wicket AS INT)) AS wickets_fallen,
      (cs.batting_score - cs.bowling_score) AS runs_difference,
      COALESCE(mc.is_knockout, 0) AS is_knockout,
      COALESCE(mc.event_tier, 3) AS event_tier,
      -- League features: use running average or format default if first match in league
      COALESCE(lra.league_avg_runs, %f) AS league_avg_runs,
      COALESCE(lra.league_avg_wicket, %f) AS league_avg_wicket
    FROM cumulative_scores cs
    LEFT JOIN match_context mc ON cs.match_id = mc.match_id
    LEFT JOIN league_running_avg lra ON cs.match_id = lra.match_id
    WHERE cs.runs_batter NOT IN (5)
      AND cs.runs_batter <= 6
    %s
  ", format_filter_bare,  # innings_totals: bare deliveries table
     format_filter_d,      # cumulative_scores: d. prefix
     format_filter_m,      # league_stats: m. prefix for the join
     # Default league averages by format (used when league has no history)
     switch(format,
       "t20" = EXPECTED_RUNS_T20,
       "odi" = EXPECTED_RUNS_ODI,
       "test" = EXPECTED_RUNS_TEST,
       EXPECTED_RUNS_T20),
     switch(format,
       "t20" = EXPECTED_WICKET_T20,
       "odi" = EXPECTED_WICKET_ODI,
       "test" = EXPECTED_WICKET_TEST,
       EXPECTED_WICKET_T20),
     if (!is.null(MATCH_LIMIT)) sprintf("LIMIT %d", MATCH_LIMIT * 1000) else "")

  # Execute query
  cli::cli_h3("Loading data")
  cli::cli_alert_info("Executing query...")
  model_data <- DBI::dbGetQuery(conn, query)

  if (nrow(model_data) == 0) {
    cli::cli_alert_warning("No data found for {format} format, skipping")
    next
  }

  cli::cli_alert_success("Loaded {.val {nrow(model_data)}} deliveries")

  # Feature Engineering ----
  cli::cli_h3("Engineering features")

  if (format %in% c("t20", "odi")) {
    # Short-form features
    model_data <- model_data %>%
      mutate(
        # Target variable
        outcome = case_when(
          is_wicket ~ 0L,
          runs_batter == 0 ~ 1L,
          runs_batter == 1 ~ 2L,
          runs_batter == 2 ~ 3L,
          runs_batter == 3 ~ 4L,
          runs_batter == 4 ~ 5L,
          runs_batter == 6 ~ 6L,
          TRUE ~ NA_integer_
        ),

        # Overs left
        overs_left = case_when(
          format == "t20" ~ pmax(0, 20 - over_ball),
          format == "odi" ~ pmax(0, 50 - over_ball),
          TRUE ~ NA_real_
        ),

        # Phase
        phase = case_when(
          format == "t20" & over < 6 ~ "powerplay",
          format == "t20" & over < 16 ~ "middle",
          format == "t20" ~ "death",
          format == "odi" & over < 10 ~ "powerplay",
          format == "odi" & over < 40 ~ "middle",
          format == "odi" ~ "death",
          TRUE ~ "middle"
        )
      )

  } else {
    # Long-form (Test) features
    model_data <- model_data %>%
      mutate(
        outcome = case_when(
          is_wicket ~ 0L,
          runs_batter == 0 ~ 1L,
          runs_batter == 1 ~ 2L,
          runs_batter == 2 ~ 3L,
          runs_batter == 3 ~ 4L,
          runs_batter == 4 ~ 5L,
          runs_batter == 6 ~ 6L,
          TRUE ~ NA_integer_
        ),

        # Phase based on ball age
        phase = case_when(
          over < 20 ~ "new_ball",
          over < 80 ~ "middle",
          TRUE ~ "old_ball"
        )
      )
  }

  # Remove NA outcomes
  model_data <- model_data %>% filter(!is.na(outcome))

  cli::cli_alert_success("Features engineered")

  # Check distribution
  outcome_table <- table(model_data$outcome)
  outcome_pct <- round(100 * outcome_table / sum(outcome_table), 1)
  cli::cli_alert_info("Outcome distribution: {paste(paste0(c('W','0','1','2','3','4','6'), ':', outcome_pct, '%'), collapse = ', ')}")

  # Train-Test Split ----
  cli::cli_h3("Creating train-test split")

  set.seed(RANDOM_SEED)
  unique_matches <- unique(model_data$match_id)
  n_train <- floor(0.8 * length(unique_matches))
  train_matches <- unique_matches[1:n_train]
  test_matches <- unique_matches[(n_train + 1):length(unique_matches)]

  train_data <- model_data %>% filter(match_id %in% train_matches)
  test_data <- model_data %>% filter(match_id %in% test_matches)

  cli::cli_alert_info("Train: {.val {nrow(train_data)}} deliveries ({.val {length(train_matches)}} matches)")
  cli::cli_alert_info("Test: {.val {nrow(test_data)}} deliveries ({.val {length(test_matches)}} matches)")

  # Create Grouped CV Folds ----
  set.seed(RANDOM_SEED)
  unique_train_matches <- unique(train_data$match_id)
  shuffled_matches <- sample(unique_train_matches)
  fold_assignments <- cut(seq_along(shuffled_matches), breaks = CV_FOLDS, labels = FALSE)

  folds <- list()
  for (i in 1:CV_FOLDS) {
    fold_matches <- shuffled_matches[fold_assignments == i]
    folds[[i]] <- which(train_data$match_id %in% fold_matches)
  }

  # Prepare XGBoost Features ----
  cli::cli_h3("Preparing XGBoost matrices")

  prepare_agnostic_xgb_features <- function(data, fmt) {
    if (fmt %in% c("t20", "odi")) {
      # Short-form features (including league running averages)
      data %>%
        mutate(
          format_t20 = as.integer(fmt == "t20"),
          format_odi = as.integer(fmt == "odi"),
          phase_powerplay = as.integer(phase == "powerplay"),
          phase_middle = as.integer(phase == "middle"),
          phase_death = as.integer(phase == "death"),
          gender_male = as.integer(tolower(gender) == "male"),
          innings_num = as.integer(innings)
        ) %>%
        select(
          outcome,
          format_t20, format_odi,
          innings_num, over, ball,
          wickets_fallen, runs_difference, overs_left,
          phase_powerplay, phase_middle, phase_death,
          gender_male,
          is_knockout, event_tier,
          # NEW: League features as continuous values (enables generalization to new leagues)
          league_avg_runs, league_avg_wicket
        )
    } else {
      # Long-form features (including league running averages)
      data %>%
        mutate(
          phase_new_ball = as.integer(phase == "new_ball"),
          phase_middle = as.integer(phase == "middle"),
          phase_old_ball = as.integer(phase == "old_ball"),
          gender_male = as.integer(tolower(gender) == "male"),
          innings_num = as.integer(innings)
        ) %>%
        select(
          outcome,
          innings_num, over, ball,
          wickets_fallen, runs_difference,
          phase_new_ball, phase_middle, phase_old_ball,
          gender_male,
          is_knockout, event_tier,
          # NEW: League features as continuous values
          league_avg_runs, league_avg_wicket
        )
    }
  }

  train_features <- prepare_agnostic_xgb_features(train_data, format)
  test_features <- prepare_agnostic_xgb_features(test_data, format)

  # Create DMatrix objects
  dtrain <- xgb.DMatrix(
    data = as.matrix(train_features %>% select(-outcome)),
    label = train_features$outcome
  )

  dtest <- xgb.DMatrix(
    data = as.matrix(test_features %>% select(-outcome)),
    label = test_features$outcome
  )

  feature_names <- colnames(train_features)[-1]
  cli::cli_alert_success("XGBoost matrices created ({.val {length(feature_names)}} features)")

  # Cross-Validation ----
  cli::cli_h3("Finding optimal rounds via CV")

  params <- list(
    objective = "multi:softprob",
    num_class = 7,
    max_depth = 6,
    eta = 0.15,
    subsample = 0.8,
    colsample_bytree = 0.8,
    eval_metric = "mlogloss"
  )

  set.seed(RANDOM_SEED)
  cv_model <- xgb.cv(
    params = params,
    data = dtrain,
    nrounds = MAX_ROUNDS,
    nfold = CV_FOLDS,
    early_stopping_rounds = EARLY_STOPPING,
    verbose = 1,
    print_every_n = 20
  )

  # Handle different xgboost versions for best iteration (v3.1+ uses early_stop$)
  best_nrounds <- cv_model$early_stop$best_iteration %||%
                  cv_model$best_iteration %||%
                  cv_model$best_iter %||%
                  cv_model$niter
  if (is.null(best_nrounds) || is.na(best_nrounds) || best_nrounds < 1) {
    # Fallback: find best score manually
    eval_log <- cv_model$evaluation_log
    if ("test_mlogloss_mean" %in% names(eval_log)) {
      best_nrounds <- which.min(eval_log$test_mlogloss_mean)
    } else {
      best_nrounds <- 100  # Safe default
    }
  }

  eval_log <- cv_model$evaluation_log
  best_score <- if ("test_mlogloss_mean" %in% names(eval_log)) {
    eval_log$test_mlogloss_mean[best_nrounds]
  } else {
    NA
  }

  cli::cli_alert_success("Best iteration: {.val {best_nrounds}}, CV mlogloss: {.val {round(best_score, 4)}}")

  # Train Final Model ----
  cli::cli_h3("Training final model")

  set.seed(RANDOM_SEED)
  xgb_model <- xgb.train(
    params = params,
    data = dtrain,
    nrounds = best_nrounds,
    evals = list(train = dtrain, test = dtest),
    verbose = 0
  )

  # Evaluate ----
  cli::cli_h3("Evaluation")

  test_probs <- predict(xgb_model, dtest)
  test_predictions <- max.col(test_probs) - 1

  accuracy <- mean(test_predictions == test_features$outcome)
  test_logloss <- mean(-log(pmax(test_probs[cbind(1:nrow(test_probs), test_features$outcome + 1)], 1e-15)))

  cli::cli_alert_success("Test accuracy: {.val {round(accuracy * 100, 2)}}%")
  cli::cli_alert_success("Test mlogloss: {.val {round(test_logloss, 4)}}")

  # Feature Importance
  importance_matrix <- xgb.importance(feature_names = feature_names, model = xgb_model)
  cli::cli_alert_info("Top features: {paste(head(importance_matrix$Feature, 5), collapse = ', ')}")

  # Save Model ----
  cli::cli_h3("Saving model")

  model_path <- file.path(models_dir, sprintf("agnostic_outcome_%s.ubj", format))
  xgb.save(xgb_model, model_path)
  cli::cli_alert_success("Model saved to {.file {model_path}}")

  # Store results
  all_results[[format]] <- list(
    model = xgb_model,
    params = params,
    best_nrounds = best_nrounds,
    best_cv_score = best_score,
    test_accuracy = accuracy,
    test_logloss = test_logloss,
    importance = importance_matrix,
    n_train = nrow(train_data),
    n_test = nrow(test_data)
  )
}

# Save Combined Results ----
cat("\n")
cli::cli_rule("Summary")
cat("\n")

results_path <- file.path(models_dir, "agnostic_model_results.rds")
saveRDS(all_results, results_path)
cli::cli_alert_success("All results saved to {.file {results_path}}")

# Print summary
cli::cli_h3("Model Performance Summary")
for (format in names(all_results)) {
  res <- all_results[[format]]
  cli::cli_alert_info("{toupper(format)}: Accuracy={round(res$test_accuracy*100,1)}%, LogLoss={round(res$test_logloss,4)}, Rounds={res$best_nrounds}")
}

cat("\n")
cli::cli_alert_success("Agnostic model training complete!")
cli::cli_alert_info("Models saved to: {.file {models_dir}}")
cli::cli_alert_info("Use load_agnostic_model() to load for skill index calculations")
cat("\n")

# Cleanup database connection
if (exists("conn") && !is.null(conn)) {
  tryCatch({
    DBI::dbDisconnect(conn, shutdown = FALSE)
  }, error = function(e) NULL)
}
