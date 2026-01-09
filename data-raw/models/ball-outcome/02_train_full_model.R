# Train Full Outcome Model ----
#
# Trains the complete delivery outcome model with ALL features.
# This model uses EVERYTHING: context + player skills + team skills + venue skills
#
# Purpose:
#   - Maximum prediction accuracy for match simulation
#   - Ball-by-ball outcome prediction using all available information
#   - Used in simulate_match_ballbyball() for categorical/expected outcome draws
#
# Features used:
#   - Context: over, ball, wickets, runs_difference, phase, innings, format, gender
#   - Match context: is_knockout, event_tier
#   - Player skills: batter_scoring_index, batter_survival_rate,
#                   bowler_economy_index, bowler_strike_rate
#   - Team skills: batting_team_runs_skill, batting_team_wicket_skill,
#                 bowling_team_runs_skill, bowling_team_wicket_skill
#   - Venue skills: venue_run_rate, venue_wicket_rate,
#                  venue_boundary_rate, venue_dot_rate
#
# REQUIRES:
#   1. install_all_bouncer_data() - to populate database
#   2. Run 03_calculate_skill_indices.R - for player skills
#   3. Run 02_calculate_team_skill_indices.R - for team skills
#   4. Run 01_calculate_venue_skill_indices.R - for venue skills
#
# Target: 7-category multinomial outcome
#   0 = Wicket, 1 = 0 runs, 2 = 1 run, 3 = 2 runs, 4 = 3 runs, 5 = 4 runs, 6 = 6 runs
#
# Output:
#   - bouncerdata/models/full_outcome_{format}.ubj

# Setup ----
library(DBI)
library(dplyr)
library(xgboost)
if (!("bouncer" %in% loadedNamespaces())) devtools::load_all()

# Configuration ----
FORMATS_TO_TRAIN <- c("t20", "odi", "test")  # Which formats to train
MATCH_LIMIT <- NULL  # NULL = all matches, or set number for testing
RANDOM_SEED <- 42
CV_FOLDS <- 5
MAX_ROUNDS <- 2000
EARLY_STOPPING <- 20

cat("\n")
cli::cli_h1("Full Outcome Model Training")
cli::cli_alert_info("Training models with ALL features (context + player + team + venue skills)")
cli::cli_alert_info("Formats: {paste(FORMATS_TO_TRAIN, collapse = ', ')}")
cat("\n")

# Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = TRUE)
cli::cli_alert_success("Connected")

# Create output directory
models_dir <- file.path("..", "bouncerdata", "models")
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

  # Determine format filter
  if (format == "t20") {
    format_filter <- "LOWER(match_type) IN ('t20', 'it20')"
    max_overs <- 20
  } else if (format == "odi") {
    format_filter <- "LOWER(match_type) IN ('odi', 'odm')"
    max_overs <- 50
  } else {
    format_filter <- "LOWER(match_type) IN ('test', 'mdm')"
    max_overs <- NULL
  }

  # Build SQL query with context features
  query <- sprintf("
    WITH innings_totals AS (
      SELECT
        match_id,
        innings,
        batting_team,
        MAX(total_runs) AS innings_total
      FROM deliveries
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
      FROM deliveries d
      WHERE %s
    ),
    match_context AS (
      SELECT DISTINCT
        m.match_id,
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
      FROM matches m
    )
    SELECT
      cs.delivery_id,
      cs.match_id,
      cs.match_type,
      cs.innings,
      cs.over,
      cs.ball,
      cs.over_ball,
      cs.venue,
      cs.gender,
      cs.batter_id,
      cs.bowler_id,
      cs.batting_team,
      cs.bowling_team,
      cs.runs_batter,
      cs.is_wicket,
      -- FIX: wickets_fallen in Cricsheet is AFTER the delivery, so subtract is_wicket
      -- to get the count BEFORE this delivery (prevents data leakage)
      (cs.wickets_fallen - CAST(cs.is_wicket AS INT)) AS wickets_fallen,
      (cs.batting_score - cs.bowling_score) AS runs_difference,
      COALESCE(mc.is_knockout, 0) AS is_knockout,
      COALESCE(mc.event_tier, 3) AS event_tier
    FROM cumulative_scores cs
    LEFT JOIN match_context mc ON cs.match_id = mc.match_id
    WHERE cs.runs_batter NOT IN (5)
      AND cs.runs_batter <= 6
    %s
  ", format_filter, format_filter,
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

  # Join Skill Indices ----
  cli::cli_h3("Joining skill indices")

  # Player skills
  cli::cli_alert_info("Adding player skills...")
  model_data <- add_skill_features(model_data, format = format, conn = conn, fill_missing = TRUE)
  n_player <- sum(!is.na(model_data$batter_scoring_index))
  cli::cli_alert_success("{n_player}/{nrow(model_data)} have player skills")

  # Team skills
  cli::cli_alert_info("Adding team skills...")
  tryCatch({
    model_data <- join_team_skill_indices(model_data, format = format, conn = conn)
    # Fill missing with 0 (neutral for residual-based)
    model_data <- model_data %>%
      mutate(
        batting_team_runs_skill = coalesce(batting_team_runs_skill, 0),
        batting_team_wicket_skill = coalesce(batting_team_wicket_skill, 0),
        bowling_team_runs_skill = coalesce(bowling_team_runs_skill, 0),
        bowling_team_wicket_skill = coalesce(bowling_team_wicket_skill, 0)
      )
    n_team <- sum(!is.na(model_data$batting_team_runs_skill) & model_data$batting_team_runs_skill != 0)
    cli::cli_alert_success("{n_team}/{nrow(model_data)} have team skills")
  }, error = function(e) {
    cli::cli_alert_warning("Team skills not available: {e$message}")
    cli::cli_alert_info("Using neutral values (0) for team skills")
    model_data <<- model_data %>%
      mutate(
        batting_team_runs_skill = 0,
        batting_team_wicket_skill = 0,
        bowling_team_runs_skill = 0,
        bowling_team_wicket_skill = 0
      )
  })

  # Venue skills
  cli::cli_alert_info("Adding venue skills...")
  tryCatch({
    model_data <- join_venue_skill_indices(model_data, format = format, conn = conn)
    # Fill missing with neutral values
    # For residual-based (run_rate, wicket_rate): 0
    # For raw EMA (boundary_rate, dot_rate): use format defaults
    start_vals <- get_venue_start_values(format)
    model_data <- model_data %>%
      mutate(
        venue_run_rate = coalesce(venue_run_rate, 0),
        venue_wicket_rate = coalesce(venue_wicket_rate, 0),
        venue_boundary_rate = coalesce(venue_boundary_rate, start_vals$boundary_rate),
        venue_dot_rate = coalesce(venue_dot_rate, start_vals$dot_rate)
      )
    n_venue <- sum(!is.na(model_data$venue_run_rate) & model_data$venue_run_rate != 0)
    cli::cli_alert_success("{n_venue}/{nrow(model_data)} have venue skills")
  }, error = function(e) {
    cli::cli_alert_warning("Venue skills not available: {e$message}")
    cli::cli_alert_info("Using neutral values for venue skills")
    start_vals <- get_venue_start_values(format)
    model_data <<- model_data %>%
      mutate(
        venue_run_rate = 0,
        venue_wicket_rate = 0,
        venue_boundary_rate = start_vals$boundary_rate,
        venue_dot_rate = start_vals$dot_rate
      )
  })

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
        overs_left = pmax(0, max_overs - over_ball),

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
        ),

        # No overs_left for Test cricket
        overs_left = NA_real_
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

  prepare_full_xgb_features <- function(data, fmt) {
    if (fmt %in% c("t20", "odi")) {
      # Short-form features
      data %>%
        mutate(
          format_t20 = as.integer(fmt == "t20"),
          format_odi = as.integer(fmt == "odi"),
          phase_powerplay = as.integer(phase == "powerplay"),
          phase_middle = as.integer(phase == "middle"),
          phase_death = as.integer(phase == "death"),
          gender_male = as.integer(tolower(gender) == "male"),
          innings_num = as.integer(innings),
          # Experience indicators (log transform for stability)
          batter_experience = log1p(coalesce(batter_balls_faced, 0)),
          bowler_experience = log1p(coalesce(bowler_balls_bowled, 0))
        ) %>%
        select(
          outcome,
          # Context features
          format_t20, format_odi,
          innings_num, over, ball,
          wickets_fallen, runs_difference, overs_left,
          phase_powerplay, phase_middle, phase_death,
          gender_male,
          is_knockout, event_tier,
          # Player skills
          batter_scoring_index, batter_survival_rate,
          bowler_economy_index, bowler_strike_rate,
          batter_experience, bowler_experience,
          # Team skills
          batting_team_runs_skill, batting_team_wicket_skill,
          bowling_team_runs_skill, bowling_team_wicket_skill,
          # Venue skills
          venue_run_rate, venue_wicket_rate,
          venue_boundary_rate, venue_dot_rate
        )
    } else {
      # Long-form features (no overs_left)
      data %>%
        mutate(
          phase_new_ball = as.integer(phase == "new_ball"),
          phase_middle = as.integer(phase == "middle"),
          phase_old_ball = as.integer(phase == "old_ball"),
          gender_male = as.integer(tolower(gender) == "male"),
          innings_num = as.integer(innings),
          batter_experience = log1p(coalesce(batter_balls_faced, 0)),
          bowler_experience = log1p(coalesce(bowler_balls_bowled, 0))
        ) %>%
        select(
          outcome,
          # Context features
          innings_num, over, ball,
          wickets_fallen, runs_difference,
          phase_new_ball, phase_middle, phase_old_ball,
          gender_male,
          is_knockout, event_tier,
          # Player skills
          batter_scoring_index, batter_survival_rate,
          bowler_economy_index, bowler_strike_rate,
          batter_experience, bowler_experience,
          # Team skills
          batting_team_runs_skill, batting_team_wicket_skill,
          bowling_team_runs_skill, bowling_team_wicket_skill,
          # Venue skills
          venue_run_rate, venue_wicket_rate,
          venue_boundary_rate, venue_dot_rate
        )
    }
  }

  train_features <- prepare_full_xgb_features(train_data, format)
  test_features <- prepare_full_xgb_features(test_data, format)

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
    eta = 0.1,
    subsample = 0.8,
    colsample_bytree = 0.8,
    eval_metric = "mlogloss"
  )

  set.seed(RANDOM_SEED)
  cv_model <- xgb.cv(
    params = params,
    data = dtrain,
    nrounds = MAX_ROUNDS,
    folds = folds,
    early_stopping_rounds = EARLY_STOPPING,
    verbose = 0,
    print_every_n = 50
  )

  # Get best iteration with fallback for different xgboost versions (v3.1+ uses early_stop$)
  best_nrounds <- cv_model$early_stop$best_iteration %||%
                  cv_model$best_iteration %||%
                  cv_model$best_iter %||%
                  cv_model$niter
  if (is.null(best_nrounds) || is.na(best_nrounds) || best_nrounds < 1) {
    eval_log <- cv_model$evaluation_log
    if ("test_mlogloss_mean" %in% names(eval_log)) {
      best_nrounds <- which.min(eval_log$test_mlogloss_mean)
    } else {
      best_nrounds <- 100  # Default fallback
    }
  }
  best_score <- cv_model$evaluation_log$test_mlogloss_mean[best_nrounds]

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
  # Reshape to matrix if needed (for multi-class, returns n_samples x n_classes)
  if (!is.matrix(test_probs)) {
    test_probs <- matrix(test_probs, ncol = 7, byrow = TRUE)
  }
  test_predictions <- max.col(test_probs) - 1

  accuracy <- mean(test_predictions == test_features$outcome)
  test_logloss <- mean(-log(pmax(test_probs[cbind(1:nrow(test_probs), test_features$outcome + 1)], 1e-15)))

  cli::cli_alert_success("Test accuracy: {.val {round(accuracy * 100, 2)}}%")
  cli::cli_alert_success("Test mlogloss: {.val {round(test_logloss, 4)}}")

  # Feature Importance
  importance_matrix <- xgb.importance(feature_names = feature_names, model = xgb_model)
  cli::cli_alert_info("Top 10 features:")
  top_features <- head(importance_matrix, 10)
  for (i in seq_len(nrow(top_features))) {
    cli::cli_alert("  {i}. {top_features$Feature[i]} (gain: {round(top_features$Gain[i], 3)})")
  }

  # Save Model ----
  cli::cli_h3("Saving model")

  model_path <- file.path(models_dir, sprintf("full_outcome_%s.ubj", format))
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
    feature_names = feature_names,
    n_train = nrow(train_data),
    n_test = nrow(test_data)
  )
}

# Save Combined Results ----
cat("\n")
cli::cli_rule("Summary")
cat("\n")

results_path <- file.path(models_dir, "full_model_results.rds")
saveRDS(all_results, results_path)
cli::cli_alert_success("All results saved to {.file {results_path}}")

# Print summary
cli::cli_h3("Model Performance Summary")
for (format in names(all_results)) {
  res <- all_results[[format]]
  cli::cli_alert_info("{toupper(format)}: Accuracy={round(res$test_accuracy*100,1)}%, LogLoss={round(res$test_logloss,4)}, Rounds={res$best_nrounds}")
}

# Compare with agnostic model
cli::cli_h3("Comparison with Agnostic Model")
agnostic_results_path <- file.path(models_dir, "agnostic_model_results.rds")
if (file.exists(agnostic_results_path)) {
  agnostic_results <- readRDS(agnostic_results_path)
  for (format in names(all_results)) {
    if (format %in% names(agnostic_results)) {
      full_ll <- all_results[[format]]$test_logloss
      agnostic_ll <- agnostic_results[[format]]$test_logloss
      improvement <- round((agnostic_ll - full_ll) / agnostic_ll * 100, 1)
      cli::cli_alert_info("{toupper(format)}: Full={round(full_ll, 4)}, Agnostic={round(agnostic_ll, 4)}, Improvement={improvement}%")
    }
  }
} else {
  cli::cli_alert_info("Agnostic model results not found for comparison")
}

cat("\n")
cli::cli_alert_success("Full model training complete!")
cli::cli_alert_info("Models saved to: {.file {models_dir}}")
cli::cli_alert_info("Use load_full_model() to load for simulations")
cat("\n")
