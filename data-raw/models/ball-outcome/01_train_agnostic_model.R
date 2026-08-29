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
# Target: OUTCOME_CATEGORIES (R/constants.R) -- wicket, 0-4 runs, 6 runs, wide
# (#81/D-P50 stage 3: wides are now trained on, not excluded; no-balls stay
# folded into the run categories, unchanged from before).
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
# Honour a caller-supplied value (run_all_models.R sets this) rather than
# overwriting it, matching the TUNE_HYPERPARAMS idiom below.
if (!exists("FORMATS_TO_TRAIN")) FORMATS_TO_TRAIN <- c("t20", "odi", "test")
MATCH_LIMIT <- NULL  # NULL = all matches, or set number for testing
RANDOM_SEED <- 42
CV_FOLDS <- 5
MAX_ROUNDS <- 2000
EARLY_STOPPING <- 20
if (!exists("TUNE_HYPERPARAMS")) TUNE_HYPERPARAMS <- FALSE  # Set TRUE before sourcing for tuning
if (!exists("TUNE_ITERATIONS")) TUNE_ITERATIONS <- 20       # Number of random search trials

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
bouncerdata_root <- find_bouncerdata_dir(create = FALSE)
if (is.null(bouncerdata_root)) {
  stop("Cannot locate bouncerdata/ directory. Run from within the bouncer/ workspace with bouncerdata/ as sibling.")
}
models_dir <- file.path(bouncerdata_root, "models")
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
    format_filter_bare <- "LOWER(match_type) IN ('t20', 'it20')"
    type_list <- "'t20', 'it20'"
  } else if (format == "odi") {
    format_filter_d <- "LOWER(d.match_type) IN ('odi', 'odm')"
    format_filter_bare <- "LOWER(match_type) IN ('odi', 'odm')"
    type_list <- "'odi', 'odm'"
  } else {
    format_filter_d <- "LOWER(d.match_type) IN ('test', 'mdm')"
    format_filter_bare <- "LOWER(match_type) IN ('test', 'mdm')"
    type_list <- "'test', 'mdm'"
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
        -- FIX: total_runs is the innings score AFTER this delivery (the parser writes
        -- the running total post-ball). Subtract the ball's own runs to get the score
        -- BEFORE it, or runs_difference leaks the target it is used to predict.
        (d.total_runs - (d.runs_batter + d.runs_extras)) AS batting_score,
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
        m.balls_per_over,
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
      cs.wides,
      cs.is_free_hit,
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
      -- Wides are no longer excluded (#81/D-P50 stage 3) -- they're now their
      -- own OUTCOME_CATEGORIES bucket ('wide'), not zero training signal.
      -- This REOPENS the population-mismatch risk the 2026-08-18 fix closed
      -- the other way: R/raa_cricsheet.R and other consumers of this model
      -- still filter wides out of what THEY score (stage 4, not done yet, is
      -- the RAA-side half of this change) -- so until that lands, this model
      -- will allocate some probability mass to a category none of its
      -- current callers ever ask about for a specific ball. That's inert,
      -- not wrong: expected-runs/wicket for a real (non-wide) ball still
      -- correctly renormalizes over all 8 categories via
      -- calculate_expected_runs()/calculate_expected_wicket_prob().
      AND cs.batter_id IS NOT NULL
      AND cs.bowler_id IS NOT NULL
      AND cs.innings BETWEEN 1
          AND CASE WHEN LOWER(cs.match_type) IN ('test', 'mdm') THEN 4 ELSE 2 END
      AND COALESCE(mc.balls_per_over, 6) = 6
    %s
  ", format_filter_bare,  # innings_totals: bare deliveries table
     format_filter_d,      # cumulative_scores: d. prefix
     if (!is.null(MATCH_LIMIT)) sprintf("LIMIT %d", MATCH_LIMIT * 1000) else "")

  # Execute query
  cli::cli_h3("Loading data")
  cli::cli_alert_info("Executing query...")
  model_data <- DBI::dbGetQuery(conn, query)

  # league_avg_runs/league_avg_wicket (bouncerverse#84/#85): a decayed
  # venue->league causal hierarchy, computed by the SAME shared function
  # raa_cricsheet.R calls at serving time, so training and serving cannot
  # independently drift the way this exact feature already had (two
  # hand-written copies of the same flat SQL window function). Replaces the
  # inline league_stats/league_running_avg CTEs this query used to carry.
  if (nrow(model_data) > 0) {
    ctx <- compute_context_features(conn, type_list)
    model_data <- merge(model_data, ctx, by = "match_id", all.x = TRUE)

    # Two real gaps caught by review (2026-08-29), both from merge()'s
    # defaults rather than the feature computation itself:
    #
    # 1. The removed SQL's COALESCE(lra.league_avg_runs, %f) guaranteed
    #    model_data NEVER carried NA in these two columns -- any match
    #    compute_context_features() doesn't cover (its query requires
    #    event_name IS NOT NULL) now arrives as a real NA after this merge.
    #    The SERVING path already coalesces (prepare_agnostic_features(),
    #    agnostic_model.R), but nothing downstream here does, so an NA would
    #    reach xgb.DMatrix() silently -- restoring the same default-fill
    #    contract training always had.
    default_runs <- switch(format,
      t20 = EXPECTED_RUNS_T20, odi = EXPECTED_RUNS_ODI, EXPECTED_RUNS_TEST)
    default_wicket <- switch(format,
      t20 = EXPECTED_WICKET_T20, odi = EXPECTED_WICKET_ODI, EXPECTED_WICKET_TEST)

    # Coverage check (review, 2026-08-29): a silently-collapsed join here would
    # recreate the exact bug this whole fix exists to close -- every affected
    # row quietly becomes the flat constant default with success lines
    # printing throughout. Known baseline is ~0.5% (matches with no
    # event_name). Abort rather than train on a broken join.
    n_missing <- sum(is.na(model_data$league_avg_runs))
    pct_missing <- 100 * n_missing / nrow(model_data)
    cli::cli_alert_info("Context features: {n_missing} rows ({round(pct_missing, 2)}%) fell back to the format default.")
    if (pct_missing > 5) {
      cli::cli_abort("Context feature coverage collapsed to {round(pct_missing, 2)}% (expected ~0.5%) -- compute_context_features() join is likely broken.")
    }

    model_data$league_avg_runs <- dplyr::coalesce(model_data$league_avg_runs, default_runs)
    model_data$league_avg_wicket <- dplyr::coalesce(model_data$league_avg_wicket, default_wicket)

    # 2. merge() defaults to sort = TRUE, reordering model_data by match_id
    #    (a lexicographic STRING sort -- "1000" < "999") as a side effect.
    #    The train/test split below is a positional cut of
    #    unique(model_data$match_id), not a random sample, so silently
    #    changing row order silently changes which matches land in train vs
    #    test. Restored to a deterministic, meaningful order explicitly
    #    rather than leaving it as an accidental byproduct of the merge.
    model_data <- model_data[order(model_data$match_id, model_data$delivery_id), ]
  }

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
        # Wicket checked first, unchanged priority: a wicket on a wide
        # (stumped/run-out/hit-wicket only -- rare) still categorizes as
        # wicket, not wide. Category indices are 0-based, matching
        # OUTCOME_CATEGORIES order (R/constants.R): wicket, 0-4, 6, wide.
        outcome = case_when(
          is_wicket ~ 0L,
          coalesce(wides, 0) > 0 ~ 7L,
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
        # Wicket checked first, unchanged priority: a wicket on a wide
        # (stumped/run-out/hit-wicket only -- rare) still categorizes as
        # wicket, not wide. Category indices are 0-based, matching
        # OUTCOME_CATEGORIES order (R/constants.R): wicket, 0-4, 6, wide.
        outcome = case_when(
          is_wicket ~ 0L,
          coalesce(wides, 0) > 0 ~ 7L,
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
  cli::cli_alert_info("Outcome distribution: {paste(paste0(OUTCOME_CATEGORIES, ':', outcome_pct, '%'), collapse = ', ')}")

  # Train-Test Split ----
  cli::cli_h3("Creating train-test split")

  set.seed(RANDOM_SEED)
  unique_matches <- sample(unique(model_data$match_id))
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
          innings_num = as.integer(innings),
          # #81/D-P50 stage 3: on a free hit the batter can (almost always)
          # only be dismissed by run-out -- every other wicket type is void.
          # A feature, not a masked wicket_kind rewrite, so the trees learn
          # the interaction rather than having it imposed.
          is_free_hit_int = as.integer(coalesce(is_free_hit, FALSE))
        ) %>%
        select(
          outcome,
          format_t20, format_odi,
          innings_num, over, ball,
          wickets_fallen, runs_difference, overs_left,
          phase_powerplay, phase_middle, phase_death,
          gender_male,
          is_knockout, event_tier,
          is_free_hit_int,
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
          innings_num = as.integer(innings),
          is_free_hit_int = as.integer(coalesce(is_free_hit, FALSE))
        ) %>%
        select(
          outcome,
          innings_num, over, ball,
          wickets_fallen, runs_difference,
          phase_new_ball, phase_middle, phase_old_ball,
          gender_male,
          is_knockout, event_tier,
          is_free_hit_int,
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

  # Hyperparameter Tuning / CV ----

  fixed_params <- list(
    objective = "multi:softprob",
    num_class = length(OUTCOME_CATEGORIES),
    eval_metric = "mlogloss",
    # #81/D-P50 stage 3: Test format's xgb.cv() crashed reproducibly with
    # zero R-level error (process fully gone, no message) on default
    # (unbounded) threading -- 3 clean attempts, fold class-imbalance and
    # memory pressure both ruled out. A controlled nthread=1 smoke test
    # completed all 10 rounds cleanly where every default-threading attempt
    # died before round 1, confirming a native OpenMP/threading crash.
    # Fixed at a small bounded value rather than left unlimited, for all
    # formats -- T20/ODI happening to succeed with default threading may
    # have been luck, not proof of safety.
    nthread = 4
  )

  if (TUNE_HYPERPARAMS) {
    cli::cli_h3("Tuning hyperparameters via random search ({TUNE_ITERATIONS} trials)")

    tuning_result <- tune_xgb_params(
      dtrain = dtrain,
      folds = folds,
      fixed_params = fixed_params,
      n_iter = TUNE_ITERATIONS,
      max_rounds = MAX_ROUNDS,
      early_stopping = EARLY_STOPPING,
      seed = RANDOM_SEED
    )

    params <- tuning_result$best_params
    cli::cli_alert_success("Best tuned params: max_depth={params$max_depth}, eta={round(params$eta, 3)}, subsample={round(params$subsample, 2)}")
  } else {
    params <- c(fixed_params, list(
      max_depth = 6,
      eta = 0.15,
      subsample = 0.8,
      colsample_bytree = 0.8
    ))
  }

  cli::cli_h3("Finding optimal rounds via CV")

  set.seed(RANDOM_SEED)
  cv_model <- xgb.cv(
    params = params,
    data = dtrain,
    nrounds = MAX_ROUNDS,
    folds = folds,
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

  # Per-cut calibration (#81/D-P50 stage 3, per docs/plans/D-P50-...md §(f)):
  # an aggregate improving while a reachable slice gets worse is exactly the
  # failure mode this repo's own model-building doctrine warns about. Cut by
  # the two things this stage actually changed -- wide (a brand-new category,
  # previously zero training signal) and free-hit (a brand-new feature).
  wide_idx <- which(OUTCOME_CATEGORIES == "wide") - 1L  # 0-based, matches `outcome`
  is_wide_true <- test_features$outcome == wide_idx
  is_wide_pred <- test_predictions == wide_idx
  n_wide_true <- sum(is_wide_true)
  wide_recall <- if (n_wide_true > 0) mean(is_wide_pred[is_wide_true]) else NA_real_
  wide_precision <- if (sum(is_wide_pred) > 0) mean(is_wide_true[is_wide_pred]) else NA_real_
  cli::cli_alert_info(
    "Wide: n={.val {n_wide_true}} true, recall={.val {round(100*wide_recall,1)}}%, precision={.val {round(100*wide_precision,1)}}%")

  non_wide <- !is_wide_true
  nonwide_accuracy <- mean(test_predictions[non_wide] == test_features$outcome[non_wide])
  nonwide_logloss <- mean(-log(pmax(
    test_probs[cbind(which(non_wide), test_features$outcome[non_wide] + 1)], 1e-15)))
  cli::cli_alert_info(
    "Non-wide only (n={.val {sum(non_wide)}}, comparable to the pre-stage-3 population): accuracy={.val {round(100*nonwide_accuracy,2)}}%, mlogloss={.val {round(nonwide_logloss,4)}}")

  fh <- test_features$is_free_hit_int == 1L
  n_fh <- sum(fh)
  if (n_fh > 0) {
    fh_accuracy <- mean(test_predictions[fh] == test_features$outcome[fh])
    nonfh_accuracy <- mean(test_predictions[!fh] == test_features$outcome[!fh])
    cli::cli_alert_info(
      "Free-hit rows (n={.val {n_fh}}): accuracy={.val {round(100*fh_accuracy,2)}}% vs non-free-hit accuracy={.val {round(100*nonfh_accuracy,2)}}%")
  } else {
    cli::cli_alert_warning("Zero free-hit rows in the test set -- cannot check that cut.")
  }

  # Feature Importance
  importance_matrix <- xgb.importance(feature_names = feature_names, model = xgb_model)
  cli::cli_alert_info("Top features: {paste(head(importance_matrix$Feature, 5), collapse = ', ')}")

  # Save Model ----
  cli::cli_h3("Saving model")

  model_path <- file.path(models_dir, sprintf("agnostic_outcome_%s.ubj", format))
  # Stamp the build date before saving. bouncer's loaders refuse an outcome
  # model that is unstamped or predates the post-delivery leak fix
  # (.check_model_vintage() in R/agnostic_model.R), because the bouncermodels
  # release served a 2026-03-27 vintage in preference to corrected local files
  # for five months without anything noticing (bouncerverse#50).
  xgb.attr(xgb_model, "bouncer_build_date") <- as.character(Sys.Date())  # not format(): `format` is the loop variable here
  # Stamp feature names AND order too (#81/D-P50 stage 4, mirroring
  # 02_train_full_model.R's #76 fix -- this trainer never had it, so
  # .assert_feature_alignment() warned rather than protected for every
  # agnostic model ever built here, discovered while wiring is_free_hit into
  # the RAA scorer). The booster's own feature_names comes back length 0
  # after an xgb.save()/xgb.load() UBJ round-trip, so width was the only
  # thing checkable -- and two same-width frames with columns in a different
  # order predict nonsense, silently. FEATURE_NAMES_ATTR /
  # .encode_feature_names() live in R/agnostic_model.R next to the
  # build-date stamp this mirrors.
  xgb.attr(xgb_model, FEATURE_NAMES_ATTR) <- .encode_feature_names(feature_names)
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
    wide_recall = wide_recall,
    wide_precision = wide_precision,
    nonwide_accuracy = nonwide_accuracy,
    nonwide_logloss = nonwide_logloss,
    n_free_hit_test = n_fh,
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

# Record Benchmarks ----
cli::cli_h3("Recording benchmarks")

# Close read-only connection first to release DuckDB lock
if (exists("conn") && !is.null(conn)) {
  tryCatch(DBI::dbDisconnect(conn, shutdown = TRUE), error = function(e) NULL)
  conn <- NULL
}

tryCatch({
  bench_conn <- get_db_connection(read_only = FALSE)

  # Compare against the PREVIOUS run BEFORE recording this one. record_benchmarks()
  # inserts a row with a newer run_timestamp, and get_latest_benchmark() selects
  # MAX(run_timestamp) -- so checking after recording compares this run against
  # itself, reports 0% change, and can never flag a regression. That is why the
  # 2026-08-18 frame fix printed "All metrics stable or improved" while T20
  # mlogloss moved 1.3805 -> 1.4137 (+2.40%, over the 2% threshold).
  # bouncerverse#84/#85 (2026-08-29): this run's train/test split membership
  # differs from every prior stored benchmark -- the split used to be a
  # positional slice of unique(match_id) with an unconsumed set.seed(), now
  # genuinely sample()'d, and the league_avg_runs/wicket feature itself
  # changed from a flat all-time mean to a decayed venue->league hierarchy.
  # A move in either direction on THIS run's regression check is not a clean
  # apples-to-apples signal against the pre-fix baseline -- don't treat a red
  # "regression" print from this specific run as proof the fix hurt accuracy,
  # or a green one as proof it helped. Re-evaluate from the NEXT run onward,
  # once the stored baseline itself reflects the new split/feature.
  cli::cli_alert_warning("Benchmark comparisons below are vs. a pre-fix baseline with a different train/test split AND feature -- not apples-to-apples for this run only.")

  for (fmt in names(all_results)) {
    # #81/D-P50 stage 3: test_logloss now spans 8 categories (added wide);
    # the stored benchmark history is from the pre-stage-3 7-category model.
    # Comparing them directly would be the SAME false-regression shape the
    # comment above already documents for a different reason -- +2.4%/+2.0%
    # measured on T20/ODI is the expected mechanical cost of representing an
    # 8th category, not a real regression. nonwide_logloss is scored on the
    # same population the stored benchmark was, so it's the comparable metric.
    regression <- check_benchmark_regression(
      conn = bench_conn,
      step_name = "agnostic_model",
      format = fmt,
      current_metrics = list(
        mlogloss = all_results[[fmt]]$nonwide_logloss
      )
    )
    if (regression$is_regression) {
      cli::cli_alert_danger("{toupper(fmt)}: {paste(regression$messages, collapse = '; ')}")
    } else {
      cli::cli_alert_success("{toupper(fmt)}: {regression$messages}")
    }
  }

  for (fmt in names(all_results)) {
    res <- all_results[[fmt]]
    record_benchmarks(
      conn = bench_conn,
      step_name = "agnostic_model",
      model_name = paste0("agnostic_outcome_", fmt),
      format = fmt,
      metrics = list(
        # "mlogloss" stays the population-comparable metric so future runs'
        # regression checks keep working across a category-count change
        # (#81/D-P50 stage 3) -- see the check above. The raw all-category
        # number is kept too, under its own name, not discarded.
        mlogloss = res$nonwide_logloss,
        mlogloss_all_categories = res$test_logloss,
        accuracy = res$test_accuracy,
        cv_mlogloss = res$best_cv_score,
        best_nrounds = res$best_nrounds
      ),
      n_train = res$n_train,
      n_test = res$n_test,
      notes = "Grouped CV folds by match_id"
    )
  }

  DBI::dbDisconnect(bench_conn, shutdown = TRUE)
}, error = function(e) {
  cli::cli_alert_warning("Benchmark recording failed: {conditionMessage(e)}")
})

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
