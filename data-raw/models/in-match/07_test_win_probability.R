# Test Cricket Win Probability Model (3-class: Win/Draw/Loss) ----
#
# Uses the PROJECTED SCORE MODEL output as a key feature.
# The projected score model already works (+44% over naive), so we
# leverage it: projected lead/deficit + overs remaining → win/draw/loss.
#
# Key design decisions:
#   - Projected innings total from Stage 1 model as core feature
#   - Overs remaining (max 450 - cumulative) for draw prediction
#   - Sample every 6th delivery (one per over) to reduce noise
#   - Upweight 3rd/4th innings (more informative)
#   - 3-class: team1_win=0, draw=1, team2_win=2

# Setup ----
library(DBI)
library(dplyr)
library(data.table)
library(xgboost)
devtools::load_all()

RANDOM_SEED <- 42
CV_FOLDS <- 5
MAX_ROUNDS <- 2000
EARLY_STOPPING <- 30

cat("\n")
cli::cli_h1("Test Win Probability Model v2 (Projection-Based)")
cat("\n")

# Load Data ----
cli::cli_h2("Loading data")

output_dir <- file.path(find_bouncerdata_dir(), "models")
conn <- get_db_connection(read_only = TRUE)

# Load stage 1 model for projected scores
stage1_path <- file.path(output_dir, "test_stage1_results.rds")
if (!file.exists(stage1_path)) stop("Run 03_projected_score_model.R with IN_MATCH_FORMAT='test' first")
stage1_results <- readRDS(stage1_path)
stage1_model <- stage1_results$model
stage1_features <- stage1_results$feature_cols
cli::cli_alert_success("Loaded Stage 1 projected score model")

# Load prepared stage 1 data (has all the rolling features we need)
stage1_data_path <- file.path(output_dir, "test_stage1_data.rds")
if (!file.exists(stage1_data_path)) stop("Run 01_prepare_all_formats.R with FORMATS_TO_PREPARE='test' first")
stage1_data <- readRDS(stage1_data_path)

# Load all Test deliveries with outcomes
deliveries <- DBI::dbGetQuery(conn, "
  SELECT
    d.delivery_id, d.match_id, d.season, d.match_date,
    d.venue, d.gender, d.batting_team, d.bowling_team,
    d.innings, d.over, d.ball,
    d.total_runs,
    (d.wickets_fallen - CAST(d.is_wicket AS INT)) AS wickets_fallen,
    m.outcome_type, m.outcome_winner, m.team1, m.team2
  FROM cricsheet.deliveries d
  JOIN cricsheet.matches m ON d.match_id = m.match_id
  WHERE LOWER(d.match_type) IN ('test', 'mdm')
    AND m.outcome_type IS NOT NULL
  ORDER BY d.match_date, d.match_id, d.innings, d.over, d.ball
")
setDT(deliveries)
cli::cli_alert_success("Loaded {nrow(deliveries)} deliveries from {uniqueN(deliveries$match_id)} matches")

# Load innings totals (for completed innings and overs)
innings_totals <- DBI::dbGetQuery(conn, "
  SELECT match_id, innings, total_runs AS innings_total,
         total_wickets AS innings_wickets, total_overs AS innings_overs
  FROM cricsheet.match_innings
  WHERE match_id IN (
    SELECT match_id FROM cricsheet.matches WHERE LOWER(match_type) IN ('test', 'mdm')
  )
  ORDER BY match_id, innings
")
setDT(innings_totals)

# Venue averages
venue_avgs <- DBI::dbGetQuery(conn, "
  SELECT venue, AVG(total_runs) as venue_avg
  FROM cricsheet.match_innings mi
  JOIN cricsheet.matches m ON mi.match_id = m.match_id
  WHERE LOWER(m.match_type) IN ('test', 'mdm') AND mi.innings = 1
  GROUP BY venue HAVING COUNT(*) >= 3
")
setDT(venue_avgs)

DBI::dbDisconnect(conn, shutdown = TRUE)

# Feature Engineering ----
cli::cli_h2("Engineering features")

# Match outcome (3-class)
deliveries[, match_outcome := fcase(
  outcome_type == "draw", 1L,
  outcome_winner == team1, 0L,
  default = 2L
)]

# Basic state
deliveries[, `:=`(
  balls_bowled = as.integer(over * 6L + ball),
  wickets_in_hand = 10L - wickets_fallen,
  current_run_rate = fifelse(over > 0, total_runs / (over + ball/6), 0),
  batting_is_team1 = as.integer(batting_team == team1)
)]

# Innings totals (wide format)
inn_wide <- dcast(innings_totals, match_id ~ paste0("inn", innings),
                  value.var = c("innings_total", "innings_wickets", "innings_overs"), fill = NA)
deliveries <- merge(deliveries, inn_wide, by = "match_id", all.x = TRUE)
deliveries <- merge(deliveries, venue_avgs, by = "venue", all.x = TRUE)
deliveries[is.na(venue_avg), venue_avg := 340]

# Team1 lead (cumulative)
deliveries[, team1_completed := fcase(
  innings == 1, 0L,
  innings == 2, fifelse(!is.na(innings_total_inn1), innings_total_inn1, 0L),
  innings == 3, fifelse(!is.na(innings_total_inn1), innings_total_inn1, 0L),
  innings == 4, fifelse(!is.na(innings_total_inn1), innings_total_inn1, 0L) +
                fifelse(!is.na(innings_total_inn3), innings_total_inn3, 0L),
  default = 0L
)]
deliveries[, team2_completed := fcase(
  innings <= 2, 0L,
  innings == 3, fifelse(!is.na(innings_total_inn2), innings_total_inn2, 0L),
  innings == 4, fifelse(!is.na(innings_total_inn2), innings_total_inn2, 0L),
  default = 0L
)]
deliveries[, team1_lead := fcase(
  batting_is_team1 == 1L, as.integer(team1_completed + total_runs - team2_completed),
  default = as.integer(team1_completed - (team2_completed + total_runs))
)]

# Cumulative match overs (from actual innings overs)
deliveries[, cum_overs := as.double(over) + fcase(
  innings == 1, 0,
  innings == 2, fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 90),
  innings == 3, fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 90) +
                fifelse(!is.na(innings_overs_inn2), as.double(innings_overs_inn2), 90),
  innings == 4, fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 90) +
                fifelse(!is.na(innings_overs_inn2), as.double(innings_overs_inn2), 90) +
                fifelse(!is.na(innings_overs_inn3), as.double(innings_overs_inn3), 90),
  default = 0
)]

MAX_OVERS <- 450
deliveries[, `:=`(
  overs_remaining = pmax(0, MAX_OVERS - cum_overs),
  match_progress = pmin(1, cum_overs / MAX_OVERS),
  approx_day = pmin(5L, as.integer(floor(cum_overs / 90) + 1))
)]

# 4th innings specific
deliveries[innings == 4, `:=`(
  target = as.integer(team1_completed - team2_completed + 1L),
  runs_needed = pmax(0L, as.integer(team1_completed - team2_completed + 1L) - total_runs)
)]
deliveries[innings == 4, `:=`(
  req_rate = fifelse(overs_remaining > 0, as.double(runs_needed) / overs_remaining, 99),
  overs_per_wicket = fifelse(wickets_in_hand > 0, overs_remaining / as.double(wickets_in_hand), 0)
)]

# Fill NAs for non-4th-innings
for (col in c("target", "runs_needed", "req_rate", "overs_per_wicket")) {
  deliveries[is.na(get(col)), (col) := 0]
}

# Add phase features needed by Stage 1 model
deliveries[, `:=`(
  phase_powerplay = 0L,  # Test doesn't use powerplay phases in the same way
  phase_middle = as.integer(over >= 20 & over < 80),
  phase_death = 0L,
  phase_new_ball = as.integer(over < 20),
  phase_old_ball = as.integer(over >= 80),
  gender_male = as.integer(tolower(gender) == "male"),
  overs_remaining_innings = 0,  # Test has no fixed overs
  overs_into_phase = fcase(
    over < 20, as.double(over),
    over < 80, as.double(over - 20),
    default = as.double(over - 80)
  )
)]

# Generate projected scores using Stage 1 model
cli::cli_h2("Generating projected scores")

# Merge rolling features from stage1 data
stage1_all <- rbind(stage1_data$train, stage1_data$test)
setDT(stage1_all)

# Get projected score for each delivery
# Stage 1 only covers 1st innings — for other innings, use current run rate projection
deliveries[, projected_innings_total := fifelse(
  innings == 1 & over > 0, total_runs * (90 / (over + ball/6)),  # Simple rate projection for all innings
  total_runs * (90 / pmax(over + ball/6, 1))
)]

# Try to use actual Stage 1 XGBoost model for 1st innings
if (!is.null(stage1_model)) {
  tryCatch({
    inn1_idx <- which(deliveries$innings == 1)
    if (length(inn1_idx) > 0) {
      # Match deliveries to stage1 prepared data by delivery_id
      inn1_delivery_ids <- deliveries$delivery_id[inn1_idx]
      matched <- stage1_all[delivery_id %in% inn1_delivery_ids]

      if (nrow(matched) > 0) {
        # Ensure all feature columns exist, fill missing with 0
        for (feat in stage1_features) {
          if (!feat %in% names(matched)) {
            matched[, (feat) := 0]
          }
        }
        matched_features <- as.matrix(matched[, ..stage1_features])
        matched_features[is.na(matched_features)] <- 0

        dmat <- xgb.DMatrix(data = matched_features)
        preds <- predict(stage1_model, dmat)

        # Map back
        pred_dt <- data.table(delivery_id = matched$delivery_id, projected_xgb = preds)
        deliveries <- merge(deliveries, pred_dt, by = "delivery_id", all.x = TRUE)
        deliveries[!is.na(projected_xgb), projected_innings_total := projected_xgb]
        deliveries[, projected_xgb := NULL]

        n_xgb <- sum(!is.na(preds))
        cli::cli_alert_success("Applied Stage 1 model to {n_xgb} 1st innings deliveries")
      }
    }
  }, error = function(e) {
    cli::cli_alert_warning("Stage 1 prediction failed: {conditionMessage(e)}")
  })
}

# Projected lead: what will team1's lead be at end of this innings?
deliveries[, projected_lead := fcase(
  batting_is_team1 == 1L & innings == 1, as.double(projected_innings_total) - venue_avg,
  batting_is_team1 == 1L, as.double(team1_completed + projected_innings_total - team2_completed) - venue_avg,
  batting_is_team1 == 0L & innings == 2, as.double(team1_completed - (team2_completed + projected_innings_total)),
  default = as.double(team1_lead)
)]

cli::cli_alert_success("Features engineered")

# Sample: one per over (reduce noise, balance innings)
cli::cli_h2("Sampling (1 per over)")

# Take last ball of each over for cleaner state
sampled <- deliveries[, .SD[.N], by = .(match_id, innings, over)]
cli::cli_alert_info("Sampled {nrow(sampled)} from {nrow(deliveries)} (1 per over)")

# Outcome distribution
cat(sprintf("  team1_win: %d, draw: %d, team2_win: %d\n",
            sum(sampled$match_outcome == 0), sum(sampled$match_outcome == 1), sum(sampled$match_outcome == 2)))

# Prepare Features ----
cli::cli_h2("Preparing feature matrix")

feature_cols <- c(
  # Current innings state
  "total_runs", "wickets_fallen", "wickets_in_hand", "current_run_rate",
  # Innings
  "batting_is_team1",
  # Match state
  "team1_lead", "projected_lead", "projected_innings_total",
  # Time (critical for draws)
  "cum_overs", "overs_remaining", "match_progress", "approx_day",
  # 4th innings chase
  "target", "runs_needed", "req_rate", "overs_per_wicket",
  # Context
  "venue_avg"
)

for (col in feature_cols) {
  sampled[is.na(get(col)), (col) := 0]
}

# Innings as numeric (not one-hot — saves features)
sampled[, innings_num := as.double(innings)]
feature_cols <- c(feature_cols, "innings_num")

# Train/test split
TEST_SEASONS <- c("2024", "2025", "2023/24", "2024/25")
train_dt <- sampled[!season %in% TEST_SEASONS]
test_dt <- sampled[season %in% TEST_SEASONS]

y_train <- train_dt$match_outcome
y_test <- test_dt$match_outcome

cli::cli_alert_info("Train: {nrow(train_dt)} samples ({uniqueN(train_dt$match_id)} matches)")
cli::cli_alert_info("Test: {nrow(test_dt)} samples ({uniqueN(test_dt$match_id)} matches)")

# Grouped CV folds
set.seed(RANDOM_SEED)
unique_matches <- unique(train_dt$match_id)
shuffled <- sample(unique_matches)
fold_ids <- cut(seq_along(shuffled), breaks = CV_FOLDS, labels = FALSE)
folds <- lapply(1:CV_FOLDS, function(i) which(train_dt$match_id %in% shuffled[fold_ids == i]))

X_train <- as.matrix(train_dt[, ..feature_cols])
X_test <- as.matrix(test_dt[, ..feature_cols])

# Upweight later innings (3rd/4th more informative than 1st/2nd)
weights_train <- fifelse(train_dt$innings >= 3, 2.0, 1.0)
weights_train <- fifelse(train_dt$innings == 4, 3.0, weights_train)

dtrain <- xgb.DMatrix(data = X_train, label = y_train, weight = weights_train)
dtest <- xgb.DMatrix(data = X_test, label = y_test)

cli::cli_alert_success("Matrices created: {ncol(X_train)} features, weights: inn1-2=1x, inn3=2x, inn4=3x")

# Train Model ----
cli::cli_h2("Training 3-class model")

params <- list(
  objective = "multi:softprob",
  num_class = 3,
  eval_metric = "mlogloss",
  max_depth = 4,
  eta = 0.05,
  subsample = 0.8,
  colsample_bytree = 0.8,
  min_child_weight = 10,
  lambda = 2
)

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

best_nrounds <- cv_model$early_stop$best_iteration %||%
  cv_model$best_iteration %||%
  which.min(cv_model$evaluation_log$test_mlogloss_mean)
if (is.null(best_nrounds) || is.na(best_nrounds)) best_nrounds <- 100

best_cv <- cv_model$evaluation_log$test_mlogloss_mean[best_nrounds]
cli::cli_alert_success("Best: {best_nrounds} rounds, CV mlogloss: {round(best_cv, 4)}")

model <- xgb.train(params = params, data = dtrain, nrounds = best_nrounds, verbose = 0)

# Evaluate ----
cli::cli_h2("Evaluation")

raw_preds <- predict(model, dtest)
pred_probs <- matrix(raw_preds, ncol = 3, byrow = TRUE)
colnames(pred_probs) <- c("team1_win", "draw", "team2_win")
pred_class <- max.col(pred_probs) - 1

eps <- 1e-7
overall_acc <- mean(pred_class == y_test)
overall_mlogloss <- -mean(sapply(seq_along(y_test), function(i) {
  log(max(pred_probs[i, y_test[i] + 1], eps))
}))

baseline_random <- -log(1/3)

cat(sprintf("\n  OVERALL: accuracy=%.1f%%, mlogloss=%.4f (random=%.4f, improvement=%+.1f%%)\n",
            overall_acc * 100, overall_mlogloss, baseline_random,
            (baseline_random - overall_mlogloss) / baseline_random * 100))

# Per-class
for (cls in 0:2) {
  cls_name <- c("Team1 Win", "Draw", "Team2 Win")[cls + 1]
  idx <- y_test == cls
  if (sum(idx) > 0) {
    recall <- mean(pred_class[idx] == cls)
    cat(sprintf("  %-12s recall=%.1f%% (%d samples)\n", cls_name, recall * 100, sum(idx)))
  }
}

# Per-innings (the key metric)
cli::cli_h3("Performance by Innings")
for (inn in 1:4) {
  idx <- test_dt$innings == inn
  if (sum(idx) > 0) {
    inn_acc <- mean(pred_class[idx] == y_test[idx])
    inn_probs <- pred_probs[idx, ]
    inn_actual <- y_test[idx]
    inn_ml <- -mean(sapply(seq_along(inn_actual), function(i) {
      log(max(inn_probs[i, inn_actual[i] + 1], eps))
    }))
    imp <- (baseline_random - inn_ml) / baseline_random * 100
    cat(sprintf("  Innings %d: accuracy=%.1f%%, mlogloss=%.4f (%+.1f%% vs random), n=%d\n",
                inn, inn_acc * 100, inn_ml, imp, sum(idx)))
  }
}

# Per-day
cli::cli_h3("Performance by Day")
for (day in 1:5) {
  idx <- test_dt$approx_day == day
  if (sum(idx) > 10) {
    day_acc <- mean(pred_class[idx] == y_test[idx])
    day_probs <- pred_probs[idx, ]
    day_actual <- y_test[idx]
    day_ml <- -mean(sapply(seq_along(day_actual), function(i) {
      log(max(day_probs[i, day_actual[i] + 1], eps))
    }))
    imp <- (baseline_random - day_ml) / baseline_random * 100
    cat(sprintf("  Day %d: accuracy=%.1f%%, mlogloss=%.4f (%+.1f%% vs random), n=%d\n",
                day, day_acc * 100, day_ml, imp, sum(idx)))
  }
}

# Draw calibration
cli::cli_h3("Draw Calibration")
draw_probs_vec <- pred_probs[, "draw"]
actual_draw <- as.integer(y_test == 1)
for (lo in c(0, 0.2, 0.4, 0.6)) {
  hi <- lo + 0.2
  idx <- draw_probs_vec >= lo & draw_probs_vec < hi
  if (sum(idx) > 20) {
    cat(sprintf("  P(draw) %.0f-%.0f%%: predicted=%.1f%%, actual=%.1f%% (n=%d)\n",
                lo*100, hi*100, mean(draw_probs_vec[idx])*100, mean(actual_draw[idx])*100, sum(idx)))
  }
}
idx80 <- draw_probs_vec >= 0.8
if (sum(idx80) > 10) {
  cat(sprintf("  P(draw) 80-100%%: predicted=%.1f%%, actual=%.1f%% (n=%d)\n",
              mean(draw_probs_vec[idx80])*100, mean(actual_draw[idx80])*100, sum(idx80)))
}

# Feature importance
cli::cli_h3("Feature Importance")
importance <- xgb.importance(model = model)
for (i in seq_len(min(12, nrow(importance)))) {
  cli::cli_alert_info("{i}. {importance$Feature[i]}: {round(importance$Gain[i], 3)}")
}

# Save ----
cli::cli_h2("Saving")
xgb.save(model, file.path(output_dir, "test_win_probability.ubj"))
saveRDS(list(
  model = model, params = params, best_nrounds = best_nrounds,
  feature_cols = feature_cols,
  metrics = list(
    accuracy = overall_acc, mlogloss = overall_mlogloss,
    cv_mlogloss = best_cv, baseline = baseline_random,
    improvement = (baseline_random - overall_mlogloss) / baseline_random * 100
  ),
  importance = importance, created_at = Sys.time()
), file.path(output_dir, "test_winprob_results.rds"))
cli::cli_alert_success("Saved")

cat(sprintf("\n  SUMMARY: %.1f%% accuracy, mlogloss %.4f (%+.1f%% vs random)\n",
            overall_acc * 100, overall_mlogloss,
            (baseline_random - overall_mlogloss) / baseline_random * 100))
