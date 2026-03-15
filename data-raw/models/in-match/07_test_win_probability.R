# Test Cricket Win Probability Model (3-class: Win/Draw/Loss) ----
#
# Unlike T20/ODI, Test cricket has:
#   - 4 innings (not 2)
#   - Draws as ~33% of outcomes
#   - No fixed target until 4th innings
#   - Time pressure (5 days) affects draw probability
#   - Pitch deterioration across innings
#
# Architecture: XGBoost multiclass (3-class: team1_win, draw, team2_win)
# One model handles all 4 innings with innings-specific features.
#
# Output:
#   - bouncerdata/models/test_win_probability.ubj
#   - bouncerdata/models/test_winprob_results.rds

# Setup ----
library(DBI)
library(dplyr)
library(data.table)
library(xgboost)
devtools::load_all()

RANDOM_SEED <- 42
CV_FOLDS <- 5
MAX_ROUNDS <- 2000
EARLY_STOPPING <- 20

cat("\n")
cli::cli_h1("Test Cricket Win Probability Model (3-class)")
cat("\n")

# Load Data ----
cli::cli_h2("Loading data")

output_dir <- file.path(find_bouncerdata_dir(), "models")
conn <- get_db_connection(read_only = TRUE)

# Load all Test deliveries with match outcomes
deliveries <- DBI::dbGetQuery(conn, "
  SELECT
    d.delivery_id, d.match_id, d.season, d.match_date,
    d.venue, d.gender, d.batting_team, d.bowling_team,
    d.innings, d.over, d.ball,
    d.runs_batter, d.runs_total, d.is_wicket,
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

# Load innings totals for each match (include total_overs for time calculation)
innings_totals <- DBI::dbGetQuery(conn, "
  SELECT match_id, innings, total_runs AS innings_total,
         total_wickets AS innings_wickets, total_overs AS innings_overs, batting_team
  FROM cricsheet.match_innings
  WHERE match_id IN (
    SELECT match_id FROM cricsheet.matches
    WHERE LOWER(match_type) IN ('test', 'mdm')
  )
  ORDER BY match_id, innings
")
setDT(innings_totals)

DBI::dbDisconnect(conn, shutdown = TRUE)

# Feature Engineering ----
cli::cli_h2("Engineering features")

# Add match outcome (3-class)
deliveries[, match_outcome := fcase(
  outcome_type == "draw", 1L,  # Draw
  outcome_winner == team1, 0L,  # Team 1 wins
  default = 2L                  # Team 2 wins
)]

# Add: is batting team = team1?
deliveries[, batting_is_team1 := as.integer(batting_team == team1)]

# Calculate cumulative match state
deliveries[, balls_bowled := over * 6L + ball]
deliveries[, wickets_in_hand := 10L - wickets_fallen]
deliveries[, current_run_rate := fifelse(balls_bowled > 0, total_runs / balls_bowled * 6, 0)]

# Pivot innings totals to wide format for lead/deficit calculation
innings_wide <- dcast(innings_totals, match_id ~ paste0("inn", innings),
                      value.var = c("innings_total", "innings_wickets", "batting_team"),
                      fill = NA)

deliveries <- merge(deliveries, innings_wide, by = "match_id", all.x = TRUE)

# Calculate lead/deficit from batting team's perspective
# Positive lead = batting team ahead, negative = behind
deliveries[, `:=`(
  # Completed innings totals for team1 and team2
  team1_completed_runs = fcase(
    innings == 1, 0L,
    innings == 2, innings_total_inn1,
    innings == 3, innings_total_inn1,
    innings == 4, innings_total_inn1 + fifelse(!is.na(innings_total_inn3), innings_total_inn3, 0L)
  ),
  team2_completed_runs = fcase(
    innings == 1, 0L,
    innings == 2, 0L,
    innings == 3, innings_total_inn2,
    innings == 4, innings_total_inn2
  )
)]

# Current lead from team1's perspective
deliveries[, team1_lead := fcase(
  batting_is_team1 == 1, team1_completed_runs + total_runs - team2_completed_runs,
  default = team1_completed_runs - (team2_completed_runs + total_runs)
)]

# How many completed innings so far?
deliveries[, completed_innings := as.integer(innings) - 1L]

# 4th innings specific: target and runs needed
deliveries[innings == 4, `:=`(
  target_runs = team1_completed_runs - team2_completed_runs + 1L,
  runs_needed = (team1_completed_runs - team2_completed_runs + 1L) - total_runs
)]

# ── TIME FEATURES (critical for draw prediction) ──
# Use ACTUAL overs from match_innings for completed innings
innings_overs_wide <- dcast(innings_totals, match_id ~ paste0("overs_inn", innings),
                             value.var = "innings_overs", fill = NA)
deliveries <- merge(deliveries, innings_overs_wide, by = "match_id", all.x = TRUE)

# Cumulative match overs = sum of completed innings overs + current over
deliveries[, prior_completed_overs := fcase(
  innings == 1, 0,
  innings == 2, fifelse(!is.na(overs_inn1), as.double(overs_inn1), 90),
  innings == 3, fifelse(!is.na(overs_inn1), as.double(overs_inn1), 90) +
                fifelse(!is.na(overs_inn2), as.double(overs_inn2), 90),
  innings == 4, fifelse(!is.na(overs_inn1), as.double(overs_inn1), 90) +
                fifelse(!is.na(overs_inn2), as.double(overs_inn2), 90) +
                fifelse(!is.na(overs_inn3), as.double(overs_inn3), 90),
  default = 0
)]

deliveries[, cumulative_match_overs := prior_completed_overs + as.double(over)]

# ~90 overs/day, 5 days = 450 max overs
MAX_TEST_OVERS <- 450
deliveries[, overs_remaining_match := pmax(0, MAX_TEST_OVERS - cumulative_match_overs)]
deliveries[, match_progress := pmin(1, cumulative_match_overs / MAX_TEST_OVERS)]
deliveries[, approx_day := pmin(5L, as.integer(floor(cumulative_match_overs / 90) + 1))]

# ── INNINGS-SPECIFIC STRATEGIC FEATURES ──

# 4th innings: the decider
# runs_needed: how many more runs to win (0 for other innings)
# overs_per_run_needed: can they get the runs in time?
# survive_for_draw: is blocking for a draw viable? (low runs needed + many overs remaining)
deliveries[innings == 4, `:=`(
  target_runs = team1_completed_runs - team2_completed_runs + 1L,
  runs_needed = pmax(0L, (team1_completed_runs - team2_completed_runs + 1L) - total_runs)
)]
deliveries[innings == 4, `:=`(
  runs_per_over_needed = fifelse(overs_remaining_match > 0, as.double(runs_needed) / overs_remaining_match, 10),
  overs_per_wicket = fifelse(wickets_in_hand > 0, overs_remaining_match / wickets_in_hand, 0),
  can_survive_for_draw = as.integer(overs_remaining_match < 60 & wickets_in_hand >= 4)
)]

# 3rd innings: setting a target
# How big is the lead, and how many overs are left for the 4th innings?
deliveries[innings == 3, `:=`(
  overs_for_4th_innings = pmax(0, overs_remaining_match - as.double(over))  # rough estimate
)]

# All innings: relative position
deliveries[, `:=`(
  lead_per_wicket = fifelse(wickets_fallen > 0, as.double(team1_lead) / wickets_fallen, as.double(team1_lead)),
  is_following_on = as.integer(innings == 3 & team1_lead < 0 & batting_is_team1 == 1)
)]

# Phase within innings (Test-specific)
deliveries[, phase := fcase(
  over < 20, "new_ball",
  over < 80, "middle",
  default = "old_ball"
)]

# One-hot encode
deliveries[, `:=`(
  phase_new_ball = as.integer(phase == "new_ball"),
  phase_middle = as.integer(phase == "middle"),
  phase_old_ball = as.integer(phase == "old_ball"),
  gender_male = as.integer(tolower(gender) == "male"),
  innings_1 = as.integer(innings == 1),
  innings_2 = as.integer(innings == 2),
  innings_3 = as.integer(innings == 3),
  innings_4 = as.integer(innings == 4)
)]

# Venue average first innings score
venue_avgs <- deliveries[innings == 1, .(venue_avg = mean(as.numeric(innings_total_inn1), na.rm = TRUE)), by = venue]
deliveries <- merge(deliveries, venue_avgs, by = "venue", all.x = TRUE)
deliveries[is.na(venue_avg), venue_avg := 340]  # global average

cli::cli_alert_success("Features engineered")

# Show outcome distribution
outcome_dist <- deliveries[, .N, by = match_outcome][order(match_outcome)]
cli::cli_alert_info("Match outcomes: team1_win={outcome_dist[match_outcome==0, N]}, draw={outcome_dist[match_outcome==1, N]}, team2_win={outcome_dist[match_outcome==2, N]}")

# Prepare Features ----
cli::cli_h2("Preparing feature matrix")

feature_cols <- c(
  # Current innings state
  "total_runs", "wickets_fallen", "wickets_in_hand",
  "balls_bowled", "current_run_rate",
  # Innings identity
  "innings_1", "innings_2", "innings_3", "innings_4",
  "batting_is_team1",
  # Match state (lead/deficit)
  "team1_lead", "completed_innings", "lead_per_wicket",
  # TIME FEATURES (critical for draw prediction)
  "cumulative_match_overs", "overs_remaining_match", "match_progress", "approx_day",
  # 4th innings chase features
  "target_runs", "runs_needed", "runs_per_over_needed", "overs_per_wicket",
  "can_survive_for_draw",
  # Strategic context
  "is_following_on",
  # Phase
  "phase_new_ball", "phase_middle", "phase_old_ball",
  # Venue/context
  "venue_avg", "gender_male"
)

# Fill NAs for innings-specific features (0 = not applicable in this innings)
for (col in c("target_runs", "runs_needed", "runs_per_over_needed",
              "overs_per_wicket", "can_survive_for_draw",
              "overs_for_4th_innings", "is_following_on")) {
  if (col %in% names(deliveries)) {
    deliveries[is.na(get(col)), (col) := 0]
  }
}

# Clean NAs
for (col in feature_cols) {
  deliveries[is.na(get(col)), (col) := 0]
}

# Train/Test split by season
TEST_SEASONS <- c("2024", "2025", "2023/24", "2024/25")
train_dt <- deliveries[!season %in% TEST_SEASONS]
test_dt <- deliveries[season %in% TEST_SEASONS]

cli::cli_alert_info("Train: {nrow(train_dt)} deliveries ({uniqueN(train_dt$match_id)} matches)")
cli::cli_alert_info("Test: {nrow(test_dt)} deliveries ({uniqueN(test_dt$match_id)} matches)")

y_train <- train_dt$match_outcome
y_test <- test_dt$match_outcome

# Grouped CV folds
set.seed(RANDOM_SEED)
unique_matches <- unique(train_dt$match_id)
shuffled <- sample(unique_matches)
fold_ids <- cut(seq_along(shuffled), breaks = CV_FOLDS, labels = FALSE)
folds <- list()
for (i in 1:CV_FOLDS) {
  fold_matches <- shuffled[fold_ids == i]
  folds[[i]] <- which(train_dt$match_id %in% fold_matches)
}

X_train <- as.matrix(train_dt[, ..feature_cols])
X_test <- as.matrix(test_dt[, ..feature_cols])

dtrain <- xgb.DMatrix(data = X_train, label = y_train)
dtest <- xgb.DMatrix(data = X_test, label = y_test)

cli::cli_alert_success("Matrices created: {ncol(X_train)} features")

# Train Model ----
cli::cli_h2("Training 3-class model")

params <- list(
  objective = "multi:softprob",
  num_class = 3,
  eval_metric = "mlogloss",
  max_depth = 5,
  eta = 0.05,
  subsample = 0.8,
  colsample_bytree = 0.8,
  min_child_weight = 5
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

best_cv_score <- cv_model$evaluation_log$test_mlogloss_mean[best_nrounds]
cli::cli_alert_success("Best iteration: {best_nrounds}, CV mlogloss: {round(best_cv_score, 4)}")

# Train final model
model <- xgb.train(
  params = params,
  data = dtrain,
  nrounds = best_nrounds,
  evals = list(train = dtrain),
  verbose = 0
)

# Evaluate ----
cli::cli_h2("Evaluation")

raw_preds <- predict(model, dtest)
pred_probs <- matrix(raw_preds, ncol = 3, byrow = TRUE)
colnames(pred_probs) <- c("team1_win", "draw", "team2_win")
pred_class <- max.col(pred_probs) - 1

# Overall accuracy
accuracy <- mean(pred_class == y_test)
cli::cli_alert_success("3-class accuracy: {round(accuracy * 100, 1)}%")

# Per-class accuracy
for (cls in 0:2) {
  cls_name <- c("Team 1 Win", "Draw", "Team 2 Win")[cls + 1]
  cls_idx <- y_test == cls
  if (sum(cls_idx) > 0) {
    cls_acc <- mean(pred_class[cls_idx] == cls)
    cls_n <- sum(cls_idx)
    cli::cli_alert_info("  {cls_name}: {round(cls_acc * 100, 1)}% recall ({cls_n} deliveries)")
  }
}

# Multiclass log loss
eps <- 1e-7
mlogloss <- -mean(sapply(seq_along(y_test), function(i) {
  log(max(pred_probs[i, y_test[i] + 1], eps))
}))
cli::cli_alert_success("mlogloss: {round(mlogloss, 4)}")

# Baselines
baseline_random <- -log(1/3)  # 1.099
cli::cli_alert_info("Baseline (random 3-class): {round(baseline_random, 4)}")

# Always predict draw
baseline_draw_acc <- mean(1L == y_test)
cli::cli_alert_info("Baseline (always draw): {round(baseline_draw_acc * 100, 1)}% accuracy")

# ELO-based
baseline_elo_acc <- mean(ifelse(test_dt$team1_lead > 0, 0L,
                                 ifelse(test_dt$team1_lead < 0, 2L, 1L)) == y_test)
cli::cli_alert_info("Baseline (predict by current lead): {round(baseline_elo_acc * 100, 1)}%")

improvement <- (baseline_random - mlogloss) / baseline_random * 100
cli::cli_alert_success("Improvement over random: {round(improvement, 1)}%")

# Per-innings accuracy
cli::cli_h3("Accuracy by innings")
for (inn in 1:4) {
  inn_idx <- test_dt$innings == inn
  if (sum(inn_idx) > 0) {
    inn_acc <- mean(pred_class[inn_idx] == y_test[inn_idx])
    inn_n <- sum(inn_idx)

    # Per-class breakdown for this innings
    inn_probs <- pred_probs[inn_idx, ]
    inn_actual <- y_test[inn_idx]
    inn_mlogloss <- -mean(sapply(seq_along(inn_actual), function(i) {
      log(max(inn_probs[i, inn_actual[i] + 1], eps))
    }))

    cli::cli_alert_info("  Innings {inn}: {round(inn_acc * 100, 1)}% accuracy, mlogloss={round(inn_mlogloss, 4)} ({inn_n} deliveries)")
  }
}

# 4th innings deep dive (most predictable)
cli::cli_h3("4th Innings Performance (Key Metric)")
inn4_idx <- test_dt$innings == 4
if (sum(inn4_idx) > 0) {
  inn4_probs <- pred_probs[inn4_idx, ]
  inn4_actual <- y_test[inn4_idx]
  inn4_pred <- pred_class[inn4_idx]
  inn4_acc <- mean(inn4_pred == inn4_actual)
  inn4_mlogloss <- -mean(sapply(seq_along(inn4_actual), function(i) {
    log(max(inn4_probs[i, inn4_actual[i] + 1], eps))
  }))

  cli::cli_alert_success("4th innings accuracy: {round(inn4_acc * 100, 1)}%")
  cli::cli_alert_success("4th innings mlogloss: {round(inn4_mlogloss, 4)} (random={round(-log(1/3), 4)})")

  # 4th innings by match progress
  inn4_dt <- test_dt[inn4_idx]
  inn4_dt$pred_class <- inn4_pred
  inn4_dt$correct <- inn4_pred == inn4_actual

  for (day in 3:5) {
    day_idx <- inn4_dt$approx_day == day
    if (sum(day_idx) > 10) {
      day_acc <- mean(inn4_dt$correct[day_idx])
      cli::cli_alert_info("  Day {day}: {round(day_acc * 100, 1)}% ({sum(day_idx)} deliveries)")
    }
  }

  # 4th innings draw calibration
  inn4_draw_probs <- inn4_probs[, "draw"]
  inn4_actual_draw <- as.integer(inn4_actual == 1)
  cat("\n  4th innings draw calibration:\n")
  for (threshold in c(0.2, 0.4, 0.6, 0.8)) {
    above <- inn4_draw_probs > threshold
    if (sum(above) > 10) {
      actual_rate <- mean(inn4_actual_draw[above])
      cat(sprintf("    P(draw) > %.0f%%: actual draw rate = %.1f%% (n=%d)\n",
                  threshold * 100, actual_rate * 100, sum(above)))
    }
  }
}

# Feature importance
cli::cli_h3("Feature importance")
importance <- xgb.importance(model = model)
for (i in seq_len(min(10, nrow(importance)))) {
  cli::cli_alert_info("  {i}. {importance$Feature[i]}: {round(importance$Gain[i], 3)}")
}

# Draw probability calibration
cli::cli_h3("Draw probability calibration")
draw_probs <- pred_probs[, "draw"]
actual_draw <- as.integer(y_test == 1)

draw_cal <- data.frame(prob = draw_probs, actual = actual_draw) %>%
  mutate(prob_bin = cut(prob, breaks = c(0, 0.1, 0.2, 0.3, 0.5, 0.7, 1.0), include.lowest = TRUE)) %>%
  group_by(prob_bin) %>%
  summarise(n = n(), mean_pred = mean(prob), actual_rate = mean(actual), .groups = "drop") %>%
  filter(n >= 100)

cat("\nDraw probability calibration:\n")
for (i in seq_len(nrow(draw_cal))) {
  cat(sprintf("  %s: Predicted %.1f%%, Actual %.1f%% (n=%d)\n",
              draw_cal$prob_bin[i], draw_cal$mean_pred[i] * 100,
              draw_cal$actual_rate[i] * 100, draw_cal$n[i]))
}

# Save ----
cli::cli_h2("Saving")

model_path <- file.path(output_dir, "test_win_probability.ubj")
xgb.save(model, model_path)
cli::cli_alert_success("Model saved to {model_path}")

results <- list(
  model = model,
  params = params,
  best_nrounds = best_nrounds,
  feature_cols = feature_cols,
  metrics = list(
    accuracy = accuracy,
    mlogloss = mlogloss,
    best_cv_mlogloss = best_cv_score,
    baseline_random = baseline_random,
    improvement_over_random = improvement
  ),
  importance = importance,
  created_at = Sys.time()
)

results_path <- file.path(output_dir, "test_winprob_results.rds")
saveRDS(results, results_path)
cli::cli_alert_success("Results saved to {results_path}")

cat("\n")
cli::cli_h1("Summary")
cat(sprintf("  3-class accuracy: %.1f%%\n", accuracy * 100))
cat(sprintf("  mlogloss: %.4f (random baseline: %.4f)\n", mlogloss, baseline_random))
cat(sprintf("  Improvement: +%.1f%% over random\n", improvement))
cat(sprintf("  Best rounds: %d\n", best_nrounds))
cat("\n")
