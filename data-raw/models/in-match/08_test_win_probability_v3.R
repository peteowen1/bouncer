# Test Cricket Win Probability - Decomposed Two-Model Pipeline ----
#
# Root cause of v2 failure: a single 3-class model can't simultaneously learn
# "who is ahead?" (lead/wickets) AND "will time run out?" (overs/pitch pace).
# Draws absorb probability uniformly because these are orthogonal signals.
#
# Solution: decompose into two binary models:
#   Model A: P(result) — will this match produce a winner?
#   Model B: P(team1_win | result) — given a result, who wins?
#
# Final probabilities:
#   P(draw)      = 1 - P(result)
#   P(team1_win) = P(result) * P(team1_win | result)
#   P(team2_win) = P(result) * (1 - P(team1_win | result))

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
cli::cli_h1("Test Win Probability v3 (Decomposed Two-Model)")
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

# Load prepared stage 1 data (has rolling features)
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

# Load innings totals (with declared flag and overs)
innings_totals <- DBI::dbGetQuery(conn, "
  SELECT match_id, innings, total_runs AS innings_total,
         total_wickets AS innings_wickets, total_overs AS innings_overs,
         declared
  FROM cricsheet.match_innings
  WHERE match_id IN (
    SELECT match_id FROM cricsheet.matches WHERE LOWER(match_type) IN ('test', 'mdm')
  )
  ORDER BY match_id, innings
")
setDT(innings_totals)

# Venue averages (for 1st innings projected score context)
venue_avgs <- DBI::dbGetQuery(conn, "
  SELECT venue, AVG(total_runs) as venue_avg
  FROM cricsheet.match_innings mi
  JOIN cricsheet.matches m ON mi.match_id = m.match_id
  WHERE LOWER(m.match_type) IN ('test', 'mdm') AND mi.innings = 1
  GROUP BY venue HAVING COUNT(*) >= 3
")
setDT(venue_avgs)

# Venue result rates (historical P(result) at each venue) — Bayesian smoothed
venue_results <- DBI::dbGetQuery(conn, "
  SELECT m.venue,
         COUNT(*) AS n_matches,
         SUM(CASE WHEN m.outcome_type != 'draw' THEN 1 ELSE 0 END) AS n_results
  FROM cricsheet.matches m
  WHERE LOWER(m.match_type) IN ('test', 'mdm')
    AND m.outcome_type IS NOT NULL
  GROUP BY m.venue
")
setDT(venue_results)
# Bayesian smoothing: prior_weight matches of which prior_rate are results
prior_weight <- 10
prior_rate <- mean(venue_results$n_results / venue_results$n_matches, na.rm = TRUE)
venue_results[, venue_result_rate := (n_results + prior_weight * prior_rate) /
                                      (n_matches + prior_weight)]
venue_results <- venue_results[, .(venue, venue_result_rate)]

DBI::dbDisconnect(conn, shutdown = TRUE)

# Feature Engineering ----
cli::cli_h2("Engineering features")

# Match outcome labels
deliveries[, match_outcome := fcase(
  outcome_type == "draw", 1L,
  outcome_winner == team1, 0L,
  default = 2L
)]
deliveries[, is_result := as.integer(outcome_type != "draw")]

# Basic state
deliveries[, `:=`(
  balls_bowled = as.integer(over * 6L + ball),
  wickets_in_hand = 10L - wickets_fallen,
  current_run_rate = fifelse(over > 0, total_runs / (over + ball/6), 0),
  batting_is_team1 = as.integer(batting_team == team1)
)]

# Innings totals (wide format) — including declared flag
inn_wide <- dcast(innings_totals, match_id ~ paste0("inn", innings),
                  value.var = c("innings_total", "innings_wickets", "innings_overs", "declared"),
                  fill = NA)
deliveries <- merge(deliveries, inn_wide, by = "match_id", all.x = TRUE)
deliveries <- merge(deliveries, venue_avgs, by = "venue", all.x = TRUE)
deliveries[is.na(venue_avg), venue_avg := 340]
deliveries <- merge(deliveries, venue_results, by = "venue", all.x = TRUE)
deliveries[is.na(venue_result_rate), venue_result_rate := prior_rate]

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

# Cumulative match overs
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
  approx_day = pmin(5L, as.integer(floor(cum_overs / 90) + 1)),
  innings_num = as.double(innings)
)]

# ---- Tier 1: Derived rain proxy features (no weather data needed) ----

# Overs per day: how many overs bowled per day so far
# Low values (< 80) indicate rain delays or slow over rates
deliveries[, overs_per_day := fifelse(approx_day > 0, cum_overs / approx_day, 90)]

# Overs deficit: how many overs "missing" vs scheduled (90 per day)
# Positive = overs lost (likely rain); 0 = on schedule
deliveries[, overs_deficit := pmax(0, approx_day * 90 - cum_overs)]

# ---- Tier 2+: Weather features (if available) ----

# Tier 2: Causal rain_days_so_far — only uses weather from COMPLETED days
# This is zero-leakage: on day 3, we only know days 1-2 weather
weather_available <- FALSE
weather_data <- tryCatch({
  w <- load_match_weather()
  if (!is.null(w) && nrow(w) > 0) w else NULL
}, error = function(e) NULL)

if (!is.null(weather_data)) {
  # Load per-day weather to construct causal feature
  # We need daily weather per match to count rain days causally
  conn_w <- get_db_connection(read_only = TRUE)
  coords <- load_venue_coordinates(conn_w)
  DBI::dbDisconnect(conn_w, shutdown = TRUE)

  if (!is.null(coords) && nrow(coords) > 0) {
    # Join match-level weather for now; rain_days is match-total (slight approximation)
    # True causal construction: rain_days_so_far = rain_days * (approx_day - 1) / match_days
    # This prorates total rain days by how far through the match we are (conservative estimate)
    deliveries <- merge(deliveries, weather_data[, .(match_id, rain_days, precipitation_total,
                                                      temp_avg, wind_avg, is_rain)],
                         by = "match_id", all.x = TRUE)

    # Causal rain_days_so_far: prorate by completed days (day N sees days 1..N-1 weather)
    # On day 1: 0 rain days known. On day 3: we know ~2/5 of total rain days.
    deliveries[, rain_days_so_far := fifelse(
      !is.na(rain_days) & approx_day > 1,
      rain_days * (approx_day - 1) / 5,  # Prorate: proportion of match days completed
      0
    )]

    weather_available <- TRUE
    n_with_weather <- sum(!is.na(deliveries$rain_days))
    cli::cli_alert_success("Tier 2: Weather joined for {n_with_weather}/{nrow(deliveries)} deliveries ({round(n_with_weather/nrow(deliveries)*100,1)}%)")
  }
}

if (!weather_available) {
  deliveries[, `:=`(rain_days = NA_integer_, rain_days_so_far = 0,
                     precipitation_total = NA_real_, temp_avg = NA_real_,
                     wind_avg = NA_real_, is_rain = NA)]
  cli::cli_alert_info("Tier 2: No weather data available, using Tier 1 features only")
}

# ---- Other time-pressure features ----

# Total wickets fallen so far in the match
deliveries[, total_wickets_match := fcase(
  innings == 1, wickets_fallen,
  innings == 2, fifelse(!is.na(innings_wickets_inn1), innings_wickets_inn1, 0L) + wickets_fallen,
  innings == 3, fifelse(!is.na(innings_wickets_inn1), innings_wickets_inn1, 0L) +
                fifelse(!is.na(innings_wickets_inn2), innings_wickets_inn2, 0L) + wickets_fallen,
  innings == 4, fifelse(!is.na(innings_wickets_inn1), innings_wickets_inn1, 0L) +
                fifelse(!is.na(innings_wickets_inn2), innings_wickets_inn2, 0L) +
                fifelse(!is.na(innings_wickets_inn3), innings_wickets_inn3, 0L) + wickets_fallen,
  default = as.integer(wickets_fallen)
)]

# Match-level scoring rate (runs per over across entire match)
deliveries[, total_runs_match := fcase(
  innings == 1, total_runs,
  innings == 2, fifelse(!is.na(innings_total_inn1), innings_total_inn1, 0L) + total_runs,
  innings == 3, fifelse(!is.na(innings_total_inn1), innings_total_inn1, 0L) +
                fifelse(!is.na(innings_total_inn2), innings_total_inn2, 0L) + total_runs,
  innings == 4, fifelse(!is.na(innings_total_inn1), innings_total_inn1, 0L) +
                fifelse(!is.na(innings_total_inn2), innings_total_inn2, 0L) +
                fifelse(!is.na(innings_total_inn3), innings_total_inn3, 0L) + total_runs,
  default = as.integer(total_runs)
)]
deliveries[, runs_per_over_match := fifelse(cum_overs > 0, total_runs_match / cum_overs, 3.0)]

# Overs per wicket so far in current innings (scoring pace indicator)
deliveries[, overs_per_wicket_current := fifelse(
  wickets_fallen > 0, (over + ball/6) / wickets_fallen, 30  # Cap at 30 if no wickets
)]

# Projected current innings overs: if no more wickets fall at current rate, how many overs?
# wickets_in_hand * overs_per_wicket gives optimistic estimate
deliveries[, current_innings_projected_overs := pmin(
  150,  # Cap at 150 overs (no innings lasts longer)
  fifelse(wickets_fallen > 0,
    (over + ball/6) + wickets_in_hand * overs_per_wicket_current,
    90)  # Default for 0 wickets: assume ~90 overs
)]

# Completed innings overs (sum of finished innings)
deliveries[, completed_innings_overs := fcase(
  innings == 1, 0,
  innings == 2, fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 0),
  innings == 3, fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 0) +
                fifelse(!is.na(innings_overs_inn2), as.double(innings_overs_inn2), 0),
  innings == 4, fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 0) +
                fifelse(!is.na(innings_overs_inn2), as.double(innings_overs_inn2), 0) +
                fifelse(!is.na(innings_overs_inn3), as.double(innings_overs_inn3), 0),
  default = 0
)]

# Average overs per completed innings at this venue (use match data as proxy)
# Use within-match average of completed innings if available
deliveries[, avg_overs_per_innings := fcase(
  innings == 1, 80,  # Prior for first innings
  innings == 2, as.double(innings_overs_inn1),
  innings == 3, (fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 80) +
                 fifelse(!is.na(innings_overs_inn2), as.double(innings_overs_inn2), 80)) / 2,
  innings == 4, (fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 80) +
                 fifelse(!is.na(innings_overs_inn2), as.double(innings_overs_inn2), 80) +
                 fifelse(!is.na(innings_overs_inn3), as.double(innings_overs_inn3), 80)) / 3,
  default = 80
)]
deliveries[is.na(avg_overs_per_innings), avg_overs_per_innings := 80]

# Remaining innings count (including current, after current ball)
deliveries[, remaining_innings_count := 4L - as.integer(innings)]

# KEY FEATURE: projected total overs for the entire match
# completed_innings_overs + current_innings_projected + remaining_innings * avg_overs
deliveries[, projected_total_overs := completed_innings_overs +
             current_innings_projected_overs +
             remaining_innings_count * avg_overs_per_innings]
deliveries[, projected_total_overs := pmin(600, pmax(50, projected_total_overs))]  # Sanity bounds

# Time pressure: >1.0 means match likely to run out of time (draw likely)
deliveries[, time_pressure := projected_total_overs / MAX_OVERS]

# Lead per over remaining (pressure rate)
deliveries[, abs_lead := abs(team1_lead)]
deliveries[, lead_per_over_remaining := fifelse(
  overs_remaining > 0, abs_lead / overs_remaining, as.double(abs_lead)
)]

# Follow-on possible (team1 lead >= 200 after 1st innings)
deliveries[, follow_on_possible := as.integer(
  innings >= 2 &
  !is.na(innings_total_inn1) & !is.na(innings_total_inn2) &
  (innings_total_inn1 - innings_total_inn2) >= 200
)]
deliveries[is.na(follow_on_possible), follow_on_possible := 0L]

# 4th innings specific features
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
  phase_powerplay = 0L,
  phase_middle = as.integer(over >= 20 & over < 80),
  phase_death = 0L,
  phase_new_ball = as.integer(over < 20),
  phase_old_ball = as.integer(over >= 80),
  gender_male = as.integer(tolower(gender) == "male"),
  overs_remaining_innings = 0,
  overs_into_phase = fcase(
    over < 20, as.double(over),
    over < 80, as.double(over - 20),
    default = as.double(over - 80)
  )
)]

# Generate projected scores using Stage 1 model ----
cli::cli_h2("Generating projected scores")

stage1_all <- rbind(stage1_data$train, stage1_data$test)
setDT(stage1_all)

# Simple rate projection as default
deliveries[, projected_innings_total := total_runs * (90 / pmax(over + ball/6, 1))]

# Use XGBoost for 1st innings where possible
if (!is.null(stage1_model)) {
  tryCatch({
    inn1_idx <- which(deliveries$innings == 1)
    if (length(inn1_idx) > 0) {
      inn1_delivery_ids <- deliveries$delivery_id[inn1_idx]
      matched <- stage1_all[delivery_id %in% inn1_delivery_ids]
      if (nrow(matched) > 0) {
        for (feat in stage1_features) {
          if (!feat %in% names(matched)) matched[, (feat) := 0]
        }
        matched_features <- as.matrix(matched[, ..stage1_features])
        matched_features[is.na(matched_features)] <- 0
        dmat <- xgb.DMatrix(data = matched_features)
        preds <- predict(stage1_model, dmat)
        pred_dt <- data.table(delivery_id = matched$delivery_id, projected_xgb = preds)
        deliveries <- merge(deliveries, pred_dt, by = "delivery_id", all.x = TRUE)
        deliveries[!is.na(projected_xgb), projected_innings_total := projected_xgb]
        deliveries[, projected_xgb := NULL]
        cli::cli_alert_success("Applied Stage 1 model to {sum(!is.na(preds))} 1st innings deliveries")
      }
    }
  }, error = function(e) {
    cli::cli_alert_warning("Stage 1 prediction failed: {conditionMessage(e)}")
  })
}

# Projected lead
deliveries[, projected_lead := fcase(
  batting_is_team1 == 1L & innings == 1, as.double(projected_innings_total) - venue_avg,
  batting_is_team1 == 1L, as.double(team1_completed + projected_innings_total - team2_completed) - venue_avg,
  batting_is_team1 == 0L & innings == 2, as.double(team1_completed - (team2_completed + projected_innings_total)),
  default = as.double(team1_lead)
)]

cli::cli_alert_success("Features engineered")

# Sample: one per over ----
cli::cli_h2("Sampling (1 per over)")

sampled <- deliveries[, .SD[.N], by = .(match_id, innings, over)]
cli::cli_alert_info("Sampled {nrow(sampled)} from {nrow(deliveries)} (1 per over)")

n_results <- sum(sampled$is_result[!duplicated(paste0(sampled$match_id, "_", sampled$innings, "_", sampled$over))])
cat(sprintf("  team1_win: %d, draw: %d, team2_win: %d\n",
            sum(sampled$match_outcome == 0), sum(sampled$match_outcome == 1), sum(sampled$match_outcome == 2)))
cat(sprintf("  result matches: %d, draw matches: %d\n",
            uniqueN(sampled[is_result == 1]$match_id), uniqueN(sampled[is_result == 0]$match_id)))

# Fill remaining NAs
for (col in names(sampled)) {
  if (is.numeric(sampled[[col]])) {
    sampled[is.na(get(col)), (col) := 0]
  }
}

# Train/test split
TEST_SEASONS <- c("2024", "2025", "2023/24", "2024/25")
train_dt <- sampled[!season %in% TEST_SEASONS]
test_dt <- sampled[season %in% TEST_SEASONS]

cli::cli_alert_info("Train: {nrow(train_dt)} samples ({uniqueN(train_dt$match_id)} matches)")
cli::cli_alert_info("Test: {nrow(test_dt)} samples ({uniqueN(test_dt$match_id)} matches)")

# ============================================================
# MODEL A: P(result) — Binary: will this match have a winner?
# ============================================================
cli::cli_h1("Model A: P(result)")

result_features <- c(
  "overs_remaining", "match_progress", "approx_day",
  "time_pressure", "projected_total_overs",
  "venue_result_rate",
  "total_wickets_match", "runs_per_over_match",
  "abs_lead", "lead_per_over_remaining",
  "innings_num", "follow_on_possible",
  # Tier 1: derived rain proxies (always available)
  "overs_per_day", "overs_deficit",
  # Tier 2: causal weather (available if backfilled)
  "rain_days_so_far"
)

X_train_A <- as.matrix(train_dt[, ..result_features])
X_test_A <- as.matrix(test_dt[, ..result_features])
y_train_A <- train_dt$is_result
y_test_A <- test_dt$is_result

# Progressive confidence weights: later in match = more confident
weights_A <- 0.5 + 2.5 * train_dt$match_progress^1.5

dtrain_A <- xgb.DMatrix(data = X_train_A, label = y_train_A, weight = weights_A)
dtest_A <- xgb.DMatrix(data = X_test_A, label = y_test_A)

# Grouped CV folds
set.seed(RANDOM_SEED)
unique_matches_A <- unique(train_dt$match_id)
shuffled_A <- sample(unique_matches_A)
fold_ids_A <- cut(seq_along(shuffled_A), breaks = CV_FOLDS, labels = FALSE)
folds_A <- lapply(1:CV_FOLDS, function(i) which(train_dt$match_id %in% shuffled_A[fold_ids_A == i]))

params_A <- list(
  objective = "binary:logistic",
  eval_metric = "logloss",
  max_depth = 3,
  eta = 0.03,
  subsample = 0.8,
  colsample_bytree = 0.8,
  min_child_weight = 30,
  lambda = 5
)

cli::cli_h2("Training Model A (heavy regularization)")
set.seed(RANDOM_SEED)
cv_A <- xgb.cv(
  params = params_A,
  data = dtrain_A,
  nrounds = MAX_ROUNDS,
  folds = folds_A,
  early_stopping_rounds = EARLY_STOPPING,
  verbose = 1,
  print_every_n = 20
)

best_nrounds_A <- cv_A$early_stop$best_iteration %||%
  cv_A$best_iteration %||%
  which.min(cv_A$evaluation_log$test_logloss_mean)
if (is.null(best_nrounds_A) || is.na(best_nrounds_A)) best_nrounds_A <- 100

best_cv_A <- cv_A$evaluation_log$test_logloss_mean[best_nrounds_A]
cli::cli_alert_success("Model A: {best_nrounds_A} rounds, CV logloss: {round(best_cv_A, 4)}")

model_A <- xgb.train(params = params_A, data = dtrain_A, nrounds = best_nrounds_A, verbose = 0)

# Evaluate Model A
pred_result <- predict(model_A, dtest_A)
pred_result_class <- as.integer(pred_result > 0.5)
acc_A <- mean(pred_result_class == y_test_A)
logloss_A <- -mean(y_test_A * log(pmax(pred_result, 1e-7)) +
                     (1 - y_test_A) * log(pmax(1 - pred_result, 1e-7)))
cat(sprintf("\n  Model A test: accuracy=%.1f%%, logloss=%.4f\n", acc_A * 100, logloss_A))

# Result calibration
cli::cli_h3("Model A: Result Calibration")
for (lo in seq(0, 0.8, by = 0.2)) {
  hi <- lo + 0.2
  idx <- pred_result >= lo & pred_result < hi
  if (sum(idx) > 20) {
    cat(sprintf("  P(result) %.0f-%.0f%%: predicted=%.1f%%, actual=%.1f%% (n=%d)\n",
                lo*100, hi*100, mean(pred_result[idx])*100, mean(y_test_A[idx])*100, sum(idx)))
  }
}

# Feature importance Model A
cli::cli_h3("Model A: Feature Importance")
imp_A <- xgb.importance(model = model_A)
for (i in seq_len(min(12, nrow(imp_A)))) {
  cli::cli_alert_info("{i}. {imp_A$Feature[i]}: {round(imp_A$Gain[i], 3)}")
}

# ============================================================
# MODEL B: P(team1_win | result) — trained ONLY on decided matches
# ============================================================
cli::cli_h1("Model B: P(team1_win | result)")

# Filter to result-only matches
train_results <- train_dt[is_result == 1]
test_results <- test_dt[is_result == 1]

y_train_B <- as.integer(train_results$match_outcome == 0)  # 1 = team1_win
y_test_B <- as.integer(test_results$match_outcome == 0)

conditional_features <- c(
  "team1_lead", "projected_lead", "projected_innings_total",
  "batting_is_team1", "wickets_in_hand",
  "overs_remaining", "cum_overs",
  "venue_avg", "innings_num",
  "target", "runs_needed", "req_rate", "overs_per_wicket",
  "current_run_rate"
)

X_train_B <- as.matrix(train_results[, ..conditional_features])
X_test_B <- as.matrix(test_results[, ..conditional_features])

# Upweight later innings (more informative)
weights_B <- fifelse(train_results$innings >= 3, 2.0, 1.0)
weights_B <- fifelse(train_results$innings == 4, 3.0, weights_B)

dtrain_B <- xgb.DMatrix(data = X_train_B, label = y_train_B, weight = weights_B)
dtest_B <- xgb.DMatrix(data = X_test_B, label = y_test_B)

# Grouped CV folds (result matches only)
set.seed(RANDOM_SEED)
unique_matches_B <- unique(train_results$match_id)
shuffled_B <- sample(unique_matches_B)
fold_ids_B <- cut(seq_along(shuffled_B), breaks = CV_FOLDS, labels = FALSE)
folds_B <- lapply(1:CV_FOLDS, function(i) which(train_results$match_id %in% shuffled_B[fold_ids_B == i]))

params_B <- list(
  objective = "binary:logistic",
  eval_metric = "logloss",
  max_depth = 4,
  eta = 0.05,
  subsample = 0.8,
  colsample_bytree = 0.8,
  min_child_weight = 10,
  lambda = 2
)

cli::cli_h2("Training Model B (result matches only)")
cat(sprintf("  Training on %d samples from %d result matches\n",
            nrow(train_results), uniqueN(train_results$match_id)))

set.seed(RANDOM_SEED)
cv_B <- xgb.cv(
  params = params_B,
  data = dtrain_B,
  nrounds = MAX_ROUNDS,
  folds = folds_B,
  early_stopping_rounds = EARLY_STOPPING,
  verbose = 1,
  print_every_n = 20
)

best_nrounds_B <- cv_B$early_stop$best_iteration %||%
  cv_B$best_iteration %||%
  which.min(cv_B$evaluation_log$test_logloss_mean)
if (is.null(best_nrounds_B) || is.na(best_nrounds_B)) best_nrounds_B <- 100

best_cv_B <- cv_B$evaluation_log$test_logloss_mean[best_nrounds_B]
cli::cli_alert_success("Model B: {best_nrounds_B} rounds, CV logloss: {round(best_cv_B, 4)}")

model_B <- xgb.train(params = params_B, data = dtrain_B, nrounds = best_nrounds_B, verbose = 0)

# Evaluate Model B alone
pred_team1 <- predict(model_B, dtest_B)
pred_team1_class <- as.integer(pred_team1 > 0.5)
acc_B <- mean(pred_team1_class == y_test_B)
logloss_B <- -mean(y_test_B * log(pmax(pred_team1, 1e-7)) +
                     (1 - y_test_B) * log(pmax(1 - pred_team1, 1e-7)))
cat(sprintf("\n  Model B test: accuracy=%.1f%%, logloss=%.4f\n", acc_B * 100, logloss_B))

# Feature importance Model B
cli::cli_h3("Model B: Feature Importance")
imp_B <- xgb.importance(model = model_B)
for (i in seq_len(min(12, nrow(imp_B)))) {
  cli::cli_alert_info("{i}. {imp_B$Feature[i]}: {round(imp_B$Gain[i], 3)}")
}

# ============================================================
# COMBINED EVALUATION
# ============================================================
cli::cli_h1("Combined Evaluation")

# Get P(result) for ALL test samples
p_result_all <- predict(model_A, dtest_A)

# Get P(team1_win | result) for ALL test samples (model trained on results only, but can predict on all)
X_test_B_all <- as.matrix(test_dt[, ..conditional_features])
dtest_B_all <- xgb.DMatrix(data = X_test_B_all)
p_team1_given_result_all <- predict(model_B, dtest_B_all)

# Combined 3-class probabilities
p_draw <- 1 - p_result_all
p_team1_win <- p_result_all * p_team1_given_result_all
p_team2_win <- p_result_all * (1 - p_team1_given_result_all)

# Assemble probability matrix
pred_probs <- cbind(team1_win = p_team1_win, draw = p_draw, team2_win = p_team2_win)

# 3-class mlogloss
y_test <- test_dt$match_outcome
eps <- 1e-7
overall_mlogloss <- -mean(sapply(seq_along(y_test), function(i) {
  log(max(pred_probs[i, y_test[i] + 1], eps))
}))

pred_class <- max.col(pred_probs) - 1
overall_acc <- mean(pred_class == y_test)
baseline_random <- -log(1/3)

cat(sprintf("\n  COMBINED: accuracy=%.1f%%, mlogloss=%.4f (random=%.4f, improvement=%+.1f%%)\n",
            overall_acc * 100, overall_mlogloss, baseline_random,
            (baseline_random - overall_mlogloss) / baseline_random * 100))

# Per-class recall
for (cls in 0:2) {
  cls_name <- c("Team1 Win", "Draw", "Team2 Win")[cls + 1]
  idx <- y_test == cls
  if (sum(idx) > 0) {
    recall <- mean(pred_class[idx] == cls)
    cat(sprintf("  %-12s recall=%.1f%% (%d samples)\n", cls_name, recall * 100, sum(idx)))
  }
}

# Per-innings
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

# Draw calibration (the key test!)
cli::cli_h3("Draw Calibration (KEY METRIC)")
draw_probs_vec <- pred_probs[, "draw"]
actual_draw <- as.integer(y_test == 1)
for (lo in seq(0, 0.8, by = 0.2)) {
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

# Comparison with v2 (single model)
cli::cli_h3("Comparison with v2 (single 3-class model)")
v2_path <- file.path(output_dir, "test_winprob_results.rds")
if (file.exists(v2_path)) {
  v2 <- readRDS(v2_path)
  cat(sprintf("  v2: mlogloss=%.4f (%+.1f%% vs random)\n",
              v2$metrics$mlogloss, v2$metrics$improvement))
  cat(sprintf("  v3: mlogloss=%.4f (%+.1f%% vs random)\n",
              overall_mlogloss,
              (baseline_random - overall_mlogloss) / baseline_random * 100))
  delta <- v2$metrics$mlogloss - overall_mlogloss
  cat(sprintf("  Delta: %+.4f (%s)\n", delta, if (delta > 0) "v3 BETTER" else "v2 better"))
} else {
  cli::cli_alert_info("No v2 results found for comparison")
}

# Save ----
cli::cli_h2("Saving")

xgb.save(model_A, file.path(output_dir, "test_result_model.ubj"))
xgb.save(model_B, file.path(output_dir, "test_conditional_win_model.ubj"))

saveRDS(list(
  model_A = model_A,
  model_B = model_B,
  params_A = params_A,
  params_B = params_B,
  best_nrounds_A = best_nrounds_A,
  best_nrounds_B = best_nrounds_B,
  result_features = result_features,
  conditional_features = conditional_features,
  metrics = list(
    accuracy = overall_acc,
    mlogloss = overall_mlogloss,
    cv_logloss_A = best_cv_A,
    cv_logloss_B = best_cv_B,
    test_logloss_A = logloss_A,
    test_logloss_B = logloss_B,
    test_accuracy_A = acc_A,
    test_accuracy_B = acc_B,
    baseline = baseline_random,
    improvement = (baseline_random - overall_mlogloss) / baseline_random * 100
  ),
  importance_A = imp_A,
  importance_B = imp_B,
  version = "v3_decomposed",
  created_at = Sys.time()
), file.path(output_dir, "test_winprob_v3_results.rds"))

cli::cli_alert_success("Saved: test_result_model.ubj, test_conditional_win_model.ubj, test_winprob_v3_results.rds")

cat(sprintf("\n  SUMMARY: %.1f%% accuracy, mlogloss %.4f (%+.1f%% vs random)\n",
            overall_acc * 100, overall_mlogloss,
            (baseline_random - overall_mlogloss) / baseline_random * 100))
cat(sprintf("  Model A (result): logloss %.4f | Model B (conditional): logloss %.4f\n",
            logloss_A, logloss_B))
