# 06 Validate 3-Way ELO System ----
#
# This script validates the 3-way ELO predictions against actual outcomes
# using IPL data as the primary benchmark.
#
# Metrics:
#   - Brier score for wicket predictions
#   - RMSE for run predictions
#   - Calibration curves (predicted vs actual by bucket)
#   - Comparison against naive baseline (format average)
#
# Prerequisites:
#   - Run 04_calculate_3way_elo.R first to populate 3-way ELO tables
#   - 07_calculate_anchor_exposure.R for calibration scores (optional)

# 1. Setup ----
library(DBI)
library(data.table)
devtools::load_all()

# 2. Configuration ----
FORMAT <- "t20"                    # Format to validate
VALIDATION_EVENTS <- c(            # Events to use for validation
 "Indian Premier League"          # Focus on IPL for high-quality data
)
USE_BLENDING <- TRUE               # Apply sample-size blending to ELOs
VALIDATION_YEARS <- c(2023, 2024, 2025)  # Recent seasons

cat("\n")
cli::cli_h1("3-Way ELO Validation")
cli::cli_alert_info("Format: {toupper(FORMAT)}")
cli::cli_alert_info("Events: {paste(VALIDATION_EVENTS, collapse = ', ')}")
cli::cli_alert_info("Use blending: {USE_BLENDING}")
cat("\n")

# 3. Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cli::cli_alert_success("Connected to database")

# 4. Load Validation Data ----
cli::cli_h2("Loading validation data")

# Build event filter
event_filter <- paste(sprintf("'%s'", VALIDATION_EVENTS), collapse = ", ")
year_filter <- paste(VALIDATION_YEARS, collapse = ", ")

# Query 3-way ELO table with actual outcomes
query <- sprintf("
 SELECT
   e.delivery_id,
   e.match_id,
   e.match_date,
   e.batter_id,
   e.bowler_id,
   e.venue,
   e.phase,
   -- Raw ELOs (before this delivery)
   e.batter_run_elo_before,
   e.batter_wicket_elo_before,
   e.bowler_run_elo_before,
   e.bowler_wicket_elo_before,
   e.venue_perm_run_elo_before,
   e.venue_perm_wicket_elo_before,
   e.venue_session_run_elo_before,
   e.venue_session_wicket_elo_before,
   -- Actual outcomes
   e.actual_runs,
   e.is_wicket,
   -- Player experience (for blending)
   e.batter_balls,
   e.bowler_balls,
   -- Expected values (from model)
   e.exp_runs,
   e.exp_wicket,
   -- Match info
   m.event_name
 FROM %s_3way_elo e
 JOIN cricsheet.matches m ON e.match_id = m.match_id
 WHERE m.event_name IN (%s)
   AND EXTRACT(YEAR FROM e.match_date) IN (%s)
 ORDER BY e.match_date, e.match_id, e.delivery_id
", FORMAT, event_filter, year_filter)

validation_data <- DBI::dbGetQuery(conn, query)
setDT(validation_data)

n_deliveries <- nrow(validation_data)
n_matches <- uniqueN(validation_data$match_id)

cli::cli_alert_success("Loaded {format(n_deliveries, big.mark = ',')} deliveries from {n_matches} matches")

if (n_deliveries == 0) {
 cli::cli_alert_danger("No data found! Check that 3-way ELO tables are populated.")
 stop("No validation data available")
}

# 5. Get Player Calibration Scores (Optional) ----
cli::cli_h2("Loading calibration scores")

calibration_table <- sprintf("%s_player_calibration", FORMAT)
has_calibration <- calibration_table %in% DBI::dbListTables(conn)

if (has_calibration) {
 calibration_data <- DBI::dbGetQuery(conn, sprintf("
   SELECT player_id, calibration_score
   FROM %s
 ", calibration_table))
 setDT(calibration_data)
 cli::cli_alert_success("Loaded {nrow(calibration_data)} player calibration scores")
} else {
 cli::cli_alert_warning("No calibration table found - will skip calibration analysis")
 calibration_data <- NULL
}

# 6. Apply Sample-Size Blending (if enabled) ----
cli::cli_h2("Calculating predictions")

if (USE_BLENDING) {
 cli::cli_alert_info("Applying sample-size blending to ELOs...")

 # Blend batter ELOs
 validation_data[, batter_run_elo_blended := blend_elo_with_replacement(
   batter_run_elo_before, batter_balls
 ), by = seq_len(nrow(validation_data))]

 validation_data[, batter_wicket_elo_blended := blend_elo_with_replacement(
   batter_wicket_elo_before, batter_balls
 ), by = seq_len(nrow(validation_data))]

 # Blend bowler ELOs
 validation_data[, bowler_run_elo_blended := blend_elo_with_replacement(
   bowler_run_elo_before, bowler_balls
 ), by = seq_len(nrow(validation_data))]

 validation_data[, bowler_wicket_elo_blended := blend_elo_with_replacement(
   bowler_wicket_elo_before, bowler_balls
 ), by = seq_len(nrow(validation_data))]

 # Use blended ELOs for predictions
 validation_data[, batter_run_elo := batter_run_elo_blended]
 validation_data[, batter_wicket_elo := batter_wicket_elo_blended]
 validation_data[, bowler_run_elo := bowler_run_elo_blended]
 validation_data[, bowler_wicket_elo := bowler_wicket_elo_blended]

 cli::cli_alert_success("Applied blending")
} else {
 validation_data[, batter_run_elo := batter_run_elo_before]
 validation_data[, batter_wicket_elo := batter_wicket_elo_before]
 validation_data[, bowler_run_elo := bowler_run_elo_before]
 validation_data[, bowler_wicket_elo := bowler_wicket_elo_before]
}

# Recalculate expected values with (possibly blended) ELOs
# Using the same formulas as the 3-way ELO system
mean_runs <- switch(FORMAT,
 "t20" = EXPECTED_RUNS_T20,
 "odi" = EXPECTED_RUNS_ODI,
 "test" = EXPECTED_RUNS_TEST,
 EXPECTED_RUNS_T20
)
mean_wicket <- switch(FORMAT,
 "t20" = EXPECTED_WICKET_T20,
 "odi" = EXPECTED_WICKET_ODI,
 "test" = EXPECTED_WICKET_TEST,
 EXPECTED_WICKET_T20
)

runs_per_elo <- switch(FORMAT,
 "t20" = THREE_WAY_RUNS_PER_100_ELO_POINTS_T20 / 100,
 "odi" = THREE_WAY_RUNS_PER_100_ELO_POINTS_ODI / 100,
 "test" = THREE_WAY_RUNS_PER_100_ELO_POINTS_TEST / 100,
 THREE_WAY_RUNS_PER_100_ELO_POINTS_T20 / 100
)

# Calculate predicted runs (model predictions)
validation_data[, pred_runs := {
 batter_contrib <- (batter_run_elo - THREE_WAY_ELO_START) * runs_per_elo
 bowler_contrib <- (THREE_WAY_ELO_START - bowler_run_elo) * runs_per_elo
 venue_perm_contrib <- (venue_perm_run_elo_before - THREE_WAY_ELO_START) * runs_per_elo
 venue_session_contrib <- (venue_session_run_elo_before - THREE_WAY_ELO_START) * runs_per_elo

 exp_r <- mean_runs +
   THREE_WAY_W_BATTER * batter_contrib +
   THREE_WAY_W_BOWLER * bowler_contrib +
   THREE_WAY_W_VENUE_PERM * venue_perm_contrib +
   THREE_WAY_W_VENUE_SESSION * venue_session_contrib

 pmax(0, pmin(6, exp_r))
}]

# Calculate predicted wicket probability
validation_data[, pred_wicket := {
 base_logit <- log(mean_wicket / (1 - mean_wicket))

 adj_logit <- base_logit -
   THREE_WAY_W_BATTER * (batter_wicket_elo - THREE_WAY_ELO_START) / THREE_WAY_ELO_DIVISOR +
   THREE_WAY_W_BOWLER * (bowler_wicket_elo - THREE_WAY_ELO_START) / THREE_WAY_ELO_DIVISOR +
   THREE_WAY_W_VENUE_PERM * (venue_perm_wicket_elo_before - THREE_WAY_ELO_START) / THREE_WAY_ELO_DIVISOR +
   THREE_WAY_W_VENUE_SESSION * (venue_session_wicket_elo_before - THREE_WAY_ELO_START) / THREE_WAY_ELO_DIVISOR

 1 / (1 + exp(-adj_logit))
}]

# Clamp predictions
validation_data[, pred_wicket := pmax(0.001, pmin(0.5, pred_wicket))]

# 7. Calculate Metrics ----
cli::cli_h2("Calculating validation metrics")

# 7a. Wicket Metrics ----
actual_wicket <- as.integer(validation_data$is_wicket)
pred_wicket <- validation_data$pred_wicket

# Brier score (lower = better)
brier_score <- mean((pred_wicket - actual_wicket)^2)

# Log loss (lower = better)
eps <- 1e-15
pred_wicket_clipped <- pmax(eps, pmin(1 - eps, pred_wicket))
log_loss <- -mean(
 actual_wicket * log(pred_wicket_clipped) +
 (1 - actual_wicket) * log(1 - pred_wicket_clipped)
)

# Baseline: naive format average
baseline_wicket <- mean(actual_wicket)
brier_baseline <- mean((baseline_wicket - actual_wicket)^2)
log_loss_baseline <- -mean(
 actual_wicket * log(baseline_wicket) +
 (1 - actual_wicket) * log(1 - baseline_wicket)
)

# Improvement
brier_improvement <- (brier_baseline - brier_score) / brier_baseline * 100
log_loss_improvement <- (log_loss_baseline - log_loss) / log_loss_baseline * 100

cat("\n--- Wicket Prediction Metrics ---\n")
cat(sprintf("Baseline wicket rate: %.2f%%\n", baseline_wicket * 100))
cat(sprintf("Brier Score:    %.6f (baseline: %.6f, improvement: %+.2f%%)\n",
           brier_score, brier_baseline, brier_improvement))
cat(sprintf("Log Loss:       %.6f (baseline: %.6f, improvement: %+.2f%%)\n",
           log_loss, log_loss_baseline, log_loss_improvement))

# 7b. Run Metrics ----
actual_runs <- validation_data$actual_runs
pred_runs <- validation_data$pred_runs

# Poisson negative log-likelihood (primary metric, lower = better)
# Loss = mean(lambda - y * log(lambda)) where lambda = predicted, y = actual
pred_runs_clipped <- pmax(eps, pred_runs)
poisson_loss <- mean(pred_runs_clipped - actual_runs * log(pred_runs_clipped))

# RMSE (lower = better)
rmse <- sqrt(mean((pred_runs - actual_runs)^2))

# MAE (lower = better)
mae <- mean(abs(pred_runs - actual_runs))

# Baseline: naive format average
baseline_runs <- mean(actual_runs)
baseline_clipped <- pmax(eps, baseline_runs)
poisson_baseline <- mean(baseline_clipped - actual_runs * log(baseline_clipped))
rmse_baseline <- sqrt(mean((baseline_runs - actual_runs)^2))
mae_baseline <- mean(abs(baseline_runs - actual_runs))

# Improvement
poisson_improvement <- (poisson_baseline - poisson_loss) / poisson_baseline * 100
rmse_improvement <- (rmse_baseline - rmse) / rmse_baseline * 100
mae_improvement <- (mae_baseline - mae) / mae_baseline * 100

cat("\n--- Run Prediction Metrics ---\n")
cat(sprintf("Baseline runs/ball: %.3f\n", baseline_runs))
cat(sprintf("Poisson Loss: %.4f (baseline: %.4f, improvement: %+.2f%%)\n",
           poisson_loss, poisson_baseline, poisson_improvement))
cat(sprintf("RMSE:    %.4f (baseline: %.4f, improvement: %+.2f%%)\n",
           rmse, rmse_baseline, rmse_improvement))
cat(sprintf("MAE:     %.4f (baseline: %.4f, improvement: %+.2f%%)\n",
           mae, mae_baseline, mae_improvement))

# 8. Calibration Curves ----
cli::cli_h2("Generating calibration curves")

# Wicket calibration by decile
validation_data[, wicket_decile := cut(pred_wicket,
                                        breaks = quantile(pred_wicket, probs = seq(0, 1, 0.1)),
                                        labels = 1:10,
                                        include.lowest = TRUE)]

wicket_calibration <- validation_data[, .(
 mean_predicted = mean(pred_wicket),
 mean_actual = mean(is_wicket),
 n = .N
), by = wicket_decile][order(wicket_decile)]

cat("\n--- Wicket Calibration by Decile ---\n")
print(wicket_calibration)

# Run calibration by bucket
validation_data[, run_bucket := cut(pred_runs,
                                     breaks = c(0, 0.5, 1.0, 1.5, 2.0, 3.0, 6.0),
                                     labels = c("0-0.5", "0.5-1.0", "1.0-1.5", "1.5-2.0", "2.0-3.0", "3.0+"),
                                     include.lowest = TRUE)]

run_calibration <- validation_data[, .(
 mean_predicted = mean(pred_runs),
 mean_actual = mean(actual_runs),
 n = .N
), by = run_bucket][order(run_bucket)]

cat("\n--- Run Calibration by Bucket ---\n")
print(run_calibration)

# 9. Sample Size Analysis ----
cli::cli_h2("Sample size analysis")

# Group players by experience level
validation_data[, batter_exp_bucket := cut(batter_balls,
                                            breaks = c(0, 100, 200, 500, 1000, Inf),
                                            labels = c("0-100", "100-200", "200-500", "500-1000", "1000+"),
                                            include.lowest = TRUE)]

sample_size_analysis <- validation_data[, .(
 mean_batter_elo = mean(batter_run_elo),
 sd_batter_elo = sd(batter_run_elo),
 brier = mean((pred_wicket - as.integer(is_wicket))^2),
 rmse = sqrt(mean((pred_runs - actual_runs)^2)),
 n = .N
), by = batter_exp_bucket][order(batter_exp_bucket)]

cat("\n--- Metrics by Batter Experience ---\n")
print(sample_size_analysis)

# 10. Extreme ELO Analysis ----
cli::cli_h2("Extreme ELO analysis")

# Find players with extreme ELOs and small samples
player_stats <- validation_data[, .(
 last_run_elo = last(batter_run_elo_before),
 blended_run_elo = last(batter_run_elo),
 balls = max(batter_balls),
 n_deliveries = .N
), by = batter_id]

# High raw ELO with small sample
extreme_high <- player_stats[last_run_elo > 1600 & balls < 200][order(-last_run_elo)]
extreme_low <- player_stats[last_run_elo < 1100 & balls < 200][order(last_run_elo)]

cat("\n--- Extreme High ELOs (small sample) ---\n")
if (nrow(extreme_high) > 0) {
 cat(sprintf("Found %d players with ELO > 1600 and < 200 balls\n", nrow(extreme_high)))
 print(head(extreme_high, 10))
} else {
 cat("No extreme high ELO players with small samples\n")
}

cat("\n--- Extreme Low ELOs (small sample) ---\n")
if (nrow(extreme_low) > 0) {
 cat(sprintf("Found %d players with ELO < 1100 and < 200 balls\n", nrow(extreme_low)))
 print(head(extreme_low, 10))
} else {
 cat("No extreme low ELO players with small samples\n")
}

# Show effect of blending on extreme ELOs
cat("\n--- Blending Effect on Extreme ELOs ---\n")
if (nrow(extreme_high) > 0) {
 cat("Blending pulls high ELOs down:\n")
 print(extreme_high[1:min(5, nrow(extreme_high)), .(
   batter_id, balls, raw_elo = last_run_elo, blended_elo = blended_run_elo,
   change = blended_run_elo - last_run_elo
 )])
}

# 11. Summary ----
cli::cli_h1("Validation Summary")

summary_table <- data.table(
 Metric = c("Brier Score", "Log Loss", "Poisson Loss", "RMSE", "MAE"),
 Model = c(brier_score, log_loss, poisson_loss, rmse, mae),
 Baseline = c(brier_baseline, log_loss_baseline, poisson_baseline, rmse_baseline, mae_baseline),
 Improvement = c(
   sprintf("%+.2f%%", brier_improvement),
   sprintf("%+.2f%%", log_loss_improvement),
   sprintf("%+.2f%%", poisson_improvement),
   sprintf("%+.2f%%", rmse_improvement),
   sprintf("%+.2f%%", mae_improvement)
 )
)

print(summary_table)

cat("\n")
if (poisson_improvement > 0 && log_loss_improvement > 0) {
 cli::cli_alert_success("Model outperforms baseline on primary metrics (Poisson + LogLoss)!")
} else if (poisson_improvement > 0 || log_loss_improvement > 0) {
 cli::cli_alert_warning("Model outperforms baseline on some primary metrics")
} else {
 cli::cli_alert_danger("Model underperforms baseline - investigate parameters")
}

# 12. Save Results ----
cli::cli_h2("Saving results")

results <- list(
 format = FORMAT,
 events = VALIDATION_EVENTS,
 use_blending = USE_BLENDING,
 n_deliveries = n_deliveries,
 n_matches = n_matches,
 metrics = list(
   brier_score = brier_score,
   log_loss = log_loss,
   poisson_loss = poisson_loss,
   rmse = rmse,
   mae = mae,
   brier_baseline = brier_baseline,
   log_loss_baseline = log_loss_baseline,
   poisson_baseline = poisson_baseline,
   rmse_baseline = rmse_baseline,
   mae_baseline = mae_baseline,
   poisson_improvement = poisson_improvement,
   brier_improvement = brier_improvement,
   log_loss_improvement = log_loss_improvement,
   rmse_improvement = rmse_improvement,
   mae_improvement = mae_improvement
 ),
 wicket_calibration = wicket_calibration,
 run_calibration = run_calibration,
 sample_size_analysis = sample_size_analysis,
 validated_at = Sys.time()
)

results_file <- file.path(find_bouncerdata_dir(), "models",
                         sprintf("3way_elo_validation_%s.rds", FORMAT))
dir.create(dirname(results_file), showWarnings = FALSE, recursive = TRUE)
saveRDS(results, results_file)

cli::cli_alert_success("Saved results to {results_file}")
cat("\n")
