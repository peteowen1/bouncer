# Diagnostic Script: Prediction Calibration Analysis
#
# Problem: Predictions may be poorly calibrated (when we predict X, X doesn't happen).
# This script creates calibration plots for both runs and wickets.
#
# Key questions:
# 1. When we predict high runs, do high runs actually occur?
# 2. When we predict high wicket probability, do wickets fall more often?
# 3. Are we over-confident or under-confident?

library(DBI)
library(data.table)
library(ggplot2)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cat("\n")
cli::cli_h1("Prediction Calibration Analysis")
cat("\n")

# ============================================================================
# 1. RUN PREDICTION CALIBRATION (Men's T20)
# ============================================================================

cli::cli_h2("1. Run Prediction Calibration")

# Get binned calibration data
run_cal <- DBI::dbGetQuery(conn, "
  WITH binned AS (
    SELECT
      FLOOR(exp_runs * 20) / 20 as pred_bin,  -- 0.05 bins
      AVG(exp_runs) as mean_predicted,
      AVG(actual_runs) as mean_actual,
      STDDEV(actual_runs) as sd_actual,
      COUNT(*) as n
    FROM mens_t20_3way_elo
    GROUP BY FLOOR(exp_runs * 20) / 20
    HAVING COUNT(*) >= 100
  )
  SELECT * FROM binned ORDER BY pred_bin
")
setDT(run_cal)

cat("\n--- Run Calibration by Prediction Bin ---\n")
cat(sprintf("%10s  %12s  %12s  %10s  %10s\n",
            "Pred Bin", "Predicted", "Actual", "Diff", "N"))
cat(sprintf("%10s  %12s  %12s  %10s  %10s\n",
            "--------", "---------", "------", "----", "-"))

for (i in 1:min(30, nrow(run_cal))) {
  diff <- run_cal$mean_actual[i] - run_cal$mean_predicted[i]
  cat(sprintf("%10.2f  %12.4f  %12.4f  %+10.4f  %10s\n",
              run_cal$pred_bin[i], run_cal$mean_predicted[i],
              run_cal$mean_actual[i], diff,
              format(run_cal$n[i], big.mark = ",")))
}

# Calculate overall calibration metrics
run_cal[, calibration_error := abs(mean_actual - mean_predicted)]
weighted_cal_error <- sum(run_cal$calibration_error * run_cal$n) / sum(run_cal$n)

cat(sprintf("\nWeighted Mean Absolute Calibration Error: %.4f runs\n", weighted_cal_error))

# Calibration slope (should be ~1.0 for perfect calibration)
if (nrow(run_cal) >= 3) {
  cal_model <- lm(mean_actual ~ mean_predicted, data = run_cal, weights = n)
  cat(sprintf("Calibration slope: %.3f (1.0 = perfect)\n", coef(cal_model)[2]))
  cat(sprintf("Calibration intercept: %.4f (0.0 = perfect)\n", coef(cal_model)[1]))

  if (coef(cal_model)[2] < 0.8) {
    cat("⚠️  Slope < 0.8: Predictions are OVERCONFIDENT (extreme predictions don't match reality)\n")
  } else if (coef(cal_model)[2] > 1.2) {
    cat("⚠️  Slope > 1.2: Predictions are UNDERCONFIDENT (should be more extreme)\n")
  } else {
    cat("✓ Slope near 1.0: Calibration is reasonable\n")
  }
}

# ============================================================================
# 2. WICKET PREDICTION CALIBRATION
# ============================================================================

cat("\n")
cli::cli_h2("2. Wicket Prediction Calibration")

wicket_cal <- DBI::dbGetQuery(conn, "
  WITH binned AS (
    SELECT
      FLOOR(exp_wicket * 100) as pred_pct,  -- 1% bins
      AVG(exp_wicket) as mean_predicted,
      AVG(CASE WHEN is_wicket THEN 1.0 ELSE 0.0 END) as mean_actual,
      COUNT(*) as n
    FROM mens_t20_3way_elo
    GROUP BY FLOOR(exp_wicket * 100)
    HAVING COUNT(*) >= 100
  )
  SELECT * FROM binned ORDER BY pred_pct
")
setDT(wicket_cal)

cat("\n--- Wicket Calibration by Prediction Bin ---\n")
cat(sprintf("%10s  %12s  %12s  %10s  %10s\n",
            "Pred (%)", "Predicted", "Actual", "Diff", "N"))
cat(sprintf("%10s  %12s  %12s  %10s  %10s\n",
            "--------", "---------", "------", "----", "-"))

for (i in 1:nrow(wicket_cal)) {
  diff <- (wicket_cal$mean_actual[i] - wicket_cal$mean_predicted[i]) * 100
  cat(sprintf("%10d  %12.2f%%  %12.2f%%  %+10.2f  %10s\n",
              wicket_cal$pred_pct[i],
              wicket_cal$mean_predicted[i] * 100,
              wicket_cal$mean_actual[i] * 100,
              diff,
              format(wicket_cal$n[i], big.mark = ",")))
}

# Brier score decomposition
cat("\n--- Wicket Calibration Metrics ---\n")

wicket_cal[, calibration_error := abs(mean_actual - mean_predicted)]
wicket_weighted_cal <- sum(wicket_cal$calibration_error * wicket_cal$n) / sum(wicket_cal$n)

cat(sprintf("Weighted Mean Absolute Calibration Error: %.4f (%.2f%%)\n",
            wicket_weighted_cal, wicket_weighted_cal * 100))

if (nrow(wicket_cal) >= 3) {
  wkt_model <- lm(mean_actual ~ mean_predicted, data = wicket_cal, weights = n)
  cat(sprintf("Calibration slope: %.3f (1.0 = perfect)\n", coef(wkt_model)[2]))
  cat(sprintf("Calibration intercept: %.4f (0.0 = perfect)\n", coef(wkt_model)[1]))
}

# ============================================================================
# 3. RELIABILITY DIAGRAMS (Console-based)
# ============================================================================

cat("\n")
cli::cli_h2("3. Reliability Diagram (ASCII)")

# Simple ASCII reliability diagram for runs
cat("\n--- Run Prediction Reliability ---\n")
cat("X-axis: Predicted | Y-axis: Actual\n")
cat("Diagonal = perfect calibration\n\n")

# Coarser bins for ASCII display
run_coarse <- run_cal[, .(
  mean_pred = weighted.mean(mean_predicted, n),
  mean_actual = weighted.mean(mean_actual, n),
  n = sum(n)
), by = .(bin = floor(pred_bin * 4) / 4)]  # 0.25 bins

max_runs <- 2.0
for (actual_level in seq(max_runs, 0, by = -0.25)) {
  line <- sprintf("%4.2f |", actual_level)
  for (pred_level in seq(0, max_runs, by = 0.25)) {
    matching <- run_coarse[bin >= pred_level - 0.125 & bin < pred_level + 0.125]
    if (nrow(matching) > 0) {
      if (abs(matching$mean_actual[1] - actual_level) < 0.125) {
        line <- paste0(line, " ● ")  # Data point
      } else if (abs(pred_level - actual_level) < 0.125) {
        line <- paste0(line, " · ")  # Diagonal
      } else {
        line <- paste0(line, "   ")
      }
    } else if (abs(pred_level - actual_level) < 0.125) {
      line <- paste0(line, " · ")  # Diagonal
    } else {
      line <- paste0(line, "   ")
    }
  }
  cat(line, "\n")
}
cat("     +", rep("-", 3 * 9), "\n")
cat("       ", paste(sprintf("%4.2f ", seq(0, max_runs, by = 0.25)), collapse = ""), "\n")

# ============================================================================
# 4. CONDITIONAL CALIBRATION (by context)
# ============================================================================

cat("\n")
cli::cli_h2("4. Conditional Calibration by Context")

# By phase (powerplay, middle, death)
# Note: 3way_elo table already has 'phase' column as string
phase_cal <- DBI::dbGetQuery(conn, "
  SELECT
    phase,
    AVG(exp_runs) as mean_predicted,
    AVG(actual_runs) as mean_actual,
    CORR(exp_runs, actual_runs) as correlation,
    COUNT(*) as n
  FROM mens_t20_3way_elo
  GROUP BY phase
  ORDER BY phase
")

cat("\n--- Calibration by Phase ---\n")
cat(sprintf("%-12s  %12s  %12s  %10s  %12s\n",
            "Phase", "Predicted", "Actual", "Corr", "N"))
for (i in 1:nrow(phase_cal)) {
  cat(sprintf("%-12s  %12.4f  %12.4f  %10.4f  %12s\n",
              phase_cal$phase[i], phase_cal$mean_predicted[i],
              phase_cal$mean_actual[i], phase_cal$correlation[i],
              format(phase_cal$n[i], big.mark = ",")))
}

# By ELO level (are extreme ELOs well-calibrated?)
elo_cal <- DBI::dbGetQuery(conn, "
  WITH elo_groups AS (
    SELECT
      CASE
        WHEN batter_run_elo_before < 1200 THEN 'Very Low (<1200)'
        WHEN batter_run_elo_before < 1400 THEN 'Low (1200-1400)'
        WHEN batter_run_elo_before < 1600 THEN 'Average (1400-1600)'
        WHEN batter_run_elo_before < 1800 THEN 'High (1600-1800)'
        ELSE 'Very High (>1800)'
      END as elo_group,
      exp_runs,
      actual_runs
    FROM mens_t20_3way_elo
  )
  SELECT
    elo_group,
    AVG(exp_runs) as mean_predicted,
    AVG(actual_runs) as mean_actual,
    AVG(exp_runs) - AVG(actual_runs) as bias,
    CORR(exp_runs, actual_runs) as correlation,
    COUNT(*) as n
  FROM elo_groups
  GROUP BY elo_group
  ORDER BY elo_group
")

cat("\n--- Calibration by Batter ELO Level ---\n")
cat(sprintf("%-22s  %10s  %10s  %8s  %8s  %10s\n",
            "Batter ELO", "Predicted", "Actual", "Bias", "Corr", "N"))
for (i in 1:nrow(elo_cal)) {
  cat(sprintf("%-22s  %10.4f  %10.4f  %+8.4f  %8.4f  %10s\n",
              elo_cal$elo_group[i], elo_cal$mean_predicted[i],
              elo_cal$mean_actual[i], elo_cal$bias[i],
              elo_cal$correlation[i], format(elo_cal$n[i], big.mark = ",")))
}

# ============================================================================
# 5. SKILL SCORE CALCULATION
# ============================================================================

cat("\n")
cli::cli_h2("5. Skill Scores vs Baseline")

# Note: agnostic_runs is not in 3way_elo table, use constant baseline (T20 mean ~1.28)
baseline_runs <- 1.28  # T20 average runs per ball

skill_scores <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    -- Run prediction metrics
    AVG(actual_runs) as mean_actual,
    AVG(exp_runs) as mean_elo_pred,
    %.3f as mean_agnostic,

    -- Poisson deviance (proper loss for count data)
    AVG(2 * (actual_runs * LN(GREATEST(actual_runs, 0.01) / GREATEST(exp_runs, 0.01))
            - (actual_runs - exp_runs))) as elo_poisson_dev,
    AVG(2 * (actual_runs * LN(GREATEST(actual_runs, 0.01) / %.3f)
            - (actual_runs - %.3f))) as agnostic_poisson_dev,

    -- MAE
    AVG(ABS(exp_runs - actual_runs)) as elo_mae,
    AVG(ABS(%.3f - actual_runs)) as agnostic_mae,

    -- Correlation (against constant is 0, so use 0.001 placeholder)
    CORR(exp_runs, actual_runs) as elo_corr,
    0.001 as agnostic_corr,

    COUNT(*) as n
  FROM mens_t20_3way_elo
", baseline_runs, baseline_runs, baseline_runs, baseline_runs))

cat("\n--- Run Prediction Skill (ELO vs Agnostic) ---\n")
cat(sprintf("Deliveries: %s\n", format(skill_scores$n, big.mark = ",")))
cat(sprintf("\n%-20s  %12s  %12s  %12s\n",
            "Metric", "Agnostic", "3-Way ELO", "% Improvement"))

# MAE
mae_improve <- (1 - skill_scores$elo_mae / skill_scores$agnostic_mae) * 100
cat(sprintf("%-20s  %12.4f  %12.4f  %+12.2f%%\n",
            "MAE", skill_scores$agnostic_mae, skill_scores$elo_mae, mae_improve))

# Correlation
corr_improve <- (skill_scores$elo_corr / skill_scores$agnostic_corr - 1) * 100
cat(sprintf("%-20s  %12.4f  %12.4f  %+12.2f%%\n",
            "Correlation", skill_scores$agnostic_corr, skill_scores$elo_corr, corr_improve))

# Poisson deviance
pois_improve <- (1 - skill_scores$elo_poisson_dev / skill_scores$agnostic_poisson_dev) * 100
cat(sprintf("%-20s  %12.4f  %12.4f  %+12.2f%%\n",
            "Poisson Deviance", skill_scores$agnostic_poisson_dev,
            skill_scores$elo_poisson_dev, pois_improve))

# ============================================================================
# 6. WICKET SKILL SCORES
# ============================================================================

cat("\n--- Wicket Prediction Skill ---\n")

wicket_skill <- DBI::dbGetQuery(conn, "
  WITH base_rate AS (
    SELECT AVG(CASE WHEN is_wicket THEN 1.0 ELSE 0.0 END) as global_rate
    FROM mens_t20_3way_elo
  )
  SELECT
    -- Actual rate
    AVG(CASE WHEN is_wicket THEN 1.0 ELSE 0.0 END) as actual_rate,

    -- Log loss
    -AVG(CASE WHEN is_wicket
           THEN LN(GREATEST(exp_wicket, 0.001))
           ELSE LN(GREATEST(1 - exp_wicket, 0.001))
         END) as elo_logloss,
    -AVG(CASE WHEN is_wicket
           THEN LN(GREATEST((SELECT global_rate FROM base_rate), 0.001))
           ELSE LN(GREATEST(1 - (SELECT global_rate FROM base_rate), 0.001))
         END) as baseline_logloss,

    -- Brier score
    AVG(POWER(exp_wicket - CASE WHEN is_wicket THEN 1.0 ELSE 0.0 END, 2)) as elo_brier,
    AVG(POWER((SELECT global_rate FROM base_rate) - CASE WHEN is_wicket THEN 1.0 ELSE 0.0 END, 2)) as baseline_brier,

    COUNT(*) as n
  FROM mens_t20_3way_elo
")

# Log loss skill
logloss_skill <- (1 - wicket_skill$elo_logloss / wicket_skill$baseline_logloss) * 100
brier_skill <- (1 - wicket_skill$elo_brier / wicket_skill$baseline_brier) * 100

cat(sprintf("\n%-20s  %12s  %12s  %12s\n",
            "Metric", "Baseline", "3-Way ELO", "% Improvement"))
cat(sprintf("%-20s  %12.4f  %12.4f  %+12.2f%%\n",
            "Log Loss", wicket_skill$baseline_logloss, wicket_skill$elo_logloss, logloss_skill))
cat(sprintf("%-20s  %12.5f  %12.5f  %+12.2f%%\n",
            "Brier Score", wicket_skill$baseline_brier, wicket_skill$elo_brier, brier_skill))

if (logloss_skill < 0) {
  cat("\n⚠️  NEGATIVE SKILL: Wicket ELO predictions are WORSE than just predicting the mean rate!\n")
}

# ============================================================================
# 7. SUMMARY
# ============================================================================

cat("\n")
cli::cli_h1("Calibration Summary")
cat("\n")

cat("KEY FINDINGS:\n")
cat(sprintf("1. Run MAE improvement over agnostic: %.2f%%\n", mae_improve))
cat(sprintf("2. Wicket log-loss skill: %.2f%% (negative = worse than baseline)\n", logloss_skill))
cat(sprintf("3. Run calibration error: %.4f runs\n", weighted_cal_error))
cat(sprintf("4. Wicket calibration error: %.2f%%\n", wicket_weighted_cal * 100))
cat("\n")
cat("SUCCESS CRITERIA:\n")
cat("- Run skill > 3% improvement (currently %.1f%%)\n", mae_improve)
cat("- Wicket skill > 0% (currently %.1f%%)\n", logloss_skill)
cat("- Calibration slope near 1.0\n")
cat("\n")
