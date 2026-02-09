# Diagnostic Script: Variance Decomposition of ELO Components
#
# Problem: We don't know which ELO component contributes most to predictions.
# This script decomposes expected_runs into batter, bowler, venue contributions.
#
# Key questions:
# 1. What % of prediction variance comes from each component?
# 2. Is venue dominating? Is player contribution too small?
# 3. How do the weights compare to their actual prediction power?

library(DBI)
library(data.table)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cat("\n")
cli::cli_h1("ELO Variance Decomposition")
cat("\n")

# ============================================================================
# 1. SAMPLE DATA AND DECOMPOSE PREDICTIONS
# ============================================================================

cli::cli_h2("1. Component Contributions (Men's T20)")

# Sample recent data for analysis (too large for full analysis)
# Note: agnostic_runs is not stored in 3way_elo table, so we use format baseline
sample_data <- DBI::dbGetQuery(conn, "
  SELECT
    -- ELO values before the delivery
    batter_run_elo_before,
    bowler_run_elo_before,
    venue_perm_run_elo_before,
    venue_session_run_elo_before,
    -- Predictions and actuals
    exp_runs,
    actual_runs
  FROM mens_t20_3way_elo
  WHERE match_date >= '2023-01-01'
  LIMIT 500000
")

# Use T20 baseline as proxy for agnostic_runs (format mean)
sample_data$agnostic_runs <- EXPECTED_RUNS_T20  # ~1.28 for T20
setDT(sample_data)

cat(sprintf("\nAnalyzing %s deliveries from 2023+\n",
            format(nrow(sample_data), big.mark = ",")))

# Get current parameters
elo_start <- THREE_WAY_ELO_START  # 1400
runs_per_100_elo <- THREE_WAY_RUNS_PER_100_ELO_POINTS_MENS_T20
runs_per_elo <- runs_per_100_elo / 100

# Get weights
weights <- get_run_elo_weights("t20", "male")
cat(sprintf("\nAttribution weights:\n"))
cat(sprintf("  w_batter:       %.3f\n", weights$w_batter))
cat(sprintf("  w_bowler:       %.3f\n", weights$w_bowler))
cat(sprintf("  w_venue_perm:   %.3f\n", weights$w_venue_perm))
cat(sprintf("  w_venue_session:%.3f\n", weights$w_venue_session))

# Decompose each component's contribution
sample_data[, `:=`(
  # ELO differences from baseline
  batter_elo_diff = batter_run_elo_before - elo_start,
  bowler_elo_diff = bowler_run_elo_before - elo_start,
  venue_perm_elo_diff = venue_perm_run_elo_before - elo_start,
  venue_session_elo_diff = venue_session_run_elo_before - elo_start
)]

sample_data[, `:=`(
  # Convert to run contributions (weighted)
  batter_contrib = weights$w_batter * batter_elo_diff * runs_per_elo,
  bowler_contrib = weights$w_bowler * (-bowler_elo_diff) * runs_per_elo,  # Inverted
  venue_perm_contrib = weights$w_venue_perm * venue_perm_elo_diff * runs_per_elo,
  venue_session_contrib = weights$w_venue_session * venue_session_elo_diff * runs_per_elo
)]

# Total ELO adjustment
sample_data[, total_elo_adjust := batter_contrib + bowler_contrib +
                                   venue_perm_contrib + venue_session_contrib]

# Reconstructed prediction (should match exp_runs closely)
sample_data[, reconstructed := agnostic_runs + total_elo_adjust]

# ============================================================================
# 2. VARIANCE ANALYSIS
# ============================================================================

cat("\n")
cli::cli_h2("2. Component Variance Analysis")

# Calculate variance of each component
variances <- data.table(
  component = c("agnostic_runs", "batter_contrib", "bowler_contrib",
                "venue_perm_contrib", "venue_session_contrib", "total_elo_adjust"),
  variance = c(
    var(sample_data$agnostic_runs, na.rm = TRUE),
    var(sample_data$batter_contrib, na.rm = TRUE),
    var(sample_data$bowler_contrib, na.rm = TRUE),
    var(sample_data$venue_perm_contrib, na.rm = TRUE),
    var(sample_data$venue_session_contrib, na.rm = TRUE),
    var(sample_data$total_elo_adjust, na.rm = TRUE)
  ),
  sd = c(
    sd(sample_data$agnostic_runs, na.rm = TRUE),
    sd(sample_data$batter_contrib, na.rm = TRUE),
    sd(sample_data$bowler_contrib, na.rm = TRUE),
    sd(sample_data$venue_perm_contrib, na.rm = TRUE),
    sd(sample_data$venue_session_contrib, na.rm = TRUE),
    sd(sample_data$total_elo_adjust, na.rm = TRUE)
  )
)

total_prediction_var <- var(sample_data$exp_runs, na.rm = TRUE)
variances[, pct_of_prediction := 100 * variance / total_prediction_var]

cat("\n--- Variance by Component ---\n")
cat(sprintf("%-20s  %12s  %8s  %12s\n", "Component", "Variance", "SD", "% of Total"))
cat(sprintf("%-20s  %12s  %8s  %12s\n", "---------", "--------", "--", "----------"))
for (i in 1:nrow(variances)) {
  cat(sprintf("%-20s  %12.6f  %8.4f  %12.1f%%\n",
              variances$component[i], variances$variance[i],
              variances$sd[i], variances$pct_of_prediction[i]))
}

# ============================================================================
# 3. COMPONENT RANGES
# ============================================================================

cat("\n")
cli::cli_h2("3. Component Value Ranges")

# Summary statistics for each contribution
summaries <- sample_data[, .(
  min = min(c(batter_contrib, bowler_contrib, venue_perm_contrib, venue_session_contrib), na.rm = TRUE),
  max = max(c(batter_contrib, bowler_contrib, venue_perm_contrib, venue_session_contrib), na.rm = TRUE)
)]

cat("\n--- Contribution Ranges ---\n")
components <- c("batter_contrib", "bowler_contrib", "venue_perm_contrib", "venue_session_contrib")
for (comp in components) {
  vals <- sample_data[[comp]]
  cat(sprintf("%-20s: min=%+.4f, max=%+.4f, mean=%+.5f, sd=%.4f\n",
              comp, min(vals, na.rm = TRUE), max(vals, na.rm = TRUE),
              mean(vals, na.rm = TRUE), sd(vals, na.rm = TRUE)))
}

# ============================================================================
# 4. CORRELATION WITH ACTUAL RUNS
# ============================================================================

cat("\n")
cli::cli_h2("4. Correlation with Actual Runs")

# How well does each component predict actual runs?
correlations <- data.table(
  component = c("agnostic_runs", "batter_contrib", "bowler_contrib",
                "venue_perm_contrib", "venue_session_contrib",
                "total_elo_adjust", "exp_runs (final)"),
  correlation = c(
    cor(sample_data$agnostic_runs, sample_data$actual_runs, use = "complete.obs"),
    cor(sample_data$batter_contrib, sample_data$actual_runs, use = "complete.obs"),
    cor(sample_data$bowler_contrib, sample_data$actual_runs, use = "complete.obs"),
    cor(sample_data$venue_perm_contrib, sample_data$actual_runs, use = "complete.obs"),
    cor(sample_data$venue_session_contrib, sample_data$actual_runs, use = "complete.obs"),
    cor(sample_data$total_elo_adjust, sample_data$actual_runs, use = "complete.obs"),
    cor(sample_data$exp_runs, sample_data$actual_runs, use = "complete.obs")
  )
)

cat("\n--- Correlation with Actual Runs ---\n")
for (i in 1:nrow(correlations)) {
  cat(sprintf("%-20s: r = %+.4f\n", correlations$component[i], correlations$correlation[i]))
}

# ============================================================================
# 5. ELO DIFFERENCE ANALYSIS
# ============================================================================

cat("\n")
cli::cli_h2("5. ELO Difference Distribution")

cat("\n--- ELO Differences from Baseline (1400) ---\n")
elo_diffs <- c("batter_elo_diff", "bowler_elo_diff",
               "venue_perm_elo_diff", "venue_session_elo_diff")
for (comp in elo_diffs) {
  vals <- sample_data[[comp]]
  cat(sprintf("%-25s: mean=%+6.1f, sd=%6.1f, range=[%+6.0f, %+6.0f]\n",
              comp, mean(vals, na.rm = TRUE), sd(vals, na.rm = TRUE),
              min(vals, na.rm = TRUE), max(vals, na.rm = TRUE)))
}

# ============================================================================
# 6. PROBLEM DIAGNOSIS
# ============================================================================

cat("\n")
cli::cli_h2("6. Problem Diagnosis")

# Calculate relative importance
batter_var <- var(sample_data$batter_contrib, na.rm = TRUE)
bowler_var <- var(sample_data$bowler_contrib, na.rm = TRUE)
venue_perm_var <- var(sample_data$venue_perm_contrib, na.rm = TRUE)
venue_session_var <- var(sample_data$venue_session_contrib, na.rm = TRUE)
total_elo_var <- batter_var + bowler_var + venue_perm_var + venue_session_var

cat("\n--- Relative Variance Contribution (among ELO components) ---\n")
cat(sprintf("  Batter:        %.1f%%\n", 100 * batter_var / total_elo_var))
cat(sprintf("  Bowler:        %.1f%%\n", 100 * bowler_var / total_elo_var))
cat(sprintf("  Venue Perm:    %.1f%%\n", 100 * venue_perm_var / total_elo_var))
cat(sprintf("  Venue Session: %.1f%%\n", 100 * venue_session_var / total_elo_var))

# Check for issues
venue_total_var <- venue_perm_var + venue_session_var
player_total_var <- batter_var + bowler_var

cat(sprintf("\n  Total Venue: %.1f%% | Total Player: %.1f%%\n",
            100 * venue_total_var / total_elo_var,
            100 * player_total_var / total_elo_var))

if (venue_perm_var > batter_var) {
  cat("\n⚠️  WARNING: Venue permanent contributes more variance than batter!\n")
  cat("    This suggests venue ELOs have drifted too far from baseline.\n")
}

if (sd(sample_data$venue_perm_elo_diff, na.rm = TRUE) > 400) {
  cat("\n⚠️  WARNING: Venue permanent ELO SD > 400 points!\n")
  cat("    This is likely too much variation for a stable venue characteristic.\n")
}

# ============================================================================
# 7. SUMMARY
# ============================================================================

cat("\n")
cli::cli_h1("Decomposition Summary")
cat("\n")

cat("KEY FINDINGS:\n")
cat(sprintf("1. Agnostic baseline accounts for %.1f%% of prediction variance\n",
            variances[component == "agnostic_runs", pct_of_prediction]))
cat(sprintf("2. Total ELO adjustment accounts for %.1f%% of prediction variance\n",
            variances[component == "total_elo_adjust", pct_of_prediction]))
cat(sprintf("3. Among ELO components:\n"))
cat(sprintf("   - Batter:        %.1f%% of ELO variance\n", 100 * batter_var / total_elo_var))
cat(sprintf("   - Bowler:        %.1f%% of ELO variance\n", 100 * bowler_var / total_elo_var))
cat(sprintf("   - Venue (total): %.1f%% of ELO variance\n", 100 * venue_total_var / total_elo_var))
cat("\n")
cat("RECOMMENDATIONS:\n")
cat("1. If venue variance > player variance, venue K-factors are too high\n")
cat("2. If agnostic accounts for >95% of variance, ELO isn't adding much\n")
cat("3. Bound venue ELOs tightly (±200 from baseline) since they should be stable\n")
cat("\n")
