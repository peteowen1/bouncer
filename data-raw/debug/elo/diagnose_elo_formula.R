# Diagnostic Script: Analyze ELO-to-Prediction Conversion
#
# Problem: Extreme venue ELOs (-3000 to +5000) may produce unrealistic predictions.
# This script investigates what happens at the extremes of the ELO scale.
#
# Key questions:
# 1. What expected runs/wickets do extreme ELOs produce?
# 2. Does the linear conversion break down at scale?
# 3. Should we use logistic conversion instead?

library(DBI)
library(data.table)
library(ggplot2)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cat("\n")
cli::cli_h1("3-Way ELO Formula Diagnosis")
cat("\n")

# ============================================================================
# 1. THEORETICAL ANALYSIS: What does the formula predict at extreme ELOs?
# ============================================================================

cli::cli_h2("1. Theoretical ELO-to-Prediction Mapping")

# Current formula parameters (from constants_3way.R)
elo_start <- THREE_WAY_ELO_START  # 1400
runs_per_100_elo <- THREE_WAY_RUNS_PER_100_ELO_POINTS_MENS_T20  # 0.0745
baseline_runs <- EXPECTED_RUNS_T20  # ~1.14

cat(sprintf("\nFormula: expected_runs = baseline + sum(weighted_elo_contrib) * runs_per_elo\n"))
cat(sprintf("  baseline = %.3f runs per ball\n", baseline_runs))
cat(sprintf("  runs_per_100_elo = %.4f\n", runs_per_100_elo))
cat(sprintf("  elo_start = %d\n", elo_start))

# Simulate what happens at various ELO levels
test_elos <- seq(500, 2500, by = 100)
elo_diff <- test_elos - elo_start
runs_per_elo <- runs_per_100_elo / 100

# Calculate expected runs for each ELO level (single entity contribution)
expected_runs <- baseline_runs + elo_diff * runs_per_elo

results_df <- data.frame(
  elo = test_elos,
  elo_diff = elo_diff,
  expected_runs = expected_runs,
  multiplier = expected_runs / baseline_runs
)

cat("\n--- Single Entity ELO Effect (e.g., batter only) ---\n")
cat(sprintf("%6s  %8s  %12s  %10s\n", "ELO", "Diff", "Exp Runs", "Multiplier"))
cat(sprintf("%6s  %8s  %12s  %10s\n", "---", "---", "--------", "----------"))
for (i in seq(1, nrow(results_df), by = 5)) {
  cat(sprintf("%6d  %+8d  %12.3f  %10.2fx\n",
              results_df$elo[i], results_df$elo_diff[i],
              results_df$expected_runs[i], results_df$multiplier[i]))
}

# Check extreme cases
cat("\n--- Extreme Combined ELO Scenarios ---\n")
# Format: batter_elo + venue_elo - bowler_elo (relative to baseline)
scenarios <- data.frame(
  description = c(
    "All average (1400 each)",
    "Good batter (1600) vs avg bowler, avg venue",
    "Good batter (1800) vs avg bowler, avg venue",
    "Elite batter (2000) vs weak bowler (1200), high-scoring venue (1700)",
    "Weak batter (1200) vs elite bowler (1800), low-scoring venue (1100)",
    "Extreme high: +400 batter, +300 venue, -300 bowler",
    "Extreme low: -400 batter, -300 venue, +300 bowler"
  ),
  batter_elo = c(1400, 1600, 1800, 2000, 1200, 1800, 1000),
  bowler_elo = c(1400, 1400, 1400, 1200, 1800, 1100, 1700),
  venue_elo = c(1400, 1400, 1400, 1700, 1100, 1700, 1100)
)

# Calculate combined ELO effect (simplified, not weighted)
scenarios$combined_diff <- (scenarios$batter_elo - elo_start) +
                           (scenarios$venue_elo - elo_start) -
                           (scenarios$bowler_elo - elo_start)
scenarios$expected_runs <- baseline_runs + scenarios$combined_diff * runs_per_elo
scenarios$multiplier <- scenarios$expected_runs / baseline_runs

cat("\nAssuming equal weights (simplified):\n")
for (i in 1:nrow(scenarios)) {
  cat(sprintf("\n%s\n", scenarios$description[i]))
  cat(sprintf("  Batter=%d, Bowler=%d, Venue=%d -> Combined diff=%+d\n",
              scenarios$batter_elo[i], scenarios$bowler_elo[i],
              scenarios$venue_elo[i], scenarios$combined_diff[i]))
  cat(sprintf("  Expected runs = %.3f (%.2fx baseline)\n",
              scenarios$expected_runs[i], scenarios$multiplier[i]))
}

# ============================================================================
# 2. EMPIRICAL ANALYSIS: What ELOs actually exist in the data?
# ============================================================================

cat("\n")
cli::cli_h2("2. Actual ELO Distribution in Data")

# Focus on Men's T20 as primary test case
stats <- DBI::dbGetQuery(conn, "
  SELECT
    -- Batter run ELO
    AVG(batter_run_elo_before) as batter_run_mean,
    STDDEV(batter_run_elo_before) as batter_run_sd,
    MIN(batter_run_elo_before) as batter_run_min,
    MAX(batter_run_elo_before) as batter_run_max,
    -- Bowler run ELO
    AVG(bowler_run_elo_before) as bowler_run_mean,
    STDDEV(bowler_run_elo_before) as bowler_run_sd,
    MIN(bowler_run_elo_before) as bowler_run_min,
    MAX(bowler_run_elo_before) as bowler_run_max,
    -- Venue permanent run ELO
    AVG(venue_perm_run_elo_before) as venue_perm_mean,
    STDDEV(venue_perm_run_elo_before) as venue_perm_sd,
    MIN(venue_perm_run_elo_before) as venue_perm_min,
    MAX(venue_perm_run_elo_before) as venue_perm_max,
    -- Venue session run ELO
    AVG(venue_session_run_elo_before) as venue_session_mean,
    STDDEV(venue_session_run_elo_before) as venue_session_sd,
    MIN(venue_session_run_elo_before) as venue_session_min,
    MAX(venue_session_run_elo_before) as venue_session_max
  FROM mens_t20_3way_elo
")

cat("\n--- Men's T20 ELO Distribution (Run ELO) ---\n")
cat(sprintf("\nBatter Run ELO:\n"))
cat(sprintf("  Mean: %.1f, SD: %.1f\n", stats$batter_run_mean, stats$batter_run_sd))
cat(sprintf("  Range: %.0f to %.0f (span: %.0f)\n",
            stats$batter_run_min, stats$batter_run_max,
            stats$batter_run_max - stats$batter_run_min))

cat(sprintf("\nBowler Run ELO:\n"))
cat(sprintf("  Mean: %.1f, SD: %.1f\n", stats$bowler_run_mean, stats$bowler_run_sd))
cat(sprintf("  Range: %.0f to %.0f (span: %.0f)\n",
            stats$bowler_run_min, stats$bowler_run_max,
            stats$bowler_run_max - stats$bowler_run_min))

cat(sprintf("\nVenue Permanent Run ELO:\n"))
cat(sprintf("  Mean: %.1f, SD: %.1f\n", stats$venue_perm_mean, stats$venue_perm_sd))
cat(sprintf("  Range: %.0f to %.0f (span: %.0f)\n",
            stats$venue_perm_min, stats$venue_perm_max,
            stats$venue_perm_max - stats$venue_perm_min))

cat(sprintf("\nVenue Session Run ELO:\n"))
cat(sprintf("  Mean: %.1f, SD: %.1f\n", stats$venue_session_mean, stats$venue_session_sd))
cat(sprintf("  Range: %.0f to %.0f (span: %.0f)\n",
            stats$venue_session_min, stats$venue_session_max,
            stats$venue_session_max - stats$venue_session_min))

# ============================================================================
# 3. CHECK ACTUAL PREDICTIONS: What exp_runs values exist?
# ============================================================================

cat("\n")
cli::cli_h2("3. Actual Prediction Distribution")

pred_stats <- DBI::dbGetQuery(conn, "
  SELECT
    MIN(exp_runs) as min_exp,
    MAX(exp_runs) as max_exp,
    AVG(exp_runs) as mean_exp,
    STDDEV(exp_runs) as sd_exp,
    PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY exp_runs) as p1,
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY exp_runs) as p5,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY exp_runs) as p95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY exp_runs) as p99,
    COUNT(*) as n,
    SUM(CASE WHEN exp_runs < 0 THEN 1 ELSE 0 END) as n_negative,
    SUM(CASE WHEN exp_runs > 3 THEN 1 ELSE 0 END) as n_over_3,
    SUM(CASE WHEN exp_runs > 6 THEN 1 ELSE 0 END) as n_over_6
  FROM mens_t20_3way_elo
")

cat("\n--- Expected Runs Predictions (Men's T20) ---\n")
cat(sprintf("Total predictions: %s\n", format(pred_stats$n, big.mark = ",")))
cat(sprintf("Mean: %.4f, SD: %.4f\n", pred_stats$mean_exp, pred_stats$sd_exp))
cat(sprintf("Range: %.4f to %.4f\n", pred_stats$min_exp, pred_stats$max_exp))
cat(sprintf("1st-99th percentile: %.4f to %.4f\n", pred_stats$p1, pred_stats$p99))
cat(sprintf("5th-95th percentile: %.4f to %.4f\n", pred_stats$p5, pred_stats$p95))
cat(sprintf("\nProblematic predictions:\n"))
cat(sprintf("  Negative: %d (%.2f%%)\n", pred_stats$n_negative,
            100 * pred_stats$n_negative / pred_stats$n))
cat(sprintf("  Over 3 runs: %d (%.2f%%)\n", pred_stats$n_over_3,
            100 * pred_stats$n_over_3 / pred_stats$n))
cat(sprintf("  Over 6 runs: %d (%.2f%%)\n", pred_stats$n_over_6,
            100 * pred_stats$n_over_6 / pred_stats$n))

# ============================================================================
# 4. INVESTIGATE EXTREME VENUE ELOS
# ============================================================================

cat("\n")
cli::cli_h2("4. Extreme Venue ELO Investigation")

extreme_venues <- DBI::dbGetQuery(conn, "
  WITH venue_stats AS (
    SELECT
      venue,
      MAX(venue_balls) as total_balls,
      MAX(venue_perm_run_elo_after) as latest_perm_elo
    FROM mens_t20_3way_elo
    GROUP BY venue
  )
  SELECT
    venue,
    total_balls,
    latest_perm_elo,
    latest_perm_elo - 1400 as elo_diff
  FROM venue_stats
  WHERE total_balls >= 5000
  ORDER BY latest_perm_elo
  LIMIT 10
")

cat("\n--- Lowest-scoring venues (5000+ balls) ---\n")
for (i in 1:nrow(extreme_venues)) {
  cat(sprintf("  %-50s ELO=%6.0f (diff=%+5.0f) balls=%6d\n",
              extreme_venues$venue[i], extreme_venues$latest_perm_elo[i],
              extreme_venues$elo_diff[i], extreme_venues$total_balls[i]))
}

extreme_venues_high <- DBI::dbGetQuery(conn, "
  WITH venue_stats AS (
    SELECT
      venue,
      MAX(venue_balls) as total_balls,
      MAX(venue_perm_run_elo_after) as latest_perm_elo
    FROM mens_t20_3way_elo
    GROUP BY venue
  )
  SELECT
    venue,
    total_balls,
    latest_perm_elo,
    latest_perm_elo - 1400 as elo_diff
  FROM venue_stats
  WHERE total_balls >= 5000
  ORDER BY latest_perm_elo DESC
  LIMIT 10
")

cat("\n--- Highest-scoring venues (5000+ balls) ---\n")
for (i in 1:nrow(extreme_venues_high)) {
  cat(sprintf("  %-50s ELO=%6.0f (diff=%+5.0f) balls=%6d\n",
              extreme_venues_high$venue[i], extreme_venues_high$latest_perm_elo[i],
              extreme_venues_high$elo_diff[i], extreme_venues_high$total_balls[i]))
}

# ============================================================================
# 5. COMPARE LINEAR VS LOGISTIC CONVERSION
# ============================================================================

cat("\n")
cli::cli_h2("5. Linear vs Logistic Conversion Comparison")

# The current formula is linear: expected = baseline + elo_diff * scale
# A logistic alternative: expected = baseline * 2 / (1 + exp(-elo_diff / sensitivity))

elo_diffs <- seq(-600, 600, by = 50)

# Linear conversion
linear_exp <- baseline_runs + elo_diffs * runs_per_elo

# Logistic conversion (bounds output between 0 and 2*baseline)
sensitivity <- 200  # ELO points for significant change
logistic_exp <- baseline_runs * 2 / (1 + exp(-elo_diffs / sensitivity))

# Alternative: Exponential (multiplicative model)
exp_factor <- 0.002  # Per ELO point
exponential_exp <- baseline_runs * exp(elo_diffs * exp_factor)

cat("\nComparison at various ELO differences:\n")
cat(sprintf("%8s  %10s  %10s  %12s\n", "ELO Diff", "Linear", "Logistic", "Exponential"))
cat(sprintf("%8s  %10s  %10s  %12s\n", "--------", "------", "--------", "-----------"))
for (i in seq(1, length(elo_diffs), by = 3)) {
  cat(sprintf("%+8d  %10.3f  %10.3f  %12.3f\n",
              elo_diffs[i], linear_exp[i], logistic_exp[i], exponential_exp[i]))
}

cat("\nKey insight: Logistic naturally bounds predictions between 0 and ~2x baseline\n")
cat("Linear can produce negative or extremely high values at extreme ELOs\n")

# ============================================================================
# 6. RECOMMENDATION SUMMARY
# ============================================================================

cat("\n")
cli::cli_h1("Diagnosis Summary")
cat("\n")

cat("FINDINGS:\n")
cat("1. Venue ELOs have extreme range (thousands of points from baseline)\n")
cat("2. These extreme ELOs are NOT small-sample noise - they have high ball counts\n")
cat("3. Linear conversion translates extreme ELOs into unrealistic predictions\n")
cat("\n")
cat("RECOMMENDATIONS:\n")
cat("1. BOUND ELOs: Hard caps at 1000-1800 range (±400 from start)\n")
cat("2. REDUCE VENUE K: Slow down venue learning to prevent drift\n")
cat("3. CONSIDER LOGISTIC: Natural bounding of predictions\n")
cat("\n")
