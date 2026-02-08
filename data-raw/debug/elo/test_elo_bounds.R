# Test Script: Verify ELO Bounds and Reduced Venue K Implementation
#
# This script tests the new bounded ELO system:
# 1. Player ELO bounds: 1000-1800
# 2. Venue ELO bounds: 1200-1600
# 3. Venue permanent K reduced by 10x
# 4. Optional logistic conversion

library(DBI)
library(data.table)
devtools::load_all()

cat("\n")
cli::cli_h1("ELO Bounds Implementation Test")
cat("\n")

# ============================================================================
# 1. TEST BOUNDS CONSTANTS
# ============================================================================

cli::cli_h2("1. Checking Constants")

cat("\n--- ELO Bounds ---\n")
cat(sprintf("Player bounds: %d to %d\n", THREE_WAY_PLAYER_ELO_MIN, THREE_WAY_PLAYER_ELO_MAX))
cat(sprintf("Venue bounds:  %d to %d\n", THREE_WAY_VENUE_ELO_MIN, THREE_WAY_VENUE_ELO_MAX))
cat(sprintf("Apply bounds:  %s\n", THREE_WAY_APPLY_BOUNDS))

cat("\n--- Venue K Reduction ---\n")
cat(sprintf("Venue perm K multiplier: %.2f (%.0fx reduction)\n",
            THREE_WAY_VENUE_PERM_K_MULTIPLIER,
            1 / THREE_WAY_VENUE_PERM_K_MULTIPLIER))

cat("\n--- Logistic Conversion ---\n")
cat(sprintf("Use logistic: %s\n", THREE_WAY_USE_LOGISTIC_CONVERSION))
cat(sprintf("Sensitivity:  %d ELO points\n", THREE_WAY_LOGISTIC_SENSITIVITY))
cat(sprintf("Max mult:     %.2f\n", THREE_WAY_LOGISTIC_MAX_MULTIPLIER))

# ============================================================================
# 2. TEST BOUNDING FUNCTIONS
# ============================================================================

cat("\n")
cli::cli_h2("2. Testing Bounding Functions")

test_elos <- c(500, 1000, 1200, 1400, 1600, 1800, 2000, 2500)

cat("\n--- Player Bounds ---\n")
cat(sprintf("%10s  %10s\n", "Input", "Bounded"))
for (elo in test_elos) {
  cat(sprintf("%10d  %10.0f\n", elo, bound_player_elo(elo)))
}

cat("\n--- Venue Bounds ---\n")
cat(sprintf("%10s  %10s\n", "Input", "Bounded"))
for (elo in test_elos) {
  cat(sprintf("%10d  %10.0f\n", elo, bound_venue_elo(elo)))
}

# ============================================================================
# 3. TEST LOGISTIC MULTIPLIER
# ============================================================================

cat("\n")
cli::cli_h2("3. Testing Logistic Multiplier")

elo_diffs <- seq(-600, 600, by = 100)

cat("\n--- Logistic Multiplier Values ---\n")
cat(sprintf("%10s  %12s  %15s\n", "ELO Diff", "Multiplier", "Effect"))
for (diff in elo_diffs) {
  mult <- calculate_logistic_multiplier(diff)
  effect <- sprintf("%.1f%% %s", abs(mult - 1) * 100, if (mult > 1) "increase" else "decrease")
  cat(sprintf("%+10d  %12.3f  %15s\n", diff, mult, effect))
}

# ============================================================================
# 4. TEST EXPECTED RUNS CALCULATION
# ============================================================================

cat("\n")
cli::cli_h2("4. Testing Expected Runs Calculation")

# Save current setting
original_logistic <- THREE_WAY_USE_LOGISTIC_CONVERSION

# Test scenarios
scenarios <- list(
  list(name = "All average", bat = 1400, bowl = 1400, v_perm = 1400, v_sess = 1400),
  list(name = "Good batter", bat = 1600, bowl = 1400, v_perm = 1400, v_sess = 1400),
  list(name = "Elite batter vs weak bowler", bat = 1800, bowl = 1200, v_perm = 1400, v_sess = 1400),
  list(name = "Weak batter vs elite bowler", bat = 1200, bowl = 1800, v_perm = 1400, v_sess = 1400),
  list(name = "High-scoring venue", bat = 1400, bowl = 1400, v_perm = 1600, v_sess = 1500),
  list(name = "Low-scoring venue", bat = 1400, bowl = 1400, v_perm = 1200, v_sess = 1300),
  list(name = "Extreme positive (bounded)", bat = 1800, bowl = 1000, v_perm = 1600, v_sess = 1600),
  list(name = "Extreme negative (bounded)", bat = 1000, bowl = 1800, v_perm = 1200, v_sess = 1200)
)

baseline <- 1.28  # T20 average runs per ball

cat("\n--- Linear Conversion (current) ---\n")
assign("THREE_WAY_USE_LOGISTIC_CONVERSION", FALSE, envir = .GlobalEnv)
cat(sprintf("%-35s  %10s  %10s\n", "Scenario", "Exp Runs", "Multiplier"))
for (s in scenarios) {
  exp_runs <- calculate_3way_expected_runs(baseline, s$bat, s$bowl, s$v_perm, s$v_sess, "t20", "male")
  cat(sprintf("%-35s  %10.3f  %10.2fx\n", s$name, exp_runs, exp_runs / baseline))
}

cat("\n--- Logistic Conversion ---\n")
assign("THREE_WAY_USE_LOGISTIC_CONVERSION", TRUE, envir = .GlobalEnv)
cat(sprintf("%-35s  %10s  %10s\n", "Scenario", "Exp Runs", "Multiplier"))
for (s in scenarios) {
  exp_runs <- calculate_3way_expected_runs(baseline, s$bat, s$bowl, s$v_perm, s$v_sess, "t20", "male")
  cat(sprintf("%-35s  %10.3f  %10.2fx\n", s$name, exp_runs, exp_runs / baseline))
}

# Restore original setting
assign("THREE_WAY_USE_LOGISTIC_CONVERSION", original_logistic, envir = .GlobalEnv)

# ============================================================================
# 5. TEST VENUE K-FACTOR REDUCTION
# ============================================================================

cat("\n")
cli::cli_h2("5. Testing Venue K-Factor Reduction")

test_balls <- c(0, 1000, 5000, 10000, 50000, 100000)

cat("\n--- Venue Permanent K-Factor (with 10x reduction) ---\n")
cat(sprintf("%12s  %10s  %15s\n", "Venue Balls", "K-Factor", "Original (10x)"))
for (balls in test_balls) {
  k <- get_3way_venue_perm_k(balls, "t20", "male")
  cat(sprintf("%12d  %10.3f  %15.2f\n", balls, k, k / THREE_WAY_VENUE_PERM_K_MULTIPLIER))
}

cat("\n--- Session K-Factor (unchanged) ---\n")
cat(sprintf("%12s  %10s\n", "Match Balls", "K-Factor"))
for (balls in c(0, 50, 100, 150, 200, 240)) {
  k <- get_3way_venue_session_k(balls, "t20", "male")
  cat(sprintf("%12d  %10.3f\n", balls, k))
}

# ============================================================================
# 6. SUMMARY
# ============================================================================

cat("\n")
cli::cli_h1("Implementation Summary")
cat("\n")

cat("CHANGES IMPLEMENTED:\n")
cat("1. Player ELO bounds: 1000-1800 (prevents extreme player ratings)\n")
cat("2. Venue ELO bounds: 1200-1600 (tighter, venues should be stable)\n")
cat("3. Venue permanent K reduced 10x (slows venue ELO drift)\n")
cat("4. Logistic conversion available (set THREE_WAY_USE_LOGISTIC_CONVERSION=TRUE)\n")
cat("\n")
cat("TO ENABLE BOUNDS: THREE_WAY_APPLY_BOUNDS <- TRUE (default)\n")
cat("TO ENABLE LOGISTIC: THREE_WAY_USE_LOGISTIC_CONVERSION <- TRUE\n")
cat("\n")
cat("NEXT STEPS:\n")
cat("1. Run diagnose_elo_formula.R to analyze current predictions\n")
cat("2. Run variance_decomposition.R to check component contributions\n")
cat("3. Run calibration_analysis.R to check prediction quality\n")
cat("4. Re-run 01_calculate_3way_elo.R with new parameters\n")
cat("5. Compare skill scores before/after\n")
cat("\n")
