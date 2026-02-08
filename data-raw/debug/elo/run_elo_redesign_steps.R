# 3-Way ELO System: Diagnosis and Redesign Execution Script
#
# Run this script from RStudio to execute the remaining steps in the ELO redesign plan.
# The plan implements bounded ELOs and reduced venue K to fix extreme ELO drift.
#
# IMPORTANT: Run this from RStudio (not Rscript) to avoid DuckDB segfaults.
#
# Status: Solution 1 (Bounded ELOs + Reduced Venue K) implemented in code.
#         Need to re-run calculation and measure improvements.

library(DBI)
library(data.table)
devtools::load_all()

cat("\n")
cli::cli_h1("3-Way ELO Redesign: Execution Steps")
cat("\n")

# ============================================================================
# STEP 0: VERIFY IMPLEMENTATION IS IN PLACE
# ============================================================================

cli::cli_h2("Step 0: Verify Implementation")

cat("\nChecking constants...\n")
cat(sprintf("  Player ELO bounds: %d - %d\n", THREE_WAY_PLAYER_ELO_MIN, THREE_WAY_PLAYER_ELO_MAX))
cat(sprintf("  Venue ELO bounds:  %d - %d\n", THREE_WAY_VENUE_ELO_MIN, THREE_WAY_VENUE_ELO_MAX))
cat(sprintf("  Apply bounds: %s\n", THREE_WAY_APPLY_BOUNDS))
cat(sprintf("  Venue perm K multiplier: %.2f (%.0fx reduction)\n",
            THREE_WAY_VENUE_PERM_K_MULTIPLIER,
            1 / THREE_WAY_VENUE_PERM_K_MULTIPLIER))
cat(sprintf("  Logistic conversion: %s\n", THREE_WAY_USE_LOGISTIC_CONVERSION))

if (!THREE_WAY_APPLY_BOUNDS) {
  cli::cli_alert_danger("ELO bounds are DISABLED! Set THREE_WAY_APPLY_BOUNDS <- TRUE")
  stop("Fix configuration before continuing")
}

cli::cli_alert_success("Implementation verified - bounds and K reduction in place")

# ============================================================================
# STEP 1: ANALYZE CURRENT (OLD) DATA
# ============================================================================

cat("\n")
cli::cli_h2("Step 1: Analyze Current Data (Before Re-calculation)")

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

# Get current ELO statistics
current_stats <- DBI::dbGetQuery(conn, "
  SELECT
    'mens_t20' as format,
    MIN(venue_perm_run_elo_before) as min_venue_elo,
    MAX(venue_perm_run_elo_before) as max_venue_elo,
    MAX(venue_perm_run_elo_before) - MIN(venue_perm_run_elo_before) as venue_range,
    MIN(batter_run_elo_before) as min_batter_elo,
    MAX(batter_run_elo_before) as max_batter_elo,
    MAX(batter_run_elo_before) - MIN(batter_run_elo_before) as batter_range,
    AVG(ABS(exp_runs - actual_runs)) as mae,
    COUNT(*) as n
  FROM mens_t20_3way_elo
")

cat("\n--- Current Data Statistics (Men's T20) ---\n")
cat(sprintf("  Venue ELO range:  %.0f to %.0f (span: %.0f points)\n",
            current_stats$min_venue_elo, current_stats$max_venue_elo, current_stats$venue_range))
cat(sprintf("  Batter ELO range: %.0f to %.0f (span: %.0f points)\n",
            current_stats$min_batter_elo, current_stats$max_batter_elo, current_stats$batter_range))
cat(sprintf("  Run prediction MAE: %.4f\n", current_stats$mae))
cat(sprintf("  Total deliveries: %s\n", format(current_stats$n, big.mark = ",")))

# Check if data needs recalculation
needs_recalc <- current_stats$venue_range > 1000 || current_stats$batter_range > 1000

if (needs_recalc) {
  cli::cli_alert_warning("Current data has extreme ELO ranges - recalculation needed!")
} else {
  cli::cli_alert_success("ELO ranges are within bounds - data may already be recalculated")
}

DBI::dbDisconnect(conn, shutdown = TRUE)

# ============================================================================
# STEP 2: RE-RUN 3-WAY ELO CALCULATION
# ============================================================================

cat("\n")
cli::cli_h2("Step 2: Re-run 3-Way ELO Calculation")

if (needs_recalc) {
  cat("\nTo recalculate 3-Way ELOs with bounded values, run:\n")
  cat("\n  source('data-raw/ratings/player/3way-elo/01_calculate_3way_elo.R')\n")
  cat("\nOr for specific format:\n")
  cat("  Set at top of script: GENDER_FILTER <- 'mens'; FORMAT_FILTER <- 't20'\n")
  cat("\nWARNING: Full recalculation takes ~30-60 minutes for men's T20.\n")
  cat("\nProceed with recalculation? (set run_recalc <- TRUE to execute)\n")

  run_recalc <- FALSE  # User must explicitly enable

  if (run_recalc) {
    cli::cli_alert_info("Starting 3-Way ELO recalculation...")
    source("data-raw/ratings/player/3way-elo/01_calculate_3way_elo.R")
  }
} else {
  cli::cli_alert_info("Skipping recalculation - data appears to be bounded already")
}

# ============================================================================
# STEP 3: MEASURE IMPROVEMENTS
# ============================================================================

cat("\n")
cli::cli_h2("Step 3: Measure Improvements")

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

# After recalculation, check the new statistics
new_stats <- DBI::dbGetQuery(conn, "
  SELECT
    MIN(venue_perm_run_elo_before) as min_venue_elo,
    MAX(venue_perm_run_elo_before) as max_venue_elo,
    MAX(venue_perm_run_elo_before) - MIN(venue_perm_run_elo_before) as venue_range,
    MIN(batter_run_elo_before) as min_batter_elo,
    MAX(batter_run_elo_before) as max_batter_elo,
    MAX(batter_run_elo_before) - MIN(batter_run_elo_before) as batter_range,
    AVG(ABS(exp_runs - actual_runs)) as elo_mae,
    COUNT(*) as n
  FROM mens_t20_3way_elo
")

cat("\n--- Post-Recalculation Statistics ---\n")
cat(sprintf("  Venue ELO range:  %.0f to %.0f (span: %.0f)\n",
            new_stats$min_venue_elo, new_stats$max_venue_elo, new_stats$venue_range))
cat(sprintf("  Batter ELO range: %.0f to %.0f (span: %.0f)\n",
            new_stats$min_batter_elo, new_stats$max_batter_elo, new_stats$batter_range))
cat(sprintf("  Run prediction MAE: %.4f\n", new_stats$elo_mae))

# Get baseline (agnostic) MAE for comparison
# Note: agnostic_runs is not in the 3way_elo table, so we calculate baseline differently
baseline_mae <- DBI::dbGetQuery(conn, "
  SELECT AVG(ABS(actual_runs - 1.28)) as baseline_mae  -- 1.28 is T20 baseline
  FROM mens_t20_3way_elo
")$baseline_mae

mae_skill <- (1 - new_stats$elo_mae / baseline_mae) * 100

cat(sprintf("\n  Baseline MAE (constant prediction): %.4f\n", baseline_mae))
cat(sprintf("  ELO MAE improvement: %.2f%%\n", mae_skill))

DBI::dbDisconnect(conn, shutdown = TRUE)

# ============================================================================
# STEP 4: SUCCESS CRITERIA CHECK
# ============================================================================

cat("\n")
cli::cli_h2("Step 4: Success Criteria")

targets <- data.frame(
  Metric = c("Venue ELO range", "Player ELO range", "Run MAE skill"),
  Target = c("<600", "<800", ">3%"),
  Actual = c(
    sprintf("%.0f", new_stats$venue_range),
    sprintf("%.0f", new_stats$batter_range),
    sprintf("%.2f%%", mae_skill)
  ),
  Status = c(
    if (new_stats$venue_range < 600) "PASS" else "FAIL",
    if (new_stats$batter_range < 800) "PASS" else "FAIL",
    if (mae_skill > 3) "PASS" else "FAIL"
  )
)

print(targets)

all_pass <- all(targets$Status == "PASS")

if (all_pass) {
  cli::cli_alert_success("All success criteria met!")
} else {
  cli::cli_alert_warning("Some criteria not met - consider additional solutions")
  cat("\nIf improvements are insufficient, consider:\n")
  cat("  1. Enable logistic conversion: THREE_WAY_USE_LOGISTIC_CONVERSION <- TRUE\n")
  cat("  2. Further reduce venue K: THREE_WAY_VENUE_PERM_K_MULTIPLIER <- 0.05\n")
  cat("  3. Tighten venue bounds: THREE_WAY_VENUE_ELO_MIN/MAX\n")
}

cat("\n")
cli::cli_h1("Execution Complete")
cat("\n")
