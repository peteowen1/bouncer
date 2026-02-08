# Run Simulation Validation ----
#
# Standalone runner script for the 3-Way ELO simulation validation.
# Run from bouncer directory:
#   Rscript data-raw/ratings/player/3way-elo/simulation/run_simulation.R
#
# Or source in R:
#   source("data-raw/ratings/player/3way-elo/simulation/run_simulation.R")

# Setup
library(data.table)
devtools::load_all()

# Source simulation components
sim_dir <- "data-raw/ratings/player/3way-elo/simulation"
source(file.path(sim_dir, "00_simulation_config.R"))
source(file.path(sim_dir, "01_generate_synthetic_data.R"))
source(file.path(sim_dir, "02_run_elo_on_synthetic.R"))
source(file.path(sim_dir, "03_validate_recovery.R"))
source(file.path(sim_dir, "04_run_full_simulation.R"))

# Run the simulation
results <- run_full_simulation(n_simulations = 1, verbose = TRUE, save_results = FALSE)

# Print final summary
cat("\n\n")
cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║                    FINAL SUMMARY                               ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n")
cat("\n")

cat(sprintf("Overall success: %s\n", if (results$success) "YES" else "NO"))
cat("\nCorrelations (True Skill vs Recovered ELO):\n")
for (name in names(results$validation$correlations)) {
  threshold <- if (grepl("venue", name)) SIM_MIN_VENUE_CORRELATION else SIM_MIN_CORRELATION
  cor_val <- results$validation$correlations[[name]]
  status <- if (cor_val >= threshold) "PASS" else "FAIL"
  cat(sprintf("  %-20s %8.3f  (threshold: %.2f)  %s\n", name, cor_val, threshold, status))
}

cat("\nTest Set Performance:\n")
cat(sprintf("  Run Poisson loss improvement:    %+.2f%%\n", results$test_metrics$run_improvement_pct))
cat(sprintf("  Wicket log-loss improvement:     %+.2f%%\n", results$test_metrics$wicket_improvement_pct))

cat("\nInterpretation:\n")
if (results$success) {
  cat("  The ELO system correctly identifies player and venue skills.\n")
  cat("  Correlations > 0.5 indicate strong skill recovery.\n")
} else {
  cat("  Some skill dimensions were not recovered successfully.\n")
  cat("  Check the validation report above for diagnostics.\n")
}

cat("\n")
