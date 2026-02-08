# Run All 3-Way ELO Optimizations
#
# Runs run ELO and wicket ELO optimization for all format-gender combinations.
#
# Format-Gender Combinations:
#   - Men's T20    (2.29M deliveries)
#   - Women's T20  (722K deliveries)
#   - Men's ODI    (2.13M deliveries)
#   - Women's ODI  (447K deliveries)
#   - Men's Test   (5.20M deliveries)
#   - Women's Test (44K deliveries - may need smaller train/test)
#
# Usage:
#   Rscript data-raw/ratings/player/3way-elo/optimization/00_run_all_optimizations.R
#
# Or run specific format-gender:
#   Rscript data-raw/ratings/player/3way-elo/optimization/02_optimize_run_elo.R t20 male
#   Rscript data-raw/ratings/player/3way-elo/optimization/03_optimize_wicket_elo.R t20 male

library(cli)

cat("\n")
cli::cli_h1("3-Way ELO Multi-Format Optimization")
cat("\n")

# Define all combinations
# Note: Women's Test has only ~44K deliveries, results may be noisy
combinations <- list(
  list(format = "t20",  gender = "male",   label = "Men's T20"),
  list(format = "t20",  gender = "female", label = "Women's T20"),
  list(format = "odi",  gender = "male",   label = "Men's ODI"),
  list(format = "odi",  gender = "female", label = "Women's ODI"),
  list(format = "test", gender = "male",   label = "Men's Test"),
  list(format = "test", gender = "female", label = "Women's Test")
)

# Track results
results_summary <- data.frame(
  combo = character(),
  run_train_improvement = numeric(),
  run_test_improvement = numeric(),
  wicket_train_improvement = numeric(),
  wicket_test_improvement = numeric(),
  stringsAsFactors = FALSE
)

# Process each combination
for (combo in combinations) {
  cli::cli_h2("{combo$label}")

  # Run ELO optimization
  cli::cli_alert_info("Running Run ELO optimization...")
  run_result <- tryCatch({
    system2("Rscript",
            args = c("data-raw/ratings/player/3way-elo/optimization/02_optimize_run_elo.R",
                     combo$format, combo$gender),
            stdout = TRUE, stderr = TRUE)
    "success"
  }, error = function(e) {
    cli::cli_alert_danger("Run ELO failed: {e$message}")
    "failed"
  })

  # Wicket ELO optimization
  cli::cli_alert_info("Running Wicket ELO optimization...")
  wicket_result <- tryCatch({
    system2("Rscript",
            args = c("data-raw/ratings/player/3way-elo/optimization/03_optimize_wicket_elo.R",
                     combo$format, combo$gender),
            stdout = TRUE, stderr = TRUE)
    "success"
  }, error = function(e) {
    cli::cli_alert_danger("Wicket ELO failed: {e$message}")
    "failed"
  })

  cli::cli_alert_success("{combo$label} complete (Run: {run_result}, Wicket: {wicket_result})")
  cat("\n")
}

# Final summary
cli::cli_h1("Optimization Complete")
cli::cli_alert_info("Results saved to bouncerdata/models/")
cli::cli_alert_info("Files: run_elo_params_{{gender}}_{{format}}.rds")
cli::cli_alert_info("       wicket_elo_params_{{gender}}_{{format}}.rds")
cat("\n")

cli::cli_h2("Next Steps")
cli::cli_bullets(c(
  "1" = "Review results in bouncerdata/models/",
  "2" = "Update R/constants_3way.R with format-gender-specific parameters",
  "3" = "Run full 3-way ELO calculation for each format-gender"
))
cat("\n")
