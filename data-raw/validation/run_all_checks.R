# Run All Validation Checks
#
# Executes all validation scripts and provides a summary report.
# Use this after pipeline runs or before releases.

library(DBI)
devtools::load_all()

cli::cli_h1("Running All Validation Checks")
cli::cli_alert_info("Started at: {Sys.time()}")

results <- list()

# Run each validation script
scripts <- c(
  "validate_pipeline_state.R",
  "validate_data_quality.R"
)

script_dir <- "data-raw/validation"

for (script in scripts) {
  script_path <- file.path(script_dir, script)
  if (file.exists(script_path)) {
    cli::cli_rule()
    source(script_path, local = TRUE)

    # Get the main function name (matches script name)
    fn_name <- gsub("\.R$", "", script)
    if (exists(fn_name, mode = "function")) {
      results[[script]] <- get(fn_name)()
    } else {
      cli::cli_alert_warning("No function {fn_name}() found in {script}")
      results[[script]] <- NA
    }
  } else {
    cli::cli_alert_danger("Script not found: {script_path}")
    results[[script]] <- FALSE
  }
}

# ============================================================================
# Summary Report
# ============================================================================
cli::cli_rule()
cli::cli_h1("Validation Summary")

passed <- sum(sapply(results, isTRUE))
failed <- sum(sapply(results, function(x) identical(x, FALSE)))
skipped <- sum(sapply(results, is.na))

cli::cli_alert_info("Total checks: {length(results)}")
if (passed > 0) cli::cli_alert_success("Passed: {passed}")
if (failed > 0) cli::cli_alert_danger("Failed: {failed}")
if (skipped > 0) cli::cli_alert_warning("Skipped: {skipped}")

cli::cli_alert_info("Completed at: {Sys.time()}")

if (failed > 0) {
  cli::cli_alert_danger("VALIDATION FAILED - Review issues above")
} else {
  cli::cli_alert_success("ALL VALIDATIONS PASSED")
}
