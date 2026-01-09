# Pipeline Helper Functions ----
#
# Internal functions used by run_full_pipeline.R
# These are not exported but available after devtools::load_all()

#' Check if a pipeline step should run
#'
#' @param step Step number to check
#' @param steps_to_run Vector of steps to run, or NULL for all
#' @return TRUE if step should run
#' @keywords internal
pipeline_should_run <- function(step, steps_to_run = NULL) {

  is.null(steps_to_run) || step %in% steps_to_run
}

#' Print step completion with timing
#'
#' @param step_name Name of the step
#' @param step_time difftime object with step duration
#' @keywords internal
pipeline_step_complete <- function(step_name, step_time) {
  mins <- round(as.numeric(step_time), 1)
  cat("\n")
  cli::cli_rule()
  cli::cli_alert_success("{step_name} COMPLETE - Duration: {mins} minutes")
  cli::cli_rule()
  cat("\n")
}
