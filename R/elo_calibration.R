# Calibration data for the ELO rebuilds.
#
# RESTORED 2026-08-20 (bouncerverse#63). `get_calibration_data()` was deleted
# on 2026-02-09 in commit 442f6ae as part of a dead-code sweep, alongside the
# genuinely dead `player_elo_*.R` files. It was not dead: eight scripts call
# it, including `01_calculate_3way_elo.R`, which is the ONLY thing that
# populates the 3-way ELO tables.
#
# So the rebuild has been unrunnable since that commit, and the tables have
# been frozen ever since -- `mens_t20_3way_elo` is rated to 2026-01-19, days
# before the deletion. Nothing failed loudly, because nothing tried to run it.
#
# The table it reads, `elo_calibration_metrics`, was never dropped and is
# still populated for all three formats.
#
# The lesson is the sweep's, not this function's: `data-raw/` is not covered by
# R CMD check, so a function only called from pipeline scripts looks unused to
# every automated check and to grep over `R/` alone.

#' Calibration Metrics for a Format
#'
#' Reads the per-format expected-value calibration written by
#' `data-raw/ratings/player/shared/01_calibrate_expected_values.R`.
#'
#' @param format Character. `"t20"`, `"odi"` or `"test"`.
#' @param conn A DBI connection.
#'
#' @return A list with `format`, `wicket_rate`, `mean_runs`,
#'   `mean_outcome_score` and `sample_size`, or `NULL` when the format has no
#'   calibration rows.
#' @keywords internal
get_calibration_data <- function(format = "t20", conn) {

  metrics <- DBI::dbGetQuery(conn, "
    SELECT metric_type, metric_key, metric_value, sample_size
    FROM elo_calibration_metrics
    WHERE format = ?
  ", params = list(format))

  if (nrow(metrics) == 0) {
    cli::cli_alert_warning("No calibration data found for format: {format}")
    cli::cli_alert_info("Run 01_calibrate_expected_values.R first")
    return(NULL)
  }

  pick <- function(type) metrics[metrics$metric_type == type &
                                   metrics$metric_key == "overall", ]
  wicket_row <- pick("wicket_rate")
  runs_row <- pick("mean_runs")
  outcome_row <- pick("mean_outcome_score")

  # The original fell back to BASE_WICKET_PROB_T20 -- for EVERY format, and
  # that constant was deleted by the same sweep, so the fallback would now
  # error rather than default. Per-format constants, which is what it meant.
  default_wicket <- switch(tolower(format),
    t20 = EXPECTED_WICKET_T20, odi = EXPECTED_WICKET_ODI,
    test = EXPECTED_WICKET_TEST, EXPECTED_WICKET_T20)

  # The nrow(metrics) == 0 guard above only catches TOTAL absence. A PARTIAL
  # row set -- say only wicket_rate stored -- returned a fully populated,
  # entirely plausible list with 1.3 and 0.25 silently substituted, and the
  # caller printed those fabricated numbers in a success line. Name what
  # defaulted.
  defaulted <- c("wicket_rate", "mean_runs", "mean_outcome_score")[
    c(nrow(wicket_row) == 0, nrow(runs_row) == 0, nrow(outcome_row) == 0)]
  if (length(defaulted)) {
    cli::cli_warn(c(
      "Calibration for {.val {format}} is missing {length(defaulted)} metric{?s}.",
      "x" = "Defaulted: {.val {defaulted}} -- these are hardcoded constants, not measurements.",
      "i" = "Re-run data-raw/ratings/player/shared/01_calibrate_expected_values.R."))
  }

  list(
    format = format,
    defaulted = defaulted,
    wicket_rate = if (nrow(wicket_row) > 0) wicket_row$metric_value else default_wicket,
    mean_runs = if (nrow(runs_row) > 0) runs_row$metric_value else 1.3,
    mean_outcome_score = if (nrow(outcome_row) > 0) outcome_row$metric_value else 0.25,
    sample_size = if (nrow(wicket_row) > 0) wicket_row$sample_size else 0
  )
}
