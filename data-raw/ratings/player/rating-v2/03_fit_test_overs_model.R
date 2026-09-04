# Fit and persist the Test/MDM expected-overs model (bouncerverse D-P51 follow-on).
#
# Test and first-class cricket have no fixed ball allocation, so
# calculate_projected_scores_vectorized()'s max_balls has no natural value.
# This fits E[balls remaining | state] separately per cricsheet match_type
# ("Test" vs "MDM"), on the recency window each type's gate run selected
# (Test 5 years, MDM 8 years -- NOT re-swept here, see test_overs_model.R's
# docs for why), and saves the fitted models to
# <bouncerdata>/models/test_overs_model_{test,mdm}.rds.
#
# Design, gate criteria, and the two rejected + one accepted hypothesis for the
# declaration-timing problem: bouncerverse docs/plans/TEST-TSA-EXPECTED-OVERS-PREDECLARATION.md
# and docs/reviews/2026-09-03-TEST-OVERS-MODEL-GATE.md. The gate was run on a
# HELD-OUT era (fit on data before a chronological cut, score after it). This
# script does the production fit: same formula, same per-type window, but using
# ALL data through today, since there is no longer a held-out question to answer
# -- the design already cleared its gate.
#
# Usage: Rscript data-raw/ratings/player/rating-v2/03_fit_test_overs_model.R
# Under PowerShell on Windows, since arrow/duckdb segfault under Git Bash R.

suppressPackageStartupMessages({
  library(DBI)
  devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE)
})
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

for (mt in c("Test", "MDM")) {
  cat(sprintf("Fitting %s overs model (%.0f-year window)...\n", mt, .TEST_OVERS_WINDOW_YEARS[[mt]]))
  fit <- fit_test_overs_model(conn, match_type = mt)
  path <- save_test_overs_model(fit)
  cat(sprintf(
    "  %-4s: %s rows / %s innings, window ending %s, saved to %s\n",
    mt, format(fit$n_rows, big.mark = ","), format(fit$n_innings, big.mark = ","),
    fit$as_at, path
  ))

  # Sanity check, not the full gate: coefficients on wkt should show the obvious
  # monotone shape (more wickets down -> less predicted resource remaining at
  # the same point in the innings), since a badly misspecified fit can still
  # converge without producing that. Evaluated at the median balls_before/
  # run_rate/lead/match_balls_before in the fitting data, innings 1.
  d <- .build_test_overs_features(conn, mt)
  as_at <- fit$as_at
  tr <- d[md <= as_at & md > fit$cut_date & innings == 1L]
  ref <- data.frame(
    balls_before = stats::median(tr$balls_before),
    run_rate = stats::median(tr$run_rate),
    lead = 0,
    inn = factor(1, levels = 1:4),
    match_balls_before = stats::median(tr$match_balls_before)
  )
  ref <- ref[rep(1, 10), ]
  ref$wkt <- factor(0:9, levels = 0:9)
  pred <- predict_test_balls_remaining(fit, ref)
  monotone <- all(diff(pred) <= 1e-6)  # non-increasing, small numeric slack
  cat(sprintf("  sanity: predicted balls remaining by wickets down: %s\n",
              paste(round(pred), collapse = ", ")))
  if (!monotone) {
    cli::cli_abort(c(
      "{mt} overs model failed the monotonicity sanity check.",
      "i" = "Predicted balls remaining did not decrease as wickets_before increased at a fixed point in the innings.",
      "i" = "This means the fit is unreliable, not just imprecise -- do not ship it. Predictions: {paste(round(pred), collapse = ', ')}"
    ))
  }
  cat("  sanity: PASS (monotone in wickets down)\n\n")
}

cat("Done. Both models saved.\n")
