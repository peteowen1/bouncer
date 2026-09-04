# Fit and persist the stage-2 Test/MDM projection correction (D-P65 fix).
#
# Run AFTER 03_fit_test_overs_model.R -- stage 2 is trained using stage-1
# values computed from the ALREADY-FITTED overs model's predictions, so it
# has to exist first.
#
# Design and why: bouncer/R/test_projection_stage2.R's header, and
# bouncerverse docs/reviews/2026-09-03-TEST-OVERS-MODEL-GATE.md's "D-P65
# diagnosed" section.
#
# Usage: Rscript data-raw/ratings/player/rating-v2/05_fit_test_stage2_correction.R
# Under PowerShell on Windows, since arrow/duckdb segfault under Git Bash R.

suppressPackageStartupMessages({
  library(DBI)
  devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE)
})
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

for (mt in c("Test", "MDM")) {
  cat(sprintf("Fitting %s stage-2 correction...\n", mt))
  om <- load_test_overs_model(mt)
  eis <- if (mt == "Test") EIS_TEST_MALE_INTL else EIS_TEST_MALE_CLUB
  fit <- fit_test_stage2_correction(conn, match_type = mt, overs_model = om, eis = eis)
  path <- save_test_stage2_correction(fit)
  cat(sprintf("  %-4s: %s rows (%d degenerate excluded), saved to %s\n",
              mt, format(fit$n_rows, big.mark = ","), fit$n_excluded_degenerate, path))

  # Sanity: held-out MAE on the true final total should beat stage1 alone.
  # This is a lower bar than the full anchor/rank gate (run separately after
  # persisting), but it catches an obviously broken fit before it reaches
  # that stage.
  d <- .build_test_overs_features(conn, mt)
  fin <- d[, .(final_total = max(total_runs)), by = .(match_id, innings)]
  d <- merge(d, fin, by = c("match_id", "innings"), all.x = TRUE)
  d[, wkt := factor(pmin(9L, wickets_before), levels = 0:9)]
  d[, inn := factor(pmin(4L, innings), levels = 1:4)]
  d[, pred_rem := predict_test_balls_remaining(om, d)]
  d[, mb := balls_before + pred_rem]
  d[, resource_remaining := (pred_rem / mb)^PROJ_DEFAULT_Z * ((10 - wickets_before) / 10)^PROJ_DEFAULT_Y]
  d[, stage1 := runs_before + PROJ_DEFAULT_A * eis * resource_remaining]
  te <- d[md > fit$cut_date & stage1 > 1 & final_total > 0]
  te[, pred_target := predict(fit$fit, newdata = te)]
  te[, pred_final := stage1 * exp(pred_target)]
  mae1 <- mean(abs(te$stage1 - te$final_total))
  mae2 <- mean(abs(te$pred_final - te$final_total))
  cat(sprintf("  sanity: held-out MAE on final total, stage1 alone %.1f -> stage1+stage2 %.1f (%.1f%%)\n",
              mae1, mae2, 100 * (mae2 - mae1) / mae1))
  if (mae2 >= mae1) {
    cli::cli_abort("{mt} stage-2 correction does not beat stage1 alone on held-out data -- do not ship it.")
  }
}
cat("\nDone. Both stage-2 corrections saved.\n")
