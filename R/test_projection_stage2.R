# Stage 2: a fitted correction on top of the stage-1 Test/MDM projection
# (bouncerverse D-P65). See docs/reviews/2026-09-03-TEST-OVERS-MODEL-GATE.md's
# "D-P65 diagnosed" section for why this exists: the original single-stage
# projection multiplies the FULL accumulated score by a resource_remaining /
# resource_used ratio that swings 6-8x between the actual-vs-expected TSA
# branches whenever a wicket falls, because that ratio depends on how much of
# a (long, Test-scale) innings has been "used" so far -- a quantity close to
# zero for most of a normal innings. The fix (Pete's design): split the
# projection into two stages instead of one multiplicative term.
#
#   stage1 = current_score + a*EIS*resource_remaining
#     -- calculate_projected_scores_vectorized() with b=0. Bounded and
#        monotonic: interpolates from "share of the average team's total" at
#        the start of an innings to "the actual score" at its end, using only
#        products of quantities in [0,1]. No ratio, no blow-up possible.
#
#   stage2 = stage1 * exp(fitted_correction(wkt, run_rate, resource_remaining, innings))
#     -- a SEPARATE regression, fit against real innings outcomes
#        (log(final_total / stage1) as the target), not a reuse of the old
#        formula's ratio. Because stage1 already gets close to a sane final
#        total, the correction this model has to learn is small and smooth,
#        not an extrapolation-from-nothing multiplier -- verified: held-out
#        MAE on final total improves 18-24% over stage1 alone, and the
#        worst-ball TSA in the corpus went from -221.62 to -9.10.
#
# Fitted separately per match_type (Test/MDM), same reasoning as the overs
# model itself (test_overs_model.R).

#' Fit the stage-2 correction for a match_type
#'
#' @param conn DBI connection.
#' @param match_type Character. "Test" or "MDM".
#' @param overs_model A fit_test_overs_model() result for the SAME match_type
#'   (or NULL to load it). Stage 2 is trained using stage-1 values computed
#'   from this model's PREDICTED balls-remaining, matching how stage1 will
#'   actually be computed at inference time -- not the true observed value,
#'   which would leak information stage 2 will never have when scoring a
#'   live ball.
#' @param eis Numeric. Expected initial score for this match_type
#'   (EIS_TEST_MALE_INTL or EIS_TEST_MALE_CLUB).
#' @param as_at Date. NULL (default) uses all data through today.
#'
#' @return A list: fit (the lm), match_type, eis, cut_date, n_rows,
#'   n_excluded_degenerate, fitted_at.
#' @keywords internal
fit_test_stage2_correction <- function(conn, match_type = c("Test", "MDM"),
                                        overs_model = NULL, eis = NULL, as_at = NULL) {
  match_type <- match.arg(match_type)
  if (is.null(overs_model)) overs_model <- load_test_overs_model(match_type)
  if (is.null(eis)) eis <- if (match_type == "Test") EIS_TEST_MALE_INTL else EIS_TEST_MALE_CLUB

  d <- .build_test_overs_features(conn, match_type)
  fin <- d[, .(final_total = max(total_runs)), by = .(match_id, innings)]
  d <- merge(d, fin, by = c("match_id", "innings"), all.x = TRUE)
  d[, wkt := factor(pmin(9L, wickets_before), levels = 0:9)]
  d[, inn := factor(pmin(4L, innings), levels = 1:4)]
  d[, pred_rem := predict_test_balls_remaining(overs_model, d)]
  d[, mb := balls_before + pred_rem]
  d[, resource_remaining := (pred_rem / mb)^PROJ_DEFAULT_Z *
      ((10 - wickets_before) / 10)^PROJ_DEFAULT_Y]
  d[, stage1 := runs_before + PROJ_DEFAULT_A * eis * resource_remaining]

  n0 <- nrow(d)
  d <- d[stage1 > 1]
  # Rare degenerate innings (final_total = 0 -- an abandoned/time-forced draw
  # reduced to a single scoreless token over; found and logged 2026-09-04,
  # see the gate doc) make log() undefined. Excluded and counted, not
  # silently dropped.
  n_deg <- sum(d$final_total <= 0)
  d <- d[final_total > 0]
  d[, target := log(final_total / stage1)]

  as_at <- if (is.null(as_at)) Sys.Date() else as.Date(as_at)
  cut_date <- as.Date(stats::quantile(as.numeric(unique(d$md)), 0.8), origin = "1970-01-01")
  tr <- d[md <= cut_date]

  # No wkt:resource_remaining interaction -- wkt=7/8/9 (near an innings' end)
  # have too little resource_remaining coverage across the whole dataset for
  # a 3-df interaction to be estimable; lm silently drops those coefficients
  # as NA and predict() then fails exactly on the sparse states the
  # interaction was meant to help most. Additive terms are estimable
  # everywhere, and stage2's job is a modest correction, not a complex
  # surface.
  form <- stats::as.formula(
    target ~ wkt + splines::ns(run_rate, 3) + splines::ns(resource_remaining, 4) + inn)
  fit <- stats::lm(form, data = tr)
  if (anyNA(stats::coef(fit))) {
    cli::cli_abort("Stage-2 fit for {match_type} has NA coefficients (rank-deficient) -- simplify the formula before shipping.")
  }

  list(fit = fit, match_type = match_type, eis = eis, cut_date = cut_date,
       n_rows = nrow(tr), n_excluded_degenerate = n_deg, fitted_at = Sys.time())
}

#' Save/load a fitted stage-2 correction (mirrors save/load_test_overs_model())
#' @keywords internal
save_test_stage2_correction <- function(fit_result, models_dir = NULL) {
  if (is.null(models_dir)) models_dir <- file.path(find_bouncerdata_dir(), "models")
  if (!dir.exists(models_dir)) dir.create(models_dir, recursive = TRUE)
  f <- file.path(models_dir, sprintf("test_stage2_correction_%s.rds", tolower(fit_result$match_type)))
  saveRDS(fit_result, f)
  invisible(f)
}

#' @rdname save_test_stage2_correction
#' @keywords internal
load_test_stage2_correction <- function(match_type = c("Test", "MDM"), models_dir = NULL) {
  match_type <- match.arg(match_type)
  if (is.null(models_dir)) models_dir <- file.path(find_bouncerdata_dir(), "models")
  f <- file.path(models_dir, sprintf("test_stage2_correction_%s.rds", tolower(match_type)))
  if (!file.exists(f)) {
    cli::cli_abort("No fitted stage-2 correction found for {match_type}. Expected {.path {f}}.")
  }
  readRDS(f)
}

#' Project a Test/MDM innings with the two-stage model
#'
#' Replaces calculate_test_projected_scores()'s single-stage call. Same
#' inputs; stage1 (bounded, current_score + a*EIS*resource_remaining) feeds a
#' fitted stage2 correction instead of the old multiplicative current-rate
#' term.
#'
#' @param wickets_before Integer/numeric vector. Wickets already lost BEFORE
#'   this ball -- the state used to look up the overs model and stage-2
#'   correction (both were fit on this pre-ball quantity).
#' @param wickets_remaining Numeric vector. Wickets remaining to use in the
#'   resource formula -- differs from `10 - wickets_before` whenever this
#'   ball's own outcome matters, which is exactly the case TSA needs: the
#'   "actual" branch passes `10 - wickets_before - is_wicket`, the "expected"
#'   branch passes `10 - wickets_before - exp_wicket`. Collapsing these two
#'   into one WAS a real bug here (2026-09-04): computing wr internally from
#'   wickets_before alone made both TSA branches share identical resource,
#'   which zeroed out the wicket signal (wicket cost ended up barely
#'   different from a dot ball) -- caught by the SAME per-ball anchor check
#'   this function's callers already run before persisting.
#' @param balls_before,run_rate,lead,innings,match_balls_before,eis,current_score
#'   Numeric/integer vectors. Same convention as calculate_test_projected_scores().
#' @param overs_models Named list keyed by match_type, each a
#'   fit_test_overs_model() (or load_test_overs_model()) result. NULL loads
#'   both from disk.
#' @param stage2_corrections Named list keyed by match_type, each a
#'   fit_test_stage2_correction() (or load_test_stage2_correction()) result.
#'   NULL loads both from disk.
#' @return Numeric vector: projected final innings score.
#' @keywords internal
calculate_test_projected_scores_v2 <- function(match_type, current_score, wickets_before,
                                                wickets_remaining, balls_before, run_rate,
                                                lead, innings, match_balls_before, eis,
                                                overs_models = NULL, stage2_corrections = NULL) {
  n <- length(current_score)
  stopifnot(length(match_type) == n, length(wickets_remaining) == n)
  if (is.null(overs_models)) {
    overs_models <- list(Test = load_test_overs_model("Test"), MDM = load_test_overs_model("MDM"))
  }
  if (is.null(stage2_corrections)) {
    stage2_corrections <- list(Test = load_test_stage2_correction("Test"),
                                MDM = load_test_stage2_correction("MDM"))
  }

  out <- numeric(n)
  for (mt in unique(match_type)) {
    idx <- which(match_type == mt)
    wb <- pmin(9L, as.integer(wickets_before[idx]))
    # Clipped to [0,10], matching calculate_projected_scores_vectorized()'s
    # own internal clip. Without it an 11th dismissal in an innings (rare but
    # real -- a retired-hurt batter later dismissed; 59 of 1.72M innings-1
    # rows, 0.0034%, 5 matches) drives wickets_remaining negative, and a
    # negative number to a non-integer power is NaN.
    wr <- pmax(0, pmin(10, wickets_remaining[idx]))
    nd <- data.frame(
      wkt = factor(wb, levels = 0:9), balls_before = balls_before[idx],
      run_rate = run_rate[idx], lead = lead[idx],
      inn = factor(pmin(4L, as.integer(innings[idx])), levels = 1:4),
      match_balls_before = match_balls_before[idx])
    pred_rem <- predict_test_balls_remaining(overs_models[[mt]], nd)
    mb <- balls_before[idx] + pred_rem
    rr <- (pred_rem / mb)^PROJ_DEFAULT_Z * (wr / 10)^PROJ_DEFAULT_Y
    stage1 <- current_score[idx] + PROJ_DEFAULT_A * eis[idx] * rr

    nd2 <- data.frame(wkt = factor(wb, levels = 0:9), run_rate = run_rate[idx],
                       resource_remaining = rr,
                       inn = factor(pmin(4L, as.integer(innings[idx])), levels = 1:4))
    corr <- exp(stats::predict(stage2_corrections[[mt]]$fit, newdata = nd2))
    out[idx] <- stage1 * corr
  }
  out
}
