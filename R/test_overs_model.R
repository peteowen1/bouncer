# Test/first-class expected-overs model (bouncerverse D-P51 follow-on, #84-adjacent
# design work). Predicts E[balls remaining] for a Test/MDM innings so
# calculate_projected_scores_vectorized()'s Duckworth-Lewis-shaped resource can be
# used where there is no fixed ball allocation. Full design, gate criteria and
# both failed/passed hypotheses: bouncerverse docs/plans/TEST-TSA-EXPECTED-OVERS-PREDECLARATION.md
# and docs/reviews/2026-09-03-TEST-OVERS-MODEL-GATE.md.
#
# Fitted SEPARATELY per cricsheet match_type ("Test" vs "MDM"), not pooled with a
# type term. Test innings length collapsed 2021-2024 (Bazball); MDM shows no era
# drift over the same span. A pooled fit would force one recency treatment onto
# both. Recency window per type was chosen by sweeping it as a nuisance axis on a
# held-out era: Test 5 years, MDM 8 years (see the gate doc). Male only -- Test
# female is 24 matches with 3 players over 500 balls in innings 1, too thin for
# an honest fit.

#' Recency window (years) chosen by the gate, per match_type
#'
#' Not a tunable default -- see fit_test_overs_model()'s docs for why.
#' @keywords internal
.TEST_OVERS_WINDOW_YEARS <- list(Test = 5, MDM = 8)

#' Formula for the Test/MDM expected-balls-remaining model
#'
#' Single source of truth so the fitting script and any refit share it exactly.
#' `wkt` (wickets already lost, as a factor 0-9) interacts with a spline on how
#' far the innings has run, since the marginal value of an over differs by how
#' many wickets are in hand. `match_balls_before` (balls bowled in the MATCH so
#' far, across all completed innings) interacted with `lead` is the closest this
#' linear/spline model gets to declaration intent -- both were tested and found
#' to move the aggregate metric only marginally; kept because they cost nothing
#' and the aggregate metric is not what a batter-level rating actually needs (see
#' the gate doc's "why the framing is probably wrong" section).
#'
#' @return A formula object.
#' @keywords internal
.test_overs_formula <- function() {
  stats::as.formula(
    balls_remaining ~ wkt * splines::ns(balls_before, 5) +
      splines::ns(run_rate, 3) + splines::ns(lead, 4) + inn +
      splines::ns(match_balls_before, 5) +
      splines::ns(lead, 3):splines::ns(match_balls_before, 3)
  )
}

#' Build the per-ball feature frame for the Test overs model
#'
#' Pulls every completed Test/MDM delivery (male, any innings <= 4,
#' wickets_before clipped to 0-9 since the 10th wicket ends the innings) and
#' derives every pre-ball state column from the row's OWN outcomes -- verified
#' empirically that total_runs/wickets_fallen in cricsheet.deliveries are
#' POST-ball inclusive, so pre-ball state is never an adjacent-row LAG.
#'
#' @param conn DBI connection.
#' @param match_type Character. "Test" or "MDM".
#' @return A data.table, one row per delivery, ready for .test_overs_formula().
#' @keywords internal
.build_test_overs_features <- function(conn, match_type = c("Test", "MDM")) {
  match_type <- match.arg(match_type)

  d <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    WITH base AS (
      SELECT d.match_id, d.innings, d.match_date,
             d.delivery_id, d.total_runs, d.runs_total, d.is_wicket, d.wickets_fallen,
             ROW_NUMBER() OVER (PARTITION BY d.match_id, d.innings ORDER BY d.delivery_id) AS ball_index,
             COUNT(*)     OVER (PARTITION BY d.match_id, d.innings) AS innings_balls
      FROM cricsheet.deliveries d
      JOIN cricsheet.matches m ON m.match_id = d.match_id
      WHERE m.match_type = '%s' AND m.gender = 'male'
    ) SELECT * FROM base", match_type)))

  d[, runs_before := total_runs - runs_total]
  d[, wickets_before := wickets_fallen - as.integer(is_wicket)]
  d[, balls_before := ball_index - 1L]
  d[, balls_remaining := innings_balls - ball_index]
  d <- d[innings <= 4L & wickets_before <= 9L]
  d[, md := as.Date(match_date)]

  inn_tot <- d[, .(inn_runs = max(total_runs)), by = .(match_id, innings)]
  data.table::setorder(inn_tot, match_id, innings)
  inn_tot[, cum_prior := cumsum(inn_runs) - inn_runs, by = match_id]
  d <- merge(d, inn_tot[, .(match_id, innings, cum_prior)],
             by = c("match_id", "innings"), all.x = TRUE)
  d[, lead := ifelse(innings == 1L, 0, runs_before - cum_prior)]
  d[, run_rate := ifelse(balls_before > 0, runs_before / balls_before, 0)]

  data.table::setorder(d, match_id, innings, delivery_id)
  inn_len <- d[, .(n = .N), by = .(match_id, innings)]
  data.table::setorder(inn_len, match_id, innings)
  inn_len[, prior_match_balls := cumsum(n) - n, by = match_id]
  d <- merge(d, inn_len[, .(match_id, innings, prior_match_balls)],
             by = c("match_id", "innings"), all.x = TRUE)
  d[, match_balls_before := prior_match_balls + balls_before]

  d[, wkt := factor(wickets_before, levels = 0:9)]
  d[, inn := factor(innings, levels = 1:4)]
  d
}

#' Fit the Test/MDM expected-balls-remaining model
#'
#' Fits on a trailing recency window ending at as_at (default: all data through
#' today, i.e. a live/production fit rather than a held-out gate run). The
#' window length is NOT re-swept here -- it was chosen once, on a held-out era,
#' in the gate (docs/reviews/2026-09-03-TEST-OVERS-MODEL-GATE.md), and
#' re-sweeping on every production refit would let the window silently drift
#' with whatever the most recent data happens to favour, which defeats the
#' point of having gated it.
#'
#' @param conn DBI connection.
#' @param match_type Character. "Test" or "MDM".
#' @param window_years Numeric. Trailing window length in years. Default is the
#'   value the gate selected for that match_type (Test 5, MDM 8) --
#'   .TEST_OVERS_WINDOW_YEARS.
#' @param as_at Date. Fit using only matches up to and including this date.
#'   NULL (default) uses all data through today, i.e. a production fit.
#'
#' @return A list: model (the fitted lm), match_type, window_years, as_at,
#'   cut_date (window start), n_rows, n_innings, fitted_at.
#' @keywords internal
fit_test_overs_model <- function(conn, match_type = c("Test", "MDM"),
                                  window_years = NULL, as_at = NULL) {
  match_type <- match.arg(match_type)
  if (is.null(window_years)) window_years <- .TEST_OVERS_WINDOW_YEARS[[match_type]]

  d <- .build_test_overs_features(conn, match_type)
  as_at <- if (is.null(as_at)) Sys.Date() else as.Date(as_at)
  cut_date <- as_at - window_years * 365.25
  tr <- d[md <= as_at & md > cut_date]

  if (nrow(tr) < 5e4 || data.table::uniqueN(tr$wkt) < 10) {
    cli::cli_abort(c(
      "Too little data to fit the {match_type} overs model.",
      "i" = "{nrow(tr)} rows, {data.table::uniqueN(tr$wkt)} distinct wicket states in the {window_years}-year window ending {as_at}."
    ))
  }

  model <- stats::lm(.test_overs_formula(), data = tr)

  list(
    model = model, match_type = match_type, window_years = window_years,
    as_at = as_at, cut_date = cut_date,
    n_rows = nrow(tr), n_innings = data.table::uniqueN(tr[, paste(match_id, innings)]),
    fitted_at = Sys.time()
  )
}

#' Predict expected balls remaining
#'
#' Thin wrapper: predict.lm() plus the floor at zero (a spline can extrapolate
#' negative near an innings' natural end).
#'
#' @param fit_result A fit_test_overs_model() result, or a bare lm.
#' @param newdata data.frame/data.table with the columns .test_overs_formula()
#'   needs: wkt, balls_before, run_rate, lead, inn, match_balls_before.
#' @return Numeric vector, same length as nrow(newdata).
#' @keywords internal
predict_test_balls_remaining <- function(fit_result, newdata) {
  model <- if (is.list(fit_result) && !is.null(fit_result$model)) fit_result$model else fit_result
  pmax(0, stats::predict(model, newdata = newdata))
}

#' Save a fitted Test overs model
#'
#' Mirrors save_projection_params()'s file convention: one RDS per segment
#' under bouncerdata/models/.
#'
#' @param fit_result A fit_test_overs_model() result.
#' @param models_dir Character. NULL uses bouncerdata/models/.
#' @return (Invisibly) the file path written to.
#' @keywords internal
save_test_overs_model <- function(fit_result, models_dir = NULL) {
  if (is.null(models_dir)) models_dir <- file.path(find_bouncerdata_dir(), "models")
  if (!dir.exists(models_dir)) dir.create(models_dir, recursive = TRUE)
  f <- file.path(models_dir, sprintf("test_overs_model_%s.rds", tolower(fit_result$match_type)))
  saveRDS(fit_result, f)
  invisible(f)
}

#' Load a fitted Test overs model
#'
#' @param match_type Character. "Test" or "MDM".
#' @param models_dir Character. NULL uses bouncerdata/models/.
#' @return A fit_test_overs_model() result.
#' @keywords internal
load_test_overs_model <- function(match_type = c("Test", "MDM"), models_dir = NULL) {
  match_type <- match.arg(match_type)
  if (is.null(models_dir)) models_dir <- file.path(find_bouncerdata_dir(), "models")
  f <- file.path(models_dir, sprintf("test_overs_model_%s.rds", tolower(match_type)))
  if (!file.exists(f)) {
    cli::cli_abort(c(
      "No fitted Test overs model found for {match_type}.",
      "i" = "Expected {.path {f}}. Run fit_test_overs_model() + save_test_overs_model() first."
    ))
  }
  readRDS(f)
}

#' Project a Test/MDM innings using the expected-overs resource model
#'
#' Test and first-class cricket have no fixed ball allocation, so
#' calculate_projected_scores_vectorized()'s max_balls argument has no natural
#' value. This supplies one: predict the expected balls remaining from the fitted
#' per-match_type model, then call the same projection function used for
#' T20/ODI with max_balls = balls_before + predicted_remaining and
#' balls_remaining = predicted_remaining.
#'
#' @param match_type Character vector, "Test" or "MDM" per row.
#' @param current_score,wickets_remaining Numeric. Current state, same
#'   convention as calculate_projected_scores_vectorized().
#' @param wickets_before,balls_before,run_rate,lead,innings,match_balls_before
#'   Numeric/integer vectors. Pre-ball feature state (see
#'   .build_test_overs_features() for how each is derived).
#' @param eis Numeric. Expected initial score. Vector or scalar.
#' @param overs_models Named list keyed by match_type, each a
#'   fit_test_overs_model() result (or load_test_overs_model() result). NULL
#'   (default) loads both from disk.
#'
#' @return Numeric vector: projected final innings score.
#' @keywords internal
calculate_test_projected_scores <- function(match_type, current_score, wickets_remaining,
                                             wickets_before, balls_before, run_rate, lead,
                                             innings, match_balls_before, eis,
                                             overs_models = NULL) {
  n <- length(current_score)
  stopifnot(length(match_type) == n)
  if (is.null(overs_models)) {
    overs_models <- list(Test = load_test_overs_model("Test"), MDM = load_test_overs_model("MDM"))
  }

  pred_rem <- numeric(n)
  for (mt in unique(match_type)) {
    idx <- which(match_type == mt)
    nd <- data.frame(
      wkt = factor(pmin(9L, as.integer(wickets_before[idx])), levels = 0:9),
      balls_before = balls_before[idx], run_rate = run_rate[idx], lead = lead[idx],
      inn = factor(pmin(4L, as.integer(innings[idx])), levels = 1:4),
      match_balls_before = match_balls_before[idx]
    )
    pred_rem[idx] <- predict_test_balls_remaining(overs_models[[mt]], nd)
  }

  calculate_projected_scores_vectorized(
    current_score = current_score, wickets_remaining = wickets_remaining,
    balls_remaining = pred_rem, expected_initial_score = eis,
    a = PROJ_DEFAULT_A, b = PROJ_DEFAULT_B, z = PROJ_DEFAULT_Z, y = PROJ_DEFAULT_Y,
    max_balls = balls_before + pred_rem
  )
}
