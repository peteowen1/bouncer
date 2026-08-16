# Empirical batting resources, fitted rather than assumed.
#
# `calculate_tail_calibration_features()` builds the single most important
# feature in the ODI chase model -- `resources_per_run`, 57% of its gain -- from
#
#     total_resources <- balls_remaining + (wickets_in_hand * 6)
#
# One wicket is assumed worth exactly six balls, in every state, never fitted.
# Measured over 13,358 T20 matches, the run cost of a wicket is:
#
#   balls left |  9 wkts |  7 |  5 |  3
#          100 |    12.4 | 22.7 |  - |  -
#           80 |     7.5 | 13.2 | 21.1 |  -
#           60 |     5.0 |  7.7 | 12.2 | 16.2
#           40 |     3.3 |  4.7 |  8.6 | 10.7
#           20 |     0.5 |  1.8 |  3.9 |  5.4
#
# A range of 0.5 to 22.7 runs -- a factor of 45 -- standing behind one constant.
#
# This file fits the surface instead. It is the same construction as a DLS
# resource table: expected runs still to come, as a function of how many balls
# and how many wickets remain.


#' Fit the Empirical Batting Resource Surface
#'
#' Estimates expected remaining innings runs for every (balls remaining,
#' wickets in hand) state, from completed first innings.
#'
#' @section Why first innings only:
#' A second innings stops when the target is passed, so its remaining-runs
#' distribution is truncated by the chase rather than by the batting side's
#' resources. Including it would bias every state downward, and worst exactly
#' where chases are decided.
#'
#' @section The functional form:
#' For each wickets-in-hand level, expected remaining runs are fitted as
#'
#'   \deqn{R(u, w) = Z_w (1 - e^{-b_w u})}
#'
#' where `u` is balls remaining. This is the Duckworth-Lewis form, and it is
#' used here for the reasons DLS uses it: it is monotone increasing in balls by
#' construction, saturates rather than growing without bound, and extrapolates
#' sanely into the sparse corners where few innings ever reach the state.
#'
#' Fitting cell means directly and smoothing them was tried first and discarded.
#' A count-weighted local mean is dominated by the common states and flattens
#' the surface — it returned a run cost of 0.3 for a wicket at 100 balls
#' remaining where the raw data says 12.4 — and isotonic repair afterwards
#' collapses sparse corners to constants. Two parameters per wicket level with
#' the right shape beats a thousand smoothed cells with the wrong one.
#'
#' `Z_w` is then forced monotone in wickets, since having more wickets in hand
#' cannot lower the expected total.
#'
#' @param format Character. "t20" or "odi".
#' @param conn DBI connection. Opened read-only and closed on exit if NULL.
#' @param min_cell Integer. Minimum observations for a cell to enter the fit.
#' @param window Ignored, retained so existing calls do not error.
#'
#' @return A `bouncer_resource_surface`: a list with `grid` (a data.table of
#'   `balls_remaining`, `wickets_in_hand`, `exp_runs`, `n`), `format`,
#'   `max_balls`, `n_matches` and `n_deliveries`.
#'
#' @export
fit_resource_surface <- function(format = c("t20", "odi"), conn = NULL,
                                 min_cell = 50L, window = 2L) {

  format <- match.arg(format)
  types <- if (format == "t20") "'T20','IT20'" else "'ODI','ODM'"
  max_balls <- get_max_balls(format)

  own <- is.null(conn)
  if (own) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }

  cli::cli_alert_info("Fitting {toupper(format)} resource surface from completed first innings...")

  d <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT d.match_id, d.over, d.ball, d.total_runs AS cum_runs,
           d.wickets_fallen AS wk
    FROM cricsheet.deliveries d
    JOIN cricsheet.matches m ON m.match_id = d.match_id
    WHERE m.match_type IN (%s) AND d.innings = 1 AND d.wickets_fallen BETWEEN 0 AND 9
  ", types)))

  if (nrow(d) == 0) cli::cli_abort("No {toupper(format)} first-innings deliveries found.")

  d[, balls_bowled := as.integer(over) * 6L + as.integer(ball)]
  d[, balls_remaining := pmax(max_balls - balls_bowled, 0L)]
  d[, final := max(cum_runs, na.rm = TRUE), by = match_id]
  d[, remaining := pmax(final - cum_runs, 0)]
  d[, wickets_in_hand := 10L - as.integer(wk)]
  d <- d[balls_remaining > 0 & balls_remaining <= max_balls]

  cells <- d[, .(exp_runs = mean(remaining), n = .N),
             by = .(balls_remaining, wickets_in_hand)]
  cells <- cells[n >= min_cell]
  if (nrow(cells) < 50) cli::cli_abort("Too few populated cells to fit a surface.")

  # Z_w (1 - exp(-b_w * u)), one pair of parameters per wickets-in-hand level.
  params <- data.table::rbindlist(lapply(1:10, function(w) {
    s <- cells[wickets_in_hand == w]
    if (nrow(s) < 5) return(NULL)
    z0 <- max(s$exp_runs) * 1.1
    fit <- tryCatch(
      stats::nls(exp_runs ~ Z * (1 - exp(-b * balls_remaining)), data = s,
                 weights = s$n, start = list(Z = z0, b = 0.01),
                 control = stats::nls.control(warnOnly = TRUE, maxiter = 200)),
      error = function(e) NULL
    )
    if (is.null(fit)) return(NULL)
    cf <- stats::coef(fit)
    data.table::data.table(wickets_in_hand = w, Z = unname(cf[["Z"]]),
                           b = unname(cf[["b"]]), cells = nrow(s), n = sum(s$n))
  }))

  if (nrow(params) < 5) cli::cli_abort("Resource surface fit failed for {toupper(format)}.")

  # Every wickets level 1-10 must be present, and every fit must be a GROWTH
  # curve. Two silent corruptions were possible without this:
  #
  # 1. A missing level merges in as NA below, and the monotone `cummax` pass
  #    propagates NA FORWARD -- so one unfitted level (say 3 wickets) turned
  #    levels 3 THROUGH 10 into NA at every ball count. That NA reaches
  #    `resources_per_run`, the largest single input to the ODI chase model,
  #    and is then zero-filled downstream, which is exactly the "zero is not a
  #    neutral value" failure the feature guard exists to prevent.
  # 2. `nls()` runs with `warnOnly = TRUE` and no bounds, so a level can
  #    converge to b <= 0. The curve Z(1 - exp(-b*u)) is then DECREASING in
  #    balls remaining and can go negative, contradicting the documented
  #    monotonicity.
  missing_w <- setdiff(1:10, params$wickets_in_hand)
  if (length(missing_w)) {
    cli::cli_abort(c(
      "Resource surface has no fit for wickets-in-hand {missing_w}.",
      "i" = "A gap propagates NA to every higher wickets level via the monotone pass.",
      "i" = "Lower {.arg min_cell} or widen the fitting window."))
  }
  bad <- params[!is.finite(Z) | !is.finite(b) | Z <= 0 | b <= 0]
  if (nrow(bad)) {
    cli::cli_abort(c(
      "Resource surface fit is not a growth curve at wickets {bad$wickets_in_hand}.",
      "i" = "Z and b must both be positive; got Z {round(bad$Z, 2)}, b {round(bad$b, 4)}."))
  }

  # More wickets in hand cannot lower the expected total.
  data.table::setorder(params, wickets_in_hand)
  params[, Z := cummax(Z)]

  grid <- data.table::CJ(balls_remaining = seq_len(max_balls),
                         wickets_in_hand = 0:10)
  grid <- merge(grid, params[, .(wickets_in_hand, Z, b)], by = "wickets_in_hand", all.x = TRUE)
  grid[, exp_runs := Z * (1 - exp(-b * balls_remaining))]
  grid[wickets_in_hand == 0L, exp_runs := 0]   # nobody left to bat

  # Monotone in wickets on the SURFACE, not just on Z. Forcing cummax on Z alone
  # leaves 38 violations, because a level with a higher asymptote but a slower
  # rate sits below a lower one at small ball counts -- the two parameters trade
  # off and only the fitted value is the thing that must not invert.
  data.table::setorder(grid, balls_remaining, wickets_in_hand)
  grid[, exp_runs := cummax(exp_runs), by = balls_remaining]
  # Belt and braces: the asserts above should make this unreachable, but a
  # single NA here silently spreads to every higher wickets level, so it is
  # not something to discover downstream in a model feature.
  if (anyNA(grid$exp_runs)) {
    cli::cli_abort("Resource surface produced {sum(is.na(grid$exp_runs))} NA cell{?s}; refusing to return it.")
  }
  grid <- merge(grid, cells[, .(balls_remaining, wickets_in_hand, n)],
                by = c("balls_remaining", "wickets_in_hand"), all.x = TRUE)
  grid[is.na(n), n := 0L]
  grid[, c("Z", "b") := NULL]

  cli::cli_alert_success(
    "Fitted {nrow(params)} wicket levels from {format(nrow(d), big.mark = ',')} deliveries."
  )

  structure(list(
    grid = grid[order(balls_remaining, wickets_in_hand)],
    params = params,
    format = format,
    max_balls = max_balls,
    n_matches = data.table::uniqueN(d$match_id),
    n_deliveries = nrow(d)
  ), class = c("bouncer_resource_surface", "list"))
}


#' Expected Remaining Runs From a Batting State
#'
#' Looks up [fit_resource_surface()] output. This is the fitted replacement for
#' `balls_remaining + wickets_in_hand * 6`.
#'
#' @param balls_remaining Integer vector.
#' @param wickets_in_hand Integer vector, recycled against `balls_remaining`.
#' @param surface A `bouncer_resource_surface`.
#'
#' @return Numeric vector of expected remaining runs. States outside the grid
#'   are clamped to its edges rather than returning NA: zero balls remaining is
#'   zero runs, and more balls than the format allows is capped at the format
#'   maximum.
#'
#' @export
resource_runs <- function(balls_remaining, wickets_in_hand, surface) {

  if (!inherits(surface, "bouncer_resource_surface")) {
    cli::cli_abort("{.arg surface} must be a {.cls bouncer_resource_surface}.")
  }

  n <- max(length(balls_remaining), length(wickets_in_hand))
  b <- rep_len(as.numeric(balls_remaining), n)
  w <- rep_len(as.numeric(wickets_in_hand), n)

  b <- pmin(pmax(round(b), 0), surface$max_balls)
  w <- pmin(pmax(round(w), 0), 10)

  key <- surface$grid$balls_remaining * 100L + surface$grid$wickets_in_hand
  idx <- match(b * 100 + w, key)
  out <- surface$grid$exp_runs[idx]

  # No balls left, or no wickets left, means no runs left -- both are exact, not
  # estimated, so they are asserted rather than looked up.
  out[b == 0 | w == 0] <- 0
  out
}


#' @export
print.bouncer_resource_surface <- function(x, ...) {
  cli::cli_h3("{toupper(x$format)} batting resource surface")
  cli::cli_text("Fitted on {x$n_matches} matches, {x$n_deliveries} first-innings deliveries.")
  g <- x$grid
  for (b in c(x$max_balls, round(x$max_balls * 0.75), round(x$max_balls / 2),
              round(x$max_balls / 4))) {
    row <- g[balls_remaining == b & wickets_in_hand %in% c(10, 7, 4, 1)]
    if (nrow(row) == 0) next
    cli::cli_text("{b} balls left: {paste(sprintf('%dw=%.0f', row$wickets_in_hand, row$exp_runs), collapse='  ')}")
  }
  invisible(x)
}
