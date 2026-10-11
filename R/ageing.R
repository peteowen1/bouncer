# Ageing curves for player rating v2 (D-P69). Measured in bouncerverse
# docs/reviews/2026-10-11-REPLACEMENT-PRIOR.md; experiments
# data-raw/ratings/player/rating-v2/validation/49-51.

#' Fit a within-player ageing curve
#'
#' Fits `v = player effect + f(age)` by alternating means, so each player is
#' compared only with himself. Comparing across players instead would mostly
#' measure who survives: the players still picked at 36 are the ones who were
#' good enough to stay. The within-player fit reduces that bias but cannot
#' remove it, because a player who declines and is dropped contributes no
#' decline years. That is why the rating applies only the rising part while a
#' player is still being picked (see [calculate_player_rating_v2()]).
#'
#' `f` is anchored at 0 for age 27 and smoothed over neighbouring ages
#' (weighted by matches). Ages with fewer than `min_n` matches are "thin".
#' Past the oldest well-measured age the curve is EXTENDED as a downward
#' parabola from the peak, fitted on the measured post-peak ages. **The
#' extension is an assumption, not a measurement**: nobody plays international
#' cricket at 60, so nothing can measure it. It exists so a long-retired
#' player's rating keeps falling with age.
#'
#' @param pm data.table with `player_id`, `v` (per-match value) and `age`
#'   (years, NA allowed).
#' @param ages Integer range of ages to bin into; ages outside are clamped.
#' @param min_n Matches an age needs to count as well measured.
#' @param iters Alternating-means iterations.
#' @return A list (`tab`, `peak`, `first`, `last`, `f_peak`, `f_last`, `b`), or
#'   NULL when there is too little dated data (fewer than 2,000 matches, or
#'   fewer than `min_n` at age 27).
#' @export
fit_ageing_curve <- function(pm, ages = 16:45, min_n = 200L, iters = 30L) {
  d <- data.table::as.data.table(pm)[!is.na(age), .(player_id, v, age)]
  d[, ab := pmin(pmax(floor(age), min(ages)), max(ages))]
  if (nrow(d) < 2000L || d[ab == 27, .N] < min_n) return(NULL)
  d[, f := 0]
  for (it in seq_len(iters)) {
    d[, a := mean(v - f), by = player_id]
    fa <- d[, .(f = mean(v - a), n = .N, players = data.table::uniqueN(player_id)), by = ab]
    fa[, f := f - fa[ab == 27, f]]
    d[fa, on = "ab", f := i.f]
  }
  data.table::setorder(fa, ab)
  fa[, f_s := vapply(ab, function(x) {
    w <- fa[abs(ab - x) <= 2]
    sum(w$f * w$n) / sum(w$n)
  }, numeric(1))]
  fa[, thin := n < min_n]
  ok <- fa[thin == FALSE]
  peak <- ok[which.max(f_s), ab]
  last <- max(ok$ab)
  f_peak <- ok[ab == peak, f_s]
  post <- ok[ab > peak]
  b <- if (nrow(post) >= 3L) {
    post[, sum(n * (ab - peak)^2 * (f_s - f_peak)) / sum(n * (ab - peak)^4)]
  } else NA_real_
  # The extension must fall. If the measured ages give no decline at all (or
  # too few points), fall back to a gentle one rather than let it rise.
  if (!is.finite(b) || b >= 0) b <- -0.01 * (abs(f_peak - min(ok$f_s)) + 1)
  list(tab = fa, peak = peak, first = min(ok$ab), last = last,
       f_peak = f_peak, f_last = ok[ab == last, f_s], b = b)
}

# The rising part only: flat after the peak, so nobody declines.
.ageing_young <- function(cv, age) {
  out <- numeric(length(age))
  ok <- !is.na(age)
  if (is.null(cv) || !any(ok)) return(out)
  a <- pmin(pmax(age[ok], cv$first), cv$peak)
  out[ok] <- stats::approx(cv$tab$ab, cv$tab$f_s, xout = a, rule = 2)$y
  out
}

# The whole curve, measured up to `last` and extended past it as a parabola
# from the peak, joined continuously at `last`.
.ageing_full <- function(cv, age) {
  out <- numeric(length(age))
  ok <- !is.na(age)
  if (is.null(cv) || !any(ok)) return(out)
  a <- pmax(age[ok], cv$first)
  inside <- a <= cv$last
  o <- numeric(length(a))
  o[inside] <- stats::approx(cv$tab$ab, cv$tab$f_s, xout = a[inside], rule = 2)$y
  o[!inside] <- cv$f_last + cv$b * ((a[!inside] - cv$peak)^2 - (cv$last - cv$peak)^2)
  out[ok] <- o
  out
}
