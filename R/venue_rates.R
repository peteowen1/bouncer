# Time-causal venue rates.
#
# `venue_result_rate` — the historical P(result) at a ground — was built over
# ALL matches at that ground, including the match being predicted. A live
# prediction cannot know its own outcome, so that is label information the
# deployed model will never have. It is not negligible: the median Test venue
# has 3 matches and 215 of 289 (74.4%) have fewer than 10, so with a prior
# weight of 10 the match's own outcome carries 1/(n+10) — 7.7% at the median
# venue, 9.1% at the worst (bouncerverse#29).
#
# Because training and serving built it identically it never showed up as a
# train/serve divergence. It inflated both, including the honest-serving figure
# used as the bar elsewhere.
#
# WHY NOT LEAVE-ONE-OUT. Subtracting the match's own outcome looks like the
# obvious repair and is much worse. Measured: holdout mlogloss 0.8171 -> 0.5000
# and accuracy 62.9% -> 79.2%, with the P(draw)>=0.8 bucket growing from 29,438
# rows to 251,364 at a 100.0% actual draw rate. A leak removal that IMPROVES the
# metric by 0.32 is not a leak removal. `is_result` IS the label, so subtracting
# it turns the feature into an anti-correlated encoding of the target: at a
# 3-match venue the value shifts by ~1/12 = 0.083 on the outcome alone. LOO
# target encoding concentrates the leak instead of removing it.
#
# The construction below is what a live prediction actually has: matches
# strictly BEFORE the current match date at that ground, expanding window, and
# never the current row's own label.

#' Historical result rate at a venue, using only earlier matches
#'
#' @param matches data.table (or data.frame) with one row per match and columns
#'   `match_id`, `venue`, `match_date`, `decided` (1 if the match reached a
#'   decided outcome and so counts toward the denominator) and `is_result`
#'   (1 if that outcome was a win rather than a draw).
#' @param prior_weight Strength of the Bayesian prior, in matches. Default 10.
#' @param prior_rate Rate to shrink toward. Default NULL, meaning the global
#'   decided-match result rate over the whole table.
#'
#' @section Ties on date:
#' Two matches at the same ground on the same date cannot see each other
#' either — a prediction made for one has no access to the other's result. Rows
#' sharing a (venue, date) therefore all see the same strictly-earlier history.
#'
#' @section The prior:
#' `prior_rate` is global and does include the current match, which is a leak of
#' weight `1/n_total` — about 0.05% against the 7.7-9.1% the per-venue term
#' carried. Left global deliberately: making it expanding too buys ~0.05% and
#' costs stability on the first few matches ever played.
#'
#' @return data.table with `match_id`, `venue_result_rate`, `n_prior` (decided
#'   matches at that ground before this one) and `at_prior` (TRUE where there is
#'   no earlier history, so the value IS the prior). Carries `prior_rate` as an
#'   attribute.
#' @keywords internal
time_causal_venue_result_rate <- function(matches, prior_weight = 10,
                                          prior_rate = NULL) {
  m <- data.table::as.data.table(matches)
  need <- c("match_id", "venue", "match_date", "decided", "is_result")
  miss <- setdiff(need, names(m))
  if (length(miss)) {
    cli::cli_abort("{.arg matches} is missing {.field {miss}}.")
  }

  m[, decided := as.integer(!is.na(decided) & decided == 1L)]
  m[, is_result := as.integer(decided == 1L & !is.na(is_result) & is_result == 1L)]

  if (is.null(prior_rate)) {
    n_dec <- sum(m$decided)
    if (n_dec == 0L) cli::cli_abort("No decided matches to estimate a prior from.")
    prior_rate <- sum(m$is_result) / n_dec
  }

  data.table::setorder(m, venue, match_date, match_id)
  m[, `:=`(cum_n = cumsum(decided), cum_r = cumsum(is_result)), by = venue]
  # Everything up to and including this DATE, minus this date's own matches --
  # so same-day fixtures at one ground cannot see each other.
  m[, `:=`(n_prior = max(cum_n) - sum(decided),
           res_prior = max(cum_r) - sum(is_result)),
    by = .(venue, match_date)]

  m[, venue_result_rate := (res_prior + prior_weight * prior_rate) /
      (n_prior + prior_weight)]
  m[, at_prior := n_prior == 0L]

  out <- m[, .(match_id, venue_result_rate, n_prior, at_prior)]
  data.table::setattr(out, "prior_rate", prior_rate)
  out[]
}

#' Mean of a per-match value at a venue, using only earlier matches
#'
#' The numeric sibling of [time_causal_venue_result_rate()], for features like
#' "average first-innings total at this ground". Same defect, same shape: a
#' venue average computed over every match at the ground **includes the match
#' being predicted**, and at a one-match venue the feature simply *is* that
#' match's own total.
#'
#' @param matches data.table with `match_id`, `venue`, `match_date` and the
#'   column named by `value_col`. Rows where that value is `NA` contribute
#'   nothing but still receive an estimate.
#' @param value_col Name of the numeric column to average.
#' @param prior_weight Strength of the prior, in matches. Default 5 — lower
#'   than the result-rate default because a total is far less noisy than a
#'   single binary outcome.
#' @param prior_value Value to shrink toward. NULL means the global mean.
#'
#' @return data.table with `match_id`, `venue_mean`, `n_prior`, `at_prior`.
#'   Carries `prior_value` as an attribute.
#' @keywords internal
time_causal_venue_mean <- function(matches, value_col, prior_weight = 5,
                                   prior_value = NULL) {
  m <- data.table::as.data.table(matches)
  need <- c("match_id", "venue", "match_date", value_col)
  miss <- setdiff(need, names(m))
  if (length(miss)) cli::cli_abort("{.arg matches} is missing {.field {miss}}.")

  m[, .v := as.numeric(get(value_col))]
  m[, .has := as.integer(!is.na(.v))]
  m[is.na(.v), .v := 0]

  if (is.null(prior_value)) {
    if (sum(m$.has) == 0L) cli::cli_abort("No usable values to estimate a prior from.")
    prior_value <- sum(m$.v) / sum(m$.has)
  }

  data.table::setorder(m, venue, match_date, match_id)
  m[, `:=`(cum_n = cumsum(.has), cum_v = cumsum(.v)), by = venue]
  # Strictly earlier by DATE, so same-day fixtures cannot see each other.
  m[, `:=`(n_prior = max(cum_n) - sum(.has),
           v_prior = max(cum_v) - sum(.v)),
    by = .(venue, match_date)]

  m[, venue_mean := (v_prior + prior_weight * prior_value) / (n_prior + prior_weight)]
  m[, at_prior := n_prior == 0L]

  out <- m[, .(match_id, venue_mean, n_prior, at_prior)]
  data.table::setattr(out, "prior_value", prior_value)
  out[]
}
