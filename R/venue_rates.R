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

#' Mean of a per-match value, shrunk through a chain of nested causal levels
#'
#' The multi-level sibling of [time_causal_venue_mean()]. That function shrinks
#' a venue average toward one fixed global scalar, which is a poor prior for a
#' venue whose competition is itself unusual (bouncerverse#83: a T20 baseline
#' scoped to one competition serves a flat constant to 82-89% of a
#' cross-competition training corpus). This shrinks through a chain instead:
#' the finest level shrinks toward the next-coarsest level's OWN causal
#' (already-shrunk) value for that same match, all the way up to a causal
#' running mean over every match to date, which is itself regularized toward
#' the whole-sample mean via `root_prior_weight` (so the very first matches in
#' the corpus, before any group has evidence, still get a defined estimate).
#'
#' Every level uses the same as-of-date discipline as [time_causal_venue_mean()]:
#' strictly earlier matches by date, same-day fixtures cannot see each other.
#'
#' @param matches data.table with `match_id`, `match_date`, every column named
#'   in `levels`, and the column named by `value_col`.
#' @param value_col Name of the numeric column to average.
#' @param levels Character vector of grouping columns, ordered FINEST to
#'   COARSEST (e.g. `c("venue", "competition")`). The row's estimate is the
#'   finest level's causal mean, shrunk toward the next level up.
#' @param weights Named numeric vector of prior weights (in matches), one per
#'   entry of `levels`, keyed by the same names.
#' @param root_prior_weight Prior weight (in matches) regularizing the root
#'   causal running mean toward `root_prior_value`. Default 30.
#' @param root_prior_value Value the root level shrinks toward. NULL (default)
#'   means the whole-sample mean of `value_col` over `matches` -- matching
#'   [time_causal_venue_mean()]'s `prior_value = NULL` behaviour, including
#'   that this constant is shared by every row and is NOT itself computed
#'   causally match-by-match (it is one fixed scalar, the same as every other
#'   caller of this codebase's shrinkage-to-a-global-average pattern).
#'
#' @return data.table with `match_id` and `hier_mean`. Carries the whole-sample
#'   `overall_mean` as an attribute.
#' @keywords internal
time_causal_hierarchical_mean <- function(matches, value_col, levels, weights,
                                          root_prior_weight = 30,
                                          root_prior_value = NULL) {
  m <- data.table::as.data.table(matches)
  need <- c("match_id", "match_date", value_col, levels)
  miss <- setdiff(need, names(m))
  if (length(miss)) cli::cli_abort("{.arg matches} is missing {.field {miss}}.")
  if (!setequal(names(weights), levels)) {
    cli::cli_abort("{.arg weights} must be named exactly by {.arg levels}.")
  }

  m[, .v := as.numeric(get(value_col))]
  m[, .has := as.integer(!is.na(.v))]
  m[is.na(.v), .v := 0]
  if (is.null(root_prior_value)) {
    if (sum(m$.has) == 0L) cli::cli_abort("No usable values to estimate a prior from.")
    root_prior_value <- sum(m$.v) / sum(m$.has)
  }
  overall_mean <- root_prior_value

  # Root: causal running mean over ALL matches to date (every level pooled),
  # itself shrunk toward the whole-sample mean -- this is what the coarsest
  # level in `levels` will shrink toward. Grouped by match_date (not just
  # subtracting the current row) so same-day siblings exclude EACH OTHER, not
  # just themselves -- matching the per-level loop below exactly. A row-only
  # subtraction here leaked same-day matches into each other's root estimate
  # (bouncerverse#83 review, 2026-08-29): verified two same-day matches with no
  # shared venue/competition still moved each other's hier_mean.
  data.table::setorder(m, match_date, match_id)
  m[, cum_n_g := cumsum(.has)]
  m[, cum_v_g := cumsum(.v)]
  m[, `:=`(n_prior_g = max(cum_n_g) - sum(.has),
           v_prior_g = max(cum_v_g) - sum(.v)),
    by = match_date]
  m[, parent_mean := (v_prior_g + root_prior_weight * overall_mean) /
      (n_prior_g + root_prior_weight)]
  m[, c("cum_n_g", "cum_v_g", "n_prior_g", "v_prior_g") := NULL]

  # Walk COARSEST to FINEST, so each level's parent is the previous level's
  # already-shrunk causal value for that row, not the raw group average.
  for (lvl in rev(levels)) {
    w <- weights[[lvl]]
    data.table::setorderv(m, c(lvl, "match_date", "match_id"))
    m[, `:=`(cum_n_l = cumsum(.has), cum_v_l = cumsum(.v)), by = c(lvl)]
    m[, `:=`(n_prior_l = max(cum_n_l) - sum(.has),
             v_prior_l = max(cum_v_l) - sum(.v)),
      by = c(lvl, "match_date")]
    m[, level_mean := (v_prior_l + w * parent_mean) / (n_prior_l + w)]
    m[, parent_mean := level_mean]
    m[, c("cum_n_l", "cum_v_l", "n_prior_l", "v_prior_l", "level_mean") := NULL]
  }

  out <- m[, .(match_id, hier_mean = parent_mean)]
  data.table::setattr(out, "overall_mean", overall_mean)
  out[]
}


#' Decayed causal prior n/v for one grouping level
#'
#' The recursive step [time_causal_hierarchical_mean_decayed()] needs at each
#' level: given one row per (group, date) with that date's own total `.has`/
#' `.v` (same-day siblings already pooled), compute the EXPONENTIALLY DECAYED
#' sum of every STRICTLY EARLIER date in the same group, weighted by
#' `exp(-days_since/half_life)`.
#'
#' Computed as a single forward pass per group (dates already sorted
#' ascending): carry `T`, the decayed cumulative total AS OF AND INCLUDING the
#' current date, forward as `T_i = daily_i + exp(-gap/half_life) * T_{i-1}`.
#' The PRIOR value at date i (excluding date i itself) is then
#' `exp(-gap/half_life) * T_{i-1}` -- one date's lag on the same recursion,
#' not a second computation. `half_life = Inf` degenerates to the undecayed
#' flat sum (`exp(-gap/Inf) == 1` for every finite gap), so this is a proper
#' superset of the undecayed behaviour, not a parallel implementation of it.
#'
#' @param daily_n,daily_v Numeric vectors, one per (group, date) row, already
#'   sorted by date ascending WITHIN the group this is called on.
#' @param days Numeric vector of that row's `match_date` as a day count
#'   (e.g. `as.numeric(match_date)`), same order as `daily_n`/`daily_v`.
#' @param half_life Numeric, in days. `Inf` = no decay.
#' @return list(n_prior, v_prior), same length and order as the inputs.
#' @keywords internal
.decayed_causal_prior <- function(daily_n, daily_v, days, half_life) {
  n <- length(daily_n)
  n_prior <- numeric(n)
  v_prior <- numeric(n)
  if (n == 0L) return(list(n_prior = n_prior, v_prior = v_prior))

  T_n <- 0; T_v <- 0  # decayed cumulative total as of (and including) the PREVIOUS date
  prev_day <- NA_real_
  for (i in seq_len(n)) {
    if (i == 1L) {
      n_prior[i] <- 0
      v_prior[i] <- 0
    } else {
      gap <- days[i] - prev_day
      decay <- if (is.infinite(half_life)) 1 else exp(-gap * log(2) / half_life)
      n_prior[i] <- decay * T_n
      v_prior[i] <- decay * T_v
      T_n <- decay * T_n
      T_v <- decay * T_v
    }
    T_n <- T_n + daily_n[i]
    T_v <- T_v + daily_v[i]
    prev_day <- days[i]
  }
  list(n_prior = n_prior, v_prior = v_prior)
}


#' Mean of a per-match value, shrunk through a chain of nested causal levels,
#' with exponential recency decay
#'
#' The decayed sibling of [time_causal_hierarchical_mean()]. That function's
#' causal running mean at every level weighs a match from 15 years ago
#' identically to one from last week -- correct for a level with a genuinely
#' stable rate, wrong for one that is actually drifting (bouncerverse#84/#85:
#' IPL's league-wide scoring rate rose from 1.34 to 1.56 runs/ball between
#' 2022 and 2026 while the flat causal mean stayed pinned near 1.25, a
#' calibration gap 40x the whole-corpus average). `half_life` lets each level
#' forget slowly-obsoleting history rather than average across all of it
#' uniformly -- matching hganjoo's T20 Metrics primer's own league/year/ground
#' nested-shrinkage design (a year LEVEL in a hierarchy) with a continuous
#' decay instead of discrete year buckets, which avoids a hard reset to the
#' coarse prior at the boundary of every new season.
#'
#' @inheritParams time_causal_hierarchical_mean
#' @param half_life Named numeric vector, in days, one per entry of `levels`
#'   (same names as `weights`) PLUS an entry named `"root"` for the root
#'   level. `Inf` for a level means "no decay" (behaves exactly like
#'   [time_causal_hierarchical_mean()] at that level) -- so a hierarchy can
#'   decay some levels and not others.
#'
#' @return Same shape as [time_causal_hierarchical_mean()].
#' @keywords internal
time_causal_hierarchical_mean_decayed <- function(matches, value_col, levels, weights,
                                                   half_life,
                                                   root_prior_weight = 30,
                                                   root_prior_value = NULL) {
  m <- data.table::as.data.table(matches)
  need <- c("match_id", "match_date", value_col, levels)
  miss <- setdiff(need, names(m))
  if (length(miss)) cli::cli_abort("{.arg matches} is missing {.field {miss}}.")
  if (!setequal(names(weights), levels)) {
    cli::cli_abort("{.arg weights} must be named exactly by {.arg levels}.")
  }
  if (!setequal(names(half_life), c(levels, "root"))) {
    cli::cli_abort("{.arg half_life} must be named exactly by {.arg levels} plus \"root\".")
  }

  m[, .v := as.numeric(get(value_col))]
  m[, .has := as.integer(!is.na(.v))]
  m[is.na(.v), .v := 0]
  if (is.null(root_prior_value)) {
    if (sum(m$.has) == 0L) cli::cli_abort("No usable values to estimate a prior from.")
    root_prior_value <- sum(m$.v) / sum(m$.has)
  }
  overall_mean <- root_prior_value
  m[, .day := as.numeric(match_date)]

  decayed_prior_by <- function(m, group_cols, hl) {
    daily <- m[, .(n = sum(.has), v = sum(.v)), by = c(group_cols, ".day")]
    data.table::setorderv(daily, c(group_cols, ".day"))
    daily[, c("n_prior", "v_prior") := .decayed_causal_prior(n, v, .day, hl), by = c(group_cols)]
    m[daily, on = c(group_cols, ".day"), `:=`(.n_prior = i.n_prior, .v_prior = i.v_prior)]
    invisible(NULL)
  }

  data.table::setorder(m, match_date, match_id)
  decayed_prior_by(m, character(0), half_life[["root"]])
  m[, parent_mean := (.v_prior + root_prior_weight * overall_mean) /
      (.n_prior + root_prior_weight)]
  m[, c(".n_prior", ".v_prior") := NULL]

  for (lvl in rev(levels)) {
    w <- weights[[lvl]]
    data.table::setorderv(m, c(lvl, "match_date", "match_id"))
    decayed_prior_by(m, lvl, half_life[[lvl]])
    m[, level_mean := (.v_prior + w * parent_mean) / (.n_prior + w)]
    m[, parent_mean := level_mean]
    m[, c(".n_prior", ".v_prior", "level_mean") := NULL]
  }

  out <- m[, .(match_id, hier_mean = parent_mean)]
  data.table::setattr(out, "overall_mean", overall_mean)
  out[]
}
