# Context features for the agnostic ball-outcome model (bouncerverse#84/#85).
#
# league_avg_runs/league_avg_wicket used to be a FLAT, unweighted, all-time
# causal running mean, partitioned by event_name only -- correct that it
# doesn't leak across leagues (verified), wrong that it treats a match from
# 2010 identically to one from last month. IPL's own scoring rate rose from
# 1.34 to 1.56 runs/ball between 2022 and 2026 (coinciding with the Impact
# Player rule) while this feature stayed pinned near 1.25 over the same
# period -- a 0.298 runs/ball calibration gap for IPL 2026, 40x the
# whole-corpus average, discovered while comparing bouncer's ratings against
# an external T20 metrics benchmark.
#
# Fix, sized before building (a cheap screen against real IPL 2026 data, not
# a full retrain first): a nested venue -> league causal hierarchy
# (time_causal_hierarchical_mean_decayed(), venue_rates.R), matching
# hganjoo's own T20 Metrics primer par-score methodology (a year LEVEL in a
# league/year/ground hierarchy) but with continuous exponential decay instead
# of discrete year buckets, so a brand-new season's first match doesn't reset
# to the coarse all-time prior the way a hard year boundary would.
#
# Screened half-lives (venue, league): undecayed 2-level hierarchy alone cuts
# the gap 0.298 -> 0.175 (venue structure matters even without decay); adding
# decay (venue=730d, league=365d) cuts it further to 0.103 -- a 65% reduction
# from the original flat single-level feature. Checked this doesn't hurt
# leagues without a real trend (SA20, BBL): both improve slightly rather than
# getting worse, so this isn't overfitting IPL at everyone else's expense.
#
# ONE function, called by both 01_train_agnostic_model.R (training) and
# raa_cricsheet.R (serving) -- these two scripts independently hand-wrote the
# identical flat SQL window-function version before this fix, exactly the
# "same list typed out separately" drift shape bouncerverse#45 already
# happened once for a different feature.

#' Venue/league context features for the agnostic model
#'
#' @param conn DBI connection, read-only.
#' @param match_type_filter Character. SQL fragment for the match_type IN
#'   (...) clause body, e.g. `"'t20', 'it20'"` -- callers already build this
#'   per-format string, passed through rather than rebuilt here.
#'
#' @return data.table with `match_id`, `league_avg_runs`, `league_avg_wicket`.
#'   Names kept as `league_avg_*` for backward compatibility with
#'   `prepare_agnostic_features()`'s existing optional-column handling, even
#'   though the value now blends in a venue level, not just league history.
#' @keywords internal
compute_context_features <- function(conn, match_type_filter) {
  d <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT m.match_id, m.match_date, m.venue, m.event_name,
           AVG(dl.runs_batter + dl.runs_extras) AS match_avg_runs,
           AVG(CAST(dl.is_wicket AS DOUBLE)) AS match_wicket_rate
    FROM cricsheet.matches m
    JOIN cricsheet.deliveries dl ON m.match_id = dl.match_id
    WHERE LOWER(m.match_type) IN (%s)
      AND m.event_name IS NOT NULL
      AND m.match_date IS NOT NULL
    GROUP BY m.match_id, m.match_date, m.venue, m.event_name
  ", match_type_filter)))

  if (!nrow(d)) {
    return(data.table::data.table(match_id = character(), league_avg_runs = numeric(),
                                  league_avg_wicket = numeric()))
  }

  # Caught by review (2026-08-29): a single NULL match_date sorts FIRST in
  # ascending order (not last, not dropped), and .decayed_causal_prior()'s
  # forward recursion has no way to compute a finite gap against it -- one
  # bad row would silently NA-poison T_n/T_v for every later date across the
  # WHOLE root-level pass (root groups by date across the entire dataset),
  # cascading into every level's parent_mean. The SQL filter above should
  # make this impossible; asserted here too rather than trusted silently,
  # matching this file's own anyDuplicated(delivery_id) precedent elsewhere.
  if (anyNA(d$match_date)) {
    cli::cli_abort("compute_context_features(): {sum(is.na(d$match_date))} row(s) have a NULL match_date -- would silently corrupt the decayed hierarchy for every later match.")
  }
  if (anyDuplicated(d$match_id)) {
    cli::cli_abort("compute_context_features(): match_id is not unique in the per-match aggregate -- a duplicate would fan out during the merge() at every call site.")
  }

  # Canonicalise venue names (bouncerverse#73) BEFORE grouping -- otherwise
  # the same ground split across name variants each looks newer/thinner than
  # its real history and the venue level over-shrinks toward league.
  if (table_exists(conn, "venue_aliases")) {
    va <- DBI::dbGetQuery(conn, "SELECT alias, canonical_venue FROM venue_aliases")
    alias_lookup <- stats::setNames(va$canonical_venue, va$alias)
    hit <- match(d$venue, names(alias_lookup))
    d$venue[!is.na(hit)] <- alias_lookup[hit[!is.na(hit)]]
  } else {
    cli::cli_warn("compute_context_features(): venue_aliases table not found -- venue names will not be canonicalised (fragmented ground names will look newer/thinner than their real history).")
  }
  d[, match_date := as.Date(match_date)]
  data.table::setnames(d, "event_name", "competition")

  overall_runs <- mean(d$match_avg_runs, na.rm = TRUE)
  overall_wicket <- mean(d$match_wicket_rate, na.rm = TRUE)

  # Half-lives in days: venue=2yr, competition=1yr, root=1yr -- the screened
  # combination (see file header). weights match this codebase's existing
  # venue/competition prior-weight convention (bouncerverse#83).
  hl <- c(venue = 730, competition = 365, root = 365)
  w <- c(venue = 5, competition = 20)

  runs <- time_causal_hierarchical_mean_decayed(
    d, "match_avg_runs", levels = c("venue", "competition"), weights = w,
    half_life = hl, root_prior_weight = 30, root_prior_value = overall_runs)
  wicket <- time_causal_hierarchical_mean_decayed(
    d, "match_wicket_rate", levels = c("venue", "competition"), weights = w,
    half_life = hl, root_prior_weight = 30, root_prior_value = overall_wicket)

  merge(
    runs[, .(match_id, league_avg_runs = hier_mean)],
    wicket[, .(match_id, league_avg_wicket = hier_mean)],
    by = "match_id"
  )
}
