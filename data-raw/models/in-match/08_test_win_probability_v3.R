# Test Cricket Win Probability - Decomposed Two-Model Pipeline ----
#
# Root cause of v2 failure: a single 3-class model can't simultaneously learn
# "who is ahead?" (lead/wickets) AND "will time run out?" (overs/pitch pace).
# Draws absorb probability uniformly because these are orthogonal signals.
#
# Solution: decompose into two binary models:
#   Model A: P(result) — will this match produce a winner?
#   Model B: P(team1_win | result) — given a result, who wins?
#
# Final probabilities:
#   P(draw)      = 1 - P(result)
#   P(team1_win) = P(result) * P(team1_win | result)
#   P(team2_win) = P(result) * (1 - P(team1_win | result))

# Setup ----
library(DBI)
library(dplyr)
library(data.table)
library(xgboost)
devtools::load_all()

RANDOM_SEED <- 42
CV_FOLDS <- 5
MAX_ROUNDS <- 2000
EARLY_STOPPING <- 30

cat("\n")
cli::cli_h1("Test Win Probability v3 (Decomposed Two-Model)")
cat("\n")

# Load Data ----
cli::cli_h2("Loading data")

output_dir <- file.path(find_bouncerdata_dir(), "models")
conn <- get_db_connection(read_only = TRUE)

# The Stage 1 projected-score model used to be loaded here and applied to
# innings-1 deliveries. #24 removed that: serving never had it, and it was
# worth 0.0004 of holdout mlogloss. This script no longer depends on
# 03_projected_score_model.R or 01_prepare_all_formats.R.

# Load all Test deliveries with outcomes
deliveries <- DBI::dbGetQuery(conn, "
  SELECT
    d.delivery_id, d.match_id, d.season, d.match_date,
    d.venue, d.gender, d.batting_team, d.bowling_team, d.match_type,
    d.innings, d.over, d.ball,
    d.total_runs,
    -- POST-delivery, deliberately. total_runs is POST (verified: on the first
    -- ball of an innings it equals that ball's runs, 100% of 10,691 rows), so
    -- shifting wickets to PRE with `- is_wicket` put the two halves of the
    -- state in different frames. Serving reads cricinfo's total_innings_runs
    -- and total_innings_wickets, both POST, so POST/POST is also the frame
    -- that matches what the models are asked at serving time (#24).
    d.wickets_fallen,
    m.outcome_type, m.outcome_winner, m.team1, m.team2
  FROM cricsheet.deliveries d
  JOIN cricsheet.matches m ON d.match_id = m.match_id
  WHERE LOWER(d.match_type) IN ('test', 'mdm')
    AND m.outcome_type IS NOT NULL
  ORDER BY d.match_date, d.match_id, d.innings, d.over, d.ball
")
setDT(deliveries)
cli::cli_alert_success("Loaded {nrow(deliveries)} deliveries from {uniqueN(deliveries$match_id)} matches")

# Load innings totals (with declared flag and overs)
innings_totals <- DBI::dbGetQuery(conn, "
  SELECT match_id, innings, total_runs AS innings_total,
         total_wickets AS innings_wickets, total_overs AS innings_overs,
         declared
  FROM cricsheet.match_innings
  WHERE match_id IN (
    SELECT match_id FROM cricsheet.matches WHERE LOWER(match_type) IN ('test', 'mdm')
  )
  ORDER BY match_id, innings
")
setDT(innings_totals)

# Venue averages, TIME-CAUSAL and per match (#24).
#
# #29 made venue_result_rate causal and left this one behind, which was an
# incomplete fix rather than a deliberate one: an average first-innings total
# over EVERY match at the ground includes the match being predicted. Measured on
# the sibling construction in the Test WPA batch, at the 79 one-match venues the
# feature correlated 1.000 with that match's own innings-1 total -- it simply WAS
# the value it was used to predict (#69).
venue_avg_raw <- DBI::dbGetQuery(conn, "
  SELECT m.match_id, m.venue, m.match_date,
         MAX(CASE WHEN mi.innings = 1 THEN mi.total_runs END) AS inn1_total
  FROM cricsheet.matches m
  LEFT JOIN cricsheet.match_innings mi ON mi.match_id = m.match_id
  WHERE LOWER(m.match_type) IN ('test', 'mdm')
  GROUP BY 1, 2, 3
")
setDT(venue_avg_raw)
venue_avg_raw[, match_date := as.Date(match_date)]
venue_avgs <- time_causal_venue_mean(venue_avg_raw, "inn1_total", prior_weight = 5)
venue_avgs <- venue_avgs[, .(match_id, venue_avg = venue_mean)]

# Venue result rates, TIME-CAUSAL and per match (#29).
#
# This was one rate per venue computed over every match at the ground --
# including the match being predicted. A live prediction cannot know its own
# outcome, and the weight is not small: the median Test venue has 3 matches, so
# with prior_weight 10 the match's own result carried 7.7% of its own feature.
# Because training and serving built it identically it never showed as a
# train/serve divergence; it inflated both.
#
# Do NOT "fix" this by subtracting the match's own outcome. That was measured
# and is far worse -- see the note at the top of R/venue_rates.R.
venue_results <- DBI::dbGetQuery(conn, "
  SELECT m.match_id, m.venue, m.match_date, m.outcome_type
  FROM cricsheet.matches m
  WHERE LOWER(m.match_type) IN ('test', 'mdm')
    AND m.outcome_type IS NOT NULL
")
setDT(venue_results)
venue_results[, `:=`(decided = 1L,
                     is_result = as.integer(outcome_type != "draw"),
                     match_date = as.Date(match_date))]
venue_results <- time_causal_venue_result_rate(venue_results, prior_weight = 10)
prior_rate <- attr(venue_results, "prior_rate")
cli::cli_alert_info(paste0(
  "Venue result rate: ", sum(venue_results$at_prior), " of ", nrow(venue_results),
  " matches (", round(100 * mean(venue_results$at_prior), 1),
  "%) are the first at their ground and fall back to the prior (",
  round(prior_rate, 3), ")."))
venue_results <- venue_results[, .(match_id, venue_result_rate)]

DBI::dbDisconnect(conn, shutdown = TRUE)

# Feature Engineering ----
cli::cli_h2("Engineering features")

# "team1" means THE SIDE BATTING FIRST -- everywhere (#30).
#
# This used cricsheet's listed `matches.team1` for both the label and
# batting_is_team1, while team1_completed/team2_completed (below) attribute
# innings 1+3 to one side and 2+4 to the other -- the batting-order
# alternation. Those are the same team only when the listed team1 happens to
# bat first, which is true for just 73.7% of Tests (899 matches; MDM is 96.6%).
# For the other quarter the label named one side while team1_lead described the
# other. Serving has always meant "the side batting first"
# (.test_wp_features() sets batting_is_team1 = innings %in% c(1,3)), so this
# also removes a train/serve difference in what p_team1_win denotes.
inn1_bat <- deliveries[innings == 1L,
                       .(inn1_batting = data.table::first(batting_team)),
                       by = match_id]
deliveries <- merge(deliveries, inn1_bat, by = "match_id", all.x = TRUE)
n_no_inn1 <- deliveries[is.na(inn1_batting), uniqueN(match_id)]
if (n_no_inn1 > 0) {
  # No innings-1 deliveries means we cannot say who batted first, and the label
  # would be a guess. Drop them loudly rather than fall back to the listed team.
  cli::cli_alert_warning(
    "{n_no_inn1} match{?es} have no innings-1 deliveries -- dropped, cannot identify who batted first")
  deliveries <- deliveries[!is.na(inn1_batting)]
}
stopifnot("every retained match must know who batted first" =
            !anyNA(deliveries$inn1_batting))

# Report the size of what this fixes: how often the listed team1 was NOT the
# side batting first, which is exactly how much label noise the old code carried.
mis <- unique(deliveries[, .(match_id, match_type, team1, inn1_batting)])
mis[, aligned := team1 == inn1_batting]
cli::cli_alert_info(
  "team1 convention: listed team1 batted first in {round(100 * mean(mis$aligned), 1)}% of {nrow(mis)} matches")
print(mis[, .(matches = .N, pct_listed_team1_batted_first = round(100 * mean(aligned), 1)),
          by = match_type])

# Match outcome labels, relative to the side batting first
deliveries[, match_outcome := fcase(
  outcome_type == "draw", 1L,
  outcome_winner == inn1_batting, 0L,
  default = 2L
)]
deliveries[, is_result := as.integer(outcome_type != "draw")]

# Basic state
deliveries[, `:=`(
  balls_bowled = as.integer(over * 6L + ball),
  wickets_in_hand = 10L - wickets_fallen,
  current_run_rate = fifelse(over > 0, total_runs / (over + ball/6), 0),
  # The alternation, matching .test_wp_features() exactly. NOT
  # `batting_team == inn1_batting`, which would be follow-on-aware and so would
  # trade one train/serve divergence for another -- serving cannot see who
  # batted first, only the innings number. Follow-on matches keep the same
  # approximation on both sides, as they always have.
  batting_is_team1 = as.integer(innings %in% c(1L, 3L))
)]

# Innings totals (wide format) — including declared flag
inn_wide <- dcast(innings_totals, match_id ~ paste0("inn", innings),
                  value.var = c("innings_total", "innings_wickets", "innings_overs", "declared"),
                  fill = NA)
deliveries <- merge(deliveries, inn_wide, by = "match_id", all.x = TRUE)
# Keyed on match_id, not venue -- what a ground averaged depends on when you ask.
deliveries <- merge(deliveries, venue_avgs, by = "match_id", all.x = TRUE)
deliveries[is.na(venue_avg), venue_avg := 340]
# Keyed on match_id, not venue: the rate is now per match, because what a
# venue's history looked like depends on when you ask (#29).
deliveries <- merge(deliveries, venue_results, by = "match_id", all.x = TRUE)
deliveries[is.na(venue_result_rate), venue_result_rate := prior_rate]

# Team1 lead (cumulative)
deliveries[, team1_completed := fcase(
  innings == 1, 0L,
  innings == 2, fifelse(!is.na(innings_total_inn1), innings_total_inn1, 0L),
  innings == 3, fifelse(!is.na(innings_total_inn1), innings_total_inn1, 0L),
  innings == 4, fifelse(!is.na(innings_total_inn1), innings_total_inn1, 0L) +
                fifelse(!is.na(innings_total_inn3), innings_total_inn3, 0L),
  default = 0L
)]
deliveries[, team2_completed := fcase(
  innings <= 2, 0L,
  innings == 3, fifelse(!is.na(innings_total_inn2), innings_total_inn2, 0L),
  innings == 4, fifelse(!is.na(innings_total_inn2), innings_total_inn2, 0L),
  default = 0L
)]
deliveries[, team1_lead := fcase(
  batting_is_team1 == 1L, as.integer(team1_completed + total_runs - team2_completed),
  default = as.integer(team1_completed - (team2_completed + total_runs))
)]

# Cumulative match overs
deliveries[, cum_overs := as.double(over) + fcase(
  innings == 1, 0,
  innings == 2, fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 90),
  innings == 3, fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 90) +
                fifelse(!is.na(innings_overs_inn2), as.double(innings_overs_inn2), 90),
  innings == 4, fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 90) +
                fifelse(!is.na(innings_overs_inn2), as.double(innings_overs_inn2), 90) +
                fifelse(!is.na(innings_overs_inn3), as.double(innings_overs_inn3), 90),
  default = 0
)]

# Overridable so the constant can be TESTED rather than assumed (#71). For a
# tree model a constant divisor is a monotone reparameterisation, so this can
# only change anything where it CLIPS -- and at 450 it clips for 3 of 3,071
# matches -- or where overs_remaining is a DENOMINATOR.
if (!exists("MAX_OVERS")) MAX_OVERS <- 450
deliveries[, `:=`(
  overs_remaining = pmax(0, MAX_OVERS - cum_overs),
  match_progress = pmin(1, cum_overs / MAX_OVERS),
  approx_day = pmin(5L, as.integer(floor(cum_overs / 90) + 1)),
  innings_num = as.double(innings)
)]

# ---- Tier 1: Derived rain proxy features (no weather data needed) ----

# Overs per day: how many overs bowled per day so far
# Low values (< 80) indicate rain delays or slow over rates
deliveries[, overs_per_day := fifelse(approx_day > 0, cum_overs / approx_day, 90)]

# Overs deficit: how many overs "missing" vs scheduled (90 per day)
# Positive = overs lost (likely rain); 0 = on schedule
deliveries[, overs_deficit := pmax(0, approx_day * 90 - cum_overs)]

# ---- Tier 2+: Weather features (if available) ----

# Tier 2: causal per-day rain (#72). `rain_days_so_far` above was a match-TOTAL
# prorated by progress through the match, which is not causal (#24) — day 2
# still carried a scaled share of days 3-5, which had not happened yet.
# `causal_rain_features()` instead sums rain over days strictly BEFORE the
# current one, from `main.venue_weather_daily` (backfilled per-day weather,
# #72), which fixes that leak in the common case.
#
# NOT exactly zero-leakage, because `approx_day` (cum_overs/90) is a Tier-1
# proxy for the calendar day, not the true one -- cricsheet carries no
# per-ball date, only one `match_date` per match. Two known edge cases: a
# washed-out day where cum_overs barely advances leaves the bucket unchanged
# (under-counts already-fallen rain -- a fidelity loss, not a leak), and a day
# that crosses 90 overs pushes its tail balls into the next day's bucket, so
# their `rain_mm_before` window can end on the day still in progress and pick
# up that same day's later rain. Narrow and would need real per-ball dates
# (not available) to close outright.
match_days_uniq <- unique(deliveries[, .(match_id, venue, match_date, day = approx_day)])
daily_weather <- tryCatch(
  load_venue_weather_daily(venues = unique(match_days_uniq$venue)),
  error = function(e) {
    cli::cli_warn("load_venue_weather_daily() failed: {conditionMessage(e)} -- falling back to Tier 1 rain features.")
    NULL
  })
weather_available <- !is.null(daily_weather) && nrow(daily_weather) > 0

if (weather_available) {
  rain_feats <- causal_rain_features(match_days_uniq, daily_weather)
  deliveries <- merge(
    deliveries,
    rain_feats[, .(match_id, day, rain_mm_before, rain_days_before, venue_rain_climatology)],
    by.x = c("match_id", "approx_day"), by.y = c("match_id", "day"), all.x = TRUE)
  deliveries[is.na(rain_mm_before), rain_mm_before := 0]
  deliveries[is.na(rain_days_before), rain_days_before := 0L]
  n_with_rain <- sum(deliveries$rain_mm_before > 0 | deliveries$rain_days_before > 0)
  n_with_climatology <- sum(!is.na(deliveries$venue_rain_climatology))
  if (n_with_rain == 0) {
    cli::cli_warn("Tier 2: joined weather but rain_mm_before/rain_days_before are constant 0 across all {nrow(deliveries)} rows -- check the match_id/day join.")
  }
  cli::cli_alert_success(
    "Tier 2: causal rain features joined; {n_with_rain}/{nrow(deliveries)} deliveries have nonzero rain-before, {n_with_climatology}/{nrow(deliveries)} ({round(n_with_climatology/nrow(deliveries)*100,1)}%) have climatology")
} else {
  deliveries[, `:=`(rain_mm_before = 0, rain_days_before = 0L,
                     venue_rain_climatology = NA_real_)]
  cli::cli_alert_warning("Tier 2: main.venue_weather_daily is empty or unreadable, using Tier 1 features only -- rain features are degenerate for this run.")
}

# ---- Other time-pressure features ----

# Total wickets fallen so far in the match
deliveries[, total_wickets_match := fcase(
  innings == 1, wickets_fallen,
  innings == 2, fifelse(!is.na(innings_wickets_inn1), innings_wickets_inn1, 0L) + wickets_fallen,
  innings == 3, fifelse(!is.na(innings_wickets_inn1), innings_wickets_inn1, 0L) +
                fifelse(!is.na(innings_wickets_inn2), innings_wickets_inn2, 0L) + wickets_fallen,
  innings == 4, fifelse(!is.na(innings_wickets_inn1), innings_wickets_inn1, 0L) +
                fifelse(!is.na(innings_wickets_inn2), innings_wickets_inn2, 0L) +
                fifelse(!is.na(innings_wickets_inn3), innings_wickets_inn3, 0L) + wickets_fallen,
  default = as.integer(wickets_fallen)
)]

# Match-level scoring rate (runs per over across entire match)
deliveries[, total_runs_match := fcase(
  innings == 1, total_runs,
  innings == 2, fifelse(!is.na(innings_total_inn1), innings_total_inn1, 0L) + total_runs,
  innings == 3, fifelse(!is.na(innings_total_inn1), innings_total_inn1, 0L) +
                fifelse(!is.na(innings_total_inn2), innings_total_inn2, 0L) + total_runs,
  innings == 4, fifelse(!is.na(innings_total_inn1), innings_total_inn1, 0L) +
                fifelse(!is.na(innings_total_inn2), innings_total_inn2, 0L) +
                fifelse(!is.na(innings_total_inn3), innings_total_inn3, 0L) + total_runs,
  default = as.integer(total_runs)
)]
deliveries[, runs_per_over_match := fifelse(cum_overs > 0, total_runs_match / cum_overs, 3.0)]

# Overs per wicket so far in current innings (scoring pace indicator)
deliveries[, overs_per_wicket_current := fifelse(
  wickets_fallen > 0, (over + ball/6) / wickets_fallen, 30  # Cap at 30 if no wickets
)]

# Projected current innings overs: if no more wickets fall at current rate, how many overs?
# wickets_in_hand * overs_per_wicket gives optimistic estimate
deliveries[, current_innings_projected_overs := pmin(
  150,  # Cap at 150 overs (no innings lasts longer)
  fifelse(wickets_fallen > 0,
    (over + ball/6) + wickets_in_hand * overs_per_wicket_current,
    90)  # Default for 0 wickets: assume ~90 overs
)]

# Completed innings overs (sum of finished innings)
deliveries[, completed_innings_overs := fcase(
  innings == 1, 0,
  innings == 2, fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 0),
  innings == 3, fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 0) +
                fifelse(!is.na(innings_overs_inn2), as.double(innings_overs_inn2), 0),
  innings == 4, fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 0) +
                fifelse(!is.na(innings_overs_inn2), as.double(innings_overs_inn2), 0) +
                fifelse(!is.na(innings_overs_inn3), as.double(innings_overs_inn3), 0),
  default = 0
)]

# Average overs per completed innings at this venue (use match data as proxy)
# Use within-match average of completed innings if available
deliveries[, avg_overs_per_innings := fcase(
  innings == 1, 80,  # Prior for first innings
  innings == 2, as.double(innings_overs_inn1),
  innings == 3, (fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 80) +
                 fifelse(!is.na(innings_overs_inn2), as.double(innings_overs_inn2), 80)) / 2,
  innings == 4, (fifelse(!is.na(innings_overs_inn1), as.double(innings_overs_inn1), 80) +
                 fifelse(!is.na(innings_overs_inn2), as.double(innings_overs_inn2), 80) +
                 fifelse(!is.na(innings_overs_inn3), as.double(innings_overs_inn3), 80)) / 3,
  default = 80
)]
deliveries[is.na(avg_overs_per_innings), avg_overs_per_innings := 80]

# Remaining innings count (including current, after current ball)
deliveries[, remaining_innings_count := 4L - as.integer(innings)]

# KEY FEATURE: projected total overs for the entire match
# completed_innings_overs + current_innings_projected + remaining_innings * avg_overs
deliveries[, projected_total_overs := completed_innings_overs +
             current_innings_projected_overs +
             remaining_innings_count * avg_overs_per_innings]
deliveries[, projected_total_overs := pmin(600, pmax(50, projected_total_overs))]  # Sanity bounds

# Time pressure: >1.0 means match likely to run out of time (draw likely)
deliveries[, time_pressure := projected_total_overs / MAX_OVERS]

# Lead per over remaining (pressure rate)
deliveries[, abs_lead := abs(team1_lead)]
deliveries[, lead_per_over_remaining := fifelse(
  overs_remaining > 0, abs_lead / overs_remaining, as.double(abs_lead)
)]

# Follow-on possible (team1 lead >= 200 after the 2nd innings is COMPLETE).
#
# This fired from innings 2 until #24. innings_total_inn2 is that innings'
# FINAL total, so on an innings-2 ball it is future information -- it says
# "this side finished 200+ behind" while they are still batting. It was not a
# harmless extra: 167,786 innings-2 deliveries (10.2% of them) were flagged,
# and P(result) among them was 0.709 against 0.625 for the rest, so Model A
# had a genuine label signal to lean on. Serving always refused to reproduce
# it, which is most of why honest serving scored so far off the holdout.
# Honest semantics, matching .test_wp_features(): only from innings 3.
deliveries[, follow_on_possible := as.integer(
  innings >= 3 &
  !is.na(innings_total_inn1) & !is.na(innings_total_inn2) &
  (innings_total_inn1 - innings_total_inn2) >= 200
)]
deliveries[is.na(follow_on_possible), follow_on_possible := 0L]

# Prior-innings declaration flags (bouncerverse#78).
#
# Same frame-of-reference discipline as follow_on_possible above: an
# innings' OWN declared status is future information until it has actually
# happened, so the raw declared_inn{k} column (from the dcast wide-merge at
# the top of this script, unconditionally populated per match by the
# match_id join) is only safe to read once innings k has ITSELF completed --
# i.e. from innings k+1 onward. Reading it unmasked would leak the current
# innings' own eventual declaration into predictions made mid-innings.
#
# Sized before building: 15.5% of all Test innings end by declaration, and
# among innings that END at 1-3 wickets down, 60-72% are declarations rather
# than dismissals or a stoppage -- exactly the state the model's other
# features (wickets_in_hand, overs_remaining) cannot otherwise distinguish
# from "still batting, not out" (D-P57).
#
# TRAINING-ONLY for now (D-P57, Pete's call): cricinfo, the live-serving data
# source (.test_wp_features() / build_cricinfo_test_win_probability()), has
# no declared field anywhere in its schema -- confirmed by grep across the
# whole cricinfo ingestion path. Serving therefore cannot populate this
# feature today; reconciling that is deliberately deferred rather than
# blocking this cricsheet-side model improvement on it.
deliveries[, prior_declared_inn1 := as.integer(innings > 1L & !is.na(declared_inn1) & declared_inn1)]
deliveries[, prior_declared_inn2 := as.integer(innings > 2L & !is.na(declared_inn2) & declared_inn2)]
deliveries[, prior_declared_inn3 := as.integer(innings > 3L & !is.na(declared_inn3) & declared_inn3)]

# 4th innings specific features
deliveries[innings == 4, `:=`(
  target = as.integer(team1_completed - team2_completed + 1L),
  runs_needed = pmax(0L, as.integer(team1_completed - team2_completed + 1L) - total_runs)
)]
deliveries[innings == 4, `:=`(
  req_rate = fifelse(overs_remaining > 0, as.double(runs_needed) / overs_remaining, 99),
  overs_per_wicket = fifelse(wickets_in_hand > 0, overs_remaining / as.double(wickets_in_hand), 0)
)]

# Fill NAs for non-4th-innings
for (col in c("target", "runs_needed", "req_rate", "overs_per_wicket")) {
  deliveries[is.na(get(col)), (col) := 0]
}

# Add phase features needed by Stage 1 model
deliveries[, `:=`(
  phase_powerplay = 0L,
  phase_middle = as.integer(over >= 20 & over < 80),
  phase_death = 0L,
  phase_new_ball = as.integer(over < 20),
  phase_old_ball = as.integer(over >= 80),
  gender_male = as.integer(tolower(gender) == "male"),
  overs_remaining_innings = 0,
  overs_into_phase = fcase(
    over < 20, as.double(over),
    over < 80, as.double(over - 20),
    default = as.double(over - 80)
  )
)]

# Generate projected scores ----
cli::cli_h2("Generating projected scores")

# Rate projection, everywhere.
#
# Training used the Stage 1 XGBoost projection for innings 1 while serving has
# only ever had the rate projection -- a train/serve divergence on the single
# most-used feature of Model B. Measured before removing it (#24): dropping the
# XGBoost arm costs 0.0004 of holdout mlogloss. It was buying nothing, so the
# divergence closes for free and the Stage 1 model is no longer a dependency
# of this script.
deliveries[, projected_innings_total := total_runs * (90 / pmax(over + ball/6, 1))]

# Projected lead
deliveries[, projected_lead := fcase(
  batting_is_team1 == 1L & innings == 1, as.double(projected_innings_total) - venue_avg,
  batting_is_team1 == 1L, as.double(team1_completed + projected_innings_total - team2_completed) - venue_avg,
  batting_is_team1 == 0L & innings == 2, as.double(team1_completed - (team2_completed + projected_innings_total)),
  default = as.double(team1_lead)
)]

cli::cli_alert_success("Features engineered")

# Sample: one per over ----
cli::cli_h2("Sampling (1 per over)")

sampled <- deliveries[, .SD[.N], by = .(match_id, innings, over)]
cli::cli_alert_info("Sampled {nrow(sampled)} from {nrow(deliveries)} (1 per over)")

n_results <- sum(sampled$is_result[!duplicated(paste0(sampled$match_id, "_", sampled$innings, "_", sampled$over))])
cat(sprintf("  team1_win: %d, draw: %d, team2_win: %d\n",
            sum(sampled$match_outcome == 0), sum(sampled$match_outcome == 1), sum(sampled$match_outcome == 2)))
cat(sprintf("  result matches: %d, draw matches: %d\n",
            uniqueN(sampled[is_result == 1]$match_id), uniqueN(sampled[is_result == 0]$match_id)))

# Fill remaining NAs. set() rather than `[is.na(get(col)), (col) := 0]` --
# get() inside [ breaks data.table's fast column-reference path and leaks RSS
# that gc() cannot see, which matters at 5.3M rows.
fill_na_zero <- function(dt) {
  for (col in names(dt)) {
    if (is.numeric(dt[[col]])) {
      na_i <- which(is.na(dt[[col]]))
      if (length(na_i)) set(dt, na_i, col, 0)
    }
  }
  invisible(dt)
}
fill_na_zero(sampled)
fill_na_zero(deliveries)

# Train/test split
TEST_SEASONS <- c("2024", "2025", "2023/24", "2024/25")
train_dt <- sampled[!season %in% TEST_SEASONS]
test_dt <- sampled[season %in% TEST_SEASONS]

# Ball-level holdout: every delivery of the test seasons, not 1 per over.
# Serving scores every ball, so this is the number comparable to the serving
# evaluation; the sampled one stays for continuity with the historic figure.
ball_test <- deliveries[season %in% TEST_SEASONS]

cli::cli_alert_info("Train: {nrow(train_dt)} samples ({uniqueN(train_dt$match_id)} matches)")
cli::cli_alert_info("Test: {nrow(test_dt)} samples ({uniqueN(test_dt$match_id)} matches)")
cli::cli_alert_info("Test (ball-level): {nrow(ball_test)} deliveries")

# ============================================================
# MODEL A: P(result) — Binary: will this match have a winner?
# ============================================================
cli::cli_h1("Model A: P(result)")

result_features <- c(
  "overs_remaining", "match_progress", "approx_day",
  "time_pressure", "projected_total_overs",
  "venue_result_rate",
  "total_wickets_match", "runs_per_over_match",
  "abs_lead", "lead_per_over_remaining",
  "innings_num", "follow_on_possible",
  "prior_declared_inn1", "prior_declared_inn2", "prior_declared_inn3",
  # Tier 1: derived rain proxies (always available)
  "overs_per_day", "overs_deficit",
  # Tier 2: causal weather (available if backfilled, #72)
  "rain_mm_before", "rain_days_before", "venue_rain_climatology"
)

X_train_A <- as.matrix(train_dt[, ..result_features])
X_test_A <- as.matrix(test_dt[, ..result_features])
y_train_A <- train_dt$is_result
y_test_A <- test_dt$is_result

# Progressive confidence weights: later in match = more confident
weights_A <- 0.5 + 2.5 * train_dt$match_progress^1.5

dtrain_A <- xgb.DMatrix(data = X_train_A, label = y_train_A, weight = weights_A)
dtest_A <- xgb.DMatrix(data = X_test_A, label = y_test_A)

# Grouped CV folds
set.seed(RANDOM_SEED)
unique_matches_A <- unique(train_dt$match_id)
shuffled_A <- sample(unique_matches_A)
fold_ids_A <- cut(seq_along(shuffled_A), breaks = CV_FOLDS, labels = FALSE)
folds_A <- lapply(1:CV_FOLDS, function(i) which(train_dt$match_id %in% shuffled_A[fold_ids_A == i]))

params_A <- list(
  objective = "binary:logistic",
  eval_metric = "logloss",
  max_depth = 3,
  eta = 0.03,
  subsample = 0.8,
  colsample_bytree = 0.8,
  min_child_weight = 30,
  lambda = 5
)

cli::cli_h2("Training Model A (heavy regularization)")
set.seed(RANDOM_SEED)
cv_A <- xgb.cv(
  params = params_A,
  data = dtrain_A,
  nrounds = MAX_ROUNDS,
  folds = folds_A,
  early_stopping_rounds = EARLY_STOPPING,
  verbose = 1,
  print_every_n = 20
)

best_nrounds_A <- cv_A$early_stop$best_iteration %||%
  cv_A$best_iteration %||%
  which.min(cv_A$evaluation_log$test_logloss_mean)
if (is.null(best_nrounds_A) || is.na(best_nrounds_A)) best_nrounds_A <- 100

best_cv_A <- cv_A$evaluation_log$test_logloss_mean[best_nrounds_A]
cli::cli_alert_success("Model A: {best_nrounds_A} rounds, CV logloss: {round(best_cv_A, 4)}")

model_A <- xgb.train(params = params_A, data = dtrain_A, nrounds = best_nrounds_A, verbose = 0)

# Evaluate Model A
pred_result <- predict(model_A, dtest_A)
pred_result_class <- as.integer(pred_result > 0.5)
acc_A <- mean(pred_result_class == y_test_A)
logloss_A <- -mean(y_test_A * log(pmax(pred_result, 1e-7)) +
                     (1 - y_test_A) * log(pmax(1 - pred_result, 1e-7)))
cat(sprintf("\n  Model A test: accuracy=%.1f%%, logloss=%.4f\n", acc_A * 100, logloss_A))

# Result calibration
cli::cli_h3("Model A: Result Calibration")
for (lo in seq(0, 0.8, by = 0.2)) {
  hi <- lo + 0.2
  idx <- pred_result >= lo & pred_result < hi
  if (sum(idx) > 20) {
    cat(sprintf("  P(result) %.0f-%.0f%%: predicted=%.1f%%, actual=%.1f%% (n=%d)\n",
                lo*100, hi*100, mean(pred_result[idx])*100, mean(y_test_A[idx])*100, sum(idx)))
  }
}

# Feature importance Model A
cli::cli_h3("Model A: Feature Importance")
imp_A <- xgb.importance(model = model_A)
for (i in seq_len(min(12, nrow(imp_A)))) {
  cli::cli_alert_info("{i}. {imp_A$Feature[i]}: {round(imp_A$Gain[i], 3)}")
}

# ============================================================
# MODEL B: P(team1_win | result) — trained ONLY on decided matches
# ============================================================
cli::cli_h1("Model B: P(team1_win | result)")

# Filter to result-only matches
train_results <- train_dt[is_result == 1]
test_results <- test_dt[is_result == 1]

y_train_B <- as.integer(train_results$match_outcome == 0)  # 1 = team1_win
y_test_B <- as.integer(test_results$match_outcome == 0)

conditional_features <- c(
  "team1_lead", "projected_lead", "projected_innings_total",
  "batting_is_team1", "wickets_in_hand",
  "overs_remaining", "cum_overs",
  "venue_avg", "innings_num",
  "target", "runs_needed", "req_rate", "overs_per_wicket",
  "current_run_rate",
  "prior_declared_inn1", "prior_declared_inn2", "prior_declared_inn3"
)

X_train_B <- as.matrix(train_results[, ..conditional_features])
X_test_B <- as.matrix(test_results[, ..conditional_features])

# Upweight later innings (more informative)
weights_B <- fifelse(train_results$innings >= 3, 2.0, 1.0)
weights_B <- fifelse(train_results$innings == 4, 3.0, weights_B)

dtrain_B <- xgb.DMatrix(data = X_train_B, label = y_train_B, weight = weights_B)
dtest_B <- xgb.DMatrix(data = X_test_B, label = y_test_B)

# Grouped CV folds (result matches only)
set.seed(RANDOM_SEED)
unique_matches_B <- unique(train_results$match_id)
shuffled_B <- sample(unique_matches_B)
fold_ids_B <- cut(seq_along(shuffled_B), breaks = CV_FOLDS, labels = FALSE)
folds_B <- lapply(1:CV_FOLDS, function(i) which(train_results$match_id %in% shuffled_B[fold_ids_B == i]))

params_B <- list(
  objective = "binary:logistic",
  eval_metric = "logloss",
  max_depth = 4,
  eta = 0.05,
  subsample = 0.8,
  colsample_bytree = 0.8,
  min_child_weight = 10,
  lambda = 2
)

cli::cli_h2("Training Model B (result matches only)")
cat(sprintf("  Training on %d samples from %d result matches\n",
            nrow(train_results), uniqueN(train_results$match_id)))

set.seed(RANDOM_SEED)
cv_B <- xgb.cv(
  params = params_B,
  data = dtrain_B,
  nrounds = MAX_ROUNDS,
  folds = folds_B,
  early_stopping_rounds = EARLY_STOPPING,
  verbose = 1,
  print_every_n = 20
)

best_nrounds_B <- cv_B$early_stop$best_iteration %||%
  cv_B$best_iteration %||%
  which.min(cv_B$evaluation_log$test_logloss_mean)
if (is.null(best_nrounds_B) || is.na(best_nrounds_B)) best_nrounds_B <- 100

best_cv_B <- cv_B$evaluation_log$test_logloss_mean[best_nrounds_B]
cli::cli_alert_success("Model B: {best_nrounds_B} rounds, CV logloss: {round(best_cv_B, 4)}")

model_B <- xgb.train(params = params_B, data = dtrain_B, nrounds = best_nrounds_B, verbose = 0)

# Evaluate Model B alone
pred_team1 <- predict(model_B, dtest_B)
pred_team1_class <- as.integer(pred_team1 > 0.5)
acc_B <- mean(pred_team1_class == y_test_B)
logloss_B <- -mean(y_test_B * log(pmax(pred_team1, 1e-7)) +
                     (1 - y_test_B) * log(pmax(1 - pred_team1, 1e-7)))
cat(sprintf("\n  Model B test: accuracy=%.1f%%, logloss=%.4f\n", acc_B * 100, logloss_B))

# Feature importance Model B
cli::cli_h3("Model B: Feature Importance")
imp_B <- xgb.importance(model = model_B)
for (i in seq_len(min(12, nrow(imp_B)))) {
  cli::cli_alert_info("{i}. {imp_B$Feature[i]}: {round(imp_B$Gain[i], 3)}")
}

# ============================================================
# COMBINED EVALUATION
# ============================================================
cli::cli_h1("Combined Evaluation")

# Get P(result) for ALL test samples
p_result_all <- predict(model_A, dtest_A)

# Get P(team1_win | result) for ALL test samples (model trained on results only, but can predict on all)
X_test_B_all <- as.matrix(test_dt[, ..conditional_features])
dtest_B_all <- xgb.DMatrix(data = X_test_B_all)
p_team1_given_result_all <- predict(model_B, dtest_B_all)

# Combined 3-class probabilities
p_draw <- 1 - p_result_all
p_team1_win <- p_result_all * p_team1_given_result_all
p_team2_win <- p_result_all * (1 - p_team1_given_result_all)

# Assemble probability matrix
pred_probs <- cbind(team1_win = p_team1_win, draw = p_draw, team2_win = p_team2_win)

# 3-class mlogloss
y_test <- test_dt$match_outcome
eps <- 1e-7
overall_mlogloss <- -mean(sapply(seq_along(y_test), function(i) {
  log(max(pred_probs[i, y_test[i] + 1], eps))
}))

pred_class <- max.col(pred_probs) - 1
overall_acc <- mean(pred_class == y_test)
baseline_random <- -log(1/3)

cat(sprintf("\n  COMBINED: accuracy=%.1f%%, mlogloss=%.4f (random=%.4f, improvement=%+.1f%%)\n",
            overall_acc * 100, overall_mlogloss, baseline_random,
            (baseline_random - overall_mlogloss) / baseline_random * 100))

# Per-class recall
for (cls in 0:2) {
  cls_name <- c("Team1 Win", "Draw", "Team2 Win")[cls + 1]
  idx <- y_test == cls
  if (sum(idx) > 0) {
    recall <- mean(pred_class[idx] == cls)
    cat(sprintf("  %-12s recall=%.1f%% (%d samples)\n", cls_name, recall * 100, sum(idx)))
  }
}

# Per-innings
cli::cli_h3("Performance by Innings")
for (inn in 1:4) {
  idx <- test_dt$innings == inn
  if (sum(idx) > 0) {
    inn_acc <- mean(pred_class[idx] == y_test[idx])
    inn_probs <- pred_probs[idx, ]
    inn_actual <- y_test[idx]
    inn_ml <- -mean(sapply(seq_along(inn_actual), function(i) {
      log(max(inn_probs[i, inn_actual[i] + 1], eps))
    }))
    imp <- (baseline_random - inn_ml) / baseline_random * 100
    cat(sprintf("  Innings %d: accuracy=%.1f%%, mlogloss=%.4f (%+.1f%% vs random), n=%d\n",
                inn, inn_acc * 100, inn_ml, imp, sum(idx)))
  }
}

# Per-day
cli::cli_h3("Performance by Day")
for (day in 1:5) {
  idx <- test_dt$approx_day == day
  if (sum(idx) > 10) {
    day_acc <- mean(pred_class[idx] == y_test[idx])
    day_probs <- pred_probs[idx, ]
    day_actual <- y_test[idx]
    day_ml <- -mean(sapply(seq_along(day_actual), function(i) {
      log(max(day_probs[i, day_actual[i] + 1], eps))
    }))
    imp <- (baseline_random - day_ml) / baseline_random * 100
    cat(sprintf("  Day %d: accuracy=%.1f%%, mlogloss=%.4f (%+.1f%% vs random), n=%d\n",
                day, day_acc * 100, day_ml, imp, sum(idx)))
  }
}

# Draw calibration (the key test!)
cli::cli_h3("Draw Calibration (KEY METRIC)")
draw_probs_vec <- pred_probs[, "draw"]
actual_draw <- as.integer(y_test == 1)
for (lo in seq(0, 0.8, by = 0.2)) {
  hi <- lo + 0.2
  idx <- draw_probs_vec >= lo & draw_probs_vec < hi
  if (sum(idx) > 20) {
    cat(sprintf("  P(draw) %.0f-%.0f%%: predicted=%.1f%%, actual=%.1f%% (n=%d)\n",
                lo*100, hi*100, mean(draw_probs_vec[idx])*100, mean(actual_draw[idx])*100, sum(idx)))
  }
}
idx80 <- draw_probs_vec >= 0.8
if (sum(idx80) > 10) {
  cat(sprintf("  P(draw) 80-100%%: predicted=%.1f%%, actual=%.1f%% (n=%d)\n",
              mean(draw_probs_vec[idx80])*100, mean(actual_draw[idx80])*100, sum(idx80)))
}

# ============================================================
# BALL-LEVEL EVALUATION + ANCHORS (#24)
# ============================================================
# The sampled number above is 1-per-over; serving scores every delivery, so
# the ball-level number is the one comparable to the serving evaluation.
# Baseline throughout is the ball-frequency base rate of the same rows -- not
# -log(1/3), which flatters everything by ignoring that draws are common.
cli::cli_h1("Ball-level evaluation")

eps <- 1e-7
mlog3 <- function(P, y) -mean(log(pmax(P[cbind(seq_along(y), y + 1L)], eps)))
freq_base <- function(y) {
  p <- as.numeric(table(factor(y, levels = 0:2)) / length(y))
  mlog3(matrix(p, nrow = length(y), ncol = 3, byrow = TRUE), y)
}

p_res_ball <- predict(model_A, xgb.DMatrix(as.matrix(ball_test[, ..result_features])))
p_t1_ball <- predict(model_B, xgb.DMatrix(as.matrix(ball_test[, ..conditional_features])))
P_ball <- cbind(team1_win = p_res_ball * p_t1_ball,
                draw = 1 - p_res_ball,
                team2_win = p_res_ball * (1 - p_t1_ball))
y_ball <- ball_test$match_outcome

ball_ml <- mlog3(P_ball, y_ball)
ball_base <- freq_base(y_ball)
ball_acc <- mean(max.col(P_ball) - 1 == y_ball)
cat(sprintf("\n  BALL-LEVEL: mlogloss=%.4f (ball-frequency baseline=%.4f), accuracy=%.1f%%\n",
            ball_ml, ball_base, 100 * ball_acc))

inn_ml <- inn_base <- rep(NA_real_, 4)
for (i in 1:4) {
  k <- ball_test$innings == i
  if (sum(k) < 50) next
  inn_ml[i] <- mlog3(P_ball[k, , drop = FALSE], y_ball[k])
  inn_base[i] <- freq_base(y_ball[k])
  cat(sprintf("  Innings %d: %.4f vs baseline %.4f  [%s]  n=%d\n",
              i, inn_ml[i], inn_base[i],
              if (inn_ml[i] < inn_base[i]) "beats" else "WORSE THAN", sum(k)))
}

draw_p <- P_ball[, "draw"]
k80 <- draw_p >= 0.8
draw80_actual <- if (sum(k80) > 20) mean(y_ball[k80] == 1) else NA_real_
cat(sprintf("  P(draw)>=0.8 -> actual draw rate %.1f%% (n=%d)\n",
            100 * draw80_actual, sum(k80)))

fo_gain <- {
  r <- match("follow_on_possible", imp_A$Feature)
  if (is.na(r)) 0 else imp_A$Gain[r]
}

# Anchors, declared in bouncerverse#24 before any model was fitted. These are
# assertions, not prints: a check that lives in a log nobody reads does not run.
cli::cli_h3("Anchor checks")
anchor <- function(label, ok) {
  cat(sprintf("  [%s] %s\n", if (isTRUE(ok)) "PASS" else "FAIL", label))
  isTRUE(ok)
}
a3 <- anchor("A3a ball-level beats the ball-frequency baseline", ball_ml < ball_base)
a3b <- anchor("A3b innings 2 and 3 each beat their own baseline",
              inn_ml[2] < inn_base[2] && inn_ml[3] < inn_base[3])
# Raw mlogloss, which is how A4 was declared and how #24 states the figure it
# is anchored against (innings 4 = 0.6893). Note it is NOT the lowest
# ratio-to-baseline -- innings 3 edges innings 4 on that -- because innings-4
# rows have a much lower baseline to beat.
a4 <- anchor("A4  innings 4 is the strongest innings (lowest mlogloss)",
             which.min(inn_ml) == 4L)
a5 <- anchor("A5  P(draw)>=0.8 bucket is >= 70% actual draws",
             !is.na(draw80_actual) && draw80_actual >= 0.70)
a2 <- anchor("A2  follow_on_possible is no longer a top-5 feature of A",
             is.na(match("follow_on_possible", imp_A$Feature)) ||
               match("follow_on_possible", imp_A$Feature) > 5)
if (!all(a3, a3b, a4, a5, a2)) {
  stop("Anchor check failed -- the method is wrong, not the anchor. ",
       "Do not ship these models until it is understood (bouncerverse#24).")
}

# Comparison with v2 (single model)
cli::cli_h3("Comparison with v2 (single 3-class model)")
v2_path <- file.path(output_dir, "test_winprob_results.rds")
if (file.exists(v2_path)) {
  v2 <- readRDS(v2_path)
  cat(sprintf("  v2: mlogloss=%.4f (%+.1f%% vs random)\n",
              v2$metrics$mlogloss, v2$metrics$improvement))
  cat(sprintf("  v3: mlogloss=%.4f (%+.1f%% vs random)\n",
              overall_mlogloss,
              (baseline_random - overall_mlogloss) / baseline_random * 100))
  delta <- v2$metrics$mlogloss - overall_mlogloss
  cat(sprintf("  Delta: %+.4f (%s)\n", delta, if (delta > 0) "v3 BETTER" else "v2 better"))
} else {
  cli::cli_alert_info("No v2 results found for comparison")
}

# Save ----
cli::cli_h2("Saving")

xgb.save(model_A, file.path(output_dir, "test_result_model.ubj"))
xgb.save(model_B, file.path(output_dir, "test_conditional_win_model.ubj"))

saveRDS(list(
  model_A = model_A,
  model_B = model_B,
  params_A = params_A,
  params_B = params_B,
  best_nrounds_A = best_nrounds_A,
  best_nrounds_B = best_nrounds_B,
  result_features = result_features,
  conditional_features = conditional_features,
  metrics = list(
    accuracy = overall_acc,
    mlogloss = overall_mlogloss,
    cv_logloss_A = best_cv_A,
    cv_logloss_B = best_cv_B,
    test_logloss_A = logloss_A,
    test_logloss_B = logloss_B,
    test_accuracy_A = acc_A,
    test_accuracy_B = acc_B,
    baseline = baseline_random,
    improvement = (baseline_random - overall_mlogloss) / baseline_random * 100,
    # #24: ball-level is the figure comparable to serving; the baseline is the
    # ball-frequency base rate of the same rows, not -log(1/3).
    ball_mlogloss = ball_ml,
    ball_baseline = ball_base,
    ball_accuracy = ball_acc,
    ball_mlogloss_by_innings = inn_ml,
    ball_baseline_by_innings = inn_base,
    draw80_actual = draw80_actual,
    follow_on_gain_A = fo_gain
  ),
  importance_A = imp_A,
  importance_B = imp_B,
  version = "v3_decomposed",
  created_at = Sys.time()
), file.path(output_dir, "test_winprob_v3_results.rds"))

cli::cli_alert_success("Saved: test_result_model.ubj, test_conditional_win_model.ubj, test_winprob_v3_results.rds")

cat(sprintf("\n  SUMMARY: %.1f%% accuracy, mlogloss %.4f (%+.1f%% vs random)\n",
            overall_acc * 100, overall_mlogloss,
            (baseline_random - overall_mlogloss) / baseline_random * 100))
cat(sprintf("  Model A (result): logloss %.4f | Model B (conditional): logloss %.4f\n",
            logloss_A, logloss_B))
