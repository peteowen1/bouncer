# Baseline Projected Score Model (Team-Agnostic) ----
#
# This script builds a simple baseline model for projected score that is
# team-agnostic. It predicts what an "average team" would score given:
#   - Venue characteristics
#   - Home/away status
#   - Toss decision
#   - Match context (knockout vs league)
#
# This baseline is used in the innings 1 win probability model to determine
# if a team is scoring "above par" or "below par" for the venue.

# Setup ----
library(DBI)
library(dplyr)
devtools::load_all()

cat("\n")
cli::cli_h1("Baseline Projected Score Model (Team-Agnostic)")
cat("\n")

# Database Connection ----
cli::cli_h2("Connecting to database")
# with_db_connection(), not a bare open + top-level on.exit(): on.exit() at a
# script's top level binds to source()'s per-statement eval() frame and fires
# IMMEDIATELY rather than at script end, when this script is sourced from a
# wrapper (run_in_match_pipeline.R sources every numbered step) -- repro'd
# directly, same fix already applied in 01_prepare_all_formats.R. A bare
# explicit dbDisconnect() after the last query fixes that but drops cleanup on
# the error path (a query throwing between open and that line leaks the
# connection); with_db_connection() closes on both the normal and error path,
# which matters here because run_in_match_pipeline.R treats this script's
# failure as non-fatal and keeps running (a leaked read-only connection still
# holds a lock that can fail a later WRITE connection in the same session --
# see this repo's CLAUDE.md DuckDB-lock troubleshooting section).

# Configuration ----
# Honour caller-supplied values (run_in_match_pipeline.R sets these) rather than
# overwriting them. Hard-coding MATCH_TYPE meant this always built a T20 IPL
# baseline whatever format the pipeline was running, and then wrote it under an
# ipl_* name that 04_win_probability_innings1.R read for every format. #49.
if (!exists("EVENT_FILTER")) EVENT_FILTER <- "Indian Premier League"
if (!exists("MATCH_TYPE")) MATCH_TYPE <- "t20"
# MIN_VENUE_MATCHES hard cutoff removed (#82) -- superseded by the
# shrinkage-to-prior in time_causal_venue_mean(), same as #80.

# CROSS_COMPETITION: bouncerverse#83 found this script's IPL-only scope leaves
# 82-89% of 04_win_probability_innings1.R's cross-competition T20 training
# corpus with a single flat constant (no venue signal at all), because that
# script trains on EVERY T20 competition while this one only ever saw IPL.
# When TRUE, EVENT_FILTER is ignored, the query covers every competition for
# MATCH_TYPE, and venue shrinkage becomes a 2-level hierarchy (venue ->
# competition -> global root, time_causal_hierarchical_mean()) instead of a
# single shrink straight to one flat scalar -- a sparse competition borrows
# strength from the global level instead of degenerating to a constant.
#
# Default TRUE as of 2026-08-29 (MODELLING-IDEAS.md), after an end-to-end
# retrain comparison on T20: 04's held-out log loss 0.5960 -> 0.5913, Brier
# 0.2063 -> 0.2045, AUC 0.7379 -> 0.7426, causal coverage 4.3% -> 100%, no
# metric moved the wrong way. Also fixes a worse, previously-unmeasured defect
# for ODI/Test specifically: run_in_match_pipeline.R sets EVENT_FILTER to the
# IPL for every format, and the IPL has no ODI or Test matches at all, so the
# FALSE-default path queries ZERO rows for those two formats. Not re-validated
# for ODI/Test tonight -- flagged in DECISIONS.md, worth a proper retrain
# comparison for those formats too rather than assuming the T20 result holds.
if (!exists("CROSS_COMPETITION")) CROSS_COMPETITION <- TRUE
if (!exists("OUTPUT_SUFFIX")) OUTPUT_SUFFIX <- ""

query_data <- with_db_connection(function(conn) {

# Load Historical Match Data ----
cli::cli_h2("Loading historical match data")

if (CROSS_COMPETITION) {
  matches_query <- "
    SELECT
      m.match_id,
      m.season,
      m.match_date,
      m.venue,
      m.city,
      m.gender,
      m.event_name,
      m.team1,
      m.team2,
      m.toss_winner,
      m.toss_decision,
      m.outcome_winner,
      m.event_match_number,
      m.event_group
    FROM cricsheet.matches m
    WHERE LOWER(m.match_type) = ?
      AND m.outcome_winner IS NOT NULL
      AND m.outcome_winner != ''
    ORDER BY m.match_date
  "
  matches_df <- DBI::dbGetQuery(conn, matches_query, params = list(MATCH_TYPE))
} else {
  matches_query <- "
    SELECT
      m.match_id,
      m.season,
      m.match_date,
      m.venue,
      m.city,
      m.team1,
      m.team2,
      m.toss_winner,
      m.toss_decision,
      m.outcome_winner,
      m.event_match_number,
      m.event_group
    FROM cricsheet.matches m
    WHERE m.event_name LIKE ?
      AND LOWER(m.match_type) = ?
      AND m.outcome_winner IS NOT NULL
      AND m.outcome_winner != ''
    ORDER BY m.match_date
  "
  matches_df <- DBI::dbGetQuery(conn, matches_query, params = list(
    paste0("%", EVENT_FILTER, "%"),
    MATCH_TYPE
  ))
}

cli::cli_alert_success("Loaded {nrow(matches_df)} matches")

# Load Innings Totals ----
cli::cli_h2("Loading innings totals")

if (CROSS_COMPETITION) {
  innings_query <- "
    SELECT
      mi.match_id,
      mi.innings,
      mi.batting_team,
      mi.total_runs,
      mi.total_wickets,
      mi.total_overs
    FROM cricsheet.match_innings mi
    WHERE mi.match_id IN (
      SELECT match_id FROM cricsheet.matches
      WHERE LOWER(match_type) = ?
        AND outcome_winner IS NOT NULL
        AND outcome_winner != ''
    )
    ORDER BY mi.match_id, mi.innings
  "
  innings_df <- DBI::dbGetQuery(conn, innings_query, params = list(MATCH_TYPE))
} else {
  innings_query <- "
    SELECT
      mi.match_id,
      mi.innings,
      mi.batting_team,
      mi.total_runs,
      mi.total_wickets,
      mi.total_overs
    FROM cricsheet.match_innings mi
    WHERE mi.match_id IN (
      SELECT match_id FROM cricsheet.matches
      WHERE event_name LIKE ?
        AND LOWER(match_type) = ?
        AND outcome_winner IS NOT NULL
        AND outcome_winner != ''
    )
    ORDER BY mi.match_id, mi.innings
  "
  innings_df <- DBI::dbGetQuery(conn, innings_query, params = list(
    paste0("%", EVENT_FILTER, "%"),
    MATCH_TYPE
  ))
}

cli::cli_alert_success("Loaded innings data for {length(unique(innings_df$match_id))} matches")

list(matches_df = matches_df, innings_df = innings_df)

}, read_only = TRUE)

matches_df <- query_data$matches_df
innings_df <- query_data$innings_df
cli::cli_alert_success("Connected to database, queried, and disconnected")

# Calculate Venue Statistics ----
cli::cli_h2("Calculating venue statistics")

# Get first innings scores by venue
if (CROSS_COMPETITION) {
  first_innings <- innings_df %>%
    filter(innings == 1) %>%
    left_join(matches_df %>% select(match_id, venue, match_date, season, gender, event_name),
              by = "match_id")
} else {
  first_innings <- innings_df %>%
    filter(innings == 1) %>%
    left_join(matches_df %>% select(match_id, venue, match_date, season), by = "match_id")
}

# TIME-CAUSAL, not whole-history (bouncerverse#82 -- the same leak #80 fixed in
# 01_prepare_all_formats.R, one script hop downstream and un-scoped to IPL, so
# not fixed by that PR). Averaging every match at a venue including the one
# being predicted meant a live prediction saw its own future: at a one-match
# venue the feature simply WAS that match's own total.
fi_dt <- data.table::as.data.table(first_innings)
fi_dt[, match_date := as.Date(match_date)]

if (CROSS_COMPETITION) {
  # 2-level hierarchical shrink (bouncerverse#83): venue, WITHIN gender (a
  # shared ground name like "Melbourne Cricket Ground" hosts very differently
  # scoring men's and women's matches) -> competition, within gender -> global
  # root (both genders pooled -- root_prior_weight is small enough that this
  # pooling only matters for the very first handful of matches of a brand new
  # venue/competition, before its own level has any evidence). Screened
  # against the single-level IPL-only prior in docs/plans/MODELLING-IDEAS.md:
  # cross-competition scope cut T20 male RMSE 39.49 -> 36.58, the added
  # competition level a further -0.7%.
  # paste(NA, "Male") is the STRING "NA Male", not NA -- so any row with a
  # join-miss venue/gender/event_name (rain-affected/abandoned matches with a
  # recorded innings total but no outcome_winner) would silently blend into
  # one shared pseudo-venue/competition bucket instead of erroring or standing
  # out. The outcome_winner filter above on innings_query already keeps these
  # out in practice; this is a second, defensive line in case some other gap
  # (a genuinely missing venue/gender in cricsheet.matches) produces an NA here.
  n_before <- nrow(fi_dt)
  fi_dt <- fi_dt[!is.na(venue) & !is.na(gender) & !is.na(event_name)]
  if (nrow(fi_dt) < n_before) {
    cli::cli_warn("Dropped {n_before - nrow(fi_dt)} first-innings row(s) with NA venue/gender/event_name before hierarchical shrinkage.")
  }
  fi_dt[, venue_g := paste(venue, gender)]
  fi_dt[, competition_g := paste(event_name, gender)]
  venue_causal <- time_causal_hierarchical_mean(
    fi_dt, "total_runs",
    levels = c("venue_g", "competition_g"),
    weights = c(venue_g = 5, competition_g = 20),
    root_prior_weight = 30
  )
  venue_causal <- venue_causal[, .(match_id, venue_avg_score = hier_mean)]
} else {
  # Kept scoped to IPL (not reusing 01_prepare_all_formats.R's cross-competition
  # venue_avg_score) because this script's whole point, in this mode, is an
  # IPL-specific baseline -- an IPL ground's scoring level is not assumed
  # identical to the same ground hosting international cricket.
  venue_causal <- time_causal_venue_mean(fi_dt, "total_runs", prior_weight = 5)
  venue_causal <- venue_causal[, .(match_id, venue_avg_score = venue_mean)]
}

first_innings <- first_innings %>%
  left_join(as.data.frame(venue_causal), by = "match_id")

# Overall average (used both as the shrinkage prior and as the fallback for
# venues/matches with no causal value)
overall_avg_score <- mean(first_innings$total_runs, na.rm = TRUE)
overall_sd_score <- sd(first_innings$total_runs, na.rm = TRUE)

cli::cli_alert_info("Overall {if (CROSS_COMPETITION) 'T20 (all competitions)' else 'IPL'} 1st innings average: {round(overall_avg_score, 1)} (SD: {round(overall_sd_score, 1)})")

# Per-venue snapshot, for a genuinely new/unseen match at serving time. This is
# NOT the training-time feature above -- it deliberately uses every known match
# at the venue (a live prediction legitimately has all completed history
# available), just regularized by the same shrinkage-to-prior instead of the
# old hard MIN_VENUE_MATCHES cutoff (superseded by shrinkage, same as #80).
# Only reached for a match_id absent from venue_stats_by_match, i.e. a
# genuinely new fixture -- everything in THIS script's own training/eval
# corpus gets the causal value above, keyed by venue alone (not gender) to
# match 04_win_probability_innings1.R's existing fallback join key.
if (CROSS_COMPETITION) {
  comp_stats <- first_innings %>%
    group_by(event_name) %>%
    summarise(
      comp_avg_score = (sum(total_runs, na.rm = TRUE) + 30 * overall_avg_score) /
        (sum(!is.na(total_runs)) + 30),
      .groups = "drop"
    )
  venue_stats <- first_innings %>%
    left_join(comp_stats, by = "event_name") %>%
    group_by(venue) %>%
    summarise(
      n_matches = n(),
      venue_avg_score = (sum(total_runs, na.rm = TRUE) + 5 * mean(comp_avg_score, na.rm = TRUE)) /
        (sum(!is.na(total_runs)) + 5),
      venue_sd_score = sd(total_runs, na.rm = TRUE),
      venue_median_score = median(total_runs, na.rm = TRUE),
      venue_min_score = min(total_runs, na.rm = TRUE),
      venue_max_score = max(total_runs, na.rm = TRUE),
      .groups = "drop"
    )
} else {
  venue_stats <- first_innings %>%
    group_by(venue) %>%
    summarise(
      n_matches = n(),
      venue_avg_score = (sum(total_runs, na.rm = TRUE) + 5 * overall_avg_score) /
        (sum(!is.na(total_runs)) + 5),
      venue_sd_score = sd(total_runs, na.rm = TRUE),
      venue_median_score = median(total_runs, na.rm = TRUE),
      venue_min_score = min(total_runs, na.rm = TRUE),
      venue_max_score = max(total_runs, na.rm = TRUE),
      .groups = "drop"
    )
}

cli::cli_alert_success("Calculated stats for {nrow(venue_stats)} venues")

# Per-match causal values, for training/eval rows where the match itself is
# known (04_win_probability_innings1.R joins this by match_id, not venue).
venue_stats_by_match <- venue_causal

# Build Match-Level Dataset ----
cli::cli_h2("Building match-level dataset")

# Join matches with innings and venue stats
match_data <- matches_df %>%
  # Add first innings info
  left_join(
    innings_df %>%
      filter(innings == 1) %>%
      select(match_id, batting_team_1 = batting_team, innings1_total = total_runs),
    by = "match_id"
  ) %>%
  # Add second innings info
  left_join(
    innings_df %>%
      filter(innings == 2) %>%
      select(match_id, batting_team_2 = batting_team, innings2_total = total_runs),
    by = "match_id"
  ) %>%
  # Venue AVERAGE: causal, per-match (#82) -- this feeds baseline_projected_score,
  # so it must not see the match's own score. Joined by match_id, not venue.
  left_join(as.data.frame(venue_causal), by = "match_id") %>%
  # Venue SD: the as-of-now per-venue snapshot. Diagnostic-only (feeds just the
  # printed z-score below, not baseline_projected_score), so left un-causal
  # rather than duplicating the shrinkage machinery for a display-only number.
  left_join(venue_stats %>% select(venue, venue_sd_score), by = "venue") %>%
  # Fill missing venue stats with overall average
  mutate(
    venue_avg_score = coalesce(venue_avg_score, overall_avg_score),
    venue_sd_score = coalesce(venue_sd_score, overall_sd_score)
  ) %>%
  # Calculate derived features
  mutate(
    # Home team indicator (team1 is usually home in IPL)
    is_home_team1 = 1,  # In IPL, team1 is typically the home team

    # Toss features
    toss_winner_batted_first = as.integer(
      (toss_winner == batting_team_1 & toss_decision == "bat") |
      (toss_winner != batting_team_1 & toss_decision == "field")
    ),
    chose_to_bat = as.integer(toss_decision == "bat"),

    # Knockout indicator (finals, eliminators, qualifiers)
    is_knockout = as.integer(
      grepl("final|eliminator|qualifier", tolower(event_match_number)) |
      grepl("final|eliminator|qualifier", tolower(event_group))
    ),

    # Season trend (IPL scores have generally increased over time)
    season_numeric = as.numeric(gsub("/.*", "", season)),

    # Did batting first team win?
    batting_first_won = as.integer(outcome_winner == batting_team_1),

    # Score relative to venue average
    innings1_vs_venue_avg = innings1_total - venue_avg_score,
    innings1_vs_venue_zscore = (innings1_total - venue_avg_score) / venue_sd_score
  ) %>%
  filter(!is.na(innings1_total), !is.na(innings2_total))

# A duplicate match_id anywhere upstream (venue_causal, an innings re-scrape)
# would fan this join out silently -- every join above is meant to be at most
# 1:1 on match_id, but nothing before this line actually checked that.
stopifnot("match-level join must not exceed one row per source match" =
            nrow(match_data) <= nrow(matches_df))

cli::cli_alert_success("Built dataset with {nrow(match_data)} complete matches")

# Analyze Baseline Factors ----
cli::cli_h2("Analyzing baseline factors")

# Venue effect
cli::cli_h3("Score by Venue (top 10 by matches)")
venue_summary <- match_data %>%
  group_by(venue) %>%
  summarise(
    matches = n(),
    avg_1st_innings = mean(innings1_total),
    avg_2nd_innings = mean(innings2_total),
    batting_first_win_rate = mean(batting_first_won),
    .groups = "drop"
  ) %>%
  arrange(desc(matches)) %>%
  head(10)

print(venue_summary)
cat("\n")

# Toss effect
cli::cli_h3("Toss Decision Impact")
toss_summary <- match_data %>%
  group_by(chose_to_bat) %>%
  summarise(
    matches = n(),
    avg_1st_innings = mean(innings1_total),
    batting_first_win_rate = mean(batting_first_won),
    .groups = "drop"
  ) %>%
  mutate(decision = ifelse(chose_to_bat == 1, "Bat First", "Field First"))

print(toss_summary)
cat("\n")

# Knockout effect
cli::cli_h3("Knockout vs League Matches")
knockout_summary <- match_data %>%
  group_by(is_knockout) %>%
  summarise(
    matches = n(),
    avg_1st_innings = mean(innings1_total),
    avg_2nd_innings = mean(innings2_total),
    batting_first_win_rate = mean(batting_first_won),
    .groups = "drop"
  ) %>%
  mutate(match_type = ifelse(is_knockout == 1, "Knockout", "League"))

print(knockout_summary)
cat("\n")

# Season trend
cli::cli_h3("Score Trend by Season")
season_summary <- match_data %>%
  group_by(season_numeric) %>%
  summarise(
    matches = n(),
    avg_1st_innings = mean(innings1_total),
    .groups = "drop"
  ) %>%
  filter(matches >= 10) %>%
  arrange(season_numeric)

print(season_summary)
cat("\n")

# Build Simple Baseline Model ----
cli::cli_h2("Building baseline projected score model")

# For a simple baseline, we use:
# baseline_projected_score = venue_avg + toss_adjustment + knockout_adjustment + season_trend

# Calculate adjustments from data
toss_bat_filtered <- toss_summary %>% filter(chose_to_bat == 1)
toss_bat_adjustment <- if (nrow(toss_bat_filtered) > 0) {
  toss_bat_filtered$avg_1st_innings - overall_avg_score
} else {
  0  # No adjustment if no data
}

knockout_filtered <- knockout_summary %>% filter(is_knockout == 1)
knockout_adjustment <- if (nrow(knockout_filtered) > 0) {
  knockout_filtered$avg_1st_innings - overall_avg_score
} else {
  0  # No adjustment if no knockout matches
}

# Season trend: simple linear trend
season_model <- lm(avg_1st_innings ~ season_numeric, data = season_summary)
season_slope <- coef(season_model)[2]

cli::cli_alert_info("Toss bat-first adjustment: {round(toss_bat_adjustment, 2)} runs")
cli::cli_alert_info("Knockout adjustment: {round(knockout_adjustment, 2)} runs")
cli::cli_alert_info("Season trend: {round(season_slope, 2)} runs/year")

# Create baseline model function
baseline_model <- list(
  overall_avg = overall_avg_score,
  overall_sd = overall_sd_score,
  # As-of-now per-venue snapshot (all history at that venue) -- correct for a
  # genuinely new/unseen match at serving time, NOT for joining onto a
  # training/eval row that's already in venue_stats_by_match (#82).
  venue_stats = venue_stats,
  # Causal, per-match_id (#82). 04_win_probability_innings1.R must join this by
  # match_id for any row from the training/eval corpus; venue_stats above is
  # only the fallback for a match_id it doesn't recognize.
  venue_stats_by_match = venue_stats_by_match,
  toss_bat_adjustment = toss_bat_adjustment,
  knockout_adjustment = knockout_adjustment,
  season_slope = season_slope,
  season_baseline = min(season_summary$season_numeric),
  created_at = Sys.time()
)

# Apply baseline model to match data
match_data <- match_data %>%
  mutate(
    baseline_projected_score = venue_avg_score +
      (chose_to_bat * toss_bat_adjustment) +
      (is_knockout * knockout_adjustment) +
      ((season_numeric - baseline_model$season_baseline) * season_slope),

    # How much above/below baseline is the actual score
    score_vs_baseline = innings1_total - baseline_projected_score
  )

# Evaluate baseline model
baseline_rmse <- sqrt(mean((match_data$innings1_total - match_data$baseline_projected_score)^2))
baseline_mae <- mean(abs(match_data$innings1_total - match_data$baseline_projected_score))
baseline_r2 <- cor(match_data$innings1_total, match_data$baseline_projected_score)^2

cli::cli_h3("Baseline Model Performance")
cli::cli_alert_info("RMSE: {round(baseline_rmse, 2)} runs")
cli::cli_alert_info("MAE: {round(baseline_mae, 2)} runs")
cli::cli_alert_info("R-squared: {round(baseline_r2, 4)}")
cat("\n")

# This is expected to be modest - team strength and match-specific factors matter!
cli::cli_alert_info("Note: Low R-squared is expected - this is a team-agnostic baseline")
cli::cli_alert_info("The baseline captures venue/context effects, not team quality")

# Save Baseline Model ----
cli::cli_h2("Saving baseline model")

if (!exists("output_dir")) output_dir <- file.path(find_bouncerdata_dir(), "models")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# OUTPUT_SUFFIX lets a CROSS_COMPETITION run be compared against the
# production IPL-only baseline without overwriting it (e.g. "_hier" ->
# t20_hier_baseline_projected_score.rds). Empty by default: unchanged path.
baseline_path <- file.path(output_dir, paste0(MATCH_TYPE, OUTPUT_SUFFIX, "_baseline_projected_score.rds"))
saveRDS(baseline_model, baseline_path)
cli::cli_alert_success("Saved baseline model to {baseline_path}")

# Save venue stats separately for easy lookup
venue_stats_path <- file.path(output_dir, paste0(MATCH_TYPE, OUTPUT_SUFFIX, "_baseline_venue_stats.rds"))
saveRDS(venue_stats, venue_stats_path)
cli::cli_alert_success("Saved venue stats to {venue_stats_path}")

# Done ----
cat("\n")
cli::cli_alert_success("Baseline projected score model complete!")
cat("\n")

cli::cli_h3("Model Summary")
cat(sprintf("  Overall IPL average: %.1f runs\n", overall_avg_score))
cat(sprintf("  Venues with stats: %d\n", nrow(venue_stats)))
cat(sprintf("  Toss bat-first effect: %+.1f runs\n", toss_bat_adjustment))
cat(sprintf("  Knockout effect: %+.1f runs\n", knockout_adjustment))
cat(sprintf("  Season trend: %+.2f runs/year\n", season_slope))
cat(sprintf("  Baseline RMSE: %.1f runs\n", baseline_rmse))
cat("\n")

cli::cli_h3("Usage")
cli::cli_bullets(c(
 "i" = "Use baseline_projected_score as the 'par score' for a venue",
 "i" = "Compare actual/projected score to baseline to see if team is above/below par",
 "i" = "This feeds into innings 1 win probability model"
))
cat("\n")
