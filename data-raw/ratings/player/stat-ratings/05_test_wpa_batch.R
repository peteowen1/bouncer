# Batch Test Win Probability Added ----
#
# Runs the v3 decomposed Test WP model over all historical Test deliveries
# from Cricsheet, computes per-delivery WPA, aggregates per player per match,
# and merges into test_player_game_data (updating batting_wpa/bowling_wpa/era).
#
# This bridges the Cricsheet-based WP model with the Cricinfo-based player
# game data by matching players via surname.

library(cli)
library(data.table)
library(xgboost)
devtools::load_all()

# ============================================================================
# Load models
# ============================================================================

cli::cli_h1("Test WPA Batch Pipeline")

models_dir <- file.path(find_bouncerdata_dir(), "models")
v3 <- readRDS(file.path(models_dir, "test_winprob_v3_results.rds"))
result_model <- v3$model_A
cond_model <- v3$model_B
result_features <- v3$result_features
cond_features <- v3$conditional_features

cli::cli_alert_success("Loaded v3 models (A: {length(result_features)} features, B: {length(cond_features)} features)")

# ============================================================================
# Load Test deliveries from Cricsheet
# ============================================================================

cli::cli_h2("Loading Cricsheet Test data")

conn <- get_db_connection(read_only = TRUE)

dt <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT
    d.delivery_id, d.match_id, d.match_date, d.season,
    d.batting_team, d.bowling_team,
    d.batter_id, d.bowler_id,
    d.innings, d.over, d.ball,
    d.runs_total, d.is_wicket,
    d.wickets_fallen,
    m.team1, m.team2,
    m.outcome_type, m.outcome_winner, m.venue
  FROM cricsheet.deliveries d
  JOIN cricsheet.matches m ON d.match_id = m.match_id
  WHERE LOWER(d.match_type) IN ('test', 'mdm')
    AND m.outcome_type IS NOT NULL
  ORDER BY d.match_date, d.match_id, d.innings, d.over, d.ball
"))

# Load innings totals
innings_totals <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT match_id, innings, total_runs AS innings_total,
         total_wickets AS innings_wickets, total_overs AS innings_overs
  FROM cricsheet.match_innings
  WHERE match_id IN (
    SELECT match_id FROM cricsheet.matches WHERE LOWER(match_type) IN ('test', 'mdm')
      AND outcome_type IS NOT NULL
  )
  ORDER BY match_id, innings
"))

# Venue stats, TIME-CAUSAL and per match (#69).
#
# Both of these were averaged over EVERY match at the ground, including the one
# being scored, and with no smoothing at all -- so at a one-match venue
# venue_result_rate simply WAS that match's outcome and venue_avg simply WAS its
# own first-innings total. #29 measured the smoothed version of the same
# construction at 0.684 correlation with the match's own result below five
# matches; unsmoothed is worse, because PRIOR = 10 was the only thing damping it.
#
# Do NOT repair this by subtracting the match's own value -- see the note at the
# top of R/venue_rates.R for why leave-one-out concentrates the leak.
venue_raw <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT m.match_id, m.venue, m.match_date, m.outcome_type,
         MAX(CASE WHEN mi.innings = 1 THEN mi.total_runs END) AS inn1_total
  FROM cricsheet.matches m
  LEFT JOIN cricsheet.match_innings mi ON mi.match_id = m.match_id
  WHERE LOWER(m.match_type) IN ('test', 'mdm')
  GROUP BY 1, 2, 3, 4
"))
venue_raw[, `:=`(match_date = as.Date(match_date),
                 decided    = as.integer(!is.na(outcome_type)),
                 is_result  = as.integer(!is.na(outcome_type) & outcome_type != "draw"))]

vr <- time_causal_venue_result_rate(venue_raw, prior_weight = 10)
va <- time_causal_venue_mean(venue_raw, "inn1_total", prior_weight = 5)
venue_stats <- merge(vr[, .(match_id, venue_result_rate)],
                     va[, .(match_id, venue_avg = venue_mean)], by = "match_id")
cli::cli_alert_info(paste0(
  "Venue features: ", sum(vr$at_prior), " of ", nrow(vr), " matches (",
  round(100 * mean(vr$at_prior), 1), "%) are the first at their ground and take ",
  "the prior (result rate ", round(attr(vr, "prior_rate"), 3),
  ", innings-1 total ", round(attr(va, "prior_value")), ")."))

DBI::dbDisconnect(conn, shutdown = TRUE)

cli::cli_alert_success("Loaded {nrow(dt)} deliveries from {uniqueN(dt$match_id)} matches")

# ============================================================================
# Build match state features per delivery (vectorized)
# ============================================================================

cli::cli_h2("Computing match state features")

MAX_OVERS <- 450

# Cumulative runs and wickets within each innings
dt[, cum_innings_runs := cumsum(runs_total), by = .(match_id, innings)]
dt[, cum_innings_runs_before := cum_innings_runs - runs_total]
dt[, cum_innings_wickets := cumsum(as.integer(is_wicket)), by = .(match_id, innings)]
dt[, cum_innings_wickets_before := cum_innings_wickets - as.integer(is_wicket)]

# Current over (fractional)
dt[, current_over := over + ball / 10]

# Team assignment: team1 bats innings 1 & 3, team2 bats innings 2 & 4
dt[, batting_is_team1 := as.integer(innings %in% c(1, 3))]

# Join innings totals for completed innings
# For each delivery, we need totals of ALL COMPLETED innings before current
innings_wide <- dcast(innings_totals, match_id ~ innings,
                       value.var = c("innings_total", "innings_wickets", "innings_overs"),
                       fill = 0)

dt[innings_wide, on = "match_id", `:=`(
  inn1_total = i.innings_total_1,
  inn2_total = i.innings_total_2,
  inn3_total = i.innings_total_3,
  inn4_total = i.innings_total_4,
  inn1_overs = i.innings_overs_1,
  inn2_overs = i.innings_overs_2,
  inn3_overs = i.innings_overs_3,
  inn4_overs = i.innings_overs_4,
  inn1_wickets = i.innings_wickets_1,
  inn2_wickets = i.innings_wickets_2
)]

# Completed innings info (before current delivery)
dt[, team1_completed := fifelse(innings > 1, inn1_total, 0L) +
                         fifelse(innings > 3, inn3_total, 0L)]
dt[, team2_completed := fifelse(innings > 2, inn2_total, 0L) +
                         fifelse(innings > 4, inn4_total, 0L)]
dt[, completed_overs := fifelse(innings > 1, inn1_overs, 0) +
                          fifelse(innings > 2, inn2_overs, 0) +
                          fifelse(innings > 3, inn3_overs, 0)]
dt[, completed_wickets := fifelse(innings > 1, inn1_wickets, 0L) +
                           fifelse(innings > 2, inn2_wickets, 0L)]

# Current state
dt[, current_score := cum_innings_runs_before]
dt[, wickets := cum_innings_wickets_before]
dt[, wickets_in_hand := 10L - wickets]

# Team1 lead
dt[, team1_lead := fifelse(
  batting_is_team1 == 1L,
  as.double(team1_completed + current_score - team2_completed),
  as.double(team1_completed - (team2_completed + current_score))
)]

# Cumulative match overs
dt[, cum_overs := completed_overs + current_over]
dt[, overs_remaining := pmax(0, MAX_OVERS - cum_overs)]
dt[, match_progress := pmin(1, cum_overs / MAX_OVERS)]
dt[, approx_day := pmin(5L, as.integer(floor(cum_overs / 90) + 1))]

# Rates
dt[, current_run_rate := fifelse(current_over > 0, current_score / current_over, 0)]
dt[, total_wickets_match := completed_wickets + wickets]
dt[, total_runs_match := team1_completed + team2_completed + current_score]
dt[, runs_per_over_match := fifelse(cum_overs > 0, total_runs_match / cum_overs, 3.0)]
dt[, overs_per_wicket_current := fifelse(wickets > 0, current_over / wickets, 30)]

# Projected overs
dt[, current_innings_projected_overs := pmin(150,
  fifelse(wickets > 0, current_over + wickets_in_hand * overs_per_wicket_current, 90))]
dt[, avg_overs_per_innings := fifelse(
  innings > 1, completed_overs / (innings - 1L), 80)]
dt[, remaining_innings_count := 4L - innings]
dt[, projected_total_overs := pmin(600, pmax(50,
  completed_overs + current_innings_projected_overs + remaining_innings_count * avg_overs_per_innings))]
dt[, time_pressure := projected_total_overs / MAX_OVERS]

# Lead features
dt[, abs_lead := abs(team1_lead)]
dt[, lead_per_over_remaining := fifelse(overs_remaining > 0, abs_lead / overs_remaining, abs_lead)]

# Follow-on
dt[, follow_on_possible := fifelse(innings >= 2L & (inn1_total - inn2_total) >= 200L, 1L, 0L)]

# Rain proxies
dt[, overs_per_day := fifelse(approx_day > 0, cum_overs / approx_day, 90)]
dt[, overs_deficit := pmax(0, approx_day * 90 - cum_overs)]
dt[, rain_days_so_far := 0]  # No weather data in batch mode

# 4th innings features
dt[, target := fifelse(innings == 4L, as.double(team1_completed - team2_completed + 1L), 0)]
dt[, runs_needed := fifelse(innings == 4L, pmax(0, target - current_score), 0)]
dt[, req_rate := fifelse(innings == 4L & overs_remaining > 0, runs_needed / overs_remaining, 0)]
dt[, overs_per_wicket := fifelse(innings == 4L & wickets_in_hand > 0, overs_remaining / wickets_in_hand, 0)]

# Venue stats -- keyed on match_id, not venue: what a ground's history looked
# like depends on when you ask (#69).
dt[venue_stats, on = "match_id", `:=`(venue_avg = i.venue_avg,
                                      venue_result_rate = i.venue_result_rate)]
dt[is.na(venue_avg), venue_avg := 340]
dt[is.na(venue_result_rate), venue_result_rate := 0.63]

# Projected innings total and lead
dt[, projected_innings_total := fifelse(current_over > 0, current_score * (90 / current_over), venue_avg)]
dt[, projected_lead := fifelse(
  batting_is_team1 == 1L,
  team1_completed + projected_innings_total - team2_completed - venue_avg,
  as.double(team1_lead)
)]

cli::cli_alert_success("Features computed for {nrow(dt)} deliveries")

# ============================================================================
# Run batch predictions
# ============================================================================

cli::cli_h2("Running v3 model predictions")

# Model A: P(result)
result_data <- dt[, .(
  overs_remaining, match_progress, approx_day = as.double(approx_day),
  time_pressure, projected_total_overs, venue_result_rate,
  total_wickets_match = as.double(total_wickets_match),
  runs_per_over_match, abs_lead, lead_per_over_remaining,
  innings_num = as.double(innings), follow_on_possible = as.double(follow_on_possible),
  overs_per_day, overs_deficit, rain_days_so_far = as.double(rain_days_so_far)
)]

# Ensure columns match model features
for (f in result_features) {
  if (!f %in% names(result_data)) result_data[, (f) := 0]
}
result_mat <- xgboost::xgb.DMatrix(as.matrix(result_data[, result_features, with = FALSE]))
p_result <- predict(result_model, result_mat)

# Model B: P(team1_win | result)
cond_data <- dt[, .(
  team1_lead, projected_lead, projected_innings_total,
  batting_is_team1 = as.double(batting_is_team1),
  wickets_in_hand = as.double(wickets_in_hand),
  overs_remaining, cum_overs, venue_avg,
  innings_num = as.double(innings),
  target, runs_needed, req_rate, overs_per_wicket,
  current_run_rate
)]

for (f in cond_features) {
  if (!f %in% names(cond_data)) cond_data[, (f) := 0]
}
cond_mat <- xgboost::xgb.DMatrix(as.matrix(cond_data[, cond_features, with = FALSE]))
p_team1_given_result <- predict(cond_model, cond_mat)

# Combined probabilities
dt[, p_team1_win := p_result * p_team1_given_result]

cli::cli_alert_success("Predictions complete: P(team1_win) range [{round(min(dt$p_team1_win), 3)}, {round(max(dt$p_team1_win), 3)}]")

# ============================================================================
# Compute per-delivery WPA
# ============================================================================

cli::cli_h2("Computing per-delivery WPA")

# Delta WP: change in P(team1_win) between consecutive deliveries
dt[, next_p_team1 := shift(p_team1_win, n = 1, type = "lead"), by = .(match_id)]
dt[, delta_wp := next_p_team1 - p_team1_win]

# Scale to percentage points (* 100) for consistency with Cricinfo WPA
dt[, delta_wp := delta_wp * 100]

# Batting WPA: positive when batting team's action increases their WP
# If team1 is batting, delta_wp > 0 means team1 WP increased (good for batter)
# If team2 is batting, delta_wp < 0 means team1 WP decreased (good for batter = team2)
dt[, batter_wpa := fifelse(batting_is_team1 == 1L, delta_wp, -delta_wp)]
dt[, bowler_wpa := -batter_wpa]

cat(sprintf("  WPA range: [%.1f, %.1f] percentage points\n", min(dt$delta_wp, na.rm=TRUE), max(dt$delta_wp, na.rm=TRUE)))

# ============================================================================
# Aggregate per player per match
# ============================================================================

cli::cli_h2("Aggregating per player per match")

# Batting aggregation
bat_wpa <- dt[!is.na(batter_wpa), .(
  batting_wpa_cricsheet = sum(batter_wpa, na.rm = TRUE),
  batting_era_cricsheet = sum(runs_total - fifelse(current_over > 0, current_score / current_over / 6, 0), na.rm = TRUE),
  batting_balls = .N
), by = .(match_id, batter_id, match_date)]

# Bowling aggregation
bowl_wpa <- dt[!is.na(bowler_wpa), .(
  bowling_wpa_cricsheet = sum(bowler_wpa, na.rm = TRUE),
  bowling_balls = .N
), by = .(match_id, bowler_id, match_date)]

cli::cli_alert_success("Batting WPA: {nrow(bat_wpa)} player-match rows, Bowling: {nrow(bowl_wpa)}")

# ============================================================================
# Match to Cricinfo player IDs via surname
# ============================================================================

cli::cli_h2("Matching Cricsheet to Cricinfo players")

conn <- get_db_connection(read_only = FALSE)

# Load Cricinfo Test player_game_data
pgd_ci <- as.data.table(DBI::dbGetQuery(conn, "SELECT * FROM test_player_game_data"))
cat(sprintf("  Cricinfo PGD: %d rows\n", nrow(pgd_ci)))

# Get player names from cricsheet.players registry
cs_players <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT player_id, player_name FROM cricsheet.players WHERE player_name IS NOT NULL
"))
bat_wpa[cs_players, on = c(batter_id = "player_id"), cs_name := i.player_name]
bowl_wpa[cs_players, on = c(bowler_id = "player_id"), cs_name := i.player_name]

# Extract surnames for matching
bat_wpa[, surname := sub("^.* ", "", cs_name)]
bowl_wpa[, surname := sub("^.* ", "", cs_name)]

# Match IDs are shared between Cricsheet and Cricinfo!
ci_match_ids <- DBI::dbGetQuery(conn, "
  SELECT match_id FROM cricinfo.matches WHERE UPPER(format) IN ('TEST', 'MDM')
")$match_id

cs_match_ids <- unique(dt$match_id)
shared_ids <- intersect(cs_match_ids, ci_match_ids)
cat(sprintf("  Shared match IDs: %d (Cricsheet: %d, Cricinfo: %d)\n",
    length(shared_ids), length(cs_match_ids), length(ci_match_ids)))

# Build direct mapping (match_id is the same in both systems)
match_map <- data.table(match_id = shared_ids, ci_match_id = shared_ids)

if (nrow(match_map) > 0) {
  # Join match mapping
  bat_wpa[match_map, on = "match_id", ci_match_id := i.ci_match_id]
  bowl_wpa[match_map, on = "match_id", ci_match_id := i.ci_match_id]

  # Match players by surname within matched matches
  # Build match-level player name lookup for efficient matching
  # For each Cricinfo player, find the matching Cricsheet player in the same match
  bat_wpa_shared <- bat_wpa[!is.na(ci_match_id) & !is.na(surname)]
  bowl_wpa_shared <- bowl_wpa[!is.na(ci_match_id) & !is.na(surname)]

  n_updated_bat <- 0L
  n_updated_bowl <- 0L

  for (i in seq_len(nrow(pgd_ci))) {
    ci_mid <- pgd_ci$match_id[i]
    ci_pid <- pgd_ci$player_id[i]
    ci_name <- pgd_ci$player_name[i]
    if (is.na(ci_name) || !ci_mid %in% shared_ids) next

    ci_surname <- ci_name  # Already surname-only from title parse

    # Try exact surname match first
    bat_match <- bat_wpa_shared[ci_match_id == ci_mid & surname == ci_surname]
    # If no match, try matching surname anywhere in the Cricsheet full name
    if (nrow(bat_match) == 0) {
      bat_match <- bat_wpa_shared[ci_match_id == ci_mid &
                                   grepl(ci_surname, cs_name, fixed = TRUE)]
    }

    if (nrow(bat_match) == 1) {
      set(pgd_ci, i, "batting_wpa", bat_match$batting_wpa_cricsheet)
      n_updated_bat <- n_updated_bat + 1L
    }

    # Same for bowling
    bowl_match <- bowl_wpa_shared[ci_match_id == ci_mid & surname == ci_surname]
    if (nrow(bowl_match) == 0) {
      bowl_match <- bowl_wpa_shared[ci_match_id == ci_mid &
                                     grepl(ci_surname, cs_name, fixed = TRUE)]
    }

    if (nrow(bowl_match) == 1) {
      set(pgd_ci, i, "bowling_wpa", bowl_match$bowling_wpa_cricsheet)
      n_updated_bowl <- n_updated_bowl + 1L
    }
  }

  pgd_ci[, total_wpa := batting_wpa + bowling_wpa]

  cat(sprintf("  Updated batting WPA: %d rows, bowling WPA: %d rows\n", n_updated_bat, n_updated_bowl))
  cat(sprintf("  Non-zero WPA: %d/%d (%.1f%%)\n",
      sum(abs(pgd_ci$total_wpa) > 0.01), nrow(pgd_ci),
      sum(abs(pgd_ci$total_wpa) > 0.01) / nrow(pgd_ci) * 100))

  # Store back to DB
  store_player_game_data(conn, pgd_ci, "test")

  # Show top Test WPA players
  cat("\n  Top 10 Test WPA players:\n")
  top <- pgd_ci[order(-abs(total_wpa))][1:10]
  for (j in seq_len(nrow(top))) {
    r <- top[j]
    nm <- if (is.na(r$player_name)) r$player_id else r$player_name
    cat(sprintf("    %2d. %-20s bat_wpa=%6.1f  bowl_wpa=%6.1f  total=%6.1f\n",
        j, nm, r$batting_wpa, r$bowling_wpa, r$total_wpa))
  }
}

DBI::dbDisconnect(conn, shutdown = TRUE)
cli::cli_h1("Test WPA Batch Pipeline Complete")
