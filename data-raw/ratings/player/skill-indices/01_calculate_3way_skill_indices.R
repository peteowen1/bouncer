# 01 Calculate 3-Way Skill Indices ----
#
# This script calculates the unified 3-way skill indices where Batter, Bowler,
# and Venue all participate in skill updates based on delivery outcomes.
#
# REPLACES: 01_calculate_3way_elo.R with additive skill system
#
# Two dimensions:
#   - Run Skill: Deviation from expected runs per ball (e.g., +0.30 runs/ball)
#   - Wicket Skill: Deviation from expected wicket probability (e.g., +2%)
#
# Key advantages over ELO:
#   - Directly interpretable (no conversion factor)
#   - Start at 0 (neutral) instead of arbitrary 1400
#   - Continuous decay toward 0 (Bayesian prior)
#   - Simpler formula: expected = baseline + weighted skill sum
#
# Key features:
#   - Dual venue component (permanent + session that resets each match)
#   - Experience-based alpha decay (like K-factor system)
#   - Uses agnostic model predictions as baseline
#   - Centrality integration for opponent quality adjustment
#
# Prerequisites:
#   - Run 01_calibrate_expected_values.R first to set up calibration data
#   - Agnostic model must be trained to get baseline predictions
#
# Output:
#   - {gender}_{format}_3way_skill tables populated in DuckDB

# 1. Setup ----
library(DBI)
library(data.table)

# Load bouncer package - find root from script location or use working directory
bouncer_root <- tryCatch({
  # When run via Rscript
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- args[grep("^--file=", args)]
  if (length(file_arg) > 0) {
    script_path <- normalizePath(sub("^--file=", "", file_arg[1]))
    # 4 levels up: skill-indices/ -> player/ -> ratings/ -> data-raw/ -> bouncer/
    normalizePath(file.path(dirname(script_path), "..", "..", "..", ".."))
  } else {
    getwd()
  }
}, error = function(e) getwd())

# Set working directory to bouncer root so get_db_path() resolves correctly
setwd(bouncer_root)

devtools::load_all(bouncer_root)

# 2. Configuration ----
FORMAT_GROUPS <- list(
  t20 = c("T20", "IT20"),
  odi = c("ODI", "ODM"),
  test = c("Test", "MDM")
)

GENDER_CATEGORIES <- list(
  mens = "male",
  womens = "female"
)

# Processing options - can be overridden via command line
# Usage: Rscript 01_calculate_3way_skill_indices.R [gender] [format]
# Examples:
#   Rscript 01_calculate_3way_skill_indices.R mens t20
#   Rscript 01_calculate_3way_skill_indices.R womens odi
#   Rscript 01_calculate_3way_skill_indices.R  # uses defaults below

args <- commandArgs(trailingOnly = TRUE)
GENDER_FILTER <- if (length(args) >= 1 && args[1] != "") args[1] else "mens"
FORMAT_FILTER <- if (length(args) >= 2 && args[2] != "") args[2] else "t20"
EVENT_FILTER <- NULL     # NULL = all events, or full event name
BATCH_SIZE <- 10000      # Deliveries per batch insert
MATCH_LIMIT <- NULL      # Set to integer to limit matches (for testing)
FORCE_FULL <- TRUE       # If TRUE, always recalculate everything

cat("\n")
cli::cli_h1("3-Way Skill Index Calculation")
cli::cli_alert_info("Unified Batter + Bowler + Venue Skill Index system")
cli::cli_alert_info("Skills are directly interpretable deviations from expected")
cat("\n")

# Determine formats to process
if (is.null(FORMAT_FILTER)) {
  formats_to_process <- names(FORMAT_GROUPS)
} else {
  formats_to_process <- FORMAT_FILTER
}

# Determine genders to process
if (is.null(GENDER_FILTER)) {
  genders_to_process <- names(GENDER_CATEGORIES)
} else {
  genders_to_process <- GENDER_FILTER
}

cli::cli_alert_info("Genders to process: {paste(toupper(genders_to_process), collapse = ', ')}")
cli::cli_alert_info("Formats to process: {paste(toupper(formats_to_process), collapse = ', ')}")
cat("\n")

# 3. Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = FALSE)
cli::cli_alert_success("Connected to database")

# 4. Process Each Gender and Format ----
for (current_gender in genders_to_process) {

gender_db_value <- GENDER_CATEGORIES[[current_gender]]

cat("\n")
cli::cli_rule("{toupper(current_gender)} CRICKET")
cat("\n")

for (current_format in formats_to_process) {

current_category <- paste0(current_gender, "_", current_format)

cat("\n")
cli::cli_h1("{toupper(current_gender)} {toupper(current_format)} 3-Way Skill Index Calculation")
cat("\n")

format_match_types <- FORMAT_GROUPS[[current_format]]
match_type_filter <- paste(sprintf("'%s'", tolower(format_match_types)), collapse = ", ")

## 4.1 Build Current Parameters ----
cli::cli_h2("Building parameters")
current_params <- build_skill_index_params(current_format, gender_db_value)
current_params$format <- current_category

cli::cli_alert_info("Alpha (run): max={current_params$alpha_run_max}, min={current_params$alpha_run_min}")
cli::cli_alert_info("Decay: {current_params$decay_rate} per delivery")
cli::cli_alert_info("Weights (run): batter={current_params$w_batter_run}, bowler={current_params$w_bowler_run}")

## 4.2 Prepare Table ----
if (FORCE_FULL) {
  create_3way_skill_table(current_category, conn, overwrite = TRUE)
}

## 4.3 Load Calibration Data ----
cli::cli_h2("Loading calibration data")
calibration <- get_calibration_data(current_format, conn)

if (is.null(calibration)) {
  cli::cli_alert_danger("No calibration data found!")
  cli::cli_alert_info("Run 01_calibrate_expected_values.R first")
  stop("Calibration data required")
}

cli::cli_alert_success("Calibration loaded: wicket_rate={round(calibration$wicket_rate * 100, 2)}%, mean_runs={round(calibration$mean_runs, 3)}")

## 4.4 Load Deliveries ----
cli::cli_h2("Loading {toupper(current_gender)} {toupper(current_format)} deliveries")

# Build event filter clause
event_filter_clause <- if (!is.null(EVENT_FILTER)) {
  sprintf("AND m.event_name LIKE '%%%s%%'", EVENT_FILTER)
} else {
  ""
}

if (!is.null(EVENT_FILTER)) {
  cli::cli_alert_info("Event filter: {EVENT_FILTER}")
}

base_query <- sprintf("
  SELECT
    d.delivery_id,
    d.match_id,
    d.match_date,
    d.batter_id,
    d.bowler_id,
    d.venue,
    d.batting_team,
    d.bowling_team,
    d.innings,
    d.over,
    d.runs_batter,
    d.is_wicket,
    d.is_boundary,
    m.event_name,
    m.outcome_type
  FROM cricsheet.deliveries d
  LEFT JOIN cricsheet.matches m ON d.match_id = m.match_id
  WHERE LOWER(d.match_type) IN (%s)
    AND m.gender = '%s'
    AND d.batter_id IS NOT NULL
    AND d.bowler_id IS NOT NULL
    %s
", match_type_filter, gender_db_value, event_filter_clause)

if (!is.null(MATCH_LIMIT)) {
  match_ids <- DBI::dbGetQuery(conn, sprintf("
    SELECT DISTINCT d.match_id
    FROM cricsheet.deliveries d
    JOIN cricsheet.matches m ON d.match_id = m.match_id
    WHERE LOWER(d.match_type) IN (%s)
      AND m.gender = '%s'
    ORDER BY d.match_date
    LIMIT %d
  ", match_type_filter, gender_db_value, MATCH_LIMIT))$match_id

  placeholders <- paste(rep("?", length(match_ids)), collapse = ", ")
  query <- paste0(base_query, sprintf(" AND d.match_id IN (%s)", placeholders))
  query <- paste0(query, " ORDER BY d.match_date, d.match_id, d.delivery_id")
  deliveries <- DBI::dbGetQuery(conn, query, params = as.list(match_ids))
} else {
  query <- paste0(base_query, " ORDER BY d.match_date, d.match_id, d.delivery_id")
  deliveries <- DBI::dbGetQuery(conn, query)
}

setDT(deliveries)

n_deliveries <- nrow(deliveries)
n_matches <- uniqueN(deliveries$match_id)

cli::cli_alert_success("Loaded {format(n_deliveries, big.mark = ',')} deliveries from {n_matches} matches")

if (n_deliveries == 0) {
  cli::cli_alert_success("No deliveries to process")
  next
}

## 4.5 Get Last Match Dates for Inactivity Calculation ----
cli::cli_h2("Calculating player inactivity")

# Get all player last match dates before each delivery
player_last_match <- DBI::dbGetQuery(conn, sprintf("
  SELECT
    batter_id as player_id,
    match_id,
    MAX(match_date) as last_match_date
  FROM cricsheet.deliveries
  WHERE LOWER(match_type) IN (%s)
  GROUP BY batter_id, match_id

  UNION ALL

  SELECT
    bowler_id as player_id,
    match_id,
    MAX(match_date) as last_match_date
  FROM cricsheet.deliveries
  WHERE LOWER(match_type) IN (%s)
  GROUP BY bowler_id, match_id
", match_type_filter, match_type_filter))
setDT(player_last_match)

# Build lookup: for each player, what was their previous match?
player_match_history <- player_last_match[order(player_id, last_match_date)]
player_match_history[, prev_match_date := shift(last_match_date, 1), by = player_id]

# Create lookup environment
batter_prev_match <- new.env(hash = TRUE)
bowler_prev_match <- new.env(hash = TRUE)

for (i in seq_len(nrow(player_match_history))) {
  player <- player_match_history$player_id[i]
  match <- player_match_history$match_id[i]
  prev_date <- player_match_history$prev_match_date[i]

  key <- paste0(player, ":", match)
  batter_prev_match[[key]] <- prev_date
  bowler_prev_match[[key]] <- prev_date
}

cli::cli_alert_success("Built inactivity lookup for {uniqueN(player_match_history$player_id)} players")

## 4.6 Initialize Entity State ----
cli::cli_h2("Initializing entity state")

# Get all unique entities
all_batters <- unique(deliveries$batter_id)
all_bowlers <- unique(deliveries$bowler_id)
all_venues <- unique(deliveries$venue)
all_players <- unique(c(all_batters, all_bowlers))

cli::cli_alert_info("{length(all_batters)} batters, {length(all_bowlers)} bowlers, {length(all_venues)} venues")

# Initialize player state environments - START AT 0 (neutral skill)
batter_run_skill <- new.env(hash = TRUE)
batter_wicket_skill <- new.env(hash = TRUE)
batter_balls_count <- new.env(hash = TRUE)

bowler_run_skill <- new.env(hash = TRUE)
bowler_wicket_skill <- new.env(hash = TRUE)
bowler_balls_count <- new.env(hash = TRUE)

venue_perm_run_skill <- new.env(hash = TRUE)
venue_perm_wicket_skill <- new.env(hash = TRUE)
venue_balls_count <- new.env(hash = TRUE)

# Initialize all entities at 0 (neutral skill)
for (player in all_batters) {
  batter_run_skill[[player]] <- SKILL_INDEX_START
  batter_wicket_skill[[player]] <- SKILL_INDEX_START
  batter_balls_count[[player]] <- 0L
}

for (player in all_bowlers) {
  bowler_run_skill[[player]] <- SKILL_INDEX_START
  bowler_wicket_skill[[player]] <- SKILL_INDEX_START
  bowler_balls_count[[player]] <- 0L
}

for (venue in all_venues) {
  venue_perm_run_skill[[venue]] <- SKILL_INDEX_START
  venue_perm_wicket_skill[[venue]] <- SKILL_INDEX_START
  venue_balls_count[[venue]] <- 0L
}

cli::cli_alert_success("Initialized all entities at skill = 0 (neutral)")

## 4.7 Pre-extract Columns ----
delivery_ids <- deliveries$delivery_id
match_ids <- deliveries$match_id
match_dates <- deliveries$match_date
batter_ids <- deliveries$batter_id
bowler_ids <- deliveries$bowler_id
venues <- deliveries$venue
innings_vec <- deliveries$innings
over_vec <- deliveries$over
runs_vec <- deliveries$runs_batter
wicket_vec <- deliveries$is_wicket
boundary_vec <- deliveries$is_boundary
event_names <- deliveries$event_name
outcome_types <- deliveries$outcome_type

## 4.7.1 Build League Running Averages ----
cli::cli_h3("Building league running averages")
league_running_avgs <- build_league_running_averages(conn, current_format, gender_db_value)
if (nrow(league_running_avgs) > 0) {
  n_leagues <- uniqueN(league_running_avgs$event_name)
  cli::cli_alert_success("Built running averages for {n_leagues} leagues")
} else {
  cli::cli_alert_warning("No league data available - using global baseline only")
}

# Variable to track current match's league baseline
current_match_baseline <- calibration$mean_runs

## 4.7.2 Pre-index Match Boundaries ----
unique_matches <- unique(match_ids)
match_start_idx <- match(unique_matches, match_ids)
match_end_idx <- c(match_start_idx[-1] - 1L, n_deliveries)
names(match_start_idx) <- unique_matches
names(match_end_idx) <- unique_matches

## 4.8 Pre-allocate Result Matrix ----
cli::cli_h2("Processing deliveries")

# Result matrix columns
result_cols <- c(
  # Player Run Skills
  "batter_run_before", "batter_run_after",
  "bowler_run_before", "bowler_run_after",
  # Player Wicket Skills
  "batter_wicket_before", "batter_wicket_after",
  "bowler_wicket_before", "bowler_wicket_after",
  # Venue Permanent Skills
  "venue_perm_run_before", "venue_perm_run_after",
  "venue_perm_wicket_before", "venue_perm_wicket_after",
  # Venue Session Skills
  "venue_session_run_before", "venue_session_run_after",
  "venue_session_wicket_before", "venue_session_wicket_after",
  # Learning rates (alpha)
  "alpha_batter_run", "alpha_bowler_run", "alpha_venue_perm_run", "alpha_venue_session_run",
  "alpha_batter_wicket", "alpha_bowler_wicket", "alpha_venue_perm_wicket", "alpha_venue_session_wicket",
  # Predictions/actuals
  "exp_runs", "exp_wicket", "actual_runs", "is_wicket_num",
  # Context
  "batter_balls", "bowler_balls", "venue_balls", "balls_in_match",
  "days_inactive_batter", "days_inactive_bowler",
  "is_knockout_num", "event_tier", "phase_num"
)

result_mat <- matrix(NA_real_, nrow = n_deliveries, ncol = length(result_cols))
colnames(result_mat) <- result_cols

# Track current match for session skill reset
current_match_id <- ""
venue_session_run_skill <- SKILL_INDEX_START
venue_session_wicket_skill <- SKILL_INDEX_START
balls_in_current_match <- 0L
matches_processed <- 0L

# Progress tracking
cli::cli_progress_bar("Calculating 3-Way Skills", total = n_deliveries)

for (i in seq_len(n_deliveries)) {
  # Get delivery info
  batter_id <- batter_ids[i]
  bowler_id <- bowler_ids[i]
  venue <- venues[i]
  match_id <- match_ids[i]
  match_date <- match_dates[i]
  innings <- innings_vec[i]
  over <- over_vec[i]
  runs <- runs_vec[i]
  is_wicket <- wicket_vec[i]
  is_boundary <- if (!is.na(boundary_vec[i])) boundary_vec[i] else FALSE
  event_name <- event_names[i]
  outcome_type <- outcome_types[i]

  # Check if new match - reset session skill
  if (match_id != current_match_id) {
    current_match_id <- match_id
    venue_session_run_skill <- reset_venue_session_skill()
    venue_session_wicket_skill <- reset_venue_session_skill()
    balls_in_current_match <- 0L
    matches_processed <- matches_processed + 1L

    # Calculate league-adjusted baseline for this match
    if (nrow(league_running_avgs) > 0) {
      current_match_baseline <- get_league_baseline_as_of(
        event_name = event_name,
        match_date = match_date,
        league_lookup = league_running_avgs,
        global_avg_runs = calibration$mean_runs
      )
    } else {
      current_match_baseline <- calibration$mean_runs
    }

    # Log progress every 500 matches
    if (matches_processed %% 500 == 0) {
      batter_skills <- unlist(mget(ls(batter_run_skill), envir = batter_run_skill))
      cli::cli_alert_info("Match {matches_processed}: Mean batter run skill = {round(mean(batter_skills), 3)}")
    }
  }

  # Get current skills (before)
  batter_run_before <- batter_run_skill[[batter_id]]
  batter_wicket_before <- batter_wicket_skill[[batter_id]]
  bowler_run_before <- bowler_run_skill[[bowler_id]]
  bowler_wicket_before <- bowler_wicket_skill[[bowler_id]]
  venue_perm_run_before <- venue_perm_run_skill[[venue]]
  venue_perm_wicket_before <- venue_perm_wicket_skill[[venue]]
  venue_session_run_before <- venue_session_run_skill
  venue_session_wicket_before <- venue_session_wicket_skill

  # Get ball counts
  batter_balls <- batter_balls_count[[batter_id]]
  bowler_balls <- bowler_balls_count[[bowler_id]]
  venue_total_balls <- venue_balls_count[[venue]]

  # Calculate days inactive
  batter_key <- paste0(batter_id, ":", match_id)
  bowler_key <- paste0(bowler_id, ":", match_id)
  batter_prev_date <- batter_prev_match[[batter_key]]
  bowler_prev_date <- bowler_prev_match[[bowler_key]]

  days_inactive_batter <- if (!is.null(batter_prev_date) && !is.na(batter_prev_date)) {
    as.integer(as.Date(match_date) - as.Date(batter_prev_date))
  } else {
    0L
  }

  days_inactive_bowler <- if (!is.null(bowler_prev_date) && !is.na(bowler_prev_date)) {
    as.integer(as.Date(match_date) - as.Date(bowler_prev_date))
  } else {
    0L
  }

  # Determine context
  phase <- get_match_phase(over, current_format)
  is_knockout <- !is.null(outcome_type) && outcome_type %in% c("knockout", "final", "semifinal")
  event_tier_val <- get_event_tier(event_name)

  # Calculate expected values using skill-adjusted formula
  exp_runs <- calculate_expected_runs_skill(
    current_match_baseline,  # agnostic baseline
    batter_run_before,
    bowler_run_before,
    venue_perm_run_before,
    venue_session_run_before,
    current_format,
    gender_db_value
  )

  exp_wicket <- calculate_expected_wicket_skill(
    calibration$wicket_rate,  # agnostic baseline
    batter_wicket_before,
    bowler_wicket_before,
    venue_perm_wicket_before,
    venue_session_wicket_before,
    current_format,
    gender_db_value
  )

  # Actual outcomes
  actual_run_score <- runs
  actual_wicket_val <- if (is_wicket) 1 else 0

  # Get learning rates (alpha) - decay with experience
  alpha_batter_run <- get_skill_alpha(batter_balls, current_format, "run", gender_db_value)
  alpha_bowler_run <- get_skill_alpha(bowler_balls, current_format, "run", gender_db_value)
  alpha_venue_perm_run <- get_venue_perm_skill_alpha(venue_total_balls, current_format)
  alpha_venue_session_run <- get_venue_session_skill_alpha(balls_in_current_match)

  alpha_batter_wicket <- get_skill_alpha(batter_balls, current_format, "wicket", gender_db_value)
  alpha_bowler_wicket <- get_skill_alpha(bowler_balls, current_format, "wicket", gender_db_value)
  alpha_venue_perm_wicket <- get_venue_perm_skill_alpha(venue_total_balls, current_format)
  alpha_venue_session_wicket <- get_venue_session_skill_alpha(balls_in_current_match)

  # Calculate skill updates (includes decay and bounds)
  run_updates <- update_run_skills(
    actual_run_score, exp_runs,
    alpha_batter_run, alpha_bowler_run, alpha_venue_perm_run, alpha_venue_session_run,
    batter_run_before, bowler_run_before, venue_perm_run_before, venue_session_run_before,
    current_format, gender_db_value
  )

  wicket_updates <- update_wicket_skills(
    actual_wicket_val, exp_wicket,
    alpha_batter_wicket, alpha_bowler_wicket, alpha_venue_perm_wicket, alpha_venue_session_wicket,
    batter_wicket_before, bowler_wicket_before, venue_perm_wicket_before, venue_session_wicket_before,
    current_format, gender_db_value
  )

  # Extract updated values
  batter_run_after <- run_updates$new_batter
  bowler_run_after <- run_updates$new_bowler
  venue_perm_run_after <- run_updates$new_venue_perm
  venue_session_run_after <- run_updates$new_venue_session

  batter_wicket_after <- wicket_updates$new_batter
  bowler_wicket_after <- wicket_updates$new_bowler
  venue_perm_wicket_after <- wicket_updates$new_venue_perm
  venue_session_wicket_after <- wicket_updates$new_venue_session

  # Update state
  batter_run_skill[[batter_id]] <- batter_run_after
  batter_wicket_skill[[batter_id]] <- batter_wicket_after
  bowler_run_skill[[bowler_id]] <- bowler_run_after
  bowler_wicket_skill[[bowler_id]] <- bowler_wicket_after
  venue_perm_run_skill[[venue]] <- venue_perm_run_after
  venue_perm_wicket_skill[[venue]] <- venue_perm_wicket_after
  venue_session_run_skill <- venue_session_run_after
  venue_session_wicket_skill <- venue_session_wicket_after

  # Update ball counts
  batter_balls_count[[batter_id]] <- batter_balls + 1L
  bowler_balls_count[[bowler_id]] <- bowler_balls + 1L
  venue_balls_count[[venue]] <- venue_total_balls + 1L
  balls_in_current_match <- balls_in_current_match + 1L

  # Store results
  phase_num <- switch(phase, "powerplay" = 1, "middle" = 2, "death" = 3, 2)

  result_mat[i, ] <- c(
    # Player Run Skills
    batter_run_before, batter_run_after,
    bowler_run_before, bowler_run_after,
    # Player Wicket Skills
    batter_wicket_before, batter_wicket_after,
    bowler_wicket_before, bowler_wicket_after,
    # Venue Permanent Skills
    venue_perm_run_before, venue_perm_run_after,
    venue_perm_wicket_before, venue_perm_wicket_after,
    # Venue Session Skills
    venue_session_run_before, venue_session_run_after,
    venue_session_wicket_before, venue_session_wicket_after,
    # Learning rates
    alpha_batter_run, alpha_bowler_run, alpha_venue_perm_run, alpha_venue_session_run,
    alpha_batter_wicket, alpha_bowler_wicket, alpha_venue_perm_wicket, alpha_venue_session_wicket,
    # Predictions/actuals
    exp_runs, exp_wicket, runs, actual_wicket_val,
    # Context
    batter_balls + 1L, bowler_balls + 1L, venue_total_balls + 1L, balls_in_current_match,
    days_inactive_batter, days_inactive_bowler,
    as.integer(is_knockout), event_tier_val, phase_num
  )

  if (i %% 10000 == 0) {
    cli::cli_progress_update(set = i)
  }

  # MEMORY CLEANUP: Periodic garbage collection
  if (i %% 100000 == 0) {
    gc(verbose = FALSE)
  }
}

cli::cli_progress_done()

## 4.9 Summary of Final Skills ----
cli::cli_h2("Skill Distribution Summary")

# Get all final skills
batter_run_skills <- unlist(mget(ls(batter_run_skill), envir = batter_run_skill))
bowler_run_skills <- unlist(mget(ls(bowler_run_skill), envir = bowler_run_skill))
venue_perm_run_skills <- unlist(mget(ls(venue_perm_run_skill), envir = venue_perm_run_skill))

cli::cli_alert_info("Batter Run Skills: mean={round(mean(batter_run_skills), 3)}, sd={round(sd(batter_run_skills), 3)}")
cli::cli_alert_info("Bowler Run Skills: mean={round(mean(bowler_run_skills), 3)}, sd={round(sd(bowler_run_skills), 3)}")
cli::cli_alert_info("Venue Perm Run Skills: mean={round(mean(venue_perm_run_skills), 3)}, sd={round(sd(venue_perm_run_skills), 3)}")

## 4.10 Build Result Data.Table ----
cli::cli_h2("Building output data")

# Convert phase numbers back to strings
phase_map <- c("powerplay", "middle", "death")

result_dt <- data.table(
  delivery_id = delivery_ids,
  match_id = match_ids,
  match_date = match_dates,
  batter_id = batter_ids,
  bowler_id = bowler_ids,
  venue = venues,

  batter_run_skill_before = result_mat[, "batter_run_before"],
  batter_run_skill_after = result_mat[, "batter_run_after"],
  bowler_run_skill_before = result_mat[, "bowler_run_before"],
  bowler_run_skill_after = result_mat[, "bowler_run_after"],

  batter_wicket_skill_before = result_mat[, "batter_wicket_before"],
  batter_wicket_skill_after = result_mat[, "batter_wicket_after"],
  bowler_wicket_skill_before = result_mat[, "bowler_wicket_before"],
  bowler_wicket_skill_after = result_mat[, "bowler_wicket_after"],

  venue_perm_run_skill_before = result_mat[, "venue_perm_run_before"],
  venue_perm_run_skill_after = result_mat[, "venue_perm_run_after"],
  venue_perm_wicket_skill_before = result_mat[, "venue_perm_wicket_before"],
  venue_perm_wicket_skill_after = result_mat[, "venue_perm_wicket_after"],

  venue_session_run_skill_before = result_mat[, "venue_session_run_before"],
  venue_session_run_skill_after = result_mat[, "venue_session_run_after"],
  venue_session_wicket_skill_before = result_mat[, "venue_session_wicket_before"],
  venue_session_wicket_skill_after = result_mat[, "venue_session_wicket_after"],

  alpha_batter_run = result_mat[, "alpha_batter_run"],
  alpha_bowler_run = result_mat[, "alpha_bowler_run"],
  alpha_venue_perm_run = result_mat[, "alpha_venue_perm_run"],
  alpha_venue_session_run = result_mat[, "alpha_venue_session_run"],
  alpha_batter_wicket = result_mat[, "alpha_batter_wicket"],
  alpha_bowler_wicket = result_mat[, "alpha_bowler_wicket"],
  alpha_venue_perm_wicket = result_mat[, "alpha_venue_perm_wicket"],
  alpha_venue_session_wicket = result_mat[, "alpha_venue_session_wicket"],

  exp_runs = result_mat[, "exp_runs"],
  exp_wicket = result_mat[, "exp_wicket"],
  actual_runs = as.integer(result_mat[, "actual_runs"]),
  is_wicket = as.logical(result_mat[, "is_wicket_num"]),

  batter_balls = as.integer(result_mat[, "batter_balls"]),
  bowler_balls = as.integer(result_mat[, "bowler_balls"]),
  venue_balls = as.integer(result_mat[, "venue_balls"]),
  balls_in_match = as.integer(result_mat[, "balls_in_match"]),
  days_inactive_batter = as.integer(result_mat[, "days_inactive_batter"]),
  days_inactive_bowler = as.integer(result_mat[, "days_inactive_bowler"]),

  is_knockout = as.logical(result_mat[, "is_knockout_num"]),
  event_tier = as.integer(result_mat[, "event_tier"]),
  phase = phase_map[as.integer(result_mat[, "phase_num"])]
)

## 4.11 Batch Insert ----
cli::cli_h2("Inserting to database")

n_batches <- ceiling(n_deliveries / BATCH_SIZE)
cli::cli_progress_bar("Inserting batches", total = n_batches)

for (b in seq_len(n_batches)) {
  start_idx <- (b - 1) * BATCH_SIZE + 1
  end_idx <- min(b * BATCH_SIZE, n_deliveries)

  batch_df <- as.data.frame(result_dt[start_idx:end_idx])
  insert_3way_skills(batch_df, current_category, conn)

  cli::cli_progress_update()
}

cli::cli_progress_done()

cli::cli_alert_success("Inserted {format(n_deliveries, big.mark = ',')} deliveries")

## 4.12 Store Parameters ----
cli::cli_h2("Storing parameters")

last_row <- result_dt[.N]
store_3way_skill_params(
  current_params,
  last_delivery_id = last_row$delivery_id,
  last_match_date = last_row$match_date,
  total_deliveries = n_deliveries,
  conn = conn
)

cli::cli_alert_success("Parameters stored")

## 4.13 Summary Statistics ----
cat("\n")
cli::cli_h2("{toupper(current_gender)} {toupper(current_format)} 3-Way Skill Index Summary")

tryCatch({
  stats <- get_3way_skill_stats(current_category, conn)

  if (!is.null(stats)) {
    cli::cli_alert_info("Total records: {format(stats$total_records, big.mark = ',')}")
    cli::cli_alert_info("Unique batters: {stats$unique_batters}")
    cli::cli_alert_info("Unique bowlers: {stats$unique_bowlers}")
    cli::cli_alert_info("Unique venues: {stats$unique_venues}")
    cli::cli_alert_info("Date range: {stats$first_date} to {stats$last_date}")

    cat("\nMean Skill Indices (centered at 0):\n")
    cat(sprintf("  Batter Run Skill:        %+.3f (sd: %.3f)\n",
                stats$mean_batter_run_skill, stats$sd_batter_run_skill))
    cat(sprintf("  Bowler Run Skill:        %+.3f (sd: %.3f)\n",
                stats$mean_bowler_run_skill, stats$sd_bowler_run_skill))
    cat(sprintf("  Batter Wicket Skill:     %+.4f\n", stats$mean_batter_wicket_skill))
    cat(sprintf("  Bowler Wicket Skill:     %+.4f\n", stats$mean_bowler_wicket_skill))
    cat(sprintf("  Venue Perm Run Skill:    %+.3f\n", stats$mean_venue_perm_run_skill))
    cat(sprintf("  Venue Session Run Skill: %+.3f\n", stats$mean_venue_session_run_skill))

    cat("\nCalibration Check:\n")
    cat(sprintf("  Mean expected runs:  %.3f\n", stats$mean_exp_runs))
    cat(sprintf("  Mean actual runs:    %.3f\n", stats$mean_actual_runs))
    cat(sprintf("  Actual wicket rate:  %.2f%%\n", stats$actual_wicket_rate * 100))
  }
}, error = function(e) {
  cli::cli_alert_warning("Could not retrieve summary stats: {e$message}")
})

cat("\n")
cli::cli_alert_success("{toupper(current_gender)} {toupper(current_format)} 3-Way Skill Index calculation complete!")
cat("\n")

# MEMORY CLEANUP: Force garbage collection between formats
gc(verbose = FALSE)

}  # End format loop

# MEMORY CLEANUP: Force garbage collection between genders
gc(verbose = FALSE)

}  # End gender loop

# 5. Final Summary ----
cat("\n")
cli::cli_h1("All Categories Complete")
cli::cli_alert_success("Processed genders: {paste(toupper(genders_to_process), collapse = ', ')}")
cli::cli_alert_success("Processed formats: {paste(toupper(formats_to_process), collapse = ', ')}")
cat("\n")

cli::cli_h3("Key Differences from ELO")
cli::cli_bullets(c(
  "i" = "Skills start at 0 (neutral) instead of 1400",
  "i" = "Skills are directly interpretable: +0.30 = 0.30 runs/ball above expected",
  "i" = "Continuous decay toward 0 provides Bayesian prior",
  "i" = "Bounds: ±0.50 for run skills, ±0.05 for wicket skills"
))
cat("\n")

cli::cli_h3("Query Example")
cat("
-- Get 3-way skill features for a delivery
SELECT
  d.*,
  s.batter_run_skill_before,
  s.bowler_run_skill_before,
  s.venue_perm_run_skill_before,
  s.venue_session_run_skill_before,
  s.exp_runs,
  s.exp_wicket
FROM cricsheet.deliveries d
JOIN mens_t20_3way_skill s ON d.delivery_id = s.delivery_id
LIMIT 10
")
cat("\n")

# Cleanup ----
if (exists("conn") && !is.null(conn)) {
  tryCatch({
    DBI::dbDisconnect(conn, shutdown = TRUE)
  }, error = function(e) NULL)
}
