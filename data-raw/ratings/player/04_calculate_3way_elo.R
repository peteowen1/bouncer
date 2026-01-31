# 04 Calculate 3-Way ELOs ----
#
# This script calculates the unified 3-way ELO ratings where Batter, Bowler,
# and Venue all participate in rating updates based on delivery outcomes.
#
# Two dimensions:
#   - Run ELO: Expected runs vs actual runs (continuous scale 0-1)
#   - Wicket ELO: Expected wicket probability vs actual wicket (binary 0/1)
#
# Key features:
#   - Dual venue component (permanent + session that resets each match)
#   - Dynamic K-factors with inactivity decay
#   - Situational modifiers (knockout, tier, phase, high chase)
#   - Post-match normalization
#
# Prerequisites:
#   - Run 01_calibrate_expected_values.R first to set up calibration data
#   - Agnostic model must be trained to get baseline predictions
#
# Output:
#   - {format}_3way_elo tables populated in DuckDB

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
    normalizePath(file.path(dirname(script_path), "..", "..", ".."))
  } else {
    getwd()
  }
}, error = function(e) getwd())

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

FORMAT_FILTER <- NULL    # NULL = all formats, or "t20", "odi", "test"
GENDER_FILTER <- NULL    # NULL = all genders, or "mens", "womens"
EVENT_FILTER <- NULL     # NULL = all events, or full event name
BATCH_SIZE <- 10000      # Deliveries per batch insert
MATCH_LIMIT <- NULL      # Set to integer to limit matches (for testing)
FORCE_FULL <- TRUE       # If TRUE, always recalculate everything
LOAD_FROM_CHECKPOINT <- FALSE  # Set TRUE to load from checkpoint file and skip calculation

cat("\n")
cli::cli_h1("3-Way ELO Calculation")
cli::cli_alert_info("Unified Batter + Bowler + Venue ELO system")
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
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
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
cli::cli_h1("{toupper(current_gender)} {toupper(current_format)} 3-Way ELO Calculation")
cat("\n")

# Check for checkpoint file
checkpoint_dir <- file.path(find_bouncerdata_dir(), "checkpoints")
checkpoint_file <- file.path(checkpoint_dir, paste0(current_category, "_3way_elo_checkpoint.qs"))

if (LOAD_FROM_CHECKPOINT && file.exists(checkpoint_file)) {
  cli::cli_alert_info("Loading from checkpoint: {checkpoint_file}")
  checkpoint_data <- readRDS(checkpoint_file)

  # Restore variables from checkpoint
  result_mat <- checkpoint_data$result_mat
  delivery_ids <- checkpoint_data$delivery_ids
  match_ids <- checkpoint_data$match_ids
  match_dates <- checkpoint_data$match_dates
  batter_ids <- checkpoint_data$batter_ids
  bowler_ids <- checkpoint_data$bowler_ids
  venues <- checkpoint_data$venues
  n_deliveries <- checkpoint_data$n_deliveries
  current_format <- checkpoint_data$current_format
  current_params <- build_3way_elo_params(current_format)
  current_params$format <- current_category

  cli::cli_alert_success("Checkpoint loaded: {format(n_deliveries, big.mark = ',')} deliveries")
  cli::cli_alert_info("Skipping to database insert...")

  # Prepare table
  create_3way_elo_table(current_format, conn, overwrite = TRUE)

  # Skip directly to section 4.10 (Build Result Data.Table)
  # The code below will be skipped via the flag
  skip_to_insert <- TRUE
} else {
  skip_to_insert <- FALSE
}

if (!skip_to_insert) {
format_match_types <- FORMAT_GROUPS[[current_format]]
match_type_filter <- paste(sprintf("'%s'", tolower(format_match_types)), collapse = ", ")

## 4.1 Build Current Parameters ----
cli::cli_h2("Building parameters")
current_params <- build_3way_elo_params(current_format)
current_params$format <- current_category

## 4.2 Prepare Table ----
if (FORCE_FULL) {
  create_3way_elo_table(current_format, conn, overwrite = TRUE)
}

## 4.3 Load Calibration Data ----
cli::cli_h2("Loading calibration data")
calibration <- get_calibration_data(current_format, conn)

if (is.null(calibration)) {
  cli::cli_alert_danger("No calibration data found!")
  cli::cli_alert_info("Run 01_calibrate_expected_values.R first")
  stop("Calibration data required")
}

# Override calibration for specific events
if (!is.null(EVENT_FILTER) && grepl("Indian Premier League", EVENT_FILTER)) {
  calibration$mean_runs <- 1.2774  # IPL-specific mean (higher scoring)
  cli::cli_alert_info("Using IPL-specific mean_runs: {calibration$mean_runs}")
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
  FROM deliveries d
  LEFT JOIN matches m ON d.match_id = m.match_id
  WHERE LOWER(d.match_type) IN (%s)
    AND m.gender = '%s'
    AND d.batter_id IS NOT NULL
    AND d.bowler_id IS NOT NULL
    %s
", match_type_filter, gender_db_value, event_filter_clause)

if (!is.null(MATCH_LIMIT)) {
  match_ids <- DBI::dbGetQuery(conn, sprintf("
    SELECT DISTINCT d.match_id
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
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
  FROM deliveries
  WHERE LOWER(match_type) IN (%s)
  GROUP BY batter_id, match_id

  UNION ALL

  SELECT
    bowler_id as player_id,
    match_id,
    MAX(match_date) as last_match_date
  FROM deliveries
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
  # Store for both batter and bowler (they share same player pool)
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

# Initialize player state environments
# Batters: run ELO (scoring) and wicket ELO (survival)
batter_run_elo <- new.env(hash = TRUE)
batter_wicket_elo <- new.env(hash = TRUE)
batter_balls_count <- new.env(hash = TRUE)

# Bowlers: run ELO (economy) and wicket ELO (strike rate)
bowler_run_elo <- new.env(hash = TRUE)
bowler_wicket_elo <- new.env(hash = TRUE)
bowler_balls_count <- new.env(hash = TRUE)

# Venue permanent ELOs (persist across matches)
venue_perm_run_elo <- new.env(hash = TRUE)
venue_perm_wicket_elo <- new.env(hash = TRUE)
venue_balls_count <- new.env(hash = TRUE)

# Initialize all players at starting ELO
for (player in all_batters) {
  batter_run_elo[[player]] <- THREE_WAY_ELO_START
  batter_wicket_elo[[player]] <- THREE_WAY_ELO_START
  batter_balls_count[[player]] <- 0L
}

for (player in all_bowlers) {
  bowler_run_elo[[player]] <- THREE_WAY_ELO_START
  bowler_wicket_elo[[player]] <- THREE_WAY_ELO_START
  bowler_balls_count[[player]] <- 0L
}

for (venue in all_venues) {
  venue_perm_run_elo[[venue]] <- THREE_WAY_ELO_START
  venue_perm_wicket_elo[[venue]] <- THREE_WAY_ELO_START
  venue_balls_count[[venue]] <- 0L
}

cli::cli_alert_success("Initialized all entities at ELO {THREE_WAY_ELO_START}")

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

## 4.8 Pre-allocate Result Matrix ----
cli::cli_h2("Processing deliveries")

# Result matrix columns
result_cols <- c(
  # Player Run ELOs
  "batter_run_before", "batter_run_after",
  "bowler_run_before", "bowler_run_after",
  # Player Wicket ELOs
  "batter_wicket_before", "batter_wicket_after",
  "bowler_wicket_before", "bowler_wicket_after",
  # Venue Permanent ELOs
  "venue_perm_run_before", "venue_perm_run_after",
  "venue_perm_wicket_before", "venue_perm_wicket_after",
  # Venue Session ELOs
  "venue_session_run_before", "venue_session_run_after",
  "venue_session_wicket_before", "venue_session_wicket_after",
  # K-factors
  "k_batter_run", "k_bowler_run", "k_venue_perm_run", "k_venue_session_run",
  "k_batter_wicket", "k_bowler_wicket", "k_venue_perm_wicket", "k_venue_session_wicket",
  # Predictions/actuals
  "exp_runs", "exp_wicket", "actual_runs", "is_wicket_num",
  # Context
  "batter_balls", "bowler_balls", "venue_balls", "balls_in_match",
  "days_inactive_batter", "days_inactive_bowler",
  "is_knockout_num", "event_tier", "phase_num"
)

result_mat <- matrix(NA_real_, nrow = n_deliveries, ncol = length(result_cols))
colnames(result_mat) <- result_cols

# Track current match for session ELO reset
current_match_id <- ""
venue_session_run_elo <- THREE_WAY_ELO_START
venue_session_wicket_elo <- THREE_WAY_ELO_START
balls_in_current_match <- 0L
matches_processed <- 0L

# Helper function for per-match normalization
normalize_elos_in_env <- function(env, target_mean = THREE_WAY_ELO_TARGET_MEAN) {
  elos <- unlist(mget(ls(env), envir = env))
  if (length(elos) == 0) return()

  drift <- mean(elos, na.rm = TRUE) - target_mean
  if (abs(drift) > THREE_WAY_NORMALIZATION_MIN_DRIFT) {
    adjustment <- -drift * THREE_WAY_NORMALIZATION_CORRECTION_RATE
    for (key in ls(env)) {
      env[[key]] <- env[[key]] + adjustment
    }
  }
}

# Progress tracking
cli::cli_progress_bar("Calculating 3-Way ELOs", total = n_deliveries)

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

  # Check if new match - apply post-match normalization and reset session ELO
  if (match_id != current_match_id) {
    # Apply per-match normalization (except for first match)
    if (current_match_id != "" && THREE_WAY_NORMALIZE_AFTER_MATCH) {
      # Normalize each entity type separately
      normalize_elos_in_env(batter_run_elo)
      normalize_elos_in_env(batter_wicket_elo)
      normalize_elos_in_env(bowler_run_elo)
      normalize_elos_in_env(bowler_wicket_elo)
      normalize_elos_in_env(venue_perm_run_elo)
      normalize_elos_in_env(venue_perm_wicket_elo)
    }

    current_match_id <- match_id
    venue_session_run_elo <- reset_venue_session_elo()
    venue_session_wicket_elo <- reset_venue_session_elo()
    balls_in_current_match <- 0L
    matches_processed <- matches_processed + 1L

    # Log progress every 500 matches
    if (matches_processed %% 500 == 0) {
      batter_elos <- unlist(mget(ls(batter_run_elo), envir = batter_run_elo))
      cli::cli_alert_info("Match {matches_processed}: Mean batter run ELO = {round(mean(batter_elos), 1)}")
    }
  }

  # Get current ELOs (before)
  batter_run_before <- batter_run_elo[[batter_id]]
  batter_wicket_before <- batter_wicket_elo[[batter_id]]
  bowler_run_before <- bowler_run_elo[[bowler_id]]
  bowler_wicket_before <- bowler_wicket_elo[[bowler_id]]
  venue_perm_run_before <- venue_perm_run_elo[[venue]]
  venue_perm_wicket_before <- venue_perm_wicket_elo[[venue]]
  venue_session_run_before <- venue_session_run_elo
  venue_session_wicket_before <- venue_session_wicket_elo

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

  # Apply inactivity decay to ELOs
  if (days_inactive_batter > THREE_WAY_INACTIVITY_THRESHOLD_DAYS) {
    batter_run_before <- apply_inactivity_decay(batter_run_before, days_inactive_batter)
    batter_wicket_before <- apply_inactivity_decay(batter_wicket_before, days_inactive_batter)
    # Update state with decayed values
    batter_run_elo[[batter_id]] <- batter_run_before
    batter_wicket_elo[[batter_id]] <- batter_wicket_before
  }

  if (days_inactive_bowler > THREE_WAY_INACTIVITY_THRESHOLD_DAYS) {
    bowler_run_before <- apply_inactivity_decay(bowler_run_before, days_inactive_bowler)
    bowler_wicket_before <- apply_inactivity_decay(bowler_wicket_before, days_inactive_bowler)
    bowler_run_elo[[bowler_id]] <- bowler_run_before
    bowler_wicket_elo[[bowler_id]] <- bowler_wicket_before
  }

  # Determine context
  phase <- get_match_phase(over, current_format)
  is_knockout <- !is.null(outcome_type) && outcome_type %in% c("knockout", "final", "semifinal")
  event_tier_val <- get_event_tier(event_name)

  # Calculate expected values using calibration baseline + ELO adjustments
  # Both expected and actual are on the runs scale (0-6)
  exp_runs <- calculate_3way_expected_runs(
    calibration$mean_runs,
    batter_run_before,
    bowler_run_before,
    venue_perm_run_before,
    venue_session_run_before,
    current_format
  )

  exp_wicket <- calculate_3way_expected_wicket(
    calibration$wicket_rate,
    batter_wicket_before,
    bowler_wicket_before,
    venue_perm_wicket_before,
    venue_session_wicket_before
  )

  # Calculate actual outcome (runs scale 0-6, matching expected)
  # Use actual runs directly, not the 0-1 scoring scale
  actual_run_score <- runs  # Already on runs scale (0, 1, 2, 3, 4, 6)
  actual_wicket_val <- if (is_wicket) 1 else 0

  # Get K-factors
  k_batter_run <- get_3way_player_k(batter_balls, days_inactive_batter, current_format, "run",
                                     phase, is_knockout, event_tier_val, FALSE)
  k_bowler_run <- get_3way_player_k(bowler_balls, days_inactive_bowler, current_format, "run",
                                     phase, is_knockout, event_tier_val, FALSE)
  k_venue_perm_run <- get_3way_venue_perm_k(venue_total_balls, current_format)
  k_venue_session_run <- get_3way_venue_session_k(balls_in_current_match, current_format)

  k_batter_wicket <- get_3way_player_k(batter_balls, days_inactive_batter, current_format, "wicket",
                                        phase, is_knockout, event_tier_val, FALSE)
  k_bowler_wicket <- get_3way_player_k(bowler_balls, days_inactive_bowler, current_format, "wicket",
                                        phase, is_knockout, event_tier_val, FALSE)
  k_venue_perm_wicket <- get_3way_venue_perm_k(venue_total_balls, current_format)
  k_venue_session_wicket <- get_3way_venue_session_k(balls_in_current_match, current_format)

  # Calculate ELO updates
  run_updates <- update_3way_run_elos(
    actual_run_score, exp_runs,
    k_batter_run, k_bowler_run, k_venue_perm_run, k_venue_session_run
  )

  wicket_updates <- update_3way_wicket_elos(
    actual_wicket_val, exp_wicket,
    k_batter_wicket, k_bowler_wicket, k_venue_perm_wicket, k_venue_session_wicket
  )

  # Apply updates
  batter_run_after <- batter_run_before + run_updates$delta_batter
  bowler_run_after <- bowler_run_before + run_updates$delta_bowler
  venue_perm_run_after <- venue_perm_run_before + run_updates$delta_venue_perm
  venue_session_run_after <- venue_session_run_before + run_updates$delta_venue_session

  batter_wicket_after <- batter_wicket_before + wicket_updates$delta_batter
  bowler_wicket_after <- bowler_wicket_before + wicket_updates$delta_bowler
  venue_perm_wicket_after <- venue_perm_wicket_before + wicket_updates$delta_venue_perm
  venue_session_wicket_after <- venue_session_wicket_before + wicket_updates$delta_venue_session

  # Update state
  batter_run_elo[[batter_id]] <- batter_run_after
  batter_wicket_elo[[batter_id]] <- batter_wicket_after
  bowler_run_elo[[bowler_id]] <- bowler_run_after
  bowler_wicket_elo[[bowler_id]] <- bowler_wicket_after
  venue_perm_run_elo[[venue]] <- venue_perm_run_after
  venue_perm_wicket_elo[[venue]] <- venue_perm_wicket_after
  venue_session_run_elo <- venue_session_run_after
  venue_session_wicket_elo <- venue_session_wicket_after

  # Update ball counts
  batter_balls_count[[batter_id]] <- batter_balls + 1L
  bowler_balls_count[[bowler_id]] <- bowler_balls + 1L
  venue_balls_count[[venue]] <- venue_total_balls + 1L
  balls_in_current_match <- balls_in_current_match + 1L

  # Store results
  phase_num <- switch(phase, "powerplay" = 1, "middle" = 2, "death" = 3, 2)

  result_mat[i, ] <- c(
    # Player Run ELOs
    batter_run_before, batter_run_after,
    bowler_run_before, bowler_run_after,
    # Player Wicket ELOs
    batter_wicket_before, batter_wicket_after,
    bowler_wicket_before, bowler_wicket_after,
    # Venue Permanent ELOs
    venue_perm_run_before, venue_perm_run_after,
    venue_perm_wicket_before, venue_perm_wicket_after,
    # Venue Session ELOs
    venue_session_run_before, venue_session_run_after,
    venue_session_wicket_before, venue_session_wicket_after,
    # K-factors
    k_batter_run, k_bowler_run, k_venue_perm_run, k_venue_session_run,
    k_batter_wicket, k_bowler_wicket, k_venue_perm_wicket, k_venue_session_wicket,
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
}

cli::cli_progress_done()

## 4.8.1 Checkpoint Save (in case downstream steps fail) ----
cli::cli_h2("Saving checkpoint")

checkpoint_dir <- file.path(find_bouncerdata_dir(), "checkpoints")
if (!dir.exists(checkpoint_dir)) dir.create(checkpoint_dir, recursive = TRUE)

checkpoint_file <- file.path(checkpoint_dir, paste0(current_category, "_3way_elo_checkpoint.qs"))

# Save the result matrix and key vectors needed to rebuild
checkpoint_data <- list(
  result_mat = result_mat,
  delivery_ids = delivery_ids,
  match_ids = match_ids,
  match_dates = match_dates,
  batter_ids = batter_ids,
  bowler_ids = bowler_ids,
  venues = venues,
  n_deliveries = n_deliveries,
  current_format = current_format,
  current_category = current_category
)
saveRDS(checkpoint_data, checkpoint_file)
cli::cli_alert_success("Checkpoint saved to {checkpoint_file}")
cli::cli_alert_info("If subsequent steps fail, you can reload with: checkpoint <- qs::qread('{checkpoint_file}')")

## 4.9 Apply Post-Match Normalization ----
# Note: Skipping normalization when loading from checkpoint as it was already applied
if (THREE_WAY_NORMALIZE_AFTER_MATCH) {
  cli::cli_h2("Applying normalization")

  # Get all final ELOs
  batter_run_elos <- unlist(mget(ls(batter_run_elo), envir = batter_run_elo))
  batter_wicket_elos <- unlist(mget(ls(batter_wicket_elo), envir = batter_wicket_elo))
  bowler_run_elos <- unlist(mget(ls(bowler_run_elo), envir = bowler_run_elo))
  bowler_wicket_elos <- unlist(mget(ls(bowler_wicket_elo), envir = bowler_wicket_elo))
  venue_perm_run_elos <- unlist(mget(ls(venue_perm_run_elo), envir = venue_perm_run_elo))
  venue_perm_wicket_elos <- unlist(mget(ls(venue_perm_wicket_elo), envir = venue_perm_wicket_elo))

  # Log drift before normalization
  cli::cli_alert_info("Mean ELOs before normalization:")
  cli::cli_alert_info("  Batter Run: {round(mean(batter_run_elos), 1)}")
  cli::cli_alert_info("  Bowler Run: {round(mean(bowler_run_elos), 1)}")
  cli::cli_alert_info("  Venue Perm Run: {round(mean(venue_perm_run_elos), 1)}")

  # Normalize each entity type separately
  batter_run_elos_norm <- normalize_3way_elos(batter_run_elos)
  batter_wicket_elos_norm <- normalize_3way_elos(batter_wicket_elos)
  bowler_run_elos_norm <- normalize_3way_elos(bowler_run_elos)
  bowler_wicket_elos_norm <- normalize_3way_elos(bowler_wicket_elos)
  venue_perm_run_elos_norm <- normalize_3way_elos(venue_perm_run_elos)
  venue_perm_wicket_elos_norm <- normalize_3way_elos(venue_perm_wicket_elos)

  cli::cli_alert_info("Mean ELOs after normalization:")
  cli::cli_alert_info("  Batter Run: {round(mean(batter_run_elos_norm), 1)}")
  cli::cli_alert_info("  Bowler Run: {round(mean(bowler_run_elos_norm), 1)}")
  cli::cli_alert_info("  Venue Perm Run: {round(mean(venue_perm_run_elos_norm), 1)}")

  # Log drift metrics (wrapped in tryCatch to handle ICU extension issues)
  tryCatch({
    log_3way_drift_metrics(current_format, "run", "batter",
                           mean(batter_run_elos_norm), sd(batter_run_elos_norm), conn)
    log_3way_drift_metrics(current_format, "wicket", "batter",
                           mean(batter_wicket_elos_norm), sd(batter_wicket_elos_norm), conn)
    log_3way_drift_metrics(current_format, "run", "bowler",
                           mean(bowler_run_elos_norm), sd(bowler_run_elos_norm), conn)
    log_3way_drift_metrics(current_format, "wicket", "bowler",
                           mean(bowler_wicket_elos_norm), sd(bowler_wicket_elos_norm), conn)
    log_3way_drift_metrics(current_format, "run", "venue",
                           mean(venue_perm_run_elos_norm), sd(venue_perm_run_elos_norm), conn)
    log_3way_drift_metrics(current_format, "wicket", "venue",
                           mean(venue_perm_wicket_elos_norm), sd(venue_perm_wicket_elos_norm), conn)
    cli::cli_alert_success("Drift metrics logged")
  }, error = function(e) {
    cli::cli_alert_warning("Could not log drift metrics (ICU extension issue?): {e$message}")
    cli::cli_alert_info("Continuing without drift logging...")
  })

  cli::cli_alert_success("Normalization complete")
}

}  # End if (!skip_to_insert)

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

  batter_run_elo_before = result_mat[, "batter_run_before"],
  batter_run_elo_after = result_mat[, "batter_run_after"],
  bowler_run_elo_before = result_mat[, "bowler_run_before"],
  bowler_run_elo_after = result_mat[, "bowler_run_after"],

  batter_wicket_elo_before = result_mat[, "batter_wicket_before"],
  batter_wicket_elo_after = result_mat[, "batter_wicket_after"],
  bowler_wicket_elo_before = result_mat[, "bowler_wicket_before"],
  bowler_wicket_elo_after = result_mat[, "bowler_wicket_after"],

  venue_perm_run_elo_before = result_mat[, "venue_perm_run_before"],
  venue_perm_run_elo_after = result_mat[, "venue_perm_run_after"],
  venue_perm_wicket_elo_before = result_mat[, "venue_perm_wicket_before"],
  venue_perm_wicket_elo_after = result_mat[, "venue_perm_wicket_after"],

  venue_session_run_elo_before = result_mat[, "venue_session_run_before"],
  venue_session_run_elo_after = result_mat[, "venue_session_run_after"],
  venue_session_wicket_elo_before = result_mat[, "venue_session_wicket_before"],
  venue_session_wicket_elo_after = result_mat[, "venue_session_wicket_after"],

  k_batter_run = result_mat[, "k_batter_run"],
  k_bowler_run = result_mat[, "k_bowler_run"],
  k_venue_perm_run = result_mat[, "k_venue_perm_run"],
  k_venue_session_run = result_mat[, "k_venue_session_run"],
  k_batter_wicket = result_mat[, "k_batter_wicket"],
  k_bowler_wicket = result_mat[, "k_bowler_wicket"],
  k_venue_perm_wicket = result_mat[, "k_venue_perm_wicket"],
  k_venue_session_wicket = result_mat[, "k_venue_session_wicket"],

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
  insert_3way_elos(batch_df, current_format, conn)

  cli::cli_progress_update()
}

cli::cli_progress_done()

cli::cli_alert_success("Inserted {format(n_deliveries, big.mark = ',')} deliveries")

## 4.12 Store Parameters ----
cli::cli_h2("Storing parameters")

last_row <- result_dt[.N]
store_3way_elo_params(
  current_params,
  last_delivery_id = last_row$delivery_id,
  last_match_date = last_row$match_date,
  total_deliveries = n_deliveries,
  conn = conn
)

cli::cli_alert_success("Parameters stored")

## 4.12.1 Cleanup checkpoint on success ----
checkpoint_file <- file.path(find_bouncerdata_dir(), "checkpoints",
                              paste0(current_category, "_3way_elo_checkpoint.qs"))
if (file.exists(checkpoint_file)) {
  file.remove(checkpoint_file)
  cli::cli_alert_success("Checkpoint file cleaned up")
}

## 4.13 Summary Statistics ----
cat("\n")
cli::cli_h2("{toupper(current_gender)} {toupper(current_format)} 3-Way ELO Summary")

# Wrap in tryCatch in case ICU extension causes issues
tryCatch({
  stats <- get_3way_elo_stats(current_format, conn)

  if (!is.null(stats)) {
    cli::cli_alert_info("Total records: {format(stats$total_records, big.mark = ',')}")
    cli::cli_alert_info("Unique batters: {stats$unique_batters}")
    cli::cli_alert_info("Unique bowlers: {stats$unique_bowlers}")
    cli::cli_alert_info("Unique venues: {stats$unique_venues}")
    cli::cli_alert_info("Date range: {stats$first_date} to {stats$last_date}")

    cat("\nMean ELOs:\n")
    cat(sprintf("  Batter Run ELO:        %.1f\n", stats$mean_batter_run_elo))
    cat(sprintf("  Bowler Run ELO:        %.1f\n", stats$mean_bowler_run_elo))
    cat(sprintf("  Batter Wicket ELO:     %.1f\n", stats$mean_batter_wicket_elo))
    cat(sprintf("  Bowler Wicket ELO:     %.1f\n", stats$mean_bowler_wicket_elo))
    cat(sprintf("  Venue Perm Run ELO:    %.1f\n", stats$mean_venue_perm_run_elo))
    cat(sprintf("  Venue Session Run ELO: %.1f\n", stats$mean_venue_session_run_elo))
  }
}, error = function(e) {
  cli::cli_alert_warning("Could not retrieve summary stats (ICU extension issue?)")
  cli::cli_alert_info("Data was saved successfully - you can query the table directly")
})

cat("\n")
cli::cli_alert_success("{toupper(current_gender)} {toupper(current_format)} 3-Way ELO calculation complete!")
cat("\n")

}  # End format loop
}  # End gender loop

# 5. Final Summary ----
cat("\n")
cli::cli_h1("All Categories Complete")
cli::cli_alert_success("Processed genders: {paste(toupper(genders_to_process), collapse = ', ')}")
cli::cli_alert_success("Processed formats: {paste(toupper(formats_to_process), collapse = ', ')}")
cat("\n")

cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "Run 05_optimize_3way_params.R to optimize K-factors",
  "i" = "Data stored in {format}_3way_elo tables",
  "i" = "Drift metrics logged in three_way_elo_drift_metrics table"
))
cat("\n")

cli::cli_h3("Query Example")
cat("
-- Get 3-way ELO features for a delivery
SELECT
  d.*,
  e.batter_run_elo_before,
  e.bowler_run_elo_before,
  e.venue_perm_run_elo_before,
  e.venue_session_run_elo_before,
  e.exp_runs,
  e.exp_wicket
FROM deliveries d
JOIN t20_3way_elo e ON d.delivery_id = e.delivery_id
LIMIT 10
")
cat("\n")
