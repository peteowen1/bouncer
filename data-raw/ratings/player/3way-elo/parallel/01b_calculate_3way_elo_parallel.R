# 04 Calculate 3-Way ELOs (Parallel Version) ----
#
# Single format/gender calculation that outputs to parquet.
# Run multiple instances in parallel, then load to DB at end.
#
# Usage: Rscript 04_calculate_3way_elo_parallel.R <format> <gender>
# Example: Rscript 04_calculate_3way_elo_parallel.R t20 mens

# 1. Setup ----
library(DBI)
library(data.table)
library(arrow)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("Usage: Rscript 04_calculate_3way_elo_parallel.R <format> <gender>\n  format: t20, odi, test\n  gender: mens, womens")
}

current_format <- tolower(args[1])
current_gender <- tolower(args[2])

# Load bouncer package
bouncer_root <- tryCatch({
  args_all <- commandArgs(trailingOnly = FALSE)
  file_arg <- args_all[grep("^--file=", args_all)]
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

BATCH_SIZE <- 10000

# Validate inputs
if (!current_format %in% names(FORMAT_GROUPS)) {
  stop("Invalid format: ", current_format, ". Must be one of: t20, odi, test")
}
if (!current_gender %in% names(GENDER_CATEGORIES)) {
  stop("Invalid gender: ", current_gender, ". Must be one of: mens, womens")
}

current_category <- paste0(current_gender, "_", current_format)
gender_db_value <- GENDER_CATEGORIES[[current_gender]]
format_match_types <- FORMAT_GROUPS[[current_format]]

cat("\n")
cli::cli_h1("{toupper(current_gender)} {toupper(current_format)} 3-Way ELO Calculation (Parallel)")
cat("\n")

# 3. Database Connection (READ ONLY for parallel safety) ----
cli::cli_h2("Connecting to database (read-only)")
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cli::cli_alert_success("Connected to database")

# 4. Build Parameters ----
cli::cli_h2("Building parameters")
current_params <- build_3way_elo_params(current_format)
current_params$format <- current_category

# 5. Load Calibration Data ----
cli::cli_h2("Loading calibration data")
calibration <- get_calibration_data(current_format, conn)

if (is.null(calibration)) {
  cli::cli_alert_danger("No calibration data found!")
  stop("Calibration data required")
}

cli::cli_alert_success("Calibration loaded: wicket_rate={round(calibration$wicket_rate * 100, 2)}%, mean_runs={round(calibration$mean_runs, 3)}")

# 6. Load Deliveries ----
cli::cli_h2("Loading {toupper(current_gender)} {toupper(current_format)} deliveries")

match_type_filter <- paste(sprintf("'%s'", tolower(format_match_types)), collapse = ", ")

query <- sprintf("
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
  ORDER BY d.match_date, d.match_id, d.delivery_id
", match_type_filter, gender_db_value)

deliveries <- DBI::dbGetQuery(conn, query)
setDT(deliveries)

n_deliveries <- nrow(deliveries)
n_matches <- uniqueN(deliveries$match_id)

cli::cli_alert_success("Loaded {format(n_deliveries, big.mark = ',')} deliveries from {n_matches} matches")

if (n_deliveries == 0) {
  cli::cli_alert_warning("No deliveries to process for {current_category}")
  quit(save = "no", status = 0)
}

# 7. Get Last Match Dates for Inactivity Calculation ----
cli::cli_h2("Calculating player inactivity")

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

player_match_history <- player_last_match[order(player_id, last_match_date)]
player_match_history[, prev_match_date := shift(last_match_date, 1), by = player_id]

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

# 8. Initialize Entity State ----
cli::cli_h2("Initializing entity state")

all_batters <- unique(deliveries$batter_id)
all_bowlers <- unique(deliveries$bowler_id)
all_venues <- unique(deliveries$venue)

cli::cli_alert_info("{length(all_batters)} batters, {length(all_bowlers)} bowlers, {length(all_venues)} venues")

batter_run_elo <- new.env(hash = TRUE)
batter_wicket_elo <- new.env(hash = TRUE)
batter_balls_count <- new.env(hash = TRUE)
bowler_run_elo <- new.env(hash = TRUE)
bowler_wicket_elo <- new.env(hash = TRUE)
bowler_balls_count <- new.env(hash = TRUE)
venue_perm_run_elo <- new.env(hash = TRUE)
venue_perm_wicket_elo <- new.env(hash = TRUE)
venue_balls_count <- new.env(hash = TRUE)

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

# 9. Pre-extract Columns ----
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

# 10. Pre-allocate Result Matrix ----
cli::cli_h2("Processing deliveries")

result_cols <- c(
  "batter_run_before", "batter_run_after",
  "bowler_run_before", "bowler_run_after",
  "batter_wicket_before", "batter_wicket_after",
  "bowler_wicket_before", "bowler_wicket_after",
  "venue_perm_run_before", "venue_perm_run_after",
  "venue_perm_wicket_before", "venue_perm_wicket_after",
  "venue_session_run_before", "venue_session_run_after",
  "venue_session_wicket_before", "venue_session_wicket_after",
  "k_batter_run", "k_bowler_run", "k_venue_perm_run", "k_venue_session_run",
  "k_batter_wicket", "k_bowler_wicket", "k_venue_perm_wicket", "k_venue_session_wicket",
  "exp_runs", "exp_wicket", "actual_runs", "is_wicket_num",
  "batter_balls", "bowler_balls", "venue_balls", "balls_in_match",
  "days_inactive_batter", "days_inactive_bowler",
  "is_knockout_num", "event_tier", "phase_num"
)

result_mat <- matrix(NA_real_, nrow = n_deliveries, ncol = length(result_cols))
colnames(result_mat) <- result_cols

current_match_id <- ""
venue_session_run_elo <- THREE_WAY_ELO_START
venue_session_wicket_elo <- THREE_WAY_ELO_START
balls_in_current_match <- 0L
matches_processed <- 0L

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

cli::cli_progress_bar("Calculating 3-Way ELOs", total = n_deliveries)

for (i in seq_len(n_deliveries)) {
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

  if (match_id != current_match_id) {
    if (current_match_id != "" && THREE_WAY_NORMALIZE_AFTER_MATCH) {
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
    if (matches_processed %% 500 == 0) {
      batter_elos <- unlist(mget(ls(batter_run_elo), envir = batter_run_elo))
      cli::cli_alert_info("Match {matches_processed}: Mean batter run ELO = {round(mean(batter_elos), 1)}")
    }
  }

  batter_run_before <- batter_run_elo[[batter_id]]
  batter_wicket_before <- batter_wicket_elo[[batter_id]]
  bowler_run_before <- bowler_run_elo[[bowler_id]]
  bowler_wicket_before <- bowler_wicket_elo[[bowler_id]]
  venue_perm_run_before <- venue_perm_run_elo[[venue]]
  venue_perm_wicket_before <- venue_perm_wicket_elo[[venue]]
  venue_session_run_before <- venue_session_run_elo
  venue_session_wicket_before <- venue_session_wicket_elo

  batter_balls <- batter_balls_count[[batter_id]]
  bowler_balls <- bowler_balls_count[[bowler_id]]
  venue_total_balls <- venue_balls_count[[venue]]

  batter_key <- paste0(batter_id, ":", match_id)
  bowler_key <- paste0(bowler_id, ":", match_id)
  batter_prev_date <- batter_prev_match[[batter_key]]
  bowler_prev_date <- bowler_prev_match[[bowler_key]]

  days_inactive_batter <- if (!is.null(batter_prev_date) && !is.na(batter_prev_date)) {
    as.integer(as.Date(match_date) - as.Date(batter_prev_date))
  } else { 0L }

  days_inactive_bowler <- if (!is.null(bowler_prev_date) && !is.na(bowler_prev_date)) {
    as.integer(as.Date(match_date) - as.Date(bowler_prev_date))
  } else { 0L }

  if (days_inactive_batter > THREE_WAY_INACTIVITY_THRESHOLD_DAYS) {
    batter_run_before <- apply_inactivity_decay(batter_run_before, days_inactive_batter)
    batter_wicket_before <- apply_inactivity_decay(batter_wicket_before, days_inactive_batter)
    batter_run_elo[[batter_id]] <- batter_run_before
    batter_wicket_elo[[batter_id]] <- batter_wicket_before
  }

  if (days_inactive_bowler > THREE_WAY_INACTIVITY_THRESHOLD_DAYS) {
    bowler_run_before <- apply_inactivity_decay(bowler_run_before, days_inactive_bowler)
    bowler_wicket_before <- apply_inactivity_decay(bowler_wicket_before, days_inactive_bowler)
    bowler_run_elo[[bowler_id]] <- bowler_run_before
    bowler_wicket_elo[[bowler_id]] <- bowler_wicket_before
  }

  phase <- get_match_phase(over, current_format)
  is_knockout <- !is.null(outcome_type) && outcome_type %in% c("knockout", "final", "semifinal")
  event_tier_val <- get_event_tier(event_name)

  exp_runs <- calculate_3way_expected_runs(
    calibration$mean_runs, batter_run_before, bowler_run_before,
    venue_perm_run_before, venue_session_run_before, current_format
  )

  exp_wicket <- calculate_3way_expected_wicket(
    calibration$wicket_rate, batter_wicket_before, bowler_wicket_before,
    venue_perm_wicket_before, venue_session_wicket_before
  )

  actual_run_score <- runs
  actual_wicket_val <- if (is_wicket) 1 else 0

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

  run_updates <- update_3way_run_elos(
    actual_run_score, exp_runs,
    k_batter_run, k_bowler_run, k_venue_perm_run, k_venue_session_run
  )

  wicket_updates <- update_3way_wicket_elos(
    actual_wicket_val, exp_wicket,
    k_batter_wicket, k_bowler_wicket, k_venue_perm_wicket, k_venue_session_wicket
  )

  batter_run_after <- batter_run_before + run_updates$delta_batter
  bowler_run_after <- bowler_run_before + run_updates$delta_bowler
  venue_perm_run_after <- venue_perm_run_before + run_updates$delta_venue_perm
  venue_session_run_after <- venue_session_run_before + run_updates$delta_venue_session

  batter_wicket_after <- batter_wicket_before + wicket_updates$delta_batter
  bowler_wicket_after <- bowler_wicket_before + wicket_updates$delta_bowler
  venue_perm_wicket_after <- venue_perm_wicket_before + wicket_updates$delta_venue_perm
  venue_session_wicket_after <- venue_session_wicket_before + wicket_updates$delta_venue_session

  batter_run_elo[[batter_id]] <- batter_run_after
  batter_wicket_elo[[batter_id]] <- batter_wicket_after
  bowler_run_elo[[bowler_id]] <- bowler_run_after
  bowler_wicket_elo[[bowler_id]] <- bowler_wicket_after
  venue_perm_run_elo[[venue]] <- venue_perm_run_after
  venue_perm_wicket_elo[[venue]] <- venue_perm_wicket_after
  venue_session_run_elo <- venue_session_run_after
  venue_session_wicket_elo <- venue_session_wicket_after

  batter_balls_count[[batter_id]] <- batter_balls + 1L
  bowler_balls_count[[bowler_id]] <- bowler_balls + 1L
  venue_balls_count[[venue]] <- venue_total_balls + 1L
  balls_in_current_match <- balls_in_current_match + 1L

  phase_num <- switch(phase, "powerplay" = 1, "middle" = 2, "death" = 3, 2)

  result_mat[i, ] <- c(
    batter_run_before, batter_run_after,
    bowler_run_before, bowler_run_after,
    batter_wicket_before, batter_wicket_after,
    bowler_wicket_before, bowler_wicket_after,
    venue_perm_run_before, venue_perm_run_after,
    venue_perm_wicket_before, venue_perm_wicket_after,
    venue_session_run_before, venue_session_run_after,
    venue_session_wicket_before, venue_session_wicket_after,
    k_batter_run, k_bowler_run, k_venue_perm_run, k_venue_session_run,
    k_batter_wicket, k_bowler_wicket, k_venue_perm_wicket, k_venue_session_wicket,
    exp_runs, exp_wicket, runs, actual_wicket_val,
    batter_balls + 1L, bowler_balls + 1L, venue_total_balls + 1L, balls_in_current_match,
    days_inactive_batter, days_inactive_bowler,
    as.integer(is_knockout), event_tier_val, phase_num
  )

  if (i %% 10000 == 0) {
    cli::cli_progress_update(set = i)
  }
}

cli::cli_progress_done()

# 11. Build Result Data.Table ----
cli::cli_h2("Building output data")

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

# 12. Write to Parquet ----
cli::cli_h2("Writing to parquet")

output_dir <- file.path(find_bouncerdata_dir(), "temp_3way_elo")
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

output_file <- file.path(output_dir, paste0(current_category, "_3way_elo.parquet"))
arrow::write_parquet(result_dt, output_file)

cli::cli_alert_success("Wrote {format(n_deliveries, big.mark = ',')} rows to {output_file}")

# 13. Summary ----
cat("\n")
cli::cli_h2("Summary")
cli::cli_alert_success("{toupper(current_gender)} {toupper(current_format)}: {format(n_deliveries, big.mark = ',')} deliveries processed")
cli::cli_alert_success("Output: {output_file}")

batter_elos <- unlist(mget(ls(batter_run_elo), envir = batter_run_elo))
cli::cli_alert_info("Final mean batter run ELO: {round(mean(batter_elos), 1)}")

cat("\n")
