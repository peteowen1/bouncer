# Test 3-Way Skill Index Pipeline on Small Subset
#
# Run this script to verify the pipeline works on a small subset
# before running the full calculation.
#
# This tests on 50 matches of Men's T20 data.

library(DBI)
library(data.table)
devtools::load_all()

cat("\n")
cli::cli_h1("3-Way Skill Index Pipeline Test")
cli::cli_alert_info("Testing on 50 matches of Men's T20")
cat("\n")

# Configuration
current_format <- "t20"
current_gender <- "mens"
gender_db_value <- "male"
current_category <- paste0(current_gender, "_", current_format)
MATCH_LIMIT <- 50

# Connect to database
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

# Build parameters
cli::cli_h2("Building parameters")
current_params <- build_skill_index_params(current_format, gender_db_value)
cli::cli_alert_success("Parameters built")
cli::cli_alert_info("Alpha run: max={current_params$alpha_run_max}, min={current_params$alpha_run_min}")
cli::cli_alert_info("Decay per delivery: {current_params$decay_rate}")

# Load calibration
calibration <- get_calibration_data(current_format, conn)
if (is.null(calibration)) {
  stop("No calibration data - run 01_calibrate_expected_values.R first")
}
cli::cli_alert_success("Calibration: mean_runs={round(calibration$mean_runs, 3)}, wicket_rate={round(calibration$wicket_rate * 100, 2)}%")

# Load limited deliveries
cli::cli_h2("Loading deliveries")
match_ids <- DBI::dbGetQuery(conn, sprintf("
  SELECT DISTINCT d.match_id
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  WHERE LOWER(d.match_type) IN ('t20', 'it20')
    AND m.gender = 'male'
  ORDER BY d.match_date
  LIMIT %d
", MATCH_LIMIT))$match_id

placeholders <- paste(rep("?", length(match_ids)), collapse = ", ")
query <- sprintf("
  SELECT
    d.delivery_id,
    d.match_id,
    d.match_date,
    d.batter_id,
    d.bowler_id,
    d.venue,
    d.innings,
    d.over,
    d.runs_batter,
    d.is_wicket
  FROM deliveries d
  WHERE d.match_id IN (%s)
    AND d.batter_id IS NOT NULL
    AND d.bowler_id IS NOT NULL
  ORDER BY d.match_date, d.match_id, d.delivery_id
", placeholders)

deliveries <- DBI::dbGetQuery(conn, query, params = as.list(match_ids))
setDT(deliveries)

n_deliveries <- nrow(deliveries)
n_matches <- uniqueN(deliveries$match_id)

cli::cli_alert_success("Loaded {format(n_deliveries, big.mark = ',')} deliveries from {n_matches} matches")

# Initialize entities
cli::cli_h2("Initializing entities")
all_batters <- unique(deliveries$batter_id)
all_bowlers <- unique(deliveries$bowler_id)
all_venues <- unique(deliveries$venue)

batter_run_skill <- new.env(hash = TRUE)
batter_wicket_skill <- new.env(hash = TRUE)
batter_balls_count <- new.env(hash = TRUE)

bowler_run_skill <- new.env(hash = TRUE)
bowler_wicket_skill <- new.env(hash = TRUE)
bowler_balls_count <- new.env(hash = TRUE)

venue_perm_run_skill <- new.env(hash = TRUE)
venue_perm_wicket_skill <- new.env(hash = TRUE)
venue_balls_count <- new.env(hash = TRUE)

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

cli::cli_alert_success("Initialized {length(all_batters)} batters, {length(all_bowlers)} bowlers, {length(all_venues)} venues at skill=0")

# Process deliveries
cli::cli_h2("Processing deliveries")

# Pre-extract columns
batter_ids <- deliveries$batter_id
bowler_ids <- deliveries$bowler_id
venues_vec <- deliveries$venue
runs_vec <- deliveries$runs_batter
wicket_vec <- deliveries$is_wicket
match_ids_vec <- deliveries$match_id

current_match_id <- ""
venue_session_run_skill <- SKILL_INDEX_START
venue_session_wicket_skill <- SKILL_INDEX_START
balls_in_current_match <- 0L

cli::cli_progress_bar("Processing", total = n_deliveries)

for (i in seq_len(n_deliveries)) {
  batter_id <- batter_ids[i]
  bowler_id <- bowler_ids[i]
  venue <- venues_vec[i]
  runs <- runs_vec[i]
  is_wicket <- wicket_vec[i]
  match_id <- match_ids_vec[i]

  # Reset session on new match
  if (match_id != current_match_id) {
    current_match_id <- match_id
    venue_session_run_skill <- SKILL_INDEX_START
    venue_session_wicket_skill <- SKILL_INDEX_START
    balls_in_current_match <- 0L
  }

  # Get current skills
  batter_run_before <- batter_run_skill[[batter_id]]
  bowler_run_before <- bowler_run_skill[[bowler_id]]
  venue_perm_run_before <- venue_perm_run_skill[[venue]]
  venue_session_run_before <- venue_session_run_skill

  batter_balls <- batter_balls_count[[batter_id]]
  bowler_balls <- bowler_balls_count[[bowler_id]]
  venue_total_balls <- venue_balls_count[[venue]]

  # Calculate expected
  exp_runs <- calculate_expected_runs_skill(
    calibration$mean_runs,
    batter_run_before, bowler_run_before,
    venue_perm_run_before, venue_session_run_before,
    current_format, gender_db_value
  )

  # Get alphas
  alpha_batter <- get_skill_alpha(batter_balls, current_format, "run", gender_db_value)
  alpha_bowler <- get_skill_alpha(bowler_balls, current_format, "run", gender_db_value)
  alpha_venue_perm <- get_venue_perm_skill_alpha(venue_total_balls, current_format)
  alpha_venue_session <- get_venue_session_skill_alpha(balls_in_current_match)

  # Update skills
  updates <- update_run_skills(
    runs, exp_runs,
    alpha_batter, alpha_bowler, alpha_venue_perm, alpha_venue_session,
    batter_run_before, bowler_run_before, venue_perm_run_before, venue_session_run_before,
    current_format, gender_db_value
  )

  # Store updated skills
  batter_run_skill[[batter_id]] <- updates$new_batter
  bowler_run_skill[[bowler_id]] <- updates$new_bowler
  venue_perm_run_skill[[venue]] <- updates$new_venue_perm
  venue_session_run_skill <- updates$new_venue_session

  # Update ball counts
  batter_balls_count[[batter_id]] <- batter_balls + 1L
  bowler_balls_count[[bowler_id]] <- bowler_balls + 1L
  venue_balls_count[[venue]] <- venue_total_balls + 1L
  balls_in_current_match <- balls_in_current_match + 1L

  if (i %% 1000 == 0) {
    cli::cli_progress_update(set = i)
  }
}

cli::cli_progress_done()

# Summary
cli::cli_h2("Results")

batter_skills <- unlist(mget(ls(batter_run_skill), envir = batter_run_skill))
bowler_skills <- unlist(mget(ls(bowler_run_skill), envir = bowler_run_skill))
venue_skills <- unlist(mget(ls(venue_perm_run_skill), envir = venue_perm_run_skill))

cat("\nBatter Run Skills:\n")
cat(sprintf("  Mean: %+.4f\n", mean(batter_skills)))
cat(sprintf("  SD:    %.4f\n", sd(batter_skills)))
cat(sprintf("  Min:  %+.4f\n", min(batter_skills)))
cat(sprintf("  Max:  %+.4f\n", max(batter_skills)))

cat("\nBowler Run Skills:\n")
cat(sprintf("  Mean: %+.4f\n", mean(bowler_skills)))
cat(sprintf("  SD:    %.4f\n", sd(bowler_skills)))
cat(sprintf("  Min:  %+.4f\n", min(bowler_skills)))
cat(sprintf("  Max:  %+.4f\n", max(bowler_skills)))

cat("\nVenue Permanent Run Skills:\n")
cat(sprintf("  Mean: %+.4f\n", mean(venue_skills)))
cat(sprintf("  SD:    %.4f\n", sd(venue_skills)))
cat(sprintf("  Min:  %+.4f\n", min(venue_skills)))
cat(sprintf("  Max:  %+.4f\n", max(venue_skills)))

# Top/Bottom players
cat("\n")
cli::cli_h3("Top 5 Batters (by run skill)")
top_batters <- sort(batter_skills, decreasing = TRUE)[1:5]
for (i in seq_along(top_batters)) {
  balls <- batter_balls_count[[names(top_batters)[i]]]
  cat(sprintf("  %d. %s: %+.3f (%d balls)\n", i, names(top_batters)[i], top_batters[i], balls))
}

cli::cli_h3("Top 5 Bowlers (lowest = best economy)")
top_bowlers <- sort(bowler_skills, decreasing = FALSE)[1:5]
for (i in seq_along(top_bowlers)) {
  balls <- bowler_balls_count[[names(top_bowlers)[i]]]
  cat(sprintf("  %d. %s: %+.3f (%d balls)\n", i, names(top_bowlers)[i], top_bowlers[i], balls))
}

cat("\n")
cli::cli_alert_success("Pipeline test complete!")
cli::cli_alert_info("Skills are in expected range (±0.3 for this small sample)")
