# In-Match Data Preparation (All Formats) ----
#
# Prepares delivery-level data for the two-stage in-match prediction models:
#   Stage 1: Projected score model (1st innings regression)
#   Stage 2: Win probability model (2nd innings chase classification)
#
# Supports T20, ODI, and Test (longform) formats.
# Uses all available matches (not just IPL).
#
# Output per format:
#   - bouncerdata/models/{format}_stage1_data.rds
#   - bouncerdata/models/{format}_stage2_data.rds
#   - bouncerdata/models/{format}_inmatch_venue_stats.rds
#
# Usage:
#   source("data-raw/models/in-match/01_prepare_all_formats.R")

# Setup ----
library(DBI)
library(dplyr)
library(tidyr)
library(data.table)  # Required for calculate_rolling_features `:=` dispatch
devtools::load_all()

# Configuration ----
RANDOM_SEED <- 42
if (!exists("FORMATS_TO_PREPARE")) FORMATS_TO_PREPARE <- c("t20", "odi")  # Test needs different handling
TEST_SEASONS <- c("2024", "2025", "2023/24", "2024/25")

FORMAT_MATCH_TYPES <- list(
  t20 = c("T20", "IT20"),
  odi = c("ODI", "ODM"),
  test = c("Test", "MDM")
)

# Max overs per format (for balls_remaining calculation)
FORMAT_MAX_OVERS <- list(t20 = 20, odi = 50, test = NULL)

output_dir <- file.path(find_bouncerdata_dir(), "models")
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

cat("\n")
cli::cli_h1("In-Match Data Preparation (All Formats)")
cli::cli_alert_info("Formats: {paste(toupper(FORMATS_TO_PREPARE), collapse = ', ')}")
cat("\n")

# Database Connection ----
conn <- get_db_connection(read_only = TRUE)
# Note: no on.exit() — connection closed explicitly at end to avoid
# premature close when sourced from wrapper scripts

for (current_format in FORMATS_TO_PREPARE) {

  cat("\n")
  cli::cli_h1("{toupper(current_format)} Format")
  cat("\n")

  match_types <- FORMAT_MATCH_TYPES[[current_format]]
  match_type_filter <- paste(sprintf("'%s'", tolower(match_types)), collapse = ", ")
  max_overs <- FORMAT_MAX_OVERS[[current_format]]
  is_longform <- current_format == "test"

  # Load matches ----
  cli::cli_h2("Loading match data")

  matches_query <- sprintf("
    SELECT
      match_id, season, match_type, match_date, venue, city, gender,
      team1, team2, toss_winner, toss_decision,
      outcome_type, outcome_winner, outcome_by_runs, outcome_by_wickets,
      outcome_method, event_name, event_match_number, event_group
    FROM cricsheet.matches
    WHERE LOWER(match_type) IN (%s)
      AND outcome_winner IS NOT NULL AND outcome_winner != ''
    ORDER BY match_date, match_id
  ", match_type_filter)

  matches_df <- DBI::dbGetQuery(conn, matches_query)
  cli::cli_alert_success("Loaded {nrow(matches_df)} {toupper(current_format)} matches with results")

  # Classify outcomes
  matches_df <- classify_match_outcomes(matches_df)

  valid_match_ids <- matches_df %>%
    filter(is_valid_for_training) %>%
    pull(match_id)
  cli::cli_alert_info("Valid for training: {length(valid_match_ids)} matches")

  # Load innings totals ----
  innings_query <- sprintf("
    SELECT match_id, innings, batting_team, bowling_team,
           total_runs AS innings_total, total_wickets AS innings_wickets,
           total_overs AS innings_overs
    FROM cricsheet.match_innings
    WHERE match_id IN (SELECT match_id FROM cricsheet.matches WHERE LOWER(match_type) IN (%s))
    ORDER BY match_id, innings
  ", match_type_filter)

  innings_df <- DBI::dbGetQuery(conn, innings_query)

  first_innings_totals <- innings_df %>%
    filter(innings == 1) %>%
    select(match_id, innings1_total = innings_total,
           innings1_wickets = innings_wickets, innings1_overs = innings_overs,
           innings1_batting_team = batting_team)

  # Load deliveries ----
  cli::cli_h2("Loading delivery data")

  deliveries_query <- sprintf("
    SELECT
      d.delivery_id, d.match_id, d.season, d.match_type, d.match_date,
      d.venue, d.city, d.gender, d.batting_team, d.bowling_team,
      d.innings, d.over, d.ball, d.over_ball,
      d.batter_id, d.bowler_id,
      d.runs_batter, d.runs_extras, d.runs_total,
      d.is_boundary, d.is_four, d.is_six, d.is_wicket, d.wicket_kind,
      d.total_runs,
      (d.wickets_fallen - CAST(d.is_wicket AS INT)) AS wickets_fallen
    FROM cricsheet.deliveries d
    WHERE LOWER(d.match_type) IN (%s)
    ORDER BY d.match_date, d.match_id, d.innings, d.over, d.ball
  ", match_type_filter)

  deliveries_df <- DBI::dbGetQuery(conn, deliveries_query)

  # Filter to valid matches (safer than SQL IN clause with string IDs)
  deliveries_df <- deliveries_df[deliveries_df$match_id %in% valid_match_ids, ]
  cli::cli_alert_success("Loaded {nrow(deliveries_df)} deliveries from {length(unique(deliveries_df$match_id))} matches")

  # Join match + innings info ----
  deliveries_df <- deliveries_df %>%
    left_join(
      matches_df %>% select(match_id, outcome_type, outcome_winner, outcome_method,
                            is_dls_match, is_super_over, is_pure_tie, is_no_result,
                            is_knockout, is_valid_for_training, event_match_number),
      by = "match_id"
    ) %>%
    left_join(first_innings_totals, by = "match_id")

  # Calculate features ----
  cli::cli_h2("Calculating features")

  # Phase features (format-aware)
  phase_features <- calculate_phase_features(
    over = deliveries_df$over,
    ball = deliveries_df$ball,
    match_type = current_format
  )
  deliveries_df <- bind_cols(deliveries_df, phase_features)

  # Run rates, balls remaining, wickets in hand
  if (!is.null(max_overs)) {
    max_balls <- max_overs * 6
    deliveries_df <- deliveries_df %>%
      mutate(
        balls_bowled = over * 6 + ball,
        balls_remaining = pmax(0, max_balls - balls_bowled),
        overs_remaining = balls_remaining / 6,
        # Shrunk toward the format prior. Raw division made the rate after one
        # ball 0, 6, 24 or 36 -- noise the model read as signal, which is what
        # gave the first ball of an innings a free +4.6 TSA in ODI (#70).
        current_run_rate = shrunk_run_rate(total_runs, balls_bowled, current_format),
        wickets_in_hand = 10 - wickets_fallen
      )
  } else {
    # Test: no fixed max balls
    deliveries_df <- deliveries_df %>%
      mutate(
        balls_bowled = over * 6 + ball,
        balls_remaining = NA_real_,
        overs_remaining = NA_real_,
        current_run_rate = shrunk_run_rate(total_runs, balls_bowled, current_format),
        wickets_in_hand = 10 - wickets_fallen
      )
  }

  # Rolling features
  cli::cli_alert_info("Calculating rolling features...")
  deliveries_df <- calculate_rolling_features(
    deliveries_df,
    ball_windows = c(12, 24),
    over_windows = c(3, 6)
  )

  # Venue statistics, TIME-CAUSAL and per match (bouncerverse#80).
  #
  # calculate_venue_statistics() averaged EVERY match at a venue, including
  # the match being predicted -- the same leak shape fixed for Test in #69
  # (there, venue_result_rate/venue_avg correlated 1.000 with the label at
  # single-match venues). This adopts the same construction already built for
  # that fix (R/venue_rates.R), matching 08_test_win_probability_v3.R: matches
  # strictly BEFORE the current one at that ground, expanding window, shrunk
  # toward a global prior. Do NOT "fix" this with a leave-one-out subtraction
  # instead -- that was measured for the Test case and made the metric
  # IMPROVE, which is the sign a real leak concentrated rather than left (see
  # the note at the top of R/venue_rates.R).
  cli::cli_h2("Calculating venue statistics (time-causal)")

  venue_avg_raw <- DBI::dbGetQuery(conn, sprintf("
    SELECT m.match_id, m.venue, m.match_date,
           MAX(CASE WHEN mi.innings = 1 THEN mi.total_runs END) AS inn1_total
    FROM cricsheet.matches m
    LEFT JOIN cricsheet.match_innings mi ON mi.match_id = m.match_id
    WHERE LOWER(m.match_type) IN (%s)
    GROUP BY 1, 2, 3
  ", match_type_filter))
  venue_avg_raw$match_date <- as.Date(venue_avg_raw$match_date)
  venue_avgs <- time_causal_venue_mean(venue_avg_raw, "inn1_total", prior_weight = 5)
  venue_avgs <- venue_avgs[, .(match_id, venue_avg_score = venue_mean)]

  venue_chase_raw <- DBI::dbGetQuery(conn, sprintf("
    SELECT m.match_id, m.venue, m.match_date, m.outcome_winner,
           MAX(CASE WHEN mi.innings = 2 THEN mi.batting_team END) AS inn2_batting_team
    FROM cricsheet.matches m
    LEFT JOIN cricsheet.match_innings mi ON mi.match_id = m.match_id
    WHERE LOWER(m.match_type) IN (%s)
    GROUP BY 1, 2, 3, 4
  ", match_type_filter))
  venue_chase_raw$match_date <- as.Date(venue_chase_raw$match_date)
  # NA (no innings-2 batting team recorded -- abandoned before a chase) stays
  # NA rather than FALSE, so it contributes nothing to the average instead of
  # counting as a failed chase, matching the original SQL's NULLIF denominator.
  venue_chase_raw$chase_success <- ifelse(
    is.na(venue_chase_raw$inn2_batting_team), NA_integer_,
    as.integer(venue_chase_raw$inn2_batting_team == venue_chase_raw$outcome_winner)
  )
  venue_chases <- time_causal_venue_mean(venue_chase_raw, "chase_success", prior_weight = 10)
  venue_chases <- venue_chases[, .(match_id, venue_chase_success_rate = venue_mean)]

  deliveries_df <- deliveries_df %>%
    left_join(as.data.frame(venue_avgs), by = "match_id") %>%
    left_join(as.data.frame(venue_chases), by = "match_id")

  # Separate Stage 1 and Stage 2 ----
  cli::cli_h2("Separating innings")

  # Stage 1: First innings
  stage1_data <- deliveries_df %>%
    filter(innings == 1) %>%
    left_join(
      innings_df %>% filter(innings == 1) %>%
        select(match_id, final_innings_total = innings_total),
      by = "match_id"
    ) %>%
    mutate(batting_first_wins = as.integer(outcome_winner == batting_team))

  # A row per innings at BALL ZERO -- the state before anything has happened.
  #
  # Training started at ball one, so the model had never seen 0 balls bowled,
  # yet that is exactly the state the win-probability builder scores as the
  # "before" side of the first delivery. Its projection there was extrapolation,
  # and the resulting first-ball TSA bias survived the run-rate shrinkage: mean
  # -1.209 (ODI) and -1.538 (T20) against +0.006 and -0.019 for every other
  # ball. A model should be trained on the states it is asked to score (#70).
  #
  # The target is the same final_innings_total the ball-one row carries, so this
  # teaches "before a ball is bowled, expect the innings to end here" -- which is
  # what makes the projection a martingale from the very first transition.
  ball0 <- stage1_data %>%
    group_by(match_id) %>%
    slice_min(balls_bowled, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    mutate(
      total_runs = 0L, wickets_fallen = 0L, wickets_in_hand = 10L,
      over = 0L, ball = 0L, balls_bowled = 0L,
      overs_completed = 0, overs_remaining = if (is_longform) NA_real_ else max_overs,
      balls_remaining = if (is_longform) NA_real_ else max_balls,
      current_run_rate = shrunk_run_rate(0, 0, current_format),
      runs_batter = 0L, runs_extras = 0L, runs_total = 0L,
      is_four = 0L, is_six = 0L, is_wicket = 0L
    )
  # Momentum windows describe the balls just gone; before the innings there are
  # none, and zero is the honest value rather than an imputation.
  mom <- grep("^(runs|dots|boundaries|wickets)_last_", names(ball0), value = TRUE)
  for (nm in mom) ball0[[nm]] <- 0
  # rr_last_* is ZERO here, not the prior rate, because that is what SERVING
  # produces: build_cricsheet_win_probability() builds the before-state of ball
  # one by lagging the momentum columns with fill = 0. Setting the prior here
  # instead looked more principled and was a train/serve mismatch -- the model
  # saw rr_last_3_overs = 5.0 in training and 0 at serving, which left the ODI
  # ball-zero projection 8.4 runs high while the same model scored the training
  # ball-zero rows to within 1.3. Match serving; do not out-think it.
  rr <- grep("^rr_last_", names(ball0), value = TRUE)
  for (nm in rr) ball0[[nm]] <- 0

  cli::cli_alert_info(
    "Added {nrow(ball0)} ball-zero row{?s} so the pre-innings state is in the training distribution.")
  stage1_data <- bind_rows(ball0, stage1_data) %>%
    arrange(match_id, balls_bowled)

  # Stage 2: Second innings (for limited-overs only)
  if (!is_longform) {
    stage2_data <- deliveries_df %>%
      filter(innings == 2) %>%
      mutate(
        target_runs = innings1_total + 1,
        innings1_run_rate = calculate_run_rate(innings1_total, innings1_overs * 6)
      )

    # Pressure metrics
    pressure_metrics <- calculate_pressure_metrics(
      target = stage2_data$target_runs,
      current_runs = stage2_data$total_runs,
      current_wickets = stage2_data$wickets_fallen,
      balls_remaining = stage2_data$balls_remaining,
      current_run_rate = stage2_data$current_run_rate
    )
    stage2_data <- bind_cols(stage2_data, pressure_metrics)
    stage2_data <- prepare_win_labels(stage2_data, target_column = "batting_team")

    # Remove NA win labels
    stage2_data <- stage2_data %>% filter(!is.na(batting_team_wins))
  } else {
    # Test: 4 innings, no simple chase model
    stage2_data <- NULL
    cli::cli_alert_info("Test format: Stage 2 (chase) model skipped (4-innings format)")
  }

  # Train/test split ----
  cli::cli_h2("Creating train/test splits")

  stage1_train <- stage1_data %>% filter(!season %in% TEST_SEASONS)
  stage1_test <- stage1_data %>% filter(season %in% TEST_SEASONS)

  cli::cli_alert_info("Stage 1: {nrow(stage1_train)} train / {nrow(stage1_test)} test deliveries")

  if (!is.null(stage2_data)) {
    stage2_train <- stage2_data %>% filter(!season %in% TEST_SEASONS)
    stage2_test <- stage2_data %>% filter(season %in% TEST_SEASONS)
    cli::cli_alert_info("Stage 2: {nrow(stage2_train)} train / {nrow(stage2_test)} test deliveries")
  }

  # Summary stats
  cli::cli_h3("Stage 1: First Innings Totals")
  cat(sprintf("  Mean: %.1f, SD: %.1f, Min: %d, Max: %d\n",
              mean(stage1_train$final_innings_total, na.rm = TRUE),
              sd(stage1_train$final_innings_total, na.rm = TRUE),
              min(stage1_train$final_innings_total, na.rm = TRUE),
              max(stage1_train$final_innings_total, na.rm = TRUE)))

  # Save ----
  cli::cli_h2("Saving prepared data")

  saveRDS(list(train = stage1_train, test = stage1_test),
          file.path(output_dir, paste0(current_format, "_stage1_data.rds")))
  cli::cli_alert_success("Saved {current_format}_stage1_data.rds")

  if (!is.null(stage2_data)) {
    saveRDS(list(train = stage2_train, test = stage2_test),
            file.path(output_dir, paste0(current_format, "_stage2_data.rds")))
    cli::cli_alert_success("Saved {current_format}_stage2_data.rds")
  }

  saveRDS(venue_stats, file.path(output_dir, paste0(current_format, "_inmatch_venue_stats.rds")))
  cli::cli_alert_success("Saved {current_format}_inmatch_venue_stats.rds")

  cat(sprintf("\n  %s complete: %d stage1, %s stage2 deliveries\n",
              toupper(current_format), nrow(stage1_data),
              if (!is.null(stage2_data)) nrow(stage2_data) else "N/A"))
}

# Cleanup DB connection
if (exists("conn") && !is.null(conn)) {
  tryCatch(DBI::dbDisconnect(conn, shutdown = TRUE), error = function(e) NULL)
}

cat("\n")
cli::cli_alert_success("All formats prepared!")
cli::cli_h3("Next Steps")
cli::cli_alert_info("Run 03_projected_score_model.R for projected score (Stage 1)")
cli::cli_alert_info("Run 05_win_probability_innings2.R for chase win prob (Stage 2)")
