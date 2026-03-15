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
MIN_VENUE_MATCHES <- 5

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
        current_run_rate = calculate_run_rate(total_runs, balls_bowled),
        wickets_in_hand = 10 - wickets_fallen
      )
  } else {
    # Test: no fixed max balls
    deliveries_df <- deliveries_df %>%
      mutate(
        balls_bowled = over * 6 + ball,
        balls_remaining = NA_real_,
        overs_remaining = NA_real_,
        current_run_rate = calculate_run_rate(total_runs, balls_bowled),
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

  # Venue statistics ----
  cli::cli_h2("Calculating venue statistics")
  venue_stats <- calculate_venue_statistics(
    conn,
    venue_filter = unique(deliveries_df$venue),
    match_type = current_format,
    min_matches = MIN_VENUE_MATCHES
  )

  deliveries_df <- deliveries_df %>%
    left_join(
      venue_stats %>% select(venue, venue_avg_score = avg_first_innings_score,
                             venue_chase_success_rate = chase_success_rate),
      by = "venue"
    )

  overall_avg_score <- mean(first_innings_totals$innings1_total, na.rm = TRUE)
  overall_chase_rate <- mean(matches_df$outcome_winner == matches_df$team2, na.rm = TRUE)

  deliveries_df <- deliveries_df %>%
    mutate(
      venue_avg_score = coalesce(venue_avg_score, overall_avg_score),
      venue_chase_success_rate = coalesce(venue_chase_success_rate, overall_chase_rate)
    )

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
