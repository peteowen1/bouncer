# IPL Data Preparation for Win Probability Modeling ----
#
# This script prepares IPL match data for the two-stage win probability model:
#   Stage 1: Projected score model (1st innings)
#   Stage 2: Win probability model (2nd innings)
#
# Output:
#   - bouncerdata/models/ipl_stage1_data.rds (1st innings deliveries + features)
#   - bouncerdata/models/ipl_stage2_data.rds (2nd innings deliveries + features)
#   - bouncerdata/models/ipl_venue_stats.rds (venue statistics)
#   - bouncerdata/models/ipl_match_metadata.rds (match-level info for reference)

# Setup ----
library(DBI)
library(dplyr)
library(tidyr)
devtools::load_all()

# Note: Utility functions are now in the package:
#   - R/feature_engineering.R (rolling features, phase, venue stats, pressure metrics)
#   - R/match_outcomes.R (DLS, ties, super overs, outcome classification)
# They are loaded via devtools::load_all() above

# Configuration ----
RANDOM_SEED <- 42
EVENT_FILTER <- "Indian Premier League"  # Filter for IPL matches
MATCH_TYPE <- "t20"
TEST_SEASONS <- c("2023", "2024", "2023/24", "2024/25")  # Hold out recent seasons (various formats)
MIN_VENUE_MATCHES <- 5  # Minimum matches for venue statistics

# Output directory for model files
output_dir <- file.path("..", "bouncerdata", "models")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

cat("\n")
cli::cli_h1("IPL Data Preparation for Win Probability Modeling")
cat("\n")

# Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cli::cli_alert_success("Connected to database")

# Match Outcome Summary ----
cli::cli_h2("Match Outcome Summary")
outcome_summary <- get_outcome_summary(conn, event_filter = EVENT_FILTER, match_type = MATCH_TYPE)

# Load Match Data ----
cli::cli_h2("Loading match data")

matches_query <- "
  SELECT
    match_id,
    season,
    match_type,
    match_date,
    venue,
    city,
    gender,
    team1,
    team2,
    toss_winner,
    toss_decision,
    outcome_type,
    outcome_winner,
    outcome_by_runs,
    outcome_by_wickets,
    outcome_method,
    event_name,
    event_match_number,
    event_group
  FROM cricsheet.matches
  WHERE event_name LIKE ?
    AND LOWER(match_type) = ?
  ORDER BY match_date, match_id
"

matches_df <- DBI::dbGetQuery(conn, matches_query, params = list(
  paste0("%", EVENT_FILTER, "%"),
  MATCH_TYPE
))

cli::cli_alert_success("Loaded {nrow(matches_df)} IPL matches")

# Classify match outcomes
matches_df <- classify_match_outcomes(matches_df)

# Diagnostic: check outcome_winner values
cli::cli_h3("Match Outcome Diagnostics")
cli::cli_alert_info("Total matches: {nrow(matches_df)}")
cli::cli_alert_info("Matches with outcome_winner: {sum(!is.na(matches_df$outcome_winner) & matches_df$outcome_winner != '')}")
cli::cli_alert_info("Matches valid for training: {sum(matches_df$is_valid_for_training, na.rm = TRUE)}")
cli::cli_alert_info("DLS matches: {sum(matches_df$is_dls_match, na.rm = TRUE)}")
cli::cli_alert_info("Super over matches: {sum(matches_df$is_super_over, na.rm = TRUE)}")
cli::cli_alert_info("Pure ties (excluded): {sum(matches_df$is_pure_tie, na.rm = TRUE)}")
cli::cli_alert_info("No results (excluded): {sum(matches_df$is_no_result, na.rm = TRUE)}")

# Get valid match IDs for training
valid_match_ids <- matches_df %>%
  filter(is_valid_for_training) %>%
  pull(match_id)

# Load Innings Totals ----
cli::cli_h2("Loading innings totals")

innings_query <- "
  SELECT
    match_id,
    innings,
    batting_team,
    bowling_team,
    total_runs as innings_total,
    total_wickets as innings_wickets,
    total_overs as innings_overs
  FROM cricsheet.match_innings
  WHERE match_id IN (
    SELECT match_id FROM cricsheet.matches
    WHERE event_name LIKE ?
      AND LOWER(match_type) = ?
  )
  ORDER BY match_id, innings
"

innings_df <- DBI::dbGetQuery(conn, innings_query, params = list(
  paste0("%", EVENT_FILTER, "%"),
  MATCH_TYPE
))

cli::cli_alert_success("Loaded innings data for {length(unique(innings_df$match_id))} matches")

# Calculate first innings totals for each match
first_innings_totals <- innings_df %>%
  filter(innings == 1) %>%
  select(match_id, innings1_total = innings_total, innings1_wickets = innings_wickets,
         innings1_overs = innings_overs, innings1_batting_team = batting_team)

# Load Deliveries Data ----
cli::cli_h2("Loading delivery data")

# This query gets all ball-by-ball data with cumulative scores
deliveries_query <- "
  SELECT
    d.delivery_id,
    d.match_id,
    d.season,
    d.match_type,
    d.match_date,
    d.venue,
    d.city,
    d.gender,
    d.batting_team,
    d.bowling_team,
    d.innings,
    d.over,
    d.ball,
    d.over_ball,
    d.batter_id,
    d.bowler_id,
    d.runs_batter,
    d.runs_extras,
    d.runs_total,
    d.is_boundary,
    d.is_four,
    d.is_six,
    d.is_wicket,
    d.wicket_kind,
    d.total_runs,
    -- FIX: wickets_fallen in Cricsheet is AFTER the delivery, so subtract is_wicket
    -- to get the count BEFORE this delivery (prevents data leakage)
    (d.wickets_fallen - CAST(d.is_wicket AS INT)) AS wickets_fallen,
    d.batter_elo_before,
    d.bowler_elo_before
  FROM cricsheet.deliveries d
  WHERE d.match_id IN (
    SELECT match_id FROM cricsheet.matches
    WHERE event_name LIKE ?
      AND LOWER(match_type) = ?
  )
  ORDER BY d.match_date, d.match_id, d.innings, d.over, d.ball
"

deliveries_df <- DBI::dbGetQuery(conn, deliveries_query, params = list(
  paste0("%", EVENT_FILTER, "%"),
  MATCH_TYPE
))

cli::cli_alert_success("Loaded {nrow(deliveries_df)} deliveries from {length(unique(deliveries_df$match_id))} matches")

# Join Match Info to Deliveries ----
cli::cli_h2("Joining match information to deliveries")

# Join match outcome info
deliveries_df <- deliveries_df %>%
  left_join(
    matches_df %>% select(match_id, outcome_type, outcome_winner, outcome_method,
                          is_dls_match, is_super_over, is_pure_tie, is_no_result,
                          is_knockout, is_valid_for_training, event_match_number),
    by = "match_id"
  )

# Join first innings totals (for 2nd innings use)
deliveries_df <- deliveries_df %>%
  left_join(
    first_innings_totals,
    by = "match_id"
  )

cli::cli_alert_success("Joined match and innings information")

# Join Skill Indices ----
cli::cli_h2("Adding player skill indices")

# Add skill features (batter_scoring_index, bowler_economy_index, etc.)
deliveries_df <- add_skill_features(deliveries_df, format = "t20", conn = conn, fill_missing = TRUE)

n_with_skills <- sum(!is.na(deliveries_df$batter_scoring_index))
cli::cli_alert_success("Added skill indices: {n_with_skills}/{nrow(deliveries_df)} deliveries matched")

# Diagnostic: check before filtering
cli::cli_h3("Pre-filter Diagnostics")
cli::cli_alert_info("Deliveries before filtering: {nrow(deliveries_df)}")
cli::cli_alert_info("Deliveries with is_valid_for_training=TRUE: {sum(deliveries_df$is_valid_for_training, na.rm = TRUE)}")
cli::cli_alert_info("Deliveries with is_valid_for_training=FALSE: {sum(!deliveries_df$is_valid_for_training, na.rm = TRUE)}")
cli::cli_alert_info("Deliveries with is_valid_for_training=NA: {sum(is.na(deliveries_df$is_valid_for_training))}")

# Filter to valid matches only
deliveries_df <- deliveries_df %>%
  filter(is_valid_for_training == TRUE)

cli::cli_alert_info("Deliveries after filtering: {nrow(deliveries_df)}")

# Calculate Features ----
cli::cli_h2("Calculating features")

# Add phase features
cli::cli_alert_info("Calculating phase features...")
phase_features <- calculate_phase_features(
  over = deliveries_df$over,
  ball = deliveries_df$ball,
  match_type = "t20"
)

deliveries_df <- bind_cols(deliveries_df, phase_features)

# Calculate current run rate
cli::cli_alert_info("Calculating run rates...")
deliveries_df <- deliveries_df %>%
  mutate(
    current_run_rate = calculate_run_rate(total_runs, balls_bowled),
    wickets_in_hand = 10 - wickets_fallen
  )

# Calculate rolling features (this may take a moment)
cli::cli_alert_info("Calculating rolling features (this may take a moment)...")
deliveries_df <- calculate_rolling_features(
  deliveries_df,
  ball_windows = c(12, 24),
  over_windows = c(3, 6)
)

cli::cli_alert_success("Feature calculation complete")

# Add Expected Outcome Features ----
cli::cli_h2("Adding expected outcome features")

# Calculate runs_difference for the outcome model
# For 1st innings: runs_difference = total_runs (bowling team hasn't batted)
# For 2nd innings: runs_difference = total_runs - innings1_total
deliveries_df <- deliveries_df %>%
  mutate(
    runs_difference = case_when(
      innings == 1 ~ total_runs,
      innings == 2 ~ total_runs - innings1_total,
      TRUE ~ total_runs
    )
  )

# Load the outcome model and add expected outcome features
outcome_model_path <- file.path(output_dir, "xgb_outcome_shortform.ubj")
if (file.exists(outcome_model_path)) {
  cli::cli_alert_info("Loading XGBoost outcome model...")
  outcome_model <- xgboost::xgb.load(outcome_model_path)

  deliveries_df <- add_expected_outcome_features(
    deliveries_df,
    model = outcome_model,
    model_type = "xgb",
    format = "shortform"
  )
  HAS_EXPECTED_OUTCOMES <- TRUE
} else {
  cli::cli_alert_warning("Outcome model not found at {outcome_model_path}")
  cli::cli_alert_info("Run outcome-models/model_xgb_outcome_shortform.R first")
  cli::cli_alert_info("Proceeding without expected outcome features")
  HAS_EXPECTED_OUTCOMES <- FALSE
}

# Calculate Venue Statistics ----
cli::cli_h2("Calculating venue statistics")

venue_stats <- calculate_venue_statistics(
  conn,
  venue_filter = unique(deliveries_df$venue),
  match_type = MATCH_TYPE,
  min_matches = MIN_VENUE_MATCHES
)

cli::cli_alert_success("Calculated statistics for {nrow(venue_stats)} venues")

# Join venue stats to deliveries
deliveries_df <- deliveries_df %>%
  left_join(
    venue_stats %>% select(venue, venue_avg_score = avg_first_innings_score,
                           venue_chase_success_rate = chase_success_rate),
    by = "venue"
  )

# Fill NA venue stats with overall average
overall_avg_score <- mean(first_innings_totals$innings1_total, na.rm = TRUE)
overall_chase_rate <- mean(matches_df$outcome_winner == matches_df$team2, na.rm = TRUE)

deliveries_df <- deliveries_df %>%
  mutate(
    venue_avg_score = coalesce(venue_avg_score, overall_avg_score),
    venue_chase_success_rate = coalesce(venue_chase_success_rate, overall_chase_rate)
  )

# Load Baseline Model (if available) ----
cli::cli_h2("Loading baseline projected score model")

baseline_path <- file.path(output_dir, "ipl_baseline_projected_score.rds")
if (file.exists(baseline_path)) {
  baseline_model <- readRDS(baseline_path)
  cli::cli_alert_success("Loaded baseline model")
  HAS_BASELINE <- TRUE
} else {
  cli::cli_alert_warning("Baseline model not found - run 02_baseline_projected_score.R first")
  cli::cli_alert_info("Proceeding without baseline features")
  HAS_BASELINE <- FALSE
}

# Separate Stage 1 and Stage 2 Data ----
cli::cli_h2("Separating Stage 1 and Stage 2 data")

# Stage 1: First innings only
# Target: Final innings total (need to join this)
# Also add: batting_first_wins (win label for innings 1)
stage1_data <- deliveries_df %>%
  filter(innings == 1) %>%
  left_join(
    innings_df %>% filter(innings == 1) %>%
      select(match_id, final_innings_total = innings_total),
    by = "match_id"
  ) %>%
  # Add win label: did the team batting first win?
  mutate(
    batting_first_wins = as.integer(outcome_winner == batting_team)
  )

# Add baseline features if available
if (HAS_BASELINE) {
  cli::cli_alert_info("Adding baseline projected score features...")

  # Calculate match-level baseline features
  match_baseline <- matches_df %>%
    mutate(
      is_knockout = as.integer(
        grepl("final|eliminator|qualifier", tolower(coalesce(as.character(event_match_number), ""))) |
        grepl("final|eliminator|qualifier", tolower(coalesce(as.character(event_group), "")))
      ),
      chose_to_bat = as.integer(toss_decision == "bat"),
      season_numeric = as.numeric(gsub("/.*", "", season))
    ) %>%
    left_join(baseline_model$venue_stats %>% select(venue, venue_avg_score_baseline = venue_avg_score), by = "venue") %>%
    mutate(
      venue_avg_for_baseline = coalesce(venue_avg_score_baseline, baseline_model$overall_avg),
      baseline_projected_score = venue_avg_for_baseline +
        (chose_to_bat * baseline_model$toss_bat_adjustment) +
        (is_knockout * baseline_model$knockout_adjustment) +
        ((season_numeric - baseline_model$season_baseline) * baseline_model$season_slope)
    ) %>%
    select(match_id, baseline_projected_score)

  stage1_data <- stage1_data %>%
    left_join(match_baseline, by = "match_id") %>%
    mutate(
      # How far above/below baseline is our current projected score
      projected_vs_baseline = ifelse(!is.na(baseline_projected_score),
                                      venue_avg_score - baseline_projected_score, 0)
    )
}

cli::cli_alert_success("Stage 1 (1st innings): {nrow(stage1_data)} deliveries from {length(unique(stage1_data$match_id))} matches")

# Check innings 1 win labels
innings1_win_rate <- mean(stage1_data$batting_first_wins, na.rm = TRUE)
cli::cli_alert_info("Batting first win rate: {round(innings1_win_rate * 100, 1)}%")

# Stage 2: Second innings only
# Need to add target and pressure metrics
stage2_data <- deliveries_df %>%
  filter(innings == 2) %>%
  mutate(
    # Target is first innings total + 1
    target_runs = innings1_total + 1,
    # Calculate first innings run rate
    innings1_run_rate = calculate_run_rate(innings1_total, innings1_overs * 6)
  )

# Add pressure metrics
cli::cli_alert_info("Calculating pressure metrics for Stage 2...")
pressure_metrics <- calculate_pressure_metrics(
  target = stage2_data$target_runs,
  current_runs = stage2_data$total_runs,
  current_wickets = stage2_data$wickets_fallen,
  balls_remaining = stage2_data$balls_remaining,
  current_run_rate = stage2_data$current_run_rate
)

stage2_data <- bind_cols(stage2_data, pressure_metrics)

# Add win labels
stage2_data <- prepare_win_labels(stage2_data, target_column = "batting_team")

cli::cli_alert_success("Stage 2 (2nd innings): {nrow(stage2_data)} deliveries from {length(unique(stage2_data$match_id))} matches")

# Check for any NA in target variable
na_labels <- sum(is.na(stage2_data$batting_team_wins))
if (na_labels > 0) {
  cli::cli_alert_warning("{na_labels} deliveries with NA win labels (will be excluded)")
  stage2_data <- stage2_data %>% filter(!is.na(batting_team_wins))
}

# Create Train/Test Splits ----
cli::cli_h2("Creating train/test splits by season")

# Diagnostic: check season values
cli::cli_h3("Season Diagnostics")
cli::cli_alert_info("Unique seasons in Stage 1 data: {paste(unique(stage1_data$season), collapse = ', ')}")
cli::cli_alert_info("TEST_SEASONS: {paste(TEST_SEASONS, collapse = ', ')}")
cli::cli_alert_info("Matches in test seasons: {sum(stage1_data$season %in% TEST_SEASONS)}")

# Stage 1 splits
stage1_train <- stage1_data %>% filter(!season %in% TEST_SEASONS)
stage1_test <- stage1_data %>% filter(season %in% TEST_SEASONS)

cli::cli_alert_info("Stage 1 Train: {nrow(stage1_train)} deliveries ({length(unique(stage1_train$match_id))} matches)")
cli::cli_alert_info("Stage 1 Test: {nrow(stage1_test)} deliveries ({length(unique(stage1_test$match_id))} matches)")

# Stage 2 splits
stage2_train <- stage2_data %>% filter(!season %in% TEST_SEASONS)
stage2_test <- stage2_data %>% filter(season %in% TEST_SEASONS)

cli::cli_alert_info("Stage 2 Train: {nrow(stage2_train)} deliveries ({length(unique(stage2_train$match_id))} matches)")
cli::cli_alert_info("Stage 2 Test: {nrow(stage2_test)} deliveries ({length(unique(stage2_test$match_id))} matches)")

# Summary Statistics ----
cli::cli_h2("Summary Statistics")

# Stage 1 target distribution
cli::cli_h3("Stage 1: First Innings Totals")
cat(sprintf("  Mean: %.1f\n", mean(stage1_train$final_innings_total, na.rm = TRUE)))
cat(sprintf("  SD: %.1f\n", sd(stage1_train$final_innings_total, na.rm = TRUE)))
cat(sprintf("  Min: %d\n", min(stage1_train$final_innings_total, na.rm = TRUE)))
cat(sprintf("  Max: %d\n", max(stage1_train$final_innings_total, na.rm = TRUE)))

# Stage 2 win rate distribution
cli::cli_h3("Stage 2: Win Rates")
overall_win_rate <- mean(stage2_train$batting_team_wins, na.rm = TRUE)
cat(sprintf("  Overall chase success rate: %.1f%%\n", overall_win_rate * 100))

# Save Prepared Data ----
cli::cli_h2("Saving prepared data")

# Save Stage 1 data
stage1_path <- file.path(output_dir, "ipl_stage1_data.rds")
saveRDS(
  list(
    train = stage1_train,
    test = stage1_test,
    all = stage1_data
  ),
  stage1_path
)
cli::cli_alert_success("Saved Stage 1 data to {stage1_path}")

# Save Stage 2 data
stage2_path <- file.path(output_dir, "ipl_stage2_data.rds")
saveRDS(
  list(
    train = stage2_train,
    test = stage2_test,
    all = stage2_data
  ),
  stage2_path
)
cli::cli_alert_success("Saved Stage 2 data to {stage2_path}")

# Save venue statistics
venue_path <- file.path(output_dir, "ipl_venue_stats.rds")
saveRDS(venue_stats, venue_path)
cli::cli_alert_success("Saved venue statistics to {venue_path}")

# Save match metadata
metadata_path <- file.path(output_dir, "ipl_match_metadata.rds")
saveRDS(
  list(
    matches = matches_df,
    innings = innings_df,
    outcome_summary = outcome_summary,
    config = list(
      event_filter = EVENT_FILTER,
      match_type = MATCH_TYPE,
      test_seasons = TEST_SEASONS,
      min_venue_matches = MIN_VENUE_MATCHES,
      created_at = Sys.time()
    )
  ),
  metadata_path
)
cli::cli_alert_success("Saved match metadata to {metadata_path}")

# Done ----
cat("\n")
cli::cli_alert_success("IPL data preparation complete!")
cat("\n")

cli::cli_h3("Next Steps")
cli::cli_bullets(c(

"i" = "Run 02_baseline_projected_score.R for venue baseline stats",
  "i" = "Run 03_projected_score_model.R to train the projected score model",
  "i" = "Run 04_win_probability_innings1.R and 05_win_probability_innings2.R for win probability",
  "i" = "Data files saved to: {output_dir}"
))

cat("\n")
cli::cli_h3("Data Summary")
cat(sprintf("  Stage 1 (1st innings): %d train / %d test deliveries\n",
            nrow(stage1_train), nrow(stage1_test)))
cat(sprintf("  Stage 2 (2nd innings): %d train / %d test deliveries\n",
            nrow(stage2_train), nrow(stage2_test)))
cat(sprintf("  Venues with statistics: %d\n", nrow(venue_stats)))
cat(sprintf("  Test seasons held out: %s\n", paste(TEST_SEASONS, collapse = ", ")))
cat("\n")
