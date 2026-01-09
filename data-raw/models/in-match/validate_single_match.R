# Validate Win Probability Model on a Single IPL Match ----
#
# This script shows ball-by-ball win probability and expected runs
# for a single match so you can eyeball whether predictions make sense.
#
# Usage: Run interactively in RStudio after loading the package

# Setup ----
library(DBI)
library(dplyr)
library(xgboost)
devtools::load_all()

# Load Models and Data ----
output_dir <- file.path("..", "bouncerdata", "models")

# Load models
stage1_results <- readRDS(file.path(output_dir, "ipl_stage1_results.rds"))
stage2_results <- readRDS(file.path(output_dir, "ipl_stage2_results.rds"))

stage1_model <- stage1_results$model
stage2_model <- stage2_results$model
stage1_feature_cols <- stage1_results$feature_cols
stage2_feature_cols <- stage2_results$feature_cols

# Load data
stage1_data <- readRDS(file.path(output_dir, "ipl_stage1_data.rds"))
stage2_data <- readRDS(file.path(output_dir, "ipl_stage2_data.rds"))
metadata <- readRDS(file.path(output_dir, "ipl_match_metadata.rds"))

# Pick a Match ----
# Use test data so we can compare predictions to actual outcome
# Get available matches
available_matches <- stage2_data$test %>%
  distinct(match_id) %>%
  left_join(metadata$matches %>% select(match_id, match_date, team1, team2, outcome_winner),
            by = "match_id") %>%
  arrange(desc(match_date))

cat("Available test matches:\n")
print(head(available_matches, 10))

# Pick the most recent match (or change this to pick a specific match_id)
selected_match_id <- available_matches$match_id[1]

cat("\n\nSelected match:", selected_match_id, "\n")

# Get Match Info ----
match_info <- metadata$matches %>% filter(match_id == selected_match_id)
cat("\n========================================\n")
cat(match_info$team1, "vs", match_info$team2, "\n")
cat("Date:", as.character(match_info$match_date), "\n")
cat("Venue:", match_info$venue, "\n")
cat("Winner:", match_info$outcome_winner, "\n")
cat("========================================\n\n")

# Get 1st Innings Data and Predictions ----
innings1 <- stage1_data$test %>%
  filter(match_id == selected_match_id) %>%
  arrange(over, ball) %>%
  as.data.frame()  # Convert from data.table to data.frame

if (nrow(innings1) > 0) {
  cat("=== 1ST INNINGS ===\n")
  cat("Batting:", innings1$batting_team[1], "\n")
  cat("Actual Final Score:", innings1$final_innings_total[1], "\n\n")

  # Prepare features for Stage 1 prediction
  s1_features <- innings1 %>%
    mutate(
      phase_powerplay = as.integer(phase == "powerplay"),
      phase_middle = as.integer(phase == "middle"),
      phase_death = as.integer(phase == "death"),
      gender_male = as.integer(gender == "male"),
      wickets_in_hand = 10 - wickets_fallen
    ) %>%
    as.data.frame()

  # Ensure all required columns exist
  for (col in stage1_feature_cols) {
    if (!col %in% names(s1_features)) {
      s1_features[[col]] <- 0
    }
  }

  s1_matrix <- as.matrix(s1_features[, stage1_feature_cols, drop = FALSE])
  s1_matrix[is.na(s1_matrix)] <- 0

  # Get projected score predictions
  innings1$projected_score <- predict(stage1_model, xgb.DMatrix(data = s1_matrix))

  # Calculate expected runs per ball (from model)
  innings1$expected_runs <- NA_real_
  for (i in seq_len(nrow(innings1))) {
    balls_left <- innings1$balls_remaining[i]
    if (balls_left > 0) {
      innings1$expected_runs[i] <- (innings1$projected_score[i] - innings1$total_runs[i]) / balls_left
    }
  }
  innings1$expected_runs <- pmax(0, pmin(innings1$expected_runs, 6))

  # ERA = actual runs - expected
  innings1$era <- innings1$runs_batter - innings1$expected_runs

  # Show key overs (end of each over)
  innings1_summary <- innings1 %>%
    group_by(over) %>%
    slice_tail(n = 1) %>%
    ungroup() %>%
    mutate(
      over_label = paste0("Over ", over + 1),
      score_display = paste0(total_runs, "/", wickets_fallen)
    ) %>%
    select(over_label, score_display, projected_score, current_run_rate)

  cat("Ball-by-ball projected scores (end of each over):\n")
  cat(sprintf("%-10s %-10s %-15s %-12s\n", "Over", "Score", "Projected", "Run Rate"))
  cat(strrep("-", 50), "\n")
  for (i in seq_len(nrow(innings1_summary))) {
    row <- innings1_summary[i, ]
    cat(sprintf("%-10s %-10s %-15.1f %-12.2f\n",
                row$over_label, row$score_display,
                row$projected_score, row$current_run_rate))
  }
  cat("\n")
}

# Get 2nd Innings Data and Predictions ----
innings2 <- stage2_data$test %>%
  filter(match_id == selected_match_id) %>%
  arrange(over, ball) %>%
  as.data.frame()  # Convert from data.table to data.frame

if (nrow(innings2) > 0) {
  cat("=== 2ND INNINGS (CHASE) ===\n")
  cat("Batting:", innings2$batting_team[1], "\n")
  cat("Target:", innings2$target_runs[1], "\n")
  cat("Chase Result:", ifelse(innings2$batting_team_wins[1] == 1, "SUCCESS", "FAILED"), "\n\n")

  # Step 1: Get Stage 1 projected score predictions
  s1_features <- innings2 %>%
    mutate(
      phase_powerplay = as.integer(phase == "powerplay"),
      phase_middle = as.integer(phase == "middle"),
      phase_death = as.integer(phase == "death"),
      gender_male = as.integer(gender == "male"),
      wickets_in_hand = 10 - wickets_fallen
    ) %>%
    as.data.frame()

  for (col in stage1_feature_cols) {
    if (!col %in% names(s1_features)) {
      s1_features[[col]] <- 0
    }
  }

  s1_matrix <- as.matrix(s1_features[, stage1_feature_cols, drop = FALSE])
  s1_matrix[is.na(s1_matrix)] <- 0

  innings2$projected_score <- predict(stage1_model, xgb.DMatrix(data = s1_matrix))

  # Step 2: Add Stage 1 outputs as features for Stage 2
  innings2$projected_vs_target <- innings2$projected_score - innings2$target_runs
  innings2$projected_win_margin <- innings2$projected_score - (innings2$target_runs - 1)

  # Prepare Stage 2 features
  s2_features <- innings2 %>%
    mutate(
      phase_powerplay = as.integer(phase == "powerplay"),
      phase_middle = as.integer(phase == "middle"),
      phase_death = as.integer(phase == "death"),
      gender_male = as.integer(gender == "male"),
      is_dls = as.integer(is_dls_match),
      is_ko = as.integer(is_knockout),
      wickets_in_hand = 10 - wickets_fallen
    ) %>%
    as.data.frame()

  for (col in stage2_feature_cols) {
    if (!col %in% names(s2_features)) {
      s2_features[[col]] <- 0
    }
  }

  s2_matrix <- as.matrix(s2_features[, stage2_feature_cols, drop = FALSE])
  s2_matrix[is.na(s2_matrix)] <- 0
  s2_matrix[is.infinite(s2_matrix)] <- 999

  # Get win probability predictions
  innings2$win_probability <- predict(stage2_model, xgb.DMatrix(data = s2_matrix))

  # Calculate expected runs for ERA
  innings2$expected_runs <- NA_real_
  for (i in seq_len(nrow(innings2))) {
    balls_left <- innings2$balls_remaining[i]
    if (balls_left > 0) {
      innings2$expected_runs[i] <- (innings2$projected_score[i] - innings2$total_runs[i]) / balls_left
    }
  }
  innings2$expected_runs <- pmax(0, pmin(innings2$expected_runs, 6))
  innings2$era <- innings2$runs_batter - innings2$expected_runs

  # Show key overs (end of each over)
  innings2_summary <- innings2 %>%
    group_by(over) %>%
    slice_tail(n = 1) %>%
    ungroup() %>%
    mutate(
      over_label = paste0("Over ", over + 1),
      score_display = paste0(total_runs, "/", wickets_fallen),
      runs_needed_display = target_runs - total_runs
    ) %>%
    select(over_label, score_display, runs_needed_display, win_probability,
           projected_score, required_run_rate)

  cat("Ball-by-ball win probability (end of each over):\n")
  cat(sprintf("%-10s %-10s %-12s %-12s %-15s %-10s\n",
              "Over", "Score", "Runs Needed", "Win Prob %", "Projected", "Req RR"))
  cat(strrep("-", 75), "\n")
  for (i in seq_len(nrow(innings2_summary))) {
    row <- innings2_summary[i, ]
    rr_display <- if (is.infinite(row$required_run_rate) || is.na(row$required_run_rate)) {
      "N/A"
    } else {
      sprintf("%.2f", row$required_run_rate)
    }
    cat(sprintf("%-10s %-10s %-12d %-12.1f %-15.1f %-10s\n",
                row$over_label, row$score_display, row$runs_needed_display,
                row$win_probability * 100, row$projected_score, rr_display))
  }
  cat("\n")

  # Show ball-by-ball detail for a key over (pick death overs if available)
  key_over <- min(18, max(innings2$over))  # Over 19 (0-indexed as 18)
  cat("=== DETAILED BALL-BY-BALL FOR OVER", key_over + 1, "===\n")

  over_detail <- innings2 %>%
    filter(over == key_over) %>%
    mutate(
      ball_label = paste0(over + 1, ".", ball),
      score_display = paste0(total_runs, "/", wickets_fallen),
      outcome = case_when(
        is_wicket ~ "WICKET",
        runs_batter == 6 ~ "SIX",
        runs_batter == 4 ~ "FOUR",
        runs_batter == 0 ~ "dot",
        TRUE ~ paste0(runs_batter)
      )
    ) %>%
    select(ball_label, batter_id, bowler_id, outcome, score_display,
           win_probability, expected_runs, era)

  if (nrow(over_detail) > 0) {
    cat(sprintf("%-8s %-20s %-20s %-8s %-10s %-10s %-10s %-8s\n",
                "Ball", "Batter", "Bowler", "Outcome", "Score", "Win%", "Exp Runs", "ERA"))
    cat(strrep("-", 100), "\n")
    for (i in seq_len(nrow(over_detail))) {
      row <- over_detail[i, ]
      cat(sprintf("%-8s %-20s %-20s %-8s %-10s %-10.1f %-10.2f %+-.2f\n",
                  row$ball_label,
                  substr(row$batter_id, 1, 20),
                  substr(row$bowler_id, 1, 20),
                  row$outcome,
                  row$score_display,
                  row$win_probability * 100,
                  row$expected_runs,
                  row$era))
    }
  }
  cat("\n")

  # Show highest impact balls (biggest WPA swings)
  cat("=== TOP 10 HIGHEST IMPACT BALLS (biggest win prob changes) ===\n")

  innings2_with_wpa <- innings2 %>%
    arrange(over, ball) %>%
    mutate(
      prev_win_prob = lag(win_probability, default = 0.5),  # Start at 50%
      wpa = win_probability - prev_win_prob,
      ball_label = paste0(over + 1, ".", ball),
      score_display = paste0(total_runs, "/", wickets_fallen),
      outcome = case_when(
        is_wicket ~ "WICKET",
        runs_batter == 6 ~ "SIX",
        runs_batter == 4 ~ "FOUR",
        runs_batter == 0 ~ "dot",
        TRUE ~ paste0(runs_batter)
      )
    ) %>%
    arrange(desc(abs(wpa))) %>%
    head(10) %>%
    select(ball_label, batter_id, bowler_id, outcome, wpa, win_probability)

  cat(sprintf("%-8s %-20s %-20s %-10s %-12s %-10s\n",
              "Ball", "Batter", "Bowler", "Outcome", "WPA", "Win%"))
  cat(strrep("-", 85), "\n")
  for (i in seq_len(nrow(innings2_with_wpa))) {
    row <- innings2_with_wpa[i, ]
    cat(sprintf("%-8s %-20s %-20s %-10s %+-.4f %-10.1f\n",
                row$ball_label,
                substr(row$batter_id, 1, 20),
                substr(row$bowler_id, 1, 20),
                row$outcome,
                row$wpa,
                row$win_probability * 100))
  }
}

cat("\n=== VALIDATION COMPLETE ===\n")
cat("Check if:\n")
cat("1. Win probability starts around 50% and moves toward 100% (chase success) or 0% (chase failed)\n")
cat("2. Projected score converges toward actual final score as innings progresses\n")
cat("3. Wickets cause big negative WPA swings, boundaries cause positive swings\n")
cat("4. Required run rate increasing causes win probability to drop\n")
cat("5. ERA is positive for boundaries, negative for dots in pressure situations\n")
