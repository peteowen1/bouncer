# WPA/ERA Analysis for IPL T20 Matches ----
#
# This script demonstrates Win Probability Added (WPA) and Expected Runs Added (ERA)
# metrics using the Stage 1 and Stage 2 models.
#
# WPA measures the change in win probability (or projected score) caused by each delivery.
# ERA measures runs scored above/below expectation for the match situation.
#
# Outputs:
#   - Top batters by WPA and ERA
#   - Top bowlers by WPA and ERA
#   - Match-level WPA analysis examples

# Setup ----
library(DBI)
library(dplyr)
library(xgboost)
devtools::load_all()

# Note: Feature engineering functions are now in R/feature_engineering.R
# They are loaded via devtools::load_all() above

cat("\n")
cli::cli_h1("WPA/ERA Analysis for IPL T20 Matches")
cat("\n")

# Load Models and Data ----
cli::cli_h2("Loading models and data")

output_dir <- file.path("..", "bouncerdata", "models")

# Load Stage 1 model (Projected Score)
stage1_results <- readRDS(file.path(output_dir, "ipl_stage1_results.rds"))
stage1_model <- stage1_results$model
stage1_feature_cols <- stage1_results$feature_cols
cli::cli_alert_success("Loaded Stage 1 model ({length(stage1_feature_cols)} features)")

# Load Stage 2 model (Win Probability)
stage2_results <- readRDS(file.path(output_dir, "ipl_stage2_results.rds"))
stage2_model <- stage2_results$model
stage2_feature_cols <- stage2_results$feature_cols
cli::cli_alert_success("Loaded Stage 2 model ({length(stage2_feature_cols)} features)")

# Load test data
stage1_data <- readRDS(file.path(output_dir, "ipl_stage1_data.rds"))
stage2_data <- readRDS(file.path(output_dir, "ipl_stage2_data.rds"))
metadata <- readRDS(file.path(output_dir, "ipl_match_metadata.rds"))

# Combine test data from both innings
innings1_test <- stage1_data$test
innings2_test <- stage2_data$test

cli::cli_alert_success("Loaded test data: {nrow(innings1_test)} 1st innings deliveries, {nrow(innings2_test)} 2nd innings deliveries")

# Calculate WPA for 1st Innings ----
cli::cli_h2("Calculating WPA for 1st Innings (Projected Score Change)")

innings1_wpa <- calculate_delivery_wpa(
  deliveries = innings1_test,
  stage1_model = stage1_model,
  stage2_model = stage2_model,
  stage1_feature_cols = stage1_feature_cols,
  stage2_feature_cols = stage2_feature_cols
)

cli::cli_alert_success("Calculated WPA for {nrow(innings1_wpa)} 1st innings deliveries")
cli::cli_alert_info("WPA range: {round(min(innings1_wpa$wpa, na.rm = TRUE), 2)} to {round(max(innings1_wpa$wpa, na.rm = TRUE), 2)} runs")
cli::cli_alert_info("Mean WPA: {round(mean(innings1_wpa$wpa, na.rm = TRUE), 4)} runs")

# Calculate WPA for 2nd Innings ----
cli::cli_h2("Calculating WPA for 2nd Innings (Win Probability Change)")

innings2_wpa <- calculate_delivery_wpa(
  deliveries = innings2_test,
  stage1_model = stage1_model,
  stage2_model = stage2_model,
  stage1_feature_cols = stage1_feature_cols,
  stage2_feature_cols = stage2_feature_cols
)

cli::cli_alert_success("Calculated WPA for {nrow(innings2_wpa)} 2nd innings deliveries")
cli::cli_alert_info("WPA range: {round(min(innings2_wpa$wpa, na.rm = TRUE), 4)} to {round(max(innings2_wpa$wpa, na.rm = TRUE), 4)} probability")
cli::cli_alert_info("Mean WPA: {round(mean(innings2_wpa$wpa, na.rm = TRUE), 6)} probability")

# Calculate ERA for All Deliveries ----
cli::cli_h2("Calculating ERA (Expected Runs Added)")

# Calculate ERA for 1st innings
innings1_era <- calculate_delivery_era(
  deliveries = innings1_test,
  stage1_model = stage1_model,
  stage1_feature_cols = stage1_feature_cols
)

# Calculate ERA for 2nd innings
innings2_era <- calculate_delivery_era(
  deliveries = innings2_test,
  stage1_model = stage1_model,
  stage1_feature_cols = stage1_feature_cols
)

cli::cli_alert_success("Calculated ERA for all deliveries")
cli::cli_alert_info("1st innings ERA range: {round(min(innings1_era$era, na.rm = TRUE), 2)} to {round(max(innings1_era$era, na.rm = TRUE), 2)}")
cli::cli_alert_info("2nd innings ERA range: {round(min(innings2_era$era, na.rm = TRUE), 2)} to {round(max(innings2_era$era, na.rm = TRUE), 2)}")

# Combine WPA and ERA data
innings1_combined <- innings1_wpa
innings1_combined$expected_runs <- innings1_era$expected_runs
innings1_combined$era <- innings1_era$era
innings1_combined$batter_era <- innings1_era$batter_era
innings1_combined$bowler_era <- innings1_era$bowler_era

innings2_combined <- innings2_wpa
innings2_combined$expected_runs <- innings2_era$expected_runs
innings2_combined$era <- innings2_era$era
innings2_combined$batter_era <- innings2_era$batter_era
innings2_combined$bowler_era <- innings2_era$bowler_era

all_wpa_era <- dplyr::bind_rows(innings1_combined, innings2_combined)
cli::cli_alert_success("Combined data: {nrow(all_wpa_era)} total deliveries")

# Top Batters by WPA ----
cli::cli_h2("Top Batters by Win Probability Added")

batter_wpa <- calculate_player_wpa(all_wpa_era, role = "batting")

cli::cli_h3("Top 15 Batters by Total WPA")
top_batters_wpa <- batter_wpa %>%
  arrange(desc(total_wpa)) %>%
  head(15) %>%
  select(player_id, matches, deliveries, total_wpa, wpa_per_delivery, clutch_moments)

print(top_batters_wpa)
cat("\n")

cli::cli_h3("Top 15 Batters by WPA per Delivery (min 50 deliveries)")
efficient_batters <- batter_wpa %>%
  filter(deliveries >= 50) %>%
  arrange(desc(wpa_per_delivery)) %>%
  head(15) %>%
  select(player_id, matches, deliveries, total_wpa, wpa_per_delivery, positive_wpa_pct)

print(efficient_batters)
cat("\n")

# Top Bowlers by WPA ----
cli::cli_h2("Top Bowlers by Win Probability Added")

bowler_wpa <- calculate_player_wpa(all_wpa_era, role = "bowling")

# Note: For bowlers, positive WPA means good (opponent's win probability decreased)
cli::cli_h3("Top 15 Bowlers by Total WPA (positive = good for bowler)")
top_bowlers_wpa <- bowler_wpa %>%
  arrange(desc(total_wpa)) %>%
  head(15) %>%
  select(player_id, matches, deliveries, total_wpa, wpa_per_delivery, clutch_moments)

print(top_bowlers_wpa)
cat("\n")

cli::cli_h3("Top 15 Bowlers by WPA per Delivery (min 50 deliveries)")
efficient_bowlers <- bowler_wpa %>%
  filter(deliveries >= 50) %>%
  arrange(desc(wpa_per_delivery)) %>%
  head(15) %>%
  select(player_id, matches, deliveries, total_wpa, wpa_per_delivery, positive_wpa_pct)

print(efficient_bowlers)
cat("\n")

# Top Batters by ERA ----
cli::cli_h2("Top Batters by Expected Runs Added")

batter_era <- calculate_player_era(all_wpa_era, role = "batting")

cli::cli_h3("Top 15 Batters by Total ERA (runs above expectation)")
top_batters_era <- batter_era %>%
  arrange(desc(total_era)) %>%
  head(15) %>%
  select(player_id, matches, deliveries, total_era, era_per_delivery)

print(top_batters_era)
cat("\n")

# Top Bowlers by ERA ----
cli::cli_h2("Top Bowlers by Expected Runs Added")

bowler_era <- calculate_player_era(all_wpa_era, role = "bowling")

# Note: For bowlers, negative ERA means good (conceded less than expected)
cli::cli_h3("Top 15 Bowlers by Total ERA (negative = good for bowler)")
top_bowlers_era <- bowler_era %>%
  arrange(total_era) %>%  # Ascending - most negative first

  head(15) %>%
  select(player_id, matches, deliveries, total_era, era_per_delivery)

print(top_bowlers_era)
cat("\n")

# Match-Level Analysis ----
cli::cli_h2("Match-Level WPA Analysis")

# Calculate match-level WPA
match_wpa <- calculate_match_wpa(all_wpa_era)

# Get sample matches for detailed analysis
sample_matches <- unique(innings2_test$match_id)[1:3]

for (mid in sample_matches) {
  match_info <- metadata$matches %>% filter(match_id == mid)
  match_deliveries <- all_wpa_era %>% filter(match_id == mid)

  cli::cli_h3("Match: {mid}")
  cat(sprintf("  %s vs %s\n", match_info$team1, match_info$team2))
  cat(sprintf("  Winner: %s\n", match_info$outcome_winner))

  # Top WPA contributors in this match
  match_player_wpa <- match_wpa %>%
    filter(match_id == mid) %>%
    arrange(desc(abs(total_wpa))) %>%
    head(6)

  cat("\n  Top WPA contributors:\n")
  for (i in seq_len(nrow(match_player_wpa))) {
    row <- match_player_wpa[i, ]
    cat(sprintf("    %s (%s): %.4f WPA\n",
                row$player_id, row$role, row$total_wpa))
  }

  # Key moments (highest absolute WPA deliveries)
  key_moments <- match_deliveries %>%
    arrange(desc(abs(wpa))) %>%
    head(5) %>%
    select(over, ball, batter_id, bowler_id, runs_total, is_wicket, wpa)

  cat("\n  Key moments (highest |WPA|):\n")
  for (i in seq_len(nrow(key_moments))) {
    row <- key_moments[i, ]
    outcome <- if (row$is_wicket) "WICKET" else paste0(row$runs_total, " runs")
    cat(sprintf("    Over %d.%d: %s vs %s - %s (WPA: %+.4f)\n",
                row$over, row$ball, row$batter_id, row$bowler_id,
                outcome, row$wpa))
  }
  cat("\n")
}

# Player Outperformance vs Expected Outcomes ----
cli::cli_h2("Player Outperformance vs Expected Ball Outcomes")

# Check if expected outcome features are available
if ("runs_above_expected" %in% names(innings1_test) || "runs_above_expected" %in% names(innings2_test)) {
  cli::cli_alert_success("Expected outcome features found in data")

  # Combine all deliveries with expected outcome features
  all_deliveries <- bind_rows(
    innings1_test %>% mutate(innings_type = "1st"),
    innings2_test %>% mutate(innings_type = "2nd")
  )

  # Batter outperformance (runs above expected)
  cli::cli_h3("Top Batters: Runs Above Expected (vs Match State)")
  batter_outperformance <- all_deliveries %>%
    filter(!is.na(runs_above_expected)) %>%
    group_by(batter_id) %>%
    summarise(
      deliveries = n(),
      total_runs = sum(runs_batter, na.rm = TRUE),
      total_exp_runs = sum(exp_runs, na.rm = TRUE),
      total_runs_above_exp = sum(runs_above_expected, na.rm = TRUE),
      runs_above_exp_per_ball = mean(runs_above_expected, na.rm = TRUE),
      actual_sr = mean(runs_batter, na.rm = TRUE) * 100,
      expected_sr = mean(exp_runs, na.rm = TRUE) * 100,
      .groups = "drop"
    ) %>%
    filter(deliveries >= 50) %>%
    arrange(desc(total_runs_above_exp))

  top_batter_outperformers <- head(batter_outperformance, 15)
  cat("\nTop 15 batters by total runs above expected (min 50 deliveries):\n")
  print(top_batter_outperformers %>%
          select(batter_id, deliveries, total_runs, total_runs_above_exp,
                 runs_above_exp_per_ball, actual_sr, expected_sr))
  cat("\n")

  # Batter underperformers
  cli::cli_h3("Bottom Batters: Runs Below Expected (vs Match State)")
  bottom_batter_performers <- tail(batter_outperformance, 15)
  cat("\nBottom 15 batters by runs above expected (min 50 deliveries):\n")
  print(bottom_batter_performers %>%
          arrange(total_runs_above_exp) %>%
          select(batter_id, deliveries, total_runs, total_runs_above_exp,
                 runs_above_exp_per_ball, actual_sr, expected_sr))
  cat("\n")

  # Bowler outperformance (conceding less than expected = good)
  cli::cli_h3("Top Bowlers: Runs Below Expected (vs Match State)")
  bowler_outperformance <- all_deliveries %>%
    filter(!is.na(runs_above_expected)) %>%
    group_by(bowler_id) %>%
    summarise(
      deliveries = n(),
      total_runs_conceded = sum(runs_batter, na.rm = TRUE),
      total_exp_runs = sum(exp_runs, na.rm = TRUE),
      total_runs_saved = -sum(runs_above_expected, na.rm = TRUE),  # Negative = good for bowler
      runs_saved_per_ball = -mean(runs_above_expected, na.rm = TRUE),
      actual_economy = mean(runs_batter, na.rm = TRUE) * 6,  # Per over
      expected_economy = mean(exp_runs, na.rm = TRUE) * 6,
      .groups = "drop"
    ) %>%
    filter(deliveries >= 50) %>%
    arrange(desc(total_runs_saved))

  top_bowler_outperformers <- head(bowler_outperformance, 15)
  cat("\nTop 15 bowlers by runs saved vs expected (min 50 deliveries):\n")
  print(top_bowler_outperformers %>%
          select(bowler_id, deliveries, total_runs_conceded, total_runs_saved,
                 runs_saved_per_ball, actual_economy, expected_economy))
  cat("\n")

  # Wicket taker outperformance (if available)
  if ("wicket_above_expected" %in% names(all_deliveries)) {
    cli::cli_h3("Top Bowlers: Wickets Above Expected")
    bowler_wicket_performance <- all_deliveries %>%
      filter(!is.na(wicket_above_expected)) %>%
      group_by(bowler_id) %>%
      summarise(
        deliveries = n(),
        total_wickets = sum(is_wicket, na.rm = TRUE),
        total_exp_wickets = sum(exp_wicket_prob, na.rm = TRUE),
        total_wickets_above_exp = sum(wicket_above_expected, na.rm = TRUE),
        wicket_rate = mean(is_wicket, na.rm = TRUE) * 100,
        expected_wicket_rate = mean(exp_wicket_prob, na.rm = TRUE) * 100,
        .groups = "drop"
      ) %>%
      filter(deliveries >= 50) %>%
      arrange(desc(total_wickets_above_exp))

    top_wicket_takers <- head(bowler_wicket_performance, 15)
    cat("\nTop 15 bowlers by wickets above expected (min 50 deliveries):\n")
    print(top_wicket_takers %>%
            select(bowler_id, deliveries, total_wickets, total_wickets_above_exp,
                   wicket_rate, expected_wicket_rate))
    cat("\n")
  }

  # Save outperformance data
  outperformance_results <- list(
    batter_runs_outperformance = batter_outperformance,
    bowler_runs_outperformance = bowler_outperformance,
    bowler_wicket_outperformance = if ("wicket_above_expected" %in% names(all_deliveries)) bowler_wicket_performance else NULL
  )

} else {
  cli::cli_alert_warning("Expected outcome features not found in data")
  cli::cli_alert_info("Run 01_prepare_data.R with the outcome model to add exp_runs and runs_above_expected")
  cli::cli_alert_info("These features enable player vs match-state comparison independent of opponent quality")
  outperformance_results <- NULL
}

# Summary Statistics ----
cli::cli_h2("Summary Statistics")

# WPA distribution
cat("WPA Distribution (all deliveries):\n")
cat(sprintf("  Mean: %.6f\n", mean(all_wpa_era$wpa, na.rm = TRUE)))
cat(sprintf("  Std Dev: %.6f\n", sd(all_wpa_era$wpa, na.rm = TRUE)))
cat(sprintf("  Min: %.6f\n", min(all_wpa_era$wpa, na.rm = TRUE)))
cat(sprintf("  Max: %.6f\n", max(all_wpa_era$wpa, na.rm = TRUE)))
cat(sprintf("  Positive WPA %%: %.1f%%\n", mean(all_wpa_era$wpa > 0, na.rm = TRUE) * 100))
cat("\n")

# ERA distribution
cat("ERA Distribution (all deliveries):\n")
cat(sprintf("  Mean: %.4f runs\n", mean(all_wpa_era$era, na.rm = TRUE)))
cat(sprintf("  Std Dev: %.4f runs\n", sd(all_wpa_era$era, na.rm = TRUE)))
cat(sprintf("  Min: %.4f runs\n", min(all_wpa_era$era, na.rm = TRUE)))
cat(sprintf("  Max: %.4f runs\n", max(all_wpa_era$era, na.rm = TRUE)))
cat("\n")

# Validation: WPA should sum to approximately 1 for winning team in 2nd innings matches
cli::cli_h3("Validation: Match-Level WPA Sums")

match_totals <- all_wpa_era %>%
  filter(innings == 2) %>%
  group_by(match_id, batting_team_wins) %>%
  summarise(
    total_wpa = sum(wpa, na.rm = TRUE),
    .groups = "drop"
  )

won_chase <- match_totals %>% filter(batting_team_wins == 1)
lost_chase <- match_totals %>% filter(batting_team_wins == 0)

cat(sprintf("Matches where batting team won chase:\n"))
cat(sprintf("  Mean total WPA: %.4f (should be ~1.0)\n", mean(won_chase$total_wpa)))
cat(sprintf("  Range: %.4f to %.4f\n", min(won_chase$total_wpa), max(won_chase$total_wpa)))

cat(sprintf("\nMatches where batting team lost chase:\n"))
cat(sprintf("  Mean total WPA: %.4f (should be ~0.0)\n", mean(lost_chase$total_wpa)))
cat(sprintf("  Range: %.4f to %.4f\n", min(lost_chase$total_wpa), max(lost_chase$total_wpa)))
cat("\n")

# Save Results ----
cli::cli_h2("Saving results")

wpa_era_results <- list(
  all_data = all_wpa_era,
  batter_wpa = batter_wpa,
  bowler_wpa = bowler_wpa,
  batter_era = batter_era,
  bowler_era = bowler_era,
  match_wpa = match_wpa,
  outperformance = outperformance_results,  # Player outperformance vs expected outcomes
  created_at = Sys.time()
)

results_path <- file.path(output_dir, "ipl_wpa_era_results.rds")
saveRDS(wpa_era_results, results_path)
cli::cli_alert_success("Results saved to {results_path}")

# Done ----
cat("\n")
cli::cli_alert_success("WPA/ERA analysis complete!")
cat("\n")

cli::cli_h3("Key Findings")
cli::cli_bullets(c(

  "i" = sprintf("Top batter by WPA: %s (%.4f)", top_batters_wpa$player_id[1], top_batters_wpa$total_wpa[1]),
  "i" = sprintf("Top bowler by WPA: %s (%.4f)", top_bowlers_wpa$player_id[1], top_bowlers_wpa$total_wpa[1]),
  "i" = sprintf("Top batter by ERA: %s (%.2f runs above expected)", top_batters_era$player_id[1], top_batters_era$total_era[1]),
  "i" = sprintf("Top bowler by ERA: %s (%.2f runs below expected)", top_bowlers_era$player_id[1], abs(top_bowlers_era$total_era[1]))
))
cat("\n")

cli::cli_h3("Interpretation Guide")
cli::cli_bullets(c(
  "*" = "WPA (Batters): Positive = helped win, Negative = hurt win chances",
  "*" = "WPA (Bowlers): Positive = reduced opponent's win chances",
  "*" = "ERA (Batters): Positive = scored more than expected for situation",
  "*" = "ERA (Bowlers): Negative = conceded less than expected",
  "*" = "Clutch moments: Deliveries with |WPA| > 0.05 (high impact)"
))
cat("\n")

cli::cli_h3("Outperformance Metrics (vs Expected Ball Outcomes)")
cli::cli_bullets(c(
  "*" = "runs_above_expected: Actual runs - expected runs (from match state model)",
  "*" = "wicket_above_expected: 1 if wicket taken, minus expected wicket probability",
  "*" = "These metrics are PLAYER-AGNOSTIC: compare actual vs average player at that match state",
  "*" = "Batters: Positive = scoring above par for the situation (over, wickets, phase)",
  "*" = "Bowlers: Negative runs_above_expected = conceding below par (good)",
  "*" = "Bowlers: Positive wicket_above_expected = taking wickets above expected rate"
))
cat("\n")
