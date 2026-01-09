# Debug script to test weighted normalization for team ELO
# Phase 2: Games + Diversity weighted normalization
# Run from bouncer/ directory

library(cli)
library(dplyr)
devtools::load_all()

cli::cli_h1("Testing Weighted Normalization (Mens International Test)")

# Configuration
FORMATS <- c("test")
CATEGORIES <- list(
  mens_international = list(gender = "male", team_type = "international")
)

FORMAT_GROUPS <- list(
  test = c("Test", "MDM")
)

# Connect to database
conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

# Get matches
for (current_format in FORMATS) {
  format_types <- FORMAT_GROUPS[[current_format]]

  for (category_name in names(CATEGORIES)) {
    cat_config <- CATEGORIES[[category_name]]

    cli::cli_h2("Category: {category_name} / Format: {current_format}")

    # Load matches for this category/format
    format_clause <- paste(sprintf("'%s'", tolower(format_types)), collapse = ", ")
    query <- sprintf("
      SELECT
        m.match_id,
        m.match_date,
        m.match_type,
        m.event_name,
        m.team1,
        m.team2,
        m.outcome_winner AS outcome_winner_id,
        m.gender,
        m.team_type,
        m.venue,
        m.unified_margin,
        COALESCE(EXTRACT(YEAR FROM m.match_date), 2020) as season
      FROM matches m
      WHERE LOWER(m.match_type) IN (%s)
        AND m.gender = '%s'
        AND m.team_type = '%s'
        AND m.outcome_type IS NOT NULL
      ORDER BY m.match_date, m.match_id
    ", format_clause, cat_config$gender, cat_config$team_type)

    matches <- DBI::dbGetQuery(conn, query)

    if (nrow(matches) == 0) {
      cli::cli_alert_warning("No matches found - skipping")
      next
    }

    cli::cli_alert_success("Found {nrow(matches)} matches")

    # Create team IDs
    matches$team1_id <- paste0(
      tolower(gsub(" ", "_", matches$team1)), "_",
      matches$gender, "_", current_format, "_", matches$team_type
    )
    matches$team2_id <- paste0(
      tolower(gsub(" ", "_", matches$team2)), "_",
      matches$gender, "_", current_format, "_", matches$team_type
    )

    # Prepare margin (already signed: positive = team1 won, negative = team2 won)
    matches$calc_unified_margin <- matches$unified_margin

    # Build home lookups and detect home team
    cli::cli_alert_info("Building home venue lookups...")
    home_lookups <- build_home_lookups(matches, current_format)

    matches$home_team <- mapply(
      detect_home_team,
      matches$team1, matches$team2,
      matches$team1_id, matches$team2_id,
      matches$venue, matches$team_type,
      MoreArgs = list(
        club_home_lookup = home_lookups$club_home,
        venue_country_lookup = home_lookups$venue_country
      )
    )

    n_home <- sum(matches$home_team != 0)
    cli::cli_alert_success("Home matches detected: {n_home}/{nrow(matches)} ({round(n_home/nrow(matches)*100, 1)}%)")

    all_teams_eventual <- unique(c(matches$team1_id, matches$team2_id))
    cli::cli_alert_info("Will see {length(all_teams_eventual)} unique teams total")

    # Initialize ELOs - teams added dynamically as they first appear
    STARTING_ELO <- 1300
    team_result_elos <- c()
    team_matches_played <- c()
    teams_seen <- character()

    # NEW: Track unique opponents for diversity weighting
    team_unique_opponents <- list()

    # NEW: Build frequency matrix incrementally
    # Using a list of lists for sparse representation
    frequency_matrix <- list()

    cli::cli_alert_info("New teams start at {STARTING_ELO} ELO (below 1500 average)")

    # Simple ELO parameters
    K_MAX <- 58
    K_MIN <- 16
    K_HALFLIFE <- 25
    HOME_ADVANTAGE <- 50

    # Weighting parameters for normalization
    GAMES_WEIGHT <- 0.7
    DIVERSITY_WEIGHT <- 0.3

    # Process matches
    n_matches <- nrow(matches)
    n_teams <- length(all_teams_eventual)
    n_records <- n_matches * n_teams

    cli::cli_alert_info("Will generate up to {format(n_records, big.mark=',')} records")

    # Pre-allocate output
    out_team_id <- character(n_records)
    out_match_id <- character(n_records)
    out_match_date <- as.Date(rep(NA, n_records))
    out_format <- character(n_records)
    out_gender <- character(n_records)
    out_team_type <- character(n_records)
    out_elo_before <- numeric(n_records)
    out_elo_after <- numeric(n_records)
    out_elo_diff <- numeric(n_records)
    out_played_in_match <- logical(n_records)
    out_matches_played <- integer(n_records)

    record_idx <- 1L
    elo_divisor <- 400

    cli::cli_progress_bar("Processing matches", total = n_matches)

    for (i in seq_len(n_matches)) {
      team1_id <- matches$team1_id[i]
      team2_id <- matches$team2_id[i]
      winner_id <- matches$outcome_winner_id[i]
      match_id <- matches$match_id[i]
      match_date <- matches$match_date[i]

      # Add new teams dynamically at STARTING_ELO (below average)
      if (!(team1_id %in% teams_seen)) {
        teams_seen <- c(teams_seen, team1_id)
        team_result_elos[team1_id] <- STARTING_ELO
        team_matches_played[team1_id] <- 0L
        team_unique_opponents[[team1_id]] <- character()
        frequency_matrix[[team1_id]] <- list()
      }
      if (!(team2_id %in% teams_seen)) {
        teams_seen <- c(teams_seen, team2_id)
        team_result_elos[team2_id] <- STARTING_ELO
        team_matches_played[team2_id] <- 0L
        team_unique_opponents[[team2_id]] <- character()
        frequency_matrix[[team2_id]] <- list()
      }

      # Update frequency matrix
      frequency_matrix[[team1_id]][[team2_id]] <-
        (frequency_matrix[[team1_id]][[team2_id]] %||% 0L) + 1L
      frequency_matrix[[team2_id]][[team1_id]] <-
        (frequency_matrix[[team2_id]][[team1_id]] %||% 0L) + 1L

      # Update unique opponents
      if (!(team2_id %in% team_unique_opponents[[team1_id]])) {
        team_unique_opponents[[team1_id]] <- c(team_unique_opponents[[team1_id]], team2_id)
      }
      if (!(team1_id %in% team_unique_opponents[[team2_id]])) {
        team_unique_opponents[[team2_id]] <- c(team_unique_opponents[[team2_id]], team1_id)
      }

      team1_elo_before <- team_result_elos[team1_id]
      team2_elo_before <- team_result_elos[team2_id]

      team1_won <- !is.na(winner_id) && winner_id == matches$team1[i]
      team2_won <- !is.na(winner_id) && winner_id == matches$team2[i]

      if (team1_won || team2_won) {
        # Dynamic K based on matches played
        k1 <- K_MIN + (K_MAX - K_MIN) * exp(-team_matches_played[team1_id] / K_HALFLIFE)
        k2 <- K_MIN + (K_MAX - K_MIN) * exp(-team_matches_played[team2_id] / K_HALFLIFE)

        # Home advantage
        home_adj <- matches$home_team[i] * HOME_ADVANTAGE

        # Expected scores
        exp1 <- 1 / (1 + 10^((team2_elo_before - team1_elo_before - home_adj) / elo_divisor))
        exp2 <- 1 - exp1

        actual1 <- if (team1_won) 1 else 0
        actual2 <- if (team2_won) 1 else 0

        team1_elo_after <- team1_elo_before + k1 * (actual1 - exp1)
        team2_elo_after <- team2_elo_before + k2 * (actual2 - exp2)
      } else {
        team1_elo_after <- team1_elo_before
        team2_elo_after <- team2_elo_before
      }

      # Update state
      team_result_elos[team1_id] <- team1_elo_after
      team_result_elos[team2_id] <- team2_elo_after
      team_matches_played[team1_id] <- team_matches_played[team1_id] + 1L
      team_matches_played[team2_id] <- team_matches_played[team2_id] + 1L

      # Capture elo_before for ALL teams (before normalization)
      all_elo_before <- team_result_elos
      all_elo_before[team1_id] <- team1_elo_before
      all_elo_before[team2_id] <- team2_elo_before

      # ============================================================
      # PHASE 2: Weighted Normalization
      # ============================================================
      # Calculate weights based on games played AND opponent diversity

      # Games weight: more games = more reliable
      games_played <- team_matches_played[teams_seen]
      max_games <- max(games_played)
      games_weight_vec <- games_played / max_games

      # Diversity weight: more unique opponents = better calibrated
      diversity <- sapply(teams_seen, function(t) length(team_unique_opponents[[t]]))
      max_diversity <- max(diversity)
      diversity_weight_vec <- diversity / max_diversity

      # Combined weight
      combined_weight <- GAMES_WEIGHT * games_weight_vec + DIVERSITY_WEIGHT * diversity_weight_vec
      normalized_weight <- combined_weight / sum(combined_weight)

      # Weighted mean (teams with more games and diversity anchor the scale)
      current_elos <- team_result_elos[teams_seen]
      weighted_mean <- sum(normalized_weight * current_elos)

      # Calculate drift and normalize
      drift <- weighted_mean - 1500
      if (abs(drift) > 0.001) {
        team_result_elos <- team_result_elos - drift
      }

      # Store a row for EVERY team seen so far
      for (team_id in teams_seen) {
        out_team_id[record_idx] <- team_id
        out_match_id[record_idx] <- match_id
        out_match_date[record_idx] <- match_date
        out_format[record_idx] <- current_format
        out_gender[record_idx] <- cat_config$gender
        out_team_type[record_idx] <- cat_config$team_type
        out_elo_before[record_idx] <- all_elo_before[team_id]
        out_elo_after[record_idx] <- team_result_elos[team_id]
        out_elo_diff[record_idx] <- team_result_elos[team_id] - all_elo_before[team_id]
        out_played_in_match[record_idx] <- (team_id == team1_id || team_id == team2_id)
        out_matches_played[record_idx] <- team_matches_played[team_id]
        record_idx <- record_idx + 1L
      }

      if (i %% 100 == 0) cli::cli_progress_update(set = i)
    }

    cli::cli_progress_done()

    # Create data frame
    actual_records <- record_idx - 1L
    elo_df <- data.frame(
      team_id = out_team_id[1:actual_records],
      match_id = out_match_id[1:actual_records],
      match_date = out_match_date[1:actual_records],
      format = out_format[1:actual_records],
      gender = out_gender[1:actual_records],
      team_type = out_team_type[1:actual_records],
      elo_before = out_elo_before[1:actual_records],
      elo_after = out_elo_after[1:actual_records],
      elo_diff = out_elo_diff[1:actual_records],
      played_in_match = out_played_in_match[1:actual_records],
      matches_played = out_matches_played[1:actual_records],
      stringsAsFactors = FALSE
    )

    cli::cli_alert_success("Generated {nrow(elo_df)} records")

    # Final standings with diversity info
    cli::cli_h2("Final Results")

    final_elos <- data.frame(
      team_id = names(team_result_elos),
      elo = unname(team_result_elos),
      matches = unname(team_matches_played),
      unique_opponents = sapply(names(team_result_elos), function(t) length(team_unique_opponents[[t]])),
      stringsAsFactors = FALSE
    ) %>%
      mutate(
        team_name = gsub("_male_test_international$", "", team_id)
      ) %>%
      arrange(desc(elo))

    # Verify mean is 1500
    cli::cli_alert_info("Final mean ELO: {round(mean(team_result_elos), 4)}")
    cli::cli_alert_info("Final weighted mean: {round(sum(normalized_weight * team_result_elos[teams_seen]), 4)}")

    # Show weighting stats
    cli::cli_h3("Weighting Statistics")
    weight_df <- data.frame(
      team = gsub("_male_test_international$", "", teams_seen),
      games = games_played,
      diversity = diversity,
      weight = round(normalized_weight * 100, 2)
    ) %>% arrange(desc(weight))

    cli::cli_alert_info("Top 5 most influential teams (by weight):")
    print(head(weight_df, 5))

    cli::cli_alert_info("Bottom 5 least influential teams:")
    print(tail(weight_df, 5))

    # TOP 10 TEAMS
    cli::cli_h2("Top 10 Teams")
    top_10 <- head(final_elos, 10)
    for (j in seq_len(nrow(top_10))) {
      cli::cli_alert_success(
        "{j}. {top_10$team_name[j]}: {round(top_10$elo[j])} ELO ({top_10$matches[j]} matches, {top_10$unique_opponents[j]} opponents)"
      )
    }

    # BOTTOM 10 TEAMS
    cli::cli_h2("Bottom 10 Teams")
    bottom_10 <- tail(final_elos, 10)
    for (j in seq_len(nrow(bottom_10))) {
      rank <- nrow(final_elos) - 10 + j
      cli::cli_alert_warning(
        "{rank}. {bottom_10$team_name[j]}: {round(bottom_10$elo[j])} ELO ({bottom_10$matches[j]} matches, {bottom_10$unique_opponents[j]} opponents)"
      )
    }

    # Show full distribution
    cli::cli_h3("ELO Distribution")
    cli::cli_alert_info("Min: {round(min(final_elos$elo))}, Max: {round(max(final_elos$elo))}, Range: {round(max(final_elos$elo) - min(final_elos$elo))}")
    cli::cli_alert_info("Teams above 1500: {sum(final_elos$elo > 1500)}, Below: {sum(final_elos$elo < 1500)}")

    # Compare simple vs weighted normalization effect
    cli::cli_h3("Normalization Comparison")
    non_playing <- elo_df[!elo_df$played_in_match, ]
    if (nrow(non_playing) > 0) {
      cli::cli_alert_info("Non-playing team adjustments: mean={round(mean(non_playing$elo_diff), 6)}, sd={round(sd(non_playing$elo_diff), 4)}")
    }
  }
}

# ============================================================
# CALIBRATION ANALYSIS
# ============================================================

cli::cli_h1("Calibration Analysis")

# Build a dataframe of match-level predictions vs actuals
# We need to reconstruct predictions at time of each match

cli::cli_h2("Rebuilding match predictions for calibration")

match_predictions <- data.frame(
  match_id = character(n_matches),
  match_date = as.Date(rep(NA, n_matches)),
  team1_id = character(n_matches),
  team2_id = character(n_matches),
  team1_elo = numeric(n_matches),
  team2_elo = numeric(n_matches),
  elo_diff = numeric(n_matches),
  raw_elo_diff = numeric(n_matches),
  home_team = integer(n_matches),
  pred_team1_win = numeric(n_matches),
  actual_team1_win = integer(n_matches),
  margin = numeric(n_matches),
  pred_margin = numeric(n_matches),
  stringsAsFactors = FALSE
)

# Reset and replay to capture predictions
team_elos_cal <- c()
team_matches_cal <- c()
teams_seen_cal <- character()

for (i in seq_len(n_matches)) {
  team1_id <- matches$team1_id[i]
  team2_id <- matches$team2_id[i]
  winner_id <- matches$outcome_winner_id[i]
  match_id <- matches$match_id[i]
  match_date <- matches$match_date[i]
  margin <- matches$calc_unified_margin[i]
  home_team <- matches$home_team[i]

  # Add new teams
  if (!(team1_id %in% teams_seen_cal)) {
    teams_seen_cal <- c(teams_seen_cal, team1_id)
    team_elos_cal[team1_id] <- STARTING_ELO
    team_matches_cal[team1_id] <- 0L
  }
  if (!(team2_id %in% teams_seen_cal)) {
    teams_seen_cal <- c(teams_seen_cal, team2_id)
    team_elos_cal[team2_id] <- STARTING_ELO
    team_matches_cal[team2_id] <- 0L
  }

  team1_elo <- team_elos_cal[team1_id]
  team2_elo <- team_elos_cal[team2_id]

  # Apply home advantage (home_team: 1 = team1 home, -1 = team2 home, 0 = neutral)
  home_adj <- home_team * HOME_ADVANTAGE
  elo_diff <- team1_elo - team2_elo + home_adj

  # Predicted win probability for team1 (with home advantage)
  pred_team1_win <- 1 / (1 + 10^(-elo_diff / 400))

  # Predicted margin (simple linear model: ~7.5 runs per 100 ELO difference for Tests)
  # Uses home-adjusted elo_diff since home advantage affects expected margin too
  runs_per_100_elo <- 7.5
  pred_margin <- elo_diff * runs_per_100_elo / 100

  # Store raw elo diff (without home) for analysis
  raw_elo_diff <- team1_elo - team2_elo

  # Actual outcome
  team1_won <- !is.na(winner_id) && winner_id == matches$team1[i]
  team2_won <- !is.na(winner_id) && winner_id == matches$team2[i]
  actual_team1_win <- if (team1_won) 1L else if (team2_won) 0L else NA_integer_

  # Actual margin - already signed from team1's perspective in database
  # Positive = team1 won, Negative = team2 won
  actual_margin <- margin  # Don't flip - it's already correct!

  # Store prediction
  match_predictions$match_id[i] <- match_id
  match_predictions$match_date[i] <- match_date
  match_predictions$team1_id[i] <- team1_id
  match_predictions$team2_id[i] <- team2_id
  match_predictions$team1_elo[i] <- team1_elo
  match_predictions$team2_elo[i] <- team2_elo
  match_predictions$elo_diff[i] <- elo_diff
  match_predictions$raw_elo_diff[i] <- raw_elo_diff
  match_predictions$home_team[i] <- home_team
  match_predictions$pred_team1_win[i] <- pred_team1_win
  match_predictions$actual_team1_win[i] <- actual_team1_win
  match_predictions$margin[i] <- actual_margin
  match_predictions$pred_margin[i] <- pred_margin

  # Update ELOs (simplified - just for calibration tracking)
  if (team1_won || team2_won) {
    k1 <- K_MIN + (K_MAX - K_MIN) * exp(-team_matches_cal[team1_id] / K_HALFLIFE)
    k2 <- K_MIN + (K_MAX - K_MIN) * exp(-team_matches_cal[team2_id] / K_HALFLIFE)

    exp1 <- pred_team1_win
    exp2 <- 1 - exp1
    actual1 <- if (team1_won) 1 else 0
    actual2 <- if (team2_won) 1 else 0

    team_elos_cal[team1_id] <- team1_elo + k1 * (actual1 - exp1)
    team_elos_cal[team2_id] <- team2_elo + k2 * (actual2 - exp2)
  }

  team_matches_cal[team1_id] <- team_matches_cal[team1_id] + 1L
  team_matches_cal[team2_id] <- team_matches_cal[team2_id] + 1L

  # Simple normalization for calibration run
  drift <- mean(team_elos_cal) - 1500
  if (abs(drift) > 0.001) {
    team_elos_cal <- team_elos_cal - drift
  }
}

cli::cli_alert_success("Built {nrow(match_predictions)} match predictions")

# ---- WIN PROBABILITY CALIBRATION ----
cli::cli_h2("Win Probability Calibration")

# Filter to matches with definitive outcomes
valid_matches <- match_predictions[!is.na(match_predictions$actual_team1_win), ]
cli::cli_alert_info("Matches with win/loss outcome: {nrow(valid_matches)}")

# Bin by predicted win probability
valid_matches$pred_bin <- cut(
  valid_matches$pred_team1_win,
  breaks = c(0, 0.2, 0.35, 0.45, 0.55, 0.65, 0.8, 1.0),
  labels = c("0-20%", "20-35%", "35-45%", "45-55%", "55-65%", "65-80%", "80-100%"),
  include.lowest = TRUE
)

win_calibration <- valid_matches %>%
  group_by(pred_bin) %>%
  summarise(
    n_matches = n(),
    pred_win_rate = mean(pred_team1_win),
    actual_win_rate = mean(actual_team1_win),
    calibration_error = actual_win_rate - pred_win_rate,
    .groups = "drop"
  )

cli::cli_h3("Win Probability Calibration by Bin")
cat(sprintf("%-12s %10s %12s %12s %12s\n",
            "Pred Bin", "N Matches", "Pred Win%", "Actual Win%", "Error"))
cat(paste(rep("-", 60), collapse = ""), "\n")

for (j in seq_len(nrow(win_calibration))) {
  row <- win_calibration[j, ]
  cat(sprintf("%-12s %10d %11.1f%% %11.1f%% %+11.1f%%\n",
              as.character(row$pred_bin),
              row$n_matches,
              row$pred_win_rate * 100,
              row$actual_win_rate * 100,
              row$calibration_error * 100))
}

# Overall calibration metrics
overall_brier <- mean((valid_matches$pred_team1_win - valid_matches$actual_team1_win)^2)
overall_log_loss <- -mean(
  valid_matches$actual_team1_win * log(pmax(valid_matches$pred_team1_win, 0.001)) +
  (1 - valid_matches$actual_team1_win) * log(pmax(1 - valid_matches$pred_team1_win, 0.001))
)
overall_accuracy <- mean((valid_matches$pred_team1_win > 0.5) == valid_matches$actual_team1_win)

cat("\n")
cli::cli_alert_info("Overall Brier Score: {round(overall_brier, 4)} (lower is better, 0.25 = random)")
cli::cli_alert_info("Overall Log Loss: {round(overall_log_loss, 4)} (lower is better, 0.693 = random)")
cli::cli_alert_info("Overall Accuracy: {round(overall_accuracy * 100, 1)}% (picking favorite)")

# ---- MARGIN OF VICTORY CALIBRATION ----
cli::cli_h2("Margin of Victory Calibration")

# Filter to matches with margin data
margin_matches <- match_predictions[!is.na(match_predictions$margin), ]
cli::cli_alert_info("Matches with margin data: {nrow(margin_matches)}")

if (nrow(margin_matches) > 50) {
  # Bin by predicted margin
  margin_matches$pred_margin_bin <- cut(
    margin_matches$pred_margin,
    breaks = c(-Inf, -75, -40, -15, 15, 40, 75, Inf),
    labels = c("<-75", "-75 to -40", "-40 to -15", "-15 to +15", "+15 to +40", "+40 to +75", ">+75")
  )

  margin_calibration <- margin_matches %>%
    group_by(pred_margin_bin) %>%
    summarise(
      n_matches = n(),
      pred_margin_mean = mean(pred_margin),
      actual_margin_mean = mean(margin),
      margin_error = actual_margin_mean - pred_margin_mean,
      margin_mae = mean(abs(margin - pred_margin)),
      .groups = "drop"
    )

  cli::cli_h3("Margin Calibration by Predicted Margin Bin")
  cat(sprintf("%-15s %8s %12s %12s %10s %10s\n",
              "Pred Margin", "N", "Pred Mean", "Actual Mean", "Error", "MAE"))
  cat(paste(rep("-", 70), collapse = ""), "\n")

  for (j in seq_len(nrow(margin_calibration))) {
    row <- margin_calibration[j, ]
    cat(sprintf("%-15s %8d %+11.1f %+11.1f %+9.1f %9.1f\n",
                as.character(row$pred_margin_bin),
                row$n_matches,
                row$pred_margin_mean,
                row$actual_margin_mean,
                row$margin_error,
                row$margin_mae))
  }

  # Overall margin metrics
  overall_margin_mae <- mean(abs(margin_matches$margin - margin_matches$pred_margin))
  overall_margin_rmse <- sqrt(mean((margin_matches$margin - margin_matches$pred_margin)^2))
  margin_correlation <- cor(margin_matches$pred_margin, margin_matches$margin)

  cat("\n")
  cli::cli_alert_info("Overall Margin MAE: {round(overall_margin_mae, 1)} runs")
  cli::cli_alert_info("Overall Margin RMSE: {round(overall_margin_rmse, 1)} runs")
  cli::cli_alert_info("Pred vs Actual Margin Correlation: {round(margin_correlation, 3)}")

  # Check if ELO diff correlates with actual margin
  elo_margin_cor <- cor(margin_matches$elo_diff, margin_matches$margin)
  cli::cli_alert_info("ELO Diff vs Actual Margin Correlation: {round(elo_margin_cor, 3)}")
} else {
  cli::cli_alert_warning("Not enough margin data for calibration analysis")
}

# ---- CALIBRATION BY ELO DIFFERENCE ----
cli::cli_h2("Calibration by ELO Difference")

valid_matches$elo_diff_bin <- cut(
  abs(valid_matches$elo_diff),
  breaks = c(0, 50, 100, 150, 200, 300, Inf),
  labels = c("0-50", "50-100", "100-150", "150-200", "200-300", "300+"),
  include.lowest = TRUE
)

elo_diff_calibration <- valid_matches %>%
  mutate(
    favorite_won = (elo_diff > 0 & actual_team1_win == 1) | (elo_diff < 0 & actual_team1_win == 0),
    pred_favorite_win = pmax(pred_team1_win, 1 - pred_team1_win)
  ) %>%
  group_by(elo_diff_bin) %>%
  summarise(
    n_matches = n(),
    pred_favorite_rate = mean(pred_favorite_win),
    actual_favorite_rate = mean(favorite_won, na.rm = TRUE),
    calibration_error = actual_favorite_rate - pred_favorite_rate,
    .groups = "drop"
  )

cli::cli_h3("Favorite Win Rate by ELO Difference")
cat(sprintf("%-12s %10s %12s %12s %12s\n",
            "ELO Diff", "N Matches", "Pred Fav%", "Actual Fav%", "Error"))
cat(paste(rep("-", 60), collapse = ""), "\n")

for (j in seq_len(nrow(elo_diff_calibration))) {
  row <- elo_diff_calibration[j, ]
  cat(sprintf("%-12s %10d %11.1f%% %11.1f%% %+11.1f%%\n",
              as.character(row$elo_diff_bin),
              row$n_matches,
              row$pred_favorite_rate * 100,
              row$actual_favorite_rate * 100,
              row$calibration_error * 100))
}

# ---- HOME/AWAY CALIBRATION ----
cli::cli_h2("Home/Away Calibration")

home_matches <- valid_matches[valid_matches$home_team == 1, ]
away_matches <- valid_matches[valid_matches$home_team == -1, ]
neutral_matches <- valid_matches[valid_matches$home_team == 0, ]

cli::cli_alert_info("Home matches (team1 at home): {nrow(home_matches)}")
cli::cli_alert_info("Away matches (team2 at home): {nrow(away_matches)}")
cli::cli_alert_info("Neutral matches: {nrow(neutral_matches)}")

if (nrow(home_matches) > 20) {
  home_win_rate <- mean(home_matches$actual_team1_win)
  home_pred_rate <- mean(home_matches$pred_team1_win)
  cli::cli_alert_info("Team1 at HOME: Predicted {round(home_pred_rate*100, 1)}%, Actual {round(home_win_rate*100, 1)}%, Error {sprintf('%+.1f%%', (home_win_rate - home_pred_rate)*100)}")
}

if (nrow(away_matches) > 20) {
  away_win_rate <- mean(away_matches$actual_team1_win)
  away_pred_rate <- mean(away_matches$pred_team1_win)
  cli::cli_alert_info("Team1 AWAY: Predicted {round(away_pred_rate*100, 1)}%, Actual {round(away_win_rate*100, 1)}%, Error {sprintf('%+.1f%%', (away_win_rate - away_pred_rate)*100)}")
}

if (nrow(neutral_matches) > 20) {
  neutral_win_rate <- mean(neutral_matches$actual_team1_win)
  neutral_pred_rate <- mean(neutral_matches$pred_team1_win)
  cli::cli_alert_info("NEUTRAL venue: Predicted {round(neutral_pred_rate*100, 1)}%, Actual {round(neutral_win_rate*100, 1)}%, Error {sprintf('%+.1f%%', (neutral_win_rate - neutral_pred_rate)*100)}")
}

# Calculate actual home advantage from data
if (nrow(home_matches) > 20 && nrow(away_matches) > 20) {
  # Compare team1's performance at home vs opponent's home
  actual_home_boost <- home_win_rate - away_win_rate
  cli::cli_alert_info("Observed home advantage: {sprintf('%+.1f%%', actual_home_boost*100)} (home win rate - away win rate)")

  # Convert to ELO points (rough approximation)
  # If home team wins X% more, that's equivalent to Y ELO points
  # Win prob = 1/(1+10^(-elo_diff/400)), so elo_diff = -400 * log10(1/p - 1)
  if (home_win_rate > 0.01 && home_win_rate < 0.99 && away_win_rate > 0.01 && away_win_rate < 0.99) {
    implied_home_elo <- -400 * log10(1/home_win_rate - 1)
    implied_away_elo <- -400 * log10(1/away_win_rate - 1)
    implied_advantage <- (implied_home_elo - implied_away_elo) / 2
    cli::cli_alert_info("Implied home advantage: ~{round(implied_advantage)} ELO points (currently using {HOME_ADVANTAGE})")
  }
}

cli::cli_alert_success("Calibration analysis complete!")
