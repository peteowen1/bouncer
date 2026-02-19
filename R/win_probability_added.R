# Win Probability Added (WPA) and Expected Runs Added (ERA) Functions
#
# These functions calculate player-level impact metrics based on
# the two-stage win probability model (Stage 1: Projected Score,
# Stage 2: Win Probability).

#' Calculate Win Probability Added (WPA) for Deliveries
#'
#' Calculates the change in win probability (2nd innings) or projected score
#' (1st innings) caused by each delivery. Positive WPA = good for batting team.
#'
#' @param deliveries data.frame or data.table with delivery data including
#'   match_id, innings, over, ball, runs_batter, runs_total, is_wicket,
#'   total_runs, wickets_fallen, and all features required by the models.
#' @param stage1_model XGBoost model for projected score (1st innings)
#' @param stage2_model XGBoost model for win probability (2nd innings)
#' @param stage1_feature_cols Character vector of Stage 1 feature column names
#' @param stage2_feature_cols Character vector of Stage 2 feature column names
#'
#' @return data.frame with additional columns:
#'   \itemize{
#'     \item predicted_before: Prediction before this delivery
#'     \item predicted_after: Prediction after this delivery
#'     \item wpa: Win Probability Added (projected score change for 1st innings)
#'     \item batter_wpa: WPA credited to batter
#'     \item bowler_wpa: WPA credited to bowler
#'   }
#'
#' @keywords internal
calculate_delivery_wpa <- function(deliveries,
                                   stage1_model,
                                   stage2_model,
                                   stage1_feature_cols,
                                   stage2_feature_cols) {

  # Convert to data.frame if data.table
  deliveries <- as.data.frame(deliveries)

  # Separate by innings
  innings1 <- deliveries[deliveries$innings == 1, ]
  innings2 <- deliveries[deliveries$innings == 2, ]

  results <- list()

  # Process 1st innings (projected score change)
  if (nrow(innings1) > 0) {
    innings1_wpa <- calculate_innings1_wpa(
      innings1,
      stage1_model,
      stage1_feature_cols
    )
    results <- c(results, list(innings1_wpa))
  }

  # Process 2nd innings (win probability change)
  if (nrow(innings2) > 0) {
    innings2_wpa <- calculate_innings2_wpa(
      innings2,
      stage1_model,
      stage2_model,
      stage1_feature_cols,
      stage2_feature_cols
    )
    results <- c(results, list(innings2_wpa))
  }

  # Combine results
  if (length(results) > 0) {
    combined <- fast_rbind(results)
    return(combined)
  } else {
    return(deliveries)
  }
}


#' Calculate WPA for 1st Innings (Projected Score Change)
#'
#' Internal function to calculate projected score changes for 1st innings.
#'
#' @param innings1 data.frame of 1st innings deliveries
#' @param stage1_model XGBoost Stage 1 model
#' @param feature_cols Feature column names for Stage 1
#'
#' @return data.frame with WPA columns added
#'
#' @keywords internal
calculate_innings1_wpa <- function(innings1, stage1_model, feature_cols) {

  # Prepare features for "before" state
  before_features <- prepare_stage1_features(innings1, feature_cols)

  # Prepare features for "after" state (simulate delivery outcome)
  after_data <- simulate_after_state(innings1)
  after_features <- prepare_stage1_features(after_data, feature_cols)

  # Get predictions
  before_matrix <- xgboost::xgb.DMatrix(data = as.matrix(before_features))
  after_matrix <- xgboost::xgb.DMatrix(data = as.matrix(after_features))

  predicted_before <- stats::predict(stage1_model, before_matrix)
  predicted_after <- stats::predict(stage1_model, after_matrix)

  # Calculate WPA (projected score change)
  wpa <- predicted_after - predicted_before

  # Assign credit/blame
  credit <- assign_wpa_credit(innings1, wpa)

  # Add to data
  innings1$predicted_before <- predicted_before
  innings1$predicted_after <- predicted_after
  innings1$wpa <- wpa
  innings1$batter_wpa <- credit$batter_wpa
  innings1$bowler_wpa <- credit$bowler_wpa


  return(innings1)
}


#' Calculate WPA for 2nd Innings (Win Probability Change)
#'
#' Internal function to calculate win probability changes for 2nd innings chase.
#'
#' @param innings2 data.frame of 2nd innings deliveries
#' @param stage1_model XGBoost Stage 1 model
#' @param stage2_model XGBoost Stage 2 model
#' @param stage1_feature_cols Feature columns for Stage 1
#' @param stage2_feature_cols Feature columns for Stage 2
#'
#' @return data.frame with WPA columns added
#'
#' @keywords internal
calculate_innings2_wpa <- function(innings2, stage1_model, stage2_model,
                                   stage1_feature_cols, stage2_feature_cols) {

  # Get Stage 1 predictions for "before" state
  before_s1_features <- prepare_stage1_features(innings2, stage1_feature_cols)
  before_s1_matrix <- xgboost::xgb.DMatrix(data = as.matrix(before_s1_features))
  projected_score_before <- stats::predict(stage1_model, before_s1_matrix)

  # Add Stage 1 predictions to data for Stage 2
  innings2_before <- innings2
  innings2_before$projected_final_score <- projected_score_before
  innings2_before$projected_vs_target <- projected_score_before - innings2_before$target_runs
  innings2_before$projected_win_margin <- projected_score_before - (innings2_before$target_runs - 1)

  # Prepare Stage 2 features for "before" state
  before_s2_features <- prepare_stage2_features(innings2_before, stage2_feature_cols)
  before_s2_matrix <- xgboost::xgb.DMatrix(data = as.matrix(before_s2_features))
  predicted_before <- stats::predict(stage2_model, before_s2_matrix)

  # Simulate "after" state
  after_data <- simulate_after_state(innings2)

  # Get Stage 1 predictions for "after" state
  after_s1_features <- prepare_stage1_features(after_data, stage1_feature_cols)
  after_s1_matrix <- xgboost::xgb.DMatrix(data = as.matrix(after_s1_features))
  projected_score_after <- stats::predict(stage1_model, after_s1_matrix)

  # Add Stage 1 predictions for "after" state
  after_data$projected_final_score <- projected_score_after
  after_data$projected_vs_target <- projected_score_after - after_data$target_runs
  after_data$projected_win_margin <- projected_score_after - (after_data$target_runs - 1)

  # Prepare Stage 2 features for "after" state
  after_s2_features <- prepare_stage2_features(after_data, stage2_feature_cols)
  after_s2_matrix <- xgboost::xgb.DMatrix(data = as.matrix(after_s2_features))
  predicted_after <- stats::predict(stage2_model, after_s2_matrix)

  # Calculate WPA (win probability change)
  wpa <- predicted_after - predicted_before

  # Assign credit/blame
  credit <- assign_wpa_credit(innings2, wpa)

  # Add to data
  innings2$predicted_before <- predicted_before
  innings2$predicted_after <- predicted_after
  innings2$wpa <- wpa
  innings2$batter_wpa <- credit$batter_wpa
  innings2$bowler_wpa <- credit$bowler_wpa

  return(innings2)
}


#' Simulate After-Delivery State
#'
#' Creates the match state after a delivery is bowled by updating
#' cumulative totals and derived features.
#'
#' @param deliveries data.frame of deliveries
#'
#' @return data.frame with updated state variables
#'
#' @keywords internal
simulate_after_state <- function(deliveries) {

  after <- deliveries

  # Update cumulative totals
  after$total_runs <- after$total_runs + after$runs_total
  after$wickets_fallen <- after$wickets_fallen + as.integer(after$is_wicket)

  # Update ball counts
  if ("balls_bowled" %in% names(after)) {
    after$balls_bowled <- after$balls_bowled + 1
  }

  # Recalculate overs
  if ("overs_completed" %in% names(after) && "balls_bowled" %in% names(after)) {
    after$overs_completed <- after$balls_bowled / 6
  }

  if ("overs_remaining" %in% names(after)) {
    after$overs_remaining <- pmax(0, after$overs_remaining - (1/6))
  }

  if ("balls_remaining" %in% names(after)) {
    after$balls_remaining <- pmax(0, after$balls_remaining - 1)
  }

  # Recalculate run rate
  if ("current_run_rate" %in% names(after) && "balls_bowled" %in% names(after)) {
    overs <- after$balls_bowled / 6
    after$current_run_rate <- ifelse(overs > 0, after$total_runs / overs, 0)
  }

  # Update wickets in hand
  after$wickets_in_hand <- 10 - after$wickets_fallen

  # For 2nd innings: update chase pressure metrics
  if ("runs_needed" %in% names(after) && "target_runs" %in% names(after)) {
    after$runs_needed <- pmax(0, after$target_runs - after$total_runs)

    if ("overs_remaining" %in% names(after)) {
      after$required_run_rate <- ifelse(
        after$overs_remaining > 0 & after$runs_needed > 0,
        after$runs_needed / after$overs_remaining,
        ifelse(after$runs_needed <= 0, 0, Inf)
      )

      if ("current_run_rate" %in% names(after)) {
        after$rr_differential <- after$required_run_rate - after$current_run_rate
      }
    }

    if ("balls_remaining" %in% names(after)) {
      after$balls_per_run_needed <- ifelse(
        after$runs_needed > 0,
        after$balls_remaining / after$runs_needed,
        Inf
      )
      after$balls_per_wicket_available <- ifelse(
        after$wickets_in_hand > 0,
        after$balls_remaining / after$wickets_in_hand,
        0
      )
    }
  }

  return(after)
}


#' Prepare Stage 1 Features for Prediction
#'
#' Prepares feature matrix for Stage 1 (projected score) model.
#'
#' @param data data.frame with delivery data
#' @param feature_cols Character vector of required feature columns
#'
#' @return matrix suitable for XGBoost prediction
#'
#' @keywords internal
prepare_stage1_features <- function(data, feature_cols) {

  # Create one-hot encoded features
  data$phase_powerplay <- as.integer(data$phase == "powerplay")
  data$phase_middle <- as.integer(data$phase == "middle")
  data$phase_death <- as.integer(data$phase == "death")
  data$gender_male <- as.integer(data$gender == "male")
  data$wickets_in_hand <- 10 - data$wickets_fallen

  # Ensure all required columns exist
  for (col in feature_cols) {
    if (!col %in% names(data)) {
      data[[col]] <- 0
    }
  }

  # Extract feature matrix
  features <- as.matrix(data[, feature_cols, drop = FALSE])

  # Handle NA and Inf
  features[is.na(features)] <- 0
  features[is.infinite(features)] <- INF_FEATURE_PLACEHOLDER

  return(features)
}


#' Prepare Stage 2 Features for Prediction
#'
#' Prepares feature matrix for Stage 2 (win probability) model.
#'
#' @param data data.frame with delivery data including Stage 1 predictions
#' @param feature_cols Character vector of required feature columns
#'
#' @return matrix suitable for XGBoost prediction
#'
#' @keywords internal
prepare_stage2_features <- function(data, feature_cols) {

  # Create one-hot encoded features
  data$phase_powerplay <- as.integer(data$phase == "powerplay")
  data$phase_middle <- as.integer(data$phase == "middle")
  data$phase_death <- as.integer(data$phase == "death")
  data$gender_male <- as.integer(data$gender == "male")
  data$is_dls <- as.integer(data$is_dls_match)
  data$is_ko <- as.integer(data$is_knockout)
  data$wickets_in_hand <- 10 - data$wickets_fallen

  # Ensure all required columns exist
  for (col in feature_cols) {
    if (!col %in% names(data)) {
      data[[col]] <- 0
    }
  }

  # Extract feature matrix
  features <- as.matrix(data[, feature_cols, drop = FALSE])

  # Handle NA and Inf
  features[is.na(features)] <- 0
  features[is.infinite(features)] <- INF_FEATURE_PLACEHOLDER

  return(features)
}


#' Assign WPA Credit to Batter and Bowler
#'
#' Assigns credit/blame for WPA to batter and bowler based on delivery outcome.
#'
#' @param deliveries data.frame with delivery outcomes
#' @param wpa Numeric vector of WPA values
#'
#' @return list with batter_wpa and bowler_wpa vectors
#'
#' @keywords internal
assign_wpa_credit <- function(deliveries, wpa) {

  # Identify extras (check column existence BEFORE access to avoid logical(0))
  is_wide <- if ("wides" %in% names(deliveries)) {
    !is.na(deliveries$wides) & deliveries$wides > 0
  } else {
    rep(FALSE, nrow(deliveries))
  }
  is_noball <- if ("noballs" %in% names(deliveries)) {
    !is.na(deliveries$noballs) & deliveries$noballs > 0
  } else {
    rep(FALSE, nrow(deliveries))
  }
  is_bye <- if ("byes" %in% names(deliveries)) {
    !is.na(deliveries$byes) & deliveries$byes > 0
  } else {
    rep(FALSE, nrow(deliveries))
  }
  is_legbye <- if ("legbyes" %in% names(deliveries)) {
    !is.na(deliveries$legbyes) & deliveries$legbyes > 0
  } else {
    rep(FALSE, nrow(deliveries))
  }

  is_bowler_extra <- is_wide | is_noball
  is_neutral_extra <- is_bye | is_legbye

  # Default: batter gets full credit, bowler gets inverse
  batter_wpa <- wpa
  bowler_wpa <- -wpa

  # Wides and no-balls: bowler only (batter didn't contribute)
  batter_wpa[is_bowler_extra] <- 0
  # bowler_wpa stays as -wpa (blame for extras)

  # Byes and leg-byes: neutral (neither batter nor bowler caused)
  batter_wpa[is_neutral_extra] <- 0
  bowler_wpa[is_neutral_extra] <- 0

  # Note: wickets - batter still gets blame via negative WPA,
  # bowler gets credit via positive -wpa (negative of negative = positive)

  list(
    batter_wpa = batter_wpa,
    bowler_wpa = bowler_wpa
  )
}


#' Calculate Expected Runs Added (ERA) for Deliveries
#'
#' Calculates how many runs were scored above/below expectation based on
#' the match situation. Uses Stage 1 model to derive expected runs.
#'
#' @param deliveries data.frame with delivery data
#' @param stage1_model XGBoost model for projected score
#' @param stage1_feature_cols Character vector of Stage 1 feature column names
#'
#' @return data.frame with additional columns:
#'   \itemize{
#'     \item expected_runs: Expected runs for this delivery situation
#'     \item era: Expected Runs Added (actual - expected)
#'     \item batter_era: ERA credited to batter
#'     \item bowler_era: ERA credited to bowler
#'   }
#'
#' @keywords internal
calculate_delivery_era <- function(deliveries, stage1_model, stage1_feature_cols) {

  deliveries <- as.data.frame(deliveries)

  # Calculate expected runs using model delta method:
  # Expected = projected_score(after neutral delivery) - projected_score(before)

  # Get "before" predictions
  before_features <- prepare_stage1_features(deliveries, stage1_feature_cols)
  before_matrix <- xgboost::xgb.DMatrix(data = as.matrix(before_features))
  projected_before <- stats::predict(stage1_model, before_matrix)

  # Simulate "after" state for neutral delivery (dot ball, no wicket)
  neutral_after <- deliveries
  neutral_after$total_runs <- neutral_after$total_runs + 0  # Dot ball
  # Don't add wicket
  if ("balls_bowled" %in% names(neutral_after)) {
    neutral_after$balls_bowled <- neutral_after$balls_bowled + 1
  }
  if ("overs_remaining" %in% names(neutral_after)) {
    neutral_after$overs_remaining <- pmax(0, neutral_after$overs_remaining - (1/6))
  }
  if ("balls_remaining" %in% names(neutral_after)) {
    neutral_after$balls_remaining <- pmax(0, neutral_after$balls_remaining - 1)
  }
  # Update run rate for neutral delivery
  if ("current_run_rate" %in% names(neutral_after) && "balls_bowled" %in% names(neutral_after)) {
    overs <- neutral_after$balls_bowled / 6
    neutral_after$current_run_rate <- ifelse(overs > 0, neutral_after$total_runs / overs, 0)
  }
  neutral_after$wickets_in_hand <- 10 - neutral_after$wickets_fallen

  # Get "after neutral" predictions
  neutral_features <- prepare_stage1_features(neutral_after, stage1_feature_cols)
  neutral_matrix <- xgboost::xgb.DMatrix(data = as.matrix(neutral_features))
  projected_neutral <- stats::predict(stage1_model, neutral_matrix)

  # Expected change in projected score for a dot ball
  # This represents the "baseline" expectation - scoring rate built into situation
  expected_change <- projected_neutral - projected_before

  # For a more intuitive expected runs:

  # Average runs per ball in T20 is ~1.3-1.5
  # We use the model's expectation which varies by situation
  # Expected runs = change in projection if a "neutral" delivery occurred
  # But this can be negative late in innings when projected score is catching up to actual

  # Simpler approach: use the model's implied run rate for the situation
  # Expected runs per ball = (projected_final - current_runs) / balls_remaining
  balls_remaining <- deliveries$balls_remaining
  balls_remaining[balls_remaining <= 0] <- 1  # Avoid division by zero
  current_runs <- deliveries$total_runs
  expected_runs_per_ball <- (projected_before - current_runs) / balls_remaining
  expected_runs_per_ball <- pmax(0, pmin(expected_runs_per_ball, 6))  # Cap at 0-6

  # ERA = actual runs - expected runs
  actual_runs <- deliveries$runs_batter
  era <- actual_runs - expected_runs_per_ball

  # Assign credit
  era_credit <- assign_era_credit(deliveries, era)

  # Add to data
  deliveries$expected_runs <- expected_runs_per_ball
  deliveries$era <- era
  deliveries$batter_era <- era_credit$batter_era
  deliveries$bowler_era <- era_credit$bowler_era

  return(deliveries)
}


#' Assign ERA Credit to Batter and Bowler
#'
#' Assigns credit/blame for ERA to batter and bowler based on delivery outcome.
#'
#' @param deliveries data.frame with delivery outcomes
#' @param era Numeric vector of ERA values
#'
#' @return list with batter_era and bowler_era vectors
#'
#' @keywords internal
assign_era_credit <- function(deliveries, era) {

  # Handle extras columns that may not exist
  is_wide <- if ("wides" %in% names(deliveries)) {
    !is.na(deliveries$wides) & deliveries$wides > 0
  } else {
    rep(FALSE, nrow(deliveries))
  }

  is_noball <- if ("noballs" %in% names(deliveries)) {
    !is.na(deliveries$noballs) & deliveries$noballs > 0
  } else {
    rep(FALSE, nrow(deliveries))
  }

  is_bye <- if ("byes" %in% names(deliveries)) {
    !is.na(deliveries$byes) & deliveries$byes > 0
  } else {
    rep(FALSE, nrow(deliveries))
  }

  is_legbye <- if ("legbyes" %in% names(deliveries)) {
    !is.na(deliveries$legbyes) & deliveries$legbyes > 0
  } else {
    rep(FALSE, nrow(deliveries))
  }

  is_bowler_extra <- is_wide | is_noball
  is_neutral_extra <- is_bye | is_legbye

  # Default: batter gets credit for positive ERA, bowler gets inverse
  batter_era <- era
  bowler_era <- -era

  # Wides and no-balls: bowler only
  batter_era[is_bowler_extra] <- 0

  # Byes and leg-byes: neutral
  batter_era[is_neutral_extra] <- 0
  bowler_era[is_neutral_extra] <- 0

  list(
    batter_era = batter_era,
    bowler_era = bowler_era
  )
}


#' Calculate Match-Level WPA/ERA for Each Player
#'
#' Aggregates delivery-level WPA and ERA to match-level totals per player.
#'
#' @param wpa_data data.frame with delivery-level WPA/ERA (output of
#'   calculate_delivery_wpa or calculate_delivery_era with both calculated)
#'
#' @return data.frame with columns:
#'   \itemize{
#'     \item match_id: Match identifier
#'     \item player_id: Player identifier
#'     \item role: "batting" or "bowling"
#'     \item deliveries: Number of deliveries involved
#'     \item total_wpa: Sum of WPA for this player in this match
#'     \item total_era: Sum of ERA for this player in this match (if available)
#'     \item key_moment_wpa: Highest single-delivery absolute WPA
#'   }
#'
#' @keywords internal
calculate_match_wpa <- function(wpa_data) {

  wpa_data <- as.data.frame(wpa_data)

  # Aggregate batting performance
  batting <- wpa_data %>%
    dplyr::group_by(match_id, batter_id) %>%
    dplyr::summarise(
      deliveries = dplyr::n(),
      total_wpa = sum(batter_wpa, na.rm = TRUE),
      total_era = if ("batter_era" %in% names(wpa_data)) sum(batter_era, na.rm = TRUE) else NA_real_,
      key_moment_wpa = max(abs(batter_wpa), na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::rename(player_id = batter_id) %>%
    dplyr::mutate(role = "batting")

  # Aggregate bowling performance
  bowling <- wpa_data %>%
    dplyr::group_by(match_id, bowler_id) %>%
    dplyr::summarise(
      deliveries = dplyr::n(),
      total_wpa = sum(bowler_wpa, na.rm = TRUE),
      total_era = if ("bowler_era" %in% names(wpa_data)) sum(bowler_era, na.rm = TRUE) else NA_real_,
      key_moment_wpa = max(abs(bowler_wpa), na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::rename(player_id = bowler_id) %>%
    dplyr::mutate(role = "bowling")

  # Combine
  result <- dplyr::bind_rows(batting, bowling)

  # Reorder columns
  result <- result[, c("match_id", "player_id", "role", "deliveries",
                       "total_wpa", "total_era", "key_moment_wpa")]

  return(result)
}


#' Calculate Player Career/Season WPA Statistics
#'
#' Aggregates WPA across matches for career or season-level analysis.
#'
#' @param wpa_data data.frame with delivery-level WPA data
#' @param role Character. Filter by role: "batting", "bowling", or "both"
#' @param group_by Character vector. Grouping variables (default: player only).
#'   Can include "season", "match_type" for additional grouping.
#'
#' @return data.frame with aggregated WPA statistics:
#'   \itemize{
#'     \item player_id: Player identifier
#'     \item role: "batting" or "bowling"
#'     \item matches: Number of matches
#'     \item deliveries: Total deliveries
#'     \item total_wpa: Career/season total WPA
#'     \item wpa_per_delivery: Average WPA per delivery
#'     \item wpa_per_match: Average WPA per match
#'     \item positive_wpa_pct: Percentage of positive WPA deliveries
#'     \item clutch_moments: Number of high-impact deliveries (|WPA| > 0.05)
#'   }
#'
#' @keywords internal
calculate_player_wpa <- function(wpa_data, role = "both", group_by = "player_id") {

  wpa_data <- as.data.frame(wpa_data)

  results <- list()

  if (role %in% c("batting", "both")) {
    # Batting WPA aggregation
    batting_agg <- wpa_data %>%
      dplyr::group_by(dplyr::across(dplyr::any_of(c(group_by, "season", "match_type"))),
                      batter_id) %>%
      dplyr::summarise(
        matches = dplyr::n_distinct(match_id),
        deliveries = dplyr::n(),
        total_wpa = sum(batter_wpa, na.rm = TRUE),
        positive_wpa_pct = mean(batter_wpa > 0, na.rm = TRUE) * 100,
        clutch_moments = sum(abs(batter_wpa) > 0.05, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::rename(player_id = batter_id) %>%
      dplyr::mutate(
        role = "batting",
        wpa_per_delivery = total_wpa / deliveries,
        wpa_per_match = total_wpa / matches
      )

    results <- c(results, list(batting_agg))
  }

  if (role %in% c("bowling", "both")) {
    # Bowling WPA aggregation
    bowling_agg <- wpa_data %>%
      dplyr::group_by(dplyr::across(dplyr::any_of(c(group_by, "season", "match_type"))),
                      bowler_id) %>%
      dplyr::summarise(
        matches = dplyr::n_distinct(match_id),
        deliveries = dplyr::n(),
        total_wpa = sum(bowler_wpa, na.rm = TRUE),
        positive_wpa_pct = mean(bowler_wpa > 0, na.rm = TRUE) * 100,
        clutch_moments = sum(abs(bowler_wpa) > 0.05, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::rename(player_id = bowler_id) %>%
      dplyr::mutate(
        role = "bowling",
        wpa_per_delivery = total_wpa / deliveries,
        wpa_per_match = total_wpa / matches
      )

    results <- c(results, list(bowling_agg))
  }

  # Combine and sort
  combined <- dplyr::bind_rows(results) %>%
    dplyr::arrange(role, dplyr::desc(total_wpa))

  return(combined)
}


#' Calculate Player Career/Season ERA Statistics
#'
#' Aggregates ERA across matches for career or season-level analysis.
#'
#' @param era_data data.frame with delivery-level ERA data
#' @param role Character. Filter by role: "batting", "bowling", or "both"
#' @param group_by Character vector. Grouping variables.
#'
#' @return data.frame with aggregated ERA statistics
#'
#' @keywords internal
calculate_player_era <- function(era_data, role = "both", group_by = "player_id") {

  era_data <- as.data.frame(era_data)

  results <- list()

  if (role %in% c("batting", "both")) {
    batting_agg <- era_data %>%
      dplyr::group_by(dplyr::across(dplyr::any_of(c(group_by, "season", "match_type"))),
                      batter_id) %>%
      dplyr::summarise(
        matches = dplyr::n_distinct(match_id),
        deliveries = dplyr::n(),
        total_era = sum(batter_era, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::rename(player_id = batter_id) %>%
      dplyr::mutate(
        role = "batting",
        era_per_delivery = total_era / deliveries,
        era_per_match = total_era / matches
      )

    results <- c(results, list(batting_agg))
  }

  if (role %in% c("bowling", "both")) {
    bowling_agg <- era_data %>%
      dplyr::group_by(dplyr::across(dplyr::any_of(c(group_by, "season", "match_type"))),
                      bowler_id) %>%
      dplyr::summarise(
        matches = dplyr::n_distinct(match_id),
        deliveries = dplyr::n(),
        total_era = sum(bowler_era, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::rename(player_id = bowler_id) %>%
      dplyr::mutate(
        role = "bowling",
        era_per_delivery = total_era / deliveries,
        era_per_match = total_era / matches
      )

    results <- c(results, list(bowling_agg))
  }

  combined <- dplyr::bind_rows(results) %>%
    dplyr::arrange(role, dplyr::desc(total_era))

  return(combined)
}
