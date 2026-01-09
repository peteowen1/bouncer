# Agnostic Outcome Model Functions
#
# Functions for loading and predicting with the agnostic delivery outcome model.
# The agnostic model predicts outcomes using ONLY context features - no player,
# team, or venue identity. This serves as the baseline expectation for calculating
# residual-based skill indices.
#
# Features used: over, ball, wickets, runs_diff, phase, innings, format, gender,
#                knockout, event_tier
# EXCLUDES: player identity, team identity, venue identity


#' Load Agnostic Outcome Model
#'
#' Loads the trained agnostic outcome prediction model for a given format.
#' The agnostic model predicts delivery outcomes using only context features,
#' serving as the baseline for residual-based skill index calculations.
#'
#' @param format Character. Format type: "t20", "odi", or "test"
#' @param model_dir Character. Directory where models are stored.
#'   If NULL (default), automatically finds bouncerdata/models directory.
#'
#' @return Loaded XGBoost model object (xgb.Booster)
#'
#' @details
#' The agnostic model differs from the full model in that it uses ONLY
#' context features (match state) with no player/team/venue information.
#' This makes it suitable for calculating baseline expectations, where
#' actual performance minus expected gives the "skill residual".
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Load T20 agnostic model
#' model <- load_agnostic_model("t20")
#'
#' # Predict on delivery data
#' probs <- predict_agnostic_outcome(model, deliveries, format = "t20")
#' exp_runs <- get_agnostic_expected_runs(probs)
#' }
load_agnostic_model <- function(format = c("t20", "odi", "test"),
                                 model_dir = NULL) {

  format <- match.arg(format)

  if (is.null(model_dir)) {
    model_dir <- get_models_dir()
  }

  # Agnostic models use XGBoost (for consistency and speed)
  model_file <- file.path(model_dir, sprintf("agnostic_outcome_%s.ubj", format))

  if (!file.exists(model_file)) {
    stop(sprintf(
      "Agnostic model not found at: %s\nRun data-raw/models/ball-outcome/01_train_agnostic_model.R first.",
      model_file
    ))
  }

  if (!requireNamespace("xgboost", quietly = TRUE)) {
    stop("Package 'xgboost' is required. Please install it.")
  }

  model <- xgboost::xgb.load(model_file)

  cli::cli_alert_success("Loaded agnostic {format} model from {.file {model_file}}")

  return(model)
}


#' Predict Agnostic Outcome Probabilities
#'
#' Generates outcome probability predictions using the agnostic model.
#' Returns a matrix of 7-class probabilities (wicket, 0, 1, 2, 3, 4, 6 runs).
#'
#' @param model XGBoost model object from load_agnostic_model()
#' @param delivery_data Data frame of deliveries with required features:
#'   match_type, innings, over, ball, wickets_fallen, runs_difference,
#'   gender, and optionally: is_knockout, event_tier
#' @param format Character. Format type: "t20", "odi", or "test"
#'
#' @return Matrix with 7 columns representing probabilities for each outcome:
#'   [1] P(wicket), [2] P(0 runs), [3] P(1 run), [4] P(2 runs),
#'   [5] P(3 runs), [6] P(4 runs), [7] P(6 runs)
#'
#' @export
#'
#' @examples
#' \dontrun{
#' model <- load_agnostic_model("t20")
#' probs <- predict_agnostic_outcome(model, deliveries, "t20")
#' exp_runs <- get_agnostic_expected_runs(probs)
#' }
predict_agnostic_outcome <- function(model, delivery_data, format = c("t20", "odi", "test")) {

  format <- match.arg(format)

  if (!requireNamespace("xgboost", quietly = TRUE)) {
    stop("Package 'xgboost' is required. Please install it.")
  }

  # Prepare features for agnostic model
  features <- prepare_agnostic_features(delivery_data, format)

  # Create DMatrix and predict
  dmat <- xgboost::xgb.DMatrix(data = as.matrix(features))
  probs <- predict(model, dmat)

  # Ensure probabilities sum to 1 (numerical precision fix)
  probs <- probs / rowSums(probs)

  return(probs)
}


#' Get Expected Runs from Agnostic Model Predictions
#'
#' Converts the 7-class probability distribution into expected runs.
#'
#' @param probs Matrix of probabilities from predict_agnostic_outcome()
#'
#' @return Numeric vector of expected runs per delivery.
#'   Formula: E(runs) = 0*P(wicket) + 0*P(0) + 1*P(1) + 2*P(2) + 3*P(3) + 4*P(4) + 6*P(6)
#'
#' @export
get_agnostic_expected_runs <- function(probs) {
  # Delegate to the existing function in expected_outcomes.R
  calculate_expected_runs(probs)
}


#' Get Expected Wicket Probability from Agnostic Model Predictions
#'
#' Extracts the wicket probability (first column) from the outcome distribution.
#'
#' @param probs Matrix of probabilities from predict_agnostic_outcome()
#'
#' @return Numeric vector of wicket probabilities.
#'
#' @export
get_agnostic_expected_wicket <- function(probs) {
  # Delegate to the existing function in expected_outcomes.R
  calculate_expected_wicket_prob(probs)
}


#' Calculate Skill Residual from Agnostic Model
#'
#' Calculates the residual (actual - expected) for each delivery,
#' which is used to update skill indices.
#'
#' @param model XGBoost model object from load_agnostic_model()
#' @param delivery_data Data frame with deliveries. Must include:
#'   - runs_batter or runs_total: actual runs scored
#'   - is_wicket: whether a wicket fell (logical or 0/1)
#'   - All features required by predict_agnostic_outcome()
#' @param format Character. Format type: "t20", "odi", or "test"
#'
#' @return Data frame with columns:
#'   - exp_runs_agnostic: Expected runs from agnostic model
#'   - exp_wicket_agnostic: Expected wicket probability from agnostic model
#'   - runs_residual: actual_runs - exp_runs_agnostic
#'   - wicket_residual: is_wicket - exp_wicket_agnostic
#'
#' @export
#'
#' @examples
#' \dontrun{
#' model <- load_agnostic_model("t20")
#' residuals <- calculate_agnostic_residuals(model, deliveries, "t20")
#'
#' # Use residuals to update skill indices (EMA formula)
#' new_bsi <- (1 - alpha) * old_bsi + alpha * residuals$runs_residual
#' }
calculate_agnostic_residuals <- function(model, delivery_data, format = c("t20", "odi", "test")) {

  format <- match.arg(format)

  # Get predictions
  probs <- predict_agnostic_outcome(model, delivery_data, format)
  exp_runs <- get_agnostic_expected_runs(probs)
  exp_wicket <- get_agnostic_expected_wicket(probs)

  # Get actual values
  actual_runs <- if ("runs_batter" %in% names(delivery_data)) {
    delivery_data$runs_batter
  } else if ("runs_total" %in% names(delivery_data)) {
    delivery_data$runs_total
  } else {
    stop("delivery_data must have 'runs_batter' or 'runs_total' column")
  }

  if (!"is_wicket" %in% names(delivery_data)) {
    stop("delivery_data must have 'is_wicket' column")
  }
  actual_wicket <- as.integer(delivery_data$is_wicket)

  # Calculate residuals
  data.frame(
    exp_runs_agnostic = exp_runs,
    exp_wicket_agnostic = exp_wicket,
    runs_residual = actual_runs - exp_runs,
    wicket_residual = actual_wicket - exp_wicket
  )
}


# ============================================================================
# Full Model Functions
# ============================================================================
# The full model uses ALL features: context + player + team + venue skills.
# This provides maximum prediction accuracy for simulations.


#' Load Full Outcome Model
#'
#' Loads the trained full outcome prediction model for a given format.
#' The full model uses all available features including player skills,
#' team skills, and venue skills for maximum prediction accuracy.
#'
#' @param format Character. Format type: "t20", "odi", or "test"
#' @param model_dir Character. Directory where models are stored.
#'   If NULL (default), automatically finds bouncerdata/models directory.
#'
#' @return Loaded XGBoost model object (xgb.Booster)
#'
#' @details
#' The full model uses all available features:
#' - Context: over, ball, wickets, runs_diff, phase, innings, format, gender
#' - Player skills: batter/bowler scoring/survival/economy/strike rate
#' - Team skills: batting/bowling team runs/wicket skill
#' - Venue skills: run rate, wicket rate, boundary rate, dot rate
#'
#' This is the model used for match simulation where maximum accuracy is needed.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Load T20 full model
#' model <- load_full_model("t20")
#'
#' # Predict on delivery data with all skill features
#' probs <- predict_full_outcome(model, deliveries, format = "t20")
#' }
load_full_model <- function(format = c("t20", "odi", "test"),
                             model_dir = NULL) {

  format <- match.arg(format)

  if (is.null(model_dir)) {
    model_dir <- get_models_dir()
  }

  model_file <- file.path(model_dir, sprintf("full_outcome_%s.ubj", format))

  if (!file.exists(model_file)) {
    stop(sprintf(
      "Full model not found at: %s\nRun data-raw/models/ball-outcome/02_train_full_model.R first.",
      model_file
    ))
  }

  if (!requireNamespace("xgboost", quietly = TRUE)) {
    stop("Package 'xgboost' is required. Please install it.")
  }

  model <- xgboost::xgb.load(model_file)

  cli::cli_alert_success("Loaded full {format} model from {.file {model_file}}")

  return(model)
}


#' Predict Full Outcome Probabilities
#'
#' Generates outcome probability predictions using the full model with all features.
#' Returns a matrix of 7-class probabilities (wicket, 0, 1, 2, 3, 4, 6 runs).
#'
#' @param model XGBoost model object from load_full_model()
#' @param delivery_data Data frame of deliveries with required features.
#'   Must include all context, player, team, and venue skill features.
#' @param format Character. Format type: "t20", "odi", or "test"
#'
#' @return Matrix with 7 columns representing probabilities for each outcome:
#'   [1] P(wicket), [2] P(0 runs), [3] P(1 run), [4] P(2 runs),
#'   [5] P(3 runs), [6] P(4 runs), [7] P(6 runs)
#'
#' @export
#'
#' @examples
#' \dontrun{
#' model <- load_full_model("t20")
#' probs <- predict_full_outcome(model, deliveries, "t20")
#' exp_runs <- get_full_expected_runs(probs)
#' }
predict_full_outcome <- function(model, delivery_data, format = c("t20", "odi", "test")) {

  format <- match.arg(format)

  if (!requireNamespace("xgboost", quietly = TRUE)) {
    stop("Package 'xgboost' is required. Please install it.")
  }

  # Prepare features for full model
  features <- prepare_full_features(delivery_data, format)

  # Create DMatrix and predict
  dmat <- xgboost::xgb.DMatrix(data = as.matrix(features))
  probs <- predict(model, dmat)

  # Ensure probabilities sum to 1 (numerical precision fix)
  probs <- probs / rowSums(probs)

  return(probs)
}


#' Get Expected Runs from Full Model Predictions
#'
#' Converts the 7-class probability distribution into expected runs.
#'
#' @param probs Matrix of probabilities from predict_full_outcome()
#'
#' @return Numeric vector of expected runs per delivery.
#'
#' @export
get_full_expected_runs <- function(probs) {
  calculate_expected_runs(probs)
}


#' Get Expected Wicket Probability from Full Model Predictions
#'
#' Extracts the wicket probability (first column) from the outcome distribution.
#'
#' @param probs Matrix of probabilities from predict_full_outcome()
#'
#' @return Numeric vector of wicket probabilities.
#'
#' @export
get_full_expected_wicket <- function(probs) {
  calculate_expected_wicket_prob(probs)
}


# ============================================================================
# Internal Helper Functions
# ============================================================================


#' Prepare Full Features for XGBoost Prediction
#'
#' Prepares the feature matrix for the full model (all features).
#'
#' @param df Data frame with delivery data including all skill indices
#' @param format Character. Format type: "t20", "odi", or "test"
#'
#' @return Data frame with features ready for XGBoost
#' @keywords internal
prepare_full_features <- function(df, format) {

  df <- as.data.frame(df)

  # Calculate derived features if not present
  if (!"over_ball" %in% names(df)) {
    df$over_ball <- df$over + df$ball / 6
  }

  # Fill missing skill indices with neutral values
  # Player skills - use starting values if missing
  df$batter_scoring_index <- dplyr::coalesce(df$batter_scoring_index, 1.25)
  df$batter_survival_rate <- dplyr::coalesce(df$batter_survival_rate, 0.975)
  df$bowler_economy_index <- dplyr::coalesce(df$bowler_economy_index, 1.25)
  df$bowler_strike_rate <- dplyr::coalesce(df$bowler_strike_rate, 0.025)
  df$batter_balls_faced <- dplyr::coalesce(df$batter_balls_faced, 0)
  df$bowler_balls_bowled <- dplyr::coalesce(df$bowler_balls_bowled, 0)

  # Team skills - 0 is neutral for residual-based
  df$batting_team_runs_skill <- dplyr::coalesce(df$batting_team_runs_skill, 0)
  df$batting_team_wicket_skill <- dplyr::coalesce(df$batting_team_wicket_skill, 0)
  df$bowling_team_runs_skill <- dplyr::coalesce(df$bowling_team_runs_skill, 0)
  df$bowling_team_wicket_skill <- dplyr::coalesce(df$bowling_team_wicket_skill, 0)

  # Venue skills
  start_vals <- get_venue_start_values(format)
  df$venue_run_rate <- dplyr::coalesce(df$venue_run_rate, 0)
  df$venue_wicket_rate <- dplyr::coalesce(df$venue_wicket_rate, 0)
  df$venue_boundary_rate <- dplyr::coalesce(df$venue_boundary_rate, start_vals$boundary_rate)
  df$venue_dot_rate <- dplyr::coalesce(df$venue_dot_rate, start_vals$dot_rate)

  # Format-specific feature engineering
  if (format %in% c("t20", "odi")) {
    # Short-form features

    # Overs left
    if (!"overs_left" %in% names(df)) {
      df$overs_left <- dplyr::case_when(
        format == "t20" ~ pmax(0, 20 - df$over_ball),
        format == "odi" ~ pmax(0, 50 - df$over_ball),
        TRUE ~ NA_real_
      )
    }

    # Phase
    if (!"phase" %in% names(df)) {
      df$phase <- dplyr::case_when(
        format == "t20" & df$over < 6 ~ "powerplay",
        format == "t20" & df$over < 16 ~ "middle",
        format == "t20" ~ "death",
        format == "odi" & df$over < 10 ~ "powerplay",
        format == "odi" & df$over < 40 ~ "middle",
        format == "odi" ~ "death",
        TRUE ~ "middle"
      )
    }

    # Create feature matrix
    result <- df %>%
      dplyr::mutate(
        format_t20 = as.integer(format == "t20"),
        format_odi = as.integer(format == "odi"),
        phase_powerplay = as.integer(phase == "powerplay"),
        phase_middle = as.integer(phase == "middle"),
        phase_death = as.integer(phase == "death"),
        gender_male = as.integer(tolower(gender) == "male"),
        innings_num = as.integer(as.character(innings)),
        is_knockout = as.integer(dplyr::coalesce(as.integer(is_knockout), 0L)),
        event_tier = dplyr::coalesce(as.numeric(event_tier), 2),
        batter_experience = log1p(batter_balls_faced),
        bowler_experience = log1p(bowler_balls_bowled)
      )

    # Select features in the correct order (must match training)
    result <- result %>%
      dplyr::select(
        # Context features
        format_t20, format_odi,
        innings_num, over, ball,
        wickets_fallen, runs_difference, overs_left,
        phase_powerplay, phase_middle, phase_death,
        gender_male,
        is_knockout, event_tier,
        # Player skills
        batter_scoring_index, batter_survival_rate,
        bowler_economy_index, bowler_strike_rate,
        batter_experience, bowler_experience,
        # Team skills
        batting_team_runs_skill, batting_team_wicket_skill,
        bowling_team_runs_skill, bowling_team_wicket_skill,
        # Venue skills
        venue_run_rate, venue_wicket_rate,
        venue_boundary_rate, venue_dot_rate
      )

  } else {
    # Long-form (Test) features

    # Phase based on ball age
    if (!"phase" %in% names(df)) {
      df$phase <- dplyr::case_when(
        df$over < 20 ~ "new_ball",
        df$over < 80 ~ "middle",
        TRUE ~ "old_ball"
      )
    }

    # Create feature matrix
    result <- df %>%
      dplyr::mutate(
        phase_new_ball = as.integer(phase == "new_ball"),
        phase_middle = as.integer(phase == "middle"),
        phase_old_ball = as.integer(phase == "old_ball"),
        gender_male = as.integer(tolower(gender) == "male"),
        innings_num = as.integer(as.character(innings)),
        is_knockout = as.integer(dplyr::coalesce(as.integer(is_knockout), 0L)),
        event_tier = dplyr::coalesce(as.numeric(event_tier), 2),
        batter_experience = log1p(batter_balls_faced),
        bowler_experience = log1p(bowler_balls_bowled)
      )

    # Select features (no overs_left for Test)
    result <- result %>%
      dplyr::select(
        # Context features
        innings_num, over, ball,
        wickets_fallen, runs_difference,
        phase_new_ball, phase_middle, phase_old_ball,
        gender_male,
        is_knockout, event_tier,
        # Player skills
        batter_scoring_index, batter_survival_rate,
        bowler_economy_index, bowler_strike_rate,
        batter_experience, bowler_experience,
        # Team skills
        batting_team_runs_skill, batting_team_wicket_skill,
        bowling_team_runs_skill, bowling_team_wicket_skill,
        # Venue skills
        venue_run_rate, venue_wicket_rate,
        venue_boundary_rate, venue_dot_rate
      )
  }

  # Handle any NA values
  result <- result %>%
    dplyr::mutate(
      dplyr::across(dplyr::everything(), ~ dplyr::coalesce(., 0))
    )

  return(result)
}


#' Prepare Agnostic Features for XGBoost Prediction
#'
#' Prepares the feature matrix for the agnostic model (context-only features).
#'
#' @param df Data frame with delivery data
#' @param format Character. Format type: "t20", "odi", or "test"
#'
#' @return Data frame with features ready for XGBoost
#' @keywords internal
prepare_agnostic_features <- function(df, format) {

  df <- as.data.frame(df)

  # Calculate derived features if not present
  if (!"over_ball" %in% names(df)) {
    df$over_ball <- df$over + df$ball / 6
  }

  # Format-specific feature engineering
  if (format %in% c("t20", "odi")) {
    # Short-form features

    # Overs left
    if (!"overs_left" %in% names(df)) {
      df$overs_left <- dplyr::case_when(
        format == "t20" ~ pmax(0, 20 - df$over_ball),
        format == "odi" ~ pmax(0, 50 - df$over_ball),
        TRUE ~ NA_real_
      )
    }

    # Phase
    if (!"phase" %in% names(df)) {
      df$phase <- dplyr::case_when(
        format == "t20" & df$over < 6 ~ "powerplay",
        format == "t20" & df$over < 16 ~ "middle",
        format == "t20" ~ "death",
        format == "odi" & df$over < 10 ~ "powerplay",
        format == "odi" & df$over < 40 ~ "middle",
        format == "odi" ~ "death",
        TRUE ~ "middle"
      )
    }

    # Create dummy variables
    result <- df %>%
      dplyr::mutate(
        format_t20 = as.integer(format == "t20"),
        format_odi = as.integer(format == "odi"),
        phase_powerplay = as.integer(phase == "powerplay"),
        phase_middle = as.integer(phase == "middle"),
        phase_death = as.integer(phase == "death"),
        gender_male = as.integer(tolower(gender) == "male"),
        innings_num = as.integer(as.character(innings)),
        # Optional context features (default to 0 if not present)
        is_knockout = as.integer(dplyr::coalesce(as.integer(is_knockout), 0L)),
        event_tier = dplyr::coalesce(as.numeric(event_tier), 2)  # Default tier 2
      )

    # Select features
    result <- result %>%
      dplyr::select(
        format_t20, format_odi,
        innings_num, over, ball,
        wickets_fallen, runs_difference, overs_left,
        phase_powerplay, phase_middle, phase_death,
        gender_male,
        is_knockout, event_tier
      )

  } else {
    # Long-form (Test) features

    # Phase based on ball age
    if (!"phase" %in% names(df)) {
      df$phase <- dplyr::case_when(
        df$over < 20 ~ "new_ball",
        df$over < 80 ~ "middle",
        TRUE ~ "old_ball"
      )
    }

    # Create dummy variables
    result <- df %>%
      dplyr::mutate(
        phase_new_ball = as.integer(phase == "new_ball"),
        phase_middle = as.integer(phase == "middle"),
        phase_old_ball = as.integer(phase == "old_ball"),
        gender_male = as.integer(tolower(gender) == "male"),
        innings_num = as.integer(as.character(innings)),
        # Optional context features
        is_knockout = as.integer(dplyr::coalesce(as.integer(is_knockout), 0L)),
        event_tier = dplyr::coalesce(as.numeric(event_tier), 2)
      )

    # Select features (no overs_left for Test)
    result <- result %>%
      dplyr::select(
        innings_num, over, ball,
        wickets_fallen, runs_difference,
        phase_new_ball, phase_middle, phase_old_ball,
        gender_male,
        is_knockout, event_tier
      )
  }

  # Handle any NA values
  result <- result %>%
    dplyr::mutate(
      dplyr::across(dplyr::everything(), ~ dplyr::coalesce(., 0))
    )

  return(result)
}
