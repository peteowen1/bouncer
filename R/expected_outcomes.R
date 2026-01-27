# Expected Outcome Functions (Player/Team Agnostic)
#
# Functions for calculating expected runs and wicket probability from
# the multinomial outcome model. Based purely on match state - no player info.
#
# Uses the trained BAM or XGBoost models from data-raw/outcome-models/


#' Calculate Expected Runs from Probability Matrix
#'
#' Converts a 7-category probability distribution into expected runs per delivery.
#' Categories: wicket, 0, 1, 2, 3, 4, 6 runs.
#'
#' @param probs Matrix or data frame with 7 columns representing probabilities
#'   for (wicket, 0, 1, 2, 3, 4, 6). Column order must match this exactly.
#'
#' @return Numeric vector of expected runs per delivery.
#'   Formula: E(runs) = sum(prob_i * runs_i) where runs = c(0, 0, 1, 2, 3, 4, 6)
#'
#' @keywords internal
calculate_expected_runs <- function(probs) {
  # Run values for each category: wicket=0, dot=0, 1, 2, 3, 4, 6
  runs_values <- c(0, 0, 1, 2, 3, 4, 6)

  probs <- as.matrix(probs)

  if (ncol(probs) != 7) {
    cli::cli_abort("probs must have 7 columns: (wicket, 0, 1, 2, 3, 4, 6)")
  }

  as.vector(probs %*% runs_values)
}


#' Calculate Expected Wicket Probability from Probability Matrix
#'
#' Extracts the wicket probability (first column) from the outcome distribution.
#'
#' @param probs Matrix or data frame with 7 columns representing probabilities.
#'   First column must be P(wicket).
#'
#' @return Numeric vector of wicket probabilities.
#'
#' @keywords internal
calculate_expected_wicket_prob <- function(probs) {
  probs <- as.matrix(probs)

  if (ncol(probs) < 1) {
    cli::cli_abort("probs must have at least 1 column")
  }

  probs[, 1]
}


#' Predict Delivery Outcomes Using Trained Model
#'
#' Generates outcome probability predictions for deliveries using a trained
#' BAM or XGBoost model. Returns expected runs, wicket probability, and
#' full probability distribution.
#'
#' @param model Trained model object (BAM from mgcv or XGBoost)
#' @param delivery_data Data frame of deliveries with required features.
#'   For shortform (T20/ODI): match_type, innings, over, ball, over_ball,
#'   wickets_fallen, runs_difference, venue, gender.
#'   For longform (Test): innings, over, ball, wickets_fallen, runs_difference,
#'   venue, gender.
#' @param model_type Character. Either "bam" or "xgb"
#' @param format Character. Either "shortform" (T20/ODI) or "longform" (Test)
#'
#' @return Data frame with columns:
#'   \itemize{
#'     \item exp_runs: Expected runs this delivery
#'     \item exp_wicket_prob: Probability of wicket
#'     \item prob_wicket, prob_0, prob_1, prob_2, prob_3, prob_4, prob_6:
#'       Individual outcome probabilities
#'   }
#'
#' @keywords internal
predict_delivery_outcomes <- function(model,
                                       delivery_data,
                                       model_type = c("xgb", "bam"),
                                       format = c("shortform", "longform")) {

  model_type <- match.arg(model_type)
  format <- match.arg(format)

  # Engineer features based on format
  if (format == "shortform") {
    pred_data <- prepare_shortform_features(delivery_data)
  } else {
    pred_data <- prepare_longform_features(delivery_data)
  }

  # Generate predictions based on model type
  if (model_type == "bam") {
    if (!requireNamespace("mgcv", quietly = TRUE)) {
      cli::cli_abort("Package {.pkg mgcv} is required for BAM models")
    }
    probs <- predict(model, newdata = pred_data, type = "response")
  } else {
    if (!requireNamespace("xgboost", quietly = TRUE)) {
      cli::cli_abort("Package {.pkg xgboost} is required for XGBoost models")
    }

    # Prepare feature matrix for XGBoost
    if (format == "shortform") {
      features <- prepare_xgb_shortform_features(pred_data)
    } else {
      features <- prepare_xgb_longform_features(pred_data)
    }

    dmat <- xgboost::xgb.DMatrix(data = as.matrix(features))
    probs <- predict(model, dmat)
  }

  # Calculate expected values
  exp_runs <- calculate_expected_runs(probs)
  exp_wicket_prob <- calculate_expected_wicket_prob(probs)

  # Build result data frame
  data.frame(
    exp_runs = exp_runs,
    exp_wicket_prob = exp_wicket_prob,
    prob_wicket = probs[, 1],
    prob_0 = probs[, 2],
    prob_1 = probs[, 3],
    prob_2 = probs[, 4],
    prob_3 = probs[, 5],
    prob_4 = probs[, 6],
    prob_6 = probs[, 7]
  )
}


#' Add Expected Outcome Features to Data Frame
#'
#' Convenience function that loads a model and adds expected outcome columns
#' to a delivery data frame. Designed for use in game-modelling scripts.
#'
#' @param df Data frame of deliveries
#' @param model Pre-loaded model object (optional). If NULL, loads from model_path.
#' @param model_path Path to saved model file (used if model is NULL)
#' @param model_type Character. Either "bam" or "xgb"
#' @param format Character. Either "shortform" or "longform"
#'
#' @return Data frame with additional columns:
#'   \itemize{
#'     \item exp_runs: Expected runs per delivery
#'     \item exp_wicket_prob: Expected wicket probability
#'     \item runs_above_expected: runs_batter - exp_runs
#'     \item wicket_above_expected: is_wicket - exp_wicket_prob (1 if wicket, 0 otherwise)
#'   }
#'
#' @keywords internal
add_expected_outcome_features <- function(df,
                                           model = NULL,
                                           model_path = NULL,
                                           model_type = c("xgb", "bam"),
                                           format = c("shortform", "longform")) {

  model_type <- match.arg(model_type)
  format <- match.arg(format)

  # Load model if not provided
  if (is.null(model)) {
    if (is.null(model_path)) {
      # Use default path
      model <- load_outcome_model(format, model_type)
    } else {
      if (model_type == "bam") {
        model <- readRDS(model_path)
      } else {
        model <- xgboost::xgb.load(model_path)
      }
    }
  }

  cli::cli_alert_info("Generating expected outcome predictions...")

  # Get predictions
  outcomes <- predict_delivery_outcomes(model, df, model_type, format)

  # Add columns to data frame

  df$exp_runs <- outcomes$exp_runs
  df$exp_wicket_prob <- outcomes$exp_wicket_prob

  # Calculate above/below expected
  if ("runs_batter" %in% names(df)) {
    df$runs_above_expected <- df$runs_batter - df$exp_runs
  } else if ("runs_total" %in% names(df)) {
    df$runs_above_expected <- df$runs_total - df$exp_runs
  }

  if ("is_wicket" %in% names(df)) {
    df$wicket_above_expected <- as.integer(df$is_wicket) - df$exp_wicket_prob
  }

  cli::cli_alert_success("Added expected outcome features: exp_runs, exp_wicket_prob, runs_above_expected, wicket_above_expected")

  df
}


# Internal helper functions for feature engineering


#' Prepare Short-form Features for Prediction
#' @keywords internal
prepare_shortform_features <- function(df) {
  df <- as.data.frame(df)

  # Calculate derived features if not present
  if (!"over_ball" %in% names(df)) {
    df$over_ball <- df$over + df$ball / 6
  }

  if (!"overs_left" %in% names(df)) {
    df$overs_left <- dplyr::case_when(
      tolower(df$match_type) == "t20" ~ pmax(0, 20 - df$over_ball),
      tolower(df$match_type) == "odi" ~ pmax(0, 50 - df$over_ball),
      TRUE ~ NA_real_
    )
  }

  if (!"phase" %in% names(df)) {
    df$phase <- dplyr::case_when(
      tolower(df$match_type) == "t20" & df$over < 6 ~ "powerplay",
      tolower(df$match_type) == "t20" & df$over < 16 ~ "middle",
      tolower(df$match_type) == "t20" ~ "death",
      tolower(df$match_type) == "odi" & df$over < 10 ~ "powerplay",
      tolower(df$match_type) == "odi" & df$over < 40 ~ "middle",
      tolower(df$match_type) == "odi" ~ "death",
      TRUE ~ "middle"
    )
  }

  # Convert to factors as expected by BAM model
  df$phase <- factor(df$phase, levels = c("powerplay", "middle", "death"))
  df$match_type <- factor(tolower(df$match_type))
  df$innings <- factor(df$innings)
  df$venue <- factor(df$venue)
  df$gender <- factor(df$gender)

  df
}


#' Prepare Long-form Features for Prediction
#' @keywords internal
prepare_longform_features <- function(df) {
  df <- as.data.frame(df)

  if (!"phase" %in% names(df)) {
    df$phase <- dplyr::case_when(
      df$over < 20 ~ "new_ball",
      df$over < 80 ~ "middle",
      TRUE ~ "old_ball"
    )
  }

  df$phase <- factor(df$phase, levels = c("new_ball", "middle", "old_ball"))
  df$match_type <- factor(tolower(df$match_type))
  df$innings <- factor(df$innings)
  df$venue <- factor(df$venue)
  df$gender <- factor(df$gender)

  df
}


#' Prepare XGBoost Short-form Feature Matrix
#'
#' Prepares features for XGBoost outcome prediction.
#' If venue skill features are present in the data, they are included.
#'
#' @param df Data frame with delivery data
#' @param include_venue Logical. If TRUE and venue features present, include them.
#'   Default TRUE.
#'
#' @return Data frame with features for XGBoost
#' @keywords internal
prepare_xgb_shortform_features <- function(df, include_venue = TRUE) {

  # Base feature engineering
  result <- df %>%
    dplyr::mutate(
      match_type_t20 = as.integer(match_type == "t20"),
      match_type_odi = as.integer(match_type == "odi"),
      phase_powerplay = as.integer(phase == "powerplay"),
      phase_middle = as.integer(phase == "middle"),
      phase_death = as.integer(phase == "death"),
      gender_male = as.integer(gender == "male"),
      innings_num = as.integer(as.character(innings))
    )

  # Base columns always included
  base_cols <- c(
    "match_type_t20", "match_type_odi",
    "innings_num", "over", "ball",
    "wickets_fallen", "runs_difference", "overs_left",
    "phase_powerplay", "phase_middle", "phase_death",
    "gender_male"
  )

  # Check if venue features are present and should be included
  venue_cols <- c("venue_run_rate", "venue_wicket_rate",
                  "venue_boundary_rate", "venue_dot_rate", "venue_balls")
  has_venue <- all(venue_cols %in% names(df)) && include_venue

  if (has_venue) {
    # Fill missing venue values with format-appropriate defaults
    # Use T20 defaults since this is shortform
    result <- result %>%
      dplyr::mutate(
        venue_run_rate = dplyr::coalesce(venue_run_rate, VENUE_START_RUN_RATE_T20),
        venue_wicket_rate = dplyr::coalesce(venue_wicket_rate, VENUE_START_WICKET_RATE_T20),
        venue_boundary_rate = dplyr::coalesce(venue_boundary_rate, VENUE_START_BOUNDARY_RATE_T20),
        venue_dot_rate = dplyr::coalesce(venue_dot_rate, VENUE_START_DOT_RATE_T20),
        venue_balls = dplyr::coalesce(venue_balls, 0L),
        venue_reliable = as.integer(venue_balls >= VENUE_MIN_BALLS_T20)
      )

    # Include venue columns
    all_cols <- c(base_cols, "venue_run_rate", "venue_wicket_rate",
                  "venue_boundary_rate", "venue_dot_rate", "venue_reliable")
  } else {
    all_cols <- base_cols
  }

  result %>%
    dplyr::select(dplyr::all_of(all_cols))
}


#' Prepare XGBoost Long-form Feature Matrix
#'
#' Prepares features for XGBoost outcome prediction (Test/multi-day matches).
#' If venue skill features are present in the data, they are included.
#'
#' @param df Data frame with delivery data
#' @param include_venue Logical. If TRUE and venue features present, include them.
#'   Default TRUE.
#'
#' @return Data frame with features for XGBoost
#' @keywords internal
prepare_xgb_longform_features <- function(df, include_venue = TRUE) {

  # Base feature engineering
  result <- df %>%
    dplyr::mutate(
      phase_new_ball = as.integer(phase == "new_ball"),
      phase_middle = as.integer(phase == "middle"),
      phase_old_ball = as.integer(phase == "old_ball"),
      gender_male = as.integer(gender == "male"),
      innings_num = as.integer(as.character(innings))
    )

  # Base columns always included
  base_cols <- c(
    "innings_num", "over", "ball",
    "wickets_fallen", "runs_difference",
    "phase_new_ball", "phase_middle", "phase_old_ball",
    "gender_male"
  )

  # Check if venue features are present and should be included
  venue_cols <- c("venue_run_rate", "venue_wicket_rate",
                  "venue_boundary_rate", "venue_dot_rate", "venue_balls")
  has_venue <- all(venue_cols %in% names(df)) && include_venue

  if (has_venue) {
    # Fill missing venue values with Test format defaults
    result <- result %>%
      dplyr::mutate(
        venue_run_rate = dplyr::coalesce(venue_run_rate, VENUE_START_RUN_RATE_TEST),
        venue_wicket_rate = dplyr::coalesce(venue_wicket_rate, VENUE_START_WICKET_RATE_TEST),
        venue_boundary_rate = dplyr::coalesce(venue_boundary_rate, VENUE_START_BOUNDARY_RATE_TEST),
        venue_dot_rate = dplyr::coalesce(venue_dot_rate, VENUE_START_DOT_RATE_TEST),
        venue_balls = dplyr::coalesce(venue_balls, 0L),
        venue_reliable = as.integer(venue_balls >= VENUE_MIN_BALLS_TEST)
      )

    # Include venue columns
    all_cols <- c(base_cols, "venue_run_rate", "venue_wicket_rate",
                  "venue_boundary_rate", "venue_dot_rate", "venue_reliable")
  } else {
    all_cols <- base_cols
  }

  result %>%
    dplyr::select(dplyr::all_of(all_cols))
}


#' Get Models Directory
#'
#' Finds the models directory within the bouncerdata directory.
#'
#' @return Character string with path to models directory
#' @keywords internal
get_models_dir <- function() {
  cwd <- getwd()

  potential_paths <- c(
    file.path(dirname(cwd), "bouncerdata", "models"),
    file.path(cwd, "bouncerdata", "models"),
    file.path(cwd, "..", "bouncerdata", "models")
  )

  for (path in potential_paths) {
    if (dir.exists(path)) {
      return(normalizePath(path))
    }
  }

  # Create if not found

  models_dir <- file.path(dirname(cwd), "bouncerdata", "models")
  if (!dir.exists(models_dir)) {
    tryCatch({
      dir.create(models_dir, recursive = TRUE)
      cli::cli_alert_info("Created models directory: {.file {models_dir}}")
      return(normalizePath(models_dir))
    }, error = function(e) {
      # Fall through
    })
  }

  # Fallback
  data_dir <- tools::R_user_dir("bouncerdata", which = "data")
  models_dir <- file.path(data_dir, "models")
  if (!dir.exists(models_dir)) {
    dir.create(models_dir, recursive = TRUE)
  }
  return(normalizePath(models_dir))
}


#' Load Outcome Model
#'
#' Loads a trained outcome prediction model from the models directory.
#'
#' @param format Character. Model format: "shortform" or "longform"
#' @param model_type Character. Model type: "xgb" or "bam"
#' @param model_dir Character. Directory where models are stored.
#'   If NULL (default), automatically finds bouncerdata/models directory.
#'
#' @return Loaded model object (xgb.Booster for XGBoost, bam for BAM)
#' @keywords internal
load_outcome_model <- function(format = c("shortform", "longform"),
                               model_type = c("xgb", "bam"),
                               model_dir = NULL) {

  format <- match.arg(format)
  model_type <- match.arg(model_type)

  if (is.null(model_dir)) {
    model_dir <- get_models_dir()
  }

  if (model_type == "xgb") {
    model_file <- file.path(model_dir, sprintf("model_xgb_outcome_%s.json", format))
    if (!file.exists(model_file)) {
      cli::cli_abort(c(
        "XGBoost model not found at: {.file {model_file}}",
        "i" = "Run model_xgb_outcome_{format}.R to train the model."
      ))
    }
    model <- xgboost::xgb.load(model_file)
  } else {
    model_file <- file.path(model_dir, sprintf("model_bam_outcome_%s.rds", format))
    if (!file.exists(model_file)) {
      cli::cli_abort(c(
        "BAM model not found at: {.file {model_file}}",
        "i" = "Run model_bam_outcome_{format}.R to train the model."
      ))
    }
    model <- readRDS(model_file)
  }

  cli::cli_alert_success("Loaded {model_type} {format} model from {.file {model_file}}")

  return(model)
}
