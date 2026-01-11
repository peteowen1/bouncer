# In-Match Win Probability Functions
#
# User-friendly functions for live/in-match win probability prediction.
# Uses the two-stage model: Stage 1 (Projected Score) -> Stage 2 (Win Probability).

# Model cache environment (internal)
.inmatch_model_cache <- new.env(parent = emptyenv())


#' Load In-Match Prediction Models
#'
#' Loads the trained in-match prediction models for a given format.
#' Models are cached for performance.
#'
#' @param format Character. Match format: "t20", "odi".
#' @param models_path Character. Path to models directory. If NULL, uses default.
#' @param force_reload Logical. If TRUE, reloads models even if cached.
#'
#' @return A list containing:
#'   \itemize{
#'     \item stage1_model - XGBoost model for projected score
#'     \item stage2_model - XGBoost model for win probability
#'     \item stage1_features - Feature column names for Stage 1
#'     \item stage2_features - Feature column names for Stage 2
#'   }
#'
#' @keywords internal
load_in_match_models <- function(format = "t20",
                                  models_path = NULL,
                                  force_reload = FALSE) {

  format <- tolower(format)
  cache_key <- paste0("inmatch_", format)

  # Check cache
  if (!force_reload && exists(cache_key, envir = .inmatch_model_cache)) {
    return(get(cache_key, envir = .inmatch_model_cache))
  }

  # Determine models path
  if (is.null(models_path)) {
    models_path <- get_db_path()
    models_path <- file.path(dirname(models_path), "models")
  }

  # Load Stage 1 results
  stage1_file <- file.path(models_path, paste0(format, "_stage1_results.rds"))
  if (!file.exists(stage1_file)) {
    cli::cli_alert_warning("Stage 1 model not found: {stage1_file}")
    cli::cli_alert_info("Run the in-match pipeline first (data-raw/models/in-match/)")
    return(NULL)
  }

  stage1_results <- readRDS(stage1_file)

  # Load Stage 2 results
  stage2_file <- file.path(models_path, paste0(format, "_stage2_results.rds"))
  if (!file.exists(stage2_file)) {
    cli::cli_alert_warning("Stage 2 model not found: {stage2_file}")
    return(NULL)
  }

  stage2_results <- readRDS(stage2_file)

  result <- list(
    stage1_model = stage1_results$model,
    stage2_model = stage2_results$model,
    stage1_features = stage1_results$feature_cols,
    stage2_features = stage2_results$feature_cols,
    format = format
  )

  # Cache result
  assign(cache_key, result, envir = .inmatch_model_cache)

  cli::cli_alert_success("Loaded in-match models for {toupper(format)}")
  return(result)
}


#' Predict Win Probability from Current Game State
#'
#' Calculate win probability for the batting-first team given the current
#' match state. Uses scoreboard-friendly inputs.
#'
#' @param current_score Integer. Current team score.
#' @param wickets Integer. Wickets fallen (0-10).
#' @param overs Numeric. Overs bowled in cricket notation (e.g., 10.3 = 10 overs + 3 balls).
#' @param innings Integer. Current innings (1 or 2).
#' @param target Integer. Target score (required if innings = 2).
#' @param format Character. Match format: "t20", "odi".
#' @param venue_stats List. Venue-specific statistics (optional).
#'   If NULL, uses format averages.
#' @param skill_adjustments List. Team/player skill adjustments (optional).
#' @param models List. Pre-loaded models from load_in_match_models().
#'   If NULL, models are loaded automatically.
#'
#' @return A `bouncer_win_prob` object containing:
#'   \itemize{
#'     \item win_prob - Win probability for batting-first team (0-1)
#'     \item projected_score - Projected final innings score
#'     \item current_score - Input current score
#'     \item wickets - Input wickets fallen
#'     \item overs - Input overs bowled
#'     \item innings - Input innings number
#'     \item format - Match format
#'   }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # First innings: India 85/2 after 10 overs
#' wp <- predict_win_probability(85, 2, 10.0, innings = 1, format = "t20")
#' print(wp)
#'
#' # Second innings: Chasing 180, currently 100/3 after 12.4 overs
#' wp <- predict_win_probability(100, 3, 12.4, innings = 2, target = 180, format = "t20")
#' print(wp)
#' }
predict_win_probability <- function(current_score,
                                     wickets,
                                     overs,
                                     innings,
                                     target = NULL,
                                     format = "t20",
                                     venue_stats = NULL,
                                     skill_adjustments = NULL,
                                     models = NULL) {

  format <- tolower(format)

  # Validate inputs
  if (!innings %in% c(1, 2)) {
    cli::cli_abort("innings must be 1 or 2")
  }

  if (innings == 2 && is.null(target)) {
    cli::cli_abort("Target is required for 2nd innings predictions")
  }

  if (wickets < 0 || wickets > 10) {
    cli::cli_abort("Wickets must be between 0 and 10")
  }

  # Convert overs to balls
  balls_bowled <- overs_to_balls(overs)

  # Get max balls for format
  max_balls <- switch(format,
    "t20" = 120,
    "it20" = 120,
    "odi" = 300,
    "odm" = 300,
    120
  )

  balls_remaining <- max_balls - balls_bowled
  overs_remaining <- balls_remaining / 6

  # Load models if not provided
  if (is.null(models)) {
    models <- load_in_match_models(format)
    if (is.null(models)) {
      cli::cli_abort("Could not load in-match models")
    }
  }

  # Build feature data frame
  feature_data <- data.frame(
    total_runs = current_score,
    wickets_fallen = wickets,
    wickets_in_hand = 10 - wickets,
    balls_bowled = balls_bowled,
    balls_remaining = balls_remaining,
    overs_completed = balls_bowled / 6,
    overs_remaining = overs_remaining,
    current_run_rate = if (balls_bowled > 0) current_score / (balls_bowled / 6) else 0,
    innings = innings,
    stringsAsFactors = FALSE
  )

  # Add phase
  phase_break <- switch(format,
    "t20" = list(powerplay = 6, death = 16),
    "odi" = list(powerplay = 10, death = 40),
    list(powerplay = 6, death = 16)
  )

  current_over <- floor(balls_bowled / 6)
  feature_data$phase <- if (current_over < phase_break$powerplay) {
    "powerplay"
  } else if (current_over >= phase_break$death) {
    "death"
  } else {
    "middle"
  }

  feature_data$phase_powerplay <- as.integer(feature_data$phase == "powerplay")
  feature_data$phase_middle <- as.integer(feature_data$phase == "middle")
  feature_data$phase_death <- as.integer(feature_data$phase == "death")

  # Add venue stats (use defaults if not provided)
  if (is.null(venue_stats)) {
    venue_stats <- get_default_venue_stats(format)
  }
  feature_data$venue_avg_first_innings <- venue_stats$avg_first_innings
  feature_data$venue_avg_second_innings <- venue_stats$avg_second_innings %||% venue_stats$avg_first_innings
  feature_data$venue_chase_win_rate <- venue_stats$chase_win_rate %||% 0.45

  # Add format defaults
  feature_data$gender <- "male"
  feature_data$gender_male <- 1L
  feature_data$is_knockout <- 0L
  feature_data$event_tier <- 1
  feature_data$is_dls_match <- FALSE
  feature_data$is_dls <- 0L
  feature_data$is_ko <- 0L

  # For 2nd innings, add chase features
  if (innings == 2) {
    feature_data$target_runs <- target
    feature_data$runs_needed <- target - current_score
    feature_data$required_run_rate <- if (overs_remaining > 0) {
      feature_data$runs_needed / overs_remaining
    } else {
      if (feature_data$runs_needed <= 0) 0 else Inf
    }
    feature_data$rr_differential <- feature_data$required_run_rate - feature_data$current_run_rate
    feature_data$balls_per_run_needed <- if (feature_data$runs_needed > 0) {
      balls_remaining / feature_data$runs_needed
    } else {
      Inf
    }
    feature_data$balls_per_wicket_available <- if (feature_data$wickets_in_hand > 0) {
      balls_remaining / feature_data$wickets_in_hand
    } else {
      0
    }
  }

  # Calculate projected score using Stage 1 model
  projected_score <- calculate_projected_score_from_model(
    feature_data, models$stage1_model, models$stage1_features, format
  )

  # Calculate win probability
  if (innings == 1) {
    # For 1st innings, we estimate based on projected score vs venue average
    # The model gives projected final score, compare to expected chase success
    above_par <- projected_score - feature_data$venue_avg_first_innings
    # Simple logistic transform based on runs above/below par
    # Each ~5 runs above par increases win prob by ~5%
    win_prob <- 0.5 + (above_par / 100)
    win_prob <- pmax(0.05, pmin(0.95, win_prob))  # Bound between 5-95%

  } else {
    # For 2nd innings, use Stage 2 model
    feature_data$projected_final_score <- projected_score
    feature_data$projected_vs_target <- projected_score - target
    feature_data$projected_win_margin <- projected_score - (target - 1)

    win_prob <- calculate_chase_win_prob(
      feature_data, models$stage2_model, models$stage2_features
    )

    # For 2nd innings, this is batting team (chasing) win probability
    # We want batting-first team probability, so invert
    win_prob <- 1 - win_prob
  }

  # Build result
  result <- list(
    win_prob = win_prob,
    projected_score = projected_score,
    current_score = current_score,
    wickets = wickets,
    overs = overs,
    innings = innings,
    target = target,
    format = format,
    runs_above_par = if (innings == 1) projected_score - feature_data$venue_avg_first_innings else NULL
  )

  class(result) <- c("bouncer_win_prob", "list")
  return(result)
}


#' @export
print.bouncer_win_prob <- function(x, ...) {
  cli::cli_h2("Win Probability Prediction")

  # Format overs for display
  overs_int <- floor(x$overs)
  overs_balls <- round((x$overs - overs_int) * 10)
  overs_str <- paste0(overs_int, ".", overs_balls)

  if (x$innings == 1) {
    cli::cli_text("Score: {x$current_score}/{x$wickets} ({overs_str} overs)")
    cli::cli_text("Projected Final Score: {round(x$projected_score)}")
    if (!is.null(x$runs_above_par)) {
      par_status <- if (x$runs_above_par > 0) "above" else "below"
      cli::cli_text("Currently: {abs(round(x$runs_above_par))} runs {par_status} par")
    }
  } else {
    cli::cli_text("Chasing: {x$target}")
    cli::cli_text("Score: {x$current_score}/{x$wickets} ({overs_str} overs)")
    runs_needed <- x$target - x$current_score
    if (runs_needed > 0) {
      cli::cli_text("Need: {runs_needed} runs from {round((120 - overs_to_balls(x$overs)) / 6, 1)} overs")
    }
  }

  cli::cli_h3("Batting First Team Win Probability")
  pct <- round(x$win_prob * 100)
  pct_chase <- 100 - pct

  bar1 <- paste(rep("=", pct %/% 5), collapse = "")
  bar2 <- paste(rep("=", pct_chase %/% 5), collapse = "")

  cat(sprintf("Batting 1st: %s %5.1f%%\n", bar1, x$win_prob * 100))
  cat(sprintf("Chasing:     %s %5.1f%%\n", bar2, (1 - x$win_prob) * 100))

  invisible(x)
}


#' Get Default Venue Statistics
#'
#' Returns average venue statistics for a format when specific venue data
#' is not available.
#'
#' @param format Character. Match format.
#'
#' @return List with venue statistics.
#'
#' @keywords internal
get_default_venue_stats <- function(format) {

  format <- tolower(format)

  switch(format,
    "t20" = list(
      avg_first_innings = 160,
      avg_second_innings = 155,
      chase_win_rate = 0.48
    ),
    "it20" = list(
      avg_first_innings = 155,
      avg_second_innings = 150,
      chase_win_rate = 0.50
    ),
    "odi" = list(
      avg_first_innings = 260,
      avg_second_innings = 255,
      chase_win_rate = 0.45
    ),
    # Default
    list(
      avg_first_innings = 160,
      avg_second_innings = 155,
      chase_win_rate = 0.48
    )
  )
}


#' Calculate Projected Score from Model
#'
#' Internal function to get projected score from Stage 1 model.
#'
#' @param data data.frame with feature data
#' @param model XGBoost model
#' @param feature_cols Feature column names
#' @param format Match format
#'
#' @return Numeric projected score
#'
#' @keywords internal
calculate_projected_score_from_model <- function(data, model, feature_cols, format) {

  # Ensure all feature columns exist
  for (col in feature_cols) {
    if (!col %in% names(data)) {
      data[[col]] <- 0
    }
  }

  # Extract features
  features <- as.matrix(data[, feature_cols, drop = FALSE])

  # Handle NA/Inf
  features[is.na(features)] <- 0
  features[is.infinite(features)] <- 999

  # Predict
  if (requireNamespace("xgboost", quietly = TRUE)) {
    dmatrix <- xgboost::xgb.DMatrix(data = features)
    projected <- stats::predict(model, dmatrix)
  } else {
    # Fallback: use simple projection formula
    projected <- calculate_projected_score(
      current_score = data$total_runs,
      wickets = data$wickets_fallen,
      overs = data$balls_bowled / 6,
      format = format
    )
  }

  return(projected)
}


#' Calculate Chase Win Probability from Model
#'
#' Internal function to get chase win probability from Stage 2 model.
#'
#' @param data data.frame with feature data including Stage 1 predictions
#' @param model XGBoost Stage 2 model
#' @param feature_cols Feature column names
#'
#' @return Numeric win probability (0-1) for chasing team
#'
#' @keywords internal
calculate_chase_win_prob <- function(data, model, feature_cols) {

  # Ensure all feature columns exist
  for (col in feature_cols) {
    if (!col %in% names(data)) {
      data[[col]] <- 0
    }
  }

  # Extract features
  features <- as.matrix(data[, feature_cols, drop = FALSE])

  # Handle NA/Inf
  features[is.na(features)] <- 0
  features[is.infinite(features)] <- 999

  # Predict
  if (requireNamespace("xgboost", quietly = TRUE)) {
    dmatrix <- xgboost::xgb.DMatrix(data = features)
    win_prob <- stats::predict(model, dmatrix)
    win_prob <- pmax(0, pmin(1, win_prob))  # Ensure 0-1 range
  } else {
    # Fallback: simple chase probability based on resources
    runs_needed <- data$runs_needed
    balls_remaining <- data$balls_remaining
    wickets_in_hand <- data$wickets_in_hand

    # Simple resource-based probability
    if (runs_needed <= 0) {
      win_prob <- 1
    } else if (balls_remaining <= 0 || wickets_in_hand <= 0) {
      win_prob <- 0
    } else {
      # Roughly: can you score 1 run per ball with resources available?
      runs_per_ball_available <- balls_remaining * (wickets_in_hand / 10)
      win_prob <- pmin(1, runs_per_ball_available / runs_needed)
      win_prob <- pmax(0.05, pmin(0.95, win_prob))
    }
  }

  return(win_prob)
}


#' Calculate Win Probability for All Deliveries in a Match
#'
#' Adds win probability columns to a data frame of deliveries.
#'
#' @param deliveries data.frame with delivery data. Must include:
#'   match_id, innings, over, ball, total_runs (cumulative), wickets_fallen.
#' @param format Character. Match format.
#' @param target Integer. 2nd innings target (extracted from data if not provided).
#' @param models List. Pre-loaded models (optional).
#'
#' @return data.frame with additional columns:
#'   \itemize{
#'     \item win_prob_before - Win probability before this delivery
#'     \item win_prob_after - Win probability after this delivery
#'     \item wpa - Win Probability Added (change from this delivery)
#'   }
#'
#' @keywords internal
add_win_probability <- function(deliveries,
                                 format = "t20",
                                 target = NULL,
                                 models = NULL) {

  # Load models if needed
  if (is.null(models)) {
    models <- load_in_match_models(format)
    if (is.null(models)) {
      cli::cli_warn("Could not load models, using fallback projection")
    }
  }

  # Get target from data if not provided
  if (is.null(target)) {
    # Target is 1st innings total + 1
    inn1 <- deliveries[deliveries$innings == 1, ]
    if (nrow(inn1) > 0) {
      target <- max(inn1$total_runs, na.rm = TRUE) + 1
    }
  }

  # Calculate win probability for each delivery
  n <- nrow(deliveries)
  win_prob_before <- numeric(n)
  win_prob_after <- numeric(n)

  for (i in seq_len(n)) {
    row <- deliveries[i, ]

    # Calculate overs in cricket notation
    over_num <- row$over
    ball_num <- row$ball
    overs <- over_num + (ball_num - 1) / 10  # Approximate cricket notation

    # Get win probability before this delivery
    # Use previous ball's total_runs and wickets
    if (i == 1 || deliveries$match_id[i] != deliveries$match_id[i-1] ||
        deliveries$innings[i] != deliveries$innings[i-1]) {
      # Start of innings
      score_before <- 0
      wickets_before <- 0
      overs_before <- 0
    } else {
      score_before <- deliveries$total_runs[i-1] %||% 0
      wickets_before <- deliveries$wickets_fallen[i-1] %||% 0
      overs_before <- deliveries$over[i-1] + (deliveries$ball[i-1] - 1) / 10
    }

    # Win probability before
    wp_before <- tryCatch({
      predict_win_probability(
        current_score = score_before,
        wickets = wickets_before,
        overs = overs_before,
        innings = row$innings,
        target = if (row$innings == 2) target else NULL,
        format = format,
        models = models
      )$win_prob
    }, error = function(e) NA_real_)

    # Win probability after (current state)
    wp_after <- tryCatch({
      predict_win_probability(
        current_score = row$total_runs %||% 0,
        wickets = row$wickets_fallen %||% 0,
        overs = overs,
        innings = row$innings,
        target = if (row$innings == 2) target else NULL,
        format = format,
        models = models
      )$win_prob
    }, error = function(e) NA_real_)

    win_prob_before[i] <- wp_before
    win_prob_after[i] <- wp_after
  }

  # Add columns
  deliveries$win_prob_before <- win_prob_before
  deliveries$win_prob_after <- win_prob_after
  deliveries$wpa <- win_prob_after - win_prob_before

  return(deliveries)
}


#' Clear In-Match Model Cache
#'
#' Clears cached models to free memory or force reload.
#'
#' @keywords internal
clear_in_match_cache <- function() {
  rm(list = ls(envir = .inmatch_model_cache), envir = .inmatch_model_cache)
  cli::cli_alert_success("In-match model cache cleared")
}
