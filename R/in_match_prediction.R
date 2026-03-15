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
#' @param format Character. Match format: "t20", "odi", "test".
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
#'   For Test format, returns a different structure with result_model and
#'   conditional_model (decomposed pipeline).
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

  # Test format uses decomposed two-model pipeline
  if (format %in% c("test", "mdm")) {
    result <- load_test_in_match_models(models_path)
    if (!is.null(result)) {
      assign(cache_key, result, envir = .inmatch_model_cache)
    }
    return(result)
  }

  # Load Stage 1 results
  stage1_file <- file.path(models_path, get_model_filename("stage1", format))
  if (!file.exists(stage1_file)) {
    cli::cli_alert_warning("Stage 1 model not found: {stage1_file}")
    cli::cli_alert_info("Run the in-match pipeline first (data-raw/models/in-match/)")
    return(NULL)
  }

  stage1_results <- readRDS(stage1_file)

  # Load Stage 2 results
  stage2_file <- file.path(models_path, get_model_filename("stage2", format))
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


#' Load Test Match In-Match Models (Decomposed Pipeline)
#'
#' @param models_path Character. Path to models directory.
#' @return List with result_model, conditional_model, and feature vectors, or NULL.
#' @keywords internal
load_test_in_match_models <- function(models_path) {

  v3_file <- file.path(models_path, "test_winprob_v3_results.rds")
  if (!file.exists(v3_file)) {
    cli::cli_alert_warning("Test v3 models not found: {v3_file}")
    cli::cli_alert_info("Run 08_test_win_probability_v3.R first")
    return(NULL)
  }

  v3 <- readRDS(v3_file)

  # Also load Stage 1 for projected scores
  stage1_file <- file.path(models_path, get_model_filename("stage1", "test"))
  stage1_model <- NULL
  stage1_features <- NULL
  if (file.exists(stage1_file)) {
    stage1_results <- readRDS(stage1_file)
    stage1_model <- stage1_results$model
    stage1_features <- stage1_results$feature_cols
  }

  result <- list(
    result_model = v3$model_A,
    conditional_model = v3$model_B,
    result_features = v3$result_features,
    conditional_features = v3$conditional_features,
    stage1_model = stage1_model,
    stage1_features = stage1_features,
    format = "test",
    pipeline = "decomposed"
  )

  cli::cli_alert_success("Loaded decomposed Test win probability models (v3)")
  return(result)
}


#' Predict Win Probability from Current Game State
#'
#' Calculate win probability for the batting-first team given the current
#' match state. Uses scoreboard-friendly inputs. For Test matches, returns
#' three-way probabilities (team1 win, draw, team2 win) via a decomposed
#' two-model pipeline.
#'
#' @param current_score Integer. Current team score.
#' @param wickets Integer. Wickets fallen (0-10).
#' @param overs Numeric. Overs bowled in cricket notation (e.g., 10.3 = 10 overs + 3 balls).
#' @param innings Integer. Current innings (1-2 for limited overs, 1-4 for Test).
#' @param target Integer. Target score (required if innings = 2 for limited overs,
#'   or innings = 4 for Test).
#' @param format Character. Match format: "t20", "odi", "test".
#' @param venue_stats List. Venue-specific statistics (optional).
#'   If NULL, uses format averages. For Test, can include venue_avg (1st innings
#'   average) and venue_result_rate (historical P(result) at venue).
#' @param match_state List. Additional Test match state (optional). Can include:
#'   \itemize{
#'     \item completed_innings - list of lists with runs, wickets, overs per innings
#'     \item batting_is_team1 - logical, is batting team the team listed first?
#'   }
#' @param skill_adjustments List. Team/player skill adjustments (optional).
#' @param models List. Pre-loaded models from load_in_match_models().
#'   If NULL, models are loaded automatically.
#'
#' @return A `bouncer_win_prob` object. For limited-overs formats:
#'   \itemize{
#'     \item win_prob - Win probability for batting-first team (0-1)
#'     \item projected_score - Projected final innings score
#'   }
#'   For Test format, additionally:
#'   \itemize{
#'     \item draw_prob - Draw probability (0-1)
#'     \item team1_win - Team 1 win probability
#'     \item team2_win - Team 2 win probability
#'   }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # T20 first innings: India 85/2 after 10 overs
#' wp <- predict_win_probability(85, 2, 10.0, innings = 1, format = "t20")
#' print(wp)
#'
#' # T20 second innings: Chasing 180, currently 100/3 after 12.4 overs
#' wp <- predict_win_probability(100, 3, 12.4, innings = 2, target = 180, format = "t20")
#' print(wp)
#'
#' # Test match: 3rd innings, team2 batting at 150/4 after 50 overs
#' wp <- predict_win_probability(150, 4, 50, innings = 3, format = "test",
#'   match_state = list(
#'     completed_innings = list(
#'       list(runs = 350, wickets = 10, overs = 120),
#'       list(runs = 280, wickets = 10, overs = 95)
#'     ),
#'     batting_is_team1 = FALSE
#'   ))
#' print(wp)
#' }
predict_win_probability <- function(current_score,
                                     wickets,
                                     overs,
                                     innings,
                                     target = NULL,
                                     format = "t20",
                                     venue_stats = NULL,
                                     match_state = NULL,
                                     skill_adjustments = NULL,
                                     models = NULL) {

  format <- tolower(format)

  # Test format uses decomposed pipeline

  if (format %in% c("test", "mdm")) {
    return(predict_test_win_probability(
      current_score = current_score,
      wickets = wickets,
      overs = overs,
      innings = innings,
      target = target,
      venue_stats = venue_stats,
      match_state = match_state,
      models = models
    ))
  }

  # Validate inputs (limited overs)
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

  # Get max balls for format (using centralized lookup)
  max_balls <- get_max_balls(format)

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

  # Add phase using central helper
  phase_bounds <- get_phase_boundaries(format)

  current_over <- floor(balls_bowled / 6)
  feature_data$phase <- if (current_over < phase_bounds$powerplay_end) {
    "powerplay"
  } else if (current_over >= phase_bounds$middle_end) {
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
      if (feature_data$runs_needed <= 0) 0 else MAX_REQUIRED_RUN_RATE
    }
    # Clamp required_run_rate to reasonable maximum
    feature_data$required_run_rate <- min(feature_data$required_run_rate, MAX_REQUIRED_RUN_RATE)

    feature_data$rr_differential <- feature_data$required_run_rate - feature_data$current_run_rate
    feature_data$balls_per_run_needed <- if (feature_data$runs_needed > 0) {
      balls_remaining / feature_data$runs_needed
    } else {
      INF_FEATURE_PLACEHOLDER  # Effectively infinite (already won)
    }
    # Clamp balls_per_run_needed to reasonable maximum
    feature_data$balls_per_run_needed <- min(feature_data$balls_per_run_needed, INF_FEATURE_PLACEHOLDER)

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
    # Try to use the trained innings 1 win probability model
    if (!is.null(models$innings1_model) && !is.null(models$innings1_features)) {
      feature_data$projected_final_score <- projected_score
      feature_data$projected_vs_baseline <- projected_score - feature_data$venue_avg_first_innings
      win_prob <- tryCatch({
        calculate_innings1_win_prob(
          feature_data, models$innings1_model, models$innings1_features
        )
      }, error = function(e) NULL)
    } else {
      win_prob <- NULL
    }

    # Fallback: logistic heuristic (better than linear)
    if (is.null(win_prob)) {
      above_par <- projected_score - feature_data$venue_avg_first_innings
      # Logistic: naturally bounded 0-1, ~65% at +30 runs, ~35% at -30 runs
      win_prob <- 1 / (1 + exp(-above_par / 40))
    }
    win_prob <- pmax(0.05, pmin(0.95, win_prob))

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
    method = if (innings == 1 && !is.null(models$innings1_model)) "model" else if (innings == 1) "heuristic" else "model",
    runs_above_par = if (innings == 1) projected_score - feature_data$venue_avg_first_innings else NULL
  )

  class(result) <- c("bouncer_win_prob", "list")
  return(result)
}


#' Predict Test Match Win Probability (Decomposed Pipeline)
#'
#' Uses two binary models: P(result) and P(team1_win | result) to produce
#' calibrated three-way probabilities for Test cricket.
#'
#' @inheritParams predict_win_probability
#' @return A bouncer_win_prob object with team1_win, draw_prob, team2_win.
#' @keywords internal
predict_test_win_probability <- function(current_score,
                                          wickets,
                                          overs,
                                          innings,
                                          target = NULL,
                                          venue_stats = NULL,
                                          match_state = NULL,
                                          models = NULL) {

  if (!innings %in% 1:4) {
    cli::cli_abort("Test innings must be 1-4")
  }
  if (wickets < 0 || wickets > 10) {
    cli::cli_abort("Wickets must be between 0 and 10")
  }

  # Load models
  if (is.null(models)) {
    models <- load_in_match_models("test")
    if (is.null(models)) {
      cli::cli_abort("Could not load Test in-match models")
    }
  }

  # Parse match_state (completed innings info)
  completed <- match_state$completed_innings %||% list()
  batting_is_team1 <- match_state$batting_is_team1 %||% (innings %in% c(1, 3))

  # Extract completed innings data
  get_inn <- function(i, field, default = 0) {
    if (i <= length(completed) && !is.null(completed[[i]][[field]])) {
      completed[[i]][[field]]
    } else {
      default
    }
  }

  # Cumulative team totals from completed innings
  team1_completed <- 0
  team2_completed <- 0
  completed_overs <- 0
  completed_wickets <- 0

  for (i in seq_along(completed)) {
    inn_runs <- get_inn(i, "runs")
    inn_wickets <- get_inn(i, "wickets")
    inn_overs <- get_inn(i, "overs", 90)

    # Team 1 bats in innings 1 and 3; team 2 in innings 2 and 4
    if (i %% 2 == 1) {
      team1_completed <- team1_completed + inn_runs
    } else {
      team2_completed <- team2_completed + inn_runs
    }
    completed_overs <- completed_overs + inn_overs
    completed_wickets <- completed_wickets + inn_wickets
  }

  # Current innings state
  current_over <- overs  # overs bowled in current innings
  current_run_rate <- if (current_over > 0) current_score / current_over else 0
  wickets_in_hand <- 10 - wickets

  # Team1 lead
  if (batting_is_team1) {
    team1_lead <- as.integer(team1_completed + current_score - team2_completed)
  } else {
    team1_lead <- as.integer(team1_completed - (team2_completed + current_score))
  }

  # Cumulative match overs
  cum_overs <- completed_overs + current_over
  MAX_OVERS <- 450
  overs_remaining <- max(0, MAX_OVERS - cum_overs)
  match_progress <- min(1, cum_overs / MAX_OVERS)
  approx_day <- min(5L, as.integer(floor(cum_overs / 90) + 1))

  # Venue stats defaults
  venue_avg <- venue_stats$avg_first_innings %||% venue_stats$venue_avg %||% 340
  venue_result_rate <- venue_stats$venue_result_rate %||% 0.63

  # Total match wickets and runs

  total_wickets_match <- completed_wickets + wickets
  total_runs_match <- team1_completed + team2_completed +
    (if (batting_is_team1) current_score else current_score)
  # Avoid double-counting: total_runs_match accounts for both teams
  # Actually need: sum of all completed innings + current innings
  total_runs_match <- sum(vapply(completed, function(x) x$runs %||% 0, numeric(1))) + current_score
  runs_per_over_match <- if (cum_overs > 0) total_runs_match / cum_overs else 3.0

  # Overs per wicket in current innings
  overs_per_wicket_current <- if (wickets > 0) current_over / wickets else 30

  # Projected current innings overs
  current_innings_projected_overs <- min(150,
    if (wickets > 0) current_over + wickets_in_hand * overs_per_wicket_current else 90
  )

  # Average overs per completed innings
  avg_overs_per_innings <- if (length(completed) > 0) {
    mean(vapply(completed, function(x) x$overs %||% 80, numeric(1)))
  } else {
    80
  }

  # Remaining innings count (after current)
  remaining_innings_count <- 4L - innings

  # Projected total overs
  projected_total_overs <- completed_overs + current_innings_projected_overs +
    remaining_innings_count * avg_overs_per_innings
  projected_total_overs <- min(600, max(50, projected_total_overs))
  time_pressure <- projected_total_overs / MAX_OVERS

  # Lead-based features
  abs_lead <- abs(team1_lead)
  lead_per_over_remaining <- if (overs_remaining > 0) abs_lead / overs_remaining else as.double(abs_lead)

  # Follow-on possible
  follow_on_possible <- 0L
  if (innings >= 2 && length(completed) >= 2) {
    inn1_runs <- get_inn(1, "runs")
    inn2_runs <- get_inn(2, "runs")
    if ((inn1_runs - inn2_runs) >= 200) follow_on_possible <- 1L
  }

  # 4th innings features
  target_val <- 0
  runs_needed <- 0
  req_rate <- 0
  overs_per_wicket_val <- 0
  if (innings == 4) {
    target_val <- if (!is.null(target)) target else as.integer(team1_completed - team2_completed + 1L)
    runs_needed <- max(0L, target_val - current_score)
    req_rate <- if (overs_remaining > 0) runs_needed / overs_remaining else 99
    overs_per_wicket_val <- if (wickets_in_hand > 0) overs_remaining / wickets_in_hand else 0
  }

  # Projected lead and innings total
  projected_innings_total <- if (current_over > 0) current_score * (90 / current_over) else venue_avg
  projected_lead <- if (batting_is_team1) {
    team1_completed + projected_innings_total - team2_completed - venue_avg
  } else {
    as.double(team1_lead)
  }

  # ---- Build feature vectors for both models ----

  # Tier 1: Derived rain proxies
  overs_per_day <- if (approx_day > 0) cum_overs / approx_day else 90
  overs_deficit <- max(0, approx_day * 90 - cum_overs)

  # Tier 2: Causal rain_days_so_far (from match_state if provided)
  rain_days_so_far <- match_state$rain_days_so_far %||% 0

  # Tier 3: At prediction time, if forecast available, add to rain estimate
  forecast_rain <- match_state$forecast_rain_days %||% 0
  if (forecast_rain > 0 && approx_day < 5) {
    # Combine observed + forecast rain days
    rain_days_so_far <- rain_days_so_far + forecast_rain
  }

  # Model A: P(result)
  result_data <- data.frame(
    overs_remaining = overs_remaining,
    match_progress = match_progress,
    approx_day = approx_day,
    time_pressure = time_pressure,
    projected_total_overs = projected_total_overs,
    venue_result_rate = venue_result_rate,
    total_wickets_match = total_wickets_match,
    runs_per_over_match = runs_per_over_match,
    abs_lead = abs_lead,
    lead_per_over_remaining = lead_per_over_remaining,
    innings_num = as.double(innings),
    follow_on_possible = follow_on_possible,
    # Tier 1: derived rain proxies
    overs_per_day = overs_per_day,
    overs_deficit = overs_deficit,
    # Tier 2/3: causal weather
    rain_days_so_far = rain_days_so_far,
    stringsAsFactors = FALSE
  )

  # Model B: P(team1_win | result)
  conditional_data <- data.frame(
    team1_lead = team1_lead,
    projected_lead = projected_lead,
    projected_innings_total = projected_innings_total,
    batting_is_team1 = as.integer(batting_is_team1),
    wickets_in_hand = wickets_in_hand,
    overs_remaining = overs_remaining,
    cum_overs = cum_overs,
    venue_avg = venue_avg,
    innings_num = as.double(innings),
    target = target_val,
    runs_needed = runs_needed,
    req_rate = req_rate,
    overs_per_wicket = overs_per_wicket_val,
    current_run_rate = current_run_rate,
    stringsAsFactors = FALSE
  )

  # Predict with both models
  p_result <- predict_with_features(models$result_model, result_data, models$result_features)
  p_team1_given_result <- predict_with_features(models$conditional_model, conditional_data, models$conditional_features)

  # Combine
  p_draw <- 1 - p_result
  p_team1_win <- p_result * p_team1_given_result
  p_team2_win <- p_result * (1 - p_team1_given_result)

  # Build result
  result <- list(
    win_prob = p_team1_win,  # Backwards-compatible: batting-first team
    team1_win = p_team1_win,
    draw_prob = p_draw,
    team2_win = p_team2_win,
    projected_score = projected_innings_total,
    current_score = current_score,
    wickets = wickets,
    overs = overs,
    innings = innings,
    target = if (innings == 4) target_val else NULL,
    format = "test",
    method = "model",
    p_result = p_result,
    p_team1_given_result = p_team1_given_result
  )

  class(result) <- c("bouncer_win_prob", "list")
  return(result)
}


#' Predict using XGBoost model with feature alignment
#'
#' @param model XGBoost model
#' @param data data.frame with feature data
#' @param feature_cols Character vector of expected feature columns
#' @return Numeric prediction (0-1)
#' @keywords internal
predict_with_features <- function(model, data, feature_cols) {
  for (col in feature_cols) {
    if (!col %in% names(data)) data[[col]] <- 0
  }
  features <- as.matrix(data[, feature_cols, drop = FALSE])
  features[is.na(features)] <- 0
  features[is.infinite(features)] <- 999

  if (requireNamespace("xgboost", quietly = TRUE)) {
    dmatrix <- xgboost::xgb.DMatrix(data = features)
    pred <- stats::predict(model, dmatrix)
    pmax(0, pmin(1, pred))
  } else {
    0.5  # Fallback
  }
}


#' Print method for bouncer_win_prob objects
#'
#' @param x A bouncer_win_prob object from predict_win_probability()
#' @param ... Additional arguments (unused)
#' @export
print.bouncer_win_prob <- function(x, ...) {
  cli::cli_h2("Win Probability Prediction")

  # Format overs for display
  overs_int <- floor(x$overs)
  overs_balls <- round((x$overs - overs_int) * 10)
  overs_str <- paste0(overs_int, ".", overs_balls)

  # Test format: 3-way display
  if (x$format %in% c("test", "mdm") && !is.null(x$draw_prob)) {
    cli::cli_text("Innings {x$innings}: {x$current_score}/{x$wickets} ({overs_str} overs)")
    if (!is.null(x$target) && x$innings == 4) {
      runs_needed <- x$target - x$current_score
      if (runs_needed > 0) {
        cli::cli_text("Target: {x$target} (need {runs_needed} more)")
      }
    }

    cli::cli_h3("Match Probabilities")
    t1 <- round(x$team1_win * 100, 1)
    dr <- round(x$draw_prob * 100, 1)
    t2 <- round(x$team2_win * 100, 1)

    bar_t1 <- paste(rep("=", max(0, round(t1 / 5))), collapse = "")
    bar_dr <- paste(rep("=", max(0, round(dr / 5))), collapse = "")
    bar_t2 <- paste(rep("=", max(0, round(t2 / 5))), collapse = "")

    cat(sprintf("Team 1 Win:  %s %5.1f%%\n", bar_t1, t1))
    cat(sprintf("Draw:        %s %5.1f%%\n", bar_dr, dr))
    cat(sprintf("Team 2 Win:  %s %5.1f%%\n", bar_t2, t2))
    invisible(x)
    return(invisible(x))
  }

  # Limited overs format
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
      cli::cli_text("Need: {runs_needed} runs from {round((get_max_balls(x$format) - overs_to_balls(x$overs)) / 6, 1)} overs")
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
    "test" = , "mdm" = list(
      avg_first_innings = 340,
      venue_avg = 340,
      venue_result_rate = 0.63
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
  features[is.infinite(features)] <- INF_FEATURE_PLACEHOLDER

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
#' Calculate Innings 1 Win Probability Using Trained Model
#'
#' @param data List with feature data (projected_final_score, etc.)
#' @param model XGBoost model for innings 1 win probability
#' @param feature_cols Character vector of feature column names
#' @return Numeric win probability (0-1)
#' @keywords internal
calculate_innings1_win_prob <- function(data, model, feature_cols) {
  # Ensure all feature columns exist
  for (col in feature_cols) {
    if (!col %in% names(data)) {
      data[[col]] <- 0
    }
  }

  features <- as.matrix(data[, feature_cols, drop = FALSE])
  features[is.na(features)] <- 0
  features[is.infinite(features)] <- INF_FEATURE_PLACEHOLDER

  if (requireNamespace("xgboost", quietly = TRUE)) {
    dmatrix <- xgboost::xgb.DMatrix(data = features)
    win_prob <- stats::predict(model, dmatrix)
    pmax(0, pmin(1, win_prob))
  } else {
    NULL  # Signal to use fallback
  }
}


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
  features[is.infinite(features)] <- INF_FEATURE_PLACEHOLDER

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
  # TODO: Vectorize this loop. Currently sequential because each state depends on
  # the previous delivery's context (innings boundaries, cumulative score tracking).
  n <- nrow(deliveries)
  win_prob_before <- numeric(n)
  win_prob_after <- numeric(n)

  for (i in seq_len(n)) {
    row <- deliveries[i, ]

    # Calculate overs in cricket notation
    over_num <- row$over
    ball_num <- row$ball
    overs <- over_num + ball_num / 10  # Cricket notation: state after delivery

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
