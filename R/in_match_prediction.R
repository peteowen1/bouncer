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

  # Innings-1 win probability model, produced by
  # data-raw/models/in-match/04_win_probability_innings1.R.
  #
  # This loader previously ignored it entirely, so predict_win_probability()
  # always saw models$innings1_model as NULL and fell back to its logistic
  # heuristic for the first innings -- for every format, including any format
  # that had the model trained and sitting on disk. Optional by design: a
  # format without one still gets the heuristic rather than an error.
  innings1_file <- file.path(models_path,
                             paste0(format, "_innings1_results.rds"))
  innings1_model <- NULL
  innings1_features <- NULL
  if (file.exists(innings1_file)) {
    innings1_results <- readRDS(innings1_file)
    innings1_model <- innings1_results$model
    innings1_features <- innings1_results$feature_cols
  }

  result <- list(
    stage1_model = stage1_results$model,
    stage2_model = stage2_results$model,
    stage1_features = stage1_results$feature_cols,
    stage2_features = stage2_results$feature_cols,
    innings1_model = innings1_model,
    innings1_features = innings1_features,
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
#' @section This model does NOT feed the player ratings:
#' Easy to assume otherwise, so stated plainly. As of 2026-08-12 the only
#' production caller of this function is [plot_win_probability()] — it draws a
#' chart. It is **not** an input to [calculate_epr()], [calculate_bouncer()],
#' or anything else in the ratings chain.
#'
#' The WPA that reaches the career ratings comes from
#' `cricinfo.balls.win_probability`, a column **scraped from ESPNcricinfo's own
#' forecaster** and differenced in `player_game_data.R`. That scraped column is
#' 0% populated for Tests and 7.7% for ODIs (see `?calculate_epr`).
#'
#' So this package trains an in-match win-probability model, and then rates
#' players using somebody else's. Whether to wire this model into
#' `player_game_data.R` is open — `docs/DECISIONS.md` D-P6. Until that is
#' decided, do not describe BOUNCER ratings as being built on bouncer's own
#' win probability, and do not assume improving these models improves the
#' ratings: today it does not.
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
#' @param recent_balls Named list of momentum features for the last few overs
#'   (`runs_last_12_balls`, `dots_last_24_balls`, `rr_last_6_overs`, ...) as
#'   produced by [calculate_rolling_features()] over the delivery sequence. The
#'   trained models use 14 of these. A scoreboard state cannot supply them, so
#'   when this is NULL they are imputed from the current run rate and a warning
#'   is emitted. They are never zero-filled -- zero means "no runs and no
#'   wickets in the last N balls", an extreme real state, and substituting it is
#'   what made the chase model return ~0.9 regardless of situation.
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
                                     models = NULL,
                                     recent_balls = NULL) {

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

  # overs_into_phase, from the same helper the training pipeline uses, so the
  # definition cannot drift between train and serve.
  feature_data$overs_into_phase <- calculate_phase_features(
    over = as.double(current_over),
    ball = as.double(balls_bowled - current_over * 6),
    match_type = format
  )$overs_into_phase

  # Momentum windows. These need recent ball history, which a scoreboard state
  # does not carry, and the training pipeline computes them with
  # calculate_rolling_features() over the delivery sequence.
  #
  # They are NOT zero-filled. Zero means "no runs and no wickets in the last N
  # balls", a real and extreme state; substituting it made the chase model
  # return ~0.9 regardless of situation. Absent history, they are imputed from
  # the current run rate -- an approximation, but one that sits inside the
  # training distribution instead of at its edge. Callers with the ball
  # sequence should pass `recent_balls` and get the real values.
  crr <- feature_data$current_run_rate
  if (is.null(recent_balls)) {
    if (isTRUE(getOption("bouncer.warn_momentum_impute", TRUE))) {
      cli::cli_warn(c(
        "No {.arg recent_balls} supplied: the 14 momentum features are imputed from the current run rate.",
        "i" = "Pass {.arg recent_balls} for the real values; silence with {.code options(bouncer.warn_momentum_impute = FALSE)}."
      ))
    }
    recent_balls <- list()
  }
  mom_default <- list(
    runs_last_12_balls = crr * 2, runs_last_24_balls = crr * 4,
    dots_last_12_balls = 12 * 0.35, dots_last_24_balls = 24 * 0.35,
    boundaries_last_12_balls = 12 * 0.12, boundaries_last_24_balls = 24 * 0.12,
    wickets_last_12_balls = 0.4, wickets_last_24_balls = 0.8,
    runs_last_3_overs = crr * 3, runs_last_6_overs = crr * 6,
    wickets_last_3_overs = 0.6, wickets_last_6_overs = 1.2,
    rr_last_3_overs = crr, rr_last_6_overs = crr
  )
  for (nm in names(mom_default)) {
    feature_data[[nm]] <- recent_balls[[nm]] %||% mom_default[[nm]]
  }

  # Add venue stats (use defaults if not provided)
  if (is.null(venue_stats)) {
    venue_stats <- get_default_venue_stats(format)
  }
  # Names the trained models use, alongside the legacy ones set below.
  feature_data$venue_avg_score <- venue_stats$avg_first_innings
  feature_data$venue_chase_success_rate <- venue_stats$chase_win_rate %||% 0.45
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

  # For 2nd innings, add chase features.
  #
  # These now come from the SAME helpers the training pipeline uses --
  # calculate_pressure_metrics() and calculate_tail_calibration_features(), both
  # in R/feature_engineering.R -- rather than being re-derived here. That is the
  # whole point: this function previously hand-rolled four of the chase features
  # and simply never produced the other nineteen, so
  # calculate_chase_win_prob() zero-filled them and the model returned ~0.9 for
  # every state. Calling the shared helpers makes train/serve parity structural
  # instead of something to be maintained by hand in two places.
  if (innings == 2) {
    feature_data$target_runs <- target

    pm <- calculate_pressure_metrics(
      target           = target,
      current_runs     = current_score,
      current_wickets  = wickets,
      balls_remaining  = balls_remaining,
      current_run_rate = feature_data$current_run_rate
    )
    for (nm in names(pm)) feature_data[[nm]] <- pm[[nm]]

    tc <- calculate_tail_calibration_features(
      runs_needed     = feature_data$runs_needed,
      balls_remaining = balls_remaining,
      wickets_in_hand = feature_data$wickets_in_hand
    )
    for (nm in names(tc)) feature_data[[nm]] <- tc[[nm]]

    # First-innings context. target is innings-1 total + 1 by construction, so
    # the total is recoverable; wickets are not, and 10 is the common case for
    # a completed innings.
    feature_data$innings1_total    <- target - 1
    feature_data$innings1_run_rate <- (target - 1) / (max_balls / 6)
    feature_data$innings1_wickets  <- 10
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


#' Score Many Delivery States for Win Probability in One Pass
#'
#' The batched twin of [predict_win_probability()]. Same features, same models,
#' same answer -- but assembled as columns and handed to XGBoost in three
#' `predict()` calls instead of three per row.
#'
#' @section Why this exists:
#' `predict_win_probability()` builds a one-row data.frame and calls
#' `predict()` on it. That costs ~32 ms per delivery, which is fine for a chart
#' and unusable for a pipeline: `cricinfo.balls` holds 940,985 deliveries, so
#' scoring the corpus one ball at a time takes about 8.3 hours. The model calls
#' were never the problem -- `calculate_projected_score_from_model()`,
#' `calculate_innings1_win_prob()` and `calculate_chase_win_prob()` already
#' accept N rows and build a single `xgb.DMatrix`. The cost was entirely in the
#' scalar feature assembly around them, so this function reproduces that
#' assembly with vectorised equivalents (`fcase` for the phase and run-rate
#' branches, the innings split done by subsetting rather than by `if`).
#'
#' @section Parity with the scalar path:
#' The two are checked against each other in
#' `tests/testthat/test-in-match-batch.R`, which scores the same states both
#' ways and requires agreement to within 1e-8. Any feature added to one must be
#' added to the other or that test fails -- which is the point, given that the
#' original serving bug was a train/serve feature mismatch nothing detected.
#'
#' @param states data.frame or data.table, one row per delivery state, with
#'   columns `current_score`, `wickets`, `overs` (cricket notation) and
#'   `innings` (1 or 2). Rows with `innings == 2` also need `target`. The 14
#'   momentum columns produced by [calculate_rolling_features()]
#'   (`runs_last_12_balls`, `rr_last_6_overs`, ...) are used when present and
#'   imputed from the current run rate, with one warning, when absent -- never
#'   zero-filled, for the reason given in [predict_win_probability()].
#' @param format Character. Limited-overs format. Test/MDM are not supported
#'   here; they run through the decomposed `predict_test_win_probability()`.
#' @param models List from [load_in_match_models()]. Loaded if NULL.
#' @param venue_stats List of venue statistics, or NULL for format defaults.
#'
#' @param detail Logical. `FALSE` (default) returns the win probability vector.
#'   `TRUE` returns a data.frame with `win_prob` and `projected_score` — the
#'   Stage 1 output, which is computed for every row regardless and is what
#'   ERA is differenced from. Exposing it avoids scoring the same states twice.
#'
#' @return Numeric vector of P(batting-first team wins), one element per row of
#'   `states`, in input order. Rows that cannot be scored are `NA_real_`. With
#'   `detail = TRUE`, a data.frame of `win_prob` and `projected_score`.
#'
#' @keywords internal
predict_win_probability_batch <- function(states,
                                          format = "t20",
                                          models = NULL,
                                          venue_stats = NULL,
                                          detail = FALSE) {

  format <- tolower(format)
  if (format %in% c("test", "mdm")) {
    cli::cli_abort(c(
      "{.fn predict_win_probability_batch} does not handle {.val {format}}.",
      "i" = "Test win probability is decomposed into result/conditional models -- use {.fn predict_test_win_probability}."
    ))
  }

  states <- as.data.frame(states)
  n <- nrow(states)
  if (n == 0L) {
    return(if (detail) {
      data.frame(win_prob = numeric(0), projected_score = numeric(0))
    } else {
      numeric(0)
    })
  }

  required <- c("current_score", "wickets", "overs", "innings")
  absent <- setdiff(required, names(states))
  if (length(absent) > 0) {
    cli::cli_abort("{.arg states} is missing required column{?s}: {.field {absent}}.")
  }

  if (is.null(models)) {
    models <- load_in_match_models(format)
    if (is.null(models)) cli::cli_abort("Could not load in-match models for {.val {format}}.")
  }
  if (is.null(venue_stats)) venue_stats <- get_default_venue_stats(format)

  innings <- as.integer(states$innings)
  if (any(!innings %in% c(1L, 2L), na.rm = TRUE)) {
    cli::cli_abort("{.field innings} must be 1 or 2; found {.val {setdiff(unique(innings), c(1L, 2L))}}.")
  }
  if (any(innings == 2L, na.rm = TRUE) && !"target" %in% names(states)) {
    cli::cli_abort("{.field target} is required when any row has {.code innings == 2}.")
  }

  balls_bowled <- overs_to_balls(states$overs)
  max_balls <- get_max_balls(format)
  balls_remaining <- max_balls - balls_bowled

  # Scalar `if (balls_bowled > 0)` in the row-at-a-time path. pmax keeps the
  # division defined so no Inf is ever produced and then discarded.
  crr <- ifelse(balls_bowled > 0, states$current_score / (pmax(balls_bowled, 1L) / 6), 0)

  fd <- data.frame(
    total_runs       = states$current_score,
    wickets_fallen   = states$wickets,
    wickets_in_hand  = 10 - states$wickets,
    balls_bowled     = balls_bowled,
    balls_remaining  = balls_remaining,
    overs_completed  = balls_bowled / 6,
    overs_remaining  = balls_remaining / 6,
    current_run_rate = crr,
    innings          = innings,
    stringsAsFactors = FALSE
  )

  phase_bounds <- get_phase_boundaries(format)
  current_over <- floor(balls_bowled / 6)
  fd$phase <- data.table::fcase(
    current_over <  phase_bounds$powerplay_end, "powerplay",
    current_over >= phase_bounds$middle_end,    "death",
    default = "middle"
  )
  fd$phase_powerplay <- as.integer(fd$phase == "powerplay")
  fd$phase_middle    <- as.integer(fd$phase == "middle")
  fd$phase_death     <- as.integer(fd$phase == "death")

  fd$overs_into_phase <- calculate_phase_features(
    over       = as.double(current_over),
    ball       = as.double(balls_bowled - current_over * 6),
    match_type = format
  )$overs_into_phase

  # Momentum. Same contract as the scalar path: real values when the caller
  # has the ball sequence, otherwise imputed from the run rate with a warning.
  mom_default <- list(
    runs_last_12_balls = crr * 2, runs_last_24_balls = crr * 4,
    dots_last_12_balls = 12 * 0.35, dots_last_24_balls = 24 * 0.35,
    boundaries_last_12_balls = 12 * 0.12, boundaries_last_24_balls = 24 * 0.12,
    wickets_last_12_balls = 0.4, wickets_last_24_balls = 0.8,
    runs_last_3_overs = crr * 3, runs_last_6_overs = crr * 6,
    wickets_last_3_overs = 0.6, wickets_last_6_overs = 1.2,
    rr_last_3_overs = crr, rr_last_6_overs = crr
  )
  supplied <- intersect(names(mom_default), names(states))
  if (length(supplied) < length(mom_default) &&
      isTRUE(getOption("bouncer.warn_momentum_impute", TRUE))) {
    cli::cli_warn(c(
      "{length(mom_default) - length(supplied)} of {length(mom_default)} momentum features were not supplied and are imputed from the current run rate.",
      "i" = "Pass them via {.fn calculate_rolling_features} output; silence with {.code options(bouncer.warn_momentum_impute = FALSE)}."
    ))
  }
  for (nm in names(mom_default)) {
    fd[[nm]] <- if (nm %in% supplied) states[[nm]] else mom_default[[nm]]
  }

  # Per-row context where the caller has it, format defaults otherwise.
  #
  # These were unconditional constants until 2026-08-13, which is a train/serve
  # mismatch wearing a different hat: training saw a real per-venue average and
  # a real gender flag, serving saw 260 runs and "male" for every delivery.
  # Measured on cricinfo deliveries, women's T20 chase ECE was 0.2299 against
  # 0.0423 for men's -- while the model's own held-out ECE is 0.0282. Supplying
  # the truth beats calibrating over the lie.
  take <- function(nm, default) {
    if (nm %in% names(states)) states[[nm]] else default
  }

  fd$venue_avg_score          <- take("venue_avg_score", venue_stats$avg_first_innings)
  fd$venue_chase_success_rate <- take("venue_chase_success_rate", venue_stats$chase_win_rate %||% 0.45)
  fd$venue_avg_first_innings  <- take("venue_avg_score", venue_stats$avg_first_innings)
  fd$venue_avg_second_innings <- take("venue_avg_second_innings",
                                      venue_stats$avg_second_innings %||% venue_stats$avg_first_innings)
  fd$venue_chase_win_rate     <- take("venue_chase_success_rate", venue_stats$chase_win_rate %||% 0.45)

  fd$gender_male <- as.integer(take("gender_male", 1L))
  fd$gender      <- ifelse(fd$gender_male == 1L, "male", "female")
  fd$is_knockout <- as.integer(take("is_knockout", 0L))
  fd$event_tier  <- take("event_tier", 1)
  fd$is_dls_match <- FALSE
  fd$is_dls      <- as.integer(take("is_dls", 0L))
  fd$is_ko       <- as.integer(take("is_knockout", 0L))

  # Stage 1 runs over every row: both branches consume the projected score.
  fd$projected_final_score <- calculate_projected_score_from_model(
    fd, models$stage1_model, models$stage1_features, format
  )

  out <- rep(NA_real_, n)
  i1 <- which(innings == 1L)
  i2 <- which(innings == 2L)

  if (length(i1) > 0) {
    f1 <- fd[i1, , drop = FALSE]
    f1$projected_vs_baseline <- f1$projected_final_score - f1$venue_avg_first_innings

    wp1 <- NULL
    if (!is.null(models$innings1_model) && !is.null(models$innings1_features)) {
      wp1 <- tryCatch(
        calculate_innings1_win_prob(f1, models$innings1_model, models$innings1_features),
        error = function(e) NULL
      )
    }
    # Same logistic fallback, and the same reason it is not a weak link: on the
    # ODI benchmark it scored 0.216 against the scraped column's 0.387.
    if (is.null(wp1)) {
      above_par <- f1$projected_final_score - f1$venue_avg_first_innings
      wp1 <- 1 / (1 + exp(-above_par / 40))
    }
    out[i1] <- pmax(0.05, pmin(0.95, wp1))
  }

  if (length(i2) > 0) {
    f2 <- fd[i2, , drop = FALSE]
    target2 <- states$target[i2]
    f2$target_runs <- target2

    pm <- calculate_pressure_metrics(
      target           = target2,
      current_runs     = f2$total_runs,
      current_wickets  = f2$wickets_fallen,
      balls_remaining  = f2$balls_remaining,
      current_run_rate = f2$current_run_rate
    )
    for (nm in names(pm)) f2[[nm]] <- pm[[nm]]

    tc <- calculate_tail_calibration_features(
      runs_needed     = f2$runs_needed,
      balls_remaining = f2$balls_remaining,
      wickets_in_hand = f2$wickets_in_hand
    )
    for (nm in names(tc)) f2[[nm]] <- tc[[nm]]

    f2$innings1_total    <- target2 - 1
    f2$innings1_run_rate <- (target2 - 1) / (max_balls / 6)
    # 10 is the common case for a completed innings but not the truth: a side
    # bowled out is a different first innings from one that closed on 6 down.
    # Callers with the ball sequence know which, and training did.
    f2$innings1_wickets  <- if ("innings1_wickets" %in% names(states)) {
      states$innings1_wickets[i2]
    } else {
      10
    }

    f2$projected_vs_target   <- f2$projected_final_score - target2
    f2$projected_win_margin  <- f2$projected_final_score - (target2 - 1)

    # calculate_chase_win_prob() answers for the chasing team; the scalar path
    # inverts to batting-first and so does this one.
    out[i2] <- 1 - calculate_chase_win_prob(f2, models$stage2_model, models$stage2_features)
  }

  if (detail) {
    return(data.frame(win_prob = out, projected_score = fd$projected_final_score))
  }

  out
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
  data <- fill_model_features(data, feature_cols)
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
  data <- fill_model_features(data, feature_cols)

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
  data <- fill_model_features(data, feature_cols)

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
  data <- fill_model_features(data, feature_cols)

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


#' Resolve the Chase Target for Each Match in a Delivery Frame
#'
#' Pulled out of [add_win_probability()] so the per-match target rule can be
#' tested without any trained model present. That matters because CI checks
#' out only this repo — `bouncerdata/models/` is absent, so every test that
#' needs a real in-match model skips, and the target logic would otherwise
#' never be re-verified by a green CI run.
#'
#' @param deliveries data.frame with at least `match_id`, `innings` and
#'   `total_runs` (cumulative). May span many matches.
#' @param target NULL to derive a target per match from its own first innings,
#'   or a single value to apply to every match in the frame.
#'
#' @return Named numeric vector of targets, keyed by `match_id`. Matches with
#'   no first-innings rows are absent from it; callers must treat a missing
#'   name as "unknown target", not as zero.
#'
#' @keywords internal
resolve_targets_by_match <- function(deliveries, target = NULL) {
  if (is.null(target)) {
    # A single target across the whole frame would score every chase against
    # the highest first-innings total present in the batch.
    inn1 <- deliveries[deliveries$innings == 1, , drop = FALSE]
    if (nrow(inn1) == 0L) return(stats::setNames(numeric(0), character(0)))
    out <- tapply(inn1$total_runs, as.character(inn1$match_id),
                  function(v) max(v, na.rm = TRUE) + 1)
    return(stats::setNames(as.numeric(out), names(out)))
  }

  if (length(target) != 1L) {
    cli::cli_abort(c(
      "{.arg target} must be a single value or NULL, not length {length(target)}.",
      "i" = "Per-match targets are derived from the data when {.arg target} is NULL."
    ))
  }

  # Caller-supplied scalar applies to every match in the frame.
  ids <- unique(as.character(deliveries$match_id))
  stats::setNames(rep(as.numeric(target), length(ids)), ids)
}


#' Calculate Win Probability for All Deliveries in a Match
#'
#' Adds win probability columns to a data frame of deliveries.
#'
#' @param deliveries data.frame with delivery data. Must include:
#'   match_id, innings, over, ball, total_runs (cumulative), wickets_fallen.
#'   May span multiple matches; chase targets are resolved per `match_id`.
#' @param format Character. Match format.
#' @param target Integer. 2nd innings target, applied to every match in
#'   `deliveries`. Leave NULL (the default) to derive a separate target for
#'   each match from its own first innings.
#' @param models List. Pre-loaded models (optional).
#' @param wpa_failure_threshold Numeric in \[0, 1\]. Abort if this proportion
#'   of deliveries or more fail to produce a win probability. Default 0.01.
#'   Set to 1 to warn but never abort.
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
                                 models = NULL,
                                 wpa_failure_threshold = 0.01) {

  # Records the first prediction error so the summary below can name a cause
  # instead of just a count. Assigned to from inside the tryCatch handlers.
  first_error <- NULL

  # Load models if needed.
  #
  # There is no fallback projection here, despite what this branch used to
  # claim: predict_win_probability() aborts outright on NULL models for
  # limited-overs formats (see its "Could not load in-match models"). Warning
  # and continuing meant looping over every delivery only to write an all-NA
  # WPA column, which calculate_epr() then absorbed silently. Fail up front.
  if (is.null(models)) {
    models <- load_in_match_models(format)
    if (is.null(models)) {
      cli::cli_abort(c(
        "Could not load in-match models for format {.val {format}}.",
        "i" = "Run the in-match pipeline first (data-raw/models/in-match/).",
        "i" = "Without models every delivery yields NA WPA and the returned frame is unusable."
      ))
    }
  }

  target_by_match <- resolve_targets_by_match(deliveries, target)

  # Calculate win probability for each delivery.
  #
  # This loop is NOT sequential: every quantity it reads is a lag of a column
  # that already exists on `deliveries` (total_runs and wickets_fallen are
  # stored cumulative). It can be replaced by data.table::shift() plus two
  # batched predict() calls -- see docs/NEXT-STEPS.md. Left as-is here to keep
  # this change to the correctness fixes.
  n <- nrow(deliveries)
  win_prob_before <- numeric(n)
  win_prob_after <- numeric(n)

  for (i in seq_len(n)) {
    row <- deliveries[i, ]

    # Single-bracket, not [[: a match with no first innings in `deliveries`
    # is absent from target_by_match, and [[ errors on a missing name where
    # [ yields NA. Normalise that NA to NULL so predict_win_probability()'s
    # own "Target is required for 2nd innings" guard fires and the delivery
    # is counted as a failure, rather than NA flowing into the features.
    match_target <- unname(target_by_match[as.character(row$match_id)])
    if (length(match_target) != 1L || is.na(match_target)) match_target <- NULL

    # State AFTER this delivery, in cricket notation.
    overs <- calculate_over_ball(row$over, row$ball)

    # State BEFORE this delivery == state after delivery i-1.
    if (i == 1 || deliveries$match_id[i] != deliveries$match_id[i-1] ||
        deliveries$innings[i] != deliveries$innings[i-1]) {
      # Start of innings
      score_before <- 0
      wickets_before <- 0
      overs_before <- 0
    } else {
      score_before <- deliveries$total_runs[i-1] %||% 0
      wickets_before <- deliveries$wickets_fallen[i-1] %||% 0
      # Must be the after-state of i-1 so that win_prob_before[i] equals
      # win_prob_after[i-1] and WPA telescopes across the innings. The
      # previous `(ball - 1) / 10` was the state one ball earlier still.
      overs_before <- calculate_over_ball(deliveries$over[i-1],
                                          deliveries$ball[i-1])
    }

    # Win probability before
    wp_before <- tryCatch({
      predict_win_probability(
        current_score = score_before,
        wickets = wickets_before,
        overs = overs_before,
        innings = row$innings,
        target = if (row$innings == 2) match_target else NULL,
        format = format,
        models = models
      )$win_prob
    }, error = function(e) {
      first_error <<- first_error %||% conditionMessage(e)
      NA_real_
    })

    # Win probability after (current state)
    wp_after <- tryCatch({
      predict_win_probability(
        current_score = row$total_runs %||% 0,
        wickets = row$wickets_fallen %||% 0,
        overs = overs,
        innings = row$innings,
        target = if (row$innings == 2) match_target else NULL,
        format = format,
        models = models
      )$win_prob
    }, error = function(e) {
      first_error <<- first_error %||% conditionMessage(e)
      NA_real_
    })

    win_prob_before[i] <- wp_before
    win_prob_after[i] <- wp_after
  }

  # A failed prediction becomes NA, which the caller has no way to distinguish
  # from a genuine 0.5 swing unless the count is surfaced.
  #
  # To be precise about the blast radius: this function's only production
  # caller is plot_win_probability() (R/visualization.R). It does NOT feed
  # calculate_epr() -- the career ratings take their WPA from
  # player_game_data.R's SQL over cricinfo.balls.win_probability, a separate
  # path. So an all-NA run here corrupts a chart, not the ratings.
  n_failed <- sum(is.na(win_prob_before) | is.na(win_prob_after))
  if (n_failed > 0) {
    pct <- 100 * n_failed / max(1L, n)
    msg <- c(
      "Win probability failed for {n_failed} of {n} deliveries ({round(pct, 1)}%).",
      "i" = "First error: {first_error}",
      "!" = "WPA is NA for these rows; downstream career ratings will be biased low."
    )
    if (pct >= wpa_failure_threshold * 100) {
      cli::cli_abort(c(msg,
        "i" = "Raise {.arg wpa_failure_threshold} to proceed anyway."))
    }
    cli::cli_warn(msg)
  }

  # Add columns
  deliveries$win_prob_before <- win_prob_before
  deliveries$win_prob_after <- win_prob_after
  deliveries$wpa <- win_prob_after - win_prob_before

  deliveries
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

#' Fill Missing Model Features, Loudly
#'
#' Every in-match predictor used to do `for (col in feature_cols) if (!col %in%
#' names(data)) data[[col]] <- 0` -- silently substituting zero for any feature
#' the caller failed to supply.
#'
#' That is how bouncer's ODI chase model came to be useless in production
#' without anyone noticing. `predict_win_probability()` builds 12 features; the
#' stage-2 model was trained on 44. The other 32 -- `runs_needed`-derived chase
#' features, every momentum window, `innings1_total` -- were zero-filled on
#' every call, so the model saw a nonsense vector and returned a near-constant
#' answer: 0.883 when the chase needed 5 runs off 60 balls, 0.947 when it needed
#' 200 off 30. Benchmarked against 20,326 real ODI deliveries it scored a Brier
#' of 0.312, worse than predicting 0.5 every ball (0.250), while the scraped
#' ESPNcricinfo number scored 0.221.
#'
#' Zero is not a neutral value for a tree model. It is a real point in feature
#' space, usually far outside the training distribution, and the model answers
#' confidently from it. Missing features must therefore be loud.
#'
#' @param data data.frame of features supplied by the caller.
#' @param feature_cols Character vector the model was trained on.
#' @param what Character. Model name, for the message.
#' @param max_missing_frac Numeric. Abort above this proportion missing.
#'   Default 0.1 -- a tenth of the feature space silently zeroed is already
#'   enough to make the output meaningless.
#'
#' @return `data` with any missing columns added as 0.
#' @keywords internal
fill_model_features <- function(data, feature_cols, what = "model",
                                max_missing_frac = 0.1) {
  missing <- setdiff(feature_cols, names(data))
  if (length(missing) > 0) {
    frac <- length(missing) / max(1L, length(feature_cols))
    msg <- c(
      "{what}: {length(missing)} of {length(feature_cols)} features were not supplied and would be zero-filled.",
      "!" = "Zero is a real point in feature space, not a neutral one -- the model will answer confidently from it.",
      "i" = "Missing: {.field {utils::head(missing, 12)}}{if (length(missing) > 12) ' ...' else ''}"
    )
    if (frac > max_missing_frac) {
      cli::cli_abort(c(msg,
        "i" = "This is {round(100*frac)}% of the feature space. Supply them, or retrain on the features the caller can actually provide."))
    }
    cli::cli_warn(msg)
    for (col in missing) data[[col]] <- 0
  }
  data
}


