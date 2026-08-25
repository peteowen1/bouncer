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


#' Date the post-delivery leak was fixed in the ball-outcome features
#'
#' Any outcome model trained before this was fitted on `batting_score` and
#' `wickets_fallen` in their POST-delivery frame — features that already knew
#' the delivery's own outcome (D-P38). Such a model is not merely stale, it is
#' wrong in a way that looks healthy: over-level calibration stayed at 0.856
#' predicted against 0.909 actual throughout, while `runs_difference` correlated
#' **1.000** with the runs off that ball.
#'
#' @keywords internal
MODEL_LEAK_FIX_DATE <- "2026-08-18"

#' Refuse an outcome model that predates the leak fix
#'
#' Models carry their build date as the xgboost attribute
#' `bouncer_build_date`, stamped at save time by the trainers in
#' `data-raw/models/ball-outcome/`. An **unstamped** model is refused too: every
#' artefact built before the stamp existed also predates the fix, so "no stamp"
#' and "stamped too early" mean the same thing.
#'
#' This exists because `load_agnostic_model()` and `load_full_model()` prefer the
#' `bouncermodels` release over local disk, and the release served a 2026-03-27
#' vintage — so any machine with `bouncermodels` installed silently got the
#' leaked baseline in preference to the corrected one sitting on disk
#' (bouncerverse#50).
#'
#' @param model Loaded xgb.Booster
#' @param model_name Character, for the error message
#' @param source Character, where it came from, for the error message
#' @return `model`, invisibly, when it passes
#' @keywords internal
.check_model_vintage <- function(model, model_name, source) {
  built <- tryCatch(xgboost::xgb.attr(model, "bouncer_build_date"),
                    error = function(e) NULL)

  if (is.null(built) || !nzchar(built)) {
    cli::cli_abort(c(
      "{.val {model_name}} from {source} carries no build date.",
      "x" = "Every model built before the {MODEL_LEAK_FIX_DATE} leak fix is unstamped,
             and was trained on post-delivery features that knew the answer (D-P38).",
      "i" = "Retrain it, or re-stamp a known-good artefact with
             {.code xgboost::xgb.attr(m, 'bouncer_build_date') <- '<date>'}.",
      "i" = "See bouncerverse#50."
    ))
  }

  if (as.Date(built) < as.Date(MODEL_LEAK_FIX_DATE)) {
    cli::cli_abort(c(
      "{.val {model_name}} from {source} was built {built}, before the
       {MODEL_LEAK_FIX_DATE} leak fix.",
      "x" = "It was trained on post-delivery {.field batting_score} and
             {.field wickets_fallen} (D-P38), so its residuals are not skill.",
      "i" = "Retrain it. See bouncerverse#50."
    ))
  }

  invisible(model)
}


#' The xgboost attribute name feature names are stamped under
#'
#' Boosters trained by `02_train_full_model.R` carry `feature_names` of length
#' 0 (an xgboost/UBJ round-trip quirk), so `m$feature_names` cannot be trusted
#' and `xgb.attr(m, "num_feature")` is empty too -- the width survives only
#' inside the booster config. That leaves WIDTH as the only alignment check:
#' two frames of the same width with columns in a different order predict
#' nonsense, silently, and no width check can see it (bouncerverse#76).
#'
#' Same pattern as `bouncer_build_date` (D-P43): stamp a plain xgboost
#' attribute at save time, and have the reader trust the attribute over the
#' booster's own (unreliable) `feature_names` slot.
#'
#' @keywords internal
FEATURE_NAMES_ATTR <- "bouncer_feature_names"

#' Encode a feature name vector for the xgboost attribute
#'
#' `xgb.attr<-` stores a single character scalar, not a vector, so the names
#' are joined on `|`. None of this package's feature names contain `|`.
#'
#' @param feature_names Character vector, in training column order.
#' @return A single string.
#' @keywords internal
.encode_feature_names <- function(feature_names) {
  paste(feature_names, collapse = "|")
}

#' Read the stamped feature-name attribute back off a model
#'
#' @param model Loaded xgb.Booster.
#' @return Character vector of feature names in training order, or `NULL` if
#'   the model predates this stamp. `02_train_full_model.R` stamps new saves
#'   automatically; an existing `.ubj` can also be re-stamped without a
#'   retrain when the exact training-order feature list can be recovered by
#'   other means (e.g. git history proving the trainer's column order hasn't
#'   changed since that model was saved) -- done for the three published
#'   `full_outcome_*.ubj` models on 2026-08-24 (bouncerverse#76/#79), verified
#'   by a byte-identical prediction round-trip before/after the re-save.
#' @keywords internal
.stamped_feature_names <- function(model) {
  raw <- tryCatch(xgboost::xgb.attr(model, FEATURE_NAMES_ATTR), error = function(e) NULL)
  if (is.null(raw) || !nzchar(raw)) return(NULL)
  strsplit(raw, "|", fixed = TRUE)[[1]]
}

# Warn at most once per session per label -- a predict function called
# per-delivery would otherwise print the same "no stamp, can't check" warning
# thousands of times.
.no_stamp_warned <- new.env(parent = emptyenv())

#' Abort if a serving frame's columns don't match a model's training order
#'
#' Positional `xgb.DMatrix`/`predict()` silently accepts a same-width frame
#' with columns in the wrong order and returns plausible, wrong numbers
#' (bouncerverse#76). This is the runtime half of that fix: a script run by
#' hand (`data-raw/validation/full_model_serving_alignment.R`) can catch drift
#' only if someone remembers to run it and reads the output. This function
#' runs on every `predict_full_outcome()`/`predict_agnostic_outcome()` call
#' instead, so a drift aborts loudly at the point it would otherwise produce a
#' silently wrong prediction.
#'
#' A model with no stamped names (every agnostic model as of bouncerverse#79 --
#' only the full model has been independently re-verified and stamped) cannot
#' be checked this way; that is reported once per session rather than silently,
#' so "checked and fine" and "never checked" stay distinguishable.
#'
#' @param model Loaded xgb.Booster.
#' @param features Data frame about to become the DMatrix, in serving order.
#' @param label Character. Used in messages, e.g. "full model (t20)".
#' @keywords internal
.assert_feature_alignment <- function(model, features, label) {
  trained <- .stamped_feature_names(model)
  if (is.null(trained)) {
    key <- paste0("nostamp:", label)
    if (is.null(.no_stamp_warned[[key]])) {
      assign(key, TRUE, envir = .no_stamp_warned)
      cli::cli_warn(c(
        "{label}: booster carries no stamped feature names -- column order is UNVERIFIED.",
        "i" = "A same-width, differently-ordered frame would predict silently wrong numbers."
      ))
    }
    return(invisible(NULL))
  }

  served <- names(features)
  if (identical(trained, served)) return(invisible(NULL))

  missing <- setdiff(trained, served)
  extra <- setdiff(served, trained)
  if (length(missing) || length(extra)) {
    cli::cli_abort(c(
      "{label}: serving frame does not match the model's trained features.",
      "x" = if (length(missing)) "Missing at serving time: {.val {missing}}",
      "x" = if (length(extra)) "Served but not trained on: {.val {extra}}"
    ))
  }
  # Same set, different order -- an unnamed DMatrix is positional.
  cli::cli_abort(
    "{label}: same features as training, but in a DIFFERENT ORDER -- would predict nonsense silently."
  )
}


#' Should the loaders read local disk before the release?
#'
#' **The rule: release first, local disk as fallback.** A consumer who installs
#' `bouncermodels` should get the published model, not whatever happens to be in
#' a sibling directory. Decided deliberately in bouncerverse#50 rather than left
#' as an accident of load order — and the reason it is safe to keep is that
#' `.check_model_vintage()` now makes a stale release **loud**: the failure this
#' rule caused (a five-month-old baseline preferred over a corrected local file)
#' is now an error, not a silent substitution.
#'
#' The one case the rule serves badly is a developer who has just retrained and
#' wants to test before publishing. That is what the option is for:
#' `options(bouncer.prefer_local_models = TRUE)`.
#'
#' @keywords internal
.prefer_local_models <- function() {
  isTRUE(getOption("bouncer.prefer_local_models", FALSE))
}


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
#' @section Feature order safety:
#' Unlike [load_full_model()] (bouncerverse#76), this booster's own feature
#' order has not been independently re-verified against
#' [prepare_agnostic_features()] and stamped, so [predict_agnostic_outcome()]'s
#' alignment check cannot run -- it warns once per session instead of
#' verifying. Still prefer it over a hand-built `xgb.DMatrix`, which gets no
#' check and no warning at all.
#'
#' @export
load_agnostic_model <- function(format = c("t20", "odi", "test"),
                                 model_dir = NULL) {

  format <- match.arg(format)
  model_name <- paste0("agnostic_outcome_", format)

  # Try bouncermodels package first (preferred)
  if (is.null(model_dir) && !.prefer_local_models() &&
      requireNamespace("bouncermodels", quietly = TRUE)) {
    model <- tryCatch(
      bouncermodels::load_bouncer_model(model_name, verbose = FALSE),
      error = function(e) {
        # Every condition load_bouncer_model() raises is a real one -- unknown
        # registry name, download failure, or a sha256 INTEGRITY failure on a
        # corrupted release asset -- never "the package isn't installed" (that
        # already returned via requireNamespace() above). Silently falling
        # through to local disk here used to look identical to a clean
        # release load with nothing to distinguish them; warn instead.
        cli::cli_warn("bouncermodels release load failed for {model_name}: {conditionMessage(e)}")
        NULL
      }
    )
    if (!is.null(model)) {
      .check_model_vintage(model, model_name, "bouncermodels")
      cli::cli_alert_success("Loaded agnostic {format} model from bouncermodels")
      return(model)
    }
  }

  # Fall back to local file
  if (is.null(model_dir)) model_dir <- get_models_dir()
  model_file <- file.path(model_dir, get_model_filename("agnostic", format))

  if (!file.exists(model_file)) {
    cli::cli_abort(c(
      "Agnostic model not found at: {.file {model_file}}",
      "i" = "Install bouncermodels: devtools::install_github('peteowen1/bouncermodels')",
      "i" = "Or run data-raw/models/ball-outcome/01_train_agnostic_model.R"
    ))
  }

  if (!requireNamespace("xgboost", quietly = TRUE)) {
    cli::cli_abort("Package {.pkg xgboost} is required. Please install it.")
  }

  model <- xgboost::xgb.load(model_file)
  .check_model_vintage(model, model_name, "local disk")
  cli::cli_alert_success("Loaded agnostic {format} model from {.file {model_file}}")
  return(model)
}


#' Predict Agnostic Outcome Probabilities
#'
#' Generates outcome probability predictions using the agnostic model.
#' Returns a matrix of probabilities in `OUTCOME_CATEGORIES` order
#' (`R/constants.R`) -- wicket, 0-4, 6, wide, since #81/D-P50 stage 3.
#'
#' @param model XGBoost model object from load_agnostic_model()
#' @param delivery_data Data frame of deliveries with required features:
#'   match_type, innings, over, ball, wickets_fallen, runs_difference,
#'   gender, and optionally: is_knockout, event_tier
#' @param format Character. Format type: "t20", "odi", or "test"
#'
#' @return Matrix with 7 columns representing probabilities for each outcome:
#'   col1=P(wicket), col2=P(0 runs), col3=P(1 run), col4=P(2 runs),
#'   col5=P(3 runs), col6=P(4 runs), col7=P(6 runs)
#'
#' @section Feature order safety:
#' Aborts if the serving frame's columns don't match the model's stamped
#' training-order feature list (bouncerverse#76) -- but the agnostic model has
#' not been independently re-verified and stamped the way the full model has
#' (bouncerverse#79), so this currently only warns once per session that the
#' check could not run, rather than actually verifying alignment.
#'
#' @export
predict_agnostic_outcome <- function(model, delivery_data, format = c("t20", "odi", "test")) {

  format <- match.arg(format)

  if (!requireNamespace("xgboost", quietly = TRUE)) {
    cli::cli_abort("Package {.pkg xgboost} is required. Please install it.")
  }

  # Prepare features for agnostic model
  features <- prepare_agnostic_features(delivery_data, format)
  .assert_feature_alignment(model, features, sprintf("agnostic model (%s)", format))

  # Create DMatrix and predict. nthread=4 matches the cap the agnostic
  # trainer pins for the same reason (01_train_agnostic_model.R, #81/D-P50
  # stage 3): unbounded default threading crashed reproducibly via a native
  # OpenMP fault during Test-format xgb.cv() on this machine. predict() takes
  # no nthread argument of its own -- the DMatrix's setting is the only
  # lever -- and a single-shot predict() over millions of rows (RAA scoring)
  # hits the same native code path CV did.
  dmat <- xgboost::xgb.DMatrix(data = as.matrix(features), nthread = 4)
  probs <- predict(model, dmat)

  # Ensure probabilities sum to 1 (numerical precision fix)
  # Guard against division by zero (can happen with degenerate inputs)
  row_sums <- rowSums(probs)
  row_sums[row_sums == 0] <- 1  # Prevent division by zero; these rows will have uniform probs
  probs <- probs / row_sums

  return(probs)
}


#' Get Expected Runs from Agnostic Model Predictions
#'
#' Converts the `OUTCOME_CATEGORIES`-shaped probability distribution into
#' expected runs.
#'
#' @param probs Matrix of probabilities from predict_agnostic_outcome()
#'
#' @return Numeric vector of expected runs per delivery.
#'   Formula: E(runs) = 0*P(wicket) + 0*P(0) + 1*P(1) + 2*P(2) + 3*P(3) + 4*P(4) + 6*P(6)
#'
#' @keywords internal
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
#' @keywords internal
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
#' @keywords internal
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
    cli::cli_abort("delivery_data must have {.field runs_batter} or {.field runs_total} column")
  }

  if (!"is_wicket" %in% names(delivery_data)) {
    cli::cli_abort("delivery_data must have {.field is_wicket} column")
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
#' @section Feature order safety (bouncerverse#76):
#' `predict()` on an xgboost matrix is positional -- it silently accepts a
#' same-width frame with columns in the wrong order and returns plausible,
#' wrong numbers. The three published full models carry their training-order
#' feature list as a `bouncer_feature_names` attribute (verified against
#' `prepare_full_features()`, `data-raw/validation/full_model_serving_alignment.R`).
#' [predict_full_outcome()] checks the serving frame against that attribute on
#' every call and aborts on a mismatch -- build a `xgb.DMatrix` by hand instead
#' and that protection is gone.
#'
#' @export
load_full_model <- function(format = c("t20", "odi", "test"),
                             model_dir = NULL) {

  format <- match.arg(format)
  model_name <- paste0("full_outcome_", format)

  # Try bouncermodels package first (preferred)
  if (is.null(model_dir) && !.prefer_local_models() &&
      requireNamespace("bouncermodels", quietly = TRUE)) {
    model <- tryCatch(
      bouncermodels::load_bouncer_model(model_name, verbose = FALSE),
      error = function(e) {
        # See load_agnostic_model()'s matching comment: every condition here
        # is real (unknown name, download failure, sha256 integrity failure
        # on a corrupted release asset), never "not installed". Warn rather
        # than silently falling through to local disk.
        cli::cli_warn("bouncermodels release load failed for {model_name}: {conditionMessage(e)}")
        NULL
      }
    )
    if (!is.null(model)) {
      .check_model_vintage(model, model_name, "bouncermodels")
      cli::cli_alert_success("Loaded full {format} model from bouncermodels")
      return(model)
    }
  }

  # Fall back to local file
  if (is.null(model_dir)) model_dir <- get_models_dir()
  model_file <- file.path(model_dir, get_model_filename("full", format))

  if (!file.exists(model_file)) {
    cli::cli_abort(c(
      "Full model not found at: {.file {model_file}}",
      "i" = "Install bouncermodels: devtools::install_github('peteowen1/bouncermodels')",
      "i" = "Or run data-raw/models/ball-outcome/02_train_full_model.R"
    ))
  }

  if (!requireNamespace("xgboost", quietly = TRUE)) {
    cli::cli_abort("Package {.pkg xgboost} is required. Please install it.")
  }

  model <- xgboost::xgb.load(model_file)
  .check_model_vintage(model, model_name, "local disk")

  cli::cli_alert_success("Loaded full {format} model from {.file {model_file}}")

  return(model)
}


#' Predict Full Outcome Probabilities
#'
#' Generates outcome probability predictions using the full model with all features.
#' Returns a matrix of `OUTCOME_CATEGORIES`-shaped probabilities (wicket, 0,
#' 1, 2, 3, 4, 6, wide -- see `R/constants.R`).
#'
#' @param model XGBoost model object from load_full_model()
#' @param delivery_data Data frame of deliveries with required features.
#'   Must include all context, player, team, and venue skill features.
#' @param format Character. Format type: "t20", "odi", or "test"
#'
#' @return Matrix with one column per `OUTCOME_CATEGORIES` entry, in that
#'   order.
#'
#' @section Feature order safety (bouncerverse#76):
#' Aborts if the serving frame's columns don't match the model's stamped
#' training-order feature list, rather than silently predicting from a
#' misaligned positional matrix.
#'
#' @export
predict_full_outcome <- function(model, delivery_data, format = c("t20", "odi", "test")) {

  format <- match.arg(format)

  if (!requireNamespace("xgboost", quietly = TRUE)) {
    cli::cli_abort("Package {.pkg xgboost} is required. Please install it.")
  }

  # Prepare features for full model
  features <- prepare_full_features(delivery_data, format)
  .assert_feature_alignment(model, features, sprintf("full model (%s)", format))

  # Create DMatrix and predict. nthread=4, same reasoning as
  # predict_agnostic_outcome() above -- the full model trainer pins it for
  # the same native OpenMP crash risk (#81/D-P50 stage 5).
  dmat <- xgboost::xgb.DMatrix(data = as.matrix(features), nthread = 4)
  probs <- predict(model, dmat)

  # Ensure probabilities sum to 1 (numerical precision fix)
  # Guard against division by zero (can happen with degenerate inputs)
  row_sums <- rowSums(probs)
  row_sums[row_sums == 0] <- 1  # Prevent division by zero; these rows will have uniform probs
  probs <- probs / row_sums

  return(probs)
}


#' Get Expected Runs from Full Model Predictions
#'
#' Converts the `OUTCOME_CATEGORIES`-shaped probability distribution into
#' expected runs.
#'
#' @param probs Matrix of probabilities from predict_full_outcome()
#'
#' @return Numeric vector of expected runs per delivery.
#'
#' @keywords internal
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
#' @keywords internal
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
    df$over_ball <- calculate_over_ball(df$over, df$ball)
  }

  # Trailing ELO columns the trained models carry (see the select below).
  for (nm in c("elo_run_diff", "elo_wicket_diff", "elo_venue_run")) {
    if (!nm %in% names(df)) df[[nm]] <- 0
  }

  # Fill missing skill indices with neutral values from constants
  # Player skills - use starting values if missing
  player_start_vals <- get_skill_start_values(format)
  df$batter_scoring_index <- dplyr::coalesce(df$batter_scoring_index, player_start_vals$scoring_index)
  df$batter_survival_rate <- dplyr::coalesce(df$batter_survival_rate, player_start_vals$survival_rate)
  df$bowler_economy_index <- dplyr::coalesce(df$bowler_economy_index, player_start_vals$economy_index)
  df$bowler_strike_rate <- dplyr::coalesce(df$bowler_strike_rate, player_start_vals$strike_rate)
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

  # #81/D-P50 stage 5: is_free_hit_int, mirroring prepare_agnostic_features().
  # Most callers have no free-hit data at all -- default to "not on a free
  # hit"; the final coalesce(., 0) sweep below also catches a stray NA.
  if (!"is_free_hit" %in% names(df)) df$is_free_hit <- FALSE
  df$is_free_hit_int <- as.integer(dplyr::coalesce(as.logical(df$is_free_hit), FALSE))

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

    # Select features in the correct order (must match training). The trained
    # models carry three trailing ELO columns (zero-filled at training when
    # INCLUDE_ELO_FEATURES is off); this xgboost build silently default-routes
    # absent columns instead of erroring, so they are supplied explicitly --
    # same hazard class as the agnostic league features (2026-08-13).
    result <- result %>%
      dplyr::mutate(
        elo_run_diff = dplyr::coalesce(elo_run_diff, 0),
        elo_wicket_diff = dplyr::coalesce(elo_wicket_diff, 0),
        elo_venue_run = dplyr::coalesce(elo_venue_run, 0)
      ) %>%
      dplyr::select(
        # Context features
        format_t20, format_odi,
        innings_num, over, ball,
        wickets_fallen, runs_difference, overs_left,
        phase_powerplay, phase_middle, phase_death,
        gender_male,
        is_knockout, event_tier,
        is_free_hit_int,
        # Player skills
        batter_scoring_index, batter_survival_rate,
        bowler_economy_index, bowler_strike_rate,
        batter_experience, bowler_experience,
        # Team skills
        batting_team_runs_skill, batting_team_wicket_skill,
        bowling_team_runs_skill, bowling_team_wicket_skill,
        # Venue skills
        venue_run_rate, venue_wicket_rate,
        venue_boundary_rate, venue_dot_rate,
        # ELO features, trailing (zeroed unless the caller supplies them)
        elo_run_diff, elo_wicket_diff, elo_venue_run
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
        is_free_hit_int,
        # Player skills
        batter_scoring_index, batter_survival_rate,
        bowler_economy_index, bowler_strike_rate,
        batter_experience, bowler_experience,
        # Team skills
        batting_team_runs_skill, batting_team_wicket_skill,
        bowling_team_runs_skill, bowling_team_wicket_skill,
        # Venue skills
        venue_run_rate, venue_wicket_rate,
        venue_boundary_rate, venue_dot_rate,
        # ELO features, trailing (zeroed unless the caller supplies them)
        elo_run_diff, elo_wicket_diff, elo_venue_run
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
    df$over_ball <- calculate_over_ball(df$over, df$ball)
  }

  # League running averages -- the models trained since 2026-03-14 carry
  # league_avg_runs / league_avg_wicket (16% of the T20 model's gain between
  # them), and this xgboost build does NOT error when a prediction matrix has
  # fewer columns than the booster: the absent features are routed down each
  # tree's default branch. Serving without them biased E[runs] by +0.17
  # runs/ball on the model's own training data before this was caught
  # (2026-08-13). Callers that cannot supply real values get training's own
  # no-history default, exactly as the training SQL COALESCEs it.
  default_runs <- switch(format,
    t20 = EXPECTED_RUNS_T20, odi = EXPECTED_RUNS_ODI, EXPECTED_RUNS_TEST)
  default_wicket <- switch(format,
    t20 = EXPECTED_WICKET_T20, odi = EXPECTED_WICKET_ODI, EXPECTED_WICKET_TEST)
  if (!"league_avg_runs" %in% names(df)) df$league_avg_runs <- NA_real_
  if (!"league_avg_wicket" %in% names(df)) df$league_avg_wicket <- NA_real_
  df$league_avg_runs <- dplyr::coalesce(df$league_avg_runs, default_runs)
  df$league_avg_wicket <- dplyr::coalesce(df$league_avg_wicket, default_wicket)

  # #81/D-P50 stage 3 added is_free_hit_int as a training feature. Most
  # callers (cricinfo-sourced scoring, the simulator) have no free-hit data
  # at all -- default to "not on a free hit" rather than erroring, same
  # shape as the league_avg_* default above.
  if (!"is_free_hit" %in% names(df)) df$is_free_hit <- FALSE
  if (!is.logical(df$is_free_hit)) {
    # as.logical() on an unparseable value (e.g. the strings "0"/"1") returns
    # NA with no warning, which coalesce() below would then silently launder
    # to FALSE -- mispricing a real free hit as a normal delivery with zero
    # indication. Fail loud instead; a genuine NA (unbackfilled row) is still
    # fine and is handled by the coalesce.
    cli::cli_abort("is_free_hit must be logical, got {.cls {class(df$is_free_hit)[1]}}.")
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
        event_tier = dplyr::coalesce(as.numeric(event_tier), 2),  # Default tier 2
        is_free_hit_int = as.integer(dplyr::coalesce(as.logical(is_free_hit), FALSE))
      )

    # Select features (order must match training exactly)
    result <- result %>%
      dplyr::select(
        format_t20, format_odi,
        innings_num, over, ball,
        wickets_fallen, runs_difference, overs_left,
        phase_powerplay, phase_middle, phase_death,
        gender_male,
        is_knockout, event_tier, is_free_hit_int,
        league_avg_runs, league_avg_wicket
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
        event_tier = dplyr::coalesce(as.numeric(event_tier), 2),
        is_free_hit_int = as.integer(dplyr::coalesce(as.logical(is_free_hit), FALSE))
      )

    # Select features (no overs_left for Test; order must match training)
    result <- result %>%
      dplyr::select(
        innings_num, over, ball,
        wickets_fallen, runs_difference,
        phase_new_ball, phase_middle, phase_old_ball,
        gender_male,
        is_knockout, event_tier, is_free_hit_int,
        league_avg_runs, league_avg_wicket
      )
  }

  # Handle any NA values
  result <- result %>%
    dplyr::mutate(
      dplyr::across(dplyr::everything(), ~ dplyr::coalesce(., 0))
    )

  return(result)
}
