# Package Constants for Bouncer
#
# Main entry point for package constants.
#
#   - constants_skill.R: Player/venue skill indices, format mappings, projections, centrality
#   - constants_3way.R: 3-Way ELO system constants
#   - globals.R: All globalVariables() declarations

# ============================================================================
# DATA ORGANIZATION FORMAT CATEGORIES
# ============================================================================

# All data partition folders (match_type x gender x team_type)
# Based on actual partitions created by daily scraper
DATA_FOLDERS <- c(
  # Test format
  "Test_male_international",
  "Test_female_international",
  # ODI format
  "ODI_male_international",
  "ODI_female_international",
  # T20 format (includes franchise leagues)
  "T20_male_international",
  "T20_male_club",
  "T20_female_international",
  "T20_female_club",
  # IT20 (domestic T20 internationals)
  "IT20_male_international",
  "IT20_female_international",
  # MDM (multi-day matches / first-class)
  "MDM_male_international",
  "MDM_male_club",
  "MDM_female_international",
  "MDM_female_club",
  # ODM (domestic one-day)
  "ODM_male_international",
  "ODM_male_club"
)

NULL


# ============================================================================
# TEAM COMPARISON CONSTANTS
# ============================================================================

# Home advantage ELO bonus (applied to team1 when neutral_venue = FALSE)
HOME_ADVANTAGE_ELO <- 50

# ELO calculation constant (standard chess-style)
ELO_DIVISOR <- 400

# Legacy start rating (used by team ELO initialization)
ELO_START_RATING <- 1500

# ============================================================================
# MODEL FILE CONSTANTS (from constants_models.R)
# ============================================================================

MODEL_AGNOSTIC_PATTERN <- "agnostic_outcome_%s.ubj"

# Full outcome models (includes player skill features)
# Format: full_outcome_{format}.ubj where format = shortform|longform
MODEL_FULL_PATTERN <- "full_outcome_%s.ubj"

# Pre-match margin prediction models
# Format: {format}_margin_model.ubj where format = t20|odi|test
MODEL_MARGIN_PATTERN <- "%s_margin_model.ubj"

# ============================================================================
# BALL-OUTCOME MODEL CATEGORIES (bouncerverse#81/D-P50, stage 2)
# ============================================================================

# Shared source of truth for the agnostic/full ball-outcome models' output
# categories. Previously hardcoded independently in ~15 places (roxygen
# comments, num_class= literals, an outcome_labels vector, a runs_values
# vector, the simulator's switch()) -- see
# docs/plans/D-P50-WIDE-CATEGORY-REBUILD.md for the audit that found the
# duplication. Column order in every trained model's probability output
# matches this vector's order exactly; changing the order here without
# retraining every model would silently misalign predictions.
#
# "wide" added #81/D-P50 stage 3. No-balls stay folded into the run
# categories (a no-ball's batter-runs are a legitimate 0-6 value,
# structurally identical to a legal ball's) -- see the plan doc for why
# only wides needed a dedicated bucket. A wide where a wicket ALSO falls
# (rare: stumped/run-out/hit-wicket only) still categorizes as "wicket",
# checked first in every case_when() that builds this label -- adding
# "wide" did not reprioritize that.
OUTCOME_CATEGORIES <- c("wicket", "0", "1", "2", "3", "4", "6", "wide")

# Run value contributed by each category, same order as OUTCOME_CATEGORIES.
# Wicket is 0 by modeling convention -- this multinomial treats "a wicket
# fell" as its own bucket regardless of any runs also scored on that ball,
# unchanged from the pre-existing (pre-constant) behavior. "wide" is not a
# single deterministic value like the run categories -- it's the empirical
# mean extras conceded on a wide (1.217, measured over 195,133 real wide
# deliveries: median 1, up to 5 on a chaotic one with byes run) rather than
# its own sub-multinomial, per the plan doc's sizing (splitting it further
# would starve each sub-bucket for a small accuracy gain).
OUTCOME_RUN_VALUES <- c(0, 0, 1, 2, 3, 4, 6, 1.217)

# In-match win probability models (2-stage)
# Format: {format}_stage1_results.rds, {format}_stage2_results.rds
MODEL_STAGE1_PATTERN <- "%s_stage1_results.rds"
MODEL_STAGE2_PATTERN <- "%s_stage2_results.rds"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

#' Get Model Filename
#'
#' Returns the standardized filename for a model type and format.
#'
#' @param model_type Character. One of: "agnostic", "full", "margin",
#'   "stage1", "stage2"
#' @param format Character. Model format:
#'   - For outcome models: "shortform" or "longform"
#'   - For margin/stage models: "t20", "odi", or "test"
#'
#' @return Character. The model filename (not full path).
#'
#' @examples
#' get_model_filename("agnostic", "shortform")
#' # Returns: "agnostic_outcome_shortform.ubj"
#'
#' get_model_filename("margin", "t20")
#' # Returns: "t20_margin_model.ubj"
#'
#' @export
get_model_filename <- function(model_type, format) {
  model_type <- tolower(model_type)
  format <- tolower(format)

  switch(model_type,
    "agnostic" = sprintf(MODEL_AGNOSTIC_PATTERN, format),
    "full" = sprintf(MODEL_FULL_PATTERN, format),
    "margin" = sprintf(MODEL_MARGIN_PATTERN, format),
    "stage1" = sprintf(MODEL_STAGE1_PATTERN, format),
    "stage2" = sprintf(MODEL_STAGE2_PATTERN, format),
    cli::cli_abort("Unknown model_type: {model_type}. Expected: agnostic, full, margin, stage1, stage2")
  )
}


#' Get Full Model Path
#'
#' Returns the full path to a model file in the models directory.
#'
#' @param model_type Character. One of: "agnostic", "full", "margin",
#'   "stage1", "stage2"
#' @param format Character. Model format (see \code{get_model_filename}).
#' @param models_dir Character. Optional models directory. If NULL, uses
#'   \code{get_models_dir()}.
#'
#' @return Character. Full path to the model file.
#'
#' @examples
#' \dontrun{
#' get_model_path("agnostic", "shortform")
#' # Returns: "/path/to/bouncerdata/models/agnostic_outcome_shortform.ubj"
#' }
#'
#' @export
get_model_path <- function(model_type, format, models_dir = NULL) {
  if (is.null(models_dir)) {
    models_dir <- get_models_dir(create = FALSE)
  }

  filename <- get_model_filename(model_type, format)
  file.path(models_dir, filename)
}


#' Check if Model Exists
#'
#' Checks if a model file exists in the models directory.
#'
#' @param model_type Character. Model type (see \code{get_model_filename}).
#' @param format Character. Model format.
#' @param models_dir Character. Optional models directory.
#'
#' @return Logical. TRUE if model file exists.
#'
#' @examples
#' \dontrun{
#' model_exists("agnostic", "shortform")
#' model_exists("full", "longform")
#' model_exists("margin", "t20")
#' }
#'
#' @export
model_exists <- function(model_type, format, models_dir = NULL) {
  path <- get_model_path(model_type, format, models_dir)
  file.exists(path)
}


#' Dismissal kinds credited to the bowler
#'
#' Run outs, retirements and obstruction are dismissals but not the bowler's
#' work. Crediting them inflates a bowler's wickets and understates his average
#' -- measured across T20 male cricket, **10,113 of 132,814 dismissals (7.6%)**
#' are not the bowler's, 9,845 of them run outs. Fixed across `data_queries.R`,
#' `player_metrics.R` and `user_api.R` in bouncerverse#31, where it had inflated
#' T20 wickets by 9.7%, understated bowling averages by 1.94 runs, and
#' **reordered** bowlers rather than merely rescaling them.
#'
#' This constant exists because the same six-element list was written out
#' separately in five places. That is the shape that let the rating tables'
#' schema drift in bouncerverse#45: two declarations of one truth.
#'
#' Note `retired hurt` is not a dismissal at all, and was being counted as one.
#'
#' @format Character vector of `wicket_kind` values.
#' @keywords internal
BOWLER_WICKET_KINDS <- c("caught", "bowled", "lbw", "caught and bowled",
                         "stumped", "hit wicket")

#' The same list as a SQL `IN` clause body
#'
#' @return Character scalar, quoted and comma-separated, for interpolation into
#'   `COALESCE(wicket_kind,'') IN (...)`.
#' @keywords internal
bowler_wicket_kinds_sql <- function() {
  paste0("'", BOWLER_WICKET_KINDS, "'", collapse = ",")
}
