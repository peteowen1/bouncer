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
# LEGACY ELO CONSTANTS (from constants_elo.R)
# ============================================================================

ELO_START_RATING <- 1500

# K-factors by match type (learning rate)
K_FACTOR_TEST <- 32
K_FACTOR_ODI <- 24
K_FACTOR_T20 <- 20
K_FACTOR_DOMESTIC <- 16

# Average runs per ball by format (used in predictions)
AVG_RUNS_PER_BALL_TEST <- 3.0
AVG_RUNS_PER_BALL_ODI <- 5.0
AVG_RUNS_PER_BALL_T20 <- 7.5

# Base wicket probability per delivery by format
BASE_WICKET_PROB_TEST <- 0.035   # ~1 wicket per 30 balls
BASE_WICKET_PROB_ODI <- 0.030    # ~1 wicket per 33 balls
BASE_WICKET_PROB_T20 <- 0.025    # ~1 wicket per 40 balls

# ELO calculation constant (standard chess-style)
ELO_DIVISOR <- 400

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
