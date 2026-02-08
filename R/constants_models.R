# Model Filename Constants
#
# Defines standardized filenames for trained models.
# All XGBoost models use .ubj (universal binary json) format for:
# - Smaller file size (binary compression)
# - Faster load times
# - Full model preservation (feature names, etc.)
#
# Split from constants.R for better maintainability.

# ============================================================================
# MODEL FILENAME PATTERNS
# ============================================================================
# These patterns define how model files are named in bouncerdata/models/

# Agnostic outcome models (no player skill, just match state)
# Format: agnostic_outcome_{format}.ubj where format = shortform|longform
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
#' @export
model_exists <- function(model_type, format, models_dir = NULL) {
  path <- get_model_path(model_type, format, models_dir)
  file.exists(path)
}
