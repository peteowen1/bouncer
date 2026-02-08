# Input Validation Helper Functions
#
# Functions for validating input data before processing.
# These help catch errors early and provide clear error messages.

# ============================================================================
# CORE DELIVERY DATA VALIDATION
# ============================================================================

#' Validate Delivery Data
#'
#' Checks that a data frame has the required columns for delivery-level analysis.
#' This is the minimum validation for any delivery data processing.
#'
#' @param df Data frame to validate.
#' @param required_cols Character vector of required column names.
#'   If NULL (default), uses standard delivery columns.
#' @param context Character. Context for error message (e.g., function name).
#'
#' @return Invisibly returns TRUE if validation passes. Stops with error if not.
#'
#' @examples
#' \dontrun{
#' # Validates that df has required columns
#' validate_delivery_data(df)
#'
#' # Custom required columns
#' validate_delivery_data(df, required_cols = c("match_id", "over", "ball"))
#' }
#'
#' @export
validate_delivery_data <- function(df,
                                    required_cols = NULL,
                                    context = "validate_delivery_data") {
  if (is.null(required_cols)) {
    required_cols <- c("over", "ball", "innings", "wickets_fallen")
  }

  # Check if data frame

if (!is.data.frame(df)) {
    cli::cli_abort(c(
      "Expected a data frame",
      "x" = "{.arg df} is class {.cls {class(df)[1]}}"
    ), call = NULL)
  }

  # Check for empty data
  if (nrow(df) == 0) {
    cli::cli_warn(c(
      "!" = "Data frame has zero rows in {.fn {context}}"
    ))
    return(invisible(TRUE))
  }

  # Check for missing columns
  missing_cols <- setdiff(required_cols, names(df))

  if (length(missing_cols) > 0) {
    cli::cli_abort(c(
      "Missing required columns in {.fn {context}}",
      "x" = "Missing: {.field {missing_cols}}",
      "i" = "Available columns: {.field {head(names(df), 10)}}{if(ncol(df) > 10) '...' else ''}"
    ), call = NULL)
  }

  invisible(TRUE)
}


# ============================================================================
# MODEL FEATURE VALIDATION
# ============================================================================

#' Validate Agnostic Model Features
#'
#' Checks that a data frame has the required columns for the agnostic model.
#' The agnostic model uses only context features (no player/team/venue identity).
#'
#' @param df Data frame to validate.
#' @param format Character. Cricket format: "t20", "odi", or "test".
#'
#' @return Invisibly returns TRUE if validation passes. Stops with error if not.
#'
#' @details
#' Required columns for agnostic model:
#' - Core: over, ball, innings, wickets_fallen
#' - Optional but recommended: gender, match_type
#'
#' @export
validate_agnostic_features <- function(df, format = "t20") {
  # Core required columns
  core_cols <- c("over", "ball", "innings", "wickets_fallen")

  validate_delivery_data(df, required_cols = core_cols, context = "validate_agnostic_features")

  # Warn about missing optional columns that improve predictions
  optional_cols <- c("gender", "runs_difference", "is_knockout", "event_tier")
  missing_optional <- setdiff(optional_cols, names(df))

  if (length(missing_optional) > 0) {
    cli::cli_warn(c(
      "!" = "Missing optional columns for agnostic model",
      "i" = "Missing: {.field {missing_optional}}",
      "i" = "These will be set to defaults but predictions may be less accurate"
    ))
  }

  # Validate format-specific requirements
  format <- normalize_format(format)
  if (format %in% c("t20", "odi")) {
    max_overs <- get_max_overs(format)
    if (any(df$over > max_overs, na.rm = TRUE)) {
      cli::cli_warn(c(
        "!" = "Over values exceed format maximum ({max_overs} overs for {format})",
        "i" = "Max over in data: {max(df$over, na.rm = TRUE)}"
      ))
    }
  }

  invisible(TRUE)
}


#' Validate Full Model Features
#'
#' Checks that a data frame has the required columns for the full outcome model.
#' The full model uses context features plus player/team/venue skill indices.
#'
#' @param df Data frame to validate.
#' @param format Character. Cricket format: "t20", "odi", or "test".
#' @param strict Logical. If TRUE, errors on missing skill columns.
#'   If FALSE (default), warns but allows processing with defaults.
#'
#' @return Invisibly returns TRUE if validation passes.
#'
#' @details
#' Required columns for full model:
#' - Core context: over, ball, innings, wickets_fallen
#' - Player skills: batter_scoring_index, batter_survival_rate,
#'   bowler_economy_index, bowler_strike_rate
#' - Team skills: batting_team_runs_skill, batting_team_wicket_skill,
#'   bowling_team_runs_skill, bowling_team_wicket_skill
#' - Venue skills: venue_run_rate, venue_wicket_rate
#'
#' @export
validate_full_features <- function(df, format = "t20", strict = FALSE) {
  # First validate core delivery data
  validate_agnostic_features(df, format)

  # Skill columns expected for full model
  player_skill_cols <- c(
    "batter_scoring_index", "batter_survival_rate",
    "bowler_economy_index", "bowler_strike_rate"
  )

  team_skill_cols <- c(
    "batting_team_runs_skill", "batting_team_wicket_skill",
    "bowling_team_runs_skill", "bowling_team_wicket_skill"
  )

  venue_skill_cols <- c(
    "venue_run_rate", "venue_wicket_rate"
  )

  all_skill_cols <- c(player_skill_cols, team_skill_cols, venue_skill_cols)
  missing_skills <- setdiff(all_skill_cols, names(df))

  if (length(missing_skills) > 0) {
    if (strict) {
      cli::cli_abort(c(
        "Missing required skill columns for full model",
        "x" = "Missing: {.field {missing_skills}}",
        "i" = "Set strict = FALSE to use default values for missing skills"
      ), call = NULL)
    } else {
      cli::cli_warn(c(
        "!" = "Missing skill columns for full model",
        "i" = "Missing: {.field {missing_skills}}",
        "i" = "These will be filled with neutral/starting values"
      ))
    }
  }

  invisible(TRUE)
}


# ============================================================================
# DATA QUALITY VALIDATION
# ============================================================================

#' Validate Data Quality
#'
#' Checks for common data quality issues in delivery data.
#'
#' @param df Data frame to validate.
#' @param check_na Logical. Check for NA values in key columns.
#' @param check_ranges Logical. Check that values are in valid ranges.
#'
#' @return Named list with validation results:
#'   - valid: Logical, TRUE if all checks pass
#'   - issues: Character vector of issues found
#'   - stats: Named list of summary statistics
#'
#' @export
validate_data_quality <- function(df, check_na = TRUE, check_ranges = TRUE) {
  issues <- character(0)
  stats <- list()

  # Row count
  stats$n_rows <- nrow(df)
  if (stats$n_rows == 0) {
    issues <- c(issues, "Data frame has zero rows")
    return(list(valid = FALSE, issues = issues, stats = stats))
  }

  # Check for NA values in key columns
  if (check_na) {
    key_cols <- intersect(c("over", "ball", "innings", "batter_id", "bowler_id"), names(df))
    for (col in key_cols) {
      na_count <- sum(is.na(df[[col]]))
      if (na_count > 0) {
        pct <- round(100 * na_count / nrow(df), 1)
        issues <- c(issues, sprintf("%s has %d NA values (%.1f%%)", col, na_count, pct))
        stats[[paste0("na_", col)]] <- na_count
      }
    }
  }

  # Check value ranges
  if (check_ranges) {
    if ("over" %in% names(df)) {
      if (any(df$over < 0, na.rm = TRUE)) {
        issues <- c(issues, "Negative over values found")
      }
    }

    if ("ball" %in% names(df)) {
      if (any(df$ball < 1 | df$ball > 6, na.rm = TRUE)) {
        invalid_balls <- sum(df$ball < 1 | df$ball > 6, na.rm = TRUE)
        issues <- c(issues, sprintf("Ball values outside 1-6 range: %d rows", invalid_balls))
      }
    }

    if ("wickets_fallen" %in% names(df)) {
      if (any(df$wickets_fallen < 0 | df$wickets_fallen > 10, na.rm = TRUE)) {
        issues <- c(issues, "Wickets fallen outside 0-10 range")
      }
    }

    if ("innings" %in% names(df)) {
      valid_innings <- c(1, 2, 3, 4)  # Test matches can have 4 innings
      if (any(!df$innings %in% valid_innings, na.rm = TRUE)) {
        issues <- c(issues, "Invalid innings values (expected 1-4)")
      }
    }
  }

  list(
    valid = length(issues) == 0,
    issues = issues,
    stats = stats
  )
}


#' Check Required Columns Exist
#'
#' Simple utility to check if required columns exist in a data frame.
#' Returns a logical rather than stopping with an error.
#'
#' @param df Data frame to check.
#' @param required_cols Character vector of required column names.
#'
#' @return Logical. TRUE if all required columns exist, FALSE otherwise.
#'
#' @examples
#' df <- data.frame(a = 1, b = 2)
#' has_required_columns(df, c("a", "b"))  # TRUE
#' has_required_columns(df, c("a", "c"))  # FALSE
#'
#' @export
has_required_columns <- function(df, required_cols) {
  all(required_cols %in% names(df))
}
