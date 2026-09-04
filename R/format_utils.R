# Format Utility Functions
#
# Centralized format normalization and lookup functions used across the package.
# Consolidates FORMAT_GROUPS and normalize_format() that were repeated across files.

# ============================================================================
# FORMAT GROUPS
# ============================================================================

#' Get Format Groups Mapping
#'
#' Returns a list mapping canonical format names to their database match types.
#' This consolidates the FORMAT_GROUPS definition that was repeated in 16+ scripts.
#'
#' @return Named list. Keys are canonical formats ("t20", "odi", "test"),
#'   values are character vectors of match types.
#'
#' @examples
#' get_format_groups()
#' # Returns: list(t20 = c("T20", "IT20"), odi = c("ODI", "ODM"), test = c("Test", "MDM"))
#'
#' get_format_groups()$t20
#' # Returns: c("T20", "IT20")
#'
#' @export
get_format_groups <- function() {
  list(
    t20 = c("T20", "IT20"),
    odi = c("ODI", "ODM"),
    test = c("Test", "MDM")
  )
}


#' Get Gender Categories Mapping
#'
#' Returns a list mapping gender labels to database values.
#'
#' @return Named list. Keys are labels ("mens", "womens"),
#'   values are database values ("male", "female").
#'
#' @examples
#' get_gender_categories()
#' # Returns: list(mens = "male", womens = "female")
#'
#' @export
get_gender_categories <- function() {
  list(
    mens = "male",
    womens = "female"
  )
}


# ============================================================================
# FORMAT NORMALIZATION
# ============================================================================

#' Normalize Cricket Format Name
#'
#' Normalizes various format names to canonical form (t20, odi, test).
#' Handles case insensitivity, whitespace, and common aliases.
#'
#' This is the canonical function for format normalization across the package.
#' See also: normalize_match_type() which is a deprecated alias.
#'
#' @param format Character. Format name to normalize
#'
#' @return Character. Canonical format: "t20", "odi", or "test"
#'
#' @examples
#' normalize_format("T20")    # "t20"
#' normalize_format("IT20")   # "t20"
#' normalize_format("ODI")    # "odi"
#' normalize_format("Test")   # "test"
#' normalize_format("MDM")    # "test" (multi-day matches)
#'
#' @export
normalize_format <- function(format) {
  format_lower <- tolower(trimws(format))

  # T20 variants (international + domestic)
  if (format_lower %in% c("t20", "t20i", "it20", "t20s", "twenty20",
                            "bbl", "wbbl", "ipl", "psl", "cpl", "sa20",
                            "bpl", "lpl", "mpl", "ilt20")) {
    return("t20")
  }

  # ODI variants
  if (format_lower %in% c("odi", "odis", "odm")) {
    return("odi")
  }

  # Test/multi-day variants
  if (format_lower %in% c("test", "tests", "mdm", "fc", "first-class")) {
    return("test")
  }

  cli::cli_abort(c(
    "Unknown cricket format: {.val {format}}",
    "i" = "Expected one of: t20, odi, test (or recognized aliases like IT20, ODM, MDM)"
  ), call = NULL)
}


#' Normalize Match Type (Deprecated)
#'
#' @description
#' `r lifecycle::badge("deprecated")`
#'
#' This function is deprecated. Use [normalize_format()] instead.
#'
#' @param match_type Character. Match type to normalize
#'
#' @return Character. Canonical format.
#'
#' @keywords internal
normalize_match_type <- function(match_type) {
  .Deprecated("normalize_format")
  normalize_format(match_type)
}


#' Get Match Types for Format
#'
#' Returns the database match type values for a given canonical format.
#'
#' @param format Character. Canonical format ("t20", "odi", "test") or
#'   match type that will be normalized first.
#'
#' @return Character vector. Database match types for the format.
#'
#' @examples
#' get_match_types_for_format("t20")   # c("T20", "IT20")
#' get_match_types_for_format("Test")  # c("Test", "MDM")
#'
#' @export
get_match_types_for_format <- function(format) {
  canonical <- normalize_format(format)
  get_format_groups()[[canonical]]
}


#' Build SQL Match Type Filter
#'
#' Generates SQL WHERE clause fragment for filtering by format.
#'
#' @param format Character. Canonical format or match type.
#' @param column Character. Column name to filter on. Default "match_type".
#'
#' @return Character. SQL fragment like "match_type IN ('T20', 'IT20')"
#'
#' @examples
#' \dontrun{
#' build_match_type_sql("t20")
#' # Returns: "match_type IN ('T20', 'IT20')"
#'
#' build_match_type_sql("test", "m.match_type")
#' # Returns: "m.match_type IN ('Test', 'MDM')"
#' }
#'
#' @keywords internal
build_match_type_sql <- function(format, column = "match_type") {
  match_types <- get_match_types_for_format(format)
  types_sql <- paste0("'", match_types, "'", collapse = ", ")
  sprintf("%s IN (%s)", column, types_sql)
}


# ============================================================================
# FORMAT-SPECIFIC LOOKUPS
# ============================================================================
# Named vector lookups replace repetitive switch statements

# Maximum balls per innings by format
.MAX_BALLS <- c(t20 = 120L, odi = 300L, test = 540L)

# Maximum overs per innings by format (Test is NULL for unlimited)
.MAX_OVERS <- c(t20 = 20L, odi = 50L, test = NA_integer_)


#' Get Maximum Balls for Format
#'
#' Returns the maximum number of balls in an innings for a given format.
#'
#' @param format Character. Cricket format ("t20", "odi", "test").
#'
#' @return Integer. Maximum balls: 120 (T20), 300 (ODI), 540 (Test).
#'
#' @examples
#' get_max_balls("t20")   # 120
#' get_max_balls("odi")   # 300
#' get_max_balls("test")  # 540
#'
#' @export
get_max_balls <- function(format) {
  canonical <- normalize_format(format)
  .MAX_BALLS[[canonical]]
}


#' Get Maximum Overs for Format
#'
#' Returns the maximum number of overs in an innings for a given format.
#'
#' @param format Character. Cricket format ("t20", "odi", "test").
#'
#' @return Integer or NULL. Maximum overs: 20 (T20), 50 (ODI), NULL (Test unlimited).
#'
#' @examples
#' get_max_overs("t20")   # 20
#' get_max_overs("odi")   # 50
#' get_max_overs("test")  # NULL
#'
#' @export
get_max_overs <- function(format) {
  canonical <- normalize_format(format)
  result <- .MAX_OVERS[[canonical]]
  if (is.na(result)) NULL else result
}


# ============================================================================
# OVER-BALL POSITION
# ============================================================================

#' Compute the `over_ball` Position Feature
#'
#' The single definition of `over_ball`, the within-innings position feature
#' consumed by every XGBoost model in the package. Use this everywhere the
#' value is computed: the Cricsheet parser that writes
#' `cricsheet.deliveries.over_ball`, and every prediction path that has to
#' reconstruct the column when it is absent from the input frame.
#'
#' @section Why this function exists:
#' Until this was centralised, `over_ball` had two incompatible definitions in
#' the codebase. The parser wrote `over + ball / 10` (and all 10,895,339 stored
#' rows use it, so every model was trained on that scale), while the prediction
#' and simulation paths reconstructed it as `over + ball / 6`. Over 10 ball 3
#' was therefore `10.3` at training time and `10.5` at scoring time, and the
#' derived `overs_left = max_overs - over_ball` inherited the error. Nothing
#' raised an error; predictions were simply scored on a feature scale the model
#' had never seen. One definition, called from every site, is what prevents
#' that recurring.
#'
#' @section D-P5, fixed 2026-09-04 (do not reintroduce):
#' `ball` -- the stored `cricsheet.deliveries.ball` column, and the raw
#' delivery-within-over position this function used to be called with --
#' counts every delivery in the over, extras included, so it reaches 19 in
#' the stored data (233,975 deliveries have `ball > 6`). With a `/ 10`
#' denominator, an over needing 10 or more deliveries spilled into the next
#' over's numeric range: over 5 ball 12 gave `6.2`, indistinguishable from
#' over 6 ball 2 (2,637 stored deliveries collided this way).
#'
#' This function's contract changed to fix it: `ball` must now be the LEGAL
#' ball count within the over (1-6, cricket broadcast notation), not the raw
#' delivery position. A wide or no-ball does not advance it -- it repeats
#' the count from before it, so `over_ball` never exceeds `over + 0.6`. The
#' parser (`cricsheet_parser.R`) tracks this as `legal_ball_num`, reset each
#' over, incrementing only on a legal delivery. `cricsheet.deliveries.ball`
#' itself stays the raw, extras-inclusive count unchanged -- other code (the
#' free-hit derivation) relies on that specifically to reconstruct true
#' bowling order via `ORDER BY (match_id, innings, over, ball)`. Do not pass
#' the raw `ball` column to this function; every ball-outcome model (agnostic
#' + full, t20/odi/test) was retrained on the corrected feature.
#'
#' @param over Integer vector. Completed overs before this delivery (0-based).
#' @param ball Integer vector. LEGAL delivery number within the over,
#'   1-based, NOT counting wides/no-balls (max 6).
#'
#' @return Numeric vector of the same length as the recycled inputs.
#'
#' Worked values: over 10 ball 3 gives `10.3`; over 0 ball 1 gives `0.1`; an
#' over with a wide before its 5th legal ball still gives `over + 0.5` for
#' that 5th ball, not `over + 0.6` or higher.
#'
#' @keywords internal
calculate_over_ball <- function(over, ball) {
  as.numeric(over) + as.numeric(ball) / 10
}

