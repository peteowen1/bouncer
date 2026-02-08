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

  # T20 variants
  if (format_lower %in% c("t20", "t20i", "it20", "t20s", "twenty20")) {
    return("t20")
  }

  # ODI variants
  if (format_lower %in% c("odi", "odis", "odm")) {
    return("odi")
  }

  # Test variants
  if (format_lower %in% c("test", "tests", "mdm")) {
    return("test")
  }

  # Default to T20 for domestic leagues and unknown formats
  cli::cli_warn("Unknown format {.val {format}}, defaulting to T20")
  "t20"
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
#' build_match_type_sql("t20")
#' # Returns: "match_type IN ('T20', 'IT20')"
#'
#' build_match_type_sql("test", "m.match_type")
#' # Returns: "m.match_type IN ('Test', 'MDM')"
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

