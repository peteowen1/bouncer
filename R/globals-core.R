# Core Global Variable Declarations
#
# This file contains:
# - Package imports
# - Pipe operator export
# - Null-coalescing operator
# - Core data variables (matches, deliveries, players)

#' @importFrom stats predict setNames runif
#' @importFrom utils head tail
#' @importFrom dplyr %>%
#' @importFrom rlang .data
#' @importFrom slider slide_dbl
NULL

# Null coalescing operator (like %||% in purrr)
# Returns y if x is NULL, otherwise returns x
`%||%` <- function(x, y) if (is.null(x)) y else x

#' Pipe operator
#'
#' See \code{dplyr::\link[dplyr:reexports]{\%>\%}} for details.
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom dplyr %>%
#' @usage lhs \%>\% rhs
#' @param lhs A value or the dplyr/magrittr placeholder.
#' @param rhs A function call using the dplyr/magrittr semantics.
#' @return The result of calling \code{rhs(lhs)}.
NULL

# ============================================================================
# Core Data Variables
# ============================================================================
# Variables used throughout the package for deliveries, matches, and players

utils::globalVariables(c(
  # Tables
  "deliveries",
  "matches",
  # Match identifiers
  "match_id",
  "match_type",
  "match_date",
  "season",
  # Player identifiers
  "player_id",
  "batter_id",
  "bowler_id",
  # Teams
  "team1",
  "team2",
  "batting_team",
  "bowling_team",
  # Venue and context
  "venue",
  "innings",
  "over",
  "ball",
  "delivery_id",
  # Runs and wickets
  "runs_batter",
  "runs_total",
  "total_runs",
  "is_wicket",
  "wicket_kind",
  "wickets_fallen",
  # Match outcome
  "outcome_winner",
  "outcome_method",
  "outcome_type",
  # Extras
  "is_extra",
  "is_wide",
  "is_noball",
  # Count helper
  "n",
  # data.table symbols
  ".",
  ":="
))

# ============================================================================
# Utility Functions
# ============================================================================

#' Check for Parallel Support
#'
#' Checks if the required packages for parallel file parsing are available.
#'
#' @return Logical. TRUE if future and furrr are available.
#' @keywords internal
has_parallel_support <- function() {
  requireNamespace("future", quietly = TRUE) &&
    requireNamespace("furrr", quietly = TRUE)
}
