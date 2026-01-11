# Team ID Utilities
#
# Centralized functions for creating consistent team and player IDs.
# All IDs are snake_case for consistency.


#' Convert String to Snake Case
#'
#' Converts a string to snake_case: lowercase with spaces replaced by underscores.
#'
#' @param x Character vector to convert
#'
#' @return Character vector in snake_case
#' @keywords internal
to_snake_case <- function(x) {

  x <- tolower(x)
  x <- gsub("\\s+", "_", x)  # Replace whitespace with underscore

  x <- gsub("[^a-z0-9_]", "", x)
  x
}


#' Create Composite Team ID
#'
#' Creates a standardized team ID combining team name, gender, format, and team type.
#' The result is in snake_case format.
#'
#' @param team Character. Team name (e.g., "Mumbai Indians")
#' @param gender Character. Gender ("male" or "female")
#' @param format Character. Match format ("t20", "odi", "test")
#' @param team_type Character. Team type ("club" or "international")
#'
#' @return Character. Composite team ID in snake_case
#' @keywords internal
make_team_id <- function(team, gender, format, team_type) {
  paste(
    to_snake_case(team),
    to_snake_case(gender),
    to_snake_case(format),
    to_snake_case(team_type),
    sep = "_"
  )
}


#' Create Composite Team ID (Vectorized)
#'
#' Vectorized version of make_team_id for use with data.table/dplyr.
#'
#' @param team Character vector. Team names
#' @param gender Character vector. Genders
#' @param format Character vector or single value. Match formats
#' @param team_type Character vector. Team types
#'
#' @return Character vector. Composite team IDs in snake_case
#' @keywords internal
make_team_id_vec <- function(team, gender, format, team_type) {

  mapply(make_team_id, team, gender, format, team_type, USE.NAMES = FALSE)
}


#' Parse Team ID Components
#'
#' Extracts components from a composite team ID.
#'
#' @param team_id Character. Composite team ID
#'
#' @return Named list with team, gender, format, team_type components
#' @keywords internal
parse_team_id <- function(team_id) {
  # Split from the end (team_type, format, gender are known patterns)
  parts <- strsplit(team_id, "_")[[1]]
  n <- length(parts)


if (n < 4) {
    return(list(team = team_id, gender = NA, format = NA, team_type = NA))
  }

  list(
    team = paste(parts[1:(n-3)], collapse = "_"),
    gender = parts[n-2],
    format = parts[n-1],
    team_type = parts[n]
  )
}


#' Extract Format from Team ID
#'
#' Quick extraction of format component from team ID.
#'
#' @param team_id Character. Composite team ID
#'
#' @return Character. Format component (t20, odi, test)
#' @keywords internal
get_format_from_team_id <- function(team_id) {
  parts <- strsplit(team_id, "_")[[1]]
  n <- length(parts)
  if (n >= 2) parts[n-1] else NA_character_
}


#' Filter Team IDs by Format
#'
#' Returns a logical vector indicating which team_ids match the given format.
#'
#' @param team_ids Character vector. Team IDs to filter
#' @param format Character. Format to match (e.g., "t20")
#'
#' @return Logical vector
#' @keywords internal
filter_team_ids_by_format <- function(team_ids, format) {
  pattern <- paste0("_", tolower(format), "_")
 grepl(pattern, team_ids, fixed = FALSE)
}
