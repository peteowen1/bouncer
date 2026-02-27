# Match Outcome Handling Functions
#
# Functions for handling special match outcomes in win probability modeling:
#   - DLS (Duckworth-Lewis-Stern) adjusted matches
#   - Tied matches
#   - Super over deciders

#' Identify DLS Matches
#'
#' Queries the database to identify matches affected by DLS method.
#'
#' @param conn DuckDB connection
#' @param event_filter Character. Event name filter (e.g., "IPL"). Default NULL = all.
#' @param match_type Character. Match type filter. Default "t20".
#'
#' @return Data frame with DLS match information including:
#'   \itemize{
#'     \item match_id: Match identifier
#'     \item event_name: Tournament/series name
#'     \item match_date: Date of match
#'     \item team1, team2: Team names
#'     \item outcome_method: Method description (contains D/L or DLS)
#'     \item outcome_winner: Winning team
#'     \item is_dls_match: Always 1 for these matches
#'   }
#'
#' @keywords internal
identify_dls_matches <- function(conn, event_filter = NULL, match_type = "t20") {

  where_clauses <- "WHERE LOWER(match_type) = ?"
  params <- list(match_type)

  if (!is.null(event_filter)) {
    where_clauses <- paste0(where_clauses, " AND event_name LIKE ?")
    params <- c(params, paste0("%", event_filter, "%"))
  }

  query <- sprintf("
    SELECT
      match_id,
      event_name,
      match_date,
      team1,
      team2,
      outcome_method,
      outcome_winner,
      outcome_by_runs,
      outcome_by_wickets,
      CASE
        WHEN LOWER(outcome_method) LIKE '%%d/l%%' THEN 1
        WHEN LOWER(outcome_method) LIKE '%%dls%%' THEN 1
        WHEN LOWER(outcome_method) LIKE '%%duckworth%%' THEN 1
        ELSE 0
      END as is_dls_match
    FROM cricsheet.matches
    %s
      AND (
        LOWER(outcome_method) LIKE '%%d/l%%'
        OR LOWER(outcome_method) LIKE '%%dls%%'
        OR LOWER(outcome_method) LIKE '%%duckworth%%'
      )
    ORDER BY match_date
  ", where_clauses)

  dls_matches <- DBI::dbGetQuery(conn, query, params = params)

  if (nrow(dls_matches) > 0) {
    cli::cli_alert_info("Found {nrow(dls_matches)} DLS-affected matches")
  }

  return(dls_matches)
}


#' Identify Tied Matches
#'
#' Queries the database to identify matches that ended in a tie.
#'
#' @param conn DuckDB connection
#' @param event_filter Character. Event name filter. Default NULL = all.
#' @param match_type Character. Match type filter. Default "t20".
#' @param include_super_overs Logical. If FALSE, excludes ties decided by super over.
#'
#' @return Data frame with tied match information including has_super_over flag
#'
#' @keywords internal
identify_tied_matches <- function(conn, event_filter = NULL, match_type = "t20",
                                  include_super_overs = TRUE) {

  where_clauses <- "WHERE LOWER(match_type) = ?"
  params <- list(match_type)

  if (!is.null(event_filter)) {
    where_clauses <- paste0(where_clauses, " AND event_name LIKE ?")
    params <- c(params, paste0("%", event_filter, "%"))
  }

  query <- sprintf("
    SELECT
      match_id,
      event_name,
      match_date,
      team1,
      team2,
      outcome_type,
      outcome_method,
      outcome_winner,
      CASE
        WHEN LOWER(outcome_method) LIKE '%%super over%%' THEN 1
        ELSE 0
      END as has_super_over
    FROM cricsheet.matches
    %s
      AND (
        LOWER(outcome_type) = 'tie'
        OR outcome_winner IS NULL
        OR LOWER(outcome_method) LIKE '%%super over%%'
      )
    ORDER BY match_date
  ", where_clauses)

  tied_matches <- DBI::dbGetQuery(conn, query, params = params)

  if (!include_super_overs && nrow(tied_matches) > 0) {
    tied_matches <- tied_matches[tied_matches$has_super_over == 0, ]
  }

  if (nrow(tied_matches) > 0) {
    n_super_over <- sum(tied_matches$has_super_over, na.rm = TRUE)
    n_pure_tie <- nrow(tied_matches) - n_super_over
    cli::cli_alert_info("Found {nrow(tied_matches)} tied/close matches: {n_pure_tie} pure ties, {n_super_over} super overs")
  }

  return(tied_matches)
}


#' Identify Super Over Matches
#'
#' Queries the database to identify matches decided by super over.
#'
#' @param conn DuckDB connection
#' @param event_filter Character. Event name filter. Default NULL = all.
#' @param match_type Character. Match type filter. Default "t20".
#'
#' @return Data frame with super over match information
#'
#' @keywords internal
identify_super_over_matches <- function(conn, event_filter = NULL, match_type = "t20") {

  where_clauses <- "WHERE LOWER(match_type) = ?"
  params <- list(match_type)

  if (!is.null(event_filter)) {
    where_clauses <- paste0(where_clauses, " AND event_name LIKE ?")
    params <- c(params, paste0("%", event_filter, "%"))
  }

  query <- sprintf("
    SELECT
      match_id,
      event_name,
      match_date,
      team1,
      team2,
      outcome_type,
      outcome_method,
      outcome_winner,
      outcome_by_runs,
      outcome_by_wickets
    FROM cricsheet.matches
    %s
      AND LOWER(outcome_method) LIKE '%%super over%%'
    ORDER BY match_date
  ", where_clauses)

  super_over_matches <- DBI::dbGetQuery(conn, query, params = params)

  if (nrow(super_over_matches) > 0) {
    cli::cli_alert_info("Found {nrow(super_over_matches)} super over matches")
  }

  return(super_over_matches)
}


#' Classify Match Outcomes
#'
#' Adds outcome classification columns to match data for filtering.
#' Identifies DLS matches, super overs, ties, no results, and knockout games.
#'
#' @param matches_df Data frame of matches with outcome columns (outcome_type,
#'   outcome_method, outcome_winner, event_match_number, event_group)
#'
#' @return Data frame with additional classification columns:
#'   \itemize{
#'     \item is_dls_match: TRUE if DLS method applied
#'     \item is_super_over: TRUE if decided by super over
#'     \item is_no_result: TRUE if match abandoned/no result
#'     \item is_pure_tie: TRUE if tied without super over
#'     \item is_knockout: TRUE if final/semi-final/qualifier/eliminator
#'     \item is_valid_for_training: TRUE if match has a clear winner
#'   }
#'
#' @keywords internal
classify_match_outcomes <- function(matches_df) {

  # Pre-compute lowercase versions to avoid issues with NA in tolower()
  matches_df <- matches_df %>%
    dplyr::mutate(
      outcome_method_lower = tolower(dplyr::coalesce(outcome_method, "")),
      outcome_type_lower = tolower(dplyr::coalesce(outcome_type, "")),
      event_match_number_lower = tolower(dplyr::coalesce(as.character(event_match_number), "")),
      event_group_lower = tolower(dplyr::coalesce(as.character(event_group), ""))
    ) %>%
    dplyr::mutate(
      is_dls_match = grepl("d/l|dls|duckworth", outcome_method_lower),
      is_super_over = grepl("super over", outcome_method_lower),
      is_no_result = grepl("no result|abandoned", outcome_type_lower) |
                     (outcome_type_lower == "" & is.na(outcome_winner)),
      is_pure_tie = (outcome_type_lower == "tie") & !is_super_over,
      is_knockout = grepl("final|semi.?final|qualifier|eliminator|playoff",
                          event_match_number_lower) |
                    grepl("final|semi.?final|qualifier|eliminator|playoff",
                          event_group_lower),
      is_valid_for_training = !is.na(outcome_winner) & outcome_winner != ""
    ) %>%
    dplyr::select(-outcome_method_lower, -outcome_type_lower,
                  -event_match_number_lower, -event_group_lower)

  return(matches_df)
}


#' Prepare Win/Loss Labels for Training
#'
#' Creates binary win/loss labels for the team batting second.
#' Handles special cases (ties, no results) by setting label to NA.
#'
#' @param deliveries_df Data frame of deliveries with match outcome info
#' @param target_column Character. Name of column containing batting team name.
#'   Default "batting_team".
#'
#' @return Data frame with batting_team_wins column (1 = batting team won,
#'   0 = batting team lost, NA = excluded match)
#'
#' @keywords internal
prepare_win_labels <- function(deliveries_df, target_column = "batting_team") {

  deliveries_df <- deliveries_df %>%
    dplyr::mutate(
      batting_team_wins = dplyr::case_when(
        is.na(outcome_winner) | outcome_winner == "" ~ NA_real_,
        dplyr::coalesce(is_no_result, FALSE) | dplyr::coalesce(is_pure_tie, FALSE) ~ NA_real_,
        outcome_winner == .data[[target_column]] ~ 1,
        TRUE ~ 0
      )
    )

  return(deliveries_df)
}


#' Filter Data for Training
#'
#' Applies standard filters to exclude matches that shouldn't be in training.
#'
#' @param data Data frame with outcome classification columns (from classify_match_outcomes)
#' @param exclude_ties Logical. Exclude pure ties. Default TRUE.
#' @param exclude_no_results Logical. Exclude no results. Default TRUE.
#' @param include_dls Logical. Include DLS matches. Default TRUE.
#' @param include_super_overs Logical. Include super over matches. Default TRUE.
#'
#' @return Filtered data frame
#'
#' @keywords internal
filter_for_training <- function(data, exclude_ties = TRUE, exclude_no_results = TRUE,
                                include_dls = TRUE, include_super_overs = TRUE) {

  filtered <- data

  if (exclude_ties) {
    n_before <- nrow(filtered)
    filtered <- filtered %>% dplyr::filter(!is_pure_tie | is.na(is_pure_tie))
    n_after <- nrow(filtered)
    if (n_before > n_after) {
      cli::cli_alert_info("Excluded {n_before - n_after} rows from pure ties")
    }
  }

  if (exclude_no_results) {
    n_before <- nrow(filtered)
    filtered <- filtered %>% dplyr::filter(!is_no_result | is.na(is_no_result))
    n_after <- nrow(filtered)
    if (n_before > n_after) {
      cli::cli_alert_info("Excluded {n_before - n_after} rows from no-result matches")
    }
  }

  if (!include_dls) {
    n_before <- nrow(filtered)
    filtered <- filtered %>% dplyr::filter(!is_dls_match | is.na(is_dls_match))
    n_after <- nrow(filtered)
    if (n_before > n_after) {
      cli::cli_alert_info("Excluded {n_before - n_after} rows from DLS matches")
    }
  }

  if (!include_super_overs) {
    n_before <- nrow(filtered)
    filtered <- filtered %>% dplyr::filter(!is_super_over | is.na(is_super_over))
    n_after <- nrow(filtered)
    if (n_before > n_after) {
      cli::cli_alert_info("Excluded {n_before - n_after} rows from super over matches")
    }
  }

  return(filtered)
}


#' Get Match Outcome Summary
#'
#' Summarizes match outcomes for a set of matches with counts and percentages.
#'
#' @param conn DuckDB connection
#' @param event_filter Character. Event name filter. Default NULL.
#' @param match_type Character. Match type filter. Default "t20".
#'
#' @return Data frame with outcome counts and percentages
#'
#' @keywords internal
get_outcome_summary <- function(conn, event_filter = NULL, match_type = "t20") {

  where_clauses <- "WHERE LOWER(match_type) = ?"
  params <- list(match_type)

  if (!is.null(event_filter)) {
    where_clauses <- paste0(where_clauses, " AND event_name LIKE ?")
    params <- c(params, paste0("%", event_filter, "%"))
  }

  query <- sprintf("
    SELECT
      COUNT(*) as total_matches,
      SUM(CASE WHEN outcome_winner IS NOT NULL AND LOWER(outcome_type) != 'tie' THEN 1 ELSE 0 END) as normal_results,
      SUM(CASE WHEN LOWER(outcome_method) LIKE '%%d/l%%' OR LOWER(outcome_method) LIKE '%%dls%%' THEN 1 ELSE 0 END) as dls_matches,
      SUM(CASE WHEN LOWER(outcome_method) LIKE '%%super over%%' THEN 1 ELSE 0 END) as super_over_matches,
      SUM(CASE WHEN LOWER(outcome_type) = 'tie' AND LOWER(outcome_method) NOT LIKE '%%super over%%' THEN 1 ELSE 0 END) as pure_ties,
      SUM(CASE WHEN LOWER(outcome_type) = 'no result' OR outcome_winner IS NULL THEN 1 ELSE 0 END) as no_results
    FROM cricsheet.matches
    %s
  ", where_clauses)

  summary <- DBI::dbGetQuery(conn, query, params = params)

  total <- summary$total_matches
  summary <- summary %>%
    dplyr::mutate(
      pct_normal = round(100 * normal_results / total, 1),
      pct_dls = round(100 * dls_matches / total, 1),
      pct_super_over = round(100 * super_over_matches / total, 1),
      pct_ties = round(100 * pure_ties / total, 1),
      pct_no_result = round(100 * no_results / total, 1)
    )

  cli::cli_h3("Match Outcome Summary")
  cli::cli_alert_info("Total matches: {summary$total_matches}")
  cli::cli_alert_info("Normal results: {summary$normal_results} ({summary$pct_normal}%)")
  cli::cli_alert_info("DLS matches: {summary$dls_matches} ({summary$pct_dls}%)")
  cli::cli_alert_info("Super overs: {summary$super_over_matches} ({summary$pct_super_over}%)")
  cli::cli_alert_info("Pure ties: {summary$pure_ties} ({summary$pct_ties}%)")
  cli::cli_alert_info("No results: {summary$no_results} ({summary$pct_no_result}%)")

  return(summary)
}
