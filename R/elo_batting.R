# Batting ELO Rating Functions

#' Update Batting ELO from Delivery
#'
#' Updates a batter's ELO rating based on a single delivery outcome.
#'
#' @param current_batting_elo Numeric. Batter's current ELO rating
#' @param bowling_elo Numeric. Bowler's current ELO rating
#' @param runs_batter Integer. Runs scored by batter
#' @param is_wicket Logical. Whether batter was dismissed
#' @param is_boundary Logical. Whether it was a boundary
#' @param match_type Character. Match type for K-factor calculation
#' @param player_matches Numeric. Number of matches batter has played
#'
#' @return Numeric. Updated batting ELO rating
#' @export
#'
#' @examples
#' # Batter scored 4 runs
#' update_batting_elo(1550, 1520, runs_batter = 4, is_wicket = FALSE,
#'                     is_boundary = TRUE, match_type = "t20")
#'
#' # Batter got out for 0
#' update_batting_elo(1550, 1520, runs_batter = 0, is_wicket = TRUE,
#'                     is_boundary = FALSE, match_type = "t20")
update_batting_elo <- function(current_batting_elo,
                                bowling_elo,
                                runs_batter,
                                is_wicket,
                                is_boundary = FALSE,
                                match_type = "t20",
                                player_matches = 0) {

  # Calculate expected outcome
  expected <- calculate_expected_outcome(current_batting_elo, bowling_elo)

  # Calculate actual outcome
  actual <- calculate_delivery_outcome_score(runs_batter, is_wicket, is_boundary)

  # Get K-factor
  k_factor <- calculate_k_factor(match_type, player_matches)

  # Calculate new ELO
  new_elo <- calculate_elo_update(current_batting_elo, expected, actual, k_factor)

  return(new_elo)
}


#' Get Player Batting ELO
#'
#' Retrieves the current batting ELO rating for a player.
#'
#' @param player_id Character. Player identifier
#' @param match_type Character. Match type ("all", "test", "odi", "t20").
#'   Default "all" returns overall rating.
#' @param as_of_date Date. Get ELO as of specific date. If NULL, returns latest.
#' @param db_path Character. Database path. If NULL, uses default.
#'
#' @return Numeric. Player's batting ELO rating (or NA if not found)
#' @export
#'
#' @examples
#' \dontrun{
#' # Get overall batting ELO
#' kohli_elo <- get_batting_elo("V Kohli")
#'
#' # Get T20-specific batting ELO
#' kohli_t20_elo <- get_batting_elo("V Kohli", match_type = "t20")
#'
#' # Get historical ELO
#' kohli_2020 <- get_batting_elo("V Kohli", as_of_date = as.Date("2020-12-31"))
#' }
get_batting_elo <- function(player_id,
                             match_type = "all",
                             as_of_date = NULL,
                             db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Build query based on parameters
  if (match_type == "all") {
    elo_column <- "elo_batting"
  } else {
    match_type <- normalize_match_type(match_type)
    elo_column <- paste0("elo_batting_", match_type)
  }

  # Build query
  if (is.null(as_of_date)) {
    # Get latest ELO
    query <- sprintf("
      SELECT %s
      FROM player_elo_history
      WHERE player_id = ?
      ORDER BY match_date DESC
      LIMIT 1
    ", elo_column)

    result <- DBI::dbGetQuery(conn, query, params = list(player_id))
  } else {
    # Get ELO as of date
    query <- sprintf("
      SELECT %s
      FROM player_elo_history
      WHERE player_id = ?
        AND match_date <= ?
      ORDER BY match_date DESC
      LIMIT 1
    ", elo_column)

    result <- DBI::dbGetQuery(conn, query, params = list(player_id, as_of_date))
  }

  if (nrow(result) == 0) {
    # Player not found, return starting ELO
    return(initialize_player_elo("batting"))
  }

  return(result[[1]])
}


#' Get Player Batting ELO History
#'
#' Retrieves the complete batting ELO rating history for a player.
#'
#' @param player_id Character. Player identifier
#' @param match_type Character. Match type filter. Default "all".
#' @param start_date Date. Start date for history. If NULL, returns all.
#' @param end_date Date. End date for history. If NULL, returns up to latest.
#' @param db_path Character. Database path. If NULL, uses default.
#'
#' @return Data frame with columns: match_id, match_date, match_type, elo_batting
#' @export
#'
#' @examples
#' \dontrun{
#' # Get full batting ELO history
#' kohli_history <- get_batting_elo_history("V Kohli")
#'
#' # Get T20 history for specific period
#' kohli_t20_2023 <- get_batting_elo_history(
#'   "V Kohli",
#'   match_type = "t20",
#'   start_date = as.Date("2023-01-01"),
#'   end_date = as.Date("2023-12-31")
#' )
#' }
get_batting_elo_history <- function(player_id,
                                     match_type = "all",
                                     start_date = NULL,
                                     end_date = NULL,
                                     db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Build query
  where_clauses <- c("player_id = ?")
  params <- list(player_id)

  if (match_type != "all") {
    match_type <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "match_type = ?")
    params <- c(params, list(match_type))
  }

  if (!is.null(start_date)) {
    where_clauses <- c(where_clauses, "match_date >= ?")
    params <- c(params, list(start_date))
  }

  if (!is.null(end_date)) {
    where_clauses <- c(where_clauses, "match_date <= ?")
    params <- c(params, list(end_date))
  }

  where_clause <- paste(where_clauses, collapse = " AND ")

  query <- sprintf("
    SELECT
      match_id,
      match_date,
      match_type,
      elo_batting,
      elo_batting_test,
      elo_batting_odi,
      elo_batting_t20
    FROM player_elo_history
    WHERE %s
    ORDER BY match_date ASC
  ", where_clause)

  result <- DBI::dbGetQuery(conn, query, params = params)

  return(result)
}


#' Calculate Batting Outcome Score
#'
#' Alias for calculate_delivery_outcome_score for batting perspective.
#'
#' @inheritParams calculate_delivery_outcome_score
#' @return Numeric value between 0 and 1
#' @export
calculate_batting_outcome_score <- function(runs_batter, is_wicket, is_boundary = FALSE) {
  calculate_delivery_outcome_score(runs_batter, is_wicket, is_boundary)
}
