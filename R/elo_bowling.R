# Bowling ELO Rating Functions

#' Update Bowling ELO from Delivery
#'
#' Updates a bowler's ELO rating based on a single delivery outcome.
#'
#' @param current_bowling_elo Numeric. Bowler's current ELO rating
#' @param batting_elo Numeric. Batter's current ELO rating
#' @param runs_batter Integer. Runs scored by batter
#' @param is_wicket Logical. Whether wicket was taken
#' @param is_boundary Logical. Whether it was a boundary
#' @param match_type Character. Match type for K-factor calculation
#' @param player_matches Numeric. Number of matches bowler has played
#'
#' @return Numeric. Updated bowling ELO rating
#' @export
#'
#' @examples
#' # Bowler took a wicket
#' update_bowling_elo(1520, 1550, runs_batter = 0, is_wicket = TRUE,
#'                     is_boundary = FALSE, match_type = "t20")
#'
#' # Bowler conceded 6 runs
#' update_bowling_elo(1520, 1550, runs_batter = 6, is_wicket = FALSE,
#'                     is_boundary = TRUE, match_type = "t20")
update_bowling_elo <- function(current_bowling_elo,
                                batting_elo,
                                runs_batter,
                                is_wicket,
                                is_boundary = FALSE,
                                match_type = "t20",
                                player_matches = 0) {

  # Calculate expected outcome (from bowler's perspective)
  # Note: bowler's expected is inverse of batter's expected
  expected_batter <- calculate_expected_outcome(batting_elo, current_bowling_elo)
  expected_bowler <- 1 - expected_batter

  # Calculate actual outcome (from batter's perspective)
  actual_batter <- calculate_delivery_outcome_score(runs_batter, is_wicket, is_boundary)
  # Bowler's actual is inverse
  actual_bowler <- 1 - actual_batter

  # Get K-factor
  k_factor <- calculate_k_factor(match_type, player_matches)

  # Calculate new ELO
  new_elo <- calculate_elo_update(current_bowling_elo, expected_bowler, actual_bowler, k_factor)

  return(new_elo)
}


#' Get Player Bowling ELO
#'
#' Retrieves the current bowling ELO rating for a player.
#'
#' @param player_id Character. Player identifier
#' @param match_type Character. Match type ("all", "test", "odi", "t20").
#'   Default "all" returns overall rating.
#' @param as_of_date Date. Get ELO as of specific date. If NULL, returns latest.
#' @param db_path Character. Database path. If NULL, uses default.
#'
#' @return Numeric. Player's bowling ELO rating (or NA if not found)
#' @export
#'
#' @examples
#' \dontrun{
#' # Get overall bowling ELO
#' bumrah_elo <- get_bowling_elo("J Bumrah")
#'
#' # Get T20-specific bowling ELO
#' bumrah_t20_elo <- get_bowling_elo("J Bumrah", match_type = "t20")
#'
#' # Get historical ELO
#' bumrah_2020 <- get_bowling_elo("J Bumrah", as_of_date = as.Date("2020-12-31"))
#' }
get_bowling_elo <- function(player_id,
                             match_type = "all",
                             as_of_date = NULL,
                             db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Build query based on parameters
  if (match_type == "all") {
    elo_column <- "elo_bowling"
  } else {
    match_type <- normalize_match_type(match_type)
    elo_column <- paste0("elo_bowling_", match_type)
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
    return(initialize_player_elo("bowling"))
  }

  return(result[[1]])
}


#' Get Player Bowling ELO History
#'
#' Retrieves the complete bowling ELO rating history for a player.
#'
#' @param player_id Character. Player identifier
#' @param match_type Character. Match type filter. Default "all".
#' @param start_date Date. Start date for history. If NULL, returns all.
#' @param end_date Date. End date for history. If NULL, returns up to latest.
#' @param db_path Character. Database path. If NULL, uses default.
#'
#' @return Data frame with columns: match_id, match_date, match_type, elo_bowling
#' @export
#'
#' @examples
#' \dontrun{
#' # Get full bowling ELO history
#' bumrah_history <- get_bowling_elo_history("J Bumrah")
#'
#' # Get T20 history for specific period
#' bumrah_t20_2023 <- get_bowling_elo_history(
#'   "J Bumrah",
#'   match_type = "t20",
#'   start_date = as.Date("2023-01-01"),
#'   end_date = as.Date("2023-12-31")
#' )
#' }
get_bowling_elo_history <- function(player_id,
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
      elo_bowling,
      elo_bowling_test,
      elo_bowling_odi,
      elo_bowling_t20
    FROM player_elo_history
    WHERE %s
    ORDER BY match_date ASC
  ", where_clause)

  result <- DBI::dbGetQuery(conn, query, params = params)

  return(result)
}


#' Calculate Bowling Outcome Score
#'
#' Converts a delivery outcome to a score from the bowler's perspective (0-1).
#'
#' @param runs_batter Integer. Runs scored by batter
#' @param is_wicket Logical. Whether wicket was taken
#' @param is_boundary Logical. Whether it was a boundary
#'
#' @return Numeric value between 0 and 1 (bowler's perspective)
#' @export
#'
#' @examples
#' # Wicket - best outcome for bowler
#' calculate_bowling_outcome_score(0, is_wicket = TRUE, is_boundary = FALSE)
#' # Returns 1.0
#'
#' # Six runs conceded - worst outcome
#' calculate_bowling_outcome_score(6, is_wicket = FALSE, is_boundary = TRUE)
#' # Returns 0.0
calculate_bowling_outcome_score <- function(runs_batter, is_wicket, is_boundary = FALSE) {
  # Get batter's score and invert it for bowler
  batter_score <- calculate_delivery_outcome_score(runs_batter, is_wicket, is_boundary)
  bowler_score <- 1 - batter_score
  return(bowler_score)
}


#' Get Player ELO
#'
#' Convenience function to get both batting and bowling ELO for a player.
#'
#' @param player_id Character. Player identifier
#' @param match_type Character. Match type. Default "all".
#' @param as_of_date Date. Get ELO as of specific date. If NULL, returns latest.
#' @param db_path Character. Database path. If NULL, uses default.
#'
#' @return Named list with batting_elo and bowling_elo
#' @export
#'
#' @examples
#' \dontrun{
#' # Get both batting and bowling ELO
#' kohli_elos <- get_player_elo("V Kohli", match_type = "t20")
#' print(kohli_elos$batting_elo)
#' print(kohli_elos$bowling_elo)
#' }
get_player_elo <- function(player_id,
                            match_type = "all",
                            as_of_date = NULL,
                            db_path = NULL) {

  batting_elo <- get_batting_elo(player_id, match_type, as_of_date, db_path)
  bowling_elo <- get_bowling_elo(player_id, match_type, as_of_date, db_path)

  list(
    player_id = player_id,
    batting_elo = batting_elo,
    bowling_elo = bowling_elo,
    match_type = match_type,
    as_of_date = as_of_date
  )
}
