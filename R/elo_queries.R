# ELO Query Functions

#' Get Player ELO at Specific Delivery
#'
#' Retrieves a player's ELO rating at a specific delivery in a match.
#' Useful for analyzing how a player's rating evolved during specific moments.
#'
#' @param delivery_id Numeric. The unique delivery identifier.
#' @param player_id Character. Player identifier.
#' @param when Character. "before" or "after" the delivery. Default "after".
#' @param rating_type Character. "batting" or "bowling". Default "batting".
#' @param db_path Character. Optional path to the database.
#'
#' @return Numeric. ELO rating at that delivery (or NA if not found).
#'
#' @examples
#' \dontrun{
#' # Get Virat Kohli's batting ELO after a specific delivery
#' get_elo_at_delivery(
#'   delivery_id = 123456,
#'   player_id = "virat_kohli",
#'   when = "after",
#'   rating_type = "batting"
#' )
#' }
#'
#' @export
get_elo_at_delivery <- function(delivery_id,
                                 player_id,
                                 when = "after",
                                 rating_type = "batting",
                                 db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Determine which column to query
  if (rating_type == "batting") {
    player_col <- "batter_id"
    elo_col <- if (when == "before") "batter_elo_before" else "batter_elo_after"
  } else {
    player_col <- "bowler_id"
    elo_col <- if (when == "before") "bowler_elo_before" else "bowler_elo_after"
  }

  query <- sprintf("
    SELECT %s as elo
    FROM deliveries
    WHERE delivery_id = ? AND %s = ?
  ", elo_col, player_col)

  result <- DBI::dbGetQuery(conn, query, params = list(delivery_id, player_id))

  if (nrow(result) == 0) {
    return(NA_real_)
  }

  return(result$elo[1])
}


#' Get ELO Progression During Match
#'
#' Retrieves a player's ELO progression throughout a specific match,
#' showing how their rating changed delivery-by-delivery.
#'
#' @param match_id Character. Match identifier.
#' @param player_id Character. Player identifier.
#' @param rating_type Character. "batting" or "bowling". Default "batting".
#' @param db_path Character. Optional path to the database.
#'
#' @return Data frame with columns:
#' \describe{
#'   \item{delivery_id}{Unique delivery identifier}
#'   \item{innings}{Innings number}
#'   \item{over}{Over number}
#'   \item{ball}{Ball number within the over}
#'   \item{runs_batter}{Runs scored by batter}
#'   \item{is_wicket}{Whether a wicket fell}
#'   \item{elo_before}{Player's ELO before this delivery}
#'   \item{elo_after}{Player's ELO after this delivery}
#' }
#'
#' @examples
#' \dontrun{
#' # Track a player's batting ELO through a match
#' progression <- get_match_elo_progression(
#'   match_id = "1234567",
#'   player_id = "virat_kohli",
#'   rating_type = "batting"
#' )
#' plot(progression$elo_after, type = "l")
#' }
#'
#' @export
get_match_elo_progression <- function(match_id,
                                       player_id,
                                       rating_type = "batting",
                                       db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Determine columns
  if (rating_type == "batting") {
    player_col <- "batter_id"
    elo_before_col <- "batter_elo_before"
    elo_after_col <- "batter_elo_after"
  } else {
    player_col <- "bowler_id"
    elo_before_col <- "bowler_elo_before"
    elo_after_col <- "bowler_elo_after"
  }

  query <- sprintf("
    SELECT
      delivery_id,
      innings,
      over,
      ball,
      runs_batter,
      is_wicket,
      %s as elo_before,
      %s as elo_after
    FROM deliveries
    WHERE match_id = ? AND %s = ?
    ORDER BY delivery_id ASC
  ", elo_before_col, elo_after_col, player_col)

  result <- DBI::dbGetQuery(conn, query, params = list(match_id, player_id))

  return(result)
}


#' Get Delivery Details with ELOs
#'
#' Gets complete delivery information including ELO ratings for both players.
#'
#' @param delivery_id Numeric. Delivery identifier
#' @param db_path Character. Database path
#'
#' @return Data frame with delivery details and ELO ratings
#' @keywords internal
get_delivery_with_elos <- function(delivery_id, db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  query <- "
    SELECT
      delivery_id,
      match_id,
      match_date,
      match_type,
      venue,
      innings,
      over,
      ball,
      batter_id,
      bowler_id,
      runs_batter,
      is_wicket,
      is_boundary,
      batter_elo_before,
      batter_elo_after,
      bowler_elo_before,
      bowler_elo_after,
      total_runs,
      wickets_fallen
    FROM deliveries
    WHERE delivery_id = ?
  "

  result <- DBI::dbGetQuery(conn, query, params = list(delivery_id))

  return(result)
}


#' Find Deliveries by ELO Range
#'
#' Finds deliveries where a player's ELO was in a specific range.
#' Useful for analyzing performance at different skill levels.
#'
#' @param player_id Character. Player identifier.
#' @param elo_min Numeric. Minimum ELO rating.
#' @param elo_max Numeric. Maximum ELO rating.
#' @param rating_type Character. "batting" or "bowling". Default "batting".
#' @param limit Integer. Maximum results to return. Default 100.
#' @param db_path Character. Optional path to the database.
#'
#' @return Data frame with columns:
#' \describe{
#'   \item{delivery_id}{Unique delivery identifier}
#'   \item{match_id}{Match identifier}
#'   \item{match_date}{Date of the match}
#'   \item{match_type}{Format (T20, ODI, Test, etc.)}
#'   \item{innings}{Innings number}
#'   \item{over}{Over number}
#'   \item{ball}{Ball number}
#'   \item{elo}{Player's ELO at this delivery}
#' }
#'
#' @examples
#' \dontrun{
#' # Find deliveries when a player was at peak form (ELO > 1600)
#' peak_deliveries <- find_deliveries_by_elo_range(
#'   player_id = "virat_kohli",
#'   elo_min = 1600,
#'   elo_max = 2000,
#'   rating_type = "batting"
#' )
#' }
#'
#' @export
find_deliveries_by_elo_range <- function(player_id,
                                          elo_min,
                                          elo_max,
                                          rating_type = "batting",
                                          limit = 100,
                                          db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  if (rating_type == "batting") {
    player_col <- "batter_id"
    elo_col <- "batter_elo_after"
  } else {
    player_col <- "bowler_id"
    elo_col <- "bowler_elo_after"
  }

  query <- sprintf("
    SELECT
      delivery_id,
      match_id,
      match_date,
      match_type,
      innings,
      over,
      ball,
      %s as elo
    FROM deliveries
    WHERE %s = ?
      AND %s BETWEEN ? AND ?
    ORDER BY match_date DESC
    LIMIT ?
  ", elo_col, player_col, elo_col)

  result <- DBI::dbGetQuery(
    conn,
    query,
    params = list(player_id, elo_min, elo_max, limit)
  )

  return(result)
}


#' Compare Player ELOs at Same Delivery
#'
#' Compares two players' ELO ratings at a specific delivery.
#'
#' @param delivery_id Numeric. Delivery identifier
#' @param when Character. "before" or "after". Default "before".
#' @param db_path Character. Database path
#'
#' @return List with batter and bowler ELO info and matchup analysis
#' @keywords internal
compare_elos_at_delivery <- function(delivery_id,
                                      when = "before",
                                      db_path = NULL) {

  delivery <- get_delivery_with_elos(delivery_id, db_path)

  if (nrow(delivery) == 0) {
    cli::cli_abort("Delivery {delivery_id} not found")
  }

  batter_elo <- if (when == "before") {
    delivery$batter_elo_before
  } else {
    delivery$batter_elo_after
  }

  bowler_elo <- if (when == "before") {
    delivery$bowler_elo_before
  } else {
    delivery$bowler_elo_after
  }

  elo_diff <- batter_elo - bowler_elo
  advantage <- if (elo_diff > 50) {
    "batter"
  } else if (elo_diff < -50) {
    "bowler"
  } else {
    "even"
  }

  expected_outcome <- calculate_expected_outcome(batter_elo, bowler_elo)

  list(
    delivery_id = delivery_id,
    batter_id = delivery$batter_id,
    bowler_id = delivery$bowler_id,
    batter_elo = batter_elo,
    bowler_elo = bowler_elo,
    elo_difference = elo_diff,
    advantage = advantage,
    expected_batter_success = expected_outcome,
    actual_runs = delivery$runs_batter,
    was_wicket = delivery$is_wicket
  )
}


# ============================================================================
# Legacy Dual-ELO Query Functions
# ============================================================================
# These query the player_elo_history table from the original dual-ELO system.
# Moved here from player_elo_batting.R and player_elo_bowling.R.

# Internal helper for getting ELO (used by both batting and bowling)
get_elo_internal <- function(player_id, rating_type, match_type, as_of_date, db_path) {
  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Build column name
  base_col <- paste0("elo_", rating_type)
  if (match_type == "all") {
    elo_column <- base_col
  } else {
    match_type <- normalize_match_type(match_type)
    elo_column <- paste0(base_col, "_", match_type)
  }

  # Build query
  if (is.null(as_of_date)) {
    query <- sprintf("
      SELECT %s
      FROM player_elo_history
      WHERE player_id = ?
      ORDER BY match_date DESC
      LIMIT 1
    ", elo_column)
    result <- DBI::dbGetQuery(conn, query, params = list(player_id))
  } else {
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
    return(initialize_player_elo(rating_type))
  }
  return(result[[1]])
}

# Internal helper for getting ELO history
get_elo_history_internal <- function(player_id, rating_type, match_type, start_date, end_date, db_path) {
  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Build query
  where_clauses <- c("player_id = ?")
  params <- list(player_id)

  if (match_type != "all") {
    mt <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "match_type = ?")
    params <- c(params, list(mt))
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

  # Select columns based on rating type
  base_col <- paste0("elo_", rating_type)
  query <- sprintf("
    SELECT
      match_id,
      match_date,
      match_type,
      %s,
      %s_test,
      %s_odi,
      %s_t20
    FROM player_elo_history
    WHERE %s
    ORDER BY match_date ASC
  ", base_col, base_col, base_col, base_col, where_clause)

  DBI::dbGetQuery(conn, query, params = params)
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
  get_elo_internal(player_id, "batting", match_type, as_of_date, db_path)
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
#' @keywords internal
get_batting_elo_history <- function(player_id,
                                     match_type = "all",
                                     start_date = NULL,
                                     end_date = NULL,
                                     db_path = NULL) {
  get_elo_history_internal(player_id, "batting", match_type, start_date, end_date, db_path)
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
  get_elo_internal(player_id, "bowling", match_type, as_of_date, db_path)
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
#' @keywords internal
get_bowling_elo_history <- function(player_id,
                                     match_type = "all",
                                     start_date = NULL,
                                     end_date = NULL,
                                     db_path = NULL) {
  get_elo_history_internal(player_id, "bowling", match_type, start_date, end_date, db_path)
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
#' @keywords internal
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
