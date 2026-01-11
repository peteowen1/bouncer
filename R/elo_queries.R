# ELO Query Functions

#' Get Player ELO at Specific Delivery
#'
#' Retrieves a player's ELO rating at a specific delivery in a match.
#'
#' @param delivery_id Numeric. The unique delivery identifier
#' @param player_id Character. Player identifier
#' @param when Character. "before" or "after" the delivery. Default "after".
#' @param rating_type Character. "batting" or "bowling". Default "batting".
#' @param db_path Character. Database path
#'
#' @return Numeric. ELO rating at that delivery (or NA if not found)
#' @keywords internal
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
#' Retrieves a player's ELO progression throughout a specific match.
#'
#' @param match_id Character. Match identifier
#' @param player_id Character. Player identifier
#' @param rating_type Character. "batting" or "bowling"
#' @param db_path Character. Database path
#'
#' @return Data frame with delivery-by-delivery ELO progression
#' @keywords internal
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
#'
#' @param player_id Character. Player identifier
#' @param elo_min Numeric. Minimum ELO
#' @param elo_max Numeric. Maximum ELO
#' @param rating_type Character. "batting" or "bowling"
#' @param limit Integer. Max results to return
#' @param db_path Character. Database path
#'
#' @return Data frame with matching deliveries
#' @keywords internal
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
