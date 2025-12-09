# Data Query Functions for Bouncer

#' Query Matches
#'
#' Query match data with filters.
#'
#' @param match_type Character. Filter by match type (e.g., "odi", "t20", "test")
#' @param season Character or numeric. Filter by season/year
#' @param team Character. Filter matches involving this team
#' @param venue Character. Filter by venue/stadium
#' @param date_range Date vector of length 2. c(start_date, end_date)
#' @param db_path Character. Database path
#'
#' @return Data frame with match data
#' @export
#'
#' @examples
#' \dontrun{
#' # Get all ODI matches in 2023
#' odis_2023 <- query_matches(match_type = "odi", season = "2023")
#'
#' # Get all India matches
#' india_matches <- query_matches(team = "India")
#'
#' # Get recent T20Is
#' recent_t20 <- query_matches(
#'   match_type = "t20",
#'   date_range = c(as.Date("2024-01-01"), as.Date("2024-12-31"))
#' )
#' }
query_matches <- function(match_type = NULL,
                          season = NULL,
                          team = NULL,
                          venue = NULL,
                          date_range = NULL,
                          db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Build WHERE clause
  where_clauses <- character()
  params <- list()

  if (!is.null(match_type)) {
    match_type <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "LOWER(match_type) = ?")
    params <- c(params, list(match_type))
  }

  if (!is.null(season)) {
    where_clauses <- c(where_clauses, "season = ?")
    params <- c(params, list(as.character(season)))
  }

  if (!is.null(team)) {
    where_clauses <- c(where_clauses, "(team1 = ? OR team2 = ?)")
    params <- c(params, list(team, team))
  }

  if (!is.null(venue)) {
    where_clauses <- c(where_clauses, "LOWER(venue) LIKE LOWER(?)")
    params <- c(params, list(paste0("%", venue, "%")))
  }

  if (!is.null(date_range) && length(date_range) == 2) {
    where_clauses <- c(where_clauses, "match_date BETWEEN ? AND ?")
    params <- c(params, list(date_range[1], date_range[2]))
  }

  # Build query
  query <- "SELECT * FROM matches"
  if (length(where_clauses) > 0) {
    query <- paste(query, "WHERE", paste(where_clauses, collapse = " AND "))
  }
  query <- paste(query, "ORDER BY match_date DESC")

  result <- DBI::dbGetQuery(conn, query, params = params)
  return(result)
}


#' Query Deliveries
#'
#' Query ball-by-ball delivery data with filters.
#'
#' @param match_id Character. Specific match ID
#' @param match_type Character. Filter by match type
#' @param player_id Character. Filter by batter or bowler
#' @param batter_id Character. Filter by specific batter
#' @param bowler_id Character. Filter by specific bowler
#' @param season Character or numeric. Filter by season
#' @param limit Integer. Limit number of results
#' @param db_path Character. Database path
#'
#' @return Data frame with delivery data
#' @export
#'
#' @examples
#' \dontrun{
#' # Get all deliveries for a specific match
#' match_balls <- query_deliveries(match_id = "12345")
#'
#' # Get all deliveries faced by a player
#' kohli_balls <- query_deliveries(batter_id = "V Kohli", limit = 1000)
#'
#' # Get all deliveries bowled by a player
#' bumrah_balls <- query_deliveries(bowler_id = "J Bumrah", limit = 1000)
#' }
query_deliveries <- function(match_id = NULL,
                              match_type = NULL,
                              player_id = NULL,
                              batter_id = NULL,
                              bowler_id = NULL,
                              season = NULL,
                              limit = NULL,
                              db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Build WHERE clause
  where_clauses <- character()
  params <- list()

  if (!is.null(match_id)) {
    where_clauses <- c(where_clauses, "match_id = ?")
    params <- c(params, list(match_id))
  }

  if (!is.null(match_type)) {
    match_type <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "LOWER(match_type) = ?")
    params <- c(params, list(match_type))
  }

  if (!is.null(player_id)) {
    where_clauses <- c(where_clauses, "(batter_id = ? OR bowler_id = ?)")
    params <- c(params, list(player_id, player_id))
  }

  if (!is.null(batter_id)) {
    where_clauses <- c(where_clauses, "batter_id = ?")
    params <- c(params, list(batter_id))
  }

  if (!is.null(bowler_id)) {
    where_clauses <- c(where_clauses, "bowler_id = ?")
    params <- c(params, list(bowler_id))
  }

  if (!is.null(season)) {
    where_clauses <- c(where_clauses, "season = ?")
    params <- c(params, list(as.character(season)))
  }

  # Build query
  query <- "SELECT * FROM deliveries"
  if (length(where_clauses) > 0) {
    query <- paste(query, "WHERE", paste(where_clauses, collapse = " AND "))
  }
  query <- paste(query, "ORDER BY match_date DESC, delivery_id ASC")

  if (!is.null(limit)) {
    query <- paste(query, "LIMIT", limit)
  }

  result <- DBI::dbGetQuery(conn, query, params = params)
  return(result)
}


#' Query Player Stats
#'
#' Get aggregated statistics for a player.
#'
#' @param player_id Character. Player identifier
#' @param match_type Character. Filter by match type
#' @param season Character. Filter by season
#' @param db_path Character. Database path
#'
#' @return Named list with batting and bowling stats
#' @export
#'
#' @examples
#' \dontrun{
#' # Get overall stats
#' kohli_stats <- query_player_stats("V Kohli")
#'
#' # Get T20-specific stats
#' kohli_t20_stats <- query_player_stats("V Kohli", match_type = "t20")
#' }
query_player_stats <- function(player_id,
                                match_type = NULL,
                                season = NULL,
                                db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Build WHERE clause
  where_clauses <- character()
  params <- list()

  if (!is.null(match_type)) {
    match_type <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "LOWER(match_type) = ?")
    params <- c(params, list(match_type))
  }

  if (!is.null(season)) {
    where_clauses <- c(where_clauses, "season = ?")
    params <- c(params, list(as.character(season)))
  }

  where_clause <- if (length(where_clauses) > 0) {
    paste("AND", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  # Batting stats
  batting_query <- sprintf("
    SELECT
      COUNT(*) as balls_faced,
      SUM(runs_batter) as runs_scored,
      SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as dismissals,
      SUM(CASE WHEN is_four THEN 1 ELSE 0 END) as fours,
      SUM(CASE WHEN is_six THEN 1 ELSE 0 END) as sixes,
      AVG(runs_batter) as avg_runs_per_ball
    FROM deliveries
    WHERE batter_id = ?
    %s
  ", where_clause)

  batting_params <- c(list(player_id), params)
  batting_stats <- DBI::dbGetQuery(conn, batting_query, params = batting_params)

  # Bowling stats
  bowling_query <- sprintf("
    SELECT
      COUNT(*) as balls_bowled,
      SUM(runs_total) as runs_conceded,
      SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets,
      AVG(runs_total) as economy_per_ball
    FROM deliveries
    WHERE bowler_id = ?
    %s
  ", where_clause)

  bowling_params <- c(list(player_id), params)
  bowling_stats <- DBI::dbGetQuery(conn, bowling_query, params = bowling_params)

  list(
    player_id = player_id,
    batting = as.list(batting_stats[1, ]),
    bowling = as.list(bowling_stats[1, ])
  )
}
