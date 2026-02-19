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
#' @param source Character. "local" (default) uses local DuckDB.
#'   "remote" queries GitHub releases via fast download.
#' @param db_path Character. Database path (only used when source = "local").
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
#'
#' # Query from remote (no local install needed)
#' remote_matches <- query_matches(match_type = "T20", source = "remote")
#' }
query_matches <- function(match_type = NULL,
                          season = NULL,
                          team = NULL,
                          venue = NULL,
                          date_range = NULL,
                          source = c("local", "remote"),
                          db_path = NULL) {

  source <- match.arg(source)

  # Build WHERE clause with parameterized queries (prevents SQL injection)
  where_clauses <- character()
  params <- list()

  if (!is.null(match_type)) {
    mt <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "LOWER(match_type) = ?")
    params <- c(params, list(mt))
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

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  if (source == "remote") {
    # Remote queries: build safe SQL for parquet (no user-controlled data in SQL)
    # Remote parquet queries don't support parameterized queries, so we escape values
    cli::cli_alert_info("Querying matches from remote...")
    remote_where <- build_remote_where_clause(
      match_type = match_type, season = season, team = team,
      venue = venue, date_range = date_range
    )
    sql_template <- sprintf("SELECT * FROM {table} %s ORDER BY match_date DESC", remote_where)
    result <- tryCatch({
      query_remote_parquet("matches", sql_template)
    }, error = function(e) {
      cli::cli_abort("Remote query failed: {e$message}")
    })
  } else {
    # Local DuckDB: use parameterized queries (safe)
    conn <- get_db_connection(path = db_path, read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

    query <- sprintf("SELECT * FROM matches %s ORDER BY match_date DESC", where_sql)
    result <- DBI::dbGetQuery(conn, query, params = params)
  }

  if (nrow(result) > 0) {
    cli::cli_alert_success("Found {format(nrow(result), big.mark=',')} matches")
  }

  return(result)
}


#' Build Remote WHERE Clause (Internal)
#'
#' Builds a WHERE clause for remote parquet queries with proper escaping.
#' Remote queries don't support parameterized queries, so we escape values.
#'
#' @param match_type Character. Match type filter
#' @param season Character. Season filter
#' @param team Character. Team filter
#' @param venue Character. Venue filter
#' @param date_range Date vector. Date range filter
#' @param player_id Character. Player ID filter
#' @param batter_id Character. Batter ID filter
#' @param bowler_id Character. Bowler ID filter
#' @param batting_team Character. Batting team filter
#' @param bowling_team Character. Bowling team filter
#' @param match_id Character. Match ID filter
#' @param city Character. City filter
#'
#' @return Character string with WHERE clause (or empty string)
#' @keywords internal
build_remote_where_clause <- function(match_type = NULL, season = NULL, team = NULL,
                                       venue = NULL, date_range = NULL, player_id = NULL,
                                       batter_id = NULL, bowler_id = NULL, batting_team = NULL,
                                       bowling_team = NULL, match_id = NULL, city = NULL) {
  where_clauses <- character()

  if (!is.null(match_type)) {
    mt <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, sprintf("LOWER(match_type) = '%s'", escape_sql_strings(mt)))
  }

  if (!is.null(season)) {
    where_clauses <- c(where_clauses, sprintf("season = '%s'", escape_sql_strings(as.character(season))))
  }

  if (!is.null(team)) {
    where_clauses <- c(where_clauses, sprintf("(team1 = '%s' OR team2 = '%s')",
                                               escape_sql_strings(team), escape_sql_strings(team)))
  }

  if (!is.null(venue)) {
    where_clauses <- c(where_clauses, sprintf("LOWER(venue) LIKE LOWER('%%%s%%')", escape_sql_strings(venue)))
  }

  if (!is.null(date_range) && length(date_range) == 2) {
    where_clauses <- c(where_clauses, sprintf("match_date BETWEEN '%s' AND '%s'",
                                               date_range[1], date_range[2]))
  }

  if (!is.null(match_id)) {
    where_clauses <- c(where_clauses, sprintf("match_id = '%s'", escape_sql_strings(match_id)))
  }

  if (!is.null(player_id)) {
    where_clauses <- c(where_clauses, sprintf("(batter_id = '%s' OR bowler_id = '%s')",
                                               escape_sql_strings(player_id), escape_sql_strings(player_id)))
  }

  if (!is.null(batter_id)) {
    where_clauses <- c(where_clauses, sprintf("batter_id = '%s'", escape_sql_strings(batter_id)))
  }

  if (!is.null(bowler_id)) {
    where_clauses <- c(where_clauses, sprintf("bowler_id = '%s'", escape_sql_strings(bowler_id)))
  }

  if (!is.null(batting_team)) {
    where_clauses <- c(where_clauses, sprintf("batting_team = '%s'", escape_sql_strings(batting_team)))
  }

  if (!is.null(bowling_team)) {
    where_clauses <- c(where_clauses, sprintf("bowling_team = '%s'", escape_sql_strings(bowling_team)))
  }

  if (!is.null(city)) {
    where_clauses <- c(where_clauses, sprintf("LOWER(city) LIKE LOWER('%%%s%%')", escape_sql_strings(city)))
  }

  if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }
}


#' Query Deliveries
#'
#' Query ball-by-ball delivery data with flexible filters.
#'
#' @param match_id Character. Specific match ID
#' @param match_type Character. Filter by match type (e.g., "t20", "odi", "test").
#'   Required for remote source (deliveries are partitioned by format).
#' @param player_id Character. Filter by batter or bowler (either role)
#' @param batter_id Character. Filter by specific batter
#' @param bowler_id Character. Filter by specific bowler
#' @param season Character or numeric. Filter by season
#' @param venue Character. Filter by venue/stadium (partial match)
#' @param city Character. Filter by city (partial match)
#' @param country Character. Filter by country (uses venue-to-country mapping).
#'   Not available for remote source.
#' @param event Character. Filter by event name (partial match, e.g., "Premier League").
#'   Not available for remote source.
#' @param date_range Date vector of length 2. c(start_date, end_date)
#' @param batting_team Character. Filter by batting team
#' @param bowling_team Character. Filter by bowling team
#' @param limit Integer. Limit number of results
#' @param source Character. "local" (default) uses local DuckDB.
#'   "remote" queries GitHub releases via fast download.
#' @param db_path Character. Database path (only used when source = "local").
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
#' # Get all deliveries bowled by a player in Australia
#' bumrah_aus <- query_deliveries(bowler_id = "J Bumrah", country = "Australia")
#'
#' # Get IPL deliveries for a matchup
#' matchup <- query_deliveries(
#'   batter_id = "V Kohli",
#'   bowler_id = "J Bumrah",
#'   event = "Indian Premier League"
#' )
#'
#' # Query from remote (requires match_type)
#' t20_balls <- query_deliveries(
#'   match_type = "T20",
#'   batter_id = "ba607b88",
#'   source = "remote",
#'   limit = 1000
#' )
#' }
query_deliveries <- function(match_id = NULL,
                              match_type = NULL,
                              player_id = NULL,
                              batter_id = NULL,
                              bowler_id = NULL,
                              season = NULL,
                              venue = NULL,
                              city = NULL,
                              country = NULL,
                              event = NULL,
                              date_range = NULL,
                              batting_team = NULL,
                              bowling_team = NULL,
                              limit = NULL,
                              source = c("local", "remote"),
                              db_path = NULL) {

  source <- match.arg(source)

  if (source == "remote") {
    return(query_deliveries_remote(
      match_id = match_id, match_type = match_type, player_id = player_id,
      batter_id = batter_id, bowler_id = bowler_id, season = season,
      venue = venue, city = city, date_range = date_range,
      batting_team = batting_team, bowling_team = bowling_team, limit = limit,
      country = country, event = event
    ))
  }

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Determine if we need to join with matches table (for event filter)
  needs_match_join <- !is.null(event)

  # Build WHERE clause
  where_clauses <- character()
  params <- list()

  if (!is.null(match_id)) {
    where_clauses <- c(where_clauses, "d.match_id = ?")
    params <- c(params, list(match_id))
  }

  if (!is.null(match_type)) {
    match_type <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "LOWER(d.match_type) = ?")
    params <- c(params, list(match_type))
  }

  if (!is.null(player_id)) {
    where_clauses <- c(where_clauses, "(d.batter_id = ? OR d.bowler_id = ?)")
    params <- c(params, list(player_id, player_id))
  }

  if (!is.null(batter_id)) {
    where_clauses <- c(where_clauses, "d.batter_id = ?")
    params <- c(params, list(batter_id))
  }

  if (!is.null(bowler_id)) {
    where_clauses <- c(where_clauses, "d.bowler_id = ?")
    params <- c(params, list(bowler_id))
  }

  if (!is.null(season)) {
    where_clauses <- c(where_clauses, "d.season = ?")
    params <- c(params, list(as.character(season)))
  }

  if (!is.null(venue)) {
    where_clauses <- c(where_clauses, "LOWER(d.venue) LIKE LOWER(?)")
    params <- c(params, list(paste0("%", venue, "%")))
  }

  if (!is.null(city)) {
    where_clauses <- c(where_clauses, "LOWER(d.city) LIKE LOWER(?)")
    params <- c(params, list(paste0("%", city, "%")))
  }

  if (!is.null(country)) {
    # Get venues in the specified country using the venue-country map
    venue_map <- get_venue_country_map()
    venues_in_country <- names(venue_map)[tolower(venue_map) == tolower(country)]
    if (length(venues_in_country) > 0) {
      placeholders <- paste(rep("?", length(venues_in_country)), collapse = ", ")
      where_clauses <- c(where_clauses, sprintf("d.venue IN (%s)", placeholders))
      params <- c(params, as.list(venues_in_country))
    } else {
      # No venues found for this country - return empty result
      where_clauses <- c(where_clauses, "1 = 0")
    }
  }

  if (!is.null(event)) {
    where_clauses <- c(where_clauses, "LOWER(m.event_name) LIKE LOWER(?)")
    params <- c(params, list(paste0("%", event, "%")))
  }

  if (!is.null(date_range) && length(date_range) == 2) {
    where_clauses <- c(where_clauses, "d.match_date BETWEEN ? AND ?")
    params <- c(params, list(date_range[1], date_range[2]))
  }

  if (!is.null(batting_team)) {
    where_clauses <- c(where_clauses, "d.batting_team = ?")
    params <- c(params, list(batting_team))
  }

  if (!is.null(bowling_team)) {
    where_clauses <- c(where_clauses, "d.bowling_team = ?")
    params <- c(params, list(bowling_team))
  }

  # Build query with optional JOIN
 if (needs_match_join) {
    query <- "SELECT d.* FROM deliveries d JOIN matches m ON d.match_id = m.match_id"
  } else {
    query <- "SELECT d.* FROM deliveries d"
  }

  if (length(where_clauses) > 0) {
    query <- paste(query, "WHERE", paste(where_clauses, collapse = " AND "))
  }
  query <- paste(query, "ORDER BY d.match_date DESC, d.delivery_id ASC")

  if (!is.null(limit)) {
    query <- paste(query, "LIMIT", limit)
  }

  result <- DBI::dbGetQuery(conn, query, params = params)
  return(result)
}


#' @keywords internal
query_deliveries_remote <- function(match_id = NULL, match_type = NULL,
                                     player_id = NULL, batter_id = NULL,
                                     bowler_id = NULL, season = NULL,
                                     venue = NULL, city = NULL,
                                     date_range = NULL, batting_team = NULL,
                                     bowling_team = NULL, limit = NULL,
                                     country = NULL, event = NULL) {

  # Remote requires match_type (deliveries are partitioned)
  if (is.null(match_type)) {
    cli::cli_abort("Remote deliveries query requires match_type (e.g., 'T20', 'ODI', 'Test')")
  }

  # Warn about unsupported filters
  if (!is.null(country)) {
    cli::cli_warn("country filter not available for remote queries (ignoring)")
  }
  if (!is.null(event)) {
    cli::cli_warn("event filter not available for remote queries (ignoring)")
  }

  # Determine parquet file
  table_name <- sprintf("deliveries_%s_male", match_type)
  cli::cli_alert_info("Querying {table_name} from remote...")

  # Build WHERE clause using safe helper (escapes single quotes)
  where_sql <- build_remote_where_clause(
    match_id = match_id,
    season = season,
    venue = venue,
    city = city,
    date_range = date_range,
    player_id = player_id,
    batter_id = batter_id,
    bowler_id = bowler_id,
    batting_team = batting_team,
    bowling_team = bowling_team
  )

  limit_sql <- if (!is.null(limit)) sprintf("LIMIT %d", as.integer(limit)) else ""

  sql_template <- sprintf(
    "SELECT * FROM {table} %s ORDER BY match_date DESC, delivery_id ASC %s",
    where_sql, limit_sql
  )

  result <- tryCatch({
    query_remote_parquet(table_name, sql_template)
  }, error = function(e) {
    cli::cli_abort("Remote query failed: {e$message}")
  })

  if (nrow(result) > 0) {
    cli::cli_alert_success("Found {format(nrow(result), big.mark=',')} deliveries")
  }

  return(result)
}


#' Query Player Stats
#'
#' Get aggregated batting and bowling statistics for a player.
#' This is a convenience wrapper that calls both \code{\link{query_batter_stats}}
#' and \code{\link{query_bowler_stats}}.
#'
#' @param player_id Character. Player identifier
#' @param match_type Character. Filter by match type (e.g., "t20", "odi", "test")
#' @param season Character or numeric. Filter by season
#' @param venue Character. Filter by venue/stadium (partial match)
#' @param city Character. Filter by city (partial match)
#' @param country Character. Filter by country (uses venue-to-country mapping)
#' @param event Character. Filter by event name (partial match)
#' @param date_range Date vector of length 2. c(start_date, end_date)
#' @param batting_team Character. Filter by batting team
#' @param bowling_team Character. Filter by bowling team
#' @param db_path Character. Database path
#'
#' @return Named list with:
#'   \itemize{
#'     \item player_id - The player identifier
#'     \item batting - Data frame from \code{\link{query_batter_stats}}
#'     \item bowling - Data frame from \code{\link{query_bowler_stats}}
#'   }
#' @export
#'
#' @examples
#' \dontrun{
#' # Get overall stats for an all-rounder
#' stokes_stats <- query_player_stats("BA Stokes")
#'
#' # Get T20-specific stats
#' kohli_t20_stats <- query_player_stats("V Kohli", match_type = "t20")
#'
#' # Get stats in Australia
#' root_aus <- query_player_stats("JE Root", country = "Australia", match_type = "test")
#' }
query_player_stats <- function(player_id,
                                match_type = NULL,
                                season = NULL,
                                venue = NULL,
                                city = NULL,
                                country = NULL,
                                event = NULL,
                                date_range = NULL,
                                batting_team = NULL,
                                bowling_team = NULL,
                                db_path = NULL) {

  # Get batting stats using query_batter_stats
  batting_stats <- query_batter_stats(
    batter_id = player_id,
    match_type = match_type,
    season = season,
    venue = venue,
    city = city,
    country = country,
    event = event,
    date_range = date_range,
    batting_team = batting_team,
    bowling_team = bowling_team,
    db_path = db_path
  )

  # Get bowling stats using query_bowler_stats
  bowling_stats <- query_bowler_stats(
    bowler_id = player_id,
    match_type = match_type,
    season = season,
    venue = venue,
    city = city,
    country = country,
    event = event,
    date_range = date_range,
    batting_team = batting_team,
    bowling_team = bowling_team,
    db_path = db_path
  )

  list(
    player_id = player_id,
    batting = batting_stats,
    bowling = bowling_stats
  )
}


#' Query Batter Stats
#'
#' Get aggregated batting statistics with flexible filters. If batter_id is
#' provided, returns stats for that player. If omitted, returns stats for all
#' batters matching the filters (grouped by batter).
#'
#' @param batter_id Character. The batter to get stats for. If NULL, returns
#'   stats for all batters matching other filters.
#' @param bowler_id Character. Filter to only deliveries from this bowler (for head-to-head)
#' @param match_type Character. Filter by match type (e.g., "t20", "odi", "test")
#' @param season Character or numeric. Filter by season
#' @param venue Character. Filter by venue/stadium (partial match)
#' @param city Character. Filter by city (partial match)
#' @param country Character. Filter by country (uses venue-to-country mapping)
#' @param event Character. Filter by event name (partial match)
#' @param date_range Date vector of length 2. c(start_date, end_date)
#' @param batting_team Character. Filter by batting team
#' @param bowling_team Character. Filter by bowling team
#' @param min_balls Integer. Minimum balls faced to include (default 0)
#' @param db_path Character. Database path
#'
#' @return Data frame with batting statistics:
#'   \itemize{
#'     \item batter_id - Player identifier
#'     \item balls_faced - Total balls faced
#'     \item runs_scored - Total runs scored
#'     \item dismissals - Times dismissed
#'     \item dots - Dot balls (0 runs)
#'     \item fours - Number of fours
#'     \item sixes - Number of sixes
#'     \item batting_average - Runs per dismissal
#'     \item runs_per_ball - Runs per ball faced
#'     \item strike_rate - Runs per 100 balls
#'     \item wicket_pct - Dismissal percentage per ball
#'   }
#' @export
#'
#' @examples
#' \dontrun{
#' # Single player stats
#' query_batter_stats("V Kohli", match_type = "t20")
#'
#' # Head-to-head vs specific bowler
#' query_batter_stats("V Kohli", bowler_id = "J Bumrah")
#'
#' # All batters in Test cricket (min 100 balls)
#' query_batter_stats(match_type = "test", min_balls = 100)
#'
#' # All batters in IPL
#' query_batter_stats(event = "Indian Premier League", min_balls = 50)
#'
#' # All batters in Australia
#' query_batter_stats(country = "Australia", match_type = "test", min_balls = 100)
#' }
query_batter_stats <- function(batter_id = NULL,
                                bowler_id = NULL,
                                match_type = NULL,
                                season = NULL,
                                venue = NULL,
                                city = NULL,
                                country = NULL,
                                event = NULL,
                                date_range = NULL,
                                batting_team = NULL,
                                bowling_team = NULL,
                                min_balls = 0,
                                db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Determine if we need to join with matches table (for event filter)
  needs_match_join <- !is.null(event)

  # Determine if we're querying single player or all players
  single_player <- !is.null(batter_id)

  # Build WHERE clause
  where_clauses <- character()
  params <- list()

  if (single_player) {
    where_clauses <- c(where_clauses, "d.batter_id = ?")
    params <- c(params, list(batter_id))
  }

  if (!is.null(bowler_id)) {
    where_clauses <- c(where_clauses, "d.bowler_id = ?")
    params <- c(params, list(bowler_id))
  }

  if (!is.null(match_type)) {
    match_type <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "LOWER(d.match_type) = ?")
    params <- c(params, list(match_type))
  }

  if (!is.null(season)) {
    where_clauses <- c(where_clauses, "d.season = ?")
    params <- c(params, list(as.character(season)))
  }

  if (!is.null(venue)) {
    where_clauses <- c(where_clauses, "LOWER(d.venue) LIKE LOWER(?)")
    params <- c(params, list(paste0("%", venue, "%")))
  }

  if (!is.null(city)) {
    where_clauses <- c(where_clauses, "LOWER(d.city) LIKE LOWER(?)")
    params <- c(params, list(paste0("%", city, "%")))
  }

  if (!is.null(country)) {
    venue_map <- get_venue_country_map()
    venues_in_country <- names(venue_map)[tolower(venue_map) == tolower(country)]
    if (length(venues_in_country) > 0) {
      placeholders <- paste(rep("?", length(venues_in_country)), collapse = ", ")
      where_clauses <- c(where_clauses, sprintf("d.venue IN (%s)", placeholders))
      params <- c(params, as.list(venues_in_country))
    } else {
      where_clauses <- c(where_clauses, "1 = 0")
    }
  }

  if (!is.null(event)) {
    where_clauses <- c(where_clauses, "LOWER(m.event_name) LIKE LOWER(?)")
    params <- c(params, list(paste0("%", event, "%")))
  }

  if (!is.null(date_range) && length(date_range) == 2) {
    where_clauses <- c(where_clauses, "d.match_date BETWEEN ? AND ?")
    params <- c(params, list(date_range[1], date_range[2]))
  }

  if (!is.null(batting_team)) {
    where_clauses <- c(where_clauses, "d.batting_team = ?")
    params <- c(params, list(batting_team))
  }

  if (!is.null(bowling_team)) {
    where_clauses <- c(where_clauses, "d.bowling_team = ?")
    params <- c(params, list(bowling_team))
  }

  # Build FROM clause (always join players table for names)
  if (needs_match_join) {
    from_clause <- "FROM deliveries d JOIN matches m ON d.match_id = m.match_id LEFT JOIN players p ON d.batter_id = p.player_id"
  } else {
    from_clause <- "FROM deliveries d LEFT JOIN players p ON d.batter_id = p.player_id"
  }

  # Build WHERE clause string
  where_clause <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  # Build query - with or without GROUP BY
  if (single_player) {
    query <- sprintf("
      SELECT
        p.player_name,
        COUNT(*) as balls_faced,
        COALESCE(SUM(d.runs_batter), 0) as runs_scored,
        SUM(CASE WHEN d.is_wicket AND d.player_out_id = d.batter_id THEN 1 ELSE 0 END) as dismissals,
        SUM(CASE WHEN d.runs_batter = 0 THEN 1 ELSE 0 END) as dots,
        COALESCE(SUM(CASE WHEN d.is_four THEN 1 ELSE 0 END), 0) as fours,
        COALESCE(SUM(CASE WHEN d.is_six THEN 1 ELSE 0 END), 0) as sixes
      %s
      %s
      GROUP BY p.player_name
    ", from_clause, where_clause)
  } else {
    query <- sprintf("
      SELECT
        d.batter_id,
        p.player_name,
        COUNT(*) as balls_faced,
        COALESCE(SUM(d.runs_batter), 0) as runs_scored,
        SUM(CASE WHEN d.is_wicket AND d.player_out_id = d.batter_id THEN 1 ELSE 0 END) as dismissals,
        SUM(CASE WHEN d.runs_batter = 0 THEN 1 ELSE 0 END) as dots,
        COALESCE(SUM(CASE WHEN d.is_four THEN 1 ELSE 0 END), 0) as fours,
        COALESCE(SUM(CASE WHEN d.is_six THEN 1 ELSE 0 END), 0) as sixes
      %s
      %s
      GROUP BY d.batter_id, p.player_name
      HAVING COUNT(*) >= %d
      ORDER BY runs_scored DESC
    ", from_clause, where_clause, min_balls)
  }

  result <- DBI::dbGetQuery(conn, query, params = params)

  # Handle empty result
  if (nrow(result) == 0) {
    if (single_player) {
      return(data.frame(
        batter_id = batter_id,
        player_name = NA_character_,
        balls_faced = 0L,
        runs_scored = 0L,
        dismissals = 0L,
        dots = 0L,
        fours = 0L,
        sixes = 0L,
        batting_average = NA_real_,
        runs_per_ball = NA_real_,
        strike_rate = NA_real_,
        wicket_pct = NA_real_,
        stringsAsFactors = FALSE
      ))
    } else {
      return(data.frame(
        batter_id = character(),
        player_name = character(),
        balls_faced = integer(),
        runs_scored = integer(),
        dismissals = integer(),
        dots = integer(),
        fours = integer(),
        sixes = integer(),
        batting_average = numeric(),
        runs_per_ball = numeric(),
        strike_rate = numeric(),
        wicket_pct = numeric(),
        stringsAsFactors = FALSE
      ))
    }
  }

  # Calculate derived metrics (vectorized for multiple players)
  result$batting_average <- ifelse(result$dismissals > 0,
                                    round(result$runs_scored / result$dismissals, 2),
                                    NA_real_)
  result$runs_per_ball <- round(result$runs_scored / result$balls_faced, 4)
  result$wickets_per_ball <- round(result$dismissals / result$balls_faced, 4)
  result$strike_rate <- round((result$runs_scored / result$balls_faced) * 100, 2)
  result$wicket_pct <- round((result$dismissals / result$balls_faced) * 100, 4)

  # Add batter_id to result if single player query
  if (single_player) {
    result <- cbind(batter_id = batter_id, result)
  }

  return(result)
}


#' Query Bowler Stats
#'
#' Get aggregated bowling statistics with flexible filters. If bowler_id is
#' provided, returns stats for that player. If omitted, returns stats for all
#' bowlers matching the filters (grouped by bowler).
#'
#' @param bowler_id Character. The bowler to get stats for. If NULL, returns
#'   stats for all bowlers matching other filters.
#' @param batter_id Character. Filter to only deliveries to this batter (for head-to-head)
#' @param match_type Character. Filter by match type (e.g., "t20", "odi", "test")
#' @param season Character or numeric. Filter by season
#' @param venue Character. Filter by venue/stadium (partial match)
#' @param city Character. Filter by city (partial match)
#' @param country Character. Filter by country (uses venue-to-country mapping)
#' @param event Character. Filter by event name (partial match)
#' @param date_range Date vector of length 2. c(start_date, end_date)
#' @param batting_team Character. Filter by batting team
#' @param bowling_team Character. Filter by bowling team
#' @param min_balls Integer. Minimum balls bowled to include (default 0)
#' @param db_path Character. Database path
#'
#' @return Data frame with bowling statistics:
#'   \itemize{
#'     \item bowler_id - Player identifier
#'     \item balls_bowled - Total balls bowled
#'     \item runs_conceded - Total runs conceded
#'     \item wickets - Total wickets taken
#'     \item dots - Dot balls bowled
#'     \item bowling_average - Runs per wicket
#'     \item runs_per_ball - Runs conceded per ball
#'     \item economy_rate - Runs per over (6 balls)
#'     \item wicket_pct - Wicket percentage per ball
#'     \item strike_rate - Balls per wicket
#'   }
#' @export
#'
#' @examples
#' \dontrun{
#' # Single player stats
#' query_bowler_stats("J Bumrah", match_type = "t20")
#'
#' # Head-to-head vs specific batter
#' query_bowler_stats("J Bumrah", batter_id = "V Kohli")
#'
#' # All bowlers in Test cricket (min 100 balls)
#' query_bowler_stats(match_type = "test", min_balls = 100)
#'
#' # All bowlers in IPL
#' query_bowler_stats(event = "Indian Premier League", min_balls = 50)
#'
#' # All bowlers in England
#' query_bowler_stats(country = "England", match_type = "test", min_balls = 100)
#' }
query_bowler_stats <- function(bowler_id = NULL,
                                batter_id = NULL,
                                match_type = NULL,
                                season = NULL,
                                venue = NULL,
                                city = NULL,
                                country = NULL,
                                event = NULL,
                                date_range = NULL,
                                batting_team = NULL,
                                bowling_team = NULL,
                                min_balls = 0,
                                db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Determine if we need to join with matches table (for event filter)
  needs_match_join <- !is.null(event)

  # Determine if we're querying single player or all players
  single_player <- !is.null(bowler_id)

  # Build WHERE clause
  where_clauses <- character()
  params <- list()

  if (single_player) {
    where_clauses <- c(where_clauses, "d.bowler_id = ?")
    params <- c(params, list(bowler_id))
  }

  if (!is.null(batter_id)) {
    where_clauses <- c(where_clauses, "d.batter_id = ?")
    params <- c(params, list(batter_id))
  }

  if (!is.null(match_type)) {
    match_type <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "LOWER(d.match_type) = ?")
    params <- c(params, list(match_type))
  }

  if (!is.null(season)) {
    where_clauses <- c(where_clauses, "d.season = ?")
    params <- c(params, list(as.character(season)))
  }

  if (!is.null(venue)) {
    where_clauses <- c(where_clauses, "LOWER(d.venue) LIKE LOWER(?)")
    params <- c(params, list(paste0("%", venue, "%")))
  }

  if (!is.null(city)) {
    where_clauses <- c(where_clauses, "LOWER(d.city) LIKE LOWER(?)")
    params <- c(params, list(paste0("%", city, "%")))
  }

  if (!is.null(country)) {
    venue_map <- get_venue_country_map()
    venues_in_country <- names(venue_map)[tolower(venue_map) == tolower(country)]
    if (length(venues_in_country) > 0) {
      placeholders <- paste(rep("?", length(venues_in_country)), collapse = ", ")
      where_clauses <- c(where_clauses, sprintf("d.venue IN (%s)", placeholders))
      params <- c(params, as.list(venues_in_country))
    } else {
      where_clauses <- c(where_clauses, "1 = 0")
    }
  }

  if (!is.null(event)) {
    where_clauses <- c(where_clauses, "LOWER(m.event_name) LIKE LOWER(?)")
    params <- c(params, list(paste0("%", event, "%")))
  }

  if (!is.null(date_range) && length(date_range) == 2) {
    where_clauses <- c(where_clauses, "d.match_date BETWEEN ? AND ?")
    params <- c(params, list(date_range[1], date_range[2]))
  }

  if (!is.null(batting_team)) {
    where_clauses <- c(where_clauses, "d.batting_team = ?")
    params <- c(params, list(batting_team))
  }

  if (!is.null(bowling_team)) {
    where_clauses <- c(where_clauses, "d.bowling_team = ?")
    params <- c(params, list(bowling_team))
  }

  # Build FROM clause (always join players table for names)
  if (needs_match_join) {
    from_clause <- "FROM deliveries d JOIN matches m ON d.match_id = m.match_id LEFT JOIN players p ON d.bowler_id = p.player_id"
  } else {
    from_clause <- "FROM deliveries d LEFT JOIN players p ON d.bowler_id = p.player_id"
  }

  # Build WHERE clause string
  where_clause <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  # Build query - with or without GROUP BY
  if (single_player) {
    query <- sprintf("
      SELECT
        p.player_name,
        COUNT(*) as balls_bowled,
        COALESCE(SUM(d.runs_total), 0) as runs_conceded,
        SUM(CASE WHEN d.is_wicket THEN 1 ELSE 0 END) as wickets,
        SUM(CASE WHEN d.runs_total = 0 THEN 1 ELSE 0 END) as dots
      %s
      %s
      GROUP BY p.player_name
    ", from_clause, where_clause)
  } else {
    query <- sprintf("
      SELECT
        d.bowler_id,
        p.player_name,
        COUNT(*) as balls_bowled,
        COALESCE(SUM(d.runs_total), 0) as runs_conceded,
        SUM(CASE WHEN d.is_wicket THEN 1 ELSE 0 END) as wickets,
        SUM(CASE WHEN d.runs_total = 0 THEN 1 ELSE 0 END) as dots
      %s
      %s
      GROUP BY d.bowler_id, p.player_name
      HAVING COUNT(*) >= %d
      ORDER BY wickets DESC
    ", from_clause, where_clause, min_balls)
  }

  result <- DBI::dbGetQuery(conn, query, params = params)

  # Handle empty result
  if (nrow(result) == 0) {
    if (single_player) {
      return(data.frame(
        bowler_id = bowler_id,
        player_name = NA_character_,
        balls_bowled = 0L,
        runs_conceded = 0L,
        wickets = 0L,
        dots = 0L,
        bowling_average = NA_real_,
        runs_per_ball = NA_real_,
        economy_rate = NA_real_,
        wicket_pct = NA_real_,
        strike_rate = NA_real_,
        stringsAsFactors = FALSE
      ))
    } else {
      return(data.frame(
        bowler_id = character(),
        player_name = character(),
        balls_bowled = integer(),
        runs_conceded = integer(),
        wickets = integer(),
        dots = integer(),
        bowling_average = numeric(),
        runs_per_ball = numeric(),
        economy_rate = numeric(),
        wicket_pct = numeric(),
        strike_rate = numeric(),
        stringsAsFactors = FALSE
      ))
    }
  }

  # Calculate derived metrics (vectorized for multiple players)
  result$bowling_average <- ifelse(result$wickets > 0,
                                    round(result$runs_conceded / result$wickets, 2),
                                    NA_real_)
  result$runs_per_ball <- round(result$runs_conceded / result$balls_bowled, 4)
  result$wickets_per_ball <- round(result$wickets / result$balls_bowled, 4)
  result$economy_rate <- round((result$runs_conceded / result$balls_bowled) * 6, 2)
  result$wicket_pct <- round((result$wickets / result$balls_bowled) * 100, 4)
  result$strike_rate <- ifelse(result$wickets > 0,
                                round(result$balls_bowled / result$wickets, 2),
                                NA_real_)

  # Add bowler_id to result if single player query
  if (single_player) {
    result <- cbind(bowler_id = bowler_id, result)
  }

  return(result)
}
