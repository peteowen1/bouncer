# Team Metrics and Analytics Functions

#' Team Batting Stats
#'
#' Get batting statistics for one or all teams. Includes runs scored, average,
#' strike rate, and boundary metrics.
#'
#' @param team Character. Team name (e.g., "India", "Australia").
#'   If NULL (default), returns stats for all teams.
#' @param match_type Character. Filter by match type: "T20", "ODI", "Test",
#'   "IT20", "MDM", or NULL for all formats. Required for remote source.
#' @param season Character. Filter by season (e.g., "2023", "2023/24").
#' @param min_matches Integer. Minimum matches to include (default 10).
#'   Only applies when team is NULL.
#' @param source Character. "local" (default) uses local DuckDB.
#'   "remote" queries GitHub releases via fast download.
#' @param db_path Character. Database path (only used when source = "local").
#'
#' @return Data frame with team batting metrics:
#'   - team, matches, innings, balls_faced, runs_scored, wickets_lost
#'   - batting_average, strike_rate, boundary_percentage
#'
#' @export
#' @examples
#' \dontrun{
#' # Get all teams' batting stats
#' all_teams <- team_batting_stats()
#'
#' # Get T20 batting stats for all teams
#' t20_teams <- team_batting_stats(match_type = "T20")
#'
#' # Get stats for a specific team
#' india_batting <- team_batting_stats("India")
#'
#' # Get T20 stats for a specific team
#' india_t20 <- team_batting_stats("India", match_type = "T20")
#'
#' # Get T20 stats from remote (no local install needed)
#' remote_stats <- team_batting_stats(match_type = "T20", source = "remote")
#' }
team_batting_stats <- function(team = NULL,
                                match_type = NULL,
                                season = NULL,
                                min_matches = 10,
                                source = c("local", "remote"),
                                db_path = NULL) {

  source <- match.arg(source)

  if (source == "remote") {
    return(team_batting_stats_remote(team, match_type, season, min_matches))
  }

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Build WHERE clause
  where_clauses <- character()
  params <- list()

  if (!is.null(team)) {
    where_clauses <- c(where_clauses, "d.batting_team = ?")
    params <- c(params, list(team))
  }

  if (!is.null(match_type)) {
    mt <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "LOWER(d.match_type) = ?")
    params <- c(params, list(mt))
  }

  if (!is.null(season)) {
    where_clauses <- c(where_clauses, "d.season = ?")
    params <- c(params, list(as.character(season)))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  # Query aggregated stats
  query <- sprintf("
    SELECT
      d.batting_team as team,
      COUNT(DISTINCT d.match_id) as matches,
      COUNT(DISTINCT d.match_id || '_' || d.innings) as innings,
      COUNT(*) as balls_faced,
      SUM(d.runs_batter) as runs_scored,
      SUM(CASE WHEN d.is_wicket THEN 1 ELSE 0 END) as wickets_lost,
      SUM(CASE WHEN d.is_four THEN 1 ELSE 0 END) as fours,
      SUM(CASE WHEN d.is_six THEN 1 ELSE 0 END) as sixes
    FROM deliveries d
    %s
    GROUP BY d.batting_team
    HAVING COUNT(DISTINCT d.match_id) >= ?
    ORDER BY runs_scored DESC
  ", where_sql)

  min_matches_filter <- if (is.null(team)) min_matches else 1
  params <- c(params, list(min_matches_filter))

  result <- DBI::dbGetQuery(conn, query, params = params)

  if (nrow(result) == 0) {
    cli::cli_warn("No batting data found for the specified filters")
    return(data.frame())
  }

  calculate_team_batting_metrics(result)
}


#' @keywords internal
team_batting_stats_remote <- function(team = NULL, match_type = NULL,
                                       season = NULL, min_matches = 10,
                                       gender = "male") {

  # Determine parquet file
  if (is.null(match_type)) {
    table_name <- paste0("deliveries_T20_", gender)
    cli::cli_alert_info("Querying T20 {gender} (specify match_type for other formats)...")
  } else {
    table_name <- paste0("deliveries_", match_type, "_", gender)
    cli::cli_alert_info("Querying {table_name}...")
  }

  # Build WHERE clause
  where_clauses <- character()
  if (!is.null(team)) {
    where_clauses <- c(where_clauses, sprintf("batting_team = '%s'", escape_sql_quotes(team)))
  }
  if (!is.null(season)) {
    where_clauses <- c(where_clauses, sprintf("season = '%s'", escape_sql_quotes(season)))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  min_matches_filter <- if (is.null(team)) min_matches else 1

  sql_template <- sprintf("
    SELECT
      batting_team as team,
      COUNT(DISTINCT match_id) as matches,
      COUNT(DISTINCT match_id || '_' || innings) as innings,
      COUNT(*) as balls_faced,
      SUM(runs_batter) as runs_scored,
      SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets_lost,
      SUM(CASE WHEN is_four THEN 1 ELSE 0 END) as fours,
      SUM(CASE WHEN is_six THEN 1 ELSE 0 END) as sixes
    FROM {table}
    %s
    GROUP BY batting_team
    HAVING COUNT(DISTINCT match_id) >= %d
    ORDER BY runs_scored DESC
  ", where_sql, min_matches_filter)

  result <- tryCatch({
    query_remote_parquet(table_name, sql_template)
  }, error = function(e) {
    cli::cli_abort("Remote query failed: {e$message}")
  })

  if (nrow(result) == 0) {
    cli::cli_warn("No batting data found for the specified filters")
    return(data.frame())
  }

  calculate_team_batting_metrics(result)
}


#' @keywords internal
calculate_team_batting_metrics <- function(result) {
  result$runs_per_ball <- round(result$runs_scored / result$balls_faced, 4)
  result$wickets_per_ball <- round(result$wickets_lost / result$balls_faced, 4)
  result$batting_average <- round(result$runs_scored / pmax(result$wickets_lost, 1), 2)
  result$strike_rate <- round((result$runs_scored / result$balls_faced) * 100, 2)
  result$boundary_percentage <- round(((result$fours + result$sixes) / result$balls_faced) * 100, 2)
  result$runs_per_match <- round(result$runs_scored / result$matches, 1)

  result <- result[, c("team", "matches", "innings", "balls_faced", "runs_scored",
                       "wickets_lost", "fours", "sixes", "runs_per_ball", "wickets_per_ball",
                       "batting_average", "strike_rate", "boundary_percentage", "runs_per_match")]

  cli::cli_alert_success("Loaded batting stats for {nrow(result)} team(s)")
  result
}


#' Team Bowling Stats
#'
#' Get bowling statistics for one or all teams. Includes wickets taken,
#' economy rate, and bowling average.
#'
#' @param team Character. Team name (e.g., "India", "Australia").
#'   If NULL (default), returns stats for all teams.
#' @param match_type Character. Filter by match type: "T20", "ODI", "Test",
#'   "IT20", "MDM", or NULL for all formats. Required for remote source.
#' @param season Character. Filter by season (e.g., "2023", "2023/24").
#' @param min_matches Integer. Minimum matches to include (default 10).
#'   Only applies when team is NULL.
#' @param source Character. "local" (default) uses local DuckDB.
#'   "remote" queries GitHub releases via fast download.
#' @param db_path Character. Database path (only used when source = "local").
#'
#' @return Data frame with team bowling metrics:
#'   - team, matches, innings, balls_bowled, runs_conceded, wickets_taken
#'   - bowling_average, economy_rate, strike_rate
#'
#' @export
#' @examples
#' \dontrun{
#' # Get all teams' bowling stats
#' all_teams <- team_bowling_stats()
#'
#' # Get T20 bowling stats for all teams
#' t20_teams <- team_bowling_stats(match_type = "T20")
#'
#' # Get stats for a specific team
#' india_bowling <- team_bowling_stats("India")
#'
#' # Get T20 stats from remote (no local install needed)
#' remote_stats <- team_bowling_stats(match_type = "T20", source = "remote")
#' }
team_bowling_stats <- function(team = NULL,
                                match_type = NULL,
                                season = NULL,
                                min_matches = 10,
                                source = c("local", "remote"),
                                db_path = NULL) {

  source <- match.arg(source)

  if (source == "remote") {
    return(team_bowling_stats_remote(team, match_type, season, min_matches))
  }

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Build WHERE clause
  where_clauses <- character()
  params <- list()

  if (!is.null(team)) {
    where_clauses <- c(where_clauses, "d.bowling_team = ?")
    params <- c(params, list(team))
  }

  if (!is.null(match_type)) {
    mt <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "LOWER(d.match_type) = ?")
    params <- c(params, list(mt))
  }

  if (!is.null(season)) {
    where_clauses <- c(where_clauses, "d.season = ?")
    params <- c(params, list(as.character(season)))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  # Query aggregated stats
  query <- sprintf("
    SELECT
      d.bowling_team as team,
      COUNT(DISTINCT d.match_id) as matches,
      COUNT(DISTINCT d.match_id || '_' || d.innings) as innings,
      COUNT(*) as balls_bowled,
      SUM(d.runs_total) as runs_conceded,
      SUM(CASE WHEN d.is_wicket THEN 1 ELSE 0 END) as wickets_taken,
      SUM(CASE WHEN d.runs_batter = 0 AND d.wides = 0 AND d.noballs = 0 THEN 1 ELSE 0 END) as dots
    FROM deliveries d
    %s
    GROUP BY d.bowling_team
    HAVING COUNT(DISTINCT d.match_id) >= ?
    ORDER BY wickets_taken DESC
  ", where_sql)

  min_matches_filter <- if (is.null(team)) min_matches else 1
  params <- c(params, list(min_matches_filter))

  result <- DBI::dbGetQuery(conn, query, params = params)

  if (nrow(result) == 0) {
    cli::cli_warn("No bowling data found for the specified filters")
    return(data.frame())
  }

  calculate_team_bowling_metrics(result)
}


#' @keywords internal
team_bowling_stats_remote <- function(team = NULL, match_type = NULL,
                                       season = NULL, min_matches = 10,
                                       gender = "male") {

  if (is.null(match_type)) {
    table_name <- paste0("deliveries_T20_", gender)
    cli::cli_alert_info("Querying T20 {gender} (specify match_type for other formats)...")
  } else {
    table_name <- paste0("deliveries_", match_type, "_", gender)
    cli::cli_alert_info("Querying {table_name}...")
  }

  where_clauses <- character()
  if (!is.null(team)) {
    where_clauses <- c(where_clauses, sprintf("bowling_team = '%s'", escape_sql_quotes(team)))
  }
  if (!is.null(season)) {
    where_clauses <- c(where_clauses, sprintf("season = '%s'", escape_sql_quotes(season)))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  min_matches_filter <- if (is.null(team)) min_matches else 1

  sql_template <- sprintf("
    SELECT
      bowling_team as team,
      COUNT(DISTINCT match_id) as matches,
      COUNT(DISTINCT match_id || '_' || innings) as innings,
      COUNT(*) as balls_bowled,
      SUM(runs_total) as runs_conceded,
      SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets_taken,
      SUM(CASE WHEN runs_batter = 0 AND wides = 0 AND noballs = 0 THEN 1 ELSE 0 END) as dots
    FROM {table}
    %s
    GROUP BY bowling_team
    HAVING COUNT(DISTINCT match_id) >= %d
    ORDER BY wickets_taken DESC
  ", where_sql, min_matches_filter)

  result <- tryCatch({
    query_remote_parquet(table_name, sql_template)
  }, error = function(e) {
    cli::cli_abort("Remote query failed: {e$message}")
  })

  if (nrow(result) == 0) {
    cli::cli_warn("No bowling data found for the specified filters")
    return(data.frame())
  }

  calculate_team_bowling_metrics(result)
}


#' @keywords internal
calculate_team_bowling_metrics <- function(result) {
  result$runs_per_ball <- round(result$runs_conceded / result$balls_bowled, 4)
  result$wickets_per_ball <- round(result$wickets_taken / result$balls_bowled, 4)
  result$bowling_average <- round(result$runs_conceded / pmax(result$wickets_taken, 1), 2)
  result$economy_rate <- round((result$runs_conceded / result$balls_bowled) * 6, 2)
  result$strike_rate <- round(result$balls_bowled / pmax(result$wickets_taken, 1), 1)
  result$dot_percentage <- round((result$dots / result$balls_bowled) * 100, 2)
  result$wickets_per_match <- round(result$wickets_taken / result$matches, 1)

  result <- result[, c("team", "matches", "innings", "balls_bowled", "runs_conceded",
                       "wickets_taken", "dots", "runs_per_ball", "wickets_per_ball",
                       "bowling_average", "economy_rate", "strike_rate", "dot_percentage",
                       "wickets_per_match")]

  cli::cli_alert_success("Loaded bowling stats for {nrow(result)} team(s)")
  result
}


#' Head to Head Record
#'
#' Get historical head-to-head record between two teams.
#'
#' @param team1 Character. First team name.
#' @param team2 Character. Second team name.
#' @param match_type Character. Filter by match type: "T20", "ODI", "Test",
#'   "IT20", "MDM", or NULL for all formats.
#' @param source Character. "local" (default) uses local DuckDB.
#'   "remote" queries GitHub releases via fast download.
#' @param db_path Character. Database path (only used when source = "local").
#'
#' @return Data frame with head-to-head record:
#'   - matches, team1_wins, team2_wins, draws/ties/no_results
#'   - team1_win_pct, team2_win_pct
#'   - recent results
#'
#' @export
#' @examples
#' \dontrun{
#' # Overall head-to-head
#' head_to_head("India", "Australia")
#'
#' # T20 head-to-head
#' head_to_head("India", "Pakistan", match_type = "T20")
#'
#' # Test match head-to-head
#' head_to_head("England", "Australia", match_type = "Test")
#'
#' # Remote head-to-head (no local install needed)
#' head_to_head("India", "Pakistan", source = "remote")
#' }
head_to_head <- function(team1, team2, match_type = NULL,
                          source = c("local", "remote"), db_path = NULL) {

  source <- match.arg(source)

  if (source == "remote") {
    return(head_to_head_remote(team1, team2, match_type))
  }

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Build WHERE clause
  where_clauses <- c(
    "((m.team1 = ? AND m.team2 = ?) OR (m.team1 = ? AND m.team2 = ?))"
  )
  params <- list(team1, team2, team2, team1)

  if (!is.null(match_type)) {
    mt <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "LOWER(m.match_type) = ?")
    params <- c(params, list(mt))
  }

  where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))

  # Get match results
  query <- sprintf("
    SELECT
      m.match_id,
      m.match_date,
      m.match_type,
      m.team1,
      m.team2,
      m.outcome_winner,
      m.outcome_type,
      m.outcome_by_runs,
      m.outcome_by_wickets,
      m.venue
    FROM matches m
    %s
    ORDER BY m.match_date DESC
  ", where_sql)

  matches <- DBI::dbGetQuery(conn, query, params = params)

  calculate_head_to_head_summary(matches, team1, team2, match_type)
}


#' @keywords internal
head_to_head_remote <- function(team1, team2, match_type = NULL) {

  cli::cli_alert_info("Querying matches from remote...")

  # Build WHERE clause - matches uses unified matches.parquet
  where_clauses <- sprintf(
    "((team1 = '%s' AND team2 = '%s') OR (team1 = '%s' AND team2 = '%s'))",
    escape_sql_quotes(team1), escape_sql_quotes(team2),
    escape_sql_quotes(team2), escape_sql_quotes(team1)
  )

  if (!is.null(match_type)) {
    where_clauses <- paste(where_clauses, "AND", sprintf("UPPER(match_type) = '%s'", toupper(escape_sql_quotes(match_type))))
  }

  sql_template <- sprintf("
    SELECT
      match_id,
      match_date,
      match_type,
      team1,
      team2,
      outcome_winner,
      outcome_type,
      outcome_by_runs,
      outcome_by_wickets,
      venue
    FROM {table}
    WHERE %s
    ORDER BY match_date DESC
  ", where_clauses)

  matches <- tryCatch({
    query_remote_parquet("matches", sql_template)
  }, error = function(e) {
    cli::cli_abort("Remote query failed: {e$message}")
  })

  calculate_head_to_head_summary(matches, team1, team2, match_type)
}


#' @keywords internal
calculate_head_to_head_summary <- function(matches, team1, team2, match_type) {

  if (nrow(matches) == 0) {
    cli::cli_warn("No matches found between {team1} and {team2}")
    return(NULL)
  }

  total_matches <- nrow(matches)
  team1_wins <- sum(matches$outcome_winner == team1, na.rm = TRUE)
  team2_wins <- sum(matches$outcome_winner == team2, na.rm = TRUE)
  draws <- sum(matches$outcome_type == "draw", na.rm = TRUE)
  ties <- sum(matches$outcome_type == "tie", na.rm = TRUE)
  no_results <- sum(matches$outcome_type == "no result", na.rm = TRUE)

  recent <- utils::head(matches, 5)
  recent_results <- sapply(seq_len(nrow(recent)), function(i) {
    if (is.na(recent$outcome_winner[i])) {
      "D"
    } else if (recent$outcome_winner[i] == team1) {
      "W"
    } else {
      "L"
    }
  })

  summary <- data.frame(
    team1 = team1,
    team2 = team2,
    match_type = match_type %||% "all",
    matches = total_matches,
    team1_wins = team1_wins,
    team2_wins = team2_wins,
    draws = draws,
    ties = ties,
    no_results = no_results,
    team1_win_pct = round((team1_wins / total_matches) * 100, 1),
    team2_win_pct = round((team2_wins / total_matches) * 100, 1),
    first_match = min(matches$match_date),
    last_match = max(matches$match_date),
    recent_form_team1 = paste(recent_results, collapse = ""),
    stringsAsFactors = FALSE
  )

  cli::cli_alert_success("{team1} vs {team2}: {total_matches} matches ({team1_wins}-{team2_wins})")
  summary
}


#' Venue Statistics
#'
#' Get statistics for one or all venues. Includes scoring rates, wicket rates,
#' and match conditions.
#'
#' @param venue Character. Venue name (partial match supported).
#'   If NULL (default), returns stats for all venues.
#' @param match_type Character. Filter by match type: "T20", "ODI", "Test",
#'   "IT20", "MDM", or NULL for all formats. Required for remote source.
#' @param min_matches Integer. Minimum matches to include (default 5).
#'   Only applies when venue is NULL.
#' @param source Character. "local" (default) uses local DuckDB.
#'   "remote" queries GitHub releases via fast download.
#' @param db_path Character. Database path (only used when source = "local").
#'
#' @return Data frame with venue metrics:
#'   - venue, city, matches, avg_first_innings, avg_second_innings
#'   - run_rate, wickets_per_match, boundary_rate
#'
#' @export
#' @examples
#' \dontrun{
#' # Get all venues' stats
#' all_venues <- venue_stats()
#'
#' # Get T20 venue stats
#' t20_venues <- venue_stats(match_type = "T20")
#'
#' # Get stats for a specific venue
#' mcg <- venue_stats("MCG")
#'
#' # Get stats for venues in a city
#' mumbai_venues <- venue_stats("Mumbai")
#'
#' # Get T20 venue stats from remote (no local install needed)
#' remote_venues <- venue_stats(match_type = "T20", source = "remote")
#' }
venue_stats <- function(venue = NULL,
                         match_type = NULL,
                         min_matches = 5,
                         source = c("local", "remote"),
                         db_path = NULL) {

  source <- match.arg(source)

  if (source == "remote") {
    return(venue_stats_remote(venue, match_type, min_matches))
  }

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Build WHERE clause for deliveries
  where_clauses <- character()
  params <- list()

  if (!is.null(venue)) {
    where_clauses <- c(where_clauses, "LOWER(d.venue) LIKE LOWER(?)")
    params <- c(params, list(paste0("%", venue, "%")))
  }

  if (!is.null(match_type)) {
    mt <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "LOWER(d.match_type) = ?")
    params <- c(params, list(mt))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  # Query aggregated stats by venue (use MAX(city) to pick non-null city)
  query <- sprintf("
    SELECT
      d.venue,
      MAX(d.city) as city,
      COUNT(DISTINCT d.match_id) as matches,
      COUNT(*) as total_balls,
      SUM(d.runs_total) as total_runs,
      SUM(CASE WHEN d.is_wicket THEN 1 ELSE 0 END) as total_wickets,
      SUM(CASE WHEN d.is_four THEN 1 ELSE 0 END) as total_fours,
      SUM(CASE WHEN d.is_six THEN 1 ELSE 0 END) as total_sixes
    FROM deliveries d
    %s
    GROUP BY d.venue
    HAVING COUNT(DISTINCT d.match_id) >= ?
    ORDER BY matches DESC
  ", where_sql)

  min_matches_filter <- if (is.null(venue)) min_matches else 1
  params <- c(params, list(min_matches_filter))

  result <- DBI::dbGetQuery(conn, query, params = params)

  if (nrow(result) == 0) {
    cli::cli_warn("No venue data found for the specified filters")
    return(data.frame())
  }

  # Get first/second innings breakdown
  innings_query <- sprintf("
    SELECT
      venue,
      innings,
      ROUND(AVG(innings_total), 0) as avg_score
    FROM (
      SELECT
        venue,
        match_id,
        innings,
        SUM(runs_total) as innings_total
      FROM deliveries d
      %s
      GROUP BY venue, match_id, innings
    )
    WHERE innings IN (1, 2)
    GROUP BY venue, innings
  ", where_sql)

  innings_stats <- DBI::dbGetQuery(conn, innings_query, params = params[seq_len(length(params) - 1)])

  calculate_venue_metrics(result, innings_stats)
}


#' @keywords internal
venue_stats_remote <- function(venue = NULL, match_type = NULL, min_matches = 5,
                                gender = "male") {

  # Determine parquet file
  if (is.null(match_type)) {
    table_name <- paste0("deliveries_T20_", gender)
    cli::cli_alert_info("Querying T20 {gender} (specify match_type for other formats)...")
  } else {
    table_name <- paste0("deliveries_", match_type, "_", gender)
    cli::cli_alert_info("Querying {table_name}...")
  }

  # Build WHERE clause
  where_clauses <- character()
  if (!is.null(venue)) {
    where_clauses <- c(where_clauses, sprintf("LOWER(venue) LIKE LOWER('%%%s%%')", escape_sql_quotes(venue)))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  min_matches_filter <- if (is.null(venue)) min_matches else 1

  # Main aggregation query
  sql_template <- sprintf("
    SELECT
      venue,
      MAX(city) as city,
      COUNT(DISTINCT match_id) as matches,
      COUNT(*) as total_balls,
      SUM(runs_total) as total_runs,
      SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as total_wickets,
      SUM(CASE WHEN is_four THEN 1 ELSE 0 END) as total_fours,
      SUM(CASE WHEN is_six THEN 1 ELSE 0 END) as total_sixes
    FROM {table}
    %s
    GROUP BY venue
    HAVING COUNT(DISTINCT match_id) >= %d
    ORDER BY matches DESC
  ", where_sql, min_matches_filter)

  result <- tryCatch({
    query_remote_parquet(table_name, sql_template)
  }, error = function(e) {
    cli::cli_abort("Remote query failed: {e$message}")
  })

  if (nrow(result) == 0) {
    cli::cli_warn("No venue data found for the specified filters")
    return(data.frame())
  }

  # Innings breakdown query
  innings_sql <- sprintf("
    SELECT
      venue,
      innings,
      ROUND(AVG(innings_total), 0) as avg_score
    FROM (
      SELECT
        venue,
        match_id,
        innings,
        SUM(runs_total) as innings_total
      FROM {table}
      %s
      GROUP BY venue, match_id, innings
    )
    WHERE innings IN (1, 2)
    GROUP BY venue, innings
  ", where_sql)

  innings_stats <- tryCatch({
    query_remote_parquet(table_name, innings_sql)
  }, error = function(e) {
    cli::cli_warn("Could not retrieve innings breakdown: {e$message}")
    data.frame()
  })

  calculate_venue_metrics(result, innings_stats)
}


#' @keywords internal
calculate_venue_metrics <- function(result, innings_stats) {
  # Calculate derived metrics
  result$runs_per_ball <- round(result$total_runs / result$total_balls, 4)
  result$wickets_per_ball <- round(result$total_wickets / result$total_balls, 4)
  result$run_rate <- round((result$total_runs / result$total_balls) * 6, 2)
  result$wickets_per_match <- round(result$total_wickets / result$matches, 1)
  result$boundary_rate <- round(((result$total_fours + result$total_sixes) / result$total_balls) * 100, 2)
  result$avg_runs_per_match <- round(result$total_runs / result$matches, 0)
  result$sixes_per_match <- round(result$total_sixes / result$matches, 1)

  # Add innings breakdown if available
  if (nrow(innings_stats) > 0) {
    first_inn <- innings_stats[innings_stats$innings == 1, c("venue", "avg_score")]
    names(first_inn)[2] <- "avg_first_innings"
    second_inn <- innings_stats[innings_stats$innings == 2, c("venue", "avg_score")]
    names(second_inn)[2] <- "avg_second_innings"

    result <- merge(result, first_inn, by = "venue", all.x = TRUE)
    result <- merge(result, second_inn, by = "venue", all.x = TRUE)
    result$avg_first_innings <- round(result$avg_first_innings, 0)
    result$avg_second_innings <- round(result$avg_second_innings, 0)
  }

  # Reorder columns and sort by matches descending
  cols <- c("venue", "city", "matches", "avg_first_innings", "avg_second_innings",
            "runs_per_ball", "wickets_per_ball", "run_rate", "wickets_per_match",
            "boundary_rate", "sixes_per_match")
  cols <- cols[cols %in% names(result)]
  result <- result[, cols]
  result <- result[order(result$matches, decreasing = TRUE), ]
  rownames(result) <- NULL

  cli::cli_alert_success("Loaded stats for {nrow(result)} venue(s)")
  result
}
