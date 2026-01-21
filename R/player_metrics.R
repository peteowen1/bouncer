# Player Metrics and Analytics Functions

#' Player Batting Stats
#'
#' Get batting statistics for one or all players. Includes average, strike rate,
#' boundary percentage, and derived metrics.
#'
#' @param player_id Character. Player identifier (e.g., "V Kohli", "BA Stokes").
#'   If NULL (default), returns stats for all players.
#' @param match_type Character. Filter by match type: "T20", "ODI", "Test",
#'   "IT20", "MDM", or NULL for all formats.
#' @param season Character. Filter by season (e.g., "2023", "2023/24").
#' @param min_balls Integer. Minimum balls faced to include (default 100).
#'   Only applies when player_id is NULL.
#' @param source Character. "local" (default) uses local DuckDB for fast SQL
#'   aggregation. "remote" loads data from GitHub releases and aggregates in R.
#' @param db_path Character. Database path (only used when source = "local").
#'
#' @return Data frame with batting metrics:
#'   - batter_id, player_name, balls_faced, runs_scored, dismissals, fours, sixes
#'   - batting_average, strike_rate, wicket_pct
#'
#' @export
#' @examples
#' \dontrun{
#' # Get all players' batting stats
#' all_batters <- player_batting_stats()
#'
#' # Get stats for a specific player
#' kohli_stats <- player_batting_stats("V Kohli")
#'
#' # Get T20 stats for all players
#' t20_batters <- player_batting_stats(match_type = "T20")
#'
#' # Get T20 stats for a specific player
#' kohli_t20 <- player_batting_stats("V Kohli", match_type = "T20")
#'
#' # Get stats from GitHub releases (no local install needed)
#' remote_stats <- player_batting_stats(match_type = "T20", source = "remote")
#' }
player_batting_stats <- function(player_id = NULL,
                                  match_type = NULL,
                                  season = NULL,
                                  min_balls = 100,
                                  source = c("local", "remote"),
                                  db_path = NULL) {

  source <- match.arg(source)

  if (source == "local") {
    # Use query_batter_stats which supports NULL player_id for all players
    batting <- query_batter_stats(
      batter_id = player_id,
      match_type = match_type,
      season = season,
      min_balls = if (is.null(player_id)) min_balls else 0,
      db_path = db_path
    )
  } else {
    # Remote: load data and aggregate in R
    batting <- aggregate_batting_stats_remote(
      player_id = player_id,
      match_type = match_type,
      season = season,
      min_balls = min_balls
    )
  }

  if (nrow(batting) == 0) {
    cli::cli_warn("No batting data found for the specified filters")
    return(data.frame())
  }

  batting
}


#' @rdname player_batting_stats
#' @export
calculate_player_batting_stats <- player_batting_stats


#' Player Bowling Stats
#'
#' Get bowling statistics for one or all players. Includes average, economy rate,
#' strike rate, and derived metrics.
#'
#' @param player_id Character. Player identifier (e.g., "JJ Bumrah", "Rashid Khan").
#'   If NULL (default), returns stats for all players.
#' @param match_type Character. Filter by match type: "T20", "ODI", "Test",
#'   "IT20", "MDM", or NULL for all formats.
#' @param season Character. Filter by season (e.g., "2023", "2023/24").
#' @param min_balls Integer. Minimum balls bowled to include (default 100).
#'   Only applies when player_id is NULL.
#' @param source Character. "local" (default) uses local DuckDB for fast SQL
#'   aggregation. "remote" loads data from GitHub releases and aggregates in R.
#' @param db_path Character. Database path (only used when source = "local").
#'
#' @return Data frame with bowling metrics:
#'   - bowler_id, player_name, balls_bowled, runs_conceded, wickets
#'   - bowling_average, economy_rate, strike_rate, wicket_pct
#'
#' @export
#' @examples
#' \dontrun{
#' # Get all players' bowling stats
#' all_bowlers <- player_bowling_stats()
#'
#' # Get stats for a specific player
#' bumrah_stats <- player_bowling_stats("JJ Bumrah")
#'
#' # Get T20 bowling stats for all players
#' t20_bowlers <- player_bowling_stats(match_type = "T20")
#'
#' # Get Test bowling stats for a specific player
#' anderson_tests <- player_bowling_stats("JM Anderson", match_type = "Test")
#'
#' # Get stats from GitHub releases (no local install needed)
#' remote_stats <- player_bowling_stats(match_type = "T20", source = "remote")
#' }
player_bowling_stats <- function(player_id = NULL,
                                  match_type = NULL,
                                  season = NULL,
                                  min_balls = 100,
                                  source = c("local", "remote"),
                                  db_path = NULL) {

  source <- match.arg(source)

  if (source == "local") {
    # Use query_bowler_stats which supports NULL player_id for all players
    bowling <- query_bowler_stats(
      bowler_id = player_id,
      match_type = match_type,
      season = season,
      min_balls = if (is.null(player_id)) min_balls else 0,
      db_path = db_path
    )
  } else {
    # Remote: load data and aggregate in R
    bowling <- aggregate_bowling_stats_remote(
      player_id = player_id,
      match_type = match_type,
      season = season,
      min_balls = min_balls
    )
  }

  if (nrow(bowling) == 0) {
    cli::cli_warn("No bowling data found for the specified filters")
    return(data.frame())
  }

  bowling
}


#' @rdname player_bowling_stats
#' @export
calculate_player_bowling_stats <- player_bowling_stats


#' Analyze Batter vs Bowler Matchup
#'
#' Analyzes historical head-to-head between a batter and bowler.
#'
#' @param batter_id Character. Batter identifier
#' @param bowler_id Character. Bowler identifier
#' @param match_type Character. Filter by match type
#' @param db_path Character. Database path
#'
#' @return List with matchup statistics and prediction
#' @keywords internal
analyze_batter_vs_bowler <- function(batter_id,
                                      bowler_id,
                                      match_type = NULL,
                                      db_path = NULL) {

  # Get historical deliveries
  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  where_clauses <- c("batter_id = ?", "bowler_id = ?")
  params <- list(batter_id, bowler_id)

  if (!is.null(match_type)) {
    match_type <- normalize_match_type(match_type)
    where_clauses <- c(where_clauses, "LOWER(match_type) = ?")
    params <- c(params, list(match_type))
  }

  query <- sprintf("
    SELECT
      COUNT(*) as balls_faced,
      SUM(runs_batter) as runs_scored,
      SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as dismissals,
      SUM(CASE WHEN is_four THEN 1 ELSE 0 END) as fours,
      SUM(CASE WHEN is_six THEN 1 ELSE 0 END) as sixes,
      AVG(runs_batter) as avg_runs_per_ball
    FROM deliveries
    WHERE %s
  ", paste(where_clauses, collapse = " AND "))

  historical <- DBI::dbGetQuery(conn, query, params = params)

  # Calculate metrics
  strike_rate <- if (historical$balls_faced > 0) {
    (historical$runs_scored / historical$balls_faced) * 100
  } else {
    0
  }

  dismissal_rate <- if (historical$balls_faced > 0) {
    (historical$dismissals / historical$balls_faced) * 100
  } else {
    0
  }

  # Get current prediction
  prediction <- predict_matchup_outcome(
    batter_id,
    bowler_id,
    context = list(match_type = match_type %||% "t20"),
    db_path = db_path
  )

  list(
    batter_id = batter_id,
    bowler_id = bowler_id,
    match_type = match_type %||% "all",
    historical = list(
      balls_faced = historical$balls_faced,
      runs_scored = historical$runs_scored,
      dismissals = historical$dismissals,
      strike_rate = round(strike_rate, 2),
      dismissal_rate = round(dismissal_rate, 2),
      boundaries = historical$fours + historical$sixes
    ),
    prediction = prediction
  )
}


#' Rank Players by ELO
#'
#' Returns top players ranked by ELO rating.
#'
#' @param rating_type Character. "batting" or "bowling"
#' @param match_type Character. Match type filter
#' @param top_n Integer. Number of players to return
#' @param db_path Character. Database path
#'
#' @return Data frame with player rankings
#' @keywords internal
rank_players <- function(rating_type = "batting",
                         match_type = "all",
                         top_n = 10,
                         db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Determine ELO column
  if (match_type == "all") {
    elo_col <- if (rating_type == "batting") "elo_batting" else "elo_bowling"
  } else {
    match_type <- normalize_match_type(match_type)
    elo_col <- sprintf("elo_%s_%s", rating_type, match_type)
  }

  # Get latest ELO for each player
  query <- sprintf("
    WITH latest_elos AS (
      SELECT
        player_id,
        %s as elo_rating,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY match_date DESC) as rn
      FROM player_elo_history
      WHERE %s IS NOT NULL
    )
    SELECT
      player_id,
      elo_rating,
      match_date as last_match_date
    FROM latest_elos
    WHERE rn = 1
    ORDER BY elo_rating DESC
    LIMIT ?
  ", elo_col, elo_col)

  result <- DBI::dbGetQuery(conn, query, params = list(top_n))

  result$rank <- seq_len(nrow(result))
  result <- result[, c("rank", "player_id", "elo_rating", "last_match_date")]

  return(result)
}


# Internal helper functions for remote aggregation --------------------------------

#' Aggregate Batting Stats from Remote Data
#'
#' Uses fast "Download + DuckDB SQL" approach (~7x faster than httpfs).
#' Downloads parquet to temp file, runs SQL aggregation, returns results.
#'
#' @param player_id Character. Player ID to filter (NULL for all players)
#' @param match_type Character. Match type filter (e.g., "T20", "ODI", "Test")
#' @param season Character. Season filter
#' @param min_balls Integer. Minimum balls faced
#' @return Data frame with batting stats
#' @keywords internal
aggregate_batting_stats_remote <- function(player_id = NULL,
                                            match_type = NULL,
                                            season = NULL,
                                            min_balls = 100) {

  # Determine which parquet file(s) to query
  if (is.null(match_type) || match_type == "all") {
    table_name <- "deliveries_T20_male"
    cli::cli_alert_info("Querying T20 male deliveries (specify match_type for other formats)...")
  } else {
    table_name <- paste0("deliveries_", match_type, "_male")
    cli::cli_alert_info("Querying {table_name}...")
  }

  # Build WHERE clause for filters
  where_clauses <- character()

  if (!is.null(player_id)) {
    where_clauses <- c(where_clauses, sprintf("batter_id = '%s'", player_id))
  }
  if (!is.null(season)) {
    where_clauses <- c(where_clauses, sprintf("season = '%s'", season))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  # Build SQL query with aggregation
  # Note: dismissals count requires checking if batter was the one dismissed
  sql_template <- sprintf("
    SELECT
      batter_id,
      COUNT(*) as balls_faced,
      SUM(runs_batter) as runs_scored,
      SUM(CASE WHEN is_wicket AND player_out_id = batter_id THEN 1 ELSE 0 END) as dismissals,
      SUM(CASE WHEN runs_batter = 0 THEN 1 ELSE 0 END) as dots,
      SUM(CASE WHEN is_four THEN 1 ELSE 0 END) as fours,
      SUM(CASE WHEN is_six THEN 1 ELSE 0 END) as sixes
    FROM {table}
    %s
    GROUP BY batter_id
    HAVING COUNT(*) >= %d
    ORDER BY runs_scored DESC
  ", where_sql, min_balls)

  # Execute fast remote query
  batting <- tryCatch({
    query_remote_parquet(table_name, sql_template)
  }, error = function(e) {
    cli::cli_alert_warning("Fast query failed, falling back to httpfs: {e$message}")
    return(NULL)
  })

  # Fallback to old method if fast query fails
  if (is.null(batting)) {
    return(aggregate_batting_stats_remote_legacy(player_id, match_type, season, min_balls))
  }

  if (nrow(batting) == 0) {
    return(data.frame())
  }

  # Load players for name lookup (small file, fast)
  players <- tryCatch({
    query_remote_parquet("players", "SELECT player_id, player_name FROM {table}")
  }, error = function(e) {
    data.frame(player_id = character(), player_name = character())
  })

  # Join with players for names
  batting <- dplyr::left_join(
    batting,
    players,
    by = c("batter_id" = "player_id")
  )

  # Reorder columns
  batting <- batting[, c("batter_id", "player_name", "balls_faced", "runs_scored",
                          "dismissals", "dots", "fours", "sixes")]

  # Calculate derived metrics
  batting$batting_average <- ifelse(batting$dismissals > 0,
                                     round(batting$runs_scored / batting$dismissals, 2),
                                     NA_real_)
  batting$runs_per_ball <- round(batting$runs_scored / batting$balls_faced, 4)
  batting$wickets_per_ball <- round(batting$dismissals / batting$balls_faced, 4)
  batting$strike_rate <- round((batting$runs_scored / batting$balls_faced) * 100, 2)
  batting$wicket_pct <- round((batting$dismissals / batting$balls_faced) * 100, 4)

  cli::cli_alert_success("Aggregated {nrow(batting)} batters")
  as.data.frame(batting)
}


#' Legacy Batting Stats Aggregation (fallback)
#' @keywords internal
aggregate_batting_stats_remote_legacy <- function(player_id = NULL,
                                                   match_type = NULL,
                                                   season = NULL,
                                                   min_balls = 100) {

  cli::cli_alert_info("Loading deliveries from remote (legacy method)...")
  deliveries <- load_deliveries(
    match_type = match_type %||% "all",
    source = "remote"
  )

  if (nrow(deliveries) == 0) return(data.frame())

  if (!is.null(season)) {
    deliveries <- deliveries[deliveries$season == as.character(season), ]
  }
  if (!is.null(player_id)) {
    deliveries <- deliveries[deliveries$batter_id == player_id, ]
  }
  if (nrow(deliveries) == 0) return(data.frame())

  players <- load_players(source = "remote")

  batting <- deliveries |>
    dplyr::group_by(batter_id) |>
    dplyr::summarise(
      balls_faced = dplyr::n(),
      runs_scored = sum(runs_batter, na.rm = TRUE),
      dismissals = sum(is_wicket & player_out_id == batter_id, na.rm = TRUE),
      dots = sum(runs_batter == 0, na.rm = TRUE),
      fours = sum(is_four, na.rm = TRUE),
      sixes = sum(is_six, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::filter(balls_faced >= min_balls) |>
    dplyr::arrange(dplyr::desc(runs_scored))

  batting <- dplyr::left_join(
    batting,
    players[, c("player_id", "player_name")],
    by = c("batter_id" = "player_id")
  )

  batting <- batting[, c("batter_id", "player_name", "balls_faced", "runs_scored",
                          "dismissals", "dots", "fours", "sixes")]

  batting$batting_average <- ifelse(batting$dismissals > 0,
                                     round(batting$runs_scored / batting$dismissals, 2),
                                     NA_real_)
  batting$runs_per_ball <- round(batting$runs_scored / batting$balls_faced, 4)
  batting$wickets_per_ball <- round(batting$dismissals / batting$balls_faced, 4)
  batting$strike_rate <- round((batting$runs_scored / batting$balls_faced) * 100, 2)
  batting$wicket_pct <- round((batting$dismissals / batting$balls_faced) * 100, 4)

  as.data.frame(batting)
}


#' Aggregate Bowling Stats from Remote Data
#'
#' Uses fast "Download + DuckDB SQL" approach (~7x faster than httpfs).
#' Downloads parquet to temp file, runs SQL aggregation, returns results.
#'
#' @param player_id Character. Player ID to filter (NULL for all players)
#' @param match_type Character. Match type filter (e.g., "T20", "ODI", "Test")
#' @param season Character. Season filter
#' @param min_balls Integer. Minimum balls bowled
#' @return Data frame with bowling stats
#' @keywords internal
aggregate_bowling_stats_remote <- function(player_id = NULL,
                                            match_type = NULL,
                                            season = NULL,
                                            min_balls = 100) {

  # Determine which parquet file(s) to query
  # File naming: deliveries_{match_type}_{gender}.parquet
  if (is.null(match_type) || match_type == "all") {
    # For "all", use T20 male as default (largest dataset)
    # Could be expanded to query multiple files
    table_name <- "deliveries_T20_male"
    cli::cli_alert_info("Querying T20 male deliveries (specify match_type for other formats)...")
  } else {
    # Map match_type to file name (assume male for now)
    table_name <- paste0("deliveries_", match_type, "_male")
    cli::cli_alert_info("Querying {table_name}...")
  }

  # Build WHERE clause for filters
  where_clauses <- character()

  if (!is.null(player_id)) {
    where_clauses <- c(where_clauses, sprintf("bowler_id = '%s'", player_id))
  }
  if (!is.null(season)) {
    where_clauses <- c(where_clauses, sprintf("season = '%s'", season))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  # Build SQL query with aggregation (runs entirely in DuckDB)
  sql_template <- sprintf("
    SELECT
      bowler_id,
      COUNT(*) as balls_bowled,
      SUM(runs_total) as runs_conceded,
      SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets,
      SUM(CASE WHEN runs_total = 0 THEN 1 ELSE 0 END) as dots
    FROM {table}
    %s
    GROUP BY bowler_id
    HAVING COUNT(*) >= %d
    ORDER BY wickets DESC
  ", where_sql, min_balls)

  # Execute fast remote query
  bowling <- tryCatch({
    query_remote_parquet(table_name, sql_template)
  }, error = function(e) {
    cli::cli_alert_warning("Fast query failed, falling back to httpfs: {e$message}")
    return(NULL)
  })

  # Fallback to old method if fast query fails
 if (is.null(bowling)) {
    return(aggregate_bowling_stats_remote_legacy(player_id, match_type, season, min_balls))
  }

  if (nrow(bowling) == 0) {
    return(data.frame())
  }

  # Load players for name lookup (small file, fast)
  players <- tryCatch({
    query_remote_parquet("players", "SELECT player_id, player_name FROM {table}")
  }, error = function(e) {
    data.frame(player_id = character(), player_name = character())
  })

  # Join with players for names
  bowling <- dplyr::left_join(
    bowling,
    players,
    by = c("bowler_id" = "player_id")
  )

  # Reorder columns
  bowling <- bowling[, c("bowler_id", "player_name", "balls_bowled", "runs_conceded",
                          "wickets", "dots")]

  # Calculate derived metrics
  bowling$bowling_average <- ifelse(bowling$wickets > 0,
                                     round(bowling$runs_conceded / bowling$wickets, 2),
                                     NA_real_)
  bowling$runs_per_ball <- round(bowling$runs_conceded / bowling$balls_bowled, 4)
  bowling$wickets_per_ball <- round(bowling$wickets / bowling$balls_bowled, 4)
  bowling$economy_rate <- round((bowling$runs_conceded / bowling$balls_bowled) * 6, 2)
  bowling$wicket_pct <- round((bowling$wickets / bowling$balls_bowled) * 100, 4)
  bowling$strike_rate <- ifelse(bowling$wickets > 0,
                                 round(bowling$balls_bowled / bowling$wickets, 2),
                                 NA_real_)

  cli::cli_alert_success("Aggregated {nrow(bowling)} bowlers")
  as.data.frame(bowling)
}


#' Legacy Bowling Stats Aggregation (fallback)
#' @keywords internal
aggregate_bowling_stats_remote_legacy <- function(player_id = NULL,
                                                   match_type = NULL,
                                                   season = NULL,
                                                   min_balls = 100) {

  cli::cli_alert_info("Loading deliveries from remote (legacy method)...")
  deliveries <- load_deliveries(
    match_type = match_type %||% "all",
    source = "remote"
  )

  if (nrow(deliveries) == 0) return(data.frame())

  if (!is.null(season)) {
    deliveries <- deliveries[deliveries$season == as.character(season), ]
  }
  if (!is.null(player_id)) {
    deliveries <- deliveries[deliveries$bowler_id == player_id, ]
  }
  if (nrow(deliveries) == 0) return(data.frame())

  players <- load_players(source = "remote")

  bowling <- deliveries |>
    dplyr::group_by(bowler_id) |>
    dplyr::summarise(
      balls_bowled = dplyr::n(),
      runs_conceded = sum(runs_total, na.rm = TRUE),
      wickets = sum(is_wicket, na.rm = TRUE),
      dots = sum(runs_total == 0, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::filter(balls_bowled >= min_balls) |>
    dplyr::arrange(dplyr::desc(wickets))

  bowling <- dplyr::left_join(
    bowling,
    players[, c("player_id", "player_name")],
    by = c("bowler_id" = "player_id")
  )

  bowling <- bowling[, c("bowler_id", "player_name", "balls_bowled", "runs_conceded",
                          "wickets", "dots")]

  bowling$bowling_average <- ifelse(bowling$wickets > 0,
                                     round(bowling$runs_conceded / bowling$wickets, 2),
                                     NA_real_)
  bowling$runs_per_ball <- round(bowling$runs_conceded / bowling$balls_bowled, 4)
  bowling$wickets_per_ball <- round(bowling$wickets / bowling$balls_bowled, 4)
  bowling$economy_rate <- round((bowling$runs_conceded / bowling$balls_bowled) * 6, 2)
  bowling$wicket_pct <- round((bowling$wickets / bowling$balls_bowled) * 100, 4)
  bowling$strike_rate <- ifelse(bowling$wickets > 0,
                                 round(bowling$balls_bowled / bowling$wickets, 2),
                                 NA_real_)

  as.data.frame(bowling)
}
