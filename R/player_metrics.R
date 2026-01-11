# Player Metrics and Analytics Functions

#' Calculate Player Batting Stats
#'
#' Calculates comprehensive batting statistics for a player including
#' average, strike rate, boundary percentage, and current ELO rating.
#'
#' @param player_id Character. Player identifier (e.g., "V Kohli", "BA Stokes").
#'   Use the format from Cricsheet data (typically "First Initial + Surname").
#' @param match_type Character. Filter by match type: "T20", "ODI", "Test",
#'   "IT20", "MDM", or NULL for all formats.
#' @param season Character. Filter by season (e.g., "2023", "2023/24").
#' @param db_path Character. Database path. If NULL, uses default location.
#'
#' @return Data frame with batting metrics:
#'   - balls_faced, runs_scored, dismissals, fours, sixes
#'   - batting_average, strike_rate, boundary_percentage
#'   - elo_batting (current rating)
#'
#' @export
#' @examples
#' \dontrun{
#' # Get overall batting stats for a player
#' kohli_stats <- calculate_player_batting_stats("V Kohli")
#'
#' # Get T20 stats only
#' kohli_t20 <- calculate_player_batting_stats("V Kohli", match_type = "T20")
#'
#' # Get stats for a specific season
#' stokes_2023 <- calculate_player_batting_stats("BA Stokes", season = "2023")
#'
#' # Get Test stats for a player
#' root_tests <- calculate_player_batting_stats("JE Root", match_type = "Test")
#' }
calculate_player_batting_stats <- function(player_id,
                                            match_type = NULL,
                                            season = NULL,
                                            db_path = NULL) {

  # Get raw stats
  stats <- query_player_stats(player_id, match_type, season, db_path)
  batting <- stats$batting

  # Calculate derived metrics
  batting$batting_average <- if (batting$dismissals > 0) {
    batting$runs_scored / batting$dismissals
  } else {
    batting$runs_scored  # Not out average
  }

  batting$strike_rate <- if (batting$balls_faced > 0) {
    (batting$runs_scored / batting$balls_faced) * 100
  } else {
    0
  }

  batting$boundary_percentage <- if (batting$balls_faced > 0) {
    ((batting$fours + batting$sixes) / batting$balls_faced) * 100
  } else {
    0
  }

  batting$dot_ball_percentage <- if (batting$balls_faced > 0) {
    balls_without_runs <- batting$balls_faced - (batting$fours + batting$sixes +
                          (batting$runs_scored - batting$fours * 4 - batting$sixes * 6))
    (balls_without_runs / batting$balls_faced) * 100
  } else {
    0
  }

  # Get ELO rating
  elo_info <- get_player_elo(player_id, match_type %||% "all", db_path = db_path)

  # Combine
  result <- data.frame(
    player_id = player_id,
    match_type = match_type %||% "all",
    season = season %||% "all",
    balls_faced = batting$balls_faced,
    runs_scored = batting$runs_scored,
    dismissals = batting$dismissals,
    fours = batting$fours,
    sixes = batting$sixes,
    batting_average = round(batting$batting_average, 2),
    strike_rate = round(batting$strike_rate, 2),
    boundary_percentage = round(batting$boundary_percentage, 2),
    elo_batting = elo_info$batting_elo,
    stringsAsFactors = FALSE
  )

  return(result)
}


#' Calculate Player Bowling Stats
#'
#' Calculates comprehensive bowling statistics for a player including
#' average, economy rate, strike rate, and current ELO rating.
#'
#' @inheritParams calculate_player_batting_stats
#'
#' @return Data frame with bowling metrics:
#'   - balls_bowled, runs_conceded, wickets
#'   - bowling_average, economy_rate, strike_rate
#'   - elo_bowling (current rating)
#'
#' @export
#' @examples
#' \dontrun{
#' # Get overall bowling stats
#' bumrah_stats <- calculate_player_bowling_stats("JJ Bumrah")
#'
#' # Get T20 bowling stats
#' rashid_t20 <- calculate_player_bowling_stats("Rashid Khan", match_type = "T20")
#'
#' # Get Test bowling stats
#' anderson_tests <- calculate_player_bowling_stats("JM Anderson", match_type = "Test")
#'
#' # Get stats for specific season
#' cummins_2023 <- calculate_player_bowling_stats("PJ Cummins", season = "2023")
#' }
calculate_player_bowling_stats <- function(player_id,
                                            match_type = NULL,
                                            season = NULL,
                                            db_path = NULL) {

  # Get raw stats
  stats <- query_player_stats(player_id, match_type, season, db_path)
  bowling <- stats$bowling

  # Calculate derived metrics
  bowling$bowling_average <- if (bowling$wickets > 0) {
    bowling$runs_conceded / bowling$wickets
  } else {
    NA_real_
  }

  bowling$economy_rate <- if (bowling$balls_bowled > 0) {
    (bowling$runs_conceded / bowling$balls_bowled) * 6  # Per over
  } else {
    0
  }

  bowling$strike_rate <- if (bowling$wickets > 0) {
    bowling$balls_bowled / bowling$wickets
  } else {
    NA_real_
  }

  # Get ELO rating
  elo_info <- get_player_elo(player_id, match_type %||% "all", db_path = db_path)

  # Combine
  result <- data.frame(
    player_id = player_id,
    match_type = match_type %||% "all",
    season = season %||% "all",
    balls_bowled = bowling$balls_bowled,
    runs_conceded = bowling$runs_conceded,
    wickets = bowling$wickets,
    bowling_average = round(bowling$bowling_average, 2),
    economy_rate = round(bowling$economy_rate, 2),
    strike_rate = round(bowling$strike_rate, 2),
    elo_bowling = elo_info$bowling_elo,
    stringsAsFactors = FALSE
  )

  return(result)
}


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
