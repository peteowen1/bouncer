# Team ELO Rating System for Cricket
#
# This module provides team-level ELO calculations, supporting both:
# - Result-based ELO: Traditional ELO updated on match wins/losses
# - Roster-based ELO: Aggregated from player ELOs of the likely XI

# Team ELO constants
TEAM_ELO_START <- 1500
TEAM_K_FACTOR <- 32


#' Normalize Match Format
#'
#' Converts various match type strings to standardized format names
#' for looking up the correct ELO and skill tables.
#'
#' @param match_type Character. Raw match type from database
#'
#' @return Character. Standardized format: "t20", "odi", or "test"
#' @keywords internal
normalize_match_format <- function(match_type) {
  format <- tolower(match_type %||% "t20")

  if (format %in% c("t20", "it20", "t20i")) {
    return("t20")
  } else if (format %in% c("odi", "odm", "oda")) {
    return("odi")
  } else if (format %in% c("test", "mdm")) {
    return("test")
  } else {
    # Default to t20 for unknown formats
    return("t20")
  }
}


#' Get Format-Specific Table Name
#'
#' Returns the appropriate table name for player ratings based on match format.
#'
#' @param base_table Character. Base table name (e.g., "player_elo", "player_skill")
#' @param match_type Character. Match type from database
#'
#' @return Character. Full table name (e.g., "t20_player_elo")
#' @keywords internal
get_format_table <- function(base_table, match_type) {
  format <- normalize_match_format(match_type)
  paste0(format, "_", base_table)
}


#' Get Likely Playing XI
#'
#' Infers the most likely playing XI for a team based on recent match participation.
#' Uses the players who have appeared most frequently in the team's last N matches.
#'
#' @param team Character. Team name
#' @param as_of_date Date. Date to look back from (exclusive - uses matches before this date)
#' @param conn DBI connection. Database connection
#' @param n_recent_matches Integer. Number of recent matches to consider (default 3)
#' @param match_type Character. Filter by match type (optional)
#' @param event_name Character. Filter by event name (optional)
#'
#' @return Data frame with player_id and appearance_count for likely XI (top 11 by appearances)
#' @keywords internal
get_likely_playing_xi <- function(team, as_of_date, conn,
                                   n_recent_matches = 3,
                                   match_type = NULL,
                                   event_name = NULL) {

  # Ensure as_of_date is properly formatted
  as_of_date_str <- as.character(as.Date(as_of_date))

  # Build query to find recent matches for this team
  match_query <- "
    SELECT DISTINCT match_id, match_date
    FROM matches
    WHERE (team1 = ? OR team2 = ?)
      AND match_date < CAST(? AS DATE)
  "
  params <- list(team, team, as_of_date_str)

  if (!is.null(match_type)) {
    match_query <- paste0(match_query, " AND LOWER(match_type) = LOWER(?)")
    params <- c(params, match_type)
  }

  if (!is.null(event_name)) {
    match_query <- paste0(match_query, " AND event_name LIKE ?")
    params <- c(params, paste0("%", event_name, "%"))
  }

  match_query <- paste0(match_query, "
    ORDER BY match_date DESC
    LIMIT ?
  ")
  params <- c(params, n_recent_matches)

  recent_matches <- DBI::dbGetQuery(conn, match_query, params = params)

  if (nrow(recent_matches) == 0) {
    return(data.frame(player_id = character(0), appearance_count = integer(0)))
  }

  # Get players who batted or bowled for this team in those matches
  match_ids <- recent_matches$match_id
  placeholders <- paste(rep("?", length(match_ids)), collapse = ", ")

  player_query <- sprintf("
    SELECT player_id, COUNT(DISTINCT match_id) as appearance_count
    FROM (
      SELECT batter_id as player_id, match_id
      FROM deliveries
      WHERE match_id IN (%s)
        AND batting_team = ?

      UNION ALL

      SELECT bowler_id as player_id, match_id
      FROM deliveries
      WHERE match_id IN (%s)
        AND bowling_team = ?
    )
    GROUP BY player_id
    ORDER BY appearance_count DESC, player_id
    LIMIT 11
  ", placeholders, placeholders)

  params <- c(as.list(match_ids), team, as.list(match_ids), team)
  likely_xi <- DBI::dbGetQuery(conn, player_query, params = params)

  return(likely_xi)
}


#' Calculate Team Roster ELO
#'
#' Calculates a team's ELO rating by aggregating player ELOs from the likely XI.
#' Returns batting, bowling, and combined ELO.
#'
#' @param team Character. Team name
#' @param as_of_date Date. Date to calculate ELO for (uses data before this date)
#' @param conn DBI connection. Database connection
#' @param match_type Character. Filter by match type (optional)
#' @param event_name Character. Filter by event name (optional)
#'
#' @return List with elo_batting, elo_bowling, elo_combined, and player_count
#' @keywords internal
calculate_team_roster_elo <- function(team, as_of_date, conn,
                                       match_type = NULL,
                                       event_name = NULL) {

  # Get likely XI
  likely_xi <- get_likely_playing_xi(team, as_of_date, conn,
                                      n_recent_matches = 3,
                                      match_type = match_type,
                                      event_name = event_name)

  if (nrow(likely_xi) == 0) {
    # No data, return starting ELO
    return(list(
      elo_batting = TEAM_ELO_START,
      elo_bowling = TEAM_ELO_START,
      elo_combined = TEAM_ELO_START,
      player_count = 0
    ))
  }

  # Determine format-specific ELO table
  format <- normalize_match_format(match_type)
  elo_table <- get_format_table("player_elo", match_type)

  # Check if format-specific table exists, otherwise fallback to deliveries
  if (!elo_table %in% DBI::dbListTables(conn)) {
    elo_table <- NULL
  }

  player_ids <- likely_xi$player_id
  placeholders <- paste(rep("?", length(player_ids)), collapse = ", ")
  as_of_date_str <- as.character(as.Date(as_of_date))

  # Use t20_player_elo table if available for T20 matches
  if (!is.null(elo_table) && elo_table %in% DBI::dbListTables(conn)) {

    # Query for latest batter ELOs from dual ELO table
    # Use average of run ELO and wicket ELO for combined batting strength
    batting_query <- sprintf("
      WITH ranked AS (
        SELECT
          batter_id as player_id,
          batter_run_elo_after,
          batter_wicket_elo_after,
          ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
        FROM %s
        WHERE batter_id IN (%s)
          AND match_date < CAST(? AS DATE)
          AND batter_run_elo_after IS NOT NULL
      )
      SELECT
        player_id,
        (batter_run_elo_after + batter_wicket_elo_after) / 2 as elo
      FROM ranked
      WHERE rn = 1
    ", elo_table, placeholders)

    batting_elos <- tryCatch({
      DBI::dbGetQuery(conn, batting_query, params = c(as.list(player_ids), as_of_date_str))
    }, error = function(e) {
      data.frame(player_id = character(0), elo = numeric(0))
    })

    # Query for latest bowler ELOs from dual ELO table
    bowling_query <- sprintf("
      WITH ranked AS (
        SELECT
          bowler_id as player_id,
          bowler_run_elo_after,
          bowler_wicket_elo_after,
          ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
        FROM %s
        WHERE bowler_id IN (%s)
          AND match_date < CAST(? AS DATE)
          AND bowler_run_elo_after IS NOT NULL
      )
      SELECT
        player_id,
        (bowler_run_elo_after + bowler_wicket_elo_after) / 2 as elo
      FROM ranked
      WHERE rn = 1
    ", elo_table, placeholders)

    bowling_elos <- tryCatch({
      DBI::dbGetQuery(conn, bowling_query, params = c(as.list(player_ids), as_of_date_str))
    }, error = function(e) {
      data.frame(player_id = character(0), elo = numeric(0))
    })

  } else {
    # Legacy fallback: query deliveries table (for non-T20 or if table missing)
    batting_query <- sprintf("
      SELECT batter_id as player_id, batter_elo_after as elo
      FROM deliveries
      WHERE batter_id IN (%s)
        AND match_date < CAST(? AS DATE)
        AND batter_elo_after IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) = 1
    ", placeholders)

    batting_elos <- tryCatch({
      DBI::dbGetQuery(conn, batting_query, params = c(as.list(player_ids), as_of_date_str))
    }, error = function(e) {
      data.frame(player_id = character(0), elo = numeric(0))
    })

    bowling_query <- sprintf("
      SELECT bowler_id as player_id, bowler_elo_after as elo
      FROM deliveries
      WHERE bowler_id IN (%s)
        AND match_date < CAST(? AS DATE)
        AND bowler_elo_after IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) = 1
    ", placeholders)

    bowling_elos <- tryCatch({
      DBI::dbGetQuery(conn, bowling_query, params = c(as.list(player_ids), as_of_date_str))
    }, error = function(e) {
      data.frame(player_id = character(0), elo = numeric(0))
    })
  }

  # Calculate average ELOs (use starting ELO for players without history)
  batting_elo_avg <- if (nrow(batting_elos) > 0) {
    mean(batting_elos$elo, na.rm = TRUE)
  } else {
    TEAM_ELO_START
  }

  bowling_elo_avg <- if (nrow(bowling_elos) > 0) {
    mean(bowling_elos$elo, na.rm = TRUE)
  } else {
    TEAM_ELO_START
  }

  # Combined ELO is average of batting and bowling
  combined_elo <- (batting_elo_avg + bowling_elo_avg) / 2

  return(list(
    elo_batting = batting_elo_avg,
    elo_bowling = bowling_elo_avg,
    elo_combined = combined_elo,
    player_count = nrow(likely_xi)
  ))
}


#' Calculate Team Result ELO Update
#'
#' Calculates the ELO update for a team based on match result.
#' Uses standard ELO formula with team-level K-factor.
#'
#' @param team_elo Numeric. Team's current ELO rating
#' @param opponent_elo Numeric. Opponent's current ELO rating
#' @param won Logical. Whether the team won the match
#' @param k_factor Numeric. K-factor for the update (default TEAM_K_FACTOR)
#'
#' @return Numeric. New ELO rating after the match
#' @keywords internal
calculate_team_result_elo_update <- function(team_elo, opponent_elo, won,
                                              k_factor = TEAM_K_FACTOR) {
  # Calculate expected outcome
  expected <- calculate_expected_outcome(team_elo, opponent_elo)

  # Actual outcome (1 for win, 0 for loss)
  actual <- if (won) 1.0 else 0.0

  # Calculate new ELO
  new_elo <- calculate_elo_update(team_elo, expected, actual, k_factor)

  return(new_elo)
}


#' Get Team ELO
#'
#' Retrieves a team's ELO rating as of a specific date.
#'
#' @param team_id Character. Team identifier/name
#' @param as_of_date Date. Date to get ELO for (uses most recent before this date)
#' @param elo_type Character. Type of ELO: "result", "roster_batting", "roster_bowling", or "roster_combined"
#' @param conn DBI connection. Database connection
#'
#' @return Numeric. Team's ELO rating, or TEAM_ELO_START if no history
#' @export
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection()
#' elo <- get_team_elo("Mumbai Indians", as.Date("2024-04-01"), "result", conn)
#' }
get_team_elo <- function(team_id, as_of_date, elo_type = "result", conn) {

  elo_column <- switch(elo_type,
    "result" = "elo_result",
    "roster_batting" = "elo_roster_batting",
    "roster_bowling" = "elo_roster_bowling",
    "roster_combined" = "elo_roster_combined",
    "elo_result"  # default
  )

  # Ensure as_of_date is properly formatted
  as_of_date_str <- as.character(as.Date(as_of_date))

  query <- sprintf("
    SELECT %s as elo
    FROM team_elo
    WHERE team_id = ?
      AND match_date < CAST(? AS DATE)
      AND %s IS NOT NULL
    ORDER BY match_date DESC
    LIMIT 1
  ", elo_column, elo_column)

  result <- DBI::dbGetQuery(conn, query, params = list(team_id, as_of_date_str))

  if (nrow(result) == 0 || is.na(result$elo[1])) {
    return(TEAM_ELO_START)
  }

  return(result$elo[1])
}


#' Calculate Team Form
#'
#' Calculates a team's recent form based on win rate in last N matches.
#'
#' @param team Character. Team name
#' @param as_of_date Date. Date to look back from
#' @param n_matches Integer. Number of matches to consider (default 5)
#' @param conn DBI connection. Database connection
#' @param match_type Character. Filter by match type (optional)
#' @param event_name Character. Filter by event name (optional)
#'
#' @return Numeric. Win rate (0 to 1), or NA if no matches found
#' @keywords internal
calculate_team_form <- function(team, as_of_date, n_matches = 5, conn,
                                 match_type = NULL, event_name = NULL) {

  # Ensure as_of_date is properly formatted
  as_of_date_str <- as.character(as.Date(as_of_date))

  query <- "
    SELECT outcome_winner
    FROM matches
    WHERE (team1 = ? OR team2 = ?)
      AND match_date < CAST(? AS DATE)
      AND outcome_winner IS NOT NULL
      AND outcome_winner != ''
  "
  params <- list(team, team, as_of_date_str)

  if (!is.null(match_type)) {
    query <- paste0(query, " AND LOWER(match_type) = LOWER(?)")
    params <- c(params, match_type)
  }

  if (!is.null(event_name)) {
    query <- paste0(query, " AND event_name LIKE ?")
    params <- c(params, paste0("%", event_name, "%"))
  }

  query <- paste0(query, "
    ORDER BY match_date DESC
    LIMIT ?
  ")
  params <- c(params, n_matches)

  results <- DBI::dbGetQuery(conn, query, params = params)

  if (nrow(results) == 0) {
    return(NA_real_)
  }

  wins <- sum(results$outcome_winner == team)
  win_rate <- wins / nrow(results)

  return(win_rate)
}


#' Calculate Head-to-Head Record
#'
#' Calculates the head-to-head record between two teams.
#'
#' @param team1 Character. First team name
#' @param team2 Character. Second team name
#' @param as_of_date Date. Date to look back from
#' @param conn DBI connection. Database connection
#' @param match_type Character. Filter by match type (optional)
#' @param event_name Character. Filter by event name (optional)
#'
#' @return List with team1_wins, team2_wins, total_matches, and team1_win_rate
#' @keywords internal
calculate_h2h_record <- function(team1, team2, as_of_date, conn,
                                  match_type = NULL, event_name = NULL) {

  # Ensure as_of_date is properly formatted
  as_of_date_str <- as.character(as.Date(as_of_date))

  query <- "
    SELECT outcome_winner
    FROM matches
    WHERE ((team1 = ? AND team2 = ?) OR (team1 = ? AND team2 = ?))
      AND match_date < CAST(? AS DATE)
      AND outcome_winner IS NOT NULL
      AND outcome_winner != ''
  "
  params <- list(team1, team2, team2, team1, as_of_date_str)

  if (!is.null(match_type)) {
    query <- paste0(query, " AND LOWER(match_type) = LOWER(?)")
    params <- c(params, match_type)
  }

  if (!is.null(event_name)) {
    query <- paste0(query, " AND event_name LIKE ?")
    params <- c(params, paste0("%", event_name, "%"))
  }

  results <- DBI::dbGetQuery(conn, query, params = params)

  if (nrow(results) == 0) {
    return(list(
      team1_wins = 0L,
      team2_wins = 0L,
      total_matches = 0L,
      team1_win_rate = NA_real_
    ))
  }

  team1_wins <- sum(results$outcome_winner == team1)
  team2_wins <- sum(results$outcome_winner == team2)
  total <- nrow(results)

  return(list(
    team1_wins = as.integer(team1_wins),
    team2_wins = as.integer(team2_wins),
    total_matches = as.integer(total),
    team1_win_rate = team1_wins / total
  ))
}


#' Calculate Team Roster Skill Indices
#'
#' Calculates team strength by aggregating player skill indices from the likely XI.
#' Uses the t20_player_skill table for EMA-based, drift-proof skill measures.
#'
#' @param team Character. Team name
#' @param as_of_date Date. Date to calculate skills for (uses data before this date)
#' @param conn DBI connection. Database connection
#' @param format Character. Format: "t20", "odi", or "test". Default "t20".
#' @param match_type Character. Filter by match type (optional)
#' @param event_name Character. Filter by event name (optional)
#' @param min_balls Integer. Minimum balls for reliable skill index. Default 30.
#'
#' @return List with aggregated skill indices:
#'   - batter_scoring_avg: Average BSI (runs/ball) for batters
#'   - batter_scoring_top5: Average BSI for top 5 batters
#'   - batter_survival_avg: Average survival rate for batters
#'   - bowler_economy_avg: Average economy index for bowlers
#'   - bowler_economy_top5: Average economy for top 5 bowlers (lower = better)
#'   - bowler_strike_avg: Average strike rate (wickets/ball) for bowlers
#'   - n_batters: Number of batters with skill data
#'   - n_bowlers: Number of bowlers with skill data
#' @keywords internal
calculate_team_roster_skill <- function(team, as_of_date, conn,
                                         format = NULL,
                                         match_type = NULL,
                                         event_name = NULL,
                                         min_balls = 30) {

  # Use match_type to determine format if not explicitly provided
  if (is.null(format) && !is.null(match_type)) {
    format <- normalize_match_format(match_type)
  } else if (is.null(format)) {
    format <- "t20"  # Default
  }

  skill_table <- get_format_table("player_skill", format)

  # Check if skill table exists
  if (!skill_table %in% DBI::dbListTables(conn)) {
    # Return default values if no skill data
    start_vals <- get_skill_start_values(format)
    return(list(
      batter_scoring_avg = start_vals$runs,
      batter_scoring_top5 = start_vals$runs,
      batter_survival_avg = start_vals$survival,
      bowler_economy_avg = start_vals$runs,
      bowler_economy_top5 = start_vals$runs,
      bowler_strike_avg = 1 - start_vals$survival,
      n_batters = 0L,
      n_bowlers = 0L
    ))
  }

  # Get likely XI
  likely_xi <- get_likely_playing_xi(team, as_of_date, conn,
                                      n_recent_matches = 3,
                                      match_type = match_type,
                                      event_name = event_name)

  if (nrow(likely_xi) == 0) {
    start_vals <- get_skill_start_values(format)
    return(list(
      batter_scoring_avg = start_vals$runs,
      batter_scoring_top5 = start_vals$runs,
      batter_survival_avg = start_vals$survival,
      bowler_economy_avg = start_vals$runs,
      bowler_economy_top5 = start_vals$runs,
      bowler_strike_avg = 1 - start_vals$survival,
      n_batters = 0L,
      n_bowlers = 0L
    ))
  }

  player_ids <- likely_xi$player_id
  placeholders <- paste(rep("?", length(player_ids)), collapse = ", ")
  as_of_date_str <- as.character(as.Date(as_of_date))

  # Get latest batter skill indices
  batter_query <- sprintf("
    WITH ranked AS (
      SELECT
        batter_id as player_id,
        batter_scoring_index,
        batter_survival_rate,
        batter_balls_faced,
        ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
      WHERE batter_id IN (%s)
        AND match_date < CAST(? AS DATE)
        AND batter_scoring_index IS NOT NULL
    )
    SELECT player_id, batter_scoring_index, batter_survival_rate, batter_balls_faced
    FROM ranked
    WHERE rn = 1
  ", skill_table, placeholders)

  batter_skills <- tryCatch({
    DBI::dbGetQuery(conn, batter_query, params = c(as.list(player_ids), as_of_date_str))
  }, error = function(e) {
    data.frame(player_id = character(0), batter_scoring_index = numeric(0),
               batter_survival_rate = numeric(0), batter_balls_faced = integer(0))
  })

  # Get latest bowler skill indices
  bowler_query <- sprintf("
    WITH ranked AS (
      SELECT
        bowler_id as player_id,
        bowler_economy_index,
        bowler_strike_rate,
        bowler_balls_bowled,
        ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
      WHERE bowler_id IN (%s)
        AND match_date < CAST(? AS DATE)
        AND bowler_economy_index IS NOT NULL
    )
    SELECT player_id, bowler_economy_index, bowler_strike_rate, bowler_balls_bowled
    FROM ranked
    WHERE rn = 1
  ", skill_table, placeholders)

  bowler_skills <- tryCatch({
    DBI::dbGetQuery(conn, bowler_query, params = c(as.list(player_ids), as_of_date_str))
  }, error = function(e) {
    data.frame(player_id = character(0), bowler_economy_index = numeric(0),
               bowler_strike_rate = numeric(0), bowler_balls_bowled = integer(0))
  })

  # Calculate aggregates with fallbacks
  start_vals <- get_skill_start_values(format)

  # Batter aggregates
  if (nrow(batter_skills) > 0) {
    # Filter to reliable batters (enough balls faced)
    reliable_batters <- batter_skills[batter_skills$batter_balls_faced >= min_balls, ]

    if (nrow(reliable_batters) > 0) {
      batter_scoring_avg <- mean(reliable_batters$batter_scoring_index, na.rm = TRUE)
      batter_survival_avg <- mean(reliable_batters$batter_survival_rate, na.rm = TRUE)

      # Top 5 batters by scoring index
      top5_idx <- order(reliable_batters$batter_scoring_index, decreasing = TRUE)[1:min(5, nrow(reliable_batters))]
      batter_scoring_top5 <- mean(reliable_batters$batter_scoring_index[top5_idx], na.rm = TRUE)
    } else {
      # Use all batters if none meet minimum balls threshold
      batter_scoring_avg <- mean(batter_skills$batter_scoring_index, na.rm = TRUE)
      batter_survival_avg <- mean(batter_skills$batter_survival_rate, na.rm = TRUE)
      top5_idx <- order(batter_skills$batter_scoring_index, decreasing = TRUE)[1:min(5, nrow(batter_skills))]
      batter_scoring_top5 <- mean(batter_skills$batter_scoring_index[top5_idx], na.rm = TRUE)
    }
    n_batters <- nrow(batter_skills)
  } else {
    batter_scoring_avg <- start_vals$runs
    batter_scoring_top5 <- start_vals$runs
    batter_survival_avg <- start_vals$survival
    n_batters <- 0L
  }

  # Bowler aggregates
  if (nrow(bowler_skills) > 0) {
    # Filter to reliable bowlers
    reliable_bowlers <- bowler_skills[bowler_skills$bowler_balls_bowled >= min_balls, ]

    if (nrow(reliable_bowlers) > 0) {
      bowler_economy_avg <- mean(reliable_bowlers$bowler_economy_index, na.rm = TRUE)
      bowler_strike_avg <- mean(reliable_bowlers$bowler_strike_rate, na.rm = TRUE)

      # Top 5 bowlers by economy (lower = better, so ascending order)
      top5_idx <- order(reliable_bowlers$bowler_economy_index, decreasing = FALSE)[1:min(5, nrow(reliable_bowlers))]
      bowler_economy_top5 <- mean(reliable_bowlers$bowler_economy_index[top5_idx], na.rm = TRUE)
    } else {
      bowler_economy_avg <- mean(bowler_skills$bowler_economy_index, na.rm = TRUE)
      bowler_strike_avg <- mean(bowler_skills$bowler_strike_rate, na.rm = TRUE)
      top5_idx <- order(bowler_skills$bowler_economy_index, decreasing = FALSE)[1:min(5, nrow(bowler_skills))]
      bowler_economy_top5 <- mean(bowler_skills$bowler_economy_index[top5_idx], na.rm = TRUE)
    }
    n_bowlers <- nrow(bowler_skills)
  } else {
    bowler_economy_avg <- start_vals$runs
    bowler_economy_top5 <- start_vals$runs
    bowler_strike_avg <- 1 - start_vals$survival
    n_bowlers <- 0L
  }

  return(list(
    batter_scoring_avg = batter_scoring_avg,
    batter_scoring_top5 = batter_scoring_top5,
    batter_survival_avg = batter_survival_avg,
    bowler_economy_avg = bowler_economy_avg,
    bowler_economy_top5 = bowler_economy_top5,
    bowler_strike_avg = bowler_strike_avg,
    n_batters = as.integer(n_batters),
    n_bowlers = as.integer(n_bowlers)
  ))
}


#' Store Team ELO
#'
#' Stores team ELO ratings in the database.
#'
#' @param team_id Character. Team identifier/name
#' @param match_id Character. Match that caused this update
#' @param match_date Date. Match date
#' @param match_type Character. Match type
#' @param event_name Character. Event/tournament name
#' @param elo_result Numeric. Result-based ELO
#' @param elo_roster_batting Numeric. Roster batting ELO
#' @param elo_roster_bowling Numeric. Roster bowling ELO
#' @param elo_roster_combined Numeric. Roster combined ELO
#' @param matches_played Integer. Running count of matches
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
store_team_elo <- function(team_id, match_id, match_date, match_type, event_name,
                           elo_result, elo_roster_batting, elo_roster_bowling,
                           elo_roster_combined, matches_played, conn) {

  DBI::dbExecute(conn, "
    INSERT INTO team_elo (
      team_id, match_id, match_date, match_type, event_name,
      elo_result, elo_roster_batting, elo_roster_bowling,
      elo_roster_combined, matches_played
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (team_id, match_id) DO UPDATE SET
      elo_result = EXCLUDED.elo_result,
      elo_roster_batting = EXCLUDED.elo_roster_batting,
      elo_roster_bowling = EXCLUDED.elo_roster_bowling,
      elo_roster_combined = EXCLUDED.elo_roster_combined,
      matches_played = EXCLUDED.matches_played
  ", params = list(
    team_id, match_id, match_date, match_type, event_name,
    elo_result, elo_roster_batting, elo_roster_bowling,
    elo_roster_combined, matches_played
  ))

  invisible(TRUE)
}


#' Calculate Margin of Victory Multiplier
#'
#' Calculates the G-factor (margin of victory multiplier) for ELO updates.
#' Based on FiveThirtyEight methodology used in NFL/NBA.
#'
#' The multiplier rewards dominant victories and punishes narrow losses,
#' while adjusting for favorite bias (favorites are expected to win by more).
#'
#' Formula: G = (abs(margin) + BASE_OFFSET)^EXPONENT / (DENOM_BASE + ELO_FACTOR * elo_diff)
#' Bounded by MOV_MIN and MOV_MAX.
#'
#' @param unified_margin Numeric. Margin of victory in runs-equivalent.
#'   Positive = winner's perspective, use abs() for magnitude.
#' @param winner_elo Numeric. ELO rating of the winning team
#' @param loser_elo Numeric. ELO rating of the losing team
#' @param format Character. Match format for potential format-specific tuning
#' @param mov_exponent Numeric. Exponent for margin (default from constants)
#' @param mov_base_offset Numeric. Base offset to prevent G=0 (default from constants)
#' @param mov_denom_base Numeric. Denominator base (default from constants)
#' @param mov_elo_factor Numeric. ELO factor for favorite adjustment (default from constants)
#' @param mov_min Numeric. Minimum G value (default from constants)
#' @param mov_max Numeric. Maximum G value (default from constants)
#'
#' @return Numeric. Multiplier (typically 0.5 to 2.5)
#' @keywords internal
calculate_mov_multiplier <- function(unified_margin, winner_elo, loser_elo,
                                      format = "t20",
                                      mov_exponent = MOV_EXPONENT,
                                      mov_base_offset = MOV_BASE_OFFSET,
                                      mov_denom_base = MOV_DENOMINATOR_BASE,
                                      mov_elo_factor = MOV_ELO_FACTOR,
                                      mov_min = MOV_MIN,
                                      mov_max = MOV_MAX) {

  # ELO difference (positive = favorite won)
  elo_diff <- winner_elo - loser_elo

  # Absolute margin magnitude
  margin_abs <- abs(unified_margin)

  # Calculate G-factor
  # Numerator grows sublinearly with margin (exponent < 1)
  numerator <- (margin_abs + mov_base_offset)^mov_exponent

  # Denominator adjusts for favorite bias
  # When favorite wins big, denominator is larger (reduces G)
  # When underdog wins big, denominator is smaller (increases G)
  denominator <- mov_denom_base + mov_elo_factor * elo_diff

  # Prevent division by very small numbers
  denominator <- max(denominator, 1)

  G <- numerator / denominator

  # Clamp to reasonable bounds
  G <- max(mov_min, min(mov_max, G))

  return(G)
}


#' Calculate ELO Update with Margin
#'
#' Calculates team ELO update incorporating margin of victory.
#' Uses the standard ELO formula with a G-factor multiplier.
#'
#' Formula: new_elo = old_elo + K * G * (actual - expected)
#'
#' @param team_elo Numeric. Team's current ELO rating
#' @param opponent_elo Numeric. Opponent's current ELO rating
#' @param won Logical or Numeric. TRUE/1 for win, FALSE/0 for loss, 0.5 for draw
#' @param unified_margin Numeric. Margin of victory (positive for wins, 0 for draws)
#' @param k_factor Numeric. Base K-factor for the update
#' @param home_advantage Numeric. Home advantage in ELO points (default 0)
#' @param format Character. Match format
#'
#' @return Numeric. New ELO rating after the match
#' @keywords internal
calculate_elo_update_with_margin <- function(team_elo, opponent_elo, won,
                                              unified_margin,
                                              k_factor = TEAM_K_FACTOR,
                                              home_advantage = 0,
                                              format = "t20") {

  # Calculate expected outcome with home advantage
  adjusted_elo <- team_elo + home_advantage
  expected <- 1 / (1 + 10^((opponent_elo - adjusted_elo) / ELO_DIVISOR))

  # Actual outcome
  if (is.logical(won)) {
    actual <- if (won) 1.0 else 0.0
  } else {
    actual <- as.numeric(won)  # Allows 0.5 for draws
  }

  # Calculate G-factor (margin multiplier)
  if (is.na(unified_margin) || unified_margin == 0 || actual == DRAW_ACTUAL_SCORE) {
    # Draw or no margin data: use neutral multiplier
    G <- DRAW_MOV_MULTIPLIER
  } else {
    # Determine winner/loser ELOs for G calculation
    if (actual > 0.5) {
      # This team won
      G <- calculate_mov_multiplier(unified_margin, team_elo, opponent_elo, format)
    } else {
      # This team lost
      G <- calculate_mov_multiplier(unified_margin, opponent_elo, team_elo, format)
    }
  }

  # Apply ELO update with margin multiplier
  new_elo <- team_elo + k_factor * G * (actual - expected)

  return(new_elo)
}
