# User-Friendly API Functions
#
# High-level wrapper functions for common cricket analytics tasks.
# These functions accept human-readable names and return formatted results.


# Internal helper: format numbers with commas (falls back to base R if scales not available)
format_number <- function(x) {
  if (requireNamespace("scales", quietly = TRUE)) {
    scales::comma(x)
  } else {
    format(x, big.mark = ",", scientific = FALSE)
  }
}

# Internal helper: validate format parameter
validate_format <- function(format, allow_null = TRUE) {
  if (is.null(format) && allow_null) {
    return(NULL)
  }
  if (is.null(format) && !allow_null) {
    cli::cli_abort("format parameter is required")
  }
  format <- tolower(format)
  valid_formats <- c("t20", "odi", "test")
  if (!format %in% valid_formats) {
    cli::cli_abort("format must be one of: {paste(valid_formats, collapse = ', ')}")
  }
  return(format)
}


# ============================================================================
# Player Functions
# ============================================================================

#' Get Player Information
#'
#' Look up a player by name or ID and get their basic info and current skill indices.
#'
#' @param name_or_id Character. Player name (partial match supported) or player ID.
#' @param format Character. Format for skill indices: "t20", "odi", or "test".
#'   If NULL, returns latest skills across all formats.
#' @param source Character. "local" (default) uses local DuckDB.
#'   "remote" queries GitHub releases via fast download.
#' @param db_path Character. Database path (only used when source = "local").
#'
#' @return A `bouncer_player` object containing:
#'   \itemize{
#'     \item player_id - Unique player identifier
#'     \item player_name - Full name
#'     \item country - Country/team
#'     \item batting_style - Left/right handed
#'     \item bowling_style - Bowling type
#'     \item skills - Current skill indices (if available)
#'   }
#' @export
#'
#' @examples
#' \dontrun{
#' # Look up by name (partial match)
#' kohli <- get_player("Virat Kohli")
#' kohli <- get_player("Kohli")  # Also works
#'
#' # Get T20-specific skills
#' kohli_t20 <- get_player("Virat Kohli", format = "t20")
#'
#' # Look up from remote (no local install needed)
#' kohli_remote <- get_player("Virat Kohli", format = "t20", source = "remote")
#'
#' # Print shows formatted output
#' print(kohli)
#' }
get_player <- function(name_or_id, format = NULL,
                       source = c("local", "remote"), db_path = NULL) {

  # Validate inputs early
  if (missing(name_or_id) || is.null(name_or_id)) {
    cli::cli_abort("name_or_id is required")
  }
  if (!is.character(name_or_id) || length(name_or_id) != 1) {
    cli::cli_abort("name_or_id must be a single character string")
  }
  if (nchar(trimws(name_or_id)) == 0) {
    cli::cli_abort("name_or_id cannot be empty")
  }

  source <- match.arg(source)

  # Validate format if provided
  format <- validate_format(format, allow_null = TRUE)

  if (source == "remote") {
    return(get_player_remote(name_or_id, format))
  }

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Try exact match first, then fuzzy match
  player <- DBI::dbGetQuery(conn, "
    SELECT * FROM players
    WHERE player_id = ? OR LOWER(player_name) = LOWER(?)
    LIMIT 1
  ", params = list(name_or_id, name_or_id))

  # If no exact match, try partial match
  if (nrow(player) == 0) {
    player <- DBI::dbGetQuery(conn, "
      SELECT * FROM players
      WHERE LOWER(player_name) LIKE LOWER(?)
      ORDER BY player_name
      LIMIT 1
    ", params = list(paste0("%", name_or_id, "%")))
  }

  if (nrow(player) == 0) {
    cli::cli_alert_warning("No player found matching '{name_or_id}'")
    return(NULL)
  }

  # Get skill indices if format specified
  skills <- NULL
  if (!is.null(format)) {
    format <- tolower(format)
    skill_table <- paste0(format, "_player_skill")

    # Check if skill table exists
    tables <- DBI::dbListTables(conn)
    if (skill_table %in% tables) {
      skills <- DBI::dbGetQuery(conn, sprintf("
        SELECT
          batter_scoring_index,
          batter_survival_rate,
          bowler_economy_index,
          bowler_strike_rate,
          delivery_id
        FROM %s
        WHERE batter_id = ? OR bowler_id = ?
        ORDER BY delivery_id DESC
        LIMIT 1
      ", skill_table), params = list(player$player_id, player$player_id))
    }
  }

  build_player_result(player, skills, format)
}


#' @keywords internal
get_player_remote <- function(name_or_id, format = NULL) {

  cli::cli_alert_info("Looking up player from remote...")

  # Helper to escape single quotes (SQL injection prevention)
  escape_sql <- function(x) gsub("'", "''", x)
  name_escaped <- escape_sql(name_or_id)

  # Query players table
  sql_exact <- sprintf(
    "SELECT * FROM {table} WHERE player_id = '%s' OR LOWER(player_name) = LOWER('%s') LIMIT 1",
    name_escaped, name_escaped
  )

  player <- tryCatch({
    query_remote_parquet("players", sql_exact)
  }, error = function(e) {
    cli::cli_abort("Remote query failed: {e$message}")
  })

  # If no exact match, try partial match
  if (nrow(player) == 0) {
    sql_fuzzy <- sprintf(
      "SELECT * FROM {table} WHERE LOWER(player_name) LIKE LOWER('%%%s%%') ORDER BY player_name LIMIT 1",
      name_escaped
    )
    player <- tryCatch({
      query_remote_parquet("players", sql_fuzzy)
    }, error = function(e) data.frame())
  }

  if (nrow(player) == 0) {
    cli::cli_alert_warning("No player found matching '{name_or_id}'")
    return(NULL)
  }

  # Get skill indices if format specified
  skills <- NULL
  if (!is.null(format)) {
    format <- tolower(format)
    skill_table <- paste0(format, "_player_skill")

    # Check if skill table exists in remote
    available <- tryCatch({
      get_remote_tables()
    }, error = function(e) character(0))

    if (skill_table %in% available) {
      player_id_escaped <- escape_sql(player$player_id)
      skill_sql <- sprintf(
        "SELECT batter_scoring_index, batter_survival_rate, bowler_economy_index, bowler_strike_rate, delivery_id
         FROM {table}
         WHERE batter_id = '%s' OR bowler_id = '%s'
         ORDER BY delivery_id DESC
         LIMIT 1",
        player_id_escaped, player_id_escaped
      )
      skills <- tryCatch({
        query_remote_parquet(skill_table, skill_sql)
      }, error = function(e) NULL)
    }
  }

  build_player_result(player, skills, format)
}


#' @keywords internal
build_player_result <- function(player, skills, format) {
  # Check if we have valid skill data
  has_skills <- !is.null(skills) && is.data.frame(skills) && nrow(skills) > 0

  result <- list(
    player_id = player$player_id,
    player_name = player$player_name,
    country = player$country %||% NA_character_,
    batting_style = player$batting_style %||% NA_character_,
    bowling_style = player$bowling_style %||% NA_character_,
    skills = if (has_skills) skills else NULL,
    format = format
  )

  class(result) <- c("bouncer_player", "list")
  cli::cli_alert_success("Found: {player$player_name}")
  return(result)
}


#' Print Method for bouncer_player Objects
#'
#' @param x A bouncer_player object from \code{get_player()}
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the input object
#' @export
print.bouncer_player <- function(x, ...) {
  cli::cli_h2("{x$player_name}")
  cli::cli_text("ID: {x$player_id}")

  if (!is.na(x$country)) {
    cli::cli_text("Country: {x$country}")
  }
  if (!is.na(x$batting_style)) {
    cli::cli_text("Batting: {x$batting_style}")
  }
  if (!is.na(x$bowling_style)) {
    cli::cli_text("Bowling: {x$bowling_style}")
  }

  if (!is.null(x$skills) && nrow(x$skills) > 0) {
    cli::cli_h3("Current Skills ({toupper(x$format)})")
    cli::cli_text("Batting Scoring Index: {round(x$skills$batter_scoring_index, 3)}")
    cli::cli_text("Batting Survival Rate: {round(x$skills$batter_survival_rate * 100, 1)}%")
    cli::cli_text("Bowling Economy Index: {round(x$skills$bowler_economy_index, 3)}")
    cli::cli_text("Bowling Strike Rate: {round(x$skills$bowler_strike_rate * 100, 1)}%")
  }

  invisible(x)
}


#' Analyze Player Performance
#'
#' Get comprehensive performance analysis for a player including stats, skills,
#' and performance trends.
#'
#' @param name_or_id Character. Player name or ID.
#' @param format Character. Format to analyze: "t20", "odi", "test", or NULL for all.
#' @param db_path Character. Database path.
#'
#' @return A `bouncer_player_analysis` object with stats, skills, and trends.
#' @export
#'
#' @examples
#' \dontrun{
#' analysis <- analyze_player("Virat Kohli", format = "t20")
#' print(analysis)
#' }
analyze_player <- function(name_or_id, format = NULL, db_path = NULL) {

  # Get player info
  player <- get_player(name_or_id, format = format, db_path = db_path)

  if (is.null(player)) {
    return(NULL)
  }

  # Get batting and bowling stats
  batting <- query_batter_stats(
    batter_id = player$player_id,
    match_type = format,
    db_path = db_path
  )

  bowling <- query_bowler_stats(
    bowler_id = player$player_id,
    match_type = format,
    db_path = db_path
  )

  result <- list(
    player = player,
    batting = batting,
    bowling = bowling,
    format = format
  )

  class(result) <- c("bouncer_player_analysis", "list")
  return(result)
}


#' Print Method for bouncer_player_analysis Objects
#'
#' @param x A bouncer_player_analysis object from \code{analyze_player()}
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the input object
#' @export
print.bouncer_player_analysis <- function(x, ...) {
  format_label <- if (is.null(x$format)) "All Formats" else toupper(x$format)

  cli::cli_h1("{x$player$player_name} - {format_label}")

  # Print player info
  if (!is.na(x$player$country)) cli::cli_text("Country: {x$player$country}")

  # Batting stats
  if (!is.null(x$batting) && x$batting$balls_faced > 0) {
    cli::cli_h2("Batting")
    cli::cli_text("Balls Faced: {format_number(x$batting$balls_faced)}")
    cli::cli_text("Runs: {format_number(x$batting$runs_scored)}")
    cli::cli_text("Average: {x$batting$batting_average %||% '-'}")
    cli::cli_text("Strike Rate: {x$batting$strike_rate}")
    cli::cli_text("Boundaries: {x$batting$fours} fours, {x$batting$sixes} sixes")
  }

  # Bowling stats
  if (!is.null(x$bowling) && x$bowling$balls_bowled > 0) {
    cli::cli_h2("Bowling")
    cli::cli_text("Balls Bowled: {format_number(x$bowling$balls_bowled)}")
    cli::cli_text("Wickets: {x$bowling$wickets}")
    cli::cli_text("Average: {x$bowling$bowling_average %||% '-'}")
    cli::cli_text("Economy: {x$bowling$economy_rate}")
    cli::cli_text("Strike Rate: {x$bowling$strike_rate %||% '-'}")
  }

  # Current skills
  if (!is.null(x$player$skills) && nrow(x$player$skills) > 0) {
    cli::cli_h2("Current Skill Indices")
    cli::cli_text("Batting Index: {round(x$player$skills$batter_scoring_index, 3)}")
    cli::cli_text("Survival Rate: {round(x$player$skills$batter_survival_rate * 100, 1)}%")
    cli::cli_text("Economy Index: {round(x$player$skills$bowler_economy_index, 3)}")
  }

  invisible(x)
}


#' Compare Two Players
#'
#' Head-to-head comparison of two players' batting and bowling records.
#'
#' @param player1 Character. First player name or ID.
#' @param player2 Character. Second player name or ID.
#' @param format Character. Match format: "t20", "odi", "test".
#' @param db_path Character. Database path.
#'
#' @return A `bouncer_player_comparison` object.
#' @export
#'
#' @examples
#' \dontrun{
#' compare_players("Virat Kohli", "Steve Smith", format = "test")
#' }
compare_players <- function(player1, player2, format = "t20", db_path = NULL) {

  p1 <- analyze_player(player1, format = format, db_path = db_path)
  p2 <- analyze_player(player2, format = format, db_path = db_path)

  if (is.null(p1) || is.null(p2)) {
    cli::cli_alert_danger("Could not find both players")
    return(NULL)
  }

  result <- list(
    player1 = p1,
    player2 = p2,
    format = format
  )

  class(result) <- c("bouncer_player_comparison", "list")
  return(result)
}


#' Print Method for bouncer_player_comparison Objects
#'
#' @param x A bouncer_player_comparison object from \code{compare_players()}
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the input object
#' @export
print.bouncer_player_comparison <- function(x, ...) {
  cli::cli_h1("Player Comparison ({toupper(x$format)})")
  cli::cli_h2("{x$player1$player$player_name} vs {x$player2$player$player_name}")

  # Create comparison table
  cli::cli_h3("Batting")
  cat(sprintf("%-20s %15s %15s\n", "Metric",
              substr(x$player1$player$player_name, 1, 15),
              substr(x$player2$player$player_name, 1, 15)))
  cat(sprintf("%-20s %15s %15s\n", "--------------------", "---------------", "---------------"))

  cat(sprintf("%-20s %15s %15s\n", "Runs",
              format_number(x$player1$batting$runs_scored),
              format_number(x$player2$batting$runs_scored)))
  cat(sprintf("%-20s %15s %15s\n", "Average",
              x$player1$batting$batting_average %||% "-",
              x$player2$batting$batting_average %||% "-"))
  cat(sprintf("%-20s %15s %15s\n", "Strike Rate",
              x$player1$batting$strike_rate,
              x$player2$batting$strike_rate))

  if (x$player1$bowling$balls_bowled > 0 || x$player2$bowling$balls_bowled > 0) {
    cli::cli_h3("Bowling")
    cat(sprintf("%-20s %15s %15s\n", "Wickets",
                x$player1$bowling$wickets,
                x$player2$bowling$wickets))
    cat(sprintf("%-20s %15s %15s\n", "Average",
                x$player1$bowling$bowling_average %||% "-",
                x$player2$bowling$bowling_average %||% "-"))
    cat(sprintf("%-20s %15s %15s\n", "Economy",
                x$player1$bowling$economy_rate,
                x$player2$bowling$economy_rate))
  }

  invisible(x)
}


# ============================================================================
# Team Functions
# ============================================================================

#' Get Team Information
#'
#' Look up a team and get their current ELO ratings.
#'
#' @param name Character. Team name.
#' @param format Character. Format for ELO: "t20", "odi", "test".
#' @param db_path Character. Database path.
#'
#' @return A `bouncer_team` object.
#' @export
#'
#' @examples
#' \dontrun{
#' india <- get_team("India")
#' print(india)
#' }
get_team <- function(name, format = NULL, db_path = NULL) {

 # Validate inputs
  if (missing(name) || is.null(name)) {
    cli::cli_abort("name is required")
  }
  if (!is.character(name) || length(name) != 1) {
    cli::cli_abort("name must be a single character string")
  }
  if (nchar(trimws(name)) == 0) {
    cli::cli_abort("name cannot be empty")
  }

  # Validate format if provided
  if (!is.null(format)) {
    format <- validate_format(format, allow_null = TRUE)
  }

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Build match type filter
  type_filter <- ""
  if (!is.null(format)) {
    format <- tolower(format)
    type_filter <- switch(format,
      "t20" = "AND LOWER(match_type) IN ('t20', 'it20')",
      "odi" = "AND LOWER(match_type) IN ('odi', 'odm')",
      "test" = "AND LOWER(match_type) IN ('test', 'mdm')",
      ""
    )
  }

  # Get latest ELO for team
  query <- sprintf("
    SELECT
      team_id,
      match_id,
      match_date,
      match_type,
      elo_result,
      elo_roster_combined,
      matches_played
    FROM team_elo
    WHERE team_id = ?
    %s
    ORDER BY match_date DESC
    LIMIT 1
  ", type_filter)

  elo <- DBI::dbGetQuery(conn, query, params = list(name))

  if (nrow(elo) == 0) {
    # Try partial match
    query <- sprintf("
      SELECT
        team_id,
        match_id,
        match_date,
        match_type,
        elo_result,
        elo_roster_combined,
        matches_played
      FROM team_elo
      WHERE LOWER(team_id) LIKE LOWER(?)
      %s
      ORDER BY match_date DESC
      LIMIT 1
    ", type_filter)

    elo <- DBI::dbGetQuery(conn, query, params = list(paste0("%", name, "%")))
  }

  if (nrow(elo) == 0) {
    cli::cli_alert_warning("No team found matching '{name}'")
    return(NULL)
  }

  result <- list(
    team_name = elo$team_id,
    elo_result = elo$elo_result,
    elo_roster = elo$elo_roster_combined,
    matches_played = elo$matches_played,
    last_match_date = elo$match_date,
    format = format
  )

  class(result) <- c("bouncer_team", "list")
  return(result)
}


#' Print Method for bouncer_team Objects
#'
#' @param x A bouncer_team object from \code{get_team()}
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the input object
#' @export
print.bouncer_team <- function(x, ...) {
  format_label <- if (is.null(x$format)) "Latest" else toupper(x$format)

  cli::cli_h2("{x$team_name}")
  cli::cli_text("Format: {format_label}")
  cli::cli_text("Result ELO: {round(x$elo_result, 0)}")
  if (!is.na(x$elo_roster)) {
    cli::cli_text("Roster ELO: {round(x$elo_roster, 0)}")
  }
  cli::cli_text("Matches Played: {x$matches_played}")
  cli::cli_text("Last Match: {x$last_match_date}")

  invisible(x)
}


#' Compare Two Teams
#'
#' Get matchup analysis between two teams including ELO comparison
#' and predicted win probability.
#'
#' @param team1 Character. First team name.
#' @param team2 Character. Second team name.
#' @param format Character. Match format.
#' @param neutral_venue Logical. If TRUE, no home advantage applied.
#' @param db_path Character. Database path.
#'
#' @return A `bouncer_team_comparison` object.
#' @export
#'
#' @examples
#' \dontrun{
#' compare_teams("India", "Australia", format = "t20")
#' }
compare_teams <- function(team1, team2, format = "t20", neutral_venue = TRUE, db_path = NULL) {

  # Validate inputs
  if (missing(team1) || is.null(team1)) {
    cli::cli_abort("team1 is required")
  }
  if (missing(team2) || is.null(team2)) {
    cli::cli_abort("team2 is required")
  }
  if (!is.character(team1) || length(team1) != 1 || nchar(trimws(team1)) == 0) {
    cli::cli_abort("team1 must be a non-empty character string")
  }
  if (!is.character(team2) || length(team2) != 1 || nchar(trimws(team2)) == 0) {
    cli::cli_abort("team2 must be a non-empty character string")
  }

  # Validate format
  format <- validate_format(format, allow_null = FALSE)

  t1 <- get_team(team1, format = format, db_path = db_path)
  t2 <- get_team(team2, format = format, db_path = db_path)

  if (is.null(t1) || is.null(t2)) {
    cli::cli_alert_danger("Could not find both teams")
    return(NULL)
  }

  # Calculate win probability from ELO difference
  elo_diff <- t1$elo_result - t2$elo_result
  win_prob_t1 <- 1 / (1 + 10^(-elo_diff / 400))

  result <- list(
    team1 = t1,
    team2 = t2,
    elo_diff = elo_diff,
    win_prob_team1 = win_prob_t1,
    win_prob_team2 = 1 - win_prob_t1,
    format = format,
    neutral_venue = neutral_venue
  )

  class(result) <- c("bouncer_team_comparison", "list")
  return(result)
}


#' Print Method for bouncer_team_comparison Objects
#'
#' @param x A bouncer_team_comparison object from \code{compare_teams()}
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the input object
#' @export
print.bouncer_team_comparison <- function(x, ...) {
  cli::cli_h1("Team Matchup ({toupper(x$format)})")
  cli::cli_h2("{x$team1$team_name} vs {x$team2$team_name}")

  cli::cli_h3("ELO Ratings")
  cat(sprintf("%-20s %8s\n", x$team1$team_name, round(x$team1$elo_result, 0)))
  cat(sprintf("%-20s %8s\n", x$team2$team_name, round(x$team2$elo_result, 0)))
  cat(sprintf("%-20s %8s\n", "Difference", sprintf("%+d", round(x$elo_diff, 0))))

  cli::cli_h3("Win Probability")
  cat(sprintf("%-20s %8.1f%%\n", x$team1$team_name, x$win_prob_team1 * 100))
  cat(sprintf("%-20s %8.1f%%\n", x$team2$team_name, x$win_prob_team2 * 100))

  invisible(x)
}


# ============================================================================
# Match Functions
# ============================================================================

#' Analyze a Match
#'
#' Get comprehensive analysis of a completed match including scores,
#' key moments, and player performances.
#'
#' @param match_id Character. The match ID to analyze.
#' @param db_path Character. Database path.
#'
#' @return A `bouncer_match` object with match details.
#' @export
#'
#' @examples
#' \dontrun{
#' match <- analyze_match("1234567")
#' print(match)
#' }
analyze_match <- function(match_id, db_path = NULL) {

  # Validate inputs
  if (missing(match_id) || is.null(match_id)) {
    cli::cli_abort("match_id is required")
  }
  if (!is.character(match_id) || length(match_id) != 1) {
    cli::cli_abort("match_id must be a single character string")
  }
  if (nchar(trimws(match_id)) == 0) {
    cli::cli_abort("match_id cannot be empty")
  }

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Get match info
  match <- DBI::dbGetQuery(conn, "
    SELECT * FROM matches WHERE match_id = ?
  ", params = list(match_id))

  if (nrow(match) == 0) {
    cli::cli_alert_warning("No match found with ID '{match_id}'")
    return(NULL)
  }

  # Get innings summaries
  innings <- DBI::dbGetQuery(conn, "
    SELECT * FROM match_innings
    WHERE match_id = ?
    ORDER BY innings
  ", params = list(match_id))

  # Get top performers (batting)
  top_batters <- DBI::dbGetQuery(conn, "
    SELECT
      batter_id,
      SUM(runs_batter) as runs,
      COUNT(*) as balls,
      SUM(CASE WHEN is_four THEN 1 ELSE 0 END) as fours,
      SUM(CASE WHEN is_six THEN 1 ELSE 0 END) as sixes
    FROM deliveries
    WHERE match_id = ?
    GROUP BY batter_id
    ORDER BY runs DESC
    LIMIT 5
  ", params = list(match_id))

  # Get top performers (bowling)
  top_bowlers <- DBI::dbGetQuery(conn, "
    SELECT
      bowler_id,
      COUNT(*) as balls,
      SUM(runs_total) as runs,
      SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets
    FROM deliveries
    WHERE match_id = ?
    GROUP BY bowler_id
    ORDER BY wickets DESC, runs ASC
    LIMIT 5
  ", params = list(match_id))

  result <- list(
    match_id = match_id,
    match_info = match,
    innings = innings,
    top_batters = top_batters,
    top_bowlers = top_bowlers
  )

  class(result) <- c("bouncer_match", "list")
  return(result)
}


#' Print Method for bouncer_match Objects
#'
#' @param x A bouncer_match object from \code{analyze_match()}
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the input object
#' @export
print.bouncer_match <- function(x, ...) {
  m <- x$match_info

  cli::cli_h1("{m$team1} vs {m$team2}")
  cli::cli_text("{m$match_type} | {m$match_date} | {m$venue}")

  # Innings summaries
  if (nrow(x$innings) > 0) {
    cli::cli_h2("Scorecard")
    for (i in seq_len(nrow(x$innings))) {
      inn <- x$innings[i, ]
      overs_str <- if (!is.na(inn$total_overs)) paste0(" (", inn$total_overs, " ov)") else ""
      cli::cli_text("{inn$batting_team}: {inn$total_runs}/{inn$total_wickets}{overs_str}")
    }
  }

  # Result
  if (!is.na(m$outcome_type)) {
    if (m$outcome_type == "runs" && !is.na(m$outcome_by_runs)) {
      cli::cli_alert_success("{m$winner} won by {m$outcome_by_runs} runs")
    } else if (m$outcome_type == "wickets" && !is.na(m$outcome_by_wickets)) {
      cli::cli_alert_success("{m$winner} won by {m$outcome_by_wickets} wickets")
    } else if (m$outcome_type %in% c("tie", "draw")) {
      cli::cli_alert_info("Match {m$outcome_type}")
    }
  }

  # Top performers
  if (nrow(x$top_batters) > 0) {
    cli::cli_h3("Top Batters")
    for (i in seq_len(min(3, nrow(x$top_batters)))) {
      b <- x$top_batters[i, ]
      sr <- round((b$runs / b$balls) * 100, 0)
      cli::cli_text("{b$batter_id}: {b$runs} ({b$balls}b) SR {sr}")
    }
  }

  if (nrow(x$top_bowlers) > 0) {
    cli::cli_h3("Top Bowlers")
    for (i in seq_len(min(3, nrow(x$top_bowlers)))) {
      b <- x$top_bowlers[i, ]
      overs <- floor(b$balls / 6)
      balls <- b$balls %% 6
      cli::cli_text("{b$bowler_id}: {b$wickets}/{b$runs} ({overs}.{balls} ov)")
    }
  }

  invisible(x)
}


# ============================================================================
# Prediction Functions
# ============================================================================

#' Predict Match Outcome
#'
#' Get win probability and expected margin for an upcoming match between two teams.
#'
#' @param team1 Character. First team name.
#' @param team2 Character. Second team name.
#' @param format Character. Match format: "t20", "odi", "test".
#' @param venue Character. Venue name (optional, for venue adjustment).
#' @param db_path Character. Database path.
#'
#' @return A `bouncer_prediction` object.
#' @export
#'
#' @examples
#' \dontrun{
#' pred <- predict_match("India", "Australia", format = "t20")
#' print(pred)
#' }
predict_match <- function(team1, team2, format = "t20", venue = NULL, db_path = NULL) {

  # Validate inputs
  if (missing(team1) || is.null(team1)) {
    cli::cli_abort("team1 is required")
  }
  if (missing(team2) || is.null(team2)) {
    cli::cli_abort("team2 is required")
  }
  if (!is.character(team1) || length(team1) != 1 || nchar(trimws(team1)) == 0) {
    cli::cli_abort("team1 must be a non-empty character string")
  }
  if (!is.character(team2) || length(team2) != 1 || nchar(trimws(team2)) == 0) {
    cli::cli_abort("team2 must be a non-empty character string")
  }

  # Validate format
  format <- validate_format(format, allow_null = FALSE)

  # Get team ELOs
  t1 <- get_team(team1, format = format, db_path = db_path)
  t2 <- get_team(team2, format = format, db_path = db_path)

  if (is.null(t1) || is.null(t2)) {
    cli::cli_alert_danger("Could not find both teams")
    return(NULL)
  }

  # Calculate win probability
  elo_diff <- t1$elo_result - t2$elo_result
  win_prob_t1 <- 1 / (1 + 10^(-elo_diff / 400))

  # Calculate expected margin
  expected_margin <- get_expected_margin(t1$elo_result, t2$elo_result, format = format)

  result <- list(
    team1 = t1$team_name,
    team2 = t2$team_name,
    format = format,
    venue = venue,
    elo_team1 = t1$elo_result,
    elo_team2 = t2$elo_result,
    elo_diff = elo_diff,
    win_prob_team1 = win_prob_t1,
    win_prob_team2 = 1 - win_prob_t1,
    expected_margin = expected_margin,
    favorite = if (win_prob_t1 > 0.5) t1$team_name else t2$team_name
  )

  class(result) <- c("bouncer_prediction", "list")
  return(result)
}


#' Print Method for bouncer_prediction Objects
#'
#' @param x A bouncer_prediction object from \code{predict_match()}
#' @param ... Additional arguments (ignored)
#'
#' @return Invisibly returns the input object
#' @export
print.bouncer_prediction <- function(x, ...) {
  cli::cli_h1("Match Prediction")
  cli::cli_h2("{x$team1} vs {x$team2}")
  cli::cli_text("Format: {toupper(x$format)}")
  if (!is.null(x$venue)) cli::cli_text("Venue: {x$venue}")

  cli::cli_h3("Win Probability")

  # Format as percentage bar
  pct1 <- round(x$win_prob_team1 * 100)
  pct2 <- round(x$win_prob_team2 * 100)

  bar1 <- paste(rep("=", pct1 %/% 5), collapse = "")
  bar2 <- paste(rep("=", pct2 %/% 5), collapse = "")

  cat(sprintf("%-15s %s %5.1f%%\n", x$team1, bar1, x$win_prob_team1 * 100))
  cat(sprintf("%-15s %s %5.1f%%\n", x$team2, bar2, x$win_prob_team2 * 100))

  cli::cli_h3("Expected Margin")
  if (x$expected_margin > 0) {
    cli::cli_text("{x$team1} by {abs(round(x$expected_margin))} runs equivalent")
  } else if (x$expected_margin < 0) {
    cli::cli_text("{x$team2} by {abs(round(x$expected_margin))} runs equivalent")
  } else {
    cli::cli_text("Even match")
  }

  cli::cli_alert_info("Favorite: {x$favorite}")

  invisible(x)
}


# ============================================================================
# Utility Functions
# ============================================================================

#' Search Players
#'
#' Search for players by name pattern.
#'
#' @param pattern Character. Search pattern (partial match).
#' @param limit Integer. Maximum results to return. Default 10.
#' @param db_path Character. Database path.
#'
#' @return Data frame of matching players.
#' @export
#'
#' @examples
#' \dontrun{
#' search_players("Kohli")
#' search_players("Smith")
#' }
search_players <- function(pattern, limit = 10, db_path = NULL) {

  # Validate inputs early
  if (missing(pattern) || is.null(pattern)) {
    cli::cli_abort("pattern is required")
  }
  if (!is.character(pattern) || length(pattern) != 1) {
    cli::cli_abort("pattern must be a single character string")
  }
  if (!is.numeric(limit) || limit < 1) {
    cli::cli_abort("limit must be a positive number")
  }

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  result <- DBI::dbGetQuery(conn, "
    SELECT player_id, player_name, country, batting_style, bowling_style
    FROM players
    WHERE LOWER(player_name) LIKE LOWER(?)
    ORDER BY player_name
    LIMIT ?
  ", params = list(paste0("%", pattern, "%"), limit))

  if (nrow(result) == 0) {
    cli::cli_alert_info("No players found matching '{pattern}'")
  }

  return(result)
}


#' Search Teams
#'
#' Search for teams in the database.
#'
#' @param pattern Character. Search pattern (partial match). If NULL, lists all teams.
#' @param limit Integer. Maximum results. Default 20.
#' @param db_path Character. Database path.
#'
#' @return Data frame of matching teams with match counts.
#' @export
#'
#' @examples
#' \dontrun{
#' search_teams()  # List all teams
#' search_teams("India")
#' }
search_teams <- function(pattern = NULL, limit = 20, db_path = NULL) {

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  if (is.null(pattern)) {
    result <- DBI::dbGetQuery(conn, "
      SELECT
        team_id as team,
        COUNT(*) as matches,
        MAX(match_date) as last_match,
        ROUND(MAX(elo_result), 0) as current_elo
      FROM team_elo
      GROUP BY team_id
      ORDER BY matches DESC
      LIMIT ?
    ", params = list(limit))
  } else {
    result <- DBI::dbGetQuery(conn, "
      SELECT
        team_id as team,
        COUNT(*) as matches,
        MAX(match_date) as last_match,
        ROUND(MAX(elo_result), 0) as current_elo
      FROM team_elo
      WHERE LOWER(team_id) LIKE LOWER(?)
      GROUP BY team_id
      ORDER BY matches DESC
      LIMIT ?
    ", params = list(paste0("%", pattern, "%"), limit))
  }

  return(result)
}
