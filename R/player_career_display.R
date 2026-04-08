# Player Career Display
# ======================
# User-facing functions for displaying career ratings and leaderboards.


#' Player Career Ratings Leaderboard
#'
#' Returns a leaderboard of player career ratings combining EPR, PSR,
#' and BOUNCER composite ratings.
#'
#' @param format Character. "t20", "odi", or "test".
#' @param role_filter Character. Filter by role: "all", "BATTER", "BOWLER",
#'   or "ALL_ROUNDER". Default "all".
#' @param n_top Integer. Number of players to show (NULL = all).
#' @param min_matches Integer. Minimum matches to include (default 10).
#'
#' @return data.table with career ratings sorted by BOUNCER rating.
#' @export
player_career_ratings <- function(format = c("t20", "odi", "test"),
                                   role_filter = "all",
                                   n_top = 20,
                                   min_matches = 10) {
  format <- match.arg(format)

  result <- bouncer_ratings(format)

  if (nrow(result) == 0) {
    cli::cli_warn("No career ratings available for {toupper(format)}")
    return(data.table::data.table())
  }

  # Filter by role
  if (role_filter != "all") {
    result <- result[role_group == toupper(role_filter)]
  }

  # Filter by minimum matches
  result <- result[n_matches >= min_matches]

  # Select display columns
  display_cols <- intersect(
    c("player_id", "role_group", "bouncer_rating",
      "total_epr", "batting_epr", "bowling_epr",
      "psr", "n_matches", "wt_matches"),
    names(result)
  )
  result <- result[, display_cols, with = FALSE]

  # Round for display
  num_cols <- setdiff(display_cols, c("player_id", "role_group", "n_matches"))
  for (col in num_cols) {
    if (col %in% names(result) && is.numeric(result[[col]])) {
      data.table::set(result, j = col, value = round(result[[col]], 3))
    }
  }

  if (!is.null(n_top)) {
    result <- utils::head(result, n_top)
  }

  result
}


#' Player Career Summary
#'
#' Detailed career summary for a single player across all value dimensions.
#'
#' @param player_id Character. Player identifier.
#' @param format Character. "t20", "odi", or "test".
#'
#' @return Named list with career stats, ratings, and per-match history.
#' @export
player_career_summary <- function(player_id, format = c("t20", "odi", "test")) {
  format <- match.arg(format)

  # Load per-match data
  pgd <- tryCatch(
    load_player_game_data(format, player_ids = player_id),
    error = function(e) {
      cli::cli_warn("Failed to load player game data: {e$message}")
      data.table::data.table()
    }
  )

  if (nrow(pgd) == 0) {
    cli::cli_warn("No data found for player {player_id} in {toupper(format)}")
    return(NULL)
  }

  # Career aggregates
  career <- pgd[, .(
    matches = .N,
    # Batting
    total_batting_runs = sum(batting_runs, na.rm = TRUE),
    total_balls_faced = sum(batting_balls_faced, na.rm = TRUE),
    batting_avg = sum(batting_runs, na.rm = TRUE) / max(sum(batting_dismissed, na.rm = TRUE), 1),
    career_strike_rate = sum(batting_runs, na.rm = TRUE) * 100 / max(sum(batting_balls_faced, na.rm = TRUE), 1),
    total_batting_wpa = sum(batting_wpa, na.rm = TRUE),
    total_batting_era = sum(batting_era, na.rm = TRUE),
    # Bowling
    total_bowling_wickets = sum(bowling_wickets, na.rm = TRUE),
    total_balls_bowled = sum(bowling_balls_bowled, na.rm = TRUE),
    career_economy = sum(bowling_runs_conceded, na.rm = TRUE) * 6 / max(sum(bowling_balls_bowled, na.rm = TRUE), 1),
    total_bowling_wpa = sum(bowling_wpa, na.rm = TRUE),
    total_bowling_era = sum(bowling_era, na.rm = TRUE),
    # Combined
    total_wpa = sum(total_wpa, na.rm = TRUE),
    total_era = sum(total_era, na.rm = TRUE)
  )]

  list(
    player_id = player_id,
    format = format,
    career = career,
    match_history = pgd
  )
}
