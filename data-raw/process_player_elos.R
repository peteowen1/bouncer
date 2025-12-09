# Process Player ELO Ratings for All Matches
#
# This script calculates ELO ratings for all players across all matches
# in chronological order. It processes deliveries and updates ELO ratings
# after each ball, storing the results in the player_elo_history table.
#
# Usage:
#   source("data-raw/process_player_elos.R")
#   process_all_player_elos()

library(bouncer)
library(dplyr)
library(DBI)

#' Process All Player ELOs
#'
#' Main function to calculate ELO ratings for all players.
#'
#' @param db_path Database path
#' @param reset Logical. If TRUE, clears existing ELO history first
#' @param match_limit Integer. Limit number of matches to process (for testing)
process_all_player_elos <- function(db_path = NULL,
                                     reset = FALSE,
                                     match_limit = NULL) {

  cli::cli_h1("Processing Player ELO Ratings")

  # Get database connection
  conn <- get_db_connection(path = db_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Optionally reset ELO history
  if (reset) {
    cli::cli_alert_warning("Resetting ELO history...")
    DBI::dbExecute(conn, "DELETE FROM player_elo_history")
    cli::cli_alert_success("ELO history cleared")
  }

  # Get all matches in chronological order
  cli::cli_alert_info("Fetching matches...")

  query <- "
    SELECT match_id, match_date, match_type, season
    FROM matches
    WHERE match_date IS NOT NULL
    ORDER BY match_date ASC, match_id ASC
  "

  if (!is.null(match_limit)) {
    query <- paste(query, "LIMIT", match_limit)
  }

  matches <- DBI::dbGetQuery(conn, query)

  cli::cli_alert_success("Found {nrow(matches)} matches to process")

  # Initialize player ELO tracking
  player_elos <- list()

  # Process each match
  cli::cli_progress_bar("Processing matches", total = nrow(matches))

  for (i in seq_len(nrow(matches))) {
    match_row <- matches[i, ]

    cli::cli_progress_update()

    # Process this match
    tryCatch({
      player_elos <- process_match_elos(
        conn,
        match_row$match_id,
        match_row$match_date,
        match_row$match_type,
        match_row$season,
        player_elos
      )
    }, error = function(e) {
      cli::cli_alert_warning("Error processing match {match_row$match_id}: {e$message}")
    })
  }

  cli::cli_progress_done()
  cli::cli_alert_success("ELO processing complete!")

  # Show summary
  cli::cli_h2("Summary")
  cli::cli_alert_info("Total players processed: {length(player_elos)}")

  # Show top 10 batters
  top_batters <- head(
    player_elos[order(sapply(player_elos, function(x) x$batting), decreasing = TRUE)],
    10
  )

  cli::cli_h3("Top 10 Batters (Overall ELO)")
  for (j in seq_along(top_batters)) {
    player <- top_batters[[j]]
    cli::cli_alert_info("{j}. {names(top_batters)[j]}: {round(player$batting, 0)}")
  }

  invisible(player_elos)
}


#' Process ELOs for a Single Match
#'
#' @keywords internal
process_match_elos <- function(conn,
                                match_id,
                                match_date,
                                match_type,
                                season,
                                player_elos) {

  # Get all deliveries for this match
  deliveries <- DBI::dbGetQuery(
    conn,
    "SELECT * FROM deliveries WHERE match_id = ? ORDER BY delivery_id ASC",
    params = list(match_id)
  )

  if (nrow(deliveries) == 0) {
    return(player_elos)
  }

  # Normalize match type
  match_type_norm <- normalize_match_type(match_type)

  # Process each delivery
  for (i in seq_len(nrow(deliveries))) {
    delivery <- deliveries[i, ]

    batter_id <- delivery$batter_id
    bowler_id <- delivery$bowler_id

    # Initialize players if new
    if (!(batter_id %in% names(player_elos))) {
      player_elos[[batter_id]] <- initialize_player_elos()
    }

    if (!(bowler_id %in% names(player_elos))) {
      player_elos[[bowler_id]] <- initialize_player_elos()
    }

    # Get current ELOs
    batter_elos <- player_elos[[batter_id]]
    bowler_elos <- player_elos[[bowler_id]]

    # Update batting ELO (overall)
    new_batting_elo <- update_batting_elo(
      current_batting_elo = batter_elos$batting,
      bowling_elo = bowler_elos$bowling,
      runs_batter = delivery$runs_batter,
      is_wicket = delivery$is_wicket,
      is_boundary = delivery$is_boundary,
      match_type = match_type,
      player_matches = batter_elos$matches
    )

    # Update bowling ELO (overall)
    new_bowling_elo <- update_bowling_elo(
      current_bowling_elo = bowler_elos$bowling,
      batting_elo = batter_elos$batting,
      runs_batter = delivery$runs_batter,
      is_wicket = delivery$is_wicket,
      is_boundary = delivery$is_boundary,
      match_type = match_type,
      player_matches = bowler_elos$matches
    )

    # Update format-specific ELOs
    batter_elos[[match_type_norm]]$batting <- update_batting_elo(
      current_batting_elo = batter_elos[[match_type_norm]]$batting,
      bowling_elo = bowler_elos[[match_type_norm]]$bowling,
      runs_batter = delivery$runs_batter,
      is_wicket = delivery$is_wicket,
      is_boundary = delivery$is_boundary,
      match_type = match_type,
      player_matches = batter_elos$matches
    )

    bowler_elos[[match_type_norm]]$bowling <- update_bowling_elo(
      current_bowling_elo = bowler_elos[[match_type_norm]]$bowling,
      batting_elo = batter_elos[[match_type_norm]]$batting,
      runs_batter = delivery$runs_batter,
      is_wicket = delivery$is_wicket,
      is_boundary = delivery$is_boundary,
      match_type = match_type,
      player_matches = bowler_elos$matches
    )

    # Store updated ELOs
    batter_elos$batting <- new_batting_elo
    bowler_elos$bowling <- new_bowling_elo

    player_elos[[batter_id]] <- batter_elos
    player_elos[[bowler_id]] <- bowler_elos
  }

  # After match, save ELO snapshots for all players who played
  players_in_match <- unique(c(deliveries$batter_id, deliveries$bowler_id))

  for (player_id in players_in_match) {
    if (player_id %in% names(player_elos)) {
      elos <- player_elos[[player_id]]

      # Increment match count
      elos$matches <- elos$matches + 1
      player_elos[[player_id]] <- elos

      # Save to database
      save_player_elo_snapshot(
        conn,
        player_id,
        match_id,
        match_date,
        match_type,
        elos
      )
    }
  }

  return(player_elos)
}


#' Initialize Player ELOs
#'
#' @keywords internal
initialize_player_elos <- function() {
  list(
    batting = ELO_START_RATING,
    bowling = ELO_START_RATING,
    test = list(batting = ELO_START_RATING, bowling = ELO_START_RATING),
    odi = list(batting = ELO_START_RATING, bowling = ELO_START_RATING),
    t20 = list(batting = ELO_START_RATING, bowling = ELO_START_RATING),
    matches = 0
  )
}


#' Save Player ELO Snapshot
#'
#' @keywords internal
save_player_elo_snapshot <- function(conn,
                                      player_id,
                                      match_id,
                                      match_date,
                                      match_type,
                                      elos) {

  # Prepare data
  snapshot <- data.frame(
    player_id = player_id,
    match_id = match_id,
    match_date = match_date,
    match_type = match_type,
    elo_batting = elos$batting,
    elo_bowling = elos$bowling,
    elo_batting_test = elos$test$batting,
    elo_batting_odi = elos$odi$batting,
    elo_batting_t20 = elos$t20$batting,
    elo_bowling_test = elos$test$bowling,
    elo_bowling_odi = elos$odi$bowling,
    elo_bowling_t20 = elos$t20$bowling,
    elo_batting_vs_opposition = NA_character_,
    elo_bowling_vs_opposition = NA_character_,
    stringsAsFactors = FALSE
  )

  # Insert or replace
  DBI::dbExecute(conn, "
    INSERT OR REPLACE INTO player_elo_history
    (player_id, match_id, match_date, match_type,
     elo_batting, elo_bowling,
     elo_batting_test, elo_batting_odi, elo_batting_t20,
     elo_bowling_test, elo_bowling_odi, elo_bowling_t20,
     elo_batting_vs_opposition, elo_bowling_vs_opposition)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ", params = as.list(snapshot))
}


# Example usage:
# source("data-raw/process_player_elos.R")
# process_all_player_elos(reset = TRUE, match_limit = 100)  # Test with 100 matches
# process_all_player_elos(reset = TRUE)  # Process all matches
