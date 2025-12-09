# Calculate Player ELO Ratings for Every Ball
#
# This script processes all matches chronologically and calculates ELO ratings
# for both batter and bowler AFTER EVERY SINGLE DELIVERY.
#
# The ELO ratings are stored directly in the deliveries table, so you can
# look up any player's exact ELO at any point in any match using delivery_id.
#
# Usage:
#   source("data-raw/02_calculate_player_elos.R")
#   calculate_all_player_elos()

library(bouncer)
library(DBI)

#' Calculate All Player ELOs
#'
#' Main function to calculate and store ELO ratings for every delivery.
#'
#' @param db_path Database path (NULL = use default from previous script)
#' @param reset Logical. If TRUE, clears existing ELO data first
#' @param match_limit Integer. Limit number of matches (for testing)
#' @param batch_size Integer. Number of deliveries to process before committing
calculate_all_player_elos <- function(db_path = NULL,
                                       reset = FALSE,
                                       match_limit = NULL,
                                       batch_size = 10000) {

  cli::cli_h1("Calculating Player ELO Ratings")

  # Get database path
  if (is.null(db_path)) {
    if (file.exists("data-raw/.db_path.rds")) {
      db_path <- readRDS("data-raw/.db_path.rds")
      cli::cli_alert_info("Using database: {.file {db_path}}")
    } else {
      db_path <- get_db_path()
    }
  }

  # Connect to database
  conn <- get_db_connection(path = db_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Optionally reset ELO columns
  if (reset) {
    cli::cli_alert_warning("Resetting ELO columns...")
    DBI::dbExecute(conn, "
      UPDATE deliveries SET
        batter_elo_before = NULL,
        bowler_elo_before = NULL,
        batter_elo_after = NULL,
        bowler_elo_after = NULL
    ")

    # Also clear player_elo_history
    DBI::dbExecute(conn, "DELETE FROM player_elo_history")

    cli::cli_alert_success("ELO data cleared")
  }

  # Get all deliveries in chronological order
  cli::cli_alert_info("Loading deliveries (this may take a while)...")

  query <- "
    SELECT delivery_id, match_id, match_date, match_type, season,
           batter_id, bowler_id, runs_batter, is_wicket, is_boundary
    FROM deliveries
    WHERE match_date IS NOT NULL
    ORDER BY match_date ASC, match_id ASC, delivery_id ASC
  "

  if (!is.null(match_limit)) {
    # Limit by matches, not deliveries
    match_query <- sprintf("
      SELECT match_id FROM matches
      WHERE match_date IS NOT NULL
      ORDER BY match_date ASC
      LIMIT %d
    ", match_limit)

    limited_matches <- DBI::dbGetQuery(conn, match_query)
    match_ids <- paste0("'", limited_matches$match_id, "'", collapse = ", ")
    query <- sprintf("%s AND match_id IN (%s)", query, match_ids)
  }

  deliveries <- DBI::dbGetQuery(conn, query)

  cli::cli_alert_success("Loaded {nrow(deliveries)} deliveries")

  # Initialize player ELO tracking
  player_elos <- list()
  player_match_counts <- list()

  # Process deliveries in batches
  cli::cli_progress_bar("Processing deliveries", total = nrow(deliveries))

  updates <- list()
  current_match <- NULL

  for (i in seq_len(nrow(deliveries))) {
    delivery <- deliveries[i, ]

    cli::cli_progress_update()

    # Track when we start a new match (for player_elo_history snapshots)
    if (is.null(current_match) || current_match != delivery$match_id) {
      # Save ELO snapshot for previous match
      if (!is.null(current_match)) {
        save_match_elo_snapshot(conn, current_match, player_elos, deliveries[i-1, ])
      }
      current_match <- delivery$match_id
    }

    batter_id <- delivery$batter_id
    bowler_id <- delivery$bowler_id

    # Initialize players if new
    if (!(batter_id %in% names(player_elos))) {
      player_elos[[batter_id]] <- ELO_START_RATING
      player_match_counts[[batter_id]] <- 0
    }

    if (!(bowler_id %in% names(player_elos))) {
      player_elos[[bowler_id]] <- ELO_START_RATING
      player_match_counts[[bowler_id]] <- 0
    }

    # Get current ELOs (before this delivery)
    batter_elo_before <- player_elos[[batter_id]]
    bowler_elo_before <- player_elos[[bowler_id]]

    # Calculate new ELOs
    batter_elo_after <- update_batting_elo(
      current_batting_elo = batter_elo_before,
      bowling_elo = bowler_elo_before,
      runs_batter = delivery$runs_batter,
      is_wicket = delivery$is_wicket,
      is_boundary = delivery$is_boundary,
      match_type = delivery$match_type,
      player_matches = player_match_counts[[batter_id]]
    )

    bowler_elo_after <- update_bowling_elo(
      current_bowling_elo = bowler_elo_before,
      batting_elo = batter_elo_before,
      runs_batter = delivery$runs_batter,
      is_wicket = delivery$is_wicket,
      is_boundary = delivery$is_boundary,
      match_type = delivery$match_type,
      player_matches = player_match_counts[[bowler_id]]
    )

    # Store the updates
    player_elos[[batter_id]] <- batter_elo_after
    player_elos[[bowler_id]] <- bowler_elo_after

    # Queue database update
    updates[[length(updates) + 1]] <- list(
      delivery_id = delivery$delivery_id,
      batter_elo_before = batter_elo_before,
      bowler_elo_before = bowler_elo_before,
      batter_elo_after = batter_elo_after,
      bowler_elo_after = bowler_elo_after
    )

    # Commit updates in batches
    if (length(updates) >= batch_size) {
      apply_elo_updates(conn, updates)
      updates <- list()
    }
  }

  # Apply any remaining updates
  if (length(updates) > 0) {
    apply_elo_updates(conn, updates)
  }

  # Save final match snapshot
  if (!is.null(current_match)) {
    save_match_elo_snapshot(conn, current_match, player_elos, deliveries[nrow(deliveries), ])
  }

  cli::cli_progress_done()

  cli::cli_alert_success("ELO calculation complete!")

  # Show summary
  show_elo_summary(conn, player_elos)

  invisible(player_elos)
}


#' Apply ELO Updates to Database
#'
#' @keywords internal
apply_elo_updates <- function(conn, updates) {
  if (length(updates) == 0) return(invisible(NULL))

  cli::cli_alert_info("Committing {length(updates)} ELO updates...")

  # Build batch update
  DBI::dbBegin(conn)

  tryCatch({
    for (update in updates) {
      DBI::dbExecute(conn, "
        UPDATE deliveries
        SET batter_elo_before = ?,
            bowler_elo_before = ?,
            batter_elo_after = ?,
            bowler_elo_after = ?
        WHERE delivery_id = ?
      ", params = list(
        update$batter_elo_before,
        update$bowler_elo_before,
        update$batter_elo_after,
        update$bowler_elo_after,
        update$delivery_id
      ))
    }

    DBI::dbCommit(conn)
  }, error = function(e) {
    DBI::dbRollback(conn)
    cli::cli_alert_danger("Error updating ELOs: {e$message}")
  })
}


#' Save Match ELO Snapshot
#'
#' Saves ELO ratings to player_elo_history table at the end of each match.
#'
#' @keywords internal
save_match_elo_snapshot <- function(conn, match_id, player_elos, last_delivery) {
  # Get unique players from this match
  players_in_match <- DBI::dbGetQuery(conn, "
    SELECT DISTINCT batter_id as player_id FROM deliveries WHERE match_id = ?
    UNION
    SELECT DISTINCT bowler_id as player_id FROM deliveries WHERE match_id = ?
  ", params = list(match_id, match_id))

  # Save snapshot for each player
  for (player_id in players_in_match$player_id) {
    if (player_id %in% names(player_elos)) {
      elo <- player_elos[[player_id]]

      DBI::dbExecute(conn, "
        INSERT OR REPLACE INTO player_elo_history
        (player_id, match_id, match_date, match_type,
         elo_batting, elo_bowling,
         elo_batting_test, elo_batting_odi, elo_batting_t20,
         elo_bowling_test, elo_bowling_odi, elo_bowling_t20)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ", params = list(
        player_id,
        match_id,
        last_delivery$match_date,
        last_delivery$match_type,
        elo,  # Overall ELO (simplified - same for all formats for now)
        elo,
        elo, elo, elo,  # Format-specific (simplified)
        elo, elo, elo
      ))
    }
  }
}


#' Show ELO Summary
#'
#' @keywords internal
show_elo_summary <- function(conn, player_elos) {
  cli::cli_h2("ELO Calculation Summary")

  # Count deliveries with ELOs
  elo_count <- DBI::dbGetQuery(conn, "
    SELECT COUNT(*) as n
    FROM deliveries
    WHERE batter_elo_after IS NOT NULL
  ")$n

  total_count <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM deliveries")$n

  cli::cli_alert_info("Deliveries with ELO: {elo_count} / {total_count}")
  cli::cli_alert_info("Unique players: {length(player_elos)}")

  # Show top 10 players by ELO
  top_players <- head(
    player_elos[order(unlist(player_elos), decreasing = TRUE)],
    10
  )

  cli::cli_h3("Top 10 Players by ELO")
  for (i in seq_along(top_players)) {
    player_id <- names(top_players)[i]
    elo <- top_players[[i]]
    cli::cli_alert_info("{i}. {player_id}: {round(elo, 0)}")
  }

  # Verify we can query ELOs
  cli::cli_h3("Verification")
  sample_query <- DBI::dbGetQuery(conn, "
    SELECT delivery_id, batter_id, bowler_id,
           batter_elo_before, batter_elo_after,
           bowler_elo_before, bowler_elo_after
    FROM deliveries
    WHERE batter_elo_after IS NOT NULL
    LIMIT 5
  ")

  if (nrow(sample_query) > 0) {
    cli::cli_alert_success("ELO columns populated successfully!")
    cli::cli_alert_info("Sample query:")
    print(sample_query)
  }
}


# Example usage:
# source("data-raw/02_calculate_player_elos.R")
#
# # Test with limited matches first
# calculate_all_player_elos(match_limit = 10, reset = TRUE)
#
# # Then process everything
# calculate_all_player_elos(reset = TRUE)
