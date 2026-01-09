# ELO Rating Processing Functions
#
# Functions for calculating and storing ball-by-ball ELO ratings

#' Calculate All Player ELOs
#'
#' Main function to calculate and store ELO ratings for every delivery.
#' Supports both fresh calculation (from scratch) and incremental processing
#' (resume from last processed match).
#'
#' @param db_path Database path (NULL = use default from saved path or get_db_path())
#' @param fresh Logical. If TRUE, clears existing ELO data and recalculates everything.
#'              If FALSE, resumes from last processed match (incremental mode).
#' @param match_limit Integer. Limit number of matches (for testing)
#' @param batch_size Integer. Number of deliveries to process before committing
#'
#' @return Invisibly returns a list of player ELO ratings
#' @export
#'
#' @examples
#' \dontrun{
#'   # Test with 10 matches
#'   calculate_all_player_elos(match_limit = 10, fresh = TRUE)
#'
#'   # Process all matches from scratch
#'   calculate_all_player_elos(fresh = TRUE)
#'
#'   # Resume from last processed match (incremental)
#'   calculate_all_player_elos(fresh = FALSE)
#' }
calculate_all_player_elos <- function(db_path = NULL,
                                       fresh = FALSE,
                                       match_limit = 10,
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

  # Ensure all previous DuckDB connections are closed
  tryCatch({
    duckdb::duckdb_shutdown(duckdb::duckdb())
  }, error = function(e) {
    # Ignore if no connections to close
  })

  # Small delay to ensure cleanup
  Sys.sleep(0.5)

  # Connect to database
  conn <- get_db_connection(path = db_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Mode indicator
  if (fresh) {
    cli::cli_alert_warning("Mode: FRESH START (will clear existing ELO data)")
  } else {
    cli::cli_alert_info("Mode: INCREMENTAL (will resume from last processed match)")
  }

  # Fresh mode: Clear all ELO data
  if (fresh) {
    clear_elo_data(conn)
  }

  # Load deliveries to process
  delivery_info <- load_deliveries_to_process(conn, fresh, match_limit)

  if (is.null(delivery_info)) {
    return(invisible(NULL))
  }

  deliveries <- delivery_info$deliveries
  matches_with_elo <- delivery_info$matches_with_elo

  # Initialize player ELO tracking
  player_state <- initialize_player_state(conn, fresh, matches_with_elo)

  # Process deliveries
  player_state <- process_all_deliveries(
    conn = conn,
    deliveries = deliveries,
    player_state = player_state,
    batch_size = batch_size
  )

  # Show summary
  show_elo_summary(conn, player_state)

  invisible(list(
    batting_elos = player_state$player_batting_elos,
    bowling_elos = player_state$player_bowling_elos
  ))
}


#' Clear ELO Data
#'
#' @keywords internal
clear_elo_data <- function(conn) {
  cli::cli_alert_warning("Clearing existing ELO data...")

  # Wrap in transaction for safety
  DBI::dbBegin(conn)
  tryCatch({
    DBI::dbExecute(conn, "
      UPDATE deliveries SET
        batter_elo_before = NULL,
        bowler_elo_before = NULL,
        batter_elo_after = NULL,
        bowler_elo_after = NULL
    ")

    # Also clear player_elo_history
    DBI::dbExecute(conn, "DELETE FROM player_elo_history")

    DBI::dbCommit(conn)
    cli::cli_alert_success("ELO data cleared")
  }, error = function(e) {
    DBI::dbRollback(conn)
    stop("Failed to reset ELO data: ", conditionMessage(e))
  })
}


#' Load Deliveries to Process
#'
#' @keywords internal
load_deliveries_to_process <- function(conn, fresh, match_limit) {
  cli::cli_alert_info("Loading deliveries (this may take a while)...")

  # Find matches that already have ELO data (incremental mode)
  matches_with_elo <- character(0)
  if (!fresh) {
    matches_with_elo <- DBI::dbGetQuery(conn, "
      SELECT DISTINCT match_id
      FROM deliveries
      WHERE batter_elo_after IS NOT NULL
    ")$match_id

    if (length(matches_with_elo) > 0) {
      cli::cli_alert_info("Found {length(matches_with_elo)} matches already processed")
    } else {
      cli::cli_alert_info("No matches processed yet, will start from beginning")
    }
  }

  # Build query to get matches needing processing
  query <- build_delivery_query(conn, matches_with_elo, match_limit)

  deliveries <- DBI::dbGetQuery(conn, query)

  if (nrow(deliveries) == 0) {
    cli::cli_alert_success("All matches already processed!")
    return(NULL)
  }

  cli::cli_alert_success("Loaded {nrow(deliveries)} deliveries to process")
  if (!fresh && length(matches_with_elo) > 0) {
    cli::cli_alert_info("Skipped {length(matches_with_elo)} already-processed matches")
  }

  list(
    deliveries = deliveries,
    matches_with_elo = matches_with_elo
  )
}


#' Build Delivery Query
#'
#' @keywords internal
build_delivery_query <- function(conn, matches_with_elo, match_limit) {
  if (!is.null(match_limit)) {
    # Limit by matches, not deliveries
    if (length(matches_with_elo) > 0) {
      match_ids_str <- paste0("'", matches_with_elo, "'", collapse = ", ")
      match_query <- sprintf("
        SELECT match_id FROM matches
        WHERE match_date IS NOT NULL
          AND match_id NOT IN (%s)
        ORDER BY match_date ASC
        LIMIT %d
      ", match_ids_str, match_limit)
    } else {
      match_query <- sprintf("
        SELECT match_id FROM matches
        WHERE match_date IS NOT NULL
        ORDER BY match_date ASC
        LIMIT %d
      ", match_limit)
    }

    matches_to_process <- DBI::dbGetQuery(conn, match_query)

    if (nrow(matches_to_process) == 0) {
      return(NULL)
    }

    match_ids <- paste0("'", matches_to_process$match_id, "'", collapse = ", ")

    sprintf("
      SELECT delivery_id, match_id, match_date, match_type, season,
             batter_id, bowler_id, runs_batter, is_wicket, is_boundary
      FROM deliveries
      WHERE match_date IS NOT NULL
        AND match_id IN (%s)
      ORDER BY match_date ASC, match_id ASC, delivery_id ASC
    ", match_ids)
  } else {
    # Process all unprocessed matches
    if (length(matches_with_elo) > 0) {
      match_ids_str <- paste0("'", matches_with_elo, "'", collapse = ", ")
      sprintf("
        SELECT delivery_id, match_id, match_date, match_type, season,
               batter_id, bowler_id, runs_batter, is_wicket, is_boundary
        FROM deliveries
        WHERE match_date IS NOT NULL
          AND match_id NOT IN (%s)
        ORDER BY match_date ASC, match_id ASC, delivery_id ASC
      ", match_ids_str)
    } else {
      "
        SELECT delivery_id, match_id, match_date, match_type, season,
               batter_id, bowler_id, runs_batter, is_wicket, is_boundary
        FROM deliveries
        WHERE match_date IS NOT NULL
        ORDER BY match_date ASC, match_id ASC, delivery_id ASC
      "
    }
  }
}


#' Initialize Player State
#'
#' @keywords internal
initialize_player_state <- function(conn, fresh, matches_with_elo) {
  player_batting_elos <- list()
  player_bowling_elos <- list()
  player_match_counts <- list()

  # Incremental mode: Load last known ELO state from player_elo_history
  if (!fresh && length(matches_with_elo) > 0) {
    cli::cli_alert_info("Loading player ELO state from last processed match...")

    # Get the most recent ELO for each player
    elo_state <- DBI::dbGetQuery(conn, "
      WITH ranked_elos AS (
        SELECT
          player_id,
          elo_batting,
          elo_bowling,
          ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY match_date DESC, match_id DESC) as rn
        FROM player_elo_history
      )
      SELECT player_id, elo_batting, elo_bowling
      FROM ranked_elos
      WHERE rn = 1
    ")

    if (nrow(elo_state) > 0) {
      for (i in 1:nrow(elo_state)) {
        player_id <- elo_state$player_id[i]
        # Load separate batting and bowling ELOs
        player_batting_elos[[player_id]] <- elo_state$elo_batting[i]
        player_bowling_elos[[player_id]] <- elo_state$elo_bowling[i]
        # Estimate match count (not critical for ELO, used for K-factor)
        player_match_counts[[player_id]] <- 10  # Assume experienced player
      }
      cli::cli_alert_success("Loaded ELO state for {length(player_batting_elos)} players")
    }
  }

  list(
    player_batting_elos = player_batting_elos,
    player_bowling_elos = player_bowling_elos,
    player_match_counts = player_match_counts
  )
}


#' Process All Deliveries
#'
#' @keywords internal
process_all_deliveries <- function(conn, deliveries, player_state, batch_size) {
  total_deliveries <- nrow(deliveries)
  cli::cli_progress_bar(
    "Processing deliveries",
    total = total_deliveries,
    format = "{cli::pb_spin} {cli::pb_current}/{cli::pb_total} [{cli::pb_percent}] ETA: {cli::pb_eta}"
  )

  updates <- list()
  current_match <- NULL
  matches_processed <- 0

  player_batting_elos <- player_state$player_batting_elos
  player_bowling_elos <- player_state$player_bowling_elos
  player_match_counts <- player_state$player_match_counts

  for (i in seq_len(nrow(deliveries))) {
    delivery <- deliveries[i, ]

    # Update progress bar
    cli::cli_progress_update()

    # Track when we start a new match (for player_elo_history snapshots)
    if (is.null(current_match) || current_match != delivery$match_id) {
      # Save ELO snapshot for previous match
      if (!is.null(current_match)) {
        save_match_elo_snapshot(conn, current_match, player_batting_elos, player_bowling_elos, deliveries[i-1, ])
        matches_processed <- matches_processed + 1
      }
      current_match <- delivery$match_id
    }

    batter_id <- delivery$batter_id
    bowler_id <- delivery$bowler_id

    # Initialize players if new
    if (!(batter_id %in% names(player_batting_elos))) {
      player_batting_elos[[batter_id]] <- ELO_START_RATING
      player_bowling_elos[[batter_id]] <- ELO_START_RATING
      player_match_counts[[batter_id]] <- 0
    }

    if (!(bowler_id %in% names(player_bowling_elos))) {
      player_batting_elos[[bowler_id]] <- ELO_START_RATING
      player_bowling_elos[[bowler_id]] <- ELO_START_RATING
      player_match_counts[[bowler_id]] <- 0
    }

    # Get current ELOs (before this delivery)
    batter_elo_before <- player_batting_elos[[batter_id]]
    bowler_elo_before <- player_bowling_elos[[bowler_id]]

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

    # Store the updates separately for batting and bowling
    player_batting_elos[[batter_id]] <- batter_elo_after
    player_bowling_elos[[bowler_id]] <- bowler_elo_after

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
    save_match_elo_snapshot(conn, current_match, player_batting_elos, player_bowling_elos, deliveries[nrow(deliveries), ])
    matches_processed <- matches_processed + 1
  }

  cli::cli_progress_done()

  cli::cli_alert_success("ELO calculation complete!")
  cli::cli_alert_info("Processed {matches_processed} matches and {total_deliveries} deliveries")

  list(
    player_batting_elos = player_batting_elos,
    player_bowling_elos = player_bowling_elos,
    player_match_counts = player_match_counts
  )
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
save_match_elo_snapshot <- function(conn, match_id, player_batting_elos, player_bowling_elos, last_delivery) {
  # Get unique players from this match
  players_in_match <- DBI::dbGetQuery(conn, "
    SELECT DISTINCT batter_id as player_id FROM deliveries WHERE match_id = ?
    UNION
    SELECT DISTINCT bowler_id as player_id FROM deliveries WHERE match_id = ?
  ", params = list(match_id, match_id))

  # Save snapshot for each player
  for (player_id in players_in_match$player_id) {
    if (player_id %in% names(player_batting_elos) && player_id %in% names(player_bowling_elos)) {
      batting_elo <- player_batting_elos[[player_id]]
      bowling_elo <- player_bowling_elos[[player_id]]

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
        batting_elo,  # Batting ELO
        bowling_elo,  # Bowling ELO (now separate!)
        batting_elo, batting_elo, batting_elo,  # Format-specific batting (simplified)
        bowling_elo, bowling_elo, bowling_elo   # Format-specific bowling (simplified)
      ))
    }
  }
}


#' Show ELO Summary
#'
#' @keywords internal
show_elo_summary <- function(conn, player_state) {
  cli::cli_h2("ELO Calculation Summary")

  # Count deliveries with ELOs
  elo_count <- DBI::dbGetQuery(conn, "
    SELECT COUNT(*) as n
    FROM deliveries
    WHERE batter_elo_after IS NOT NULL
  ")$n

  total_count <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM deliveries")$n

  cli::cli_alert_info("Deliveries with ELO: {elo_count} / {total_count}")
  cli::cli_alert_info("Unique players: {length(player_state$player_batting_elos)}")

  # Show top 10 batters by batting ELO
  top_batters <- head(
    player_state$player_batting_elos[order(unlist(player_state$player_batting_elos), decreasing = TRUE)],
    10
  )

  cli::cli_h3("Top 10 Batters by Batting ELO")
  for (i in seq_along(top_batters)) {
    player_id <- names(top_batters)[i]
    elo <- top_batters[[i]]
    cli::cli_alert_info("{i}. {player_id}: {round(elo, 0)}")
  }

  # Show top 10 bowlers by bowling ELO
  top_bowlers <- head(
    player_state$player_bowling_elos[order(unlist(player_state$player_bowling_elos), decreasing = TRUE)],
    10
  )

  cli::cli_h3("Top 10 Bowlers by Bowling ELO")
  for (i in seq_along(top_bowlers)) {
    player_id <- names(top_bowlers)[i]
    elo <- top_bowlers[[i]]
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


#' View Latest ELO Rankings
#'
#' Displays the latest ELO rankings for all players, showing top performers
#' and struggling players for both batting and bowling.
#'
#' @param db_path Database path (NULL = use default)
#' @param top_n Number of top players to show (default: 20)
#' @param bottom_n Number of bottom players to show (default: 10)
#'
#' @return Invisibly returns a data frame with all latest ELOs
#' @export
#'
#' @examples
#' \dontrun{
#'   # View default rankings
#'   view_latest_elos()
#'
#'   # Show top 30 and bottom 5
#'   view_latest_elos(top_n = 30, bottom_n = 5)
#' }
view_latest_elos <- function(db_path = NULL, top_n = 20, bottom_n = 10) {

  cli::cli_h2("Latest ELO Rankings")

  # Get database path
  if (is.null(db_path)) {
    if (file.exists("data-raw/.db_path.rds")) {
      db_path <- readRDS("data-raw/.db_path.rds")
    } else {
      db_path <- get_db_path()
    }
  }

  conn <- get_db_connection(path = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Get latest ELO for each player
  latest_elos <- DBI::dbGetQuery(conn, "
    WITH ranked_elos AS (
      SELECT
        player_id,
        elo_batting,
        elo_bowling,
        match_date,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY match_date DESC, match_id DESC) as rn
      FROM player_elo_history
    )
    SELECT player_id, elo_batting, elo_bowling, match_date
    FROM ranked_elos
    WHERE rn = 1
    ORDER BY elo_batting DESC
  ")

  if (nrow(latest_elos) == 0) {
    cli::cli_alert_warning("No ELO data found. Run calculate_all_player_elos() first.")
    return(invisible(NULL))
  }

  # Top Batters
  cli::cli_h3("Top {top_n} Batters by ELO")
  top_batters <- head(latest_elos[order(latest_elos$elo_batting, decreasing = TRUE), ], top_n)
  top_batters$rank <- 1:nrow(top_batters)
  top_batters$elo_batting <- round(top_batters$elo_batting, 0)
  print(top_batters[, c("rank", "player_id", "elo_batting", "match_date")])

  cat("\n")

  # Bottom Batters
  cli::cli_h3("Bottom {bottom_n} Batters by ELO")
  bottom_batters <- tail(latest_elos[order(latest_elos$elo_batting, decreasing = TRUE), ], bottom_n)
  bottom_batters$rank <- (nrow(latest_elos) - bottom_n + 1):nrow(latest_elos)
  bottom_batters$elo_batting <- round(bottom_batters$elo_batting, 0)
  print(bottom_batters[, c("rank", "player_id", "elo_batting", "match_date")])

  cat("\n")

  # Top Bowlers
  cli::cli_h3("Top {top_n} Bowlers by ELO")
  top_bowlers <- head(latest_elos[order(latest_elos$elo_bowling, decreasing = TRUE), ], top_n)
  top_bowlers$rank <- 1:nrow(top_bowlers)
  top_bowlers$elo_bowling <- round(top_bowlers$elo_bowling, 0)
  print(top_bowlers[, c("rank", "player_id", "elo_bowling", "match_date")])

  cat("\n")

  # Bottom Bowlers
  cli::cli_h3("Bottom {bottom_n} Bowlers by ELO")
  bottom_bowlers <- tail(latest_elos[order(latest_elos$elo_bowling, decreasing = TRUE), ], bottom_n)
  bottom_bowlers$rank <- (nrow(latest_elos) - bottom_n + 1):nrow(latest_elos)
  bottom_bowlers$elo_bowling <- round(bottom_bowlers$elo_bowling, 0)
  print(bottom_bowlers[, c("rank", "player_id", "elo_bowling", "match_date")])

  cat("\n")

  # Summary stats
  cli::cli_h3("ELO Distribution Summary")
  cli::cli_alert_info("Total players: {nrow(latest_elos)}")
  cli::cli_alert_info("Batting ELO - Mean: {round(mean(latest_elos$elo_batting), 0)}, Median: {round(median(latest_elos$elo_batting), 0)}, SD: {round(sd(latest_elos$elo_batting), 0)}")
  cli::cli_alert_info("Bowling ELO - Mean: {round(mean(latest_elos$elo_bowling), 0)}, Median: {round(median(latest_elos$elo_bowling), 0)}, SD: {round(sd(latest_elos$elo_bowling), 0)}")

  invisible(latest_elos)
}
