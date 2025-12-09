# Data Loading Functions

#' Load Match Data to Database
#'
#' Loads parsed cricket match data into the DuckDB database.
#'
#' @param parsed_data List returned from \code{parse_cricsheet_json}
#' @param path Database path. If NULL, uses default.
#' @param verify Logical. If TRUE, validates data before loading.
#'
#' @return Invisibly returns TRUE on success
#' @export
#'
#' @examples
#' \dontrun{
#' # Parse and load a match
#' match_data <- parse_cricsheet_json("match.json")
#' load_match_data(match_data)
#' }
load_match_data <- function(parsed_data, path = NULL, verify = TRUE) {
  if (verify) {
    validate_match_data(parsed_data)
  }

  conn <- get_db_connection(path = path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Start transaction
  DBI::dbBegin(conn)

  tryCatch({
    # Load matches
    if (nrow(parsed_data$match_info) > 0) {
      load_matches_to_db(conn, parsed_data$match_info)
    }

    # Load deliveries
    if (nrow(parsed_data$deliveries) > 0) {
      load_deliveries_to_db(conn, parsed_data$deliveries)
    }

    # Load innings
    if (nrow(parsed_data$innings) > 0) {
      load_innings_to_db(conn, parsed_data$innings)
    }

    # Load players
    if (nrow(parsed_data$players) > 0) {
      load_players_to_db(conn, parsed_data$players)
    }

    # Commit transaction
    DBI::dbCommit(conn)

    cli::cli_alert_success("Loaded match {parsed_data$match_info$match_id}")

    invisible(TRUE)
  }, error = function(e) {
    DBI::dbRollback(conn)
    cli::cli_abort("Failed to load match data: {e$message}")
  })
}


#' Load Matches to Database
#'
#' Inserts match metadata into the matches table.
#'
#' @param conn DuckDB connection
#' @param matches_df Data frame with match data
#'
#' @return Number of rows inserted
#' @keywords internal
load_matches_to_db <- function(conn, matches_df) {
  # Check if match already exists
  existing <- DBI::dbGetQuery(
    conn,
    "SELECT match_id FROM matches WHERE match_id = ?",
    params = list(matches_df$match_id[1])
  )

  if (nrow(existing) > 0) {
    cli::cli_alert_info("Match {matches_df$match_id[1]} already exists, skipping")
    return(0)
  }

  # Insert
  DBI::dbAppendTable(conn, "matches", matches_df)
}


#' Load Deliveries to Database
#'
#' Inserts delivery data into the deliveries table.
#'
#' @param conn DuckDB connection
#' @param deliveries_df Data frame with delivery data
#'
#' @return Number of rows inserted
#' @keywords internal
load_deliveries_to_db <- function(conn, deliveries_df) {
  # Insert deliveries (skip duplicates)
  DBI::dbExecute(conn, "
    INSERT OR IGNORE INTO deliveries SELECT * FROM deliveries_df
  ")
}


#' Load Innings to Database
#'
#' Inserts innings data into the match_innings table.
#'
#' @param conn DuckDB connection
#' @param innings_df Data frame with innings data
#'
#' @return Number of rows inserted
#' @keywords internal
load_innings_to_db <- function(conn, innings_df) {
  DBI::dbAppendTable(conn, "match_innings", innings_df)
}


#' Load Players to Database
#'
#' Inserts or updates player data in the players table.
#'
#' @param conn DuckDB connection
#' @param players_df Data frame with player data
#'
#' @return Number of rows inserted/updated
#' @keywords internal
load_players_to_db <- function(conn, players_df) {
  # Insert only new players (ignore duplicates)
  for (i in 1:nrow(players_df)) {
    player <- players_df[i, ]

    # Check if player exists
    existing <- DBI::dbGetQuery(
      conn,
      "SELECT player_id FROM players WHERE player_id = ?",
      params = list(player$player_id)
    )

    if (nrow(existing) == 0) {
      DBI::dbAppendTable(conn, "players", player)
    }
  }
}


#' Batch Load Matches
#'
#' Loads multiple match files into the database.
#'
#' @param file_paths Character vector of paths to JSON files
#' @param path Database path. If NULL, uses default.
#' @param parallel Logical. If TRUE, uses parallel processing (future feature).
#' @param progress Logical. If TRUE, shows progress bar.
#'
#' @return Invisibly returns count of successfully loaded matches
#' @export
#'
#' @examples
#' \dontrun{
#' # Load all JSON files in a directory
#' files <- list.files("cricket_data/", pattern = "\\.json$", full.names = TRUE)
#' batch_load_matches(files)
#' }
batch_load_matches <- function(file_paths, path = NULL, parallel = FALSE, progress = TRUE) {
  n_files <- length(file_paths)

  cli::cli_h2("Batch loading {n_files} matches")

  success_count <- 0
  error_count <- 0

  for (i in seq_along(file_paths)) {
    file_path <- file_paths[i]

    if (progress) {
      cli::cli_progress_step(
        "Loading match {i}/{n_files}: {basename(file_path)}",
        msg_done = "Loaded {i}/{n_files}",
        msg_failed = "Failed {i}/{n_files}"
      )
    }

    tryCatch({
      # Parse
      parsed <- parse_cricsheet_json(file_path)

      # Load
      load_match_data(parsed, path = path, verify = FALSE)

      success_count <- success_count + 1
    }, error = function(e) {
      cli::cli_alert_danger("Error loading {basename(file_path)}: {e$message}")
      error_count <- error_count + 1
    })
  }

  cli::cli_alert_success("Successfully loaded {success_count} matches")
  if (error_count > 0) {
    cli::cli_alert_warning("{error_count} matches failed to load")
  }

  invisible(success_count)
}
