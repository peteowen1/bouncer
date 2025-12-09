# User-Facing Functions for Bouncer Data Management

#' Install Bouncer Cricket Data
#'
#' Downloads cricket data from Cricsheet and loads it into a local DuckDB database.
#' This is the main function for setting up your bouncer cricket data.
#'
#' @param formats Character vector of formats to download. Options: "test", "odi", "t20i".
#'   Default is c("odi", "t20i").
#' @param leagues Character vector of leagues to download. Options: "ipl", "bbl", "cpl",
#'   "psl", "wbbl", etc. Default is c("ipl").
#' @param gender Character string. "male", "female", or "both". Default "male".
#' @param start_season Character or numeric. Only download matches from this season onwards.
#'   If NULL, downloads all available data.
#' @param db_path Database path. If NULL, uses default system data directory.
#' @param download_path Path to store downloaded files. If NULL, uses temp directory.
#' @param keep_downloads Logical. If TRUE, keeps downloaded JSON files. Default FALSE.
#'
#' @return Invisibly returns database path
#' @export
#'
#' @examples
#' \dontrun{
#' # Install ODI and T20I data plus IPL
#' install_bouncer_data()
#'
#' # Install all formats
#' install_bouncer_data(
#'   formats = c("test", "odi", "t20i"),
#'   leagues = c("ipl", "bbl")
#' )
#'
#' # Install only recent data (from 2020 onwards)
#' install_bouncer_data(start_season = 2020)
#' }
install_bouncer_data <- function(formats = c("odi", "t20i"),
                                  leagues = c("ipl"),
                                  gender = "male",
                                  start_season = NULL,
                                  db_path = NULL,
                                  download_path = NULL,
                                  keep_downloads = FALSE) {

  cli::cli_h1("Installing Bouncer Cricket Data")

  # Initialize database
  cli::cli_h2("Step 1: Initialize Database")
  db_path <- if (is.null(db_path)) get_db_path() else db_path

  if (!file.exists(db_path)) {
    initialize_bouncer_database(path = db_path)
  } else {
    cli::cli_alert_info("Database already exists at {.file {db_path}}")
  }

  # Setup download path
  if (is.null(download_path)) {
    download_path <- file.path(tempdir(), "bouncer_downloads")
  }

  if (!dir.exists(download_path)) {
    dir.create(download_path, recursive = TRUE)
  }

  # Download and load each format
  if (length(formats) > 0) {
    cli::cli_h2("Step 2: Download and Load Format Data")

    genders_to_download <- if (gender == "both") {
      c("male", "female")
    } else {
      gender
    }

    for (fmt in formats) {
      for (gnd in genders_to_download) {
        cli::cli_alert_info("Processing {fmt} ({gnd})")

        # Download
        files <- tryCatch({
          download_format_data(
            format = fmt,
            gender = gnd,
            output_path = file.path(download_path, paste0(fmt, "_", gnd))
          )
        }, error = function(e) {
          cli::cli_alert_warning("Failed to download {fmt} ({gnd}): {e$message}")
          character(0)
        })

        if (length(files) > 0) {
          # Filter by season if requested
          if (!is.null(start_season)) {
            files <- filter_files_by_season(files, start_season)
          }

          # Load to database
          if (length(files) > 0) {
            batch_load_matches(files, path = db_path)
          } else {
            cli::cli_alert_info("No files to load after season filtering")
          }
        }
      }
    }
  }

  # Download and load each league
  if (length(leagues) > 0) {
    cli::cli_h2("Step 3: Download and Load League Data")

    for (league in leagues) {
      cli::cli_alert_info("Processing {league}")

      # Download
      files <- tryCatch({
        download_league_data(
          league = league,
          output_path = file.path(download_path, league)
        )
      }, error = function(e) {
        cli::cli_alert_warning("Failed to download {league}: {e$message}")
        character(0)
      })

      if (length(files) > 0) {
        # Filter by season if requested
        if (!is.null(start_season)) {
          files <- filter_files_by_season(files, start_season)
        }

        # Load to database
        if (length(files) > 0) {
          batch_load_matches(files, path = db_path)
        }
      }
    }
  }

  # Clean up downloads if requested
  if (!keep_downloads && dir.exists(download_path)) {
    cli::cli_alert_info("Cleaning up downloads...")
    unlink(download_path, recursive = TRUE)
  }

  # Show summary
  cli::cli_h2("Installation Complete!")
  get_data_info(path = db_path)

  cli::cli_alert_success("Database ready at {.file {db_path}}")
  cli::cli_alert_info("Load the bouncer package to analyze this data")

  invisible(db_path)
}


#' Get Data Info
#'
#' Displays information about the installed cricket data.
#'
#' @param path Database path. If NULL, uses default.
#'
#' @return Invisibly returns a list with data statistics
#' @export
#'
#' @examples
#' \dontrun{
#' # Show what data is installed
#' get_data_info()
#' }
get_data_info <- function(path = NULL) {
  if (is.null(path)) {
    path <- get_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_alert_danger("No database found. Run {.fn install_bouncer_data} first.")
    return(invisible(NULL))
  }

  conn <- get_db_connection(path = path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  # Get counts
  n_matches <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM matches")$n
  n_deliveries <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM deliveries")$n
  n_players <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM players")$n

  # Get match type breakdown
  match_types <- DBI::dbGetQuery(conn, "
    SELECT match_type, COUNT(*) as n
    FROM matches
    GROUP BY match_type
    ORDER BY n DESC
  ")

  # Get date range
  date_range <- DBI::dbGetQuery(conn, "
    SELECT MIN(match_date) as earliest, MAX(match_date) as latest
    FROM matches
    WHERE match_date IS NOT NULL
  ")

  cli::cli_h2("Bouncer Cricket Data Summary")
  cli::cli_alert_info("Matches: {scales::comma(n_matches)}")
  cli::cli_alert_info("Deliveries: {scales::comma(n_deliveries)}")
  cli::cli_alert_info("Players: {scales::comma(n_players)}")

  if (nrow(date_range) > 0 && !is.na(date_range$earliest)) {
    cli::cli_alert_info("Date range: {date_range$earliest} to {date_range$latest}")
  }

  if (nrow(match_types) > 0) {
    cli::cli_h3("Matches by Type")
    for (i in 1:nrow(match_types)) {
      cli::cli_alert_info("{match_types$match_type[i]}: {match_types$n[i]}")
    }
  }

  info <- list(
    n_matches = n_matches,
    n_deliveries = n_deliveries,
    n_players = n_players,
    match_types = match_types,
    date_range = date_range
  )

  invisible(info)
}


#' List Available Formats in Database
#'
#' Shows what cricket formats are currently in the database.
#'
#' @param path Database path. If NULL, uses default.
#'
#' @return A data frame with format information
#' @export
#'
#' @examples
#' \dontrun{
#' # See what formats are installed
#' list_available_formats()
#' }
list_available_formats <- function(path = NULL) {
  if (is.null(path)) {
    path <- get_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_alert_danger("No database found.")
    return(invisible(NULL))
  }

  conn <- get_db_connection(path = path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  formats <- DBI::dbGetQuery(conn, "
    SELECT
      match_type,
      gender,
      COUNT(*) as n_matches,
      MIN(match_date) as earliest_match,
      MAX(match_date) as latest_match
    FROM matches
    WHERE match_type IS NOT NULL
    GROUP BY match_type, gender
    ORDER BY n_matches DESC
  ")

  print(formats)
  invisible(formats)
}


#' Filter Files by Season
#'
#' Filters JSON files to only include matches from specified season onwards.
#'
#' @param files Character vector of file paths
#' @param start_season Numeric or character season
#'
#' @return Filtered character vector of file paths
#' @keywords internal
filter_files_by_season <- function(files, start_season) {
  # For now, return all files (season filtering would require parsing each file)
  # This is a placeholder for future implementation
  cli::cli_alert_info("Season filtering will be implemented in future version")
  return(files)
}
