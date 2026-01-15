# Database Connection Functions for Bouncer

#' Check DuckDB Availability
#'
#' Internal helper to check if DuckDB is installed.
#' Called by database functions since duckdb is in Suggests.
#'
#' @return TRUE if available, otherwise stops with error
#' @keywords internal
check_duckdb_available <- function() {

  if (!requireNamespace("DBI", quietly = TRUE)) {
    cli::cli_abort(c(
      "Package {.pkg DBI} is required for database operations.",
      "i" = "Install with: {.code install.packages('DBI')}"
    ))
  }
  if (!requireNamespace("duckdb", quietly = TRUE)) {
    cli::cli_abort(c(
      "Package {.pkg duckdb} is required for database operations.",
      "i" = "Install with: {.code install.packages('duckdb')}"
    ))
  }
  invisible(TRUE)
}


#' Get Database Connection
#'
#' Internal function to get a connection to the Bouncer DuckDB database.
#'
#' @param path Character string specifying the database file path. If NULL,
#'   uses the default system data directory.
#' @param read_only Logical. If TRUE, opens database in read-only mode.
#'
#' @return A DuckDB connection object
#' @keywords internal
get_db_connection <- function(path = NULL, read_only = FALSE) {
  check_duckdb_available()
  path <- ensure_db_exists(path)

  conn <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = path,
    read_only = read_only
  )

  return(conn)
}


#' Get Database Path
#'
#' Returns the path to the Bouncer DuckDB database.
#'
#' Default location priority:
#' 1. `../bouncerdata/bouncer.duckdb` (when running from bouncer/ package dir)
#' 2. `bouncerdata/bouncer.duckdb` (when running from bouncerverse/ root)
#' 3. Falls back to R user data directory if project paths don't exist
#'
#' @param path Character string specifying a custom database path. If NULL,
#'   returns the default path.
#'
#' @return Character string with the database path
#' @export
#'
#' @examples
#' \dontrun{
#' # Get default database path
#' get_db_path()
#'
#' # Specify custom path
#' get_db_path("~/my_cricket_data/bouncer.duckdb")
#' }
get_db_path <- function(path = NULL) {
  if (!is.null(path)) {
    return(normalizePath(path, mustWork = FALSE))
  }

  # Try project paths first (bouncerdata is sibling to bouncer package)
  # Priority 1: Running from bouncer/ directory
  project_path_1 <- file.path("..", "bouncerdata", "bouncer.duckdb")
  if (file.exists(project_path_1) || file.exists(dirname(project_path_1))) {
    return(normalizePath(project_path_1, mustWork = FALSE))
  }

  # Priority 2: Running from bouncerverse/ root directory
  project_path_2 <- file.path("bouncerdata", "bouncer.duckdb")
  if (file.exists(project_path_2) || file.exists(dirname(project_path_2))) {
    return(normalizePath(project_path_2, mustWork = FALSE))
  }

  # Fallback: R user data directory (for users without project structure)
  data_dir <- tools::R_user_dir("bouncerdata", which = "data")
  file.path(data_dir, "bouncer.duckdb")
}


#' Ensure Database Exists
#'
#' Checks if database exists, and initializes it if not.
#'
#' @param path Character string specifying the database file path. If NULL,
#'   uses the default system data directory.
#'
#' @return The database path
#' @keywords internal
ensure_db_exists <- function(path = NULL) {
  if (is.null(path)) {
    path <- get_db_path()
  }

  if (!file.exists(path)) {
    cli::cli_alert_info("Database not found. Initializing...")
    initialize_bouncer_database(path = path, overwrite = FALSE)
  }

  return(path)
}


#' Connect to Bouncer Database
#'
#' User-facing function to connect to the Bouncer cricket database.
#' Use this connection to query data directly with SQL if needed.
#'
#' @param path Character string specifying the database file path. If NULL,
#'   uses the default system data directory.
#' @param read_only Logical. If TRUE, opens database in read-only mode.
#'   Default is FALSE.
#'
#' @return A DuckDB connection object. Remember to disconnect when done using
#'   \code{disconnect_bouncer(conn)}.
#' @export
#'
#' @examples
#' \dontrun{
#' # Connect to database
#' conn <- connect_to_bouncer()
#'
#' # Query data
#' matches <- DBI::dbGetQuery(conn, "SELECT * FROM matches LIMIT 10")
#'
#' # Always disconnect when done
#' disconnect_bouncer(conn)
#' }
connect_to_bouncer <- function(path = NULL, read_only = FALSE) {
  check_duckdb_available()
  path <- ensure_db_exists(path)

  cli::cli_alert_info("Connecting to database at {.file {path}}")

  conn <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = path,
    read_only = read_only
  )

  cli::cli_alert_success("Connected successfully")

  # Print helpful message
  cli::cli_alert_info("Use {.fn disconnect_bouncer} when done")

  return(conn)
}


#' Disconnect from Bouncer Database
#'
#' Properly disconnects from the DuckDB database.
#'
#' @param conn A DuckDB connection object
#' @param shutdown Logical. If TRUE, shuts down the DuckDB instance.
#'   Default is TRUE.
#'
#' @return Invisibly returns TRUE
#' @export
#'
#' @examples
#' \dontrun{
#' conn <- connect_to_bouncer()
#' # ... do work ...
#' disconnect_bouncer(conn)
#' }
disconnect_bouncer <- function(conn, shutdown = TRUE) {
  check_duckdb_available()
  if (!DBI::dbIsValid(conn)) {
    cli::cli_alert_warning("Connection already closed")
    return(invisible(FALSE))
  }

  DBI::dbDisconnect(conn, shutdown = shutdown)
  cli::cli_alert_success("Disconnected from database")

  invisible(TRUE)
}


# Unused helper functions removed - use get_db_connection() directly
