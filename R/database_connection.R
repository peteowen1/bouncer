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
  if (is.null(path)) path <- get_db_path()
  cli::cli_alert_info("Connecting to database at {.file {path}}")

  conn <- get_db_connection(path = path, read_only = read_only)

  cli::cli_alert_success("Connected successfully")
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


#' Force Close All DuckDB Connections
#'
#' Emergency function to release file locks when you get
#' "file is being used by another process" errors.
#'
#' This function:
#' 1. Forces garbage collection to finalize orphaned connections
#' 2. Attempts to shutdown any DuckDB driver instances
#'
#' If the file is still locked after calling this, restart R with Ctrl+Shift+F10.
#'
#' @return Invisibly returns TRUE
#' @export
#'
#' @examples
#' \dontrun{
#' # If you get a file lock error:
#' force_close_duckdb()
#'
#' # Then retry your operation
#' install_all_bouncer_data(fresh = TRUE)
#' }
force_close_duckdb <- function() {
  check_duckdb_available()


  # Force garbage collection to finalize any orphaned connection objects
  # R's finalizers will call disconnect on any unreferenced connections
  gc()

  # Step 2: Try to shutdown any active DuckDB instances
  # Note: duckdb::duckdb() creates a new driver each time, so this may not
  # catch the actual driver holding the lock, but it's worth trying
  tryCatch({
    drv <- duckdb::duckdb()
    DBI::dbDisconnect(drv, shutdown = TRUE)
  }, error = function(e) {
    # Log error for debugging but don't fail
    # Driver may not exist or already be shutdown
    cli::cli_alert_info("DuckDB driver shutdown note: {e$message}")
  })

  # Small delay to let OS release file handles
  Sys.sleep(0.5)

  cli::cli_alert_success("Attempted to close all DuckDB connections")
  cli::cli_alert_info("If file is still locked, restart R with {.kbd Ctrl+Shift+F10}")

  invisible(TRUE)
}


# ============================================================================
# DATABASE SETUP (from database_setup.R)
# ============================================================================

initialize_bouncer_database <- function(path = NULL, overwrite = FALSE, skip_indexes = FALSE, verbose = FALSE) {
  if (is.null(path)) {
    path <- get_default_db_path()
  }

  # Create directory if it doesn't exist
  db_dir <- dirname(path)
  if (!dir.exists(db_dir)) {
    dir.create(db_dir, recursive = TRUE)
    cli::cli_alert_success("Created directory: {.file {db_dir}}")
  }

  # Check if database already exists
  if (file.exists(path) && !overwrite) {
    cli::cli_alert_warning("Database already exists at {.file {path}}")
    cli::cli_alert_info("Use overwrite = TRUE to replace it")
    return(invisible(path))
  }

  if (file.exists(path) && overwrite) {
    cli::cli_alert_warning("Overwriting existing database at {.file {path}}")
    file.remove(path)
  }

  # Create database and schema
  cli::cli_alert_info("Initializing DuckDB database...")

  check_duckdb_available()
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = path)
  on.exit(DBI::dbDisconnect(conn, shutdown = FALSE))  # Don't shutdown - allows subsequent connections

  # Create schema (from database_schema.R)
  create_schema(conn, verbose = verbose)

  # Create indexes (skip if bulk loading - they'll be created after data load)
  if (!skip_indexes) {
    create_indexes(conn, verbose = verbose)
  }

  cli::cli_alert_success("Database initialized at {.file {path}}")

  invisible(path)
}


#' Find Bouncerdata Directory
#'
#' Finds or creates the bouncerdata directory for storing data files.
#' Prefers project-local directory (bouncerverse/bouncerdata) if it exists,
#' otherwise falls back to system data directory.
#'
#' @param create Logical. Whether to create directory if not found. Default TRUE.
#'
#' @return Character string with directory path
#' @export
find_bouncerdata_dir <- function(create = TRUE) {
  cwd <- normalizePath(getwd(), winslash = "/")

  # Walk up the directory tree looking for bouncerdata as a sibling
  # This handles cases where we're in bouncer/, bouncer/data-raw/, etc.
  current <- cwd
  for (i in 1:10) {  # Max 10 levels up
    parent <- dirname(current)
    if (parent == current) break  # Reached root

    # Check for bouncerdata sibling
    sibling_path <- file.path(parent, "bouncerdata")
    if (dir.exists(sibling_path)) {
      return(normalizePath(sibling_path, winslash = "/"))
    }

    # Also check if bouncerdata is a child of current (if we're in bouncerverse/)
    child_path <- file.path(current, "bouncerdata")
    if (dir.exists(child_path)) {
      return(normalizePath(child_path, winslash = "/"))
    }

    current <- parent
  }

  if (!create) {
    return(NULL)
  }

  # If not found, create as sibling to 'bouncer' directory
  # Walk up until we find 'bouncer' folder, then create sibling
  current <- cwd
  for (i in 1:10) {
    if (basename(current) == "bouncer") {
      parent_bouncerdata <- file.path(dirname(current), "bouncerdata")
      dir.create(parent_bouncerdata, recursive = TRUE)
      cli::cli_alert_info("Created data directory: {.file {parent_bouncerdata}}")
      return(normalizePath(parent_bouncerdata, winslash = "/"))
    }
    parent <- dirname(current)
    if (parent == current) break
    current <- parent
  }

  # Fallback to system directory
  data_dir <- tools::R_user_dir("bouncerdata", which = "data")
  if (!dir.exists(data_dir)) {
    dir.create(data_dir, recursive = TRUE)
  }
  return(data_dir)
}


#' Get Default Database Path
#'
#' Returns the default path for the Bouncer DuckDB database.
#' This is a convenience wrapper around \code{\link{get_db_path}()} for
#' backward compatibility.
#'
#' For new code, prefer using \code{get_db_path()} directly as it also
#' accepts custom path parameters.
#'
#' @return Character string with database path
#'
#' @seealso \code{\link{get_db_path}} for the primary path resolution function
#'
#' @keywords internal
get_default_db_path <- function() {

  # Delegate to get_db_path() for consistent path resolution

  get_db_path()
}
# NOTE: get_models_dir() has been moved to expected_outcomes.R and is now exported.
