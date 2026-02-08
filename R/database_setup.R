# Database Setup Functions for Bouncer
#
# Main entry point for database initialization.
# Schema, indexes, migrations, and utilities are in separate files:
#   - database_schema.R: Table definitions (create_schema)
#   - database_indexes.R: Index creation (create_indexes, drop_bulk_load_indexes)
#   - database_migration.R: Schema migrations (add_prediction_tables, etc.)
#   - database_3way_elo.R: 3-way ELO table operations
#   - database_utils.R: Verification, deletion helpers

#' Initialize Bouncer Database
#'
#' Creates a new DuckDB database file with the schema for cricket data storage.
#'
#' @param path Character string specifying the database file path. If NULL,
#'   uses the default system data directory.
#' @param overwrite Logical. If TRUE, overwrites existing database. Default FALSE.
#' @param skip_indexes Logical. If TRUE, skips index creation. Useful when
#'   bulk loading data (indexes are faster to create after data is loaded).
#'   Default FALSE.
#' @param verbose Logical. If TRUE, shows progress for each table/index creation.
#'   Default FALSE for cleaner output.
#'
#' @return Invisibly returns the database path
#' @export
#'
#' @examples
#' \dontrun{
#' # Initialize database in default location
#' initialize_bouncer_database()
#'
#' # Initialize in custom location
#' initialize_bouncer_database("~/cricket_data/bouncer.duckdb")
#'
#' # Initialize without indexes (for bulk loading)
#' initialize_bouncer_database(skip_indexes = TRUE)
#'
#' # Initialize with detailed progress
#' initialize_bouncer_database(verbose = TRUE)
#' }
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
