# Database Connection Functions for Bouncer

# Cached result for check_duckdb_available() — avoids 15+ redundant
# requireNamespace() calls per session (both packages are in Imports).
.duckdb_cache <- new.env(parent = emptyenv())

#' Check DuckDB Availability
#'
#' Internal helper to check if DuckDB and DBI are installed.
#' Both are in Imports, but this provides a clear error if something is wrong.
#' Result is cached after first successful check.
#'
#' @return TRUE if available, otherwise stops with error
#' @keywords internal
check_duckdb_available <- function() {
  if (isTRUE(.duckdb_cache$available)) return(invisible(TRUE))

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
  .duckdb_cache$available <- TRUE
  invisible(TRUE)
}


#' Check if a Table Exists in DuckDB
#'
#' Schema-aware table existence check using information_schema.
#' Unlike \code{DBI::dbListTables()}, which only returns tables in the
#' main schema, this function correctly checks any schema.
#'
#' @param conn DuckDB connection.
#' @param table_name Character. Table name, optionally schema-qualified
#'   (e.g., "team_elo" or "cricsheet.matches").
#'
#' @return Logical. TRUE if the table exists.
#' @keywords internal
table_exists <- function(conn, table_name) {
  parts <- strsplit(table_name, ".", fixed = TRUE)[[1]]
  if (length(parts) == 2) {
    schema <- parts[1]
    tbl <- parts[2]
  } else if (length(parts) == 1) {
    schema <- "main"
    tbl <- parts[1]
  } else {
    # A three-part name previously fell into the one-part branch and checked
    # main.<first-part> -- a confidently wrong answer rather than an error.
    cli::cli_abort(c(
      "{.arg table_name} must be {.val table} or {.val schema.table}, got {.val {table_name}}.",
      "i" = "Found {length(parts)} dot-separated parts."
    ))
  }
  nrow(DBI::dbGetQuery(conn,
    "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
    params = list(schema, tbl)
  )) > 0
}


#' Record how a data path was resolved
#'
#' Carried as an attribute so it travels with the path without changing its
#' type — `file.path()`, `dirname()` and friends see a plain character string,
#' so none of the ~60 callers of [find_bouncerdata_dir()] are affected.
#'
#' One of `"sibling"`, `"child"` (the repo tree was found), `"created"` (a
#' fresh `bouncerdata/` was made next to `bouncer/`), or `"user_data"` (the
#' rappdirs fallback — almost always wrong inside this workspace).
#'
#' @keywords internal
.tag_resolution <- function(path, how) {
  attr(path, "bouncer_resolution") <- how
  path
}

#' Read back a resolution tag, defaulting to unknown
#' @keywords internal
.db_resolution <- function(path) {
  how <- attr(path, "bouncer_resolution", exact = TRUE)
  if (is.null(how)) "unknown" else how
}

# Warn at most once per session per (path, kind) -- get_db_connection() is
# called from dozens of places and a repeated warning is a warning nobody reads.
.db_warned <- new.env(parent = emptyenv())

.warn_once <- function(key, message, .envir = parent.frame()) {
  if (!is.null(.db_warned[[key]])) return(invisible(FALSE))
  assign(key, TRUE, envir = .db_warned)
  # .envir matters: cli interpolates against the CALLER's frame, and every
  # `{path}` / `{how}` in these messages lives there, not here.
  if (isTRUE(getOption("bouncer.strict_db", FALSE))) {
    cli::cli_abort(message, .envir = .envir)
  } else {
    cli::cli_warn(message, .envir = .envir)
  }
  invisible(TRUE)
}

#' Warn when an implicitly-resolved database is probably not the one you want
#'
#' @section Why this exists:
#' `find_bouncerdata_dir()` falls back to the rappdirs user-data directory when
#' the walk up the tree finds no `bouncerdata/` sibling — and `ensure_db_exists()`
#' then *initialises a database there*. The result is a structurally valid
#' connection in which every schema exists and every table has **zero rows**, so
#' every query succeeds and returns nothing. That is strictly worse than a
#' missing file, which at least aborts and names the path: an empty corpus is
#' indistinguishable from a legitimate "this format has no data" answer, and it
#' cost a wrong conclusion and a debugging session on 2026-08-17
#' (bouncerverse#46) — the same script, the same SQL, only the working directory
#' different, returning 22,266 matches from the repo and 0 from a scratch
#' directory.
#'
#' Two independent signals, because either alone can be the real story: the
#' resolution strategy, and whether the corpus is actually populated.
#'
#' Only fires for an **implicitly** resolved path. An explicit `path=` is the
#' caller's own choice — including the temporary empty databases the tests build.
#'
#' @param conn Open connection.
#' @param path The resolved path, carrying its resolution attribute.
#' @return Invisibly, TRUE if anything was reported.
#' @keywords internal
.check_db_is_plausible <- function(conn, path) {
  how <- .db_resolution(path)
  reported <- FALSE

  if (how %in% c("user_data", "created")) {
    reported <- .warn_once(paste0("res:", path), c(
      "Database resolved by {.strong fallback}, not by finding the repo tree.",
      "!" = "Using {.file {path}}",
      "i" = "No {.file bouncerdata/} was found walking up from {.file {getwd()}}.",
      "i" = "This location is usually EMPTY, so queries succeed and return nothing.",
      "i" = "Run from inside the repo, or pass {.arg path} explicitly."
    )) || reported
  }

  n <- tryCatch(
    DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricsheet.matches")$n,
    error = function(e) NA_integer_
  )

  if (is.na(n)) {
    reported <- .warn_once(paste0("schema:", path), c(
      "Could not read {.field cricsheet.matches} from {.file {path}}.",
      "i" = "The database may be uninitialised or of an older schema."
    )) || reported
  } else if (n == 0L) {
    reported <- .warn_once(paste0("empty:", path), c(
      "The database at {.file {path}} has {.strong zero} matches.",
      "!" = "Every query against it will succeed and return nothing.",
      "i" = "Resolved by: {how}. Size: {round(file.size(path) / 1e6, 1)} MB.",
      "i" = "Set {.code options(bouncer.strict_db = TRUE)} to make this an error."
    )) || reported
  }

  invisible(reported)
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
  implicit <- is.null(path)
  path <- ensure_db_exists(path)

  conn <- DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = path,
    read_only = read_only
  )

  # Only for an implicitly resolved path -- see .check_db_is_plausible(). An
  # explicit path= is the caller's own choice, including the empty temporary
  # databases the tests build.
  if (implicit) .check_db_is_plausible(conn, path)

  return(conn)
}


#' Run an Expression Against a Scoped Database Connection
#'
#' Opens a connection, passes it to `fn`, and disconnects on the way out
#' whether `fn` returned or threw.
#'
#' @section Why this exists:
#' DuckDB permits exactly one write connection at a time, so a write
#' connection leaked by an error holds the lock for the remainder of the R
#' session — every later write fails with "Could not set lock", far from the
#' code that caused it. The `open / do work / dbDisconnect` sequence written
#' without `on.exit` is safe only on the happy path; this makes the disconnect
#' unconditional. Prefer it to hand-rolling `on.exit` at each call site.
#'
#' Not a fit when the connection outlives the call — [connect_to_bouncer()]
#' hands ownership to its caller and must not use this.
#'
#' @param fn Function of one argument, the connection.
#' @param path Character. Database path, passed to [get_db_connection()].
#' @param read_only Logical. Open read-only. Default FALSE.
#'
#' @return Whatever `fn` returns.
#'
#' @examples
#' \dontrun{
#' n <- with_db_connection(function(conn) {
#'   DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricsheet.matches")$n
#' }, read_only = TRUE)
#' }
#'
#' @keywords internal
with_db_connection <- function(fn, path = NULL, read_only = FALSE) {
  conn <- get_db_connection(path = path, read_only = read_only)
  on.exit(
    # The tryCatch is deliberate -- a failing disconnect must not clobber a
    # more informative error thrown by fn(). But it must not be SILENT: a
    # disconnect that throws can leave the write lock held, which is the exact
    # failure this function exists to prevent. Swallowing it would hide the
    # disease one layer down, and worse than the original bare call, which at
    # least raised. Warn, always.
    tryCatch(
      DBI::dbDisconnect(conn, shutdown = TRUE),
      error = function(e) cli::cli_warn(c(
        "Failed to close the database connection cleanly: {conditionMessage(e)}",
        "!" = "A write lock may still be held for the rest of this session.",
        "i" = "If later writes fail with {.q Could not set lock}, this is why."
      ))
    ),
    add = TRUE
  )
  fn(conn)
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

  # Use find_bouncerdata_dir() which reliably walks up the directory tree
  # This works correctly from any working directory (bouncer/, data-raw/, etc.)
  bouncerdata_dir <- find_bouncerdata_dir(create = FALSE)
  if (!is.null(bouncerdata_dir)) {
    return(.tag_resolution(file.path(bouncerdata_dir, "bouncer.duckdb"),
                           .db_resolution(bouncerdata_dir)))
  }

  # Fallback: R user data directory (for users without project structure)
  data_dir <- tools::R_user_dir("bouncerdata", which = "data")
  if (!dir.exists(data_dir)) dir.create(data_dir, recursive = TRUE)
  .tag_resolution(file.path(data_dir, "bouncer.duckdb"), "user_data")
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
#' matches <- DBI::dbGetQuery(conn, "SELECT * FROM cricsheet.matches LIMIT 10")
#'
#' # Always disconnect when done
#' disconnect_bouncer(conn)
#' }
connect_to_bouncer <- function(path = NULL, read_only = FALSE) {
  implicit <- is.null(path)
  if (implicit) path <- get_db_path()
  cli::cli_alert_info("Connecting to database at {.file {path}}")

  conn <- get_db_connection(path = path, read_only = read_only)

  # get_db_connection() only checks a path it resolved itself, and this function
  # resolves its own before handing it over -- so without this line the check
  # would be skipped at the one place a user without the repo layout arrives.
  if (implicit) .check_db_is_plausible(conn, path)

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
# DATABASE SETUP
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
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  # Create schema
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
#'
#' @examples
#' \dontrun{
#' # Find existing bouncerdata directory
#' data_dir <- find_bouncerdata_dir(create = FALSE)
#'
#' # Find or create bouncerdata directory
#' data_dir <- find_bouncerdata_dir()
#' }
#'
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
      return(.tag_resolution(normalizePath(sibling_path, winslash = "/"), "sibling"))
    }

    # Also check if bouncerdata is a child of current (if we're in bouncerverse/)
    child_path <- file.path(current, "bouncerdata")
    if (dir.exists(child_path)) {
      return(.tag_resolution(normalizePath(child_path, winslash = "/"), "child"))
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
      return(.tag_resolution(normalizePath(parent_bouncerdata, winslash = "/"), "created"))
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
  return(.tag_resolution(data_dir, "user_data"))
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
