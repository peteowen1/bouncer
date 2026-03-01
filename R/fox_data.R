# Fox Sports Data Functions
#
# Ingestion and loaders for Fox Sports ball-by-ball, player, and match data.
# Fox Sports data covers Australian domestic cricket (BBL, WBBL, Sheffield Shield)
# plus international matches broadcast by Fox Sports.
#
# Data flow:
#   Fox scraper → combined parquets → ingest_fox_sports_data() → DuckDB tables
#   DuckDB tables → load_fox_*() → R data.frames
#   GitHub release → load_fox_*(source="remote") → R data.frames


# ============================================================================
# INGESTION
# ============================================================================

#' Ingest Fox Sports Data into DuckDB
#'
#' Scans the fox_cricket directory for combined parquet files and loads
#' ball-by-ball, player, and match detail data into DuckDB. Skips matches
#' already in the database.
#'
#' @param fox_dir Character. Path to the fox_cricket data directory
#'   (e.g., "../bouncerdata/fox_cricket"). If NULL, auto-detects from
#'   bouncerdata sibling directory.
#' @param path Character. Database file path. If NULL, uses default.
#' @param formats Character vector. Formats to ingest (e.g., "test", "t20i",
#'   "bbl"). If NULL, auto-discovers all available formats from directory.
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return Invisibly returns a list with counts of ingested records.
#' @export
#'
#' @examples
#' \dontrun{
#' # Ingest all formats from default location
#' ingest_fox_sports_data()
#'
#' # Ingest only BBL data
#' ingest_fox_sports_data(formats = "bbl")
#'
#' # Ingest from a specific directory
#' ingest_fox_sports_data(fox_dir = "path/to/fox_cricket")
#' }
ingest_fox_sports_data <- function(fox_dir = NULL,
                                    path = NULL,
                                    formats = NULL,
                                    verbose = TRUE) {
  # Auto-detect fox_cricket directory
  if (is.null(fox_dir)) {
    bd_dir <- find_bouncerdata_dir(create = FALSE)
    if (is.null(bd_dir)) {
      cli::cli_abort("Cannot find bouncerdata directory. Provide {.arg fox_dir} explicitly.")
    }
    fox_dir <- file.path(bd_dir, "fox_cricket")
  }

  if (!dir.exists(fox_dir)) {
    cli::cli_abort("Fox Sports directory not found: {.file {fox_dir}}")
  }

  conn <- get_db_connection(path = path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  # Ensure tables exist (idempotent)
  create_foxsports_tables(conn, verbose = FALSE)

  # Auto-discover formats from combined parquet filenames
  if (is.null(formats)) {
    ball_files <- list.files(fox_dir, pattern = "^all_.*_matches\\.parquet$",
                              recursive = TRUE, full.names = FALSE)
    formats <- sub("^all_(.*)_matches\\.parquet$", "\\1", basename(ball_files))
  }

  if (length(formats) == 0) {
    cli::cli_alert_warning("No combined parquet files found in {.file {fox_dir}}")
    return(invisible(list(balls = 0L, players = 0L, details = 0L, failed = 0L)))
  }

  total_balls <- 0L
  total_players <- 0L
  total_details <- 0L
  total_failed <- 0L

  for (fmt in formats) {
    fmt_lower <- tolower(fmt)

    # --- Ingest balls (from all_{fmt}_matches.parquet) ---
    balls_file <- file.path(fox_dir, sprintf("all_%s_matches.parquet", fmt_lower))
    if (file.exists(balls_file)) {
      n <- ingest_fox_table(conn, balls_file, "foxsports.balls", fmt_lower, verbose)
      if (is.null(n)) {
        total_failed <- total_failed + 1L
      } else {
        total_balls <- total_balls + n
      }
    }

    # --- Ingest players (from all_{fmt}_players.parquet) ---
    players_file <- file.path(fox_dir, sprintf("all_%s_players.parquet", fmt_lower))
    if (file.exists(players_file)) {
      n <- ingest_fox_table(conn, players_file, "foxsports.players", fmt_lower, verbose)
      if (is.null(n)) {
        total_failed <- total_failed + 1L
      } else {
        total_players <- total_players + n
      }
    }

    # --- Ingest details (from all_{fmt}_details.parquet) ---
    details_file <- file.path(fox_dir, sprintf("all_%s_details.parquet", fmt_lower))
    if (file.exists(details_file)) {
      gender <- fox_format_gender(fmt_lower)
      n <- ingest_fox_table(conn, details_file, "foxsports.details", fmt_lower,
                             verbose, gender = gender)
      if (is.null(n)) {
        total_failed <- total_failed + 1L
      } else {
        total_details <- total_details + n
      }
    }
  }

  if (verbose) {
    cli::cli_h3("Fox Sports ingestion complete")
    cli::cli_alert_success(
      "{format(total_balls, big.mark=',')} balls, {format(total_players, big.mark=',')} players, {format(total_details, big.mark=',')} details"
    )
    if (total_failed > 0) {
      cli::cli_alert_warning("{total_failed} file{?s} failed to load")
    }
  }

  invisible(list(
    balls = total_balls,
    players = total_players,
    details = total_details,
    failed = total_failed
  ))
}


#' Ingest a Single Fox Sports Parquet into DuckDB
#'
#' Uses DuckDB's native read_parquet() for efficient bulk loading.
#' Adds format (and optionally gender) columns during INSERT.
#' Skips match IDs already present for the given format.
#'
#' @param conn DuckDB connection.
#' @param parquet_path Path to the combined parquet file.
#' @param table_name Schema-qualified table name (e.g., "foxsports.balls").
#' @param format Character. Format label to add.
#' @param verbose Logical. Print progress.
#' @param gender Character or NULL. Gender to add (details table only).
#'
#' @return Number of rows inserted, or NULL on error.
#' @keywords internal
ingest_fox_table <- function(conn, parquet_path, table_name, format,
                              verbose = TRUE, gender = NULL) {
  fp <- normalizePath(parquet_path, winslash = "/", mustWork = TRUE)

  # Check existing match IDs for this format
  existing <- tryCatch(
    DBI::dbGetQuery(conn, sprintf(
      "SELECT DISTINCT match_id FROM %s WHERE format = '%s'",
      table_name, escape_sql_quotes(format)
    ))$match_id,
    error = function(e) {
      if (!grepl("does not exist|not found|no such table", e$message, ignore.case = TRUE)) {
        cli::cli_alert_warning("Failed to check existing data: {e$message}")
      }
      integer(0)
    }
  )

  # Build extra columns for INSERT
  extra_cols <- sprintf("'%s' AS format", escape_sql_quotes(format))
  if (!is.null(gender)) {
    extra_cols <- paste0(extra_cols, sprintf(", '%s' AS gender", escape_sql_quotes(gender)))
  }

  # Build WHERE clause to skip existing match IDs
  where_clause <- ""
  if (length(existing) > 0) {
    ids_sql <- paste(existing, collapse = ", ")
    where_clause <- sprintf(" WHERE CAST(match_id AS INTEGER) NOT IN (%s)", ids_sql)
  }

  tryCatch({
    n <- DBI::dbExecute(conn, sprintf(
      "INSERT INTO %s SELECT *, %s FROM read_parquet('%s')%s",
      table_name, extra_cols, fp, where_clause
    ))

    if (verbose) {
      total_in_file <- DBI::dbGetQuery(conn, sprintf(
        "SELECT COUNT(*) AS n FROM read_parquet('%s')", fp
      ))$n
      skipped <- total_in_file - n
      cli::cli_alert_info(
        "{basename(parquet_path)}: loaded {format(n, big.mark=',')} rows (skipped {format(skipped, big.mark=',')} existing)"
      )
    }

    n
  }, error = function(e) {
    cli::cli_alert_warning("Failed to load {basename(parquet_path)}: {e$message}")
    NULL
  })
}


# ============================================================================
# FORMAT HELPERS
# ============================================================================

#' Determine Gender from Fox Sports Format Code
#'
#' Maps Fox Sports format codes to gender using an explicit allowlist.
#' Returns "female" for known women's format codes (wt20i, wodi, wbbl,
#' wpl, wncl, wt20wc); all other codes return "male".
#'
#' @param format Character. Fox Sports format code (case-insensitive).
#'
#' @return Character. "female" or "male".
#' @keywords internal
fox_format_gender <- function(format) {
  fmt_lower <- tolower(format)
  female_formats <- c("wt20i", "wodi", "wbbl", "wpl", "wncl", "wt20wc")
  if (fmt_lower %in% female_formats) "female" else "male"
}


# ============================================================================
# R LOADERS
# ============================================================================

#' Load Fox Sports Ball-by-Ball Data
#'
#' Load ball-by-ball data from Fox Sports from local DuckDB or remote
#' GitHub release.
#'
#' @param match_ids Integer or numeric vector. Filter for specific match IDs.
#'   If NULL, loads all.
#' @param format Character. Format filter (e.g., "bbl", "test"). If NULL,
#'   loads all formats.
#' @param gender Character. "male", "female", or NULL (all genders).
#'   Only applies when filtering via the details table.
#' @param source Character. "local" (default) or "remote".
#'
#' @return Data frame of ball-by-ball data.
#' @export
#'
#' @examples
#' \dontrun{
#' # Load all BBL balls
#' bbl_balls <- load_fox_balls(format = "bbl")
#'
#' # Load balls for a specific match
#' balls <- load_fox_balls(match_ids = 123456)
#'
#' # From remote
#' balls <- load_fox_balls(format = "test", source = "remote")
#' }
load_fox_balls <- function(match_ids = NULL, format = NULL,
                            gender = NULL,
                            source = c("local", "remote")) {
  source <- match.arg(source)

  if (source == "remote") {
    return(load_fox_remote("balls", match_ids, format, gender))
  }

  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  where_clauses <- character()

  if (!is.null(match_ids)) {
    ids_sql <- paste(as.integer(match_ids), collapse = ", ")
    where_clauses <- c(where_clauses, sprintf("b.match_id IN (%s)", ids_sql))
  }
  if (!is.null(format)) {
    where_clauses <- c(where_clauses,
      sprintf("LOWER(b.format) = '%s'", escape_sql_quotes(tolower(format))))
  }

  # Gender filter requires joining with details table
  if (!is.null(gender)) {
    where_clauses <- c(where_clauses,
      sprintf("LOWER(d.gender) = '%s'", escape_sql_quotes(tolower(gender))))
    where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))
    sql <- sprintf(
      "SELECT b.* FROM foxsports.balls b
       INNER JOIN foxsports.details d ON b.match_id = d.match_id
       %s ORDER BY b.match_id, b.innings, b.over, b.ball",
      where_sql)
  } else if (length(where_clauses) > 0) {
    where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))
    sql <- sprintf(
      "SELECT b.* FROM foxsports.balls b %s
       ORDER BY b.match_id, b.innings, b.over, b.ball",
      where_sql)
  } else {
    sql <- "SELECT * FROM foxsports.balls ORDER BY match_id, innings, over, ball"
  }

  result <- tryCatch(
    DBI::dbGetQuery(conn, sql),
    error = function(e) {
      if (grepl("does not exist|not found|no such table", e$message, ignore.case = TRUE)) {
        cli::cli_abort(c(
          "Fox Sports tables not found in database.",
          "i" = "Run {.fn add_foxsports_tables} or {.fn ingest_fox_sports_data} first."))
      }
      stop(e)
    }
  )

  if (nrow(result) == 0) {
    cli::cli_warn("No Fox Sports ball data found for the specified filters")
    return(data.frame())
  }

  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} Fox Sports balls")
  result
}


#' Load Fox Sports Match Details
#'
#' Load match-level metadata including venue, teams, toss, and officials.
#'
#' @inheritParams load_fox_balls
#' @param gender Character. "male", "female", or NULL (all genders).
#'   Filters directly on the \code{gender} column.
#'
#' @return Data frame of match details.
#' @export
#'
#' @examples
#' \dontrun{
#' # All BBL match details
#' bbl <- load_fox_details(format = "bbl")
#'
#' # Women's matches only
#' womens <- load_fox_details(gender = "female")
#' }
load_fox_details <- function(match_ids = NULL, format = NULL,
                              gender = NULL,
                              source = c("local", "remote")) {
  source <- match.arg(source)

  if (source == "remote") {
    return(load_fox_remote("details", match_ids, format, gender))
  }

  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  where_clauses <- character()
  if (!is.null(match_ids)) {
    ids_sql <- paste(as.integer(match_ids), collapse = ", ")
    where_clauses <- c(where_clauses, sprintf("match_id IN (%s)", ids_sql))
  }
  if (!is.null(format)) {
    where_clauses <- c(where_clauses,
      sprintf("LOWER(format) = '%s'", escape_sql_quotes(tolower(format))))
  }
  if (!is.null(gender)) {
    where_clauses <- c(where_clauses,
      sprintf("LOWER(gender) = '%s'", escape_sql_quotes(tolower(gender))))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  sql <- sprintf("SELECT * FROM foxsports.details %s ORDER BY match_id DESC", where_sql)
  result <- tryCatch(
    DBI::dbGetQuery(conn, sql),
    error = function(e) {
      if (grepl("does not exist|not found|no such table", e$message, ignore.case = TRUE)) {
        cli::cli_abort(c(
          "Fox Sports tables not found in database.",
          "i" = "Run {.fn add_foxsports_tables} or {.fn ingest_fox_sports_data} first."))
      }
      stop(e)
    }
  )

  if (nrow(result) == 0) {
    cli::cli_warn("No Fox Sports match details found for the specified filters")
    return(data.frame())
  }

  cli::cli_alert_success("Loaded {nrow(result)} Fox Sports match detail{?s}")
  result
}


#' Load Fox Sports Player Data
#'
#' Load player/squad data from Fox Sports matches.
#'
#' @inheritParams load_fox_balls
#'
#' @return Data frame of player data.
#' @export
#'
#' @examples
#' \dontrun{
#' # All BBL players
#' bbl_players <- load_fox_players(format = "bbl")
#'
#' # Players from a specific match
#' players <- load_fox_players(match_ids = 123456)
#' }
load_fox_players <- function(match_ids = NULL, format = NULL,
                              gender = NULL,
                              source = c("local", "remote")) {
  source <- match.arg(source)

  if (source == "remote") {
    return(load_fox_remote("players", match_ids, format, gender))
  }

  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  where_clauses <- character()

  if (!is.null(match_ids)) {
    ids_sql <- paste(as.integer(match_ids), collapse = ", ")
    where_clauses <- c(where_clauses, sprintf("p.match_id IN (%s)", ids_sql))
  }
  if (!is.null(format)) {
    where_clauses <- c(where_clauses,
      sprintf("LOWER(p.format) = '%s'", escape_sql_quotes(tolower(format))))
  }

  # Gender filter requires joining with details table
  if (!is.null(gender)) {
    where_clauses <- c(where_clauses,
      sprintf("LOWER(d.gender) = '%s'", escape_sql_quotes(tolower(gender))))
    where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))
    sql <- sprintf(
      "SELECT p.* FROM foxsports.players p
       INNER JOIN foxsports.details d ON p.match_id = d.match_id
       %s ORDER BY p.match_id, p.team_id, p.position_order",
      where_sql)
  } else if (length(where_clauses) > 0) {
    where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))
    sql <- sprintf(
      "SELECT p.* FROM foxsports.players p %s
       ORDER BY p.match_id, p.team_id, p.position_order",
      where_sql)
  } else {
    sql <- "SELECT * FROM foxsports.players ORDER BY match_id, team_id, position_order"
  }

  result <- tryCatch(
    DBI::dbGetQuery(conn, sql),
    error = function(e) {
      if (grepl("does not exist|not found|no such table", e$message, ignore.case = TRUE)) {
        cli::cli_abort(c(
          "Fox Sports tables not found in database.",
          "i" = "Run {.fn add_foxsports_tables} or {.fn ingest_fox_sports_data} first."))
      }
      stop(e)
    }
  )

  if (nrow(result) == 0) {
    cli::cli_warn("No Fox Sports player data found for the specified filters")
    return(data.frame())
  }

  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} Fox Sports player rows")
  result
}


# ============================================================================
# REMOTE LOADERS
# ============================================================================

# Session-level cache for foxsports remote release info
.fox_remote_cache <- new.env(parent = emptyenv())


#' Get Fox Sports Release Info
#'
#' Gets the foxsports release info, with session-level caching.
#'
#' @return List with release info (tag_name, assets, etc.).
#' @keywords internal
get_fox_release <- function() {
  if (exists("release_info", envir = .fox_remote_cache)) {
    return(get("release_info", envir = .fox_remote_cache))
  }

  cli::cli_alert_info("Finding foxsports release...")
  release <- tryCatch(
    get_latest_release(type = "foxsports"),
    error = function(e) {
      cli::cli_abort(c(
        "Failed to find foxsports release on GitHub.",
        "i" = "Check your internet connection, or use {.code source = \"local\"}.",
        "x" = conditionMessage(e)))
    }
  )
  assign("release_info", release, envir = .fox_remote_cache)
  release
}


#' Query Remote Fox Sports Parquet
#'
#' Downloads a parquet file from the foxsports GitHub release to a temp
#' file, then runs a SQL query on it.
#'
#' @param asset_name Character. Name of the release asset (with .parquet).
#' @param sql_template Character. SQL query with \code{\{table\}} placeholder.
#'
#' @return Data frame with query results.
#' @keywords internal
query_remote_fox_parquet <- function(asset_name, sql_template) {
  release <- get_fox_release()

  parquet_url <- sprintf(
    "https://github.com/peteowen1/bouncerdata/releases/download/%s/%s",
    release$tag_name, asset_name
  )

  temp_file <- tempfile(fileext = ".parquet")
  on.exit(unlink(temp_file), add = TRUE)

  tryCatch(
    httr2::request(parquet_url) |>
      httr2::req_timeout(300) |>
      httr2::req_perform(path = temp_file),
    error = function(e) {
      cli::cli_abort(c(
        "Failed to download {.val {asset_name}} from foxsports release.",
        "i" = "Release tag: {release$tag_name}",
        "x" = conditionMessage(e)))
    }
  )

  temp_file_normalized <- normalizePath(temp_file, winslash = "/", mustWork = FALSE)
  if (!file.exists(temp_file_normalized)) {
    cli::cli_abort("Downloaded file missing: {.file {temp_file}}")
  }

  check_duckdb_available()
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  sql <- gsub("\\{table\\}", sprintf("'%s'", temp_file_normalized), sql_template)
  DBI::dbGetQuery(conn, sql)
}


#' Load Fox Sports Data from Remote
#'
#' Downloads combined parquets from foxsports release and queries them.
#' Asset naming: \code{all_\{format\}_matches.parquet} (balls),
#' \code{all_\{format\}_players.parquet}, \code{all_\{format\}_details.parquet}.
#'
#' @param table_type Character. "balls", "players", or "details".
#' @param match_ids Integer vector of match IDs, or NULL.
#' @param format Character. Format filter, or NULL.
#' @param gender Character. Gender filter, or NULL.
#'
#' @return Data frame.
#' @keywords internal
load_fox_remote <- function(table_type, match_ids, format, gender) {
  release <- get_fox_release()
  assets <- sapply(release$assets, function(a) a$name)

  # Map table_type to asset filename pattern
  asset_suffix <- switch(table_type,
    "balls" = "matches",
    "players" = "players",
    "details" = "details",
    cli::cli_abort("Unknown table_type: {.val {table_type}}. Must be 'balls', 'players', or 'details'.")
  )

  # Find matching assets
  pattern <- sprintf("^all_.*_%s\\.parquet$", asset_suffix)
  matching_assets <- assets[grepl(pattern, assets)]

  if (!is.null(format)) {
    matching_assets <- matching_assets[grepl(tolower(format), tolower(matching_assets))]
  }

  if (length(matching_assets) == 0) {
    cli::cli_warn("No remote Fox Sports {table_type} assets found")
    return(data.frame())
  }

  # Build SQL
  where_clauses <- character()
  if (!is.null(match_ids)) {
    ids_sql <- paste(as.integer(match_ids), collapse = ", ")
    where_clauses <- c(where_clauses, sprintf("CAST(match_id AS INTEGER) IN (%s)", ids_sql))
  }
  if (!is.null(gender) && table_type != "details") {
    cli::cli_warn(c(
      "Gender filter is not available for {table_type} in remote mode.",
      "i" = "Use format-specific codes instead (e.g., {.val wbbl} for women's BBL)."))
  }
  if (!is.null(gender) && table_type == "details") {
    # Gender is only derivable from format name for remote, not stored in parquet
    # Filter by format name instead
    female_fmts <- c("wt20i", "wodi", "wbbl", "wpl", "wncl", "wt20wc")
    if (tolower(gender) == "female") {
      matching_assets <- matching_assets[grepl(paste(female_fmts, collapse = "|"),
                                                tolower(matching_assets))]
    } else {
      matching_assets <- matching_assets[!grepl(paste(female_fmts, collapse = "|"),
                                                 tolower(matching_assets))]
    }
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  sql_template <- sprintf("SELECT * FROM {table} %s", where_sql)

  cli::cli_alert_info("Downloading {length(matching_assets)} Fox Sports {table_type} file{?s}...")

  dfs <- lapply(matching_assets, function(asset_name) {
    tryCatch({
      query_remote_fox_parquet(asset_name, sql_template)
    }, error = function(e) {
      cli::cli_alert_warning("Failed to download {asset_name}: {e$message}")
      NULL
    })
  })

  valid_dfs <- Filter(function(x) !is.null(x) && nrow(x) > 0, dfs)
  if (length(valid_dfs) == 0) {
    cli::cli_warn("No remote Fox Sports {table_type} data found")
    return(data.frame())
  }

  result <- dplyr::bind_rows(valid_dfs)
  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} Fox Sports {table_type} rows from remote")
  result
}
