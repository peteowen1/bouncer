# Cricinfo Data Functions
#
# Ingestion, loaders, and fixtures API for Cricinfo Hawkeye ball-by-ball data.
# Cricinfo data includes Hawkeye fields (wagon wheel, pitch map, shot type)
# not available in Cricsheet.
#
# Data flow:
#   Python scraper → parquets → ingest_cricinfo_data() → DuckDB tables
#   DuckDB tables → load_cricinfo_*() → R data.frames
#   GitHub release → load_cricinfo_*(source="remote") → R data.frames


# ============================================================================
# INGESTION
# ============================================================================

#' Ingest Cricinfo Data into DuckDB
#'
#' Scans Cricinfo parquet directories for ball-by-ball, match, and innings
#' data and loads them into DuckDB. Skips matches already in the database.
#' Also loads the fixtures index if present.
#'
#' @param cricinfo_dir Character. Path to the cricinfo data directory
#'   (e.g., "../bouncerdata/cricinfo"). If NULL, auto-detects from
#'   bouncerdata sibling directory.
#' @param path Character. Database file path. If NULL, uses default.
#' @param formats Character vector. Formats to ingest. Default
#'   c("t20i", "odi", "test").
#' @param genders Character vector. Genders to ingest. Default
#'   c("male", "female").
#' @param verbose Logical. Print progress messages. Default TRUE.
#'
#' @return Invisibly returns a list with counts of ingested records.
#' @export
#'
#' @examples
#' \dontrun{
#' # Ingest all formats from default location
#' ingest_cricinfo_data()
#'
#' # Ingest only T20I male data
#' ingest_cricinfo_data(formats = "t20i", genders = "male")
#'
#' # Ingest from a specific directory
#' ingest_cricinfo_data(cricinfo_dir = "path/to/cricinfo")
#' }
ingest_cricinfo_data <- function(cricinfo_dir = NULL,
                                  path = NULL,
                                  formats = c("t20i", "odi", "test"),
                                  genders = c("male", "female"),
                                  verbose = TRUE) {
  # Auto-detect cricinfo directory

  if (is.null(cricinfo_dir)) {
    bd_dir <- find_bouncerdata_dir()
    if (is.null(bd_dir)) {
      cli::cli_abort("Cannot find bouncerdata directory. Provide {.arg cricinfo_dir} explicitly.")
    }
    cricinfo_dir <- file.path(bd_dir, "cricinfo")
  }

  if (!dir.exists(cricinfo_dir)) {
    cli::cli_abort("Cricinfo directory not found: {.file {cricinfo_dir}}")
  }

  conn <- get_db_connection(path = path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  # Ensure tables exist (idempotent)
  create_cricinfo_tables(conn, verbose = FALSE)

  total_matches <- 0L
  total_balls <- 0L
  total_innings <- 0L
  total_failed <- 0L

  for (fmt in formats) {
    for (gnd in genders) {
      dir_name <- paste0(fmt, "_", gnd)
      data_dir <- file.path(cricinfo_dir, dir_name)
      if (!dir.exists(data_dir)) next

      # Discover match IDs from both _match and _balls parquets
      match_parquets <- list.files(data_dir, pattern = "_match\\.parquet$",
                                    full.names = TRUE)
      ball_parquets <- list.files(data_dir, pattern = "_balls\\.parquet$",
                                   full.names = TRUE)
      all_ids <- unique(c(
        sub("_match\\.parquet$", "", basename(match_parquets)),
        sub("_balls\\.parquet$", "", basename(ball_parquets))
      ))
      if (length(all_ids) == 0) next

      # Get already-loaded match IDs
      existing <- tryCatch(
        DBI::dbGetQuery(conn,
          "SELECT DISTINCT match_id FROM cricinfo.matches")$match_id,
        error = function(e) {
          if (!grepl("does not exist|not found|no such table", e$message, ignore.case = TRUE)) {
            cli::cli_alert_warning("Failed to check existing matches: {e$message}")
          }
          character(0)
        }
      )

      new_ids <- setdiff(all_ids, existing)

      if (length(new_ids) == 0) {
        if (verbose) cli::cli_alert_info("{dir_name}: all {length(all_ids)} matches already loaded")
        next
      }

      if (verbose) {
        cli::cli_alert_info("{dir_name}: loading {length(new_ids)} new matches (skipping {length(all_ids) - length(new_ids)} existing)")
      }

      # --- Ingest matches (snake_case already) ---
      match_files <- file.path(data_dir, paste0(new_ids, "_match.parquet"))
      match_files <- match_files[file.exists(match_files)]
      if (length(match_files) > 0) {
        n_matches <- ingest_cricinfo_matches(conn, match_files)
        total_matches <- total_matches + n_matches
        total_failed <- total_failed + (attr(n_matches, "n_failed") %||% 0L)

        # Backfill gender from directory name (match parquets don't include it)
        loaded_ids <- sub("_match\\.parquet$", "", basename(match_files))
        validate_match_ids(loaded_ids, context = "ingest_cricinfo_data")
        ids_sql <- paste0("'", escape_sql_quotes(loaded_ids), "'", collapse = ", ")
        DBI::dbExecute(conn, sprintf(
          "UPDATE cricinfo.matches SET gender = '%s' WHERE match_id IN (%s) AND gender IS NULL",
          escape_sql_quotes(gnd), ids_sql
        ))
      }

      # --- Ingest balls (camelCase → snake_case mapping) ---
      ball_paths <- file.path(data_dir, paste0(new_ids, "_balls.parquet"))
      ball_exists <- file.exists(ball_paths)
      ball_files_new <- ball_paths[ball_exists]
      ball_ids_new <- new_ids[ball_exists]
      if (length(ball_files_new) > 0) {
        n_balls <- ingest_cricinfo_balls(conn, ball_files_new, ball_ids_new)
        total_balls <- total_balls + n_balls
        total_failed <- total_failed + (attr(n_balls, "n_failed") %||% 0L)
      }

      # --- Ingest innings (snake_case already) ---
      innings_paths <- file.path(data_dir, paste0(new_ids, "_innings.parquet"))
      innings_exists <- file.exists(innings_paths)
      innings_files_new <- innings_paths[innings_exists]
      innings_ids_new <- new_ids[innings_exists]
      if (length(innings_files_new) > 0) {
        n_innings <- ingest_cricinfo_innings(conn, innings_files_new, innings_ids_new)
        total_innings <- total_innings + n_innings
        total_failed <- total_failed + (attr(n_innings, "n_failed") %||% 0L)
      }
    }
  }

  # --- Load fixtures table ---
  n_fixtures <- ingest_cricinfo_fixtures(conn, cricinfo_dir, verbose)

  if (verbose) {
    cli::cli_h3("Cricinfo ingestion complete")
    cli::cli_alert_success("{total_matches} matches, {format(total_balls, big.mark=',')} balls, {format(total_innings, big.mark=',')} innings rows, {format(n_fixtures, big.mark=',')} fixtures")
    if (total_failed > 0) {
      cli::cli_alert_warning("{total_failed} file{?s} failed to load (see warnings above)")
    }
  }

  invisible(list(
    matches = total_matches,
    balls = total_balls,
    innings = total_innings,
    fixtures = n_fixtures,
    failed = total_failed
  ))
}


#' Ingest Cricinfo Match Parquets
#'
#' Loads match metadata parquets into cricinfo.matches table.
#' Match parquets already use snake_case columns.
#'
#' @param conn DuckDB connection.
#' @param match_files Character vector of match parquet file paths.
#'
#' @return Number of rows inserted.
#' @keywords internal
ingest_cricinfo_matches <- function(conn, match_files) {
  n_inserted <- 0L
  n_failed <- 0L

  for (f in match_files) {
    fp <- normalizePath(f, winslash = "/", mustWork = TRUE)
    # Extract match_id from filename to backfill when parquet has NULL match_id
    file_match_id <- sub("_match\\.parquet$", "", basename(f))
    validate_match_ids(file_match_id, context = "ingest_cricinfo_matches")
    tryCatch({
      n <- DBI::dbExecute(conn, sprintf("
        INSERT OR IGNORE INTO cricinfo.matches
        SELECT * REPLACE (COALESCE(CAST(match_id AS VARCHAR), '%s') AS match_id)
        FROM read_parquet('%s')
      ", escape_sql_quotes(file_match_id), fp))
      n_inserted <- n_inserted + n
    }, error = function(e) {
      cli::cli_alert_warning("Failed to load match {basename(f)}: {e$message}")
      n_failed <<- n_failed + 1L
    })
  }

  attr(n_inserted, "n_failed") <- n_failed
  n_inserted
}


#' Ingest Cricinfo Ball Parquets
#'
#' Loads ball-by-ball parquets into cricinfo.balls table with
#' camelCase to snake_case column mapping.
#'
#' @param conn DuckDB connection.
#' @param ball_files Character vector of ball parquet file paths.
#' @param match_ids Character vector of match IDs (parallel to ball_files).
#'
#' @return Number of rows inserted.
#' @keywords internal
ingest_cricinfo_balls <- function(conn, ball_files, match_ids) {
  n_inserted <- 0L
  n_failed <- 0L

  for (i in seq_along(ball_files)) {
    fp <- normalizePath(ball_files[i], winslash = "/", mustWork = TRUE)
    mid <- match_ids[i]
    validate_match_ids(mid, context = "ingest_cricinfo_balls")

    tryCatch({
      n <- DBI::dbExecute(conn, sprintf("
        INSERT OR IGNORE INTO cricinfo.balls
        SELECT
          CAST(\"id\" AS VARCHAR) AS id,
          '%s' AS match_id,
          \"inningNumber\" AS innings_number,
          \"overNumber\" AS over_number,
          \"ballNumber\" AS ball_number,
          \"oversActual\" AS overs_actual,
          \"oversUnique\" AS overs_unique,
          \"totalRuns\" AS total_runs,
          \"batsmanRuns\" AS batsman_runs,
          \"isFour\" AS is_four,
          \"isSix\" AS is_six,
          \"isWicket\" AS is_wicket,
          \"dismissalType\" AS dismissal_type,
          \"dismissalText\" AS dismissal_text,
          \"wides\" AS wides,
          \"noballs\" AS noballs,
          \"byes\" AS byes,
          \"legbyes\" AS legbyes,
          \"penalties\" AS penalties,
          \"wagonX\" AS wagon_x,
          \"wagonY\" AS wagon_y,
          \"wagonZone\" AS wagon_zone,
          \"pitchLine\" AS pitch_line,
          \"pitchLength\" AS pitch_length,
          \"shotType\" AS shot_type,
          \"shotControl\" AS shot_control,
          CAST(\"batsmanPlayerId\" AS VARCHAR) AS batsman_player_id,
          CAST(\"bowlerPlayerId\" AS VARCHAR) AS bowler_player_id,
          CAST(\"nonStrikerPlayerId\" AS VARCHAR) AS non_striker_player_id,
          CAST(\"outPlayerId\" AS VARCHAR) AS out_player_id,
          \"totalInningRuns\" AS total_innings_runs,
          \"totalInningWickets\" AS total_innings_wickets,
          \"predicted_score\" AS predicted_score,
          \"win_probability\" AS win_probability,
          \"event_type\" AS event_type,
          \"drs_successful\" AS drs_successful,
          \"title\" AS title,
          \"timestamp\" AS timestamp
        FROM read_parquet('%s')
      ", escape_sql_quotes(mid), fp))
      n_inserted <- n_inserted + n
    }, error = function(e) {
      cli::cli_alert_warning("Failed to load balls for match {mid}: {e$message}")
      n_failed <<- n_failed + 1L
    })
  }

  attr(n_inserted, "n_failed") <- n_failed
  n_inserted
}


#' Ingest Cricinfo Innings Parquets
#'
#' Loads innings/scorecard parquets into cricinfo.innings table.
#' Innings parquets already use snake_case columns.
#'
#' @param conn DuckDB connection.
#' @param innings_files Character vector of innings parquet file paths.
#' @param match_ids Character vector of match IDs (parallel to innings_files).
#'
#' @return Number of rows inserted.
#' @keywords internal
ingest_cricinfo_innings <- function(conn, innings_files, match_ids) {
  n_inserted <- 0L
  n_failed <- 0L

  for (i in seq_along(innings_files)) {
    fp <- normalizePath(innings_files[i], winslash = "/", mustWork = TRUE)
    mid <- match_ids[i]

    tryCatch({
      n <- DBI::dbExecute(conn, sprintf("
        INSERT OR IGNORE INTO cricinfo.innings
        SELECT '%s' AS match_id, *
        FROM read_parquet('%s')
      ", escape_sql_quotes(mid), fp))
      n_inserted <- n_inserted + n
    }, error = function(e) {
      cli::cli_alert_warning("Failed to load innings for match {mid}: {e$message}")
      n_failed <<- n_failed + 1L
    })
  }

  attr(n_inserted, "n_failed") <- n_failed
  n_inserted
}


#' Ingest Cricinfo Fixtures
#'
#' Loads the fixtures.parquet index into cricinfo.fixtures table.
#' Uses DELETE + INSERT for idempotent updates.
#'
#' @param conn DuckDB connection.
#' @param cricinfo_dir Character. Path to cricinfo data directory.
#' @param verbose Logical. Print progress.
#'
#' @return Number of fixtures loaded.
#' @keywords internal
ingest_cricinfo_fixtures <- function(conn, cricinfo_dir, verbose = TRUE) {
  fixtures_path <- file.path(cricinfo_dir, "fixtures.parquet")
  if (!file.exists(fixtures_path)) {
    if (verbose) cli::cli_alert_info("No fixtures.parquet found, skipping")
    return(0L)
  }

  fp <- normalizePath(fixtures_path, winslash = "/", mustWork = TRUE)

  # Replace all fixtures (idempotent full refresh)
  DBI::dbExecute(conn, "DELETE FROM cricinfo.fixtures")
  n <- DBI::dbExecute(conn, sprintf("
    INSERT INTO cricinfo.fixtures
    SELECT * FROM read_parquet('%s')
  ", fp))

  if (verbose) cli::cli_alert_success("Loaded {format(n, big.mark=',')} fixtures")
  n
}


# ============================================================================
# FORMAT NORMALIZATION
# ============================================================================

# Cricinfo API uses different format labels than our system:
#   API: T20, ODI, TEST, HUNDRED_BALL
#   Our system: t20i, odi, test
# This helper builds SQL that handles both conventions.

cricinfo_format_sql <- function(column, format) {
  fmt_upper <- toupper(format)
  if (fmt_upper %in% c("T20I", "T20", "IT20")) {
    sprintf("UPPER(%s) IN ('T20', 'T20I', 'IT20')", column)
  } else if (fmt_upper %in% c("ODI", "ODM")) {
    sprintf("UPPER(%s) IN ('ODI', 'ODM')", column)
  } else if (fmt_upper == "TEST") {
    sprintf("UPPER(%s) IN ('TEST', 'MDM')", column)
  } else {
    sprintf("UPPER(%s) = '%s'", column, escape_sql_quotes(fmt_upper))
  }
}


# ============================================================================
# FIXTURES API
# ============================================================================

#' Load Cricinfo Fixtures
#'
#' Returns the Cricinfo fixtures index — a schedule of all international
#' matches with status, teams, venue, and whether ball-by-ball data exists.
#'
#' @param format Character. "t20i", "odi", "test", or "all" (default).
#' @param gender Character. "male", "female", or "all" (default).
#' @param status Character. Match status filter: "all" (default), "POST"
#'   (completed), "PRE" (upcoming), "LIVE".
#' @param source Character. "remote" (default) downloads fixtures.parquet
#'   from the cricinfo GitHub release. "local" queries DuckDB.
#'
#' @return Data frame of fixtures.
#' @export
#'
#' @examples
#' \dontrun{
#' # All completed T20I fixtures
#' load_cricinfo_fixtures(format = "t20i", status = "POST")
#'
#' # All upcoming matches
#' load_cricinfo_fixtures(status = "PRE")
#'
#' # From remote (no local DB needed)
#' load_cricinfo_fixtures(source = "remote")
#' }
load_cricinfo_fixtures <- function(format = "all", gender = "all",
                                    status = "all",
                                    source = c("remote", "local")) {
  source <- match.arg(source)

  where_clauses <- character()
  if (format != "all") {
    where_clauses <- c(where_clauses,
      sprintf("format = '%s'", escape_sql_quotes(tolower(format))))
  }
  if (gender != "all") {
    where_clauses <- c(where_clauses,
      sprintf("gender = '%s'", escape_sql_quotes(tolower(gender))))
  }
  if (status != "all") {
    where_clauses <- c(where_clauses,
      sprintf("status = '%s'", escape_sql_quotes(toupper(status))))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  sql <- sprintf("SELECT * FROM {table} %s ORDER BY start_date DESC", where_sql)

  if (source == "remote") {
    cli::cli_alert_info("Loading Cricinfo fixtures from remote...")
    result <- tryCatch({
      query_remote_cricinfo_parquet("fixtures", sql)
    }, error = function(e) {
      cli::cli_abort("Remote fixtures query failed: {e$message}")
    })
  } else {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

    local_sql <- gsub("\\{table\\}", "cricinfo.fixtures", sql)
    result <- DBI::dbGetQuery(conn, local_sql)
  }

  if (nrow(result) == 0) {
    cli::cli_warn("No fixtures found for the specified filters")
    return(data.frame())
  }

  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} fixtures")
  result
}


#' Get Upcoming Matches
#'
#' Convenience function returning upcoming matches within a date window.
#'
#' @param format Character. "t20i", "odi", "test", or "all" (default).
#' @param gender Character. "male", "female", or "all" (default).
#' @param days_ahead Integer. Number of days ahead to look. Default 30.
#' @param source Character. "remote" (default) or "local".
#'
#' @return Data frame of upcoming fixtures.
#' @export
#'
#' @examples
#' \dontrun{
#' # Upcoming T20Is in the next 2 weeks
#' get_upcoming_matches(format = "t20i", days_ahead = 14)
#' }
get_upcoming_matches <- function(format = "all", gender = "all",
                                  days_ahead = 30,
                                  source = c("remote", "local")) {
  source <- match.arg(source)

  fixtures <- load_cricinfo_fixtures(format = format, gender = gender,
                                      status = "all", source = source)
  if (nrow(fixtures) == 0) return(data.frame())

  # Filter for PRE/LIVE status and within date window
  cutoff <- as.character(Sys.Date() + days_ahead)
  today <- as.character(Sys.Date())

  upcoming <- fixtures[
    fixtures$status %in% c("PRE", "LIVE") &
    !is.na(fixtures$start_date) &
    fixtures$start_date >= today &
    fixtures$start_date <= cutoff,
  ]

  if (nrow(upcoming) == 0) {
    cli::cli_alert_info("No upcoming matches in the next {days_ahead} days")
    return(data.frame())
  }

  # Sort by date ascending (soonest first)
  upcoming <- upcoming[order(upcoming$start_date), ]
  cli::cli_alert_success("Found {nrow(upcoming)} upcoming match{?es}")
  upcoming
}


#' Get Unscraped Matches
#'
#' Returns completed matches that don't yet have ball-by-ball data scraped.
#' Useful for identifying what still needs scraping.
#'
#' @param format Character. "t20i", "odi", "test", or "all" (default).
#' @param gender Character. "male", "female", or "all" (default).
#' @param source Character. "remote" (default) or "local".
#'
#' @return Data frame of unscraped fixtures.
#' @export
#'
#' @examples
#' \dontrun{
#' # T20Is that need scraping
#' get_unscraped_matches(format = "t20i")
#' }
get_unscraped_matches <- function(format = "all", gender = "all",
                                   source = c("remote", "local")) {
  source <- match.arg(source)

  fixtures <- load_cricinfo_fixtures(format = format, gender = gender,
                                      status = "all", source = source)
  if (nrow(fixtures) == 0) return(data.frame())

  # Completed but not scraped
  unscraped <- fixtures[
    fixtures$status %in% c("POST", "FINISHED", "RESULT") &
    (is.na(fixtures$has_ball_by_ball) | !fixtures$has_ball_by_ball),
  ]

  if (nrow(unscraped) == 0) {
    cli::cli_alert_info("All completed matches have ball-by-ball data")
    return(data.frame())
  }

  unscraped <- unscraped[order(unscraped$start_date, decreasing = TRUE), ]
  cli::cli_alert_success("Found {format(nrow(unscraped), big.mark=',')} unscraped match{?es}")
  unscraped
}


# ============================================================================
# R LOADERS
# ============================================================================

#' Query a Cricinfo Table from Local DuckDB
#'
#' Shared helper for all local Cricinfo loaders. Builds WHERE clauses,
#' optionally JOINs with cricinfo.matches for format/gender filtering,
#' and returns the result.
#'
#' @param table Character. Schema-qualified table (e.g., "cricinfo.balls").
#' @param alias Character. Table alias in SQL (e.g., "b", "m", "i").
#' @param order_cols Character. ORDER BY clause columns.
#' @param match_ids Character vector of match IDs (NULL = all).
#' @param format Character. Format filter (NULL = all).
#' @param gender Character. Gender filter (NULL = all).
#' @param data_label Character. Human-readable label for messages.
#'
#' @return Data frame.
#' @keywords internal
query_cricinfo_table <- function(table, alias, order_cols, match_ids, format,
                                  gender, data_label) {
  conn <- get_db_connection(read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  needs_join <- !is.null(format) || !is.null(gender)
  # For matches table, format/gender are direct columns — no join needed
  is_matches <- (table == "cricinfo.matches")
  needs_join <- needs_join && !is_matches

  where_clauses <- character()

  if (!is.null(match_ids)) {
    validate_match_ids(match_ids, context = paste0("load_cricinfo_", data_label))
    ids_sql <- paste0("'", escape_sql_quotes(match_ids), "'", collapse = ", ")
    prefix <- if (needs_join || !is_matches) paste0(alias, ".") else ""
    where_clauses <- c(where_clauses, sprintf("%smatch_id IN (%s)", prefix, ids_sql))
  }
  if (!is.null(format)) {
    fmt_col <- if (needs_join) "m.format" else "format"
    where_clauses <- c(where_clauses, cricinfo_format_sql(fmt_col, format))
  }
  if (!is.null(gender)) {
    gnd_col <- if (needs_join) "m.gender" else "gender"
    where_clauses <- c(where_clauses,
      sprintf("LOWER(%s) = '%s'", gnd_col, escape_sql_quotes(tolower(gender))))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  if (needs_join) {
    sql <- sprintf(
      "SELECT %s.* FROM %s %s INNER JOIN cricinfo.matches m ON %s.match_id = m.match_id %s ORDER BY %s",
      alias, table, alias, alias, where_sql, order_cols)
  } else if (is_matches) {
    sql <- sprintf("SELECT * FROM %s %s ORDER BY %s", table, where_sql, order_cols)
  } else {
    sql <- sprintf(
      "SELECT %s.* FROM %s %s %s ORDER BY %s",
      alias, table, alias, where_sql, order_cols)
  }

  result <- DBI::dbGetQuery(conn, sql)

  if (nrow(result) == 0) {
    cli::cli_warn("No Cricinfo {data_label} data found for the specified filters")
    return(data.frame())
  }

  cli::cli_alert_success("Loaded {base::format(nrow(result), big.mark=',')} Cricinfo {data_label} rows")
  result
}


#' Load Cricinfo Ball-by-Ball Data
#'
#' Load ball-by-ball data with Hawkeye fields (wagon wheel, pitch map,
#' shot type, win probability) from local DuckDB or remote GitHub release.
#'
#' @param match_ids Character vector. Filter for specific match IDs.
#'   If NULL, loads all.
#' @param format Character. "t20i", "odi", "test", or NULL (all formats).
#' @param gender Character. "male", "female", or NULL (all genders).
#' @param source Character. "remote" (default) or "local".
#'
#' @return Data frame of ball-by-ball data with Hawkeye columns.
#' @export
#'
#' @examples
#' \dontrun{
#' # Load balls for a specific match
#' balls <- load_cricinfo_balls(match_ids = "1502145")
#'
#' # Load all T20I balls
#' t20_balls <- load_cricinfo_balls(format = "t20i")
#'
#' # From remote
#' balls <- load_cricinfo_balls(match_ids = "1502145", source = "remote")
#' }
load_cricinfo_balls <- function(match_ids = NULL, format = NULL,
                                 gender = NULL,
                                 source = c("remote", "local")) {
  source <- match.arg(source)
  if (source == "remote") {
    return(load_cricinfo_remote("balls", match_ids, format, gender))
  }
  query_cricinfo_table("cricinfo.balls", "b",
    "b.match_id, b.innings_number, b.over_number, b.ball_number",
    match_ids, format, gender, "balls")
}


#' Load Cricinfo Match Metadata
#'
#' Load match-level metadata including venue, teams, officials, and
#' Hawkeye source information.
#'
#' @inheritParams load_cricinfo_balls
#'
#' @return Data frame of match metadata.
#' @export
#'
#' @examples
#' \dontrun{
#' match <- load_cricinfo_match(match_ids = "1502145")
#' t20_matches <- load_cricinfo_match(format = "t20i")
#' }
load_cricinfo_match <- function(match_ids = NULL, format = NULL,
                                 gender = NULL,
                                 source = c("remote", "local")) {
  source <- match.arg(source)
  if (source == "remote") {
    return(load_cricinfo_remote("match", match_ids, format, gender))
  }
  query_cricinfo_table("cricinfo.matches", "m",
    "start_date DESC",
    match_ids, format, gender, "match")
}


#' Load Cricinfo Innings/Scorecard Data
#'
#' Load batting scorecards with player info (DOB, batting/bowling style,
#' playing role) and batting statistics per innings.
#'
#' @inheritParams load_cricinfo_balls
#'
#' @return Data frame of batting scorecard data.
#' @export
#'
#' @examples
#' \dontrun{
#' innings <- load_cricinfo_innings(match_ids = "1502145")
#' t20_innings <- load_cricinfo_innings(format = "t20i")
#' }
load_cricinfo_innings <- function(match_ids = NULL, format = NULL,
                                   gender = NULL,
                                   source = c("remote", "local")) {
  source <- match.arg(source)
  if (source == "remote") {
    return(load_cricinfo_remote("innings", match_ids, format, gender))
  }
  query_cricinfo_table("cricinfo.innings", "i",
    "i.match_id, i.innings_number, i.batting_position",
    match_ids, format, gender, "innings")
}


# ============================================================================
# REMOTE LOADERS (download from cricinfo release)
# ============================================================================

# Session-level cache for cricinfo remote release info
.cricinfo_remote_cache <- new.env(parent = emptyenv())


#' Query Remote Cricinfo Parquet
#'
#' Downloads a parquet file from the cricinfo GitHub release to a temp
#' file, then runs a SQL query on it. Same pattern as query_remote_parquet()
#' but for the cricinfo release tag.
#'
#' @param table_name Character. Name of the parquet file (without .parquet).
#' @param sql_template Character. SQL query with \code{\{table\}} placeholder.
#'
#' @return Data frame with query results.
#' @keywords internal
query_remote_cricinfo_parquet <- function(table_name, sql_template) {
  release <- get_cricinfo_release()

  parquet_url <- sprintf(
    "https://github.com/peteowen1/bouncerdata/releases/download/%s/%s.parquet",
    release$tag_name, table_name
  )

  temp_file <- tempfile(fileext = ".parquet")
  on.exit(unlink(temp_file), add = TRUE)

  httr2::request(parquet_url) |>
    httr2::req_timeout(300) |>
    httr2::req_perform(path = temp_file)

  temp_file_normalized <- normalizePath(temp_file, winslash = "/", mustWork = TRUE)

  check_duckdb_available()
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  sql <- gsub("\\{table\\}", sprintf("'%s'", temp_file_normalized), sql_template)
  DBI::dbGetQuery(conn, sql)
}


#' Get Cricinfo Release Info
#'
#' Gets the cricinfo release info, with session-level caching.
#'
#' @return List with release info (tag_name, assets, etc.).
#' @keywords internal
get_cricinfo_release <- function() {
  if (exists("release_info", envir = .cricinfo_remote_cache)) {
    return(get("release_info", envir = .cricinfo_remote_cache))
  }

  cli::cli_alert_info("Finding cricinfo release...")
  release <- get_latest_release(type = "cricinfo")
  assign("release_info", release, envir = .cricinfo_remote_cache)
  release
}


#' Load Cricinfo Data from Remote
#'
#' Shared helper for remote Cricinfo loading. Downloads combined parquets
#' from the cricinfo release and filters by match_id.
#'
#' @param data_type Character. One of "balls", "match", "innings".
#' @param match_ids Character vector of match IDs (required).
#' @param format Character. Format filter (e.g., "t20i"). If NULL, searches all.
#' @param gender Character. Gender filter (e.g., "male"). If NULL, searches all.
#'
#' @return Data frame.
#' @keywords internal
load_cricinfo_remote <- function(data_type, match_ids, format, gender) {
  if (is.null(match_ids)) {
    cli::cli_abort("Remote Cricinfo {data_type} loading requires {.arg match_ids}")
  }

  release <- get_cricinfo_release()
  assets <- sapply(release$assets, function(a) a$name)

  pattern <- sprintf("^cricinfo_%s_.*\\.parquet$", data_type)
  matching_assets <- assets[grepl(pattern, assets)]
  if (!is.null(format)) matching_assets <- matching_assets[grepl(format, matching_assets)]
  if (!is.null(gender)) matching_assets <- matching_assets[grepl(gender, matching_assets)]

  if (length(matching_assets) == 0) {
    cli::cli_warn("No remote {data_type} data assets found")
    return(data.frame())
  }

  validate_match_ids(match_ids, context = paste0("load_cricinfo_", data_type, "_remote"))
  ids_sql <- paste0("'", escape_sql_quotes(match_ids), "'", collapse = ", ")
  sql_template <- sprintf("SELECT * FROM {{table}} WHERE match_id IN (%s)", ids_sql)

  dfs <- lapply(matching_assets, function(asset_name) {
    table_name <- tools::file_path_sans_ext(asset_name)
    tryCatch({
      query_remote_cricinfo_parquet(table_name, sql_template)
    }, error = function(e) {
      cli::cli_alert_warning("Failed to download {asset_name}: {e$message}")
      NULL
    })
  })

  valid_dfs <- Filter(function(x) !is.null(x) && nrow(x) > 0, dfs)
  if (length(valid_dfs) == 0) {
    cli::cli_warn("No remote {data_type} data found for requested match IDs")
    return(data.frame())
  }

  result <- dplyr::bind_rows(valid_dfs)
  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} Cricinfo {data_type} rows from remote")
  result
}


# Remote wrappers removed — public loaders call load_cricinfo_remote() directly.
