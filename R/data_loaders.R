# Data Loader Functions
#
# User-facing functions for loading bouncer data from local DuckDB or remote
# GitHub releases. Uses DuckDB for both - local queries the database file,
# remote uses httpfs to query parquet files directly from GitHub.
#
# All functions use source = "local" (default) or "remote".

# Session-level cache for remote connection setup
.bouncer_remote_cache <- new.env(parent = emptyenv())


#' Fast Remote Parquet Query
#'
#' Downloads a parquet file from GitHub releases to a temp file, then runs
#' a SQL query on it using DuckDB. This is ~7x faster than httpfs for
#' aggregation queries because:
#' 1. Download is done once with optimized HTTP
#' 2. DuckDB queries local file (no network latency per query)
#' 3. Only aggregated results come into R memory
#'
#' @param table_name Character. Name of the parquet file (without .parquet).
#' @param sql_template Character. SQL query template with \code{\{table\}} placeholder.
#' @param release Optional. Release info from get_latest_release().
#'
#' @return Data frame with query results.
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' # Aggregate bowling stats
#' sql <- "SELECT bowler_id, COUNT(*) as balls FROM \{table\} GROUP BY bowler_id"
#' result <- query_remote_parquet("deliveries_ODI_male", sql)
#' }
query_remote_parquet <- function(table_name, sql_template, release = NULL) {
  # Get release info (use cache)
  if (is.null(release)) {
    if (!exists("release_info", envir = .bouncer_remote_cache)) {
      cli::cli_alert_info("Finding latest GitHub release...")
      release <- get_latest_release(type = "cricsheet")
      assign("release_info", release, envir = .bouncer_remote_cache)
    } else {
      release <- get("release_info", envir = .bouncer_remote_cache)
    }
  }

  # Build URL
  parquet_url <- sprintf(
    "https://github.com/peteowen1/bouncerdata/releases/download/%s/%s.parquet",
    release$tag_name, table_name
  )

  # Download to temp file
  temp_file <- tempfile(fileext = ".parquet")
  on.exit(unlink(temp_file), add = TRUE)

  httr2::request(parquet_url) |>
    httr2::req_timeout(300) |>
    httr2::req_perform(path = temp_file)

  # Normalize path for DuckDB (use forward slashes on all platforms)
  temp_file_normalized <- normalizePath(temp_file, winslash = "/", mustWork = TRUE)

  # Run SQL query with DuckDB
  check_duckdb_available()
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

  # Replace {table} placeholder with actual file path
  sql <- gsub("\\{table\\}", sprintf("'%s'", temp_file_normalized), sql_template)

  DBI::dbGetQuery(conn, sql)
}


#' Get Available Remote Tables
#'
#' Returns list of parquet files available in the GitHub cricsheet release.
#' Useful for discovering what data is available for remote queries.
#'
#' @return Character vector of table names (without .parquet extension).
#'
#' @export
#' @examples
#' \dontrun{
#' # See what tables are available remotely
#' tables <- get_remote_tables()
#' print(tables)
#' # Example output: "matches", "players", "deliveries_T20_male", etc.
#' }
get_remote_tables <- function() {
  if (!exists("release_info", envir = .bouncer_remote_cache)) {
    release <- get_latest_release(type = "cricsheet")
    assign("release_info", release, envir = .bouncer_remote_cache)
  } else {
    release <- get("release_info", envir = .bouncer_remote_cache)
  }

  assets <- sapply(release$assets, function(a) a$name)
  parquets <- assets[grepl("\\.parquet$", assets)]
  tools::file_path_sans_ext(parquets)
}


#' Get Data Connection
#'
#' Internal helper to get a DuckDB connection for data loading.
#' For local: connects to the bouncer.duckdb file.
#' For remote: creates an in-memory DuckDB with httpfs views to GitHub parquet.
#'
#' @param source Character. "local" or "remote".
#'
#' @return A DuckDB connection. Caller is responsible for disconnecting.
#' @keywords internal
get_data_connection <- function(source = c("local", "remote")) {
  source <- match.arg(source)

  if (source == "local") {
    return(get_db_connection(read_only = TRUE))
  }

  create_remote_connection()
}


#' Create Remote Connection with GitHub Parquet Views
#'
#' Creates a DuckDB connection with httpfs extension loaded and views
#' pointing to parquet files on GitHub releases.
#'
#' @return A DuckDB connection with views for all tables.
#' @keywords internal
create_remote_connection <- function() {
  # Get release info (cache it for the session)
  if (!exists("release_info", envir = .bouncer_remote_cache)) {
    cli::cli_alert_info("Finding latest GitHub release...")
    release <- get_latest_release(type = "cricsheet")
    assign("release_info", release, envir = .bouncer_remote_cache)
  } else {
    release <- get("release_info", envir = .bouncer_remote_cache)
  }

  base_url <- sprintf("https://github.com/peteowen1/bouncerdata/releases/download/%s",
                      release$tag_name)

  # Create DuckDB connection
  check_duckdb_available()
  conn <- DBI::dbConnect(duckdb::duckdb())

  # Install and load httpfs extension
  DBI::dbExecute(conn, "INSTALL httpfs")
  DBI::dbExecute(conn, "LOAD httpfs")

  # Get list of available parquet files from release assets
  available_assets <- sapply(release$assets, function(a) a$name)
  parquet_assets <- available_assets[grepl("\\.parquet$", available_assets)]

  # Create views for each parquet file
  for (asset_name in parquet_assets) {
    table_name <- tools::file_path_sans_ext(asset_name)
    parquet_url <- sprintf("%s/%s", base_url, asset_name)

    view_sql <- sprintf("CREATE VIEW %s AS SELECT * FROM '%s'",
                        table_name, parquet_url)
    tryCatch(
      DBI::dbExecute(conn, view_sql),
      error = function(e) NULL
    )
  }

  # Create unified 'matches' view (UNION ALL of all matches_* tables)
  match_views <- grep("^matches_", parquet_assets, value = TRUE)
  match_views <- tools::file_path_sans_ext(match_views)
  if (length(match_views) > 0) {
    union_sql <- paste0("SELECT * FROM ", match_views, collapse = " UNION ALL ")
    tryCatch(
      DBI::dbExecute(conn, sprintf("CREATE VIEW matches AS %s", union_sql)),
      error = function(e) NULL
    )
  }

  # Create unified 'deliveries' view
  delivery_views <- grep("^deliveries_", parquet_assets, value = TRUE)
  delivery_views <- tools::file_path_sans_ext(delivery_views)
  if (length(delivery_views) > 0) {
    union_sql <- paste0("SELECT * FROM ", delivery_views, collapse = " UNION ALL ")
    tryCatch(
      DBI::dbExecute(conn, sprintf("CREATE VIEW deliveries AS %s", union_sql)),
      error = function(e) NULL
    )
  }

  cli::cli_alert_success("Connected to remote data (release: {release$tag_name})")
  conn
}


#' All Valid Match Types
#' @keywords internal
ALL_MATCH_TYPES <- c("Test", "ODI", "T20", "IT20", "MDM", "ODM")


#' Load Matches Data
#'
#' Load match data from local DuckDB or remote GitHub releases.
#' Filter by match_type, gender, and team_type.
#'
#' @param match_type Character or vector. One or more of: "Test", "ODI", "T20",
#'   "IT20", "MDM", "ODM", or "all" (default).
#' @param gender Character. "male", "female", or "all" (default).
#' @param team_type Character. "international", "club", or "all" (default).
#' @param source Character. "local" (default) uses local DuckDB.
#'   "remote" queries GitHub releases via fast download.
#'
#' @return Data frame of matches, sorted by most recent first.
#'
#' @export
#' @examples
#' \dontrun{
#' # Load all matches from local database
#' matches <- load_matches()
#'
#' # Load only T20 international men's matches
#' t20i_men <- load_matches("T20", "male", "international")
#'
#' # Load multiple match types
#' limited_overs <- load_matches(c("ODI", "T20"), "male", "international")
#'
#' # Load from GitHub releases (no local install needed)
#' matches <- load_matches(source = "remote")
#' }
load_matches <- function(match_type = "all", gender = "all", team_type = "all",
                         source = c("local", "remote")) {
  source <- match.arg(source)

  # Build WHERE clause
  where_clauses <- character()

  if (!identical(match_type, "all")) {
    types_sql <- paste0("'", match_type, "'", collapse = ", ")
    where_clauses <- c(where_clauses, sprintf("match_type IN (%s)", types_sql))
  }
  if (gender != "all") {
    where_clauses <- c(where_clauses, sprintf("gender = '%s'", gender))
  }
  if (team_type != "all") {
    where_clauses <- c(where_clauses, sprintf("team_type = '%s'", team_type))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  if (source == "remote") {
    # Fast remote: download + local query
    cli::cli_alert_info("Loading matches from remote...")
    sql_template <- sprintf("SELECT * FROM {table} %s ORDER BY match_date DESC", where_sql)
    result <- tryCatch({
      query_remote_parquet("matches", sql_template)
    }, error = function(e) {
      cli::cli_abort("Remote query failed: {e$message}")
    })
  } else {
    # Local DuckDB
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))
    sql <- sprintf("SELECT * FROM matches %s ORDER BY match_date DESC", where_sql)
    result <- DBI::dbGetQuery(conn, sql)
  }

  if (nrow(result) == 0) {
    cli::cli_warn("No matches found for the specified filters")
    return(data.frame())
  }

  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} matches")
  result
}


#' Load Deliveries Data
#'
#' Load ball-by-ball delivery data from local DuckDB or remote GitHub releases.
#' Filter by match_type, gender, team_type, and specific match_ids.
#'
#' @param match_type Character or vector. One or more of: "Test", "ODI", "T20",
#'   "IT20", "MDM", "ODM", or "all" (default).
#' @param gender Character. "male", "female", or "all" (default).
#' @param team_type Character. "international", "club", or "all" (default).
#' @param match_ids Character vector. Optional filter for specific match_ids.
#' @param source Character. "local" (default) or "remote".
#'
#' @return Data frame of deliveries.
#'
#' @export
#' @examples
#' \dontrun{
#' # Load all deliveries from local database
#' deliveries <- load_deliveries()
#'
#' # Load only T20 men's deliveries
#' t20_men <- load_deliveries("T20", "male")
#'
#' # Load IPL deliveries (T20, male, club)
#' ipl <- load_deliveries("T20", "male", "club")
#'
#' # Load specific matches
#' subset <- load_deliveries(match_ids = c("1234567", "1234568"))
#'
#' # Load from GitHub releases
#' deliveries <- load_deliveries(source = "remote")
#' }
load_deliveries <- function(match_type = "all", gender = "all", team_type = "all",
                            match_ids = NULL, source = c("local", "remote")) {
  source <- match.arg(source)

  if (source == "remote") {
    # Fast remote: download + local query
    # Deliveries are partitioned by format_gender, so we need the right file
    if (identical(match_type, "all") || length(match_type) > 1) {
      cli::cli_abort("Remote deliveries requires a single match_type (e.g., 'T20', 'ODI', 'Test')")
    }

    # Determine the correct parquet file
    gender_suffix <- if (gender == "all") "male" else gender
    table_name <- sprintf("deliveries_%s_%s", match_type, gender_suffix)

    cli::cli_alert_info("Loading {table_name} from remote...")

    # Build WHERE clause for direct query (no joins needed - deliveries has match_type)
    where_clauses <- character()
    if (!is.null(match_ids)) {
      ids_sql <- paste0("'", match_ids, "'", collapse = ", ")
      where_clauses <- c(where_clauses, sprintf("match_id IN (%s)", ids_sql))
    }

    where_sql <- if (length(where_clauses) > 0) {
      paste("WHERE", paste(where_clauses, collapse = " AND "))
    } else {
      ""
    }

    sql_template <- sprintf("SELECT * FROM {table} %s", where_sql)
    result <- tryCatch({
      query_remote_parquet(table_name, sql_template)
    }, error = function(e) {
      cli::cli_abort("Remote query failed: {e$message}")
    })

  } else {
    # Local DuckDB
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

    # Build query - need to join with matches for filtering
    # For efficiency, if filtering by match_ids only, query deliveries directly
    if (!is.null(match_ids) && identical(match_type, "all") &&
        gender == "all" && team_type == "all") {
      # Direct query with match_id filter
      ids_sql <- paste0("'", match_ids, "'", collapse = ", ")
      sql <- sprintf("SELECT * FROM deliveries WHERE match_id IN (%s)", ids_sql)
    } else {
      # Need to filter through matches table
      where_clauses <- character()

      if (!identical(match_type, "all")) {
        types_sql <- paste0("'", match_type, "'", collapse = ", ")
        where_clauses <- c(where_clauses, sprintf("m.match_type IN (%s)", types_sql))
      }
      if (gender != "all") {
        where_clauses <- c(where_clauses, sprintf("m.gender = '%s'", gender))
      }
      if (team_type != "all") {
        where_clauses <- c(where_clauses, sprintf("m.team_type = '%s'", team_type))
      }
      if (!is.null(match_ids)) {
        ids_sql <- paste0("'", match_ids, "'", collapse = ", ")
        where_clauses <- c(where_clauses, sprintf("d.match_id IN (%s)", ids_sql))
      }

      if (length(where_clauses) > 0) {
        where_sql <- paste("WHERE", paste(where_clauses, collapse = " AND "))
        sql <- sprintf(
          "SELECT d.* FROM deliveries d
           INNER JOIN matches m ON d.match_id = m.match_id
           %s", where_sql
        )
      } else {
        sql <- "SELECT * FROM deliveries"
      }
    }

    result <- DBI::dbGetQuery(conn, sql)
  }

  if (nrow(result) == 0) {
    cli::cli_warn("No deliveries found for the specified filters")
    return(data.frame())
  }

  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} deliveries")
  result
}


#' Load Players Data
#'
#' Load the player registry from local DuckDB or remote GitHub releases.
#'
#' @param source Character. "local" (default) or "remote".
#'
#' @return Data frame of players.
#'
#' @export
#' @examples
#' \dontrun{
#' players <- load_players()
#' players <- load_players(source = "remote")
#' }
load_players <- function(source = c("local", "remote")) {
  source <- match.arg(source)

  if (source == "remote") {
    # Fast remote: download + local query
    cli::cli_alert_info("Loading players from remote...")
    result <- tryCatch({
      query_remote_parquet("players", "SELECT * FROM {table}")
    }, error = function(e) {
      cli::cli_abort("Remote query failed: {e$message}")
    })
  } else {
    # Local DuckDB
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))
    result <- DBI::dbGetQuery(conn, "SELECT * FROM players")
  }

  if (nrow(result) == 0) {
    cli::cli_warn("No players found")
    return(data.frame())
  }

  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} players")
  result
}


#' Load Match Innings Data
#'
#' Load innings-level summaries from local DuckDB or remote GitHub releases.
#' Includes target info, super over flags, and absent hurt data.
#'
#' @param match_type Character or vector. Filter by match type.
#' @param gender Character. "male", "female", or "all".
#' @param match_ids Character vector. Filter for specific matches.
#' @param source Character. "local" (default) or "remote".
#'
#' @return Data frame of innings summaries.
#'
#' @export
#' @examples
#' \dontrun{
#' innings <- load_innings()
#' t20_innings <- load_innings(match_type = "T20")
#' }
load_innings <- function(match_type = "all", gender = "all",
                         match_ids = NULL, source = c("local", "remote")) {
  source <- match.arg(source)

  # Build WHERE clause
  where_clauses <- character()
  if (!is.null(match_ids)) {
    ids_sql <- paste0("'", match_ids, "'", collapse = ", ")
    where_clauses <- c(where_clauses, sprintf("match_id IN (%s)", ids_sql))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  if (source == "remote") {
    # Fast remote: download + local query
    cli::cli_alert_info("Loading innings from remote...")

    # Check if match_innings parquet exists
    available <- tryCatch({
      get_remote_tables()
    }, error = function(e) character(0))

    if (!"match_innings" %in% available) {
      cli::cli_abort("match_innings not available in remote release. Use source='local'.")
    }

    sql_template <- sprintf("SELECT * FROM {table} %s", where_sql)
    result <- tryCatch({
      query_remote_parquet("match_innings", sql_template)
    }, error = function(e) {
      cli::cli_abort("Remote query failed: {e$message}")
    })

    # Apply match_type/gender filters after loading if needed (no join available)
    if (!identical(match_type, "all") || gender != "all") {
      cli::cli_alert_info("Applying match type/gender filters...")
      matches <- load_matches(match_type = match_type, gender = gender, source = "remote")
      result <- result[result$match_id %in% matches$match_id, ]
    }
  } else {
    # Local DuckDB - can do efficient joins
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

    join_where <- character()
    if (!identical(match_type, "all")) {
      types_sql <- paste0("'", match_type, "'", collapse = ", ")
      join_where <- c(join_where, sprintf("m.match_type IN (%s)", types_sql))
    }
    if (gender != "all") {
      join_where <- c(join_where, sprintf("m.gender = '%s'", gender))
    }
    if (!is.null(match_ids)) {
      ids_sql <- paste0("'", match_ids, "'", collapse = ", ")
      join_where <- c(join_where, sprintf("i.match_id IN (%s)", ids_sql))
    }

    if (length(join_where) > 0) {
      where_sql <- paste("WHERE", paste(join_where, collapse = " AND "))
      sql <- sprintf(
        "SELECT i.* FROM match_innings i
         INNER JOIN matches m ON i.match_id = m.match_id
         %s", where_sql
      )
    } else {
      sql <- "SELECT * FROM match_innings"
    }

    result <- DBI::dbGetQuery(conn, sql)
  }

  if (nrow(result) == 0) {
    cli::cli_warn("No innings data found for the specified filters")
    return(data.frame())
  }

  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} innings records")
  result
}


#' Load Powerplays Data
#'
#' Load powerplay periods from local DuckDB or remote GitHub releases.
#' Contains powerplay boundaries for each innings.
#'
#' @param match_type Character or vector. Filter by match type.
#' @param match_ids Character vector. Filter for specific matches.
#' @param source Character. "local" (default) or "remote".
#'
#' @return Data frame with powerplay periods (powerplay_id, match_id, innings,
#'   from_over, to_over, powerplay_type).
#'
#' @export
#' @examples
#' \dontrun{
#' powerplays <- load_powerplays()
#' t20_powerplays <- load_powerplays(match_type = "T20")
#' }
load_powerplays <- function(match_type = "all", match_ids = NULL,
                            source = c("local", "remote")) {
  source <- match.arg(source)

  # Build WHERE clause for match_ids
  where_clauses <- character()
  if (!is.null(match_ids)) {
    ids_sql <- paste0("'", match_ids, "'", collapse = ", ")
    where_clauses <- c(where_clauses, sprintf("match_id IN (%s)", ids_sql))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  if (source == "remote") {
    # Fast remote: download + local query
    cli::cli_alert_info("Loading powerplays from remote...")

    # Check if innings_powerplays parquet exists
    available <- tryCatch({
      get_remote_tables()
    }, error = function(e) character(0))

    if (!"innings_powerplays" %in% available) {
      cli::cli_abort("innings_powerplays not available in remote release. Use source='local'.")
    }

    sql_template <- sprintf("SELECT * FROM {table} %s", where_sql)
    result <- tryCatch({
      query_remote_parquet("innings_powerplays", sql_template)
    }, error = function(e) {
      cli::cli_abort("Remote query failed: {e$message}")
    })

    # Apply match_type filter after loading if needed
    if (!identical(match_type, "all")) {
      cli::cli_alert_info("Applying match type filter...")
      matches <- load_matches(match_type = match_type, source = "remote")
      result <- result[result$match_id %in% matches$match_id, ]
    }
  } else {
    # Local DuckDB - can do efficient joins
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

    join_where <- character()
    if (!identical(match_type, "all")) {
      types_sql <- paste0("'", match_type, "'", collapse = ", ")
      join_where <- c(join_where, sprintf("m.match_type IN (%s)", types_sql))
    }
    if (!is.null(match_ids)) {
      ids_sql <- paste0("'", match_ids, "'", collapse = ", ")
      join_where <- c(join_where, sprintf("p.match_id IN (%s)", ids_sql))
    }

    if (length(join_where) > 0) {
      where_sql <- paste("WHERE", paste(join_where, collapse = " AND "))
      sql <- sprintf(
        "SELECT p.* FROM innings_powerplays p
         INNER JOIN matches m ON p.match_id = m.match_id
         %s", where_sql
      )
    } else {
      sql <- "SELECT * FROM innings_powerplays"
    }

    result <- DBI::dbGetQuery(conn, sql)
  }

  if (nrow(result) == 0) {
    cli::cli_warn("No powerplay data found for the specified filters")
    return(data.frame())
  }

  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} powerplay records")
  result
}


#' Load Player Skill Indices
#'
#' Load player skill indices for a specific format or all formats.
#'
#' @param match_format Character. "t20", "odi", "test", or "all" (default).
#' @param source Character. "local" (default) or "remote".
#'
#' @return Data frame of player skill indices.
#'
#' @export
#' @examples
#' \dontrun{
#' # Load T20 player skills
#' t20_skills <- load_player_skill("t20")
#'
#' # Load all formats
#' all_skills <- load_player_skill("all")
#' }
load_player_skill <- function(match_format = "all", source = c("local", "remote")) {
  source <- match.arg(source)

  formats <- if (match_format == "all") c("t20", "odi", "test") else tolower(match_format)

  if (source == "remote") {
    # Fast remote: download + local query for each format
    cli::cli_alert_info("Loading player skills from remote...")
    available <- tryCatch({
      get_remote_tables()
    }, error = function(e) character(0))

    dfs <- lapply(formats, function(fmt) {
      table_name <- paste0(fmt, "_player_skill")
      if (!(table_name %in% available)) {
        return(NULL)
      }

      sql_template <- sprintf("SELECT *, '%s' AS format FROM {table}", fmt)
      tryCatch({
        query_remote_parquet(table_name, sql_template)
      }, error = function(e) NULL)
    })
  } else {
    # Local DuckDB
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

    dfs <- lapply(formats, function(fmt) {
      table_name <- paste0(fmt, "_player_skill")
      tables <- DBI::dbListTables(conn)
      if (!(table_name %in% tables)) {
        return(NULL)
      }

      sql <- sprintf("SELECT *, '%s' AS format FROM %s", fmt, table_name)
      tryCatch(
        DBI::dbGetQuery(conn, sql),
        error = function(e) NULL
      )
    })
  }

  valid_dfs <- Filter(Negate(is.null), dfs)

  if (length(valid_dfs) == 0) {
    cli::cli_warn("No player skill data found")
    return(data.frame())
  }

  result <- dplyr::bind_rows(valid_dfs)
  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} player skill records")
  result
}


#' Load Team Skill Indices
#'
#' Load team skill indices for a specific format or all formats.
#'
#' @inheritParams load_player_skill
#'
#' @return Data frame of team skill indices.
#'
#' @export
load_team_skill <- function(match_format = "all", source = c("local", "remote")) {
  source <- match.arg(source)

  formats <- if (match_format == "all") c("t20", "odi", "test") else tolower(match_format)

  if (source == "remote") {
    # Fast remote: download + local query for each format
    cli::cli_alert_info("Loading team skills from remote...")
    available <- tryCatch({
      get_remote_tables()
    }, error = function(e) character(0))

    dfs <- lapply(formats, function(fmt) {
      table_name <- paste0(fmt, "_team_skill")
      if (!(table_name %in% available)) {
        return(NULL)
      }

      sql_template <- sprintf("SELECT *, '%s' AS format FROM {table}", fmt)
      tryCatch({
        query_remote_parquet(table_name, sql_template)
      }, error = function(e) NULL)
    })
  } else {
    # Local DuckDB
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

    dfs <- lapply(formats, function(fmt) {
      table_name <- paste0(fmt, "_team_skill")
      tables <- DBI::dbListTables(conn)
      if (!(table_name %in% tables)) {
        return(NULL)
      }

      sql <- sprintf("SELECT *, '%s' AS format FROM %s", fmt, table_name)
      tryCatch(
        DBI::dbGetQuery(conn, sql),
        error = function(e) NULL
      )
    })
  }

  valid_dfs <- Filter(Negate(is.null), dfs)

  if (length(valid_dfs) == 0) {
    cli::cli_warn("No team skill data found")
    return(data.frame())
  }

  result <- dplyr::bind_rows(valid_dfs)
  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} team skill records")
  result
}


#' Load Venue Skill Indices
#'
#' Load venue skill indices for a specific format or all formats.
#'
#' @inheritParams load_player_skill
#'
#' @return Data frame of venue skill indices.
#'
#' @export
load_venue_skill <- function(match_format = "all", source = c("local", "remote")) {
  source <- match.arg(source)

  formats <- if (match_format == "all") c("t20", "odi", "test") else tolower(match_format)

  if (source == "remote") {
    # Fast remote: download + local query for each format
    cli::cli_alert_info("Loading venue skills from remote...")
    available <- tryCatch({
      get_remote_tables()
    }, error = function(e) character(0))

    dfs <- lapply(formats, function(fmt) {
      table_name <- paste0(fmt, "_venue_skill")
      if (!(table_name %in% available)) {
        return(NULL)
      }

      sql_template <- sprintf("SELECT *, '%s' AS format FROM {table}", fmt)
      tryCatch({
        query_remote_parquet(table_name, sql_template)
      }, error = function(e) NULL)
    })
  } else {
    # Local DuckDB
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

    dfs <- lapply(formats, function(fmt) {
      table_name <- paste0(fmt, "_venue_skill")
      tables <- DBI::dbListTables(conn)
      if (!(table_name %in% tables)) {
        return(NULL)
      }

      sql <- sprintf("SELECT *, '%s' AS format FROM %s", fmt, table_name)
      tryCatch(
        DBI::dbGetQuery(conn, sql),
        error = function(e) NULL
      )
    })
  }

  valid_dfs <- Filter(Negate(is.null), dfs)

  if (length(valid_dfs) == 0) {
    cli::cli_warn("No venue skill data found")
    return(data.frame())
  }

  result <- dplyr::bind_rows(valid_dfs)
  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} venue skill records")
  result
}


#' Load Team ELO Ratings
#'
#' Load team ELO ratings from local DuckDB or remote GitHub releases.
#'
#' @param source Character. "local" (default) or "remote".
#'
#' @return Data frame of team ELO ratings.
#'
#' @export
#' @examples
#' \dontrun{
#' elo <- load_team_elo()
#' elo <- load_team_elo(source = "remote")
#' }
load_team_elo <- function(source = c("local", "remote")) {
  source <- match.arg(source)

  if (source == "remote") {
    # Fast remote: download + local query
    cli::cli_alert_info("Loading team ELO from remote...")
    result <- tryCatch({
      query_remote_parquet("team_elo", "SELECT * FROM {table}")
    }, error = function(e) {
      cli::cli_abort("Remote query failed: {e$message}")
    })
  } else {
    # Local DuckDB
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))
    result <- DBI::dbGetQuery(conn, "SELECT * FROM team_elo")
  }

  if (nrow(result) == 0) {
    cli::cli_warn("No team ELO data found")
    return(data.frame())
  }

  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} team ELO records")
  result
}


#' Clear Remote Data Cache
#'
#' Clears the session-level cache of remote release info, forcing
#' the next load to re-fetch from GitHub.
#'
#' @return Invisible NULL.
#'
#' @export
#' @examples
#' \dontrun{
#' clear_remote_cache()
#' }
clear_remote_cache <- function() {
  rm(list = ls(envir = .bouncer_remote_cache), envir = .bouncer_remote_cache)
  cli::cli_alert_success("Remote data cache cleared")
  invisible(NULL)
}
