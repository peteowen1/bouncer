# Release Installation Functions
#
# User-facing functions for installing bouncer data from GitHub Releases.
# This provides an alternative to install_all_bouncer_data() that downloads
# pre-processed data from releases instead of building locally.

#' Get Latest Release Info from GitHub
#'
#' Fetches metadata about the latest release from the bouncerdata repository.
#'
#' @param repo Character. GitHub repository in "owner/repo" format.
#'   Default is "peteowen1/bouncerdata".
#' @param type Character. Release type to find: "daily" (JSON archives),
#'   "weekly" (parquet files), or "any" (most recent of either).
#'
#' @return List with release information including tag_name, published_at,
#'   and assets.
#'
#' @export
#' @examples
#' \dontrun{
#' # Get latest daily release
#' release <- get_latest_release(type = "daily")
#' print(release$tag_name)
#'
#' # Get latest weekly release (parquet files)
#' release <- get_latest_release(type = "weekly")
#' }
get_latest_release <- function(repo = "peteowen1/bouncerdata", type = "any") {
  type <- match.arg(type, c("any", "daily", "weekly"))

  # Build API URL
  if (type == "any") {
    url <- sprintf("https://api.github.com/repos/%s/releases/latest", repo)
  } else {
    url <- sprintf("https://api.github.com/repos/%s/releases", repo)
  }

  cli::cli_alert_info("Checking GitHub releases...")

  resp <- httr2::request(url) |>
    httr2::req_headers(Accept = "application/vnd.github.v3+json") |>
    httr2::req_user_agent("bouncer R package") |>
    httr2::req_timeout(30) |>
    httr2::req_perform()

  if (type == "any") {
    return(httr2::resp_body_json(resp))
  }

  # Filter releases by type
  releases <- httr2::resp_body_json(resp)

  pattern <- if (type == "weekly") "-weekly$" else "^v[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}$"

  matching <- Filter(function(r) grepl(pattern, r$tag_name), releases)

  if (length(matching) == 0) {
    cli::cli_abort("No {type} releases found in {repo}")
  }

  matching[[1]]  # Return most recent matching release
}


#' Download Release Asset
#'
#' Downloads a specific asset from a GitHub release.
#'
#' @param asset_url Character. URL of the asset to download.
#' @param dest_path Character. Destination file path.
#' @param show_progress Logical. Show download progress bar. Default TRUE.
#'
#' @return Invisibly returns the destination path.
#' @keywords internal
download_release_asset <- function(asset_url, dest_path, show_progress = TRUE) {
  req <- httr2::request(asset_url) |>
    httr2::req_headers(Accept = "application/octet-stream") |>
    httr2::req_user_agent("bouncer R package") |>
    httr2::req_timeout(600)  # 10 minute timeout for large files

  if (show_progress) {
    req <- req |> httr2::req_progress()
  }

  req |> httr2::req_perform(path = dest_path)

  invisible(dest_path)
}


#' Install Bouncerdata from GitHub Release
#'
#' Downloads and installs cricket data from the bouncerdata GitHub releases.
#' This is an alternative to `install_all_bouncer_data()` that downloads
#' pre-processed data instead of building locally from Cricsheet.
#'
#' @param repo Character. GitHub repository. Default "peteowen1/bouncerdata".
#' @param formats Character vector. Data formats to download:
#'   - "long_form" = Tests, First-class matches
#'   - "short_form" = ODIs, T20s, franchise leagues
#'   - "all" = both (default)
#' @param genders Character vector. "male", "female", or c("male", "female").
#'   Default is both.
#' @param types Character vector. "international", "club", or both.
#'   Default is both.
#' @param tag Character. Specific release tag to download, or "latest".
#' @param data_dir Character. Directory to store data. Default uses
#'   `find_bouncerdata_dir()`.
#' @param build_db Logical. If TRUE, build DuckDB from downloaded JSONs.
#'   Default TRUE.
#'
#' @return Invisibly returns the data directory path.
#'
#' @export
#' @examples
#' \dontrun{
#' # Install all data
#' install_bouncerdata_from_release()
#'
#' # Install only short form (T20s, ODIs)
#' install_bouncerdata_from_release(formats = "short_form")
#'
#' # Install only IPL (short form, male, club)
#' install_bouncerdata_from_release(
#'   formats = "short_form",
#'   genders = "male",
#'   types = "club"
#' )
#' }
install_bouncerdata_from_release <- function(repo = "peteowen1/bouncerdata",
                                              formats = "all",
                                              genders = c("male", "female"),
                                              types = c("international", "club"),
                                              tag = "latest",
                                              data_dir = NULL,
                                              build_db = TRUE) {

  cli::cli_h1("Installing Bouncerdata from GitHub Release")

  # Setup data directory
  if (is.null(data_dir)) {
    data_dir <- find_bouncerdata_dir(create = TRUE)
  }
  if (!dir.exists(data_dir)) {
    dir.create(data_dir, recursive = TRUE)
  }

  json_dir <- file.path(data_dir, "json_files")
  dir.create(json_dir, showWarnings = FALSE, recursive = TRUE)

  # Determine which folders to download
  if ("all" %in% formats) {
    formats <- c("long_form", "short_form")
  }

  folders_to_download <- character()
  for (fmt in formats) {
    for (gnd in genders) {
      for (typ in types) {
        folders_to_download <- c(folders_to_download,
                                  paste(fmt, gnd, typ, sep = "_"))
      }
    }
  }

  cli::cli_alert_info("Will download {length(folders_to_download)} data categories")

  # Get release
  cli::cli_h2("Step 1: Finding release")
  release <- if (tag == "latest") {
    get_latest_release(repo, type = "daily")
  } else {
    url <- sprintf("https://api.github.com/repos/%s/releases/tags/%s", repo, tag)
    resp <- httr2::request(url) |>
      httr2::req_headers(Accept = "application/vnd.github.v3+json") |>
      httr2::req_perform()
    httr2::resp_body_json(resp)
  }

  cli::cli_alert_success("Found release: {release$tag_name}")

  # Download requested ZIP files
  cli::cli_h2("Step 2: Downloading data")

  downloaded_count <- 0
  for (folder in folders_to_download) {
    zip_name <- paste0(folder, ".zip")

    # Find matching asset
    asset <- NULL
    for (a in release$assets) {
      if (a$name == zip_name) {
        asset <- a
        break
      }
    }

    if (is.null(asset)) {
      cli::cli_alert_warning("Asset not found: {zip_name}")
      next
    }

    # Download
    size_mb <- asset$size / 1024 / 1024
    cli::cli_alert_info("Downloading {zip_name} ({round(size_mb, 1)} MB)...")

    temp_zip <- tempfile(fileext = ".zip")
    download_release_asset(asset$browser_download_url, temp_zip)

    # Extract to json_dir
    folder_path <- file.path(json_dir, folder)
    dir.create(folder_path, showWarnings = FALSE, recursive = TRUE)
    zip::unzip(temp_zip, exdir = folder_path)
    file.remove(temp_zip)

    n_files <- length(list.files(folder_path, pattern = "\\.json$"))
    cli::cli_alert_success("{folder}: {n_files} matches")
    downloaded_count <- downloaded_count + n_files
  }

  cli::cli_alert_success("Downloaded {downloaded_count} match files")

  # Build database if requested
  if (build_db) {
    cli::cli_h2("Step 3: Building database")

    json_files <- list.files(json_dir, pattern = "\\.json$",
                              full.names = TRUE, recursive = TRUE)

    if (length(json_files) > 0) {
      initialize_bouncer_database(overwrite = TRUE)
      batch_load_matches(json_files, progress = TRUE)
      get_data_info()
    }
  }

  cli::cli_alert_success("Installation complete!")
  cli::cli_alert_info("Data directory: {data_dir}")

  invisible(data_dir)
}


#' Install Parquet Files from Weekly Release
#'
#' Downloads pre-built parquet files from the weekly release.
#' These can be queried directly with DuckDB or Arrow without
#' building a local database.
#'
#' @param repo Character. GitHub repository. Default "peteowen1/bouncerdata".
#' @param tables Character vector. Tables to download, or "all".
#'   Options: "matches", "deliveries_long_form", "deliveries_short_form",
#'   "players", "test_player_skill", "odi_player_skill", "t20_player_skill",
#'   "test_team_skill", "odi_team_skill", "t20_team_skill",
#'   "test_venue_skill", "odi_venue_skill", "t20_venue_skill", "team_elo"
#' @param data_dir Character. Directory to store parquet files.
#' @param tag Character. Specific release tag, or "latest".
#'
#' @return Character vector of downloaded file paths.
#'
#' @export
#' @examples
#' \dontrun{
#' # Download all parquet files
#' install_parquets_from_release()
#'
#' # Download only matches and deliveries
#' install_parquets_from_release(
#'   tables = c("matches", "deliveries_short_form")
#' )
#' }
install_parquets_from_release <- function(repo = "peteowen1/bouncerdata",
                                           tables = "all",
                                           data_dir = NULL,
                                           tag = "latest") {

  cli::cli_h1("Installing Parquet Files from Weekly Release")

  # Setup data directory
  if (is.null(data_dir)) {
    data_dir <- file.path(find_bouncerdata_dir(create = TRUE), "parquet")
  }
  dir.create(data_dir, showWarnings = FALSE, recursive = TRUE)

  # All available tables (8-way splits for matches/deliveries)
  all_tables <- c(
    # Unified tables
    "players", "team_elo",
    # Matches - 8 splits
    paste0("matches_", DATA_FOLDERS),
    # Deliveries - 8 splits
    paste0("deliveries_", DATA_FOLDERS),
    # Skill indices - format-specific
    "test_player_skill", "odi_player_skill", "t20_player_skill",
    "test_team_skill", "odi_team_skill", "t20_team_skill",
    "test_venue_skill", "odi_venue_skill", "t20_venue_skill"
  )

  if ("all" %in% tables) {
    tables <- all_tables
  }

  # Get weekly release
  release <- get_latest_release(repo, type = "weekly")
  cli::cli_alert_success("Found release: {release$tag_name}")

  # Download each table
  downloaded <- character()
  for (table in tables) {
    parquet_name <- paste0(table, ".parquet")

    # Find asset
    asset <- NULL
    for (a in release$assets) {
      if (a$name == parquet_name) {
        asset <- a
        break
      }
    }

    if (is.null(asset)) {
      cli::cli_alert_warning("Table not found: {table}")
      next
    }

    size_mb <- asset$size / 1024 / 1024
    cli::cli_alert_info("Downloading {parquet_name} ({round(size_mb, 1)} MB)...")

    dest_path <- file.path(data_dir, parquet_name)
    download_release_asset(asset$browser_download_url, dest_path)

    cli::cli_alert_success("Downloaded: {parquet_name}")
    downloaded <- c(downloaded, dest_path)
  }

  cli::cli_alert_success("Downloaded {length(downloaded)} parquet files to {data_dir}")

  invisible(downloaded)
}


#' Update Bouncerdata from Release
#'
#' Checks for new releases and updates local data if available.
#'
#' @param repo Character. GitHub repository.
#' @param data_dir Character. Data directory to update.
#'
#' @return Logical. TRUE if update was performed, FALSE if already up to date.
#'
#' @export
#' @examples
#' \dontrun{
#' # Check for and apply updates
#' update_bouncerdata()
#' }
update_bouncerdata <- function(repo = "peteowen1/bouncerdata",
                                 data_dir = NULL) {

  cli::cli_h1("Checking for Bouncerdata Updates")

  if (is.null(data_dir)) {
    data_dir <- find_bouncerdata_dir()
  }

  # Get current version from local manifest
  manifest_path <- file.path(data_dir, "json_files", "manifest.json")
  local_version <- if (file.exists(manifest_path)) {
    manifest <- jsonlite::fromJSON(manifest_path)
    manifest$release_date %||% "0.0.0"
  } else {
    "0.0.0"
  }

  cli::cli_alert_info("Local version: {local_version}")

  # Get latest release
  release <- get_latest_release(repo, type = "daily")
  remote_version <- release$tag_name

  cli::cli_alert_info("Remote version: {remote_version}")

  if (remote_version <= local_version) {
    cli::cli_alert_success("Already up to date!")
    return(invisible(FALSE))
  }

  cli::cli_alert_info("Update available: {local_version} -> {remote_version}")

  # Download and install
  install_bouncerdata_from_release(repo = repo, data_dir = data_dir)

  invisible(TRUE)
}


#' Connect to Bouncerdata from GitHub Releases
#'
#' Creates a DuckDB connection with views pointing to parquet files
#' hosted on GitHub Releases. No download required - queries run
#' directly against the remote files.
#'
#' @param repo Character. GitHub repository. Default "peteowen1/bouncerdata".
#' @param tag Character. Release tag to use, or "latest" for most recent weekly.
#' @param tables Character vector. Tables to create views for, or "all".
#'   Available: "matches", "players", "deliveries_long_form",
#'   "deliveries_short_form", "t20_player_skill", etc.
#'
#' @return A DuckDB connection with views for each table. Use standard
#'   DBI functions to query (dbGetQuery, dbSendQuery, etc.).
#'
#' @details
#' This function creates a lightweight connection that queries parquet
#' files directly from GitHub without downloading them first. This is
#' convenient for quick analyses but slower than local files for large
#' queries. For heavy analysis, use `install_parquets_from_release()`
#' to download files locally first.
#'
#' Remember to disconnect when done: `DBI::dbDisconnect(con, shutdown = TRUE)`
#'
#' @export
#' @examples
#' \dontrun{
#' # Connect to latest data
#' con <- connect_bouncerdata()
#'
#' # Query matches
#' ipl_matches <- DBI::dbGetQuery(con,
#'   "SELECT * FROM matches
#'    WHERE event_name LIKE '%Indian Premier League%'
#'    ORDER BY match_date DESC
#'    LIMIT 100")
#'
#' # Query T20 deliveries
#' kohli_balls <- DBI::dbGetQuery(con,
#'   "SELECT * FROM deliveries_short_form
#'    WHERE batter_id = 'V Kohli'
#'    LIMIT 1000")
#'
#' # Get player skill ratings
#' top_batters <- DBI::dbGetQuery(con,
#'   "SELECT * FROM t20_player_skill
#'    ORDER BY batter_scoring_index DESC
#'    LIMIT 20")
#'
#' # Don't forget to disconnect!
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
connect_bouncerdata <- function(repo = "peteowen1/bouncerdata",
                                 tag = "latest",
                                 tables = "all") {


  # All available tables in weekly releases (8-way splits for matches/deliveries)
  all_tables <- c(
    # Unified tables
    "players", "team_elo",
    # Matches - 8 splits
    paste0("matches_", DATA_FOLDERS),
    # Deliveries - 8 splits
    paste0("deliveries_", DATA_FOLDERS),
    # Skill indices - format-specific
    "test_player_skill", "odi_player_skill", "t20_player_skill",
    "test_team_skill", "odi_team_skill", "t20_team_skill",
    "test_venue_skill", "odi_venue_skill", "t20_venue_skill"
  )

  if ("all" %in% tables) {
    tables <- all_tables
  }

  # Get release info
  cli::cli_alert_info("Finding latest weekly release...")
  release <- get_latest_release(repo, type = "weekly")
  cli::cli_alert_success("Using release: {release$tag_name}")

  # Build asset URL map
  base_url <- sprintf("https://github.com/%s/releases/download/%s",
                      repo, release$tag_name)

  # Create DuckDB connection
  con <- DBI::dbConnect(duckdb::duckdb())

  # Install and load httpfs extension for remote file access
  DBI::dbExecute(con, "INSTALL httpfs")
  DBI::dbExecute(con, "LOAD httpfs")

  # Create views for each table
  views_created <- character()
  for (table in tables) {
    parquet_url <- sprintf("%s/%s.parquet", base_url, table)

    # Check if asset exists in release
    asset_exists <- any(sapply(release$assets, function(a) {
      a$name == paste0(table, ".parquet")
    }))

    if (!asset_exists) {
      cli::cli_alert_warning("Table not available: {table}")
      next
    }

    # Create view
    view_sql <- sprintf("CREATE VIEW %s AS SELECT * FROM '%s'", table, parquet_url)
    tryCatch({
      DBI::dbExecute(con, view_sql)
      views_created <- c(views_created, table)
    }, error = function(e) {
      cli::cli_alert_warning("Failed to create view for {table}: {e$message}")
    })
  }

  # Create convenience UNION views for matches and deliveries
  match_views <- grep("^matches_", views_created, value = TRUE)
  if (length(match_views) > 0) {
    union_sql <- paste0("SELECT * FROM ", match_views, collapse = " UNION ALL ")
    tryCatch({
      DBI::dbExecute(con, sprintf("CREATE VIEW matches AS %s", union_sql))
      views_created <- c(views_created, "matches")
    }, error = function(e) NULL)
  }

  delivery_views <- grep("^deliveries_", views_created, value = TRUE)
  if (length(delivery_views) > 0) {
    union_sql <- paste0("SELECT * FROM ", delivery_views, collapse = " UNION ALL ")
    tryCatch({
      DBI::dbExecute(con, sprintf("CREATE VIEW deliveries AS %s", union_sql))
      views_created <- c(views_created, "deliveries")
    }, error = function(e) NULL)
  }

  cli::cli_alert_success("Connected! {length(views_created)} tables available")
  cli::cli_alert_info("Query with: DBI::dbGetQuery(con, 'SELECT * FROM matches LIMIT 10')")

  con
}


#' Query Bouncerdata (One-liner)
#'
#' Execute a SQL query against bouncerdata parquet files with a single
#' function call. Creates a temporary connection, runs the query, and
#' returns the results.
#'
#' @param sql Character. SQL query to execute. Reference tables directly
#'   by name (matches, deliveries_short_form, etc.).
#' @param repo Character. GitHub repository. Default "peteowen1/bouncerdata".
#'
#' @return Data frame with query results.
#'
#' @details
#' This is a convenience wrapper for quick one-off queries. For multiple
#' queries or interactive exploration, use `connect_bouncerdata()` instead
#' to avoid reconnecting each time.
#'
#' @export
#' @examples
#' \dontrun{
#' # Quick query - no connection management needed
#' recent_t20s <- query_bouncerdata(
#'   "SELECT match_id, team1, team2, match_date
#'    FROM matches
#'    WHERE match_type = 'T20'
#'    ORDER BY match_date DESC
#'    LIMIT 20"
#' )
#'
#' # Get top T20 batters by skill index
#' top_batters <- query_bouncerdata(
#'   "SELECT player_id, batter_scoring_index, batter_survival_rate
#'    FROM t20_player_skill
#'    ORDER BY batter_scoring_index DESC
#'    LIMIT 10"
#' )
#' }
query_bouncerdata <- function(sql, repo = "peteowen1/bouncerdata") {

  # Get release info (cached within session would be nice, but keep simple)
  release <- get_latest_release(repo, type = "weekly")
  base_url <- sprintf("https://github.com/%s/releases/download/%s",
                      repo, release$tag_name)

  # Create temporary connection
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  # Load httpfs
  DBI::dbExecute(con, "INSTALL httpfs")
  DBI::dbExecute(con, "LOAD httpfs")

  # Create views for all available tables (8-way splits)
  tables <- c(
    # Unified tables
    "players", "team_elo",
    # Matches - 8 splits
    paste0("matches_", DATA_FOLDERS),
    # Deliveries - 8 splits
    paste0("deliveries_", DATA_FOLDERS),
    # Skill indices - format-specific
    "test_player_skill", "odi_player_skill", "t20_player_skill",
    "test_team_skill", "odi_team_skill", "t20_team_skill",
    "test_venue_skill", "odi_venue_skill", "t20_venue_skill"
  )

  for (table in tables) {
    parquet_url <- sprintf("%s/%s.parquet", base_url, table)
    view_sql <- sprintf("CREATE VIEW IF NOT EXISTS %s AS SELECT * FROM '%s'",
                        table, parquet_url)
    tryCatch(
      DBI::dbExecute(con, view_sql),
      error = function(e) NULL
    )
  }

  # Create convenience UNION views for 'matches' and 'deliveries'
  match_tables <- paste0("matches_", DATA_FOLDERS)
  match_union <- paste0("SELECT * FROM ", match_tables, collapse = " UNION ALL ")
  tryCatch(
    DBI::dbExecute(con, sprintf("CREATE VIEW IF NOT EXISTS matches AS %s", match_union)),
    error = function(e) NULL
  )

  delivery_tables <- paste0("deliveries_", DATA_FOLDERS)
  delivery_union <- paste0("SELECT * FROM ", delivery_tables, collapse = " UNION ALL ")
  tryCatch(
    DBI::dbExecute(con, sprintf("CREATE VIEW IF NOT EXISTS deliveries AS %s", delivery_union)),
    error = function(e) NULL
  )

  # Execute query
  DBI::dbGetQuery(con, sql)
}
