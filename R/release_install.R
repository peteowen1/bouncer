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
#' @param type Character. Release type to find: "cricsheet" (parquet data),
#'   "daily" (JSON archives), "weekly" (legacy parquet), or "any" (most recent).
#'
#' @return List with release information including tag_name, published_at,
#'   and assets.
#'
#' @export
#' @examples
#' \dontrun{
#' # Get core release (parquet files from daily scraper)
#' release <- get_latest_release(type = "cricsheet")
#' print(release$tag_name)
#'
#' # Get latest daily release (JSON archives)
#' release <- get_latest_release(type = "daily")
#' }
get_latest_release <- function(repo = "peteowen1/bouncerdata", type = "any") {
  type <- match.arg(type, c("any", "cricsheet", "daily", "weekly"))

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

  pattern <- if (type == "cricsheet") {
    "^cricsheet$"
  } else if (type == "weekly") {
    "-weekly$"
  } else {
    "^v[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}$"  # daily releases
  }

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

    # Use tryCatch to ensure temp file cleanup on error
    tryCatch({
      download_release_asset(asset$browser_download_url, temp_zip)

      # Extract to json_dir
      folder_path <- file.path(json_dir, folder)
      dir.create(folder_path, showWarnings = FALSE, recursive = TRUE)
      zip::unzip(temp_zip, exdir = folder_path)
    }, finally = {
      # Always clean up temp file
      if (file.exists(temp_zip)) unlink(temp_zip)
    })

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


#' Install Parquet Files from Core Release
#'
#' Downloads pre-built parquet files from the core release.
#' These can be queried directly with DuckDB or Arrow without
#' building a local database.
#'
#' @param repo Character. GitHub repository. Default "peteowen1/bouncerdata".
#' @param tables Character vector. Tables to download, or "all".
#'   Options: "matches", "players", "team_elo", or delivery partitions like
#'   "deliveries_T20_male_club", "deliveries_Test_male_international", etc.
#'   See DATA_FOLDERS constant for all available partitions.
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
#' # Download only matches and T20 deliveries
#' install_parquets_from_release(
#'   tables = c("matches", "deliveries_T20_male_club")
#' )
#' }
install_parquets_from_release <- function(repo = "peteowen1/bouncerdata",
                                           tables = "all",
                                           data_dir = NULL,
                                           tag = "latest") {

  cli::cli_h1("Installing Parquet Files from Core Release")

  # Setup data directory
  if (is.null(data_dir)) {
    data_dir <- file.path(find_bouncerdata_dir(create = TRUE), "parquet")
  }
  dir.create(data_dir, showWarnings = FALSE, recursive = TRUE)

  # All available tables (partitioned by match_type × gender × team_type)
  all_tables <- c(
    # Unified tables
    "players", "team_elo",
    # Matches - partitioned
    paste0("matches_", DATA_FOLDERS),
    # Deliveries - partitioned
    paste0("deliveries_", DATA_FOLDERS),
    # Skill indices - format-specific
    "test_player_skill", "odi_player_skill", "t20_player_skill",
    "test_team_skill", "odi_team_skill", "t20_team_skill",
    "test_venue_skill", "odi_venue_skill", "t20_venue_skill"
  )

  if ("all" %in% tables) {
    tables <- all_tables
  }

  # Get core release (parquet data from daily scraper)
  release <- get_latest_release(repo, type = "cricsheet")
  cli::cli_alert_success("Found release: {release$tag_name}")

  # Download each table (use list to avoid O(n²) vector growth)
  downloaded_list <- vector("list", length(tables))
  download_idx <- 0L

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
    download_idx <- download_idx + 1L
    downloaded_list[[download_idx]] <- dest_path
  }

  # Convert list to vector (efficient: single allocation)
  downloaded <- unlist(downloaded_list[seq_len(download_idx)])

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


