# Data Loader Functions
#
# User-facing functions for loading bouncer data from local cache or remote
# GitHub releases. Follows the pannadata pattern with source = "local"/"remote".

# Session-level cache for remote parquet data
.bouncer_remote_cache <- new.env(parent = emptyenv())


#' Get Remote Parquet Cache Directory
#'
#' Downloads parquet files from GitHub releases to a temporary directory
#' and caches the path for the session lifetime. Subsequent calls return
#' the cached path without re-downloading.
#'
#' @param repo Character. GitHub repository. Default "peteowen1/bouncerdata".
#' @param tag Character. Release tag. Default "core" for core data.
#' @param force Logical. Force re-download even if cached.
#'
#' @return Path to directory containing parquet files.
#' @keywords internal
get_remote_parquet_cache <- function(repo = "peteowen1/bouncerdata",
                                      tag = "core",
                                      force = FALSE) {

  cache_key <- paste0("parquet_", gsub("[^a-zA-Z0-9]", "_", tag))

  # Return cached path if available
  if (!force && exists(cache_key, envir = .bouncer_remote_cache)) {
    cached_path <- get(cache_key, envir = .bouncer_remote_cache)
    if (dir.exists(cached_path)) {
      return(cached_path)
    }
  }

  cli::cli_alert_info("Downloading parquet files from GitHub releases ({tag})...")

  # Fetch specific release by tag
  url <- sprintf("https://api.github.com/repos/%s/releases/tags/%s", repo, tag)

  resp <- httr2::request(url) |>
    httr2::req_headers(Accept = "application/vnd.github.v3+json") |>
    httr2::req_user_agent("bouncer R package") |>
    httr2::req_timeout(30) |>
    httr2::req_perform()

  release <- httr2::resp_body_json(resp)
  actual_tag <- release$tag_name

  # Create temp directory for this session
  cache_dir <- file.path(tempdir(), paste0("bouncerdata_remote_", actual_tag))

  if (dir.exists(cache_dir) && !force) {
    # Already downloaded in this session
    assign(cache_key, cache_dir, envir = .bouncer_remote_cache)
    cli::cli_alert_success("Using cached remote data: {cache_dir}")
    return(cache_dir)
  }

  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)

  # Download all parquet files
  for (asset in release$assets) {
    if (grepl("\\.parquet$", asset$name)) {
      dest_path <- file.path(cache_dir, asset$name)
      cli::cli_alert_info("  Downloading {asset$name}...")
      download_release_asset(asset$browser_download_url, dest_path, show_progress = FALSE)
    }
  }

  # Cache the path
  assign(cache_key, cache_dir, envir = .bouncer_remote_cache)

  cli::cli_alert_success("Downloaded {length(list.files(cache_dir))} parquet files")
  cache_dir
}


#' Get Local Parquet Directory
#'
#' Returns the path to local parquet files.
#'
#' @return Path to local parquet directory.
#' @keywords internal
get_local_parquet_dir <- function() {
  data_dir <- find_bouncerdata_dir()
  file.path(data_dir, "parquet")
}


#' Apply Data Filters
#'
#' Internal function to filter data by format, gender, and type.
#'
#' @param data Data frame with match_type, gender, team_type columns.
#' @param format "long_form", "short_form", or "all"
#' @param gender "male", "female", or "all"
#' @param type "international", "club", or "all"
#'
#' @return Filtered data frame.
#' @keywords internal
apply_data_filters <- function(data, format = "all", gender = "all", type = "all") {
  if (format != "all") {
    if (format == "long_form") {
      data <- data[data$match_type %in% FORMAT_LONG_FORM, ]
    } else if (format == "short_form") {
      data <- data[!data$match_type %in% FORMAT_LONG_FORM, ]
    }
  }

  if (gender != "all") {
    data <- data[data$gender == gender, ]
  }

  if (type != "all") {
    data <- data[data$team_type == type, ]
  }

  data
}


#' Load Matches Data
#'
#' Load match data from local cache or remote GitHub releases.
#' Optionally filter by format (long/short), gender (male/female),
#' and type (international/club).
#'
#' @param format Character. "long_form", "short_form", or "all" (default).
#' @param gender Character. "male", "female", or "all" (default).
#' @param type Character. "international", "club", or "all" (default).
#' @param source Character. "local" (default) or "remote".
#'
#' @return Data frame of matches.
#'
#' @export
#' @examples
#' \dontrun{
#' # Load all matches locally
#' matches <- load_matches()
#'
#' # Load only T20 international men's matches
#' t20i_men <- load_matches("short_form", "male", "international")
#'
#' # Load from GitHub releases
#' matches <- load_matches(source = "remote")
#' }
load_matches <- function(format = "all", gender = "all", type = "all",
                         source = c("local", "remote")) {
  source <- match.arg(source)

  base_dir <- if (source == "local") {
    get_local_parquet_dir()
  } else {
    get_remote_parquet_cache()
  }

  path <- file.path(base_dir, "matches.parquet")

  if (!file.exists(path)) {
    cli::cli_warn("Matches data not found at {path}")
    return(data.frame())
  }

  result <- arrow::read_parquet(path)

  # Apply filters if specified
  result <- apply_data_filters(result, format, gender, type)

  if (nrow(result) == 0) {
    cli::cli_warn("No match data found for the specified filters")
    return(data.frame())
  }

  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} matches")
  result
}


#' Load Deliveries Data
#'
#' Load ball-by-ball delivery data from local cache or remote GitHub releases.
#' Optionally filter by format (long/short), gender (male/female),
#' and type (international/club).
#'
#' @inheritParams load_matches
#'
#' @return Data frame of deliveries.
#'
#' @export
#' @examples
#' \dontrun{
#' # Load all deliveries locally
#' deliveries <- load_deliveries()
#'
#' # Load only T20 men's deliveries
#' t20_men <- load_deliveries("short_form", "male")
#'
#' # Load IPL deliveries (short form, male, club)
#' ipl <- load_deliveries("short_form", "male", "club")
#'
#' # Load from GitHub releases
#' deliveries <- load_deliveries(source = "remote")
#' }
load_deliveries <- function(format = "all", gender = "all", type = "all",
                            source = c("local", "remote")) {
  source <- match.arg(source)

  base_dir <- if (source == "local") {
    get_local_parquet_dir()
  } else {
    get_remote_parquet_cache()
  }

  path <- file.path(base_dir, "deliveries.parquet")

  if (!file.exists(path)) {
    cli::cli_warn("Deliveries data not found at {path}")
    return(data.frame())
  }

  result <- arrow::read_parquet(path)

  # Apply filters if specified
  result <- apply_data_filters(result, format, gender, type)

  if (nrow(result) == 0) {
    cli::cli_warn("No delivery data found for the specified filters")
    return(data.frame())
  }

  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} deliveries")
  result
}


#' Load Players Data
#'
#' Load the player registry from local cache or remote GitHub releases.
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

  base_dir <- if (source == "local") {
    get_local_parquet_dir()
  } else {
    get_remote_parquet_cache()
  }

  path <- file.path(base_dir, "players.parquet")

  if (!file.exists(path)) {
    cli::cli_warn("Players data not found at {path}")
    return(data.frame())
  }

  result <- arrow::read_parquet(path)
  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} players")
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

  formats <- if (match_format == "all") c("t20", "odi", "test") else match_format

  base_dir <- if (source == "local") {
    get_local_parquet_dir()
  } else {
    get_remote_parquet_cache()
  }

  dfs <- lapply(formats, function(fmt) {
    path <- file.path(base_dir, paste0(fmt, "_player_skill.parquet"))
    if (file.exists(path)) {
      df <- arrow::read_parquet(path)
      df$format <- fmt
      df
    } else {
      NULL
    }
  })

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

  formats <- if (match_format == "all") c("t20", "odi", "test") else match_format

  base_dir <- if (source == "local") {
    get_local_parquet_dir()
  } else {
    get_remote_parquet_cache()
  }

  dfs <- lapply(formats, function(fmt) {
    path <- file.path(base_dir, paste0(fmt, "_team_skill.parquet"))
    if (file.exists(path)) {
      df <- arrow::read_parquet(path)
      df$format <- fmt
      df
    } else {
      NULL
    }
  })

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

  formats <- if (match_format == "all") c("t20", "odi", "test") else match_format

  base_dir <- if (source == "local") {
    get_local_parquet_dir()
  } else {
    get_remote_parquet_cache()
  }

  dfs <- lapply(formats, function(fmt) {
    path <- file.path(base_dir, paste0(fmt, "_venue_skill.parquet"))
    if (file.exists(path)) {
      df <- arrow::read_parquet(path)
      df$format <- fmt
      df
    } else {
      NULL
    }
  })

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
#' Load team ELO ratings from local cache or remote GitHub releases.
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

  base_dir <- if (source == "local") {
    get_local_parquet_dir()
  } else {
    get_remote_parquet_cache()
  }

  path <- file.path(base_dir, "team_elo.parquet")

  if (!file.exists(path)) {
    cli::cli_warn("Team ELO data not found at {path}")
    return(data.frame())
  }

  result <- arrow::read_parquet(path)
  cli::cli_alert_success("Loaded {format(nrow(result), big.mark=',')} team ELO records")
  result
}


#' Clear Remote Parquet Cache
#'
#' Clears the session-level cache of remote parquet data, forcing
#' the next load to re-download from GitHub.
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
  cli::cli_alert_success("Remote parquet cache cleared")
  invisible(NULL)
}
