# Cricsheet Data Download Functions

#' Download Cricsheet Data
#'
#' Downloads cricket match data from Cricsheet in JSON format.
#'
#' @param match_type Character string specifying the match type. Options:
#'   "test", "odi", "t20", "t20i", "ipl", "bbl", "cpl", "psl", "wbbl", etc.
#' @param gender Character string specifying gender. Options: "male", "female".
#'   Default is "male".
#' @param data_format Character string specifying data format. Options: "json"
#'   (recommended), "csv". Default is "json".
#' @param output_path Character string specifying where to save downloaded files.
#'   If NULL, uses a temporary directory.
#' @param extract Logical. If TRUE, extracts ZIP files after download.
#'   Default is TRUE.
#'
#' @return Character vector of paths to downloaded (and optionally extracted) files
#' @export
#'
#' @examples
#' \dontrun{
#' # Download ODI data
#' files <- download_cricsheet_data("odi")
#'
#' # Download women's T20I data
#' files <- download_cricsheet_data("t20i", gender = "female")
#'
#' # Download to specific location
#' files <- download_cricsheet_data("ipl", output_path = "~/cricket_data/ipl")
#' }
download_cricsheet_data <- function(match_type,
                                     gender = "male",
                                     data_format = "json",
                                     output_path = NULL,
                                     extract = TRUE) {

  # Validate inputs
  match_type <- tolower(match_type)
  gender <- tolower(gender)
  data_format <- tolower(data_format)

  if (!gender %in% c("male", "female")) {
    cli::cli_abort("gender must be 'male' or 'female'")
  }

  if (!data_format %in% c("json", "csv")) {
    cli::cli_abort("data_format must be 'json' or 'csv'")
  }

  # Setup output directory
  if (is.null(output_path)) {
    output_path <- tempdir()
  }

  if (!dir.exists(output_path)) {
    dir.create(output_path, recursive = TRUE)
    cli::cli_alert_success("Created directory: {.file {output_path}}")
  }

  # Get download URL
  url <- get_cricsheet_url(match_type, gender, data_format)

  cli::cli_h2("Downloading {match_type} data from Cricsheet")
  cli::cli_alert_info("Format: {data_format}")
  cli::cli_alert_info("Gender: {gender}")
  cli::cli_alert_info("URL: {.url {url}}")

  # Construct file path
  filename <- basename(url)
  dest_file <- file.path(output_path, filename)

  # Download file
  cli::cli_alert_info("Downloading...")

  tryCatch({
    httr2::request(url) |>
      httr2::req_user_agent("bouncerdata R package (https://github.com/yourusername/bouncerverse)") |>
      httr2::req_progress() |>
      httr2::req_perform(path = dest_file)

    cli::cli_alert_success("Downloaded: {.file {dest_file}}")
  }, error = function(e) {
    cli::cli_abort("Failed to download from Cricsheet: {e$message}")
  })

  # Extract if requested
  if (extract && tools::file_ext(dest_file) == "zip") {
    cli::cli_alert_info("Extracting ZIP file...")

    extract_dir <- file.path(output_path, tools::file_path_sans_ext(filename))

    if (!dir.exists(extract_dir)) {
      dir.create(extract_dir, recursive = TRUE)
    }

    zip::unzip(dest_file, exdir = extract_dir)

    # Get list of extracted files
    extracted_files <- list.files(extract_dir, full.names = TRUE, recursive = TRUE)

    cli::cli_alert_success("Extracted {length(extracted_files)} files to {.file {extract_dir}}")

    return(invisible(extracted_files))
  }

  return(invisible(dest_file))
}


#' Get Cricsheet Download URL
#'
#' Constructs the download URL for Cricsheet data.
#'
#' @param match_type Character string specifying the match type
#' @param gender Character string specifying gender ("male" or "female")
#' @param data_format Character string specifying format ("json" or "csv")
#'
#' @return Character string with download URL
#' @keywords internal
get_cricsheet_url <- function(match_type, gender = "male", data_format = "json") {
  base_url <- "https://cricsheet.org/downloads"

  # Handle match type naming
  match_type <- tolower(match_type)

  # Map common match types to Cricsheet filenames
  # Cricsheet uses specific naming conventions
  match_type_map <- c(
    "test" = "tests",
    "odi" = "odis",
    "t20" = "t20s",
    "t20i" = "t20s",  # International T20s
    "ipl" = "ipl",
    "bbl" = "bbl",
    "cpl" = "cpl",
    "psl" = "psl",
    "wbbl" = "wbbl",
    "blast" = "blast",
    "hundred" = "hundred",
    "ilt20" = "ilt20",
    "msl" = "msl",
    "sa20" = "sa20",
    "vitality_blast" = "blast"
  )

  cricsheet_name <- match_type_map[match_type]

  if (is.na(cricsheet_name)) {
    cli::cli_alert_warning("Unknown match type: {match_type}. Using as-is.")
    cricsheet_name <- match_type
  }

  # Construct filename based on gender and format
  # Cricsheet naming: {match_type}_{gender}_{format}.zip
  # e.g., odis_male_json.zip, t20s_female_json.zip

  if (data_format == "json") {
    if (gender == "female") {
      filename <- paste0(cricsheet_name, "_female_json.zip")
    } else {
      filename <- paste0(cricsheet_name, "_json.zip")
    }
  } else if (data_format == "csv") {
    if (gender == "female") {
      filename <- paste0(cricsheet_name, "_female_csv2.zip")
    } else {
      filename <- paste0(cricsheet_name, "_csv2.zip")
    }
  }

  url <- paste0(base_url, "/", filename)

  return(url)
}


#' Download Format Data
#'
#' Downloads data for a specific cricket format (Test, ODI, T20I).
#'
#' @param format Character string: "test", "odi", or "t20i"
#' @param gender Character string: "male" or "female". Default "male".
#' @param output_path Output directory path. If NULL, uses temp directory.
#'
#' @return Character vector of extracted file paths
#' @export
#'
#' @examples
#' \dontrun{
#' # Download all ODI matches
#' odi_files <- download_format_data("odi")
#'
#' # Download women's Test matches
#' test_files <- download_format_data("test", gender = "female")
#' }
download_format_data <- function(format, gender = "male", output_path = NULL) {
  format <- tolower(format)

  if (!format %in% c("test", "odi", "t20i", "t20")) {
    cli::cli_abort("format must be one of: test, odi, t20i")
  }

  download_cricsheet_data(
    match_type = format,
    gender = gender,
    data_format = "json",
    output_path = output_path,
    extract = TRUE
  )
}


#' Download League Data
#'
#' Downloads data for a specific cricket league (IPL, BBL, etc.).
#'
#' @param league Character string specifying the league: "ipl", "bbl", "cpl",
#'   "psl", "wbbl", "hundred", etc.
#' @param output_path Output directory path. If NULL, uses temp directory.
#'
#' @return Character vector of extracted file paths
#' @export
#'
#' @examples
#' \dontrun{
#' # Download IPL data
#' ipl_files <- download_league_data("ipl")
#'
#' # Download Big Bash League data
#' bbl_files <- download_league_data("bbl")
#' }
download_league_data <- function(league, output_path = NULL) {
  league <- tolower(league)

  valid_leagues <- c("ipl", "bbl", "cpl", "psl", "wbbl", "hundred",
                     "blast", "ilt20", "msl", "sa20")

  if (!league %in% valid_leagues) {
    cli::cli_alert_warning("Unknown league: {league}. Attempting download anyway.")
  }

  # Leagues are typically male, except WBBL
  gender <- if (league == "wbbl") "female" else "male"

  download_cricsheet_data(
    match_type = league,
    gender = gender,
    data_format = "json",
    output_path = output_path,
    extract = TRUE
  )
}


#' List Available Datasets
#'
#' Lists available cricket datasets on Cricsheet.
#'
#' @return A data frame with available datasets
#' @export
#'
#' @examples
#' \dontrun{
#' # See what's available
#' datasets <- list_available_datasets()
#' print(datasets)
#' }
list_available_datasets <- function() {
  # Create a data frame of available datasets
  datasets <- data.frame(
    category = c(
      rep("International", 6),
      rep("Domestic Leagues", 10),
      rep("Domestic Leagues", 10)
    ),
    match_type = c(
      # International
      "test", "odi", "t20i",
      "test", "odi", "t20i",
      # Male leagues
      "ipl", "bbl", "cpl", "psl", "hundred", "blast",
      "ilt20", "msl", "sa20", "vitality_blast",
      # Female leagues
      "wbbl", "hundred_women", "blast_women", "wt20_challenge",
      "wcl", "fairbreak", "womens_cpl", "wpl", "rachael_heyhoe_flint", "charlotte_edwards"
    ),
    gender = c(
      # International
      "male", "male", "male",
      "female", "female", "female",
      # Male leagues
      rep("male", 10),
      # Female leagues
      rep("female", 10)
    ),
    description = c(
      # International
      "Test matches (men)", "One Day Internationals (men)", "T20 Internationals (men)",
      "Test matches (women)", "One Day Internationals (women)", "T20 Internationals (women)",
      # Male leagues
      "Indian Premier League", "Big Bash League", "Caribbean Premier League",
      "Pakistan Super League", "The Hundred", "T20 Blast",
      "International League T20", "Mzansi Super League", "SA20",
      "Vitality Blast",
      # Female leagues
      "Women's Big Bash League", "The Hundred (Women)", "Women's T20 Blast",
      "Women's T20 Challenge", "Women's Cricket League", "FairBreak Invitational",
      "Women's Caribbean Premier League", "Women's Premier League",
      "Rachael Heyhoe Flint Trophy", "Charlotte Edwards Cup"
    ),
    stringsAsFactors = FALSE
  )

  cli::cli_h2("Available Cricsheet Datasets")
  print(datasets)

  invisible(datasets)
}
