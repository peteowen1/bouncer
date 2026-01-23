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
#' @keywords internal
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
#' @keywords internal
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
#' @keywords internal
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


# Unused list_available_datasets() function removed - check Cricsheet.org directly


#' Compare ZIP Contents to Local Files
#'
#' Compares files in a ZIP archive to local files by size.
#' Used to detect new and changed files for incremental updates.
#'
#' @param zip_file Path to the ZIP file
#' @param local_dir Directory containing local JSON files
#'
#' @return List with:
#'   - new_files: Character vector of filenames in ZIP but not local
#'   - changed_files: Character vector of filenames that differ in size
#'   - unchanged_files: Character vector of filenames with same size
#'   - zip_info: Data frame from zip::zip_list() with file metadata
#'
#' @keywords internal
compare_zip_to_local <- function(zip_file, local_dir) {
  # Get ZIP contents
  zip_info <- zip::zip_list(zip_file)
  zip_json <- zip_info[grepl("\\.json$", zip_info$filename), ]

  if (nrow(zip_json) == 0) {
    return(list(
      new_files = character(0),
      changed_files = character(0),
      unchanged_files = character(0),
      zip_info = zip_info
    ))
  }

  # Get local file info
  local_files <- list.files(local_dir, pattern = "\\.json$", full.names = TRUE)
  local_df <- data.frame(
    filename = basename(local_files),
    local_size = file.info(local_files)$size,
    stringsAsFactors = FALSE
  )

  # Compare by filename (basename only)
  zip_df <- data.frame(
    filename = basename(zip_json$filename),
    zip_path = zip_json$filename,
    zip_size = zip_json$uncompressed_size,
    stringsAsFactors = FALSE
  )

  # Merge to compare
  merged <- merge(zip_df, local_df, by = "filename", all.x = TRUE)

  # Categorize
  new_files <- merged$filename[is.na(merged$local_size)]
  changed_files <- merged$filename[!is.na(merged$local_size) &
                                     merged$zip_size != merged$local_size]
  unchanged_files <- merged$filename[!is.na(merged$local_size) &
                                       merged$zip_size == merged$local_size]

  list(
    new_files = new_files,
    changed_files = changed_files,
    unchanged_files = unchanged_files,
    zip_info = zip_json
  )
}


#' Download All Cricsheet JSON Data (Incremental with Size Comparison)
#'
#' Downloads the complete all_json.zip from Cricsheet containing ALL matches
#' (all formats, all leagues, all genders) with smart incremental updates.
#'
#' This function:
#' - Always downloads the ZIP (fast, ~30 seconds)
#' - Compares file sizes to detect NEW and CHANGED files
#' - Extracts only files that are new or have changed size
#' - Returns information about what changed for database cleanup
#'
#' Test matches update daily as play progresses - same filename, new content.
#' Size comparison detects these updates.
#'
#' @param output_path Directory to extract files. If NULL, uses bouncerdata directory.
#' @param keep_zip Logical. Keep the ZIP file after extraction. Default FALSE (saves ~93MB).
#' @param fresh Logical. If TRUE, extracts all files (ignores existing). If FALSE (default),
#'   only extracts new or changed files based on size comparison.
#'
#' @return List with:
#'   - all_files: Character vector of all JSON file paths
#'   - new_files: Character vector of newly extracted filenames
#'   - changed_files: Character vector of updated filenames (size changed)
#'   - unchanged_count: Count of files that didn't change
#'
#' @keywords internal
download_all_cricsheet_data <- function(output_path = NULL, keep_zip = FALSE, fresh = FALSE) {
  url <- "https://cricsheet.org/downloads/all_json.zip"

  # Setup paths
  if (is.null(output_path)) {
    output_path <- find_bouncerdata_dir(create = TRUE)
  }

  if (!dir.exists(output_path)) {
    dir.create(output_path, recursive = TRUE)
    cli::cli_alert_success("Created directory: {.file {output_path}}")
  }

  extract_dir <- file.path(output_path, "json_files")
  zip_file <- file.path(output_path, "all_json.zip")

  cli::cli_h2("Cricsheet Data Sync")

  # Always download the ZIP (fast ~30s, simpler than HTTP HEAD check)
  cli::cli_alert_info("Downloading from Cricsheet (~93MB)...")

  # Remove old ZIP if exists
  if (file.exists(zip_file)) {
    file.remove(zip_file)
  }

  tryCatch({
    httr2::request(url) |>
      httr2::req_user_agent("bouncer R package") |>
      httr2::req_progress() |>
      httr2::req_perform(path = zip_file)

    cli::cli_alert_success("Downloaded ZIP file")
  }, error = function(e) {
    cli::cli_abort("Failed to download from Cricsheet: {e$message}")
  })

  # Create extract directory if needed
  if (!dir.exists(extract_dir)) {
    dir.create(extract_dir, recursive = TRUE)
  }

  if (fresh) {
    # Fresh mode: skip comparison, extract everything
    cli::cli_alert_info("Fresh install - extracting all files...")

    # Clear existing JSON files
    old_files <- list.files(extract_dir, pattern = "\\.json$", full.names = TRUE)
    if (length(old_files) > 0) {
      file.remove(old_files)
      cli::cli_alert_info("Removed {length(old_files)} old files")
    }

    # Extract all
    zip::unzip(zip_file, exdir = extract_dir, junkpaths = TRUE)

    zip_info <- zip::zip_list(zip_file)
    n_extracted <- sum(grepl("\\.json$", zip_info$filename))
    cli::cli_alert_success("Extracted {n_extracted} JSON files")

    # For fresh mode, all files are "new"
    new_files <- basename(zip_info$filename[grepl("\\.json$", zip_info$filename)])
    changed_files <- character(0)
    n_unchanged <- 0L

  } else {
    # Incremental mode: compare and extract only new/changed
    cli::cli_alert_info("Comparing with local files...")
    comparison <- compare_zip_to_local(zip_file, extract_dir)

    n_new <- length(comparison$new_files)
    n_changed <- length(comparison$changed_files)
    n_unchanged <- length(comparison$unchanged_files)
    n_total_zip <- nrow(comparison$zip_info)

    cli::cli_alert_info("ZIP contains {n_total_zip} matches")
    cli::cli_alert_info("Local has {n_unchanged + n_changed} matches")
    cli::cli_alert_success("NEW: {n_new} | CHANGED: {n_changed} | UNCHANGED: {n_unchanged}")

    # Determine files to extract (new + changed)
    files_to_extract_names <- c(comparison$new_files, comparison$changed_files)

    if (length(files_to_extract_names) > 0) {
      # Get full paths from ZIP info
      files_to_extract <- comparison$zip_info$filename[
        basename(comparison$zip_info$filename) %in% files_to_extract_names
      ]

      cli::cli_alert_info("Extracting {length(files_to_extract)} files...")
      zip::unzip(zip_file, files = files_to_extract, exdir = extract_dir,
                 junkpaths = TRUE)

      cli::cli_alert_success("Extracted {n_new} new + {n_changed} changed files")
    } else {
      cli::cli_alert_success("No new or changed files to extract")
    }

    new_files <- comparison$new_files
    changed_files <- comparison$changed_files
  }

  # Clean up ZIP (don't keep by default - saves 93MB)
  if (!keep_zip && file.exists(zip_file)) {
    file.remove(zip_file)
    cli::cli_alert_info("Removed ZIP file (use keep_zip=TRUE to keep)")
  }

  # Get all JSON files
  all_files <- list.files(extract_dir, pattern = "\\.json$",
                          full.names = TRUE, recursive = TRUE)

  cli::cli_alert_success("Total: {length(all_files)} JSON files available")

  # Return detailed info for database updates
  list(
    all_files = all_files,
    new_files = new_files,
    changed_files = changed_files,
    unchanged_count = n_unchanged
  )
}
