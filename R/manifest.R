# Manifest Functions for Cricsheet Release
#
# Functions for generating and fetching manifest.json from the cricsheet release.
# The manifest enables quick match_id lookup without downloading full parquet files.

# Session-level cache for remote manifest
.bouncer_manifest_cache <- new.env(parent = emptyenv())

#' Get Remote Manifest
#'
#' Downloads and parses the manifest.json from the cricsheet GitHub release.
#' Results are cached for the session to avoid repeated downloads.
#'
#' @param repo Character. GitHub repository. Default "peteowen1/bouncerdata".
#' @param tag Character. Release tag. Default "cricsheet".
#' @param force Logical. Force re-download even if cached. Default FALSE.
#'
#' @return List containing:
#'   - updated_at: Timestamp when manifest was generated
#'   - match_count: Total number of matches
#'   - match_ids: Character vector of all match_ids
#'   - partitions: Named list of partition metadata
#'
#' @export
#' @examples
#' \dontrun{
#' manifest <- get_remote_manifest()
#' length(manifest$match_ids)  # Number of matches in release
#' names(manifest$partitions)  # Available partitions
#' }
get_remote_manifest <- function(repo = "peteowen1/bouncerdata",
                                  tag = "cricsheet",
                                  force = FALSE) {

  cache_key <- paste0("manifest_", gsub("[^a-zA-Z0-9]", "_", tag))

  # Return cached manifest if available
  if (!force && exists(cache_key, envir = .bouncer_manifest_cache)) {
    cached <- get(cache_key, envir = .bouncer_manifest_cache)
    if (!is.null(cached)) {
      cli::cli_alert_info("Using cached manifest ({cached$match_count} matches)")
      return(cached)
    }
  }

  cli::cli_alert_info("Downloading manifest from GitHub releases ({tag})...")

  # Build direct URL to manifest.json
  url <- sprintf(
    "https://github.com/%s/releases/download/%s/manifest.json",
    repo, tag
  )

  # Download and parse
  manifest <- tryCatch({
    resp <- httr2::request(url) |>
      httr2::req_timeout(30) |>
      httr2::req_perform()

    jsonlite::fromJSON(httr2::resp_body_string(resp), simplifyVector = TRUE)
  }, error = function(e) {
    cli::cli_abort("Failed to download manifest: {e$message}")
  })

  # Cache the manifest
  assign(cache_key, manifest, envir = .bouncer_manifest_cache)

  cli::cli_alert_success("Downloaded manifest ({manifest$match_count} matches, updated {manifest$updated_at})")
  manifest
}


#' Get Remote Match IDs
#'
#' Quick lookup of all match_ids in the cricsheet release without downloading
#' the full parquet files. Useful for determining which matches need to be
#' scraped or updated.
#'
#' @inheritParams get_remote_manifest
#'
#' @return Character vector of match_ids
#'
#' @export
#' @examples
#' \dontrun{
#' ids <- get_remote_match_ids()
#' length(ids)  # Total matches
#'
#' # Check if a match exists
#' "1234567" %in% ids
#' }
get_remote_match_ids <- function(repo = "peteowen1/bouncerdata",
                                   tag = "cricsheet",
                                   force = FALSE) {

  manifest <- get_remote_manifest(repo = repo, tag = tag, force = force)
  manifest$match_ids
}


#' Get Remote Partition Info
#'
#' Get metadata about available partitions in the cricsheet release.
#' Useful for deciding which partition files to download.
#'
#' @inheritParams get_remote_manifest
#'
#' @return Data frame with columns:
#'   - partition: Partition key (e.g., "T20_male")
#'   - match_count: Number of matches in partition
#'   - file_size_bytes: Size of parquet file
#'   - file_size_mb: Size in MB
#'
#' @export
#' @examples
#' \dontrun{
#' partitions <- get_remote_partition_info()
#' partitions[order(-partitions$file_size_mb), ]  # Largest first
#' }
get_remote_partition_info <- function(repo = "peteowen1/bouncerdata",
                                       tag = "cricsheet",
                                       force = FALSE) {

  manifest <- get_remote_manifest(repo = repo, tag = tag, force = force)

  if (is.null(manifest$partitions) || length(manifest$partitions) == 0) {
    cli::cli_warn("No partition information in manifest")
    return(data.frame(
      partition = character(),
      match_count = integer(),
      file_size_bytes = integer(),
      file_size_mb = numeric(),
      stringsAsFactors = FALSE
    ))
  }

  # Convert to data frame
  partition_names <- names(manifest$partitions)
  df <- data.frame(
    partition = partition_names,
    match_count = vapply(manifest$partitions, function(p) p$match_count, integer(1)),
    file_size_bytes = vapply(manifest$partitions, function(p) p$file_size_bytes, numeric(1)),
    stringsAsFactors = FALSE
  )

  df$file_size_mb <- round(df$file_size_bytes / (1024^2), 2)

  df
}


#' Generate Manifest from Local Data
#'
#' Creates a manifest.json from local parquet files or database.
#' Used by the upload script to generate the manifest for release.
#'
#' @param parquet_dir Path to directory containing parquet files.
#' @param conn Optional DuckDB connection. If provided, uses database directly.
#' @param output_path Path to write manifest.json. If NULL, returns list only.
#'
#' @return List with manifest structure (invisibly if output_path is provided)
#'
#' @keywords internal
generate_manifest <- function(parquet_dir = NULL, conn = NULL, output_path = NULL) {

  match_ids <- NULL
  partitions <- list()

  if (!is.null(conn)) {
    # Generate from database
    match_ids <- DBI::dbGetQuery(conn, "SELECT match_id FROM matches ORDER BY match_id")$match_id

    # Get partition stats
    partition_stats <- DBI::dbGetQuery(conn, "
      SELECT match_type, gender, COUNT(DISTINCT match_id) as match_count
      FROM matches
      GROUP BY match_type, gender
    ")

    for (i in seq_len(nrow(partition_stats))) {
      key <- paste(partition_stats$match_type[i], partition_stats$gender[i], sep = "_")
      partitions[[key]] <- list(
        match_count = partition_stats$match_count[i],
        file_size_bytes = 0  # Will be updated after export
      )
    }

  } else if (!is.null(parquet_dir)) {
    # Generate from parquet files
    matches_path <- file.path(parquet_dir, "matches.parquet")
    if (!file.exists(matches_path)) {
      cli::cli_abort("matches.parquet not found in {parquet_dir}")
    }

    matches <- arrow::read_parquet(matches_path, col_select = c("match_id", "match_type", "gender"))
    match_ids <- sort(unique(matches$match_id))

    # Get partition stats from matches
    for (mt in unique(matches$match_type)) {
      for (gender in unique(matches$gender[matches$match_type == mt])) {
        key <- paste(mt, gender, sep = "_")
        count <- sum(matches$match_type == mt & matches$gender == gender)

        filepath <- file.path(parquet_dir, paste0("deliveries_", key, ".parquet"))
        file_size <- if (file.exists(filepath)) file.size(filepath) else 0

        partitions[[key]] <- list(
          match_count = count,
          file_size_bytes = file_size
        )
      }
    }

  } else {
    cli::cli_abort("Either parquet_dir or conn must be provided")
  }

  manifest <- list(
    updated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    match_count = length(match_ids),
    match_ids = match_ids,
    partitions = partitions
  )

  if (!is.null(output_path)) {
    jsonlite::write_json(manifest, output_path, auto_unbox = TRUE, pretty = TRUE)
    cli::cli_alert_success("Generated manifest at {.file {output_path}}")
    return(invisible(manifest))
  }

  manifest
}


#' Update Manifest with New Match IDs
#'
#' Incrementally updates a manifest by adding new match_ids.
#' Used by the daily scraper to update the manifest without regenerating.
#'
#' @param manifest Existing manifest list
#' @param new_match_ids Character vector of new match_ids to add
#' @param partition_updates Named list of partition updates
#'   (e.g., list(T20_male = list(match_count = 10, file_size_bytes = 1000)))
#'
#' @return Updated manifest list
#'
#' @keywords internal
update_manifest <- function(manifest, new_match_ids, partition_updates = NULL) {

  if (length(new_match_ids) == 0) {
    cli::cli_alert_info("No new match_ids to add")
    return(manifest)
  }

  # Add new match_ids (avoid duplicates)
  existing_ids <- manifest$match_ids
  truly_new <- setdiff(new_match_ids, existing_ids)

  if (length(truly_new) == 0) {
    cli::cli_alert_info("All match_ids already in manifest")
    return(manifest)
  }

  manifest$match_ids <- sort(c(existing_ids, truly_new))
  manifest$match_count <- length(manifest$match_ids)
  manifest$updated_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  # Update partition info if provided
  if (!is.null(partition_updates)) {
    for (key in names(partition_updates)) {
      if (is.null(manifest$partitions[[key]])) {
        manifest$partitions[[key]] <- partition_updates[[key]]
      } else {
        manifest$partitions[[key]]$match_count <- partition_updates[[key]]$match_count
        manifest$partitions[[key]]$file_size_bytes <- partition_updates[[key]]$file_size_bytes
      }
    }
  }

  cli::cli_alert_success("Updated manifest: added {length(truly_new)} new match_ids (total: {manifest$match_count})")
  manifest
}


#' Clear Manifest Cache
#'
#' Clears the session-level cache of remote manifests, forcing
#' the next call to re-download from GitHub.
#'
#' @return Invisible NULL.
#'
#' @export
#' @examples
#' \dontrun{
#' clear_manifest_cache()
#' }
clear_manifest_cache <- function() {
  rm(list = ls(envir = .bouncer_manifest_cache), envir = .bouncer_manifest_cache)
  cli::cli_alert_success("Manifest cache cleared")
  invisible(NULL)
}
