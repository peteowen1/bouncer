# Shared Utility Functions for Skill Index Modules
#
# Common helper functions used across player, team, and venue skill index modules.


#' Normalize Cricket Format Name
#'
#' Normalizes various format names to canonical form (t20, odi, test).
#' Handles case insensitivity and common aliases.
#'
#' @param format Character. Format name to normalize
#'
#' @return Character. Canonical format: "t20", "odi", or "test"
#' @keywords internal
normalize_format <- function(format) {
  format_lower <- tolower(format)

  switch(format_lower,
    "t20"  = "t20",
    "it20" = "t20",
    "odi"  = "odi",
    "odm"  = "odi",
    "test" = "test",
    "mdm"  = "test",
    "t20"  # default
  )
}


#' Get Format-Specific Skill Table Name
#'
#' Returns the database table name for a format and entity type.
#'
#' @param format Character. Format name (will be normalized)
#' @param entity_type Character. One of "player_skill", "team_skill", "venue_skill"
#'
#' @return Character. Table name like "t20_player_skill"
#' @keywords internal
get_skill_table_name <- function(format, entity_type) {
  paste0(normalize_format(format), "_", entity_type)
}


#' Check Skill Table Exists
#'
#' Checks if a skill table exists in the database.
#'
#' @param conn DBI connection
#' @param format Character. Format name
#' @param entity_type Character. One of "player_skill", "team_skill", "venue_skill"
#'
#' @return Logical. TRUE if table exists
#' @keywords internal
skill_table_exists <- function(conn, format, entity_type) {
  table_name <- get_skill_table_name(format, entity_type)
  table_name %in% DBI::dbListTables(conn)
}


#' Batch Process Delivery IDs for Skill Join
#'
#' Helper function to handle batch processing of large delivery ID lists.
#' Used internally by join_*_skill_indices functions.
#'
#' @param delivery_ids Character vector. Delivery IDs to process
#' @param query_fn Function. Function that takes a vector of IDs and returns results
#' @param batch_size Integer. Number of IDs per batch. Default 10000.
#' @param verbose Logical. Whether to show progress. Default TRUE.
#'
#' @return Combined data frame from all batches
#' @keywords internal
batch_skill_query <- function(delivery_ids, query_fn, batch_size = 10000, verbose = TRUE) {
  if (length(delivery_ids) <= batch_size) {
    return(query_fn(delivery_ids))
  }

  if (verbose) {
    cli::cli_alert_info("Fetching skill indices in batches...")
  }

  n_batches <- ceiling(length(delivery_ids) / batch_size)
  all_results <- vector("list", n_batches)

  for (i in seq_len(n_batches)) {
    start_idx <- (i - 1) * batch_size + 1
    end_idx <- min(i * batch_size, length(delivery_ids))
    batch_ids <- delivery_ids[start_idx:end_idx]

    all_results[[i]] <- query_fn(batch_ids)

    if (verbose && i %% 10 == 0) {
      cli::cli_alert_info("Fetched {i}/{n_batches} batches...")
    }
  }

  do.call(rbind, all_results)
}


#' Escape SQL String Values
#'
#' Escapes single quotes in strings for safe SQL injection.
#'
#' @param x Character vector. Strings to escape
#'
#' @return Character vector. Escaped strings
#' @keywords internal
escape_sql_strings <- function(x) {
  gsub("'", "''", x)
}
