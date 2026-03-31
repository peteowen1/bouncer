# Player Game Data Storage
#
# DB storage and retrieval for per-player per-match value metrics.
# Tables: {format}_player_game_data (one per format: t20, odi, test)


#' Store Player Game Data to DuckDB
#'
#' Upserts player game data into the format-specific table.
#'
#' @param conn DBI connection (must be writable).
#' @param data data.table from \code{\link{create_player_game_data}}.
#' @param format Character. Match format: "t20", "odi", or "test".
#'
#' @return Number of rows inserted/updated (invisibly).
#' @keywords internal
store_player_game_data <- function(conn, data, format = c("t20", "odi", "test")) {
  format <- match.arg(format)
  table_name <- paste0(format, "_player_game_data")

  if (nrow(data) == 0) {
    cli::cli_alert_warning("No data to store for {toupper(format)}")
    return(invisible(0L))
  }

  # Get target table column order to ensure correct mapping
  target_cols <- DBI::dbGetQuery(conn, sprintf(
    "SELECT column_name FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position",
    table_name
  ))$column_name

  # Reorder data to match DB schema
  available_cols <- intersect(target_cols, names(data))
  data_ordered <- data[, available_cols, with = FALSE]

  # Register data.table as a DuckDB view for INSERT
  duckdb::duckdb_register(conn, "pgd_staging", data_ordered)
  on.exit(duckdb::duckdb_unregister(conn, "pgd_staging"), add = TRUE)

  # Truncate and replace (full rebuild per format)
  DBI::dbExecute(conn, sprintf("DELETE FROM %s", table_name))

  # Insert with explicit column names for safety
  col_list <- paste(available_cols, collapse = ", ")
  n <- DBI::dbExecute(conn, sprintf(
    "INSERT INTO %s (%s) SELECT %s FROM pgd_staging", table_name, col_list, col_list
  ))

  cli::cli_alert_success("Stored {n} rows in {table_name}")
  invisible(n)
}


#' Load Player Game Data
#'
#' Retrieves player game data from DuckDB or GitHub release.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param match_ids Character vector. Filter to specific matches (NULL = all).
#' @param player_ids Character vector. Filter to specific players (NULL = all).
#' @param source Character. "local" for DuckDB, "remote" for GitHub release.
#' @param db_path Character. Custom DB path (only for source = "local").
#'
#' @return data.table with player game data.
#' @export
load_player_game_data <- function(format = c("t20", "odi", "test"),
                                  match_ids = NULL,
                                  player_ids = NULL,
                                  source = c("local", "remote"),
                                  path = NULL) {
  format <- match.arg(format)
  source <- match.arg(source)

  if (source == "remote") {
    return(.load_player_game_data_remote(format, match_ids, player_ids))
  }

  conn <- get_db_connection(path = path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  table_name <- paste0(format, "_player_game_data")

  # Check table exists
  tables <- DBI::dbListTables(conn)
  if (!table_name %in% tables) {
    cli::cli_abort("Table {table_name} not found. Run the player game data pipeline first.")
  }

  where_clauses <- character(0)
  if (!is.null(match_ids)) {
    ids_sql <- paste(sprintf("'%s'", match_ids), collapse = ", ")
    where_clauses <- c(where_clauses, sprintf("match_id IN (%s)", ids_sql))
  }
  if (!is.null(player_ids)) {
    ids_sql <- paste(sprintf("'%s'", player_ids), collapse = ", ")
    where_clauses <- c(where_clauses, sprintf("player_id IN (%s)", ids_sql))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste("WHERE", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  query <- sprintf("SELECT * FROM %s %s ORDER BY match_date, match_id", table_name, where_sql)
  result <- DBI::dbGetQuery(conn, query)
  data.table::as.data.table(result)
}


#' Download and Read a Parquet File from a GitHub Release
#'
#' Downloads a parquet file from a piggyback release and reads it as a data.table.
#' Caches the download in a temporary directory for the R session.
#'
#' @param tag Character. Release tag (e.g., "ratings", "cricsheet").
#' @param file_name Character. Asset file name (e.g., "t20_stat_ratings.parquet").
#' @param repo Character. GitHub repo (default: "peteowen1/bouncerdata").
#'
#' @return data.table with the parquet contents.
#' @keywords internal
load_release_parquet <- function(tag, file_name, repo = "peteowen1/bouncerdata") {
  if (!requireNamespace("piggyback", quietly = TRUE)) {
    cli::cli_abort("Package {.pkg piggyback} required for remote loading")
  }
  if (!requireNamespace("arrow", quietly = TRUE)) {
    cli::cli_abort("Package {.pkg arrow} required for parquet reading")
  }

  cache_dir <- file.path(tempdir(), "bouncer_release_cache", tag)
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  local_path <- file.path(cache_dir, file_name)

  if (!file.exists(local_path)) {
    cli::cli_alert_info("Downloading {file_name} from {repo}@{tag}...")
    piggyback::pb_download(file_name, dest = cache_dir, repo = repo, tag = tag,
                            .token = Sys.getenv("GITHUB_PAT"))
  }

  if (!file.exists(local_path)) {
    cli::cli_abort("Failed to download {file_name} from {repo}@{tag}")
  }

  data.table::as.data.table(arrow::read_parquet(local_path))
}


#' Load Player Game Data from GitHub Release
#' @param format Character. Match format.
#' @param match_ids Character vector or NULL.
#' @param player_ids Character vector or NULL.
#' @return data.table
#' @keywords internal
.load_player_game_data_remote <- function(format, match_ids = NULL,
                                           player_ids = NULL) {
  file_name <- sprintf("%s_player_game_data.parquet", format)
  dt <- load_release_parquet("ratings", file_name)

  if (!is.null(match_ids)) {
    dt <- dt[match_id %in% match_ids]
  }
  if (!is.null(player_ids)) {
    dt <- dt[player_id %in% player_ids]
  }

  dt
}
