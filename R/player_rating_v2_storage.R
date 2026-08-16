# Persisting the v2 ratings.
#
# Until now `calculate_player_rating_v2()` and `calculate_player_value_v2()`
# returned a data.table and nothing wrote it, so every consumer had to hold an
# 18GB database and half an hour of compute to see a leaderboard. These write
# both to `main`, keyed by bucket, so the blog and the predictions pipeline can
# read them from a release parquet instead.
#
# Replacement is per (format, gender, role), never a whole-table wipe: a failed
# run of one bucket must not delete the other three.

.rating_v2_cols <- c(
  "format", "gender", "role", "rank", "player_id", "player_name", "rating",
  "matches", "balls", "effective_matches", "last_match", "as_at")

.value_v2_cols <- c(
  "format", "gender", "rank", "player_id", "player_name", "total_value",
  "bat_value", "bowl_value", "matches", "bat_balls", "bowl_balls",
  "calibrated", "as_at")

.recreate_if_stale <- function(conn, table_name, wanted) {
  existing <- DBI::dbGetQuery(conn, sprintf("
    SELECT column_name FROM information_schema.columns
    WHERE table_schema = 'main' AND table_name = '%s'", table_name))$column_name
  if (length(existing) > 0 && !setequal(existing, wanted)) {
    cli::cli_alert_warning(
      "{.field main.{table_name}} has an outdated shape ({length(existing)} column{?s}); recreating it.")
    DBI::dbExecute(conn, sprintf("DROP TABLE main.%s", table_name))
  }
}

#' Store Player Ratings
#'
#' @param conn DBI connection with write access.
#' @param data Output of [calculate_player_rating_v2()].
#' @param format,gender,role Bucket the rows belong to. Existing rows for that
#'   exact bucket are replaced; other buckets are untouched.
#' @param as_at Date the rating was computed as of.
#' @param table_name Target table in `main`.
#' @return Number of rows written, invisibly.
#' @export
store_player_rating_v2 <- function(conn, data, format, gender, role,
                                   as_at = NULL,
                                   table_name = "player_rating_v2") {
  d <- data.table::as.data.table(data)
  if (!nrow(d)) cli::cli_abort("No rows to store for {format}/{gender}/{role}.")
  d[, `:=`(format = toupper(format), gender = gender, role = role,
           as_at = if (is.null(as_at)) max(d$last_match) else as.Date(as_at))]

  .recreate_if_stale(conn, table_name, .rating_v2_cols)
  DBI::dbExecute(conn, sprintf("
    CREATE TABLE IF NOT EXISTS main.%s (
      format            VARCHAR,
      gender            VARCHAR,
      role              VARCHAR,
      rank              INTEGER,
      player_id         VARCHAR,
      player_name       VARCHAR,
      rating            DOUBLE,
      matches           INTEGER,
      balls             INTEGER,
      effective_matches DOUBLE,
      last_match        DATE,
      as_at             DATE
    )", table_name))

  DBI::dbExecute(conn, sprintf(
    "DELETE FROM main.%s WHERE format = '%s' AND gender = '%s' AND role = '%s'",
    table_name, toupper(format), gender, role))
  duckdb::duckdb_register(conn, "rating_v2_staging", d[, .SD, .SDcols = .rating_v2_cols])
  on.exit(duckdb::duckdb_unregister(conn, "rating_v2_staging"), add = TRUE)
  cols <- paste(.rating_v2_cols, collapse = ", ")
  n <- DBI::dbExecute(conn, sprintf(
    "INSERT INTO main.%s (%s) SELECT %s FROM rating_v2_staging", table_name, cols, cols))
  cli::cli_alert_success(
    "Stored {n} {gender} {toupper(format)} {role} rating{?s} in {.field main.{table_name}}.")
  invisible(n)
}

#' Store Combined Player Values
#'
#' @param conn DBI connection with write access.
#' @param data Output of [calculate_player_value_v2()].
#' @param format,gender Bucket the rows belong to.
#' @param as_at Date the value was computed as of.
#' @param table_name Target table in `main`.
#' @return Number of rows written, invisibly.
#' @export
store_player_value_v2 <- function(conn, data, format, gender,
                                  as_at = NULL,
                                  table_name = "player_value_v2") {
  d <- data.table::as.data.table(data)
  if (!nrow(d)) cli::cli_abort("No rows to store for {format}/{gender}.")
  # Sys.Date() would stamp WHEN THE SCRIPT RAN, not what the rating is as of.
  # The two differ whenever a bucket has no recent cricket -- women's ODI last
  # played 2026-07-12 -- and a consumer joining the two tables would then find
  # them disagreeing about the same run.
  stamp <- if (!is.null(as_at)) as.Date(as_at) else attr(data, "as_at")
  if (is.null(stamp)) {
    cli::cli_abort(c("No {.arg as_at} given and none carried on {.arg data}.",
                     "i" = "Pass the date the value was computed as of."))
  }
  d[, `:=`(format = toupper(format), gender = gender, as_at = as.Date(stamp))]

  .recreate_if_stale(conn, table_name, .value_v2_cols)
  DBI::dbExecute(conn, sprintf("
    CREATE TABLE IF NOT EXISTS main.%s (
      format      VARCHAR,
      gender      VARCHAR,
      rank        INTEGER,
      player_id   VARCHAR,
      player_name VARCHAR,
      total_value DOUBLE,
      bat_value   DOUBLE,
      bowl_value  DOUBLE,
      matches     INTEGER,
      bat_balls   INTEGER,
      bowl_balls  INTEGER,
      calibrated  DOUBLE,
      as_at       DATE
    )", table_name))

  DBI::dbExecute(conn, sprintf(
    "DELETE FROM main.%s WHERE format = '%s' AND gender = '%s'",
    table_name, toupper(format), gender))
  duckdb::duckdb_register(conn, "value_v2_staging", d[, .SD, .SDcols = .value_v2_cols])
  on.exit(duckdb::duckdb_unregister(conn, "value_v2_staging"), add = TRUE)
  cols <- paste(.value_v2_cols, collapse = ", ")
  n <- DBI::dbExecute(conn, sprintf(
    "INSERT INTO main.%s (%s) SELECT %s FROM value_v2_staging", table_name, cols, cols))
  cli::cli_alert_success(
    "Stored {n} {gender} {toupper(format)} value{?s} in {.field main.{table_name}}.")
  invisible(n)
}

#' Load Stored Player Ratings
#'
#' @param conn DBI connection; opened read-only and closed on exit if NULL.
#' @param format,gender,role Filters; NULL for all.
#' @param table_name Source table in `main`.
#' @return data.table, best first within each bucket.
#' @export
load_player_rating_v2 <- function(conn = NULL, format = NULL, gender = NULL,
                                  role = NULL, table_name = "player_rating_v2") {
  own <- is.null(conn)
  if (own) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }
  where <- c(if (!is.null(format)) sprintf("format = '%s'", toupper(format)),
             if (!is.null(gender)) sprintf("gender = '%s'", gender),
             if (!is.null(role))   sprintf("role = '%s'", role))
  sql <- sprintf("SELECT * FROM main.%s%s ORDER BY format, gender, role, rank",
                 table_name,
                 if (length(where)) paste0(" WHERE ", paste(where, collapse = " AND ")) else "")
  data.table::as.data.table(DBI::dbGetQuery(conn, sql))
}
