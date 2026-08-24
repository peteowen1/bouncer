# Persisting the v2 ratings.
#
# Until now `calculate_player_rating_v2()` and `calculate_player_value_v2()`
# returned a data.table and nothing wrote it, so every consumer had to hold an
# 18GB database and half an hour of compute to see a leaderboard. These write
# both to `main`, keyed by bucket, so the blog and the predictions pipeline can
# read them from a release parquet instead.
#
# Replacement is per bucket, never a whole-table wipe: a failed run of one
# bucket must not delete the others. The bucket key is (format, gender, role)
# for the rating table and (format, gender) for the value table, which has no
# role column.

# One source of truth for each table shape. The column list and the CREATE TABLE
# body used to be written out separately, which is how they were able to drift --
# and responding to that drift by dropping the table is what #45 is about.
.rating_v2_schema <- c(
  format = "VARCHAR", gender = "VARCHAR", role = "VARCHAR", rank = "INTEGER",
  player_id = "VARCHAR", player_name = "VARCHAR", rating = "DOUBLE",
  average = "DOUBLE", main_comp = "VARCHAR", matches = "INTEGER",
  balls = "INTEGER", effective_matches = "DOUBLE", last_match = "DATE",
  as_at = "DATE")

.value_v2_schema <- c(
  format = "VARCHAR", gender = "VARCHAR", rank = "INTEGER",
  player_id = "VARCHAR", player_name = "VARCHAR", total_value = "DOUBLE",
  bat_value = "DOUBLE", bowl_value = "DOUBLE", matches = "INTEGER",
  bat_balls = "INTEGER", bowl_balls = "INTEGER", calibrated = "DOUBLE",
  as_at = "DATE")

.rating_v2_cols <- names(.rating_v2_schema)
.value_v2_cols  <- names(.value_v2_schema)

#' CREATE TABLE body from a schema vector
#' @keywords internal
.schema_ddl <- function(schema) {
  paste(sprintf("      %-17s %s", names(schema), unname(schema)), collapse = ",\n")
}

# DuckDB auto-commits every dbExecute unless a transaction is open, so a
# multi-statement replacement is not atomic by default. `expr` is run inside
# one transaction and rolled back whole on any error.
.in_transaction <- function(conn, expr) {
  DBI::dbBegin(conn)
  out <- tryCatch(expr(), error = function(e) {
    DBI::dbRollback(conn)
    cli::cli_abort(c("Write rolled back; nothing was changed.",
                     "x" = conditionMessage(e)))
  })
  DBI::dbCommit(conn)
  out
}

#' Bring a stored table up to the current schema WITHOUT dropping it
#'
#' @section Why this replaced a DROP:
#' This path used to answer any shape mismatch by dropping the whole table,
#' contradicting the contract stated at the top of this file -- replacement is
#' per bucket, never a whole-table wipe. It ran on every call, so adding a single
#' column meant the first successful store dropped and recreated the table; if
#' any later bucket then failed its `check_anchor()` abort, the buckets not yet
#' re-stored were PERMANENTLY GONE, not merely stale. `.in_transaction()` does
#' not help: the drop and the re-store of THAT bucket share a transaction, but
#' the other seven buckets were never in it. The only trace was one
#' `cli_alert_warning` inside a multi-bucket log (bouncerverse#45).
#'
#' Adding a column is the case that actually occurs, and `ALTER TABLE ... ADD
#' COLUMN` handles it while preserving every row.
#'
#' Two things it deliberately does NOT do:
#'
#' * **Drop a column the current schema no longer names.** That is data loss to
#'   tidy up metadata. It is reported and left alone; `INSERT` names its columns
#'   explicitly, so a spare column is inert.
#' * **Change a column type.** There is no non-destructive rewrite, so it aborts
#'   and says what it found. A loud stop beats a silent wipe, which is the whole
#'   point of this function.
#'
#' @param conn DBI connection with write access.
#' @param table_name Table in `main`.
#' @param schema Named character vector of column -> SQL type.
#' @return Invisibly, TRUE if anything was changed or reported.
#' @keywords internal
.migrate_schema <- function(conn, table_name, schema) {
  info <- DBI::dbGetQuery(conn, sprintf("
    SELECT column_name, data_type FROM information_schema.columns
    WHERE table_schema = 'main' AND table_name = '%s'", table_name))

  # No table yet: CREATE TABLE IF NOT EXISTS has already done the work.
  if (!nrow(info)) return(invisible(FALSE))

  existing <- info$column_name
  missing  <- setdiff(names(schema), existing)
  extra    <- setdiff(existing, names(schema))

  shared <- intersect(names(schema), existing)
  have <- toupper(info$data_type[match(shared, existing)])
  want <- toupper(unname(schema[shared]))
  bad  <- have != want
  if (any(bad)) {
    cli::cli_abort(c(
      "{.field main.{table_name}} has {sum(bad)} column{?s} of the wrong type.",
      "x" = "{.field {shared[bad]}}: stored {.val {have[bad]}}, expected {.val {want[bad]}}.",
      "i" = "Migrating a type in place is not safe, and dropping the table would
             destroy every bucket (bouncerverse#45).",
      "i" = "Rebuild deliberately: copy into a new table with the right types,
             check the row counts, then rename."
    ))
  }

  for (col in missing) {
    DBI::dbExecute(conn, sprintf("ALTER TABLE main.%s ADD COLUMN %s %s",
                                 table_name, col, schema[[col]]))
  }
  if (length(missing)) {
    cli::cli_alert_info(
      "Added {length(missing)} column{?s} to {.field main.{table_name}}: {.field {missing}}.
       Existing rows are kept, with NULL in the new column{?s}.")
  }
  if (length(extra)) {
    cli::cli_alert_info(
      "{.field main.{table_name}} carries {length(extra)} column{?s} the current
       schema does not use: {.field {extra}}. Left in place -- dropping them
       would lose data.")
  }

  invisible(length(missing) > 0 || length(extra) > 0)
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

  ensure_table <- function() {
    DBI::dbExecute(conn, sprintf(
      "CREATE TABLE IF NOT EXISTS main.%s (\n%s\n    )",
      table_name, .schema_ddl(.rating_v2_schema)))
    .migrate_schema(conn, table_name, .rating_v2_schema)
  }

  duckdb::duckdb_register(conn, "rating_v2_staging", d[, .SD, .SDcols = .rating_v2_cols])
  on.exit(duckdb::duckdb_unregister(conn, "rating_v2_staging"), add = TRUE)
  cols <- paste(.rating_v2_cols, collapse = ", ")
  # DELETE and INSERT in ONE transaction. DuckDB auto-commits each dbExecute,
  # so without this a DELETE that succeeds followed by an INSERT that fails
  # leaves the bucket permanently EMPTY -- "replacement" that destroys the
  # thing it was replacing.
  n <- .in_transaction(conn, function() {
    ensure_table()
    DBI::dbExecute(conn, sprintf(
      "DELETE FROM main.%s WHERE format = '%s' AND gender = '%s' AND role = '%s'",
      table_name, toupper(format), gender, role))
    DBI::dbExecute(conn, sprintf(
      "INSERT INTO main.%s (%s) SELECT %s FROM rating_v2_staging", table_name, cols, cols))
  })
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

  ensure_table <- function() {
    DBI::dbExecute(conn, sprintf(
      "CREATE TABLE IF NOT EXISTS main.%s (\n%s\n    )",
      table_name, .schema_ddl(.value_v2_schema)))
    .migrate_schema(conn, table_name, .value_v2_schema)
  }

  duckdb::duckdb_register(conn, "value_v2_staging", d[, .SD, .SDcols = .value_v2_cols])
  on.exit(duckdb::duckdb_unregister(conn, "value_v2_staging"), add = TRUE)
  cols <- paste(.value_v2_cols, collapse = ", ")
  n <- .in_transaction(conn, function() {
    ensure_table()
    DBI::dbExecute(conn, sprintf(
      "DELETE FROM main.%s WHERE format = '%s' AND gender = '%s'",
      table_name, toupper(format), gender))
    DBI::dbExecute(conn, sprintf(
      "INSERT INTO main.%s (%s) SELECT %s FROM value_v2_staging", table_name, cols, cols))
  })
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
