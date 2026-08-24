# Rewrite the player-reference columns of cricsheet.deliveries for matches that
# stored NAMES instead of registry ids (bouncerverse#74).
#
# WHY SURGICAL, not a reload. batch_load_matches() deletes and re-inserts whole
# matches, and cricsheet.matches has a 34th column, unified_margin, that is
# COMPUTED downstream and absent from the parser's 33. Its SELECT * insert
# therefore fails on a RE-insert -- something the loader had never been asked
# to do, because until now its only job was adding new matches. That failure
# left 995 matches deleted until a backup restored them. This updates six
# columns joined on delivery_id, so every computed column and every row
# survives by construction.
#
# RESUMABLE. Each match commits on its own, and the work list is derived from
# the database each run -- so an interrupted run simply leaves fewer to do.
#
# Usage:
#   Rscript data-raw/data-acquisition/fix_74_player_ids.R <json_dir> [--commit] [--limit N]
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(jsonlite); library(data.table)})

a <- commandArgs(trailingOnly = TRUE)
SRC <- a[1]
commit <- "--commit" %in% a
lim <- if ("--limit" %in% a) as.integer(a[which(a == "--limit") + 1L]) else Inf
stopifnot(dir.exists(SRC))

ID_COLS <- c("batter_id", "bowler_id", "non_striker_id", "player_out_id",
             "fielder1_id", "fielder2_id")

conn <- get_db_connection(read_only = FALSE)
all_affected <- dbGetQuery(conn, "
  SELECT DISTINCT match_id FROM cricsheet.deliveries
  WHERE NOT regexp_matches(batter_id, '^[0-9a-f]{8}$')")$match_id
todo <- all_affected[file.exists(file.path(SRC, paste0(all_affected, ".json")))]
n_missing_json <- length(all_affected) - length(todo)
if (is.finite(lim)) todo <- head(todo, lim)
mode <- if (commit) "" else " (DRY RUN -- nothing will be written)"
cli::cli_alert_info("{length(todo)} match{?es} to fix{mode}")
if (n_missing_json > 0L) {
  cli::cli_alert_warning(
    "{n_missing_json} affected match{?es} have no local JSON in {.file {SRC}} and will stay name-keyed.")
}

ok <- 0L; skipped <- character(0); t0 <- Sys.time()
for (i in seq_along(todo)) {
  mid <- todo[i]
  res <- tryCatch({
    j <- fromJSON(file.path(SRC, paste0(mid, ".json")), simplifyVector = FALSE)
    p <- suppressWarnings(parse_all_data(j, parse_match_info(j, mid)))
    if (p$registry_fallback > 0) stop("fresh file still falls back")
    fresh <- as.data.table(p$deliveries)[, c("delivery_id", ID_COLS), with = FALSE]
    cur <- dbGetQuery(conn, sprintf(
      "SELECT delivery_id FROM cricsheet.deliveries WHERE match_id = '%s'", mid))$delivery_id
    # The key must cover every stored row, or the update fixes some deliveries
    # and silently leaves others on the old ids.
    if (length(setdiff(cur, fresh$delivery_id)) || length(setdiff(fresh$delivery_id, cur)))
      stop("delivery_id sets differ")
    if (!commit) "dry" else {
    dbWriteTable(conn, "fix74_tmp", as.data.frame(fresh), overwrite = TRUE, temporary = TRUE)
    .in_transaction(conn, function() {
      sets <- paste(sprintf("%s = f.%s", ID_COLS, ID_COLS), collapse = ", ")
      dbExecute(conn, sprintf("
        UPDATE cricsheet.deliveries AS d SET %s FROM fix74_tmp AS f
        WHERE d.delivery_id = f.delivery_id AND d.match_id = '%s'", sets, mid))
    })
    dbExecute(conn, "DROP TABLE IF EXISTS fix74_tmp")
    # dbExecute's return is not a row count in DuckDB, so verify from the data.
    chk <- dbGetQuery(conn, sprintf("
      SELECT COUNT(*) AS n_rows,
             SUM(CASE WHEN regexp_matches(batter_id,'^[0-9a-f]{8}$') THEN 0 ELSE 1 END) AS name_keyed
      FROM cricsheet.deliveries WHERE match_id = '%s'", mid))
    if (chk$n_rows != length(cur)) stop(sprintf("row count changed %d -> %d", length(cur), chk$n_rows))
    if (chk$name_keyed != 0) stop(sprintf("%d rows still name-keyed", chk$name_keyed))
    "ok"
    }
  }, error = function(e) paste0("ERR: ", conditionMessage(e)))
  if (identical(res, "ok") || identical(res, "dry")) ok <- ok + 1L
  else skipped <- c(skipped, paste(mid, res))
  if (i %% 50 == 0 || i == length(todo)) {
    cat(sprintf("%d/%d | %d ok | %d skipped | %.1f mins\n", i, length(todo), ok,
                length(skipped), as.numeric(difftime(Sys.time(), t0, units = "mins"))))
    flush.console()
  }
}
dbDisconnect(conn, shutdown = TRUE)
cli::cli_h2("Done")
verb <- if (commit) "fixed" else "would be fixed"
cli::cli_alert_success("{ok} match{?es} {verb}")
if (length(skipped)) {
  # Named, not counted: a silently skipped match stays split forever.
  cli::cli_alert_warning("{length(skipped)} NOT fixed:")
  for (s in head(skipped, 20)) cli::cli_bullets(c("*" = s))
}
