# Backfill is_free_hit on cricsheet.deliveries (bouncerverse#81/D-P50, stage 1).
#
# Cricsheet has no free_hit field (verified against the published schema and
# a real no-ball delivery -- see docs/plans/D-P50-WIDE-CATEGORY-REBUILD.md).
# is_free_hit is derived post-parse via compute_is_free_hit(): a no-ball
# triggers a free hit on the next delivery, carrying forward through any
# further illegal deliveries until a legal one is bowled. One pass over the
# whole table, no re-parse needed -- see that function's roxygen for the
# vectorized derivation and why it's correct without an explicit loop.
#
# Usage:
#   Rscript data-raw/data-acquisition/backfill_free_hit.R [--commit]
# Without --commit this is a dry run: computes and reports, writes nothing.

suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})

commit <- "--commit" %in% commandArgs(trailingOnly = TRUE)
cli::cli_h1("Backfill is_free_hit{if (!commit) ' (DRY RUN)'}")

conn <- get_db_connection(read_only = !commit)
on.exit(try(dbDisconnect(conn, shutdown = TRUE), silent = TRUE), add = TRUE)
if (commit) ensure_free_hit_column(conn)

cli::cli_h2("Loading deliveries")
d <- as.data.table(dbGetQuery(conn, "
  SELECT delivery_id, match_id, match_type, innings, over, ball, wides, noballs
  FROM cricsheet.deliveries
"))
cli::cli_alert_success("Loaded {format(nrow(d), big.mark=',')} deliveries")

cli::cli_h2("Computing is_free_hit")
d[, is_free_hit := compute_is_free_hit(.SD)]

n_free_hit <- sum(d$is_free_hit)
n_noball <- sum(d$noballs > 0)
cli::cli_alert_info(
  "{format(n_free_hit, big.mark=',')} free-hit balls ({round(100*n_free_hit/nrow(d), 3)}%), against {format(n_noball, big.mark=',')} no-balls ({round(100*n_noball/nrow(d), 3)}%) -- ratio {round(n_free_hit/n_noball, 3)}")
# A genuine implementation should land close to 1.0-1.3x: every no-ball
# produces at least one free-hit ball, plus extra when illegal deliveries
# chain. Far outside that band means the carry-forward logic is wrong, not
# that free hits are simply rare. On --commit this must ABORT, not warn --
# the row-count/NULL checks after the write only catch a join gone wrong,
# not a computation that's confidently, fully-populated WRONG.
if (n_free_hit / n_noball < 0.9 || n_free_hit / n_noball > 2.0) {
  msg <- "free_hit:noball ratio {round(n_free_hit/n_noball, 3)} is outside the expected 0.9-2.0 band -- check compute_is_free_hit() before trusting this backfill."
  if (commit) cli::cli_abort(msg) else cli::cli_alert_warning(msg)
}

cli::cli_h3("By format")
print(d[, .(n = .N, free_hit_rate = round(100 * mean(is_free_hit), 3)), by = match_type][order(-n)])

if (!commit) {
  cli::cli_alert_info("Dry run -- nothing written. Re-run with --commit to persist.")
  dbDisconnect(conn, shutdown = TRUE)
  quit(save = "no", status = 0)
}

cli::cli_h2("Writing back")
n_before <- dbGetQuery(conn, "SELECT COUNT(*) n FROM cricsheet.deliveries")$n

staging <- d[, .(delivery_id, is_free_hit)]
dbWriteTable(conn, "free_hit_staging", as.data.frame(staging), overwrite = TRUE, temporary = TRUE)
.in_transaction(conn, function() {
  dbExecute(conn, "
    UPDATE cricsheet.deliveries AS del SET is_free_hit = s.is_free_hit
    FROM free_hit_staging AS s
    WHERE del.delivery_id = s.delivery_id")
})
dbExecute(conn, "DROP TABLE IF EXISTS free_hit_staging")

chk <- dbGetQuery(conn, "
  SELECT COUNT(*) AS n_rows, SUM(CASE WHEN is_free_hit IS NULL THEN 1 ELSE 0 END) AS n_null
  FROM cricsheet.deliveries")
if (chk$n_rows != n_before) {
  cli::cli_abort("Row count changed during backfill: {n_before} -> {chk$n_rows}. Investigate before trusting this table.")
}
if (chk$n_null > 0) {
  cli::cli_abort("{chk$n_null} rows still have NULL is_free_hit after the update -- the staging join missed some delivery_ids.")
}
cli::cli_alert_success("Wrote {format(n_before, big.mark=',')} rows, 0 nulls, row count unchanged.")

dbDisconnect(conn, shutdown = TRUE)
cli::cli_alert_success("Done.")
