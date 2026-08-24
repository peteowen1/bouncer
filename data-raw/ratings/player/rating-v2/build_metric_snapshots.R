# Time-causal snapshots of calculate_player_rating_v2() under a chosen METRIC,
# so the team rating can be tested on RVAA and TSA rather than RAA alone.
#
# WHY. The team rating tested so far is built on calculate_player_value_v2(),
# whose per-ball quantity is `raa - opponent_effect` -- RUNS ONLY, no wicket
# term. In Test cricket wickets are the currency, so a bowling contribution
# credited purely through runs prevented may be measuring the wrong thing.
#
# calculate_player_rating_v2(metric = ...) offers "composite" (RVAA = RAA +
# lambda*WAA), "runs", "wickets" and "team_score" (TSA).
#
# IMPORTANT: these are PER ROLE and the two roles are NOT on a common scale --
# the function's own @return says they "must not be added". So they are stored
# per role and consumed as SEPARATE model features, never summed.
#
# Usage: Rscript build_metric_snapshots.R --metric composite --from 2023-01-01 --by 1
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})

a <- commandArgs(trailingOnly = TRUE)
getopt <- function(f, d) { i <- which(a == f); if (length(i)) a[i + 1L] else d }
METRIC    <- getopt("--metric", "composite")
FROM      <- as.Date(getopt("--from", "2023-01-01"))
BY_MONTHS <- as.integer(getopt("--by", "1"))
MIN_BALLS <- as.integer(getopt("--min-balls", "1"))
# Not every metric exists for every format. TSA is a projected-final-score
# effect, which needs a fixed ball allocation, so Test has none by construction
# (see validation/30_tsa_persist.R). Building the full grid anyway produced 264
# builds that all "failed" with a binder error on a column that was never going
# to be there -- loud, but indistinguishable from a real breakage.
FORMATS <- strsplit(getopt("--formats", "t20,odi,test"), ",")[[1]]
TBL <- paste0("player_metric_snapshots_", METRIC)

conn <- get_db_connection(read_only = TRUE)
last <- as.Date(dbGetQuery(conn, "SELECT MAX(match_date) d FROM cricsheet.matches")$d)
dbDisconnect(conn, shutdown = TRUE)
dates <- seq(FROM, last, by = paste(BY_MONTHS, "months"))

GRID <- expand.grid(as_at = dates, format = FORMATS,
                    role = c("batter", "bowler"), stringsAsFactors = FALSE)
cli::cli_alert_info("metric={METRIC}, formats {FORMATS}, male only: {nrow(GRID)} builds at ~19s -> ~{round(nrow(GRID)*19/60)} min")

conn <- get_db_connection(read_only = FALSE)
on.exit(try(dbDisconnect(conn, shutdown = TRUE), silent = TRUE), add = TRUE)
if (!table_exists(conn, TBL)) {
  dbExecute(conn, sprintf("CREATE TABLE %s (as_at DATE, format VARCHAR, role VARCHAR,
    player_id VARCHAR, player_name VARCHAR, rating DOUBLE, balls INTEGER,
    built_at TIMESTAMP)", TBL))
  cli::cli_alert_success("Created {TBL}")
}
done <- dbGetQuery(conn, sprintf("SELECT DISTINCT as_at, format, role FROM %s", TBL))
key <- function(x) paste(as.character(x$as_at), x$format, x$role)
todo <- GRID[!(key(GRID) %in% key(done)), , drop = FALSE]
cli::cli_alert_info("{nrow(done)} already built; {nrow(todo)} to do")

ok <- 0L; failed <- character(0); t0 <- Sys.time()
for (i in seq_len(nrow(todo))) {
  g <- todo[i, ]
  res <- tryCatch({
    r <- calculate_player_rating_v2(g$format, "male", g$role, conn = conn,
                                    as_at = as.Date(g$as_at), min_balls = MIN_BALLS,
                                    metric = METRIC)
    if (is.null(r) || !nrow(r)) stop("no players rated")
    keep <- data.frame(as_at = as.Date(g$as_at), format = g$format, role = g$role,
                       player_id = r$player_id, player_name = r$player_name,
                       rating = as.numeric(r$rating), balls = as.integer(r$balls),
                       built_at = Sys.time(), stringsAsFactors = FALSE)
    .in_transaction(conn, function() {
      dbExecute(conn, sprintf("DELETE FROM %s WHERE as_at=DATE '%s' AND format='%s' AND role='%s'",
                              TBL, as.Date(g$as_at), g$format, g$role))
      dbWriteTable(conn, TBL, keep, append = TRUE)
    })
    "ok"
  }, error = function(e) paste0("ERR: ", conditionMessage(e)))
  if (identical(res, "ok")) ok <- ok + 1L else
    failed <- c(failed, sprintf("%s %s/%s %s", g$as_at, g$format, g$role, res))
  if (i %% 10 == 0 || i == nrow(todo))
    cat(sprintf("%d/%d | %d ok | %d failed | %.1f mins\n", i, nrow(todo), ok,
                length(failed), as.numeric(difftime(Sys.time(), t0, units = "mins"))))
}
cli::cli_alert_success("{ok} build{?s} stored in {TBL}")
if (length(failed)) { cli::cli_alert_warning("{length(failed)} failed:")
  for (f in failed) cli::cli_bullets(c("*" = f)) }
# Every build failing is a broken run, not a run with failures, and the
# difference matters because the progress line looks identical either way --
# "264/264 | 0 ok | 264 failed" scrolled past as if it were work being done.
if (ok == 0L && nrow(todo) > 0L) {
  cli::cli_abort("Every one of {nrow(todo)} builds failed -- {TBL} gained nothing.")
}
