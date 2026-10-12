# Causal rating history (D-P69 step 4, D-P70): every rated player's v2 rating
# on the 1st of each month, each month refit from scratch on matches up to
# that date only -- competition factors, opponent effects, shrink target and
# ageing curve included. So a chart shows what the rating would have said at
# the time, not today's fit replayed backwards.
#
# One parquet per (bucket, role, date) under OUT, skipped when present, so a
# stopped run resumes. `--combine` unions them into player_rating_history.parquet
# with `days_since_last_match`, which lets a chart stop at a player's last game
# and a "current best" list drop players who have stopped.
#
# Usage (PowerShell; arrow segfaults under Git Bash R). Launch detached:
#   Rscript 06_build_rating_history.R --sizing             # time one date
#   Rscript 06_build_rating_history.R --from 2015-01-01    # build the grid
#   Rscript 06_build_rating_history.R --combine            # union to one file
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table); library(arrow)})

a <- commandArgs(trailingOnly = TRUE)
getopt <- function(flag, default) { i <- which(a == flag); if (length(i)) a[i + 1L] else default }
FROM <- as.Date(getopt("--from", "2015-01-01"))
TO   <- as.Date(getopt("--to", format(Sys.Date(), "%Y-%m-01")))
OUT  <- getopt("--out", file.path(find_bouncerdata_dir(), "source", "rating_history"))  # source/ is gitignored
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

BUCKETS <- list(
  list(format = "t20",  gender = "male"),
  list(format = "odi",  gender = "male"),
  list(format = "t20",  gender = "female"),
  list(format = "odi",  gender = "female"),
  list(format = "test", gender = "male"),
  list(format = "test", gender = "male", match_type_filter = "test", store_as = "test_intl"))

if ("--combine" %in% a) {
  files <- list.files(OUT, "[.]parquet$", full.names = TRUE)
  if (!length(files)) stop("No history files under ", OUT)
  h <- rbindlist(lapply(files, function(f) as.data.table(read_parquet(f))), fill = TRUE)
  dup <- h[, .N, by = .(bucket, role, as_at, player_id)][N > 1L, .N]
  if (dup) stop(dup, " duplicated player-month rows")
  h[, days_since_last_match := as.integer(as_at - as.Date(last_match))]
  stopifnot(all(h$days_since_last_match >= 0L))
  setorder(h, bucket, role, as_at, rank)
  path <- file.path(dirname(OUT), "player_rating_history.parquet")
  write_parquet(h, path)
  print(h[, .(dates = uniqueN(as_at), first = min(as_at), last = max(as_at), rows = .N,
              players = uniqueN(player_id)), by = .(bucket, role)])
  cat(sprintf("Wrote %s rows to %s\n", format(nrow(h), big.mark = ","), normalizePath(path)))
  quit(save = "no")
}

DATES <- seq(FROM, TO, by = "month")
if ("--sizing" %in% a) DATES <- TO
conn <- get_db_connection(read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)
idmap <- build_player_id_map(conn)

t_all <- Sys.time()
for (d in rev(seq_along(DATES))) {     # newest first: the most-used months land first
  as_at <- DATES[d]
  for (b in BUCKETS) {
    store_as <- if (is.null(b$store_as)) b$format else b$store_as
    bucket <- paste(store_as, b$gender, sep = "_")
    paths <- setNames(file.path(OUT, sprintf("%s_%s_%s.parquet", bucket, c("batter", "bowler"), as_at)),
                      c("batter", "bowler"))
    if (all(file.exists(paths))) next
    t0 <- Sys.time()
    # A bucket with too little data before this date (early women's ODI) fails
    # here; record the gap and move on rather than stop the grid.
    ok <- tryCatch({
      factors <- suppressMessages(fit_competition_factors(conn, b$format, b$gender,
                                                          id_map = idmap, as_at = as_at))
      for (role in c("batter", "bowler")) {
        if (file.exists(paths[[role]])) next
        r <- suppressMessages(calculate_player_rating_v2(b$format, b$gender, role = role, conn = conn,
               as_at = as_at, factors = factors, id_map = idmap,
               match_type_filter = b$match_type_filter))
        r <- as.data.table(r)
        r[, `:=`(bucket = bucket, role = role, as_at = as_at)]
        write_parquet(r, paths[[role]])
      }
      TRUE
    }, error = function(e) { message(sprintf("SKIP %s %s: %s", bucket, as_at, conditionMessage(e))); FALSE })
    if (ok) message(sprintf("%s %s done in %.0fs (%.1f min total)", bucket, as_at,
                            as.numeric(difftime(Sys.time(), t0, units = "secs")),
                            as.numeric(difftime(Sys.time(), t_all, units = "mins"))))
  }
}
message(sprintf("Finished %d dates in %.1f min", length(DATES),
                as.numeric(difftime(Sys.time(), t_all, units = "mins"))))
