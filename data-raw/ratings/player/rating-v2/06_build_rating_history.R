# Causal rating history (D-P69 step 4, D-P70): every rated player's v2 rating
# on the 1st of each month, each month refit from scratch on matches up to
# that date only -- competition factors, opponent effects, shrink target and
# ageing curve included. So a chart shows what the rating would have said at
# the time, not today's fit replayed backwards.
#
# Two inputs are NOT cut at the date: the per-ball run values come from
# main.cricsheet_ball_raa, scored by the agnostic model trained on all years,
# and the player id map is built from all appearances. Both are small next to
# the refit, but the history is causal in its fit, not in every input.
#
# One parquet per (bucket, role, date) under OUT, skipped when present, so a
# stopped run resumes. `--combine` unions them into player_rating_history.parquet
# with `days_since_last_match`, which lets a chart stop at a player's last game
# and a "current best" list drop players who have stopped. It drops the
# career-context columns (average, main_comp): the grid built before
# player_career_context() took `as_at` carried all-time values there.
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
  parts <- lapply(files, function(f) tryCatch(as.data.table(read_parquet(f)),
    error = function(e) stop("Unreadable history file ", f, " (delete it and rerun the grid): ",
                             conditionMessage(e))))
  cols <- names(parts[[1]])
  drift <- files[!vapply(parts, function(p) identical(names(p), cols), logical(1))]
  if (length(drift)) stop(length(drift), " files have different columns from ", basename(files[1]),
                          ", e.g. ", basename(drift[1]))
  h <- rbindlist(parts)
  rm(parts)
  ctx <- intersect(c("main_comp", "main_comp_share", "average"), names(h))
  if (length(ctx)) {
    h[, (ctx) := NULL]
    message("Dropped career-context columns (not cut at as_at in older files): ", toString(ctx))
  }
  dup <- h[, .N, by = .(bucket, role, as_at, player_id)][N > 1L, .N]
  if (dup) stop(dup, " duplicated player-month rows")
  h[, days_since_last_match := as.integer(as_at - as.Date(last_match))]
  stopifnot(nrow(h) > 0L, !anyNA(h$days_since_last_match), all(h$days_since_last_match >= 0L))
  # `rank` counts everyone ever rated, so a retired player's rank drifts as
  # others age past him (Broad: 25th -> 15th in the year after he retired, on
  # a flat rating). Rank the history the way the ratings page does
  # (build_blog_data.R, Pete 2026-10-11): only players with effective_matches
  # >= 1 who played within 3 years of that month. Others get NA.
  RETIRED_AFTER_YEARS <- 3
  setnames(h, "rank", "rank_all")
  h[, current := effective_matches >= 1 & days_since_last_match <= RETIRED_AFTER_YEARS * 365.25]
  h[current == TRUE, rank := frank(-rating, ties.method = "first"), by = .(bucket, role, as_at)]
  h[, current := NULL]
  setcolorder(h, c("bucket", "role", "as_at", "rank", "rank_all"))
  setorder(h, bucket, role, as_at, rank, rank_all)
  path <- file.path(dirname(OUT), "player_rating_history.parquet")
  write_parquet(h, path)
  print(h[, .(dates = uniqueN(as_at), first = min(as_at), last = max(as_at), rows = .N,
              ranked_current = sum(!is.na(rank)), players = uniqueN(player_id)), by = .(bucket, role)])
  cat(sprintf("Wrote %s rows to %s\n", format(nrow(h), big.mark = ","), normalizePath(path)))
  quit(save = "no")
}

DATES <- seq(FROM, TO, by = "month")
if ("--sizing" %in% a) DATES <- TO
conn <- get_db_connection(read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)
idmap <- build_player_id_map(conn)

# A month with too little data for a bucket (early women's ODI) fails with
# one of these; anything else is a bug and stops the grid.
THIN_DATA <- paste(c("No deliveries for", "deliveries on or before", "players clear the reference",
                     "No reference competition appears", "Could not fit an ageing curve",
                     "have a date of birth"), collapse = "|")
skips <- list()

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
        # Write then rename, so a run killed mid-write leaves no truncated file
        # that the resume check would skip forever.
        tmp <- paste0(paths[[role]], ".tmp")
        write_parquet(r, tmp)
        if (!file.rename(tmp, paths[[role]])) stop("Could not rename ", tmp)
      }
      TRUE
    }, error = function(e) {
      msg <- conditionMessage(e)
      if (!grepl(THIN_DATA, msg)) stop(sprintf("%s %s: %s", bucket, as_at, msg), call. = FALSE)
      message(sprintf("SKIP %s %s: %s", bucket, as_at, msg))
      skips[[length(skips) + 1L]] <<- data.table(bucket = bucket, as_at = as_at, reason = msg)
      FALSE
    })
    if (ok) message(sprintf("%s %s done in %.0fs (%.1f min total)", bucket, as_at,
                            as.numeric(difftime(Sys.time(), t0, units = "secs")),
                            as.numeric(difftime(Sys.time(), t_all, units = "mins"))))
  }
}
skips <- rbindlist(skips)
if (nrow(skips)) fwrite(skips, file.path(OUT, "skips.csv"))
message(sprintf("Finished %d dates in %.1f min; %d bucket-months skipped for thin data%s",
                length(DATES), as.numeric(difftime(Sys.time(), t_all, units = "mins")), nrow(skips),
                if (nrow(skips)) paste0(" (", file.path(OUT, "skips.csv"), ")") else ""))
