# Score the Test rating with R/rating_validation.R -- the 12-month-forward,
# rolling-origin harness that is already hardcoded to Test+MDM male and was
# built for the rejected delivery-level ELO. Primary loss was pre-declared in
# TEST-LAMBDA-PREDECLARATION.md as THIS harness, not a next-match metric,
# because a Test specialist plays a handful of Tests a year.
#
# The rating at each origin is computed with as_at = origin - 1 day, which now
# genuinely truncates the opponent fit and the competition factors as well as
# the aggregation. Before today's fix it truncated only the aggregation, so this
# harness would have scored a rating that already knew the future against
# baselines that did not.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
OUT <- "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda"

origins <- RATING_VAL_ORIGINS
cat("origins:", paste(format(origins), collapse = " "), "\n")
cat("(first is calibration-only and never scored)\n\n")

id_map <- build_player_id_map(conn)

for (role in c("batter", "bowler")) {
  cat("\n", strrep("=", 72), "\n", toupper(role), "\n", strrep("=", 72), "\n", sep = "")

  # --- rating time series, one fit per origin, each blind to its own future ---
  rs <- list()
  for (T0 in as.list(origins)) {
    a <- T0 - 1L
    r <- tryCatch(
      suppressMessages(calculate_player_rating_v2(
        "test", "male", role = role, conn = conn, as_at = a, id_map = id_map)),
      error = function(e) { cat("  ", format(T0), "ERROR:", conditionMessage(e), "\n"); NULL })
    if (is.null(r)) next
    r <- as.data.table(r)[, .(player_id, rating)]
    r[, match_date := a]
    rs[[format(T0)]] <- r
    cat(sprintf("  as_at %s -> %d players\n", format(a), nrow(r)))
  }
  ratings <- rbindlist(rs)
  setorder(ratings, match_date)

  pool <- load_rating_pool(conn, role = role)
  cat(sprintf("\n  pool: %s rows, %d players\n",
              format(nrow(pool), big.mark = ","), uniqueN(pool$player_id)))

  frame <- build_rating_frame(pool, ratings, rating_cols = "rating", origins = origins)
  cat(sprintf("  frame: %s rows across %d origins\n",
              format(nrow(frame), big.mark = ","), uniqueN(frame$origin)))

  for (tgt in c("runs", "events")) {
    cat(sprintf("\n  --- target: %s ---\n", tgt))
    sc <- tryCatch(score_rating(frame, target = tgt, rating_col = "rating",
                                origins = origins),
                   error = function(e) { cat("   ERROR:", conditionMessage(e), "\n"); NULL })
    if (is.null(sc)) next
    print(summarise_rating_score(sc, label = sprintf("v2 test %s/%s", role, tgt)))
    saveRDS(sc, file.path(OUT, sprintf("harness_%s_%s.rds", role, tgt)))
  }
}
