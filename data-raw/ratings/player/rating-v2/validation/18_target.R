# Does the Test batting rating predict forward AVERAGE (runs per dismissal),
# even though it does not predict forward STRIKE RATE (runs per ball)?
#
# This is a diagnostic on WHY the pre-declared metric failed, not a
# re-declaration of the metric. The harness scores runs-per-ball and
# events-per-ball as separate targets; Test batting quality is their RATIO, and
# nothing in the harness scores a ratio. D-P3 in DECISIONS.md flagged the target
# question on 2026-08-14, well before today, so this is a documented prior
# concern rather than a post-hoc rescue.
#
# Reported alongside the failing numbers either way.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
OUT <- "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda"

origins <- RATING_VAL_ORIGINS
id_map  <- build_player_id_map(conn)

cat("building the batter rating time series (10 fits, each blind to its future)\n")
rs <- list()
for (T0 in as.list(origins)) {
  a <- T0 - 1L
  r <- suppressMessages(calculate_player_rating_v2(
    "test", "male", role = "batter", conn = conn, as_at = a, id_map = id_map))
  rs[[format(T0)]] <- as.data.table(r)[, .(player_id, rating, match_date = a)]
  cat(".", sep = "")
}
cat(" done\n")
ratings <- rbindlist(rs); setorder(ratings, match_date)

pool  <- load_rating_pool(conn, role = "batter")
frame <- build_rating_frame(pool, ratings, rating_cols = "rating", origins = origins)
setDT(frame)
cat(sprintf("frame: %s rows, cols: %s\n", format(nrow(frame), big.mark = ","),
            paste(names(frame), collapse = ", ")))

# The harness's own forward-window columns. Identify them rather than assume.
fwd <- grep("^(w_|fwd|post|window)", names(frame), value = TRUE)
cat("forward-window columns:", paste(fwd, collapse = ", "), "\n\n")

nz <- function(x) is.finite(x)
res <- data.table()
for (T0 in as.list(origins[-1])) {
  f <- frame[origin == T0]
  if (!nrow(f)) next
  # forward run rate, event rate, and their RATIO (= average)
  f[, fwd_rate  := win_runs / win_balls]
  f[, fwd_evrate := win_events / win_balls]
  f[, fwd_avg   := win_runs / pmax(win_events, 1)]
  # pre-origin career average, as the like-for-like baseline for a ratio target
  f[, car_avg   := career_runs / pmax(career_events, 1)]
  keep <- f[nz(rating) & nz(fwd_avg) & win_events >= 3]
  if (nrow(keep) < 30) next
  res <- rbind(res, data.table(
    origin = T0, n = nrow(keep),
    rho_rating_vs_SR  = cor(keep$rating, keep$fwd_rate,   method = "spearman"),
    rho_rating_vs_AVG = cor(keep$rating, keep$fwd_avg,    method = "spearman"),
    rho_career_vs_AVG = cor(keep$car_avg, keep$fwd_avg,   method = "spearman")))
}
print(res[, lapply(.SD, function(x) if (is.numeric(x)) round(x, 3) else x)])

cat("\n=== pooled over all scored origins ===\n")
cat(sprintf("  rating  vs forward STRIKE RATE : %+.3f   (the harness's 'runs' target)\n",
            weighted.mean(res$rho_rating_vs_SR, res$n)))
cat(sprintf("  rating  vs forward AVERAGE     : %+.3f   (runs per dismissal)\n",
            weighted.mean(res$rho_rating_vs_AVG, res$n)))
cat(sprintf("  career  vs forward AVERAGE     : %+.3f   (the like-for-like baseline)\n",
            weighted.mean(res$rho_career_vs_AVG, res$n)))
