# Derive an exposure floor from RELIABILITY rather than a round number.
#
# Split-half: split a player's balls into two halves by a stable alternating
# sort, correlate the half-means, and Spearman-Brown up to full length. The
# floor is the ball count at which a rating carries enough of its own signal to
# be worth listing. Same estimator as the shrinkage prior (D-P41), which needs
# no distributional assumption.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown=TRUE))
id_map <- build_player_id_map(conn)

rel_at <- function(fmt, role, n_balls) {
  idc <- paste0(role, "_id")
  b <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT r.delivery_id, r.%s AS pid, r.raa FROM main.cricsheet_ball_raa r
    WHERE r.format='%s' AND r.gender='male'", idc, toupper(fmt))))
  canonicalise_player_ids(b, id_map)
  b <- b[!is.na(pid)]
  setorder(b, pid, delivery_id)
  b[, k := seq_len(.N), by = pid]
  b <- b[k <= n_balls]
  keep <- b[, .N, by = pid][N == n_balls, pid]
  b <- b[pid %in% keep]
  if (uniqueN(b$pid) < 40) return(c(NA_real_, uniqueN(b$pid)))
  b[, half := k %% 2L]
  w <- dcast(b[, .(m = mean(raa)), by = .(pid, half)], pid ~ half, value.var = "m")
  setnames(w, c("pid","h0","h1"))
  r <- suppressWarnings(cor(w$h0, w$h1, use = "complete.obs"))
  c(2*r/(1+r), nrow(w))          # Spearman-Brown to full length
}

cat(sprintf("%-6s %-8s %8s %10s %8s\n", "format", "role", "balls", "reliab", "players"))
for (fmt in c("t20","odi","test")) for (role in c("batter","bowler")) {
  for (n in c(200L, 500L, 1000L, 2000L)) {
    v <- rel_at(fmt, role, n)
    cat(sprintf("%-6s %-8s %8d %10s %8d\n", toupper(fmt), role, n,
        if (is.na(v[1])) "-" else sprintf("%.3f", v[1]), v[2]))
  }
}
