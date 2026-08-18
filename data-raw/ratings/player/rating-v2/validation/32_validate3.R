# Which of the three ratings predicts a player's NEXT match best?
#
# Two questions, deliberately separated:
#  Q1 does the rating machinery beat a plain career mean, per metric, each
#     predicting its OWN forward value? (is each rating worth having)
#  Q2 predicting one COMMON target -- forward raw team-score contribution, the
#     closest thing to "what the player actually did for his team" -- which of
#     the three predicts it best? (which should be the headline rating)
#
# Adjustments frozen on pre-2018 data and evaluated after, as in the earlier
# sweeps. That is a lower bound, but it is the SAME lower bound for all three
# metrics, so the comparison between them is fair.
# Innings 1 only throughout, because TSA exists only there and all three
# metrics must be measured on identical rows.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

CUT <- as.Date("2018-01-01"); MIN_PRIOR <- 10L
id_map <- build_player_id_map(conn)

b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id,
         r.raa_run, r.waa, r.tsa, COALESCE(%s,'unknown') AS comp
  FROM main.cricsheet_ball_raa r
  JOIN cricsheet.matches m ON m.match_id = r.match_id
  WHERE r.format='T20' AND r.gender='male' AND r.tsa IS NOT NULL",
  .competition_sql("t20"))))
canonicalise_player_ids(b, id_map)
cat(sprintf("innings-1 T20 male deliveries: %s\n", format(nrow(b), big.mark=",")))

pre <- b[match_date < CUT]
facs <- list(
  runs   = fit_competition_factors(conn, "t20","male", id_map=id_map, as_at=CUT-1L, basis="runs"),
  wkt    = fit_competition_factors(conn, "t20","male", id_map=id_map, as_at=CUT-1L, basis="survival"),
  tsa    = fit_competition_factors(conn, "t20","male", id_map=id_map, as_at=CUT-1L, basis="runs"))

mk <- function(col, fac) {
  fmap <- setNames(fac$factor, fac$comp)
  p <- copy(pre); p[, raa := get(col)]
  eff <- fit_two_way_effects(p, prior_balls = 60, iterations = 20)
  d <- copy(b); d[, cf := fmap[comp]][is.na(cf), cf := 1]
  d[eff$bowler, on="bowler_id", bo := i.eff][is.na(bo), bo := 0]
  d[, val := (get(col) - bo) / cf]
  d[, .(v = sum(val), raw = sum(get(col))), by = .(player_id = batter_id, match_id, match_date)]
}
pm <- list(runs = mk("raa_run", facs$runs),
           wkt  = mk("waa",     facs$wkt),
           tsa  = mk("tsa",     facs$tsa))
for (k in names(pm)) { setorder(pm[[k]], player_id, match_date, match_id)
                       pm[[k]][, idx := seq_len(.N), by = player_id] }

dec <- function(v, dt, d, prior, pop) {
  n <- length(v); rt <- rep(NA_real_, n); sw <- 0; svw <- 0
  if (n >= 2L) for (i in 2:n) {
    a <- exp(-as.numeric(dt[i]-dt[i-1L])/d); svw <- a*(svw+v[i-1L]); sw <- a*(sw+1)
    rt[i] <- (svw + prior*pop)/(sw + prior) }
  rt }

for (k in names(pm)) {
  x <- pm[[k]]; pop <- x[, mean(v)]
  x[, rt := dec(v, match_date, 1095, 20, pop), by = player_id]
  x[, cw := { cs <- cumsum(raw); c(NA, cs[-.N]/seq_len(.N-1L)) }, by = player_id]
}

fwd <- function(x, H) x[, { cs <- cumsum(raw); k <- pmin(.N, idx+H-1L)
                            (cs[k]-c(0,cs)[idx])/(k-idx+1L) }, by = player_id]$V1

cat("\n=== Q1: does the rating beat a plain career mean, per metric? ===\n")
cat(sprintf("  %-12s %8s %10s %10s %9s\n","metric","horizon","rating","career","gain"))
for (k in names(pm)) for (H in c(1L,5L)) {
  x <- pm[[k]]; x[, f := fwd(x, H)]
  e <- x[idx-1L >= MIN_PRIOR & match_date >= CUT & is.finite(rt) & is.finite(cw) & is.finite(f)]
  r <- cor(e$rt,e$f,method="spearman"); c0 <- cor(e$cw,e$f,method="spearman")
  cat(sprintf("  %-12s next %-3d %10.4f %10.4f %+8.1f%%   n=%s\n", k, H, r, c0,
              100*(r-c0)/abs(c0), format(nrow(e), big.mark=",")))
}

cat("\n=== Q2: predicting the SAME target (forward raw team-score) ===\n")
tgt <- pm$tsa[, .(player_id, match_id, idx_t = idx)]
for (H in c(1L,5L)) {
  pm$tsa[, f := fwd(pm$tsa, H)]
  key <- pm$tsa[, .(player_id, match_id, match_date, f, idx)]
  cat(sprintf("\n  horizon next %d\n", H))
  for (k in names(pm)) {
    j <- merge(pm[[k]][, .(player_id, match_id, rt, cw, idx)],
               key[, .(player_id, match_id, f)], by = c("player_id","match_id"))
    e <- j[idx-1L >= MIN_PRIOR & is.finite(rt) & is.finite(f)]
    e <- merge(e, pm$tsa[, .(player_id, match_id, match_date)], by = c("player_id","match_id"))
    e <- e[match_date >= CUT]
    cat(sprintf("    %-12s rating rho %+.4f   n=%s\n", k,
                cor(e$rt, e$f, method="spearman"), format(nrow(e), big.mark=",")))
  }
  j <- pm$tsa[idx-1L >= MIN_PRIOR & match_date >= CUT & is.finite(cw) & is.finite(f)]
  cat(sprintf("    %-12s career rho %+.4f\n", "BASELINE", cor(j$cw, j$f, method="spearman")))
}
