# Is there ANY population of switchers that bridges a real strength gap?
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
CUT <- "2024-01-01"
fac <- fit_competition_factors(conn, "t20", "male", as_at = CUT)
fmap <- setNames(fac$factor, fac$comp)
b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.batter_id, r.raa, %s AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  WHERE r.format='T20' AND r.gender='male' AND r.match_date > DATE '%s'",
  bouncer:::.competition_sql("t20"), CUT)))
idmap <- build_player_id_map(conn); canonicalise_player_ids(b, idmap)
b[, cf := fmap[comp]]; b <- b[!is.na(cf)]

cat("=== strength-gap switcher pairs available at each ball threshold ===\n")
cat(sprintf("%8s %9s %9s %11s %11s\n","min balls","players","pairs",">25% gap",">50% gap"))
for (MINB in c(30L, 60L, 100L, 150L)) {
  pc <- b[, .(balls=.N, raw=mean(raa)), by=.(batter_id, comp)][balls >= MINB]
  pc <- pc[batter_id %in% pc[, .N, by=batter_id][N >= 2, batter_id]]
  if (!nrow(pc)) next
  pr <- merge(pc[, .(batter_id, cA=comp, rA=raw)], pc[, .(batter_id, cB=comp, rB=raw)],
              by="batter_id", allow.cartesian=TRUE)[cA != cB]
  pr[, gap := abs(fmap[cB]/fmap[cA] - 1)]
  cat(sprintf("%8d %9s %9s %11s %11s\n", MINB,
      format(uniqueN(pc$batter_id), big.mark=","), format(nrow(pr), big.mark=","),
      format(pr[gap>0.25,.N], big.mark=","), format(pr[gap>0.50,.N], big.mark=",")))
}

cat("\n=== same, over the FULL history rather than post-2024 ===\n")
b2 <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.batter_id, r.raa, %s AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  WHERE r.format='T20' AND r.gender='male'", bouncer:::.competition_sql("t20"))))
canonicalise_player_ids(b2, idmap)
b2[, cf := fmap[comp]]; b2 <- b2[!is.na(cf)]
cat(sprintf("%8s %9s %9s %11s %11s\n","min balls","players","pairs",">25% gap",">50% gap"))
for (MINB in c(60L, 150L)) {
  pc <- b2[, .(balls=.N, raw=mean(raa)), by=.(batter_id, comp)][balls >= MINB]
  pc <- pc[batter_id %in% pc[, .N, by=batter_id][N >= 2, batter_id]]
  pr <- merge(pc[, .(batter_id, cA=comp, rA=raw)], pc[, .(batter_id, cB=comp, rB=raw)],
              by="batter_id", allow.cartesian=TRUE)[cA != cB]
  pr[, gap := abs(fmap[cB]/fmap[cA] - 1)]
  cat(sprintf("%8d %9s %9s %11s %11s\n", MINB,
      format(uniqueN(pc$batter_id), big.mark=","), format(nrow(pr), big.mark=","),
      format(pr[gap>0.25,.N], big.mark=","), format(pr[gap>0.50,.N], big.mark=",")))
}
cat("\n(full-history counts are NOT out-of-sample -- the factors were fitted on\n pre-2024 data, so only the post-2024 rows above are a clean test)\n")
