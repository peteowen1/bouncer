# Split the out-of-sample test by DIRECTION of the move. The earlier aggregate
# pooled steps up and steps down using a symmetric |ratio-1| gap, which can hide
# opposite behaviour. Factors fitted as_at 2024-01-01; all rows after it.
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

test <- function(MINB, GAP) {
  pc <- b[, .(balls=.N, raw=mean(raa)), by=.(batter_id, comp)][balls >= MINB]
  pc <- pc[batter_id %in% pc[, .N, by=batter_id][N >= 2, batter_id]]
  pr <- merge(pc[, .(batter_id, cA=comp, rA=raw)], pc[, .(batter_id, cB=comp, rB=raw)],
              by="batter_id", allow.cartesian=TRUE)[cA != cB]
  pr[, `:=`(fA = fmap[cA], fB = fmap[cB])]
  pr[, ratio := fB/fA]
  pr <- pr[abs(ratio - 1) > GAP]
  pr[, dir := fifelse(fB < fA, "step UP (weak -> strong)", "step DOWN (strong -> weak)")]
  pr[, pred := rA * ratio]
  cat(sprintf("\n--- min %d balls, gap > %.0f%% ---\n", MINB, 100*GAP))
  for (d in c("step UP (weak -> strong)", "step DOWN (strong -> weak)")) {
    s <- pr[dir == d]
    if (nrow(s) < 30) { cat(sprintf("  %-28s n=%d, too few\n", d, nrow(s))); next }
    mn <- s[, mean(abs(rB - rA))]; ma <- s[, mean(abs(rB - pred))]
    dd <- s[, abs(rB - rA) - abs(rB - pred)]
    set.seed(42); bs <- replicate(2000, mean(sample(dd, length(dd), replace=TRUE)))
    ci <- quantile(bs, c(.025,.975))
    cat(sprintf("  %-28s n=%5s  naive %.4f  adj %.4f  gain %+6.1f%%  CI [%+.4f,%+.4f] %s\n",
        d, format(nrow(s), big.mark=","), mn, ma, 100*(mn-ma)/mn, ci[1], ci[2],
        ifelse(ci[1]>0,"HELPS", ifelse(ci[2]<0,"HURTS","n.s."))))
  }
}
cat("=== out-of-sample, split by direction of the move ===\n")
test(30L, 0.25); test(60L, 0.25); test(30L, 0.50)
