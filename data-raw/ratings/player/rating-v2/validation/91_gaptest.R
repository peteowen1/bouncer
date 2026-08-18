# The informative test: same-scale out-of-sample prediction, restricted to
# switcher pairs where the factors actually ask for a correction (>25% apart).
# Factors fitted on data up to 2024-01-01; every row here is after it.
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

run <- function(MINB, GAP) {
  pc <- b[, .(balls=.N, raw=mean(raa)), by=.(batter_id, comp)][balls >= MINB]
  pc <- pc[batter_id %in% pc[, .N, by=batter_id][N >= 2, batter_id]]
  pr <- merge(pc[, .(batter_id, cA=comp, rA=raw)], pc[, .(batter_id, cB=comp, rB=raw)],
              by="batter_id", allow.cartesian=TRUE)[cA != cB]
  pr[, ratio := fmap[cB]/fmap[cA]]
  pr <- pr[abs(ratio - 1) > GAP]
  if (nrow(pr) < 40) { cat(sprintf("  minB=%d gap>%.0f%%: only %d pairs\n", MINB, 100*GAP, nrow(pr))); return(invisible()) }
  sc <- function(fm, lab) {
    pr[, pred := rA * (fm[cB]/fm[cA])]
    mn <- pr[, mean(abs(rB - rA))]; ma <- pr[, mean(abs(rB - pred))]
    d  <- pr[, abs(rB - rA) - abs(rB - pred)]
    set.seed(42); bs <- replicate(2000, mean(sample(d, length(d), replace=TRUE)))
    ci <- quantile(bs, c(.025,.975))
    cat(sprintf("    %-18s naive %.4f  adj %.4f  gain %+6.1f%%  CI [%+.4f, %+.4f] %s\n",
        lab, mn, ma, 100*(mn-ma)/mn, ci[1], ci[2],
        ifelse(ci[1]>0,"HELPS", ifelse(ci[2]<0,"hurts","not distinguishable"))))
  }
  cat(sprintf("\n  minB=%d, gap>%.0f%%, n=%s pairs (%s players)\n", MINB, 100*GAP,
      format(nrow(pr), big.mark=","), format(uniqueN(pr$batter_id), big.mark=",")))
  sc(fmap, "real factors")
  for (s in 1:3) { set.seed(200+s); sh <- fmap; names(sh) <- sample(names(fmap)); sc(sh, sprintf("placebo %d", s)) }
}
cat("=== out-of-sample, strength-gap pairs only ===\n")
run(30L, 0.25); run(60L, 0.25); run(30L, 0.50)
