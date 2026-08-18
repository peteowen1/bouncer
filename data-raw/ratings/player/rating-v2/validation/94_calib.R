# Are the competition adjustments CALIBRATED? Bin by how much adjustment was
# applied, then per bin show what we predicted vs what actually happened.
# bias = mean(actual - predicted). Negative bias = we predicted too high =
# the adjustment did not discount enough.
# Factors fitted as_at 2024-01-01; every row tested is after it.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
CUT <- "2024-01-01"; MINB <- 30L
fac <- fit_competition_factors(conn, "t20", "male", as_at = CUT)
fmap <- setNames(fac$factor, fac$comp)
b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.batter_id, r.raa, %s AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  WHERE r.format='T20' AND r.gender='male' AND r.match_date > DATE '%s'",
  bouncer:::.competition_sql("t20"), CUT)))
idmap <- build_player_id_map(conn); canonicalise_player_ids(b, idmap)
b[, cf := fmap[comp]]; b <- b[!is.na(cf)]
pc <- b[, .(balls=.N, raw=mean(raa)), by=.(batter_id, comp)][balls >= MINB]
pc <- pc[batter_id %in% pc[, .N, by=batter_id][N >= 2, batter_id]]
pr <- merge(pc[, .(batter_id, cA=comp, rA=raw, nA=balls)],
            pc[, .(batter_id, cB=comp, rB=raw, nB=balls)], by="batter_id",
            allow.cartesian=TRUE)[cA != cB]
pr[, `:=`(fA=fmap[cA], fB=fmap[cB])][, ratio := fB/fA]
pr[, pred := rA * ratio]

up <- pr[ratio < 1]   # the operation the rating performs
cat(sprintf("STEP UP only (weak -> strong): %s pairs, %s players\n",
            format(nrow(up), big.mark=","), format(uniqueN(up$batter_id), big.mark=",")))
cat("T20 men, RAA in RUNS PER BALL. bias = actual - predicted.\n")
cat("negative bias => predicted too high => adjustment did not discount enough.\n\n")

up[, bin := cut(ratio, breaks=c(0,0.45,0.60,0.75,0.85,0.95,1.0),
                labels=c("<0.45 (huge)","0.45-0.60","0.60-0.75","0.75-0.85","0.85-0.95","0.95-1.00 (tiny)"),
                include.lowest=TRUE)]
cat(sprintf("%-18s %6s %7s %8s %9s %9s %8s %8s %8s\n",
    "adjustment (f_B/f_A)","n","mean r","weak RAA","predicted","actual","bias","MAE adj","MAE naive"))
t <- up[, .(n=.N, mr=mean(ratio), wk=mean(rA), pd=mean(pred), ac=mean(rB),
            bias=mean(rB-pred), mae_a=mean(abs(rB-pred)), mae_n=mean(abs(rB-rA))), by=bin]
setorder(t, bin)
for (i in 1:nrow(t)) with(t[i], cat(sprintf(
  "%-18s %6d %7.2f %+8.3f %+9.3f %+9.3f %+8.3f %8.3f %8.3f\n",
  as.character(bin), n, mr, wk, pd, ac, bias, mae_a, mae_n)))

cat("\n=== calibration slope: regress ACTUAL on PREDICTED (1.00 = calibrated) ===\n")
m <- lm(rB ~ pred, data=up); ci <- confint(m)["pred",]
cat(sprintf("  slope %.3f  95%% CI [%.3f, %.3f]  intercept %+.4f\n",
    coef(m)["pred"], ci[1], ci[2], coef(m)[1]))
cat(sprintf("  %s\n", ifelse(ci[2] < 1, "slope < 1: predictions too SPREAD -- adjustment under-corrects",
                      ifelse(ci[1] > 1, "slope > 1: adjustment over-corrects", "consistent with calibrated"))))
m0 <- lm(rB ~ rA, data=up)
cat(sprintf("  (unadjusted for comparison: slope %.3f)\n", coef(m0)["rA"]))

cat("\n=== what discount would have been optimal, by bin? ===\n")
cat("   implied = mean(actual)/mean(weak RAA); applied = mean ratio\n")
t2 <- up[abs(rA) > 0.05, .(n=.N, applied=mean(ratio), implied=mean(rB)/mean(rA)), by=bin]
setorder(t2, bin)
for (i in 1:nrow(t2)) with(t2[i], cat(sprintf("  %-18s n=%5d  applied %.2f  implied %+.2f\n",
    as.character(bin), n, applied, implied)))
