# Concrete: players with real volume in a WEAK league who then played a STRONG
# competition. Show unadjusted RAA in the weak league, what the factor predicts
# for the strong one, and what actually happened.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)

fac <- fit_competition_factors(conn, "t20", "male")
fmap <- setNames(fac$factor, fac$comp)
b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.batter_id, r.raa, r.actual_runs, %s AS comp, p.player_name
  FROM main.cricsheet_ball_raa r
  JOIN cricsheet.matches m ON m.match_id=r.match_id
  LEFT JOIN cricsheet.players p ON p.player_id=r.batter_id
  WHERE r.format='T20' AND r.gender='male'", bouncer:::.competition_sql("t20"))))
idmap <- build_player_id_map(conn); canonicalise_player_ids(b, idmap)
nm <- b[!is.na(player_name), .(player_name = player_name[1]), by=batter_id]
b[, cf := fmap[comp]]; b <- b[!is.na(cf)]

STRONG <- b[cf <= 1.05, unique(comp)]
pc <- b[, .(balls=.N, raa=mean(raa), runs=sum(actual_runs)), by=.(batter_id, comp)]
pc[, cf := fmap[comp]]
weak   <- pc[cf >= 1.35 & balls >= 100]
strong <- pc[comp %in% STRONG & balls >= 40]
j <- merge(weak[, .(batter_id, wcomp=comp, wcf=cf, wballs=balls, wraa=raa)],
           strong[, .(batter_id, scomp=comp, scf=cf, sballs=balls, sraa=raa)], by="batter_id")
j <- merge(j, nm, by="batter_id")
j[, pred_adj := wraa * (scf/wcf)]
j[, err_naive := abs(sraa - wraa)][, err_adj := abs(sraa - pred_adj)]
setorder(j, -wraa)

cat(sprintf("%d weak-league -> strong-competition moves (>=100 balls weak, >=40 strong)\n\n", nrow(j)))
cat("T20 men. All RAA figures are RUNS PER BALL.\n\n")
cat(sprintf("%-20s %-26s %5s %7s %6s | %-22s %5s %7s | %7s %7s\n",
    "player","weak league","balls","RAA","factor","strong comp","balls","RAA","pred adj","better?"))
show <- rbind(head(j[wraa > 0], 8), head(j[wraa < 0], 6))
for (i in 1:nrow(show)) with(show[i], cat(sprintf(
  "%-20s %-26s %5d %+7.3f %6.2f | %-22s %5d %+7.3f | %+7.3f %7s\n",
  substr(player_name,1,20), substr(wcomp,1,26), wballs, wraa, wcf,
  substr(scomp,1,22), sballs, sraa, pred_adj,
  ifelse(err_adj < err_naive, "ADJ", "naive"))))

cat("\n=== does the adjustment help, split by whether the player was ABOVE or BELOW average in the weak league? ===\n")
for (grp in c("above", "below")) {
  s <- if (grp=="above") j[wraa > 0] else j[wraa < 0]
  if (!nrow(s)) next
  cat(sprintf("  %-6s average in weak league (n=%3d): naive MAE %.3f  adjusted MAE %.3f  gain %+6.1f%%  adj better in %d of %d\n",
      grp, nrow(s), mean(s$err_naive), mean(s$err_adj),
      100*(mean(s$err_naive)-mean(s$err_adj))/mean(s$err_naive),
      s[err_adj < err_naive, .N], nrow(s)))
}
cat("\n  Multiplying a NEGATIVE RAA by a shrink factor moves it toward zero --\n")
cat("  i.e. it predicts a below-average weak-league player gets BETTER when\n")
cat("  they step up. That is the mis-specification.\n")
