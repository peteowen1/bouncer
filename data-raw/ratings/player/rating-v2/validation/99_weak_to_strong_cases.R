# Players with 100+ balls in BOTH a weak league and a strong competition.
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
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  LEFT JOIN cricsheet.players p ON p.player_id=r.batter_id
  WHERE r.format='T20' AND r.gender='male'", bouncer:::.competition_sql("t20"))))
idmap <- build_player_id_map(conn); canonicalise_player_ids(b, idmap)
nm <- b[!is.na(player_name), .(player_name=player_name[1]), by=batter_id]
b[, cf := fmap[comp]]; b <- b[!is.na(cf)]
pc <- b[, .(balls=.N, raa=mean(raa), runs=sum(actual_runs)), by=.(batter_id, comp)]
pc[, cf := fmap[comp]]
weak   <- pc[cf >= 1.30 & balls >= 100]
strong <- pc[cf <= 1.05 & balls >= 100]
j <- merge(weak[,.(batter_id, wcomp=comp, wcf=cf, wb=balls, wraa=raa)],
           strong[,.(batter_id, scomp=comp, scf=cf, sb=balls, sraa=raa)], by="batter_id")
j <- merge(j, nm, by="batter_id")
j[, pred := wraa * (scf/wcf)][, drop := sraa - wraa]
setorder(j, -wraa)
cat(sprintf("%d cases with 100+ balls in BOTH a weak league (factor>=1.30) and a strong one (<=1.05)\n", nrow(j)))
cat("T20 men. RAA in RUNS PER BALL.\n\n")
cat(sprintf("%-19s %-27s %5s %7s %5s | %-24s %5s %7s | %7s %7s\n",
  "player","weak league","balls","RAA","fact","strong competition","balls","RAA","pred","actual drop"))
for (i in 1:min(20, nrow(j))) with(j[i], cat(sprintf(
  "%-19s %-27s %5d %+7.3f %5.2f | %-24s %5d %+7.3f | %+7.3f %+7.3f\n",
  substr(player_name,1,19), substr(wcomp,1,27), wb, wraa, wcf,
  substr(scomp,1,24), sb, sraa, pred, drop)))
cat(sprintf("\nof these %d: mean weak RAA %+.3f, mean strong RAA %+.3f, mean drop %+.3f\n",
    nrow(j), mean(j$wraa), mean(j$sraa), mean(j$drop)))
cat(sprintf("  mean factor-predicted value %+.3f  vs actual %+.3f  -> factors %s\n",
    mean(j$pred), mean(j$sraa),
    ifelse(mean(j$sraa) < mean(j$pred), "UNDER-discount", "OVER-discount")))
cat(sprintf("  MAE naive %.3f  MAE adjusted %.3f  gain %+.1f%%\n",
    mean(abs(j$sraa-j$wraa)), mean(abs(j$sraa-j$pred)),
    100*(mean(abs(j$sraa-j$wraa))-mean(abs(j$sraa-j$pred)))/mean(abs(j$sraa-j$wraa))))
