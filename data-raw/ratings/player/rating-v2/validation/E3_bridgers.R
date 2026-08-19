# Who has played International (Developing) AND a reference competition, and
# what happened to their scoring when they stepped across? This calibrates the
# 1.60 Developing factor directly, on the players it is meant to describe.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
idmap <- build_player_id_map(conn)
d <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.batter_id AS player_id, %s AS comp, COUNT(*) balls,
         SUM(r.actual_runs) runs, SUM(r.is_wicket) outs,
         AVG(r.raa_run) raa, AVG(r.raa) rvaa, p.player_name
  FROM main.cricsheet_ball_raa r
  JOIN cricsheet.matches m ON m.match_id=r.match_id
  LEFT JOIN cricsheet.players p ON p.player_id=r.batter_id
  WHERE r.format='T20' AND r.gender='male' GROUP BY 1,2,8",
  bouncer:::.competition_sql("t20"))))
canonicalise_player_ids(d, idmap)
nm <- d[!is.na(player_name), .(player_name=player_name[1]), by=player_id]
d <- d[, .(balls=sum(balls), runs=sum(runs), outs=sum(outs),
           raa=weighted.mean(raa, balls), rvaa=weighted.mean(rvaa, balls)),
       by=.(player_id, comp)]

dev <- d[comp == "International (Developing)" & balls >= 40]
ref <- d[comp %in% COMPETITION_REFERENCE_T20,
         .(r_balls=sum(balls), r_runs=sum(runs), r_outs=sum(outs),
           r_raa=weighted.mean(raa, balls)), by=player_id][r_balls >= 40]
j <- merge(dev, ref, by="player_id")
j <- merge(j, nm, by="player_id", all.x=TRUE)
j[, `:=`(avg_dev = runs/pmax(outs,1), avg_ref = r_runs/pmax(r_outs,1), drop = r_raa - raa)]
setorder(j, -balls)
cat(sprintf("Players with 40+ balls in BOTH International (Developing) and a reference\n"))
cat(sprintf("competition: %d. Developing factor currently 1.60.\n\n", nrow(j)))
cat(sprintf("%-22s %7s %7s %8s | %7s %7s %8s | %8s\n",
    "player","dev bls","dev avg","dev RAA","ref bls","ref avg","ref RAA","RAA drop"))
for (i in 1:min(20,nrow(j))) with(j[i], cat(sprintf(
  "%-22s %7d %7.1f %+8.3f | %7d %7.1f %+8.3f | %+8.3f\n",
  substr(ifelse(is.na(player_name),"?",player_name),1,22),
  balls, avg_dev, raa, r_balls, avg_ref, r_raa, drop)))
cat(sprintf("\n--- means over %d bridgers ---\n", nrow(j)))
cat(sprintf("  Developing : %d balls, RAA %+.3f/ball, average %.1f\n",
    sum(j$balls), weighted.mean(j$raa, j$balls), sum(j$runs)/sum(j$outs)))
cat(sprintf("  Reference  : %d balls, RAA %+.3f/ball, average %.1f\n",
    sum(j$r_balls), weighted.mean(j$r_raa, j$r_balls), sum(j$r_runs)/sum(j$r_outs)))
cat(sprintf("  implied factor from averages = %.2f  (currently applied: 1.60)\n",
    (sum(j$runs)/sum(j$outs)) / (sum(j$r_runs)/sum(j$r_outs))))
cat(sprintf("  mean RAA drop stepping across: %+.3f runs/ball\n", weighted.mean(j$drop, pmin(j$balls, j$r_balls))))
