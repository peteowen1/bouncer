setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
f <- as.data.table(fit_competition_factors(conn, "t20", "male"))
his <- c("International (Developing)","ICC Qualifying Pathway","Central Europe Cup",
         "Budapest Cup","Viking Cup","Continental Cup","ECA Men's European Cup")
cat("step 0 = fitted DIRECTLY against the reference set (IPL, BBL, PSL, SA20,\n")
cat("         CPL, ILT20, T20 World Cup, Vitality Blast)\n")
cat("step 1+ = fitted against an already-rated competition, so the factor is a\n")
cat("         PRODUCT of estimates and error compounds along the chain\n\n")
cat(sprintf("%-38s %8s %6s %9s\n","competition","factor","step","bridges"))
for (c in his) {
  r <- f[comp == c]
  if (nrow(r)) cat(sprintf("%-38s %8.2f %6d %9s\n", substr(c,1,38), r$factor[1], r$step[1],
      ifelse(is.na(r$n_bridges[1]),"-",r$n_bridges[1])))
  else cat(sprintf("%-38s %8s\n", substr(c,1,38), "UNRATED"))
}
cat("\n=== the fit as a whole ===\n")
t <- f[, .(competitions=.N, median_factor=round(median(factor),2),
           min=round(min(factor),2), max=round(max(factor),2)), by=step][order(step)]
print(t, row.names=FALSE)
cat("\n=== how much of the top 25's exposure is to step-0 vs chained competitions? ===\n")
idmap <- build_player_id_map(conn)
a <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.batter_id AS player_id, %s AS comp, COUNT(*) balls
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  WHERE r.format='T20' AND r.gender='male' GROUP BY 1,2",
  bouncer:::.competition_sql("t20"))))
canonicalise_player_ids(a, idmap)
a <- merge(a[, .(balls=sum(balls)), by=.(player_id, comp)], f[, .(comp, step)], by="comp", all.x=TRUE)
ex <- a[, .(total=sum(balls), direct=sum(balls[!is.na(step) & step==0])), by=player_id]
ex[, direct_share := direct/total]
r <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT rank, player_id, player_name FROM main.player_rating_v2
  WHERE format='T20' AND gender='male' AND role='batter' ORDER BY rating DESC LIMIT 25"))
r <- merge(r, ex, by="player_id", all.x=TRUE); setorder(r, rank)
cat(sprintf("  median share of balls in DIRECTLY-rated competitions, top 25: %.0f%%\n",
    100*median(r$direct_share, na.rm=TRUE)))
cat(sprintf("  Karanbir Singh: %.0f%%\n", 100*r[grepl("Karanbir", player_name), direct_share]))
cat(sprintf("  lowest in the top 25: %.0f%% (%s)\n",
    100*min(r$direct_share, na.rm=TRUE), r[which.min(direct_share), player_name]))
