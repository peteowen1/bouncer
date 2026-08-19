setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
fac <- as.data.table(fit_competition_factors(conn, "t20", "male"))
fmap <- setNames(fac$factor, fac$comp)
d <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT %s AS comp, COUNT(*) balls, SUM(r.actual_runs) runs, SUM(r.is_wicket) outs,
         AVG(r.raa) rvaa
  FROM main.cricsheet_ball_raa r
  JOIN cricsheet.matches m ON m.match_id=r.match_id
  JOIN cricsheet.players p ON p.player_id=r.batter_id
  WHERE r.format='T20' AND r.gender='male' AND p.player_name='Karanbir Singh'
  GROUP BY 1 ORDER BY balls DESC", bouncer:::.competition_sql("t20"))))
d[, factor := fmap[comp]]
d[, ref := comp %in% COMPETITION_REFERENCE_T20]
cat("Karanbir Singh -- every competition, with CURRENT factors (T20 men)\n\n")
cat(sprintf("%-38s %6s %6s %5s %7s %8s %9s %5s\n",
    "competition","balls","runs","outs","avg","RVAA/bl","factor","ref?"))
for (i in 1:nrow(d)) with(d[i], cat(sprintf("%-38s %6d %6d %5d %7.1f %+8.3f %9s %5s\n",
    substr(comp,1,38), balls, runs, outs, runs/pmax(outs,1), rvaa,
    ifelse(is.na(factor),"unrated",sprintf("%.2f",factor)), ifelse(ref,"YES","no"))))
cat(sprintf("\n  competitions played: %d   distinct rated: %d   reference balls: %d\n",
    nrow(d), d[!is.na(factor), .N], d[ref==TRUE, sum(balls)]))
cat(sprintf("  total %d balls, %d runs, %d dismissals, average %.1f, SR %.1f\n",
    sum(d$balls), sum(d$runs), sum(d$outs), sum(d$runs)/max(sum(d$outs),1),
    100*sum(d$runs)/sum(d$balls)))

cat("\n=== how do the top 25 compare on NUMBER of competitions? ===\n")
idmap <- build_player_id_map(conn)
a <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.batter_id AS player_id, %s AS comp, COUNT(*) balls
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  WHERE r.format='T20' AND r.gender='male' GROUP BY 1,2",
  bouncer:::.competition_sql("t20"))))
canonicalise_player_ids(a, idmap)
a <- a[, .(balls=sum(balls)), by=.(player_id, comp)]
n <- a[balls >= 30, .(ncomp = .N), by=player_id]
r <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT rank, player_id, player_name FROM main.player_rating_v2
  WHERE format='T20' AND gender='male' AND role='batter' ORDER BY rating DESC LIMIT 25"))
r <- merge(r, n, by="player_id", all.x=TRUE); setorder(r, rank)
cat(sprintf("  median competitions (>=30 balls) across the top 25: %.0f\n", median(r$ncomp, na.rm=TRUE)))
cat(sprintf("  Karanbir Singh: %d competitions\n", r[grepl("Karanbir", player_name), ncomp]))
cat(sprintf("  range across top 25: %d to %d\n", min(r$ncomp,na.rm=TRUE), max(r$ncomp,na.rm=TRUE)))
