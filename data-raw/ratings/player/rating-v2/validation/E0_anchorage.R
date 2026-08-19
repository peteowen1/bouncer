# Is Karanbir's rank a shrinkage problem or a factor problem? Neither, if his
# rating rests on cricket that never touches the reference scale: then it is an
# EXTRAPOLATION with no anchor, and no amount of tuning either knob fixes it.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
idmap <- build_player_id_map(conn)
d <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.batter_id AS player_id, %s AS comp, COUNT(*) balls
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  WHERE r.format='T20' AND r.gender='male' GROUP BY 1,2",
  bouncer:::.competition_sql("t20"))))
canonicalise_player_ids(d, idmap)
d <- d[, .(balls=sum(balls)), by=.(player_id, comp)]
d[, ref := comp %in% COMPETITION_REFERENCE_T20]
ex <- d[, .(total = sum(balls), ref_balls = sum(balls[ref])), by=player_id]
ex[, ref_share := ref_balls/total]

r <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT rank, player_id, player_name, rating, matches, balls
  FROM main.player_rating_v2 WHERE format='T20' AND gender='male' AND role='batter'
  ORDER BY rating DESC LIMIT 40"))
r <- merge(r, ex, by="player_id", all.x=TRUE)
setorder(r, rank)
cat("Reference competitions = IPL, BBL, PSL, SA20, CPL, ILT20, T20 World Cup, Vitality Blast.\n")
cat("A player with 0 reference balls is ranked purely by extrapolation.\n\n")
cat(sprintf("%-4s %-22s %8s %8s %10s %9s\n","#","player","rating","balls","ref balls","ref share"))
for (i in 1:25) with(r[i], cat(sprintf("%-4d %-22s %8.3f %8s %10s %8.0f%%\n",
    rank, substr(player_name,1,22), rating, format(balls, big.mark=","),
    format(ifelse(is.na(ref_balls),0,ref_balls), big.mark=","),
    100*ifelse(is.na(ref_share),0,ref_share))))
cat(sprintf("\ntop 25 with ZERO reference-competition balls: %d\n", r[1:25][is.na(ref_balls) | ref_balls == 0, .N]))
cat(sprintf("top 25 with under 10%% reference exposure   : %d\n", r[1:25][is.na(ref_share) | ref_share < 0.10, .N]))
cat(sprintf("median reference share across the top 25   : %.0f%%\n", 100*median(r[1:25]$ref_share, na.rm=TRUE)))
