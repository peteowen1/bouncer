# Is rating_diff partly measuring squad COVERAGE rather than squad quality?
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown=TRUE), add=TRUE)
types <- list(t20="'t20','it20'", odi="'odi','odm'", test="'test','mdm'")
for (fmt in c("t20","odi","test")) {
  snaps <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT as_at, player_id, bat_value, bowl_value FROM main.player_value_v2_snapshots
    WHERE format='%s' AND gender='male'", fmt)))
  sd_dates <- sort(unique(as.Date(snaps$as_at)))
  m <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT m.match_id, CAST(m.match_date AS DATE) match_date,
           (SELECT MIN(batting_team) FROM cricsheet.match_innings i
            WHERE i.match_id=m.match_id AND i.innings=1) bat_first, m.team1, m.team2
    FROM cricsheet.matches m WHERE LOWER(m.match_type) IN (%s) AND m.gender='male'
      AND m.unified_margin IS NOT NULL AND m.unified_margin <> 0", types[[fmt]])))
  m <- m[!is.na(bat_first)]; m[, snap := pick_snapshot(match_date, sd_dates)]
  m <- m[!is.na(snap)]
  app <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT match_id, team, player_id FROM (
      SELECT match_id, batting_team team, batter_id player_id FROM cricsheet.deliveries
      WHERE LOWER(match_type) IN (%s) AND gender='male'
      UNION
      SELECT match_id, bowling_team, bowler_id FROM cricsheet.deliveries
      WHERE LOWER(match_type) IN (%s) AND gender='male')", types[[fmt]], types[[fmt]])))
  app <- app[match_id %in% m$match_id]
  app <- merge(app, m[, .(match_id, snap)], by="match_id")
  app <- merge(app, snaps[, .(snap=as.Date(as_at), player_id, bat_value, bowl_value)],
               by=c("snap","player_id"), all.x=TRUE)
  side <- app[, .(s = sum(bat_value + bowl_value, na.rm=TRUE),
                  n = sum(!is.na(bat_value))), by=.(match_id, team)]
  side <- side[n >= 6]
  cat(sprintf("\n%s: %d sides, players rated per side mean %.1f sd %.2f range %d-%d\n",
      toupper(fmt), nrow(side), mean(side$n), sd(side$n), min(side$n), max(side$n)))
  cat(sprintf("  cor(team rating sum, n rated) = %+.3f\n", cor(side$s, side$n)))
  cat(sprintf("  cor(team rating MEAN, n rated) = %+.3f\n", cor(side$s/side$n, side$n)))
}
