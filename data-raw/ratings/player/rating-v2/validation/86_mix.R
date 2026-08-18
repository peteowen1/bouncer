# How much can a WITHIN-COMPETITION metric see the competition factors?
# A factor is a constant divisor inside its own competition, so it can only move
# a within-competition ranking through players whose rating mixes competitions.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)

b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.match_id, r.match_date, r.batter_id, %s AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  WHERE r.format='T20' AND r.gender='male'", bouncer:::.competition_sql("t20"))))
idmap <- build_player_id_map(conn); canonicalise_player_ids(b, idmap)
pm <- unique(b[, .(player_id=batter_id, match_id, match_date, comp)])
setorder(pm, player_id, match_date)

DECAY <- 1095
ref <- max(pm$match_date)
pm[, w := exp(-as.numeric(ref - match_date)/DECAY)]

# per player: decay-weighted share of history in their single biggest competition
sh <- pm[, .(w = sum(w)), by=.(player_id, comp)]
tot <- sh[, .(tw = sum(w), top = max(w), ncomp = .N), by=player_id]
tot[, top_share := top/tw]

cat(sprintf("T20 men: %s players with any history\n\n", format(nrow(tot), big.mark=",")))
cat("=== decay-weighted share of a player's history in their MAIN competition ===\n")
print(round(quantile(tot$top_share, c(0,.1,.25,.5,.75,.9,1)), 3))
cat(sprintf("\n  players whose history is 100%% one competition : %s (%.1f%%)\n",
    format(tot[top_share > 0.999, .N], big.mark=","), 100*tot[top_share>0.999,.N]/nrow(tot)))
cat(sprintf("  players with >=90%% in one competition         : %s (%.1f%%)\n",
    format(tot[top_share >= 0.90, .N], big.mark=","), 100*tot[top_share>=0.90,.N]/nrow(tot)))
cat(sprintf("  median number of competitions per player       : %.0f\n", median(tot$ncomp)))

cat("\n=== weighted by exposure: whose ratings actually carry weight? ===\n")
tot[, wt := tw]
cat(sprintf("  exposure-weighted mean top-competition share   : %.3f\n",
    tot[, sum(top_share*wt)/sum(wt)]))
cat(sprintf("  exposure-weighted share of players 100%% single : %.1f%%\n",
    100*tot[top_share>0.999, sum(wt)]/tot[, sum(wt)]))

cat("\n=== for the four largest competitions: of players appearing there,\n")
cat("    what share of their history is elsewhere? ===\n")
big <- sh[, .(w=sum(w)), by=comp][order(-w)][1:4]
for (cp in big$comp) {
  ids <- sh[comp==cp, player_id]
  x <- merge(tot[player_id %in% ids, .(player_id, tw)],
             sh[comp==cp & player_id %in% ids, .(player_id, w_here=w)], by="player_id")
  x[, elsewhere := 1 - w_here/tw]
  cat(sprintf("  %-32s n=%4d  mean elsewhere %.1f%%  median %.1f%%\n",
      substr(cp,1,32), nrow(x), 100*mean(x$elsewhere), 100*median(x$elsewhere)))
}
