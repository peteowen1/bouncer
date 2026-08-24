# Before/after the competition-offset switch: T20 men's batting top 25, plus
# the two checks that say whether the sign defect is actually gone.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table); library(arrow)})
SP <- "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad"
conn <- dbConnect(duckdb::duckdb(), dbdir = file.path(find_bouncerdata_dir(), "bouncer.duckdb"),
                  read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

before <- as.data.table(read_parquet(file.path(SP, "before_rating_v2.parquet")))
after  <- as.data.table(dbGetQuery(conn, "SELECT * FROM main.player_rating_v2"))
FMT <- "T20"; GEN <- "male"; ROLE <- "batter"
bb <- before[format == FMT & gender == GEN & role == ROLE, .(player_id, r0 = rating, k0 = rank)]
aa <- after [format == FMT & gender == GEN & role == ROLE, .(player_id, r1 = rating, k1 = rank)]
cmp <- merge(aa, bb, by = "player_id", all.x = TRUE)

idmap <- build_player_id_map(conn)
ctx <- as.data.table(dbGetQuery(conn, sprintf("
  SELECT r.batter_id AS player_id, %s AS comp, COUNT(*) balls,
         SUM(r.actual_runs) runs, SUM(r.is_wicket) outs,
         MODE(d.batting_team) team, MAX(p.player_name) player_name
  FROM main.cricsheet_ball_raa r
  JOIN cricsheet.matches m ON m.match_id = r.match_id
  JOIN cricsheet.deliveries d ON d.delivery_id = r.delivery_id
  LEFT JOIN cricsheet.players p ON p.player_id = r.batter_id
  WHERE r.format='T20' AND r.gender='male' GROUP BY 1,2", .competition_sql("t20"))))
canonicalise_player_ids(ctx, idmap)
ctx <- ctx[, .(balls = sum(balls), runs = sum(runs), outs = sum(outs),
               team = team[which.max(balls)], player_name = player_name[1]),
           by = .(player_id, comp)]
tot <- ctx[, .(balls = sum(balls), avg = sum(runs) / pmax(sum(outs), 1),
               modal = comp[which.max(balls)],
               country = team[which.max(balls)],
               name = player_name[which(!is.na(player_name))[1]],
               ref_share = sum(balls[comp %in% COMPETITION_REFERENCE_T20]) / sum(balls)),
           by = player_id]
cmp <- merge(cmp, tot, by = "player_id", all.x = TRUE)
setorder(cmp, k1)

cat("=== T20 MEN'S BATTING, top 25 after the switch to a competition OFFSET ===\n")
cat("Rating is reference-equivalent RVAA per match. 'was' is the divisive-factor\n")
cat("rating this replaces. 'ref%' is the share of the player's balls in the\n")
cat("reference leagues (IPL, BBL, PSL, SA20, CPL, ILT20, Vitality Blast, T20 WC).\n\n")
cat(sprintf("%-4s %-20s %-13s %-22s %6s %6s %7s %7s %5s %5s\n",
    "rank", "player", "country", "modal league", "balls", "avg", "rating", "was", "wasR", "ref%"))
for (i in 1:25) with(cmp[i], cat(sprintf(
  "%-4d %-20s %-13s %-22s %6d %6.1f %+7.3f %+7.3f %5s %4.0f%%\n",
  k1, substr(ifelse(is.na(name), "?", name), 1, 20), substr(country, 1, 13),
  substr(modal, 1, 22), balls, avg, r1, r0, k0, 100 * ref_share)))

cat("\n=== movement ===\n")
cmp[, move := k0 - k1]
big <- cmp[!is.na(k0)][order(-abs(move))][1:10]
cat(sprintf("%-22s %-22s %6s %6s %7s %6s\n", "player", "modal league", "was", "now", "move", "ref%"))
for (i in 1:nrow(big)) with(big[i], cat(sprintf("%-22s %-22s %6d %6d %+7d %5.0f%%\n",
  substr(ifelse(is.na(name), "?", name), 1, 22), substr(modal, 1, 22), k0, k1, move,
  100 * ref_share)))

k <- cmp[grepl("Karanbir", name)]
cat("\n=== Karanbir Singh ===\n")
if (nrow(k)) for (i in 1:nrow(k)) with(k[i], cat(sprintf(
  "  rank %d (was %s), rating %+.3f (was %+.3f), %d balls, average %.1f, %.0f%% reference\n",
  k1, k0, r1, r0, balls, avg, 100 * ref_share))) else cat("  not in the rated pool\n")

cat(sprintf("\nSpearman between the old and new orderings: %.4f over %d players\n",
    cor(cmp$k0, cmp$k1, method = "spearman", use = "complete.obs"), sum(!is.na(cmp$k0))))
