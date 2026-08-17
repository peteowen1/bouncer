# The pre-declared RAA validations for Test.
#
# NOTE ON WHAT IS BEING TESTED: this is RAA per ball, with NO opponent
# adjustment and NO competition factor. The pool is 886 Tests against 2,161
# domestic first-class matches, so domestic players are EXPECTED to dominate a
# raw RAA leaderboard -- that is the same shape as Karanbir Singh in T20 before
# the competition discount, and it is what steps 6-7 exist to fix. Recorded here
# so a domestic-heavy top 20 is read as "the adjustment is not applied yet",
# not as "the rating is broken".
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})

DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

cat("=== V1. mean RAA ~ 0 and the pot is NOT fixed ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT ROUND(AVG(raa),5) AS mean_raa, ROUND(STDDEV(raa),3) AS sd_raa,
         ROUND(MIN(raa),1) AS min_raa, ROUND(MAX(raa),1) AS max_raa
  FROM main.cricsheet_ball_raa WHERE format='TEST' AND gender='male'"))
pot <- DBI::dbGetQuery(conn, "
  SELECT ROUND(STDDEV(m),3) AS sd_match_total, ROUND(AVG(m),3) AS mean_match_total
  FROM (SELECT match_id, SUM(raa) AS m FROM main.cricsheet_ball_raa
        WHERE format='TEST' AND gender='male' GROUP BY match_id)")
print(pot)
cat("  sd of per-match RAA total must be >> 0: a fixed pot would force it to 0.\n")

cat("\n=== V2. batting RAA aggregated per batter (>= 3000 balls) ===\n")
b <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT r.batter_id, COUNT(*) AS balls, SUM(r.raa) AS raa_total,
         AVG(r.raa) AS raa_per_ball, SUM(r.actual_runs) AS runs,
         SUM(CASE WHEN r.is_wicket THEN 1 ELSE 0 END) AS outs,
         SUM(CASE WHEN LOWER(d.match_type)='test' THEN 1 ELSE 0 END) AS test_balls
  FROM main.cricsheet_ball_raa r
  JOIN cricsheet.deliveries d ON d.delivery_id = r.delivery_id
  WHERE r.format='TEST' AND r.gender='male'
  GROUP BY r.batter_id HAVING COUNT(*) >= 3000"))
setorder(b, -raa_per_ball)
b[, rank := .I]
b[, test_share := round(test_balls / balls, 3)]
b[, avg := round(runs / pmax(outs, 1), 1)]
b[, raa_100 := round(100 * raa_per_ball, 2)]

nm <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT player_id AS batter_id, ANY_VALUE(player_name) AS player
  FROM cricsheet.players GROUP BY player_id"))
b <- merge(b, nm, by = "batter_id", all.x = TRUE)
setorder(b, rank)
cat(sprintf("  qualifying batters: %d\n\n", nrow(b)))
print(b[1:20, .(rank, player, raa_100, avg, balls, test_share)])

cat("\n=== V3. ANCHOR CHECK (pre-declared: Root, Kohli, Smith, Williamson) ===\n")
cat("  Declared for the FINAL rating, checked here on raw RAA as a diagnostic.\n")
for (q in c("JE Root", "V Kohli", "SPD Smith", "KS Williamson")) {
  f <- tryCatch(find_player(q, conn = conn, quiet = TRUE), error = function(e) NULL)
  if (is.null(f) || !nrow(f)) { cat(sprintf("  %-16s NOT FOUND\n", q)); next }
  id <- f$player_id[1]
  r <- b[batter_id == id]
  if (!nrow(r)) { cat(sprintf("  %-16s below the 3000-ball threshold\n", q)); next }
  cat(sprintf("  %-16s rank %4d / %d   raa/100 %+6.2f   avg %5.1f   test_share %.2f\n",
              q, r$rank, nrow(b), r$raa_100, r$avg, r$test_share))
}

cat("\n=== V4. face validity: RAA vs batting average (should be positive, not 1) ===\n")
cat(sprintf("  Spearman(raa_per_ball, avg) = %.3f  (n=%d)\n",
            cor(b$raa_per_ball, b$avg, method = "spearman"), nrow(b)))
cat("  A correlation near 1 would mean RAA adds nothing over the traditional number.\n")

cat("\n=== V5. does raw RAA favour DOMESTIC cricket? (the expected failure) ===\n")
b[, pool := ifelse(test_share >= 0.5, "mostly Test", "mostly domestic")]
print(b[, .(players = .N, mean_raa_100 = round(100*mean(raa_per_ball), 2),
            mean_avg = round(mean(avg), 1)), by = pool])
cat(sprintf("\n  top 20 that is mostly-domestic: %d of 20\n",
            b[1:20, sum(pool == "mostly domestic")]))
cat("  This is the competition-factor gap, not a lambda problem.\n")

cat("\n=== V6. position spread: does RAA flatten the batting-order bias in raw runs? ===\n")
ps <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT CASE WHEN r.over_number < 10 THEN 'overs 0-9'
              WHEN r.over_number < 40 THEN 'overs 10-39'
              WHEN r.over_number < 80 THEN 'overs 40-79'
              ELSE 'overs 80+' END AS phase,
         COUNT(*) AS balls,
         ROUND(AVG(r.actual_runs),4) AS mean_runs,
         ROUND(AVG(r.raa),4) AS mean_raa
  FROM main.cricsheet_ball_raa r
  WHERE r.format='TEST' AND r.gender='male' GROUP BY 1 ORDER BY 1"))
print(ps)
cat(sprintf("  spread in mean_runs %.4f  vs in mean_raa %.4f  (RAA should be far flatter)\n",
            diff(range(ps$mean_runs)), diff(range(ps$mean_raa))))
