# Why does the rating LOSE by 62.9% in the CSA T20 Challenge?
#
# Leading hypothesis: SA20 launched 2023-01, so South Africa's best players left
# the domestic competition. The merged unit (MiWAY 2012 + Ram Slam 2013-17 +
# CSA T20 Challenge 2016-24) therefore spans a structural break in difficulty
# and is forced to carry ONE factor across it -- and my alias merge may have
# made this worse by removing the separate Ram Slam factor that partly captured
# the earlier era.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)

cat("=== 1. the three names, by season ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT m.event_name, CAST(YEAR(d.match_date) AS INT) AS yr,
         COUNT(DISTINCT d.match_id) AS matches,
         ROUND(AVG(d.runs_total),3) AS runs_per_ball
  FROM cricsheet.deliveries d JOIN cricsheet.matches m ON m.match_id=d.match_id
  WHERE m.event_name IN ('MiWAY T20 Challenge','Ram Slam T20 Challenge','CSA T20 Challenge')
    AND d.gender='male'
  GROUP BY 1,2 ORDER BY 2"))

cat("\n=== 2. did the competition get WEAKER when SA20 launched (2023-01)? ===\n")
cat("    proxy: share of its players who also appear in a major league that season\n")
print(DBI::dbGetQuery(conn, "
  WITH csa AS (
    SELECT DISTINCT CAST(YEAR(d.match_date) AS INT) AS yr, d.batter_id
    FROM cricsheet.deliveries d JOIN cricsheet.matches m ON m.match_id=d.match_id
    WHERE m.event_name IN ('MiWAY T20 Challenge','Ram Slam T20 Challenge','CSA T20 Challenge')
      AND d.gender='male'),
  maj AS (
    SELECT DISTINCT CAST(YEAR(d.match_date) AS INT) AS yr, d.batter_id
    FROM cricsheet.deliveries d JOIN cricsheet.matches m ON m.match_id=d.match_id
    WHERE m.event_name IN ('Indian Premier League','Big Bash League','SA20',
                           'Pakistan Super League','Caribbean Premier League',
                           'International League T20') AND d.gender='male')
  SELECT c.yr, COUNT(*) AS csa_players,
         SUM(CASE WHEN mj.batter_id IS NOT NULL THEN 1 ELSE 0 END) AS also_major,
         ROUND(100.0*SUM(CASE WHEN mj.batter_id IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),1) AS pct
  FROM csa c LEFT JOIN maj mj ON mj.yr=c.yr AND mj.batter_id=c.batter_id
  GROUP BY 1 ORDER BY 1"))

cat("\n=== 3. the fitted factor, merged vs per-name ===\n")
id_map <- build_player_id_map(conn)
f <- as.data.table(suppressMessages(fit_competition_factors(conn,"t20","male",id_map=id_map)))
print(f[comp %in% c("CSA T20 Challenge","SA20","Mzansi Super League")])
cat("\n  (before the merge these were three separate competitions with their own\n")
cat("   factors; now one factor spans 2012-2024 including the SA20 break)\n")
