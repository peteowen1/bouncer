suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
library(DBI)
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

cat("=== balls per innings, by format (the quantity lambda should scale with) ===\n")
print(dbGetQuery(conn, "
  SELECT match_type,
         COUNT(*) AS innings,
         ROUND(AVG(balls), 1)  AS mean_balls,
         ROUND(MEDIAN(balls),1) AS median_balls,
         ROUND(AVG(runs), 1)   AS mean_runs,
         ROUND(AVG(wkts), 2)   AS mean_wkts,
         ROUND(AVG(runs) / NULLIF(AVG(wkts),0), 1) AS runs_per_wkt
  FROM (
    SELECT match_id, innings, match_type,
           COUNT(*) AS balls,
           SUM(runs_total) AS runs,
           SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS wkts
    FROM cricsheet.deliveries
    WHERE gender='male' AND match_type IN ('T20','ODI','Test','MDM')
    GROUP BY match_id, innings, match_type
  )
  GROUP BY match_type ORDER BY mean_balls"))

cat("\n=== Test+MDM male: the actual rating pool ===\n")
print(dbGetQuery(conn, "
  SELECT COUNT(DISTINCT match_id) AS matches, COUNT(*) AS deliveries,
         MIN(match_date) AS from_date, MAX(match_date) AS to_date
  FROM cricsheet.deliveries
  WHERE match_type IN ('Test','MDM') AND gender='male'"))

cat("\n=== outcome distribution, Test+MDM male (the draw problem, sized) ===\n")
print(dbGetQuery(conn, "
  SELECT match_type, outcome_type, COUNT(*) AS matches,
         ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (PARTITION BY match_type), 1) AS pct
  FROM cricsheet.matches
  WHERE match_type IN ('Test','MDM') AND gender='male'
  GROUP BY 1,2 ORDER BY 1,2"))

cat("\n=== does a 4th innings exist often enough to fit a chase surface? ===\n")
print(dbGetQuery(conn, "
  SELECT innings, COUNT(DISTINCT match_id) AS matches, COUNT(*) AS deliveries
  FROM cricsheet.deliveries
  WHERE match_type IN ('Test','MDM') AND gender='male'
  GROUP BY innings ORDER BY innings"))
