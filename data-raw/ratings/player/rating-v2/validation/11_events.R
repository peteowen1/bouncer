suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

cat("=== how much of the Test+MDM pool even HAS an event_name? ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT LOWER(m.match_type) AS mt,
         SUM(CASE WHEN m.event_name IS NULL THEN 1 ELSE 0 END) AS null_event,
         COUNT(*) AS matches,
         ROUND(100.0*SUM(CASE WHEN m.event_name IS NULL THEN 1 ELSE 0 END)/COUNT(*),1) AS pct_null
  FROM cricsheet.matches m
  WHERE LOWER(m.match_type) IN ('test','mdm') AND m.gender='male'
  GROUP BY 1"))
cat("  fit_competition_factors() filters event_name IS NOT NULL, so a high null\n")
cat("  share would silently shrink the pool it can rate.\n")

cat("\n=== TEST event_names by balls (top 25) ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT m.event_name, COUNT(DISTINCT d.match_id) AS matches, COUNT(*) AS balls
  FROM cricsheet.deliveries d JOIN cricsheet.matches m ON m.match_id=d.match_id
  WHERE LOWER(d.match_type)='test' AND d.gender='male' AND m.event_name IS NOT NULL
  GROUP BY 1 ORDER BY balls DESC LIMIT 25"))

cat("\n=== MDM event_names by balls (all) ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT m.event_name, COUNT(DISTINCT d.match_id) AS matches, COUNT(*) AS balls
  FROM cricsheet.deliveries d JOIN cricsheet.matches m ON m.match_id=d.match_id
  WHERE LOWER(d.match_type)='mdm' AND d.gender='male' AND m.event_name IS NOT NULL
  GROUP BY 1 ORDER BY balls DESC"))

cat("\n=== share of Test+MDM balls carried by Test vs MDM ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT LOWER(match_type) AS mt, COUNT(*) AS balls,
         ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),1) AS pct
  FROM cricsheet.deliveries
  WHERE LOWER(match_type) IN ('test','mdm') AND gender='male'
  GROUP BY 1"))
