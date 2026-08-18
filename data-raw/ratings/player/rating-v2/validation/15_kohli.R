suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

f <- find_player("V Kohli", conn = conn, quiet = TRUE)
id <- f$player_id[1]

cat("=== Kohli: last appearance by format (is he still playing Tests?) ===\n")
print(DBI::dbGetQuery(conn, sprintf("
  SELECT LOWER(match_type) AS fmt, COUNT(DISTINCT match_id) AS matches,
         MIN(match_date) AS first_match, MAX(match_date) AS last_match
  FROM cricsheet.deliveries
  WHERE batter_id = '%s' GROUP BY 1 ORDER BY last_match DESC", id)))

cat("\n=== Test batting by calendar year, 2018 onward ===\n")
print(DBI::dbGetQuery(conn, sprintf("
  SELECT CAST(YEAR(match_date) AS INT) AS yr,
         COUNT(DISTINCT match_id) AS tests,
         SUM(runs_batter) AS runs,
         SUM(CASE WHEN player_out_id = batter_id THEN 1 ELSE 0 END) AS outs,
         ROUND(SUM(runs_batter)*1.0/NULLIF(SUM(CASE WHEN player_out_id=batter_id THEN 1 ELSE 0 END),0),1) AS avg
  FROM cricsheet.deliveries
  WHERE batter_id='%s' AND LOWER(match_type)='test' AND YEAR(match_date) >= 2018
  GROUP BY 1 ORDER BY 1", id)))

cat("\n=== how far back is his last Test from the rating's as_at (2026-08-02)? ===\n")
lst <- DBI::dbGetQuery(conn, sprintf("
  SELECT MAX(match_date) AS d FROM cricsheet.deliveries
  WHERE batter_id='%s' AND LOWER(match_type)='test'", id))$d
cat("  last Test:", format(lst), "  days before as_at:",
    as.integer(as.Date("2026-08-02") - as.Date(lst)), "\n")
cat("  decay half-life is 1095 days, so weight now ~",
    sprintf("%.2f", exp(-as.numeric(as.Date("2026-08-02") - as.Date(lst)) / 1095)), "\n")
