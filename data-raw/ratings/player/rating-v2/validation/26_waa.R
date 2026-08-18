# Add WAA to main.cricsheet_ball_raa without rescoring: it is derivable from
# columns already stored (exp_wicket, is_wicket).
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

have <- DBI::dbGetQuery(conn, "
  SELECT column_name FROM information_schema.columns
  WHERE table_schema='main' AND table_name='cricsheet_ball_raa'")$column_name
cat("existing columns:", paste(have, collapse=", "), "\n")

if (!"waa" %in% have) {
  DBI::dbExecute(conn, "ALTER TABLE main.cricsheet_ball_raa ADD COLUMN waa DOUBLE")
  cat("added column waa\n")
}
n <- DBI::dbExecute(conn, "
  UPDATE main.cricsheet_ball_raa
  SET waa = exp_wicket - CAST(is_wicket AS INT)")
cat(sprintf("backfilled %s rows\n", format(n, big.mark=",")))

cat("\n=== CONSISTENCY: does lambda * waa reproduce the stored raa_wicket? ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT format,
         ROUND(MAX(ABS(raa_wicket - (CASE format WHEN 'T20' THEN 9.0
                                                 WHEN 'ODI' THEN 23.0
                                                 ELSE 33.0 END) * waa)), 9) AS max_abs_diff,
         COUNT(*) AS rows
  FROM main.cricsheet_ball_raa GROUP BY format ORDER BY format"))
cat("  max_abs_diff ~0 confirms waa is exactly raa_wicket/lambda, so nothing was\n")
cat("  rescored and nothing changed -- the wicket term is simply now unpriced.\n")

cat("\n=== WAA sanity: mean should be ~0, and a dismissal ~ -1 ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT format, ROUND(AVG(waa),5) AS mean_waa, ROUND(STDDEV(waa),4) AS sd_waa,
         ROUND(AVG(CASE WHEN is_wicket THEN waa END),4) AS mean_on_dismissal,
         ROUND(AVG(CASE WHEN NOT is_wicket THEN waa END),5) AS mean_on_survival
  FROM main.cricsheet_ball_raa GROUP BY format ORDER BY format"))
