setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
conn <- get_db_connection(read_only = TRUE); on.exit(DBI::dbDisconnect(conn, shutdown=TRUE))
cat("=== mean TSA by ball of the innings, first 3 overs ===\n")
cat("If TSA is an unbiased delta, mean should hover near zero at every ball.\n\n")
print(DBI::dbGetQuery(conn, "
  SELECT format, over_number, ROUND(AVG(delta_ps),3) AS mean_tsa,
         ROUND(STDDEV(delta_ps),2) AS sd, COUNT(*) AS n
  FROM main.bouncer_wp_from_cricsheet
  WHERE innings_number = 1 AND over_number <= 3
  GROUP BY 1,2 ORDER BY 1,2"))
cat("\n=== and across the whole innings, by phase ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT format,
         CASE WHEN over_number < 6 THEN 'a 0-5' WHEN over_number < 15 THEN 'b 6-14'
              WHEN over_number < 40 THEN 'c 15-39' ELSE 'd 40+' END AS phase,
         ROUND(AVG(delta_ps),3) AS mean_tsa, COUNT(*) AS n
  FROM main.bouncer_wp_from_cricsheet WHERE innings_number = 1
  GROUP BY 1,2 ORDER BY 1,2"))
cat("\n=== does the very first ball carry a bonus nobody earned? ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT format, ROUND(AVG(CASE WHEN over_number=0 AND ball_number=1 THEN delta_ps END),3) AS ball1,
         ROUND(AVG(CASE WHEN over_number=0 AND ball_number>1 THEN delta_ps END),3) AS rest_of_over1,
         ROUND(AVG(CASE WHEN over_number=1 THEN delta_ps END),3) AS over2
  FROM main.bouncer_wp_from_cricsheet WHERE innings_number = 1 GROUP BY 1 ORDER BY 1"))
