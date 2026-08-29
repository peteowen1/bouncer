setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages(library(data.table))
conn <- get_db_connection(read_only = TRUE); on.exit(DBI::dbDisconnect(conn, shutdown=TRUE))
cat("=== the newly scored period (after 2026-02-25) vs the rest ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT format, CASE WHEN match_date > DATE '2026-02-25' THEN 'new' ELSE 'existing' END AS era,
         COUNT(*) AS rows, ROUND(AVG(delta_ps),4) AS mean_tsa, ROUND(STDDEV(delta_ps),4) AS sd_tsa,
         ROUND(AVG(delta_wp),6) AS mean_dwp, ROUND(AVG(win_prob_before),4) AS mean_wp
  FROM main.bouncer_wp_from_cricsheet GROUP BY 1,2 ORDER BY 1,2"))

cat("\n=== ANCHOR: first ball of an innings, 0/0 ===\n")
cat("win probability should sit near the base rate, and TSA near zero.\n")
print(DBI::dbGetQuery(conn, "
  SELECT w.format, COUNT(*) AS innings,
         ROUND(AVG(w.win_prob_before),4) AS mean_wp_before,
         ROUND(MIN(w.win_prob_before),3) AS min_wp, ROUND(MAX(w.win_prob_before),3) AS max_wp,
         ROUND(AVG(w.delta_ps),4) AS mean_tsa
  FROM main.bouncer_wp_from_cricsheet w
  WHERE w.over_number = 0 AND w.ball_number = 1 AND w.innings_number = 1
  GROUP BY 1 ORDER BY 1"))

cat("\n=== TSA must sum to roughly zero within an innings (it is a delta) ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT format, ROUND(AVG(inn_sum),3) AS mean_innings_tsa_sum, ROUND(STDDEV(inn_sum),2) AS sd
  FROM (SELECT format, match_id, innings_number, SUM(delta_ps) AS inn_sum
        FROM main.bouncer_wp_from_cricsheet GROUP BY 1,2,3) t
  GROUP BY 1 ORDER BY 1"))

cat("\n=== outlier hunt: extreme single-ball TSA ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT format, ROUND(MIN(delta_ps),1) AS min_tsa, ROUND(MAX(delta_ps),1) AS max_tsa,
         SUM(CASE WHEN ABS(delta_ps) > 50 THEN 1 ELSE 0 END) AS beyond_50
  FROM main.bouncer_wp_from_cricsheet GROUP BY 1 ORDER BY 1"))
