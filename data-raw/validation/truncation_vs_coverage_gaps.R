# Is a short innings genuine truncation, or a cricsheet coverage gap?
#
# Decisive test: the SCORECARD total (cricsheet.match_innings) against the sum
# of ball-by-ball runs. If the scorecard says 300 and only 200 runs of balls
# exist, that is missing data. If they agree and the innings is short, the
# innings really was cut short.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages(library(data.table))
conn <- get_db_connection(read_only = TRUE); on.exit(DBI::dbDisconnect(conn, shutdown=TRUE))
d <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT m.match_id,
         CASE WHEN LOWER(m.match_type) IN ('t20','it20') THEN 'T20' ELSE 'ODI' END AS fmt,
         CAST(m.match_date AS DATE) AS md,
         COALESCE(m.outcome_method,'') AS method,
         mi.total_runs AS card_runs, mi.total_wickets AS card_wkts,
         b.bbb_runs, b.balls
  FROM cricsheet.matches m
  JOIN cricsheet.match_innings mi ON mi.match_id = m.match_id AND mi.innings = 1
  JOIN (SELECT match_id, SUM(runs_total) AS bbb_runs, COUNT(*) AS balls
        FROM cricsheet.deliveries WHERE innings = 1 GROUP BY 1) b ON b.match_id = m.match_id
  WHERE LOWER(m.match_type) IN ('t20','it20','odi','odm') AND COALESCE(m.balls_per_over,6)=6"))
d[, sched := ifelse(fmt == "T20", 120L, 300L)]
d[, short := balls < 0.9 * sched & card_wkts < 10]
d[, gap := card_runs - bbb_runs]
cat(sprintf("innings-1 records: %s\n\n", format(nrow(d), big.mark=",")))
cat("=== do scorecard and ball-by-ball agree? ===\n")
print(d[, .(innings = .N,
            agree = sum(abs(gap) <= 1),
            pct_agree = round(100*mean(abs(gap) <= 1),1),
            mean_gap_when_disagree = round(mean(gap[abs(gap) > 1]),1)), by = fmt])
cat("\n=== of the SHORT innings, how many are real vs a data gap? ===\n")
print(d[short == TRUE, .(short_innings = .N,
        real_truncation = sum(abs(gap) <= 1),
        coverage_gap    = sum(abs(gap) > 1),
        pct_gap         = round(100*mean(abs(gap) > 1),1)), by = fmt])
cat("\n=== real truncations: are they flagged as DLS? ===\n")
print(d[short == TRUE & abs(gap) <= 1,
        .(n = .N, with_method_flag = sum(method != ""),
          pct_flagged = round(100*mean(method != ""),1),
          mean_total = round(mean(bbb_runs),1), mean_balls = round(mean(balls))), by = fmt])
cat("\n=== so what is the true truncation share? ===\n")
print(d[, .(innings = .N,
            true_truncated = sum(short & abs(gap) <= 1),
            pct = round(100*mean(short & abs(gap) <= 1),1)), by = fmt])
