# TSA is a delta, so summing it across an innings telescopes:
#   sum(TSA) = proj(last ball) - proj(ball 0)
# If proj(ball 0) is unbiased, the mean of that sum is ~0 and any early/late
# drift is a WITHIN-innings redistribution -- a conditional calibration issue,
# not a wrong starting point. Separate the two.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages(library(data.table))
conn <- get_db_connection(read_only = TRUE); on.exit(DBI::dbDisconnect(conn, shutdown=TRUE))

d <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT w.format, w.match_id, w.innings_number,
         SUM(w.delta_ps) AS tsa_sum,
         MIN(w.proj_score_before) FILTER (WHERE w.over_number=0 AND w.ball_number=1) AS proj_start,
         MAX(w.proj_score_after)  AS proj_end,
         COUNT(*) AS balls
  FROM main.bouncer_wp_from_cricsheet w
  WHERE w.innings_number = 1
  GROUP BY 1,2,3"))
tot <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT d.match_id, d.innings AS innings_number, SUM(d.runs_total) AS final_total
  FROM cricsheet.deliveries d WHERE d.innings = 1 GROUP BY 1,2"))
d <- merge(d, tot, by = c("match_id","innings_number"))
d <- d[!is.na(proj_start) & balls >= 30]

cat(sprintf("innings analysed: %s\n\n", format(nrow(d), big.mark=",")))
cat("=== is the STARTING projection unbiased? ===\n")
print(d[, .(innings = .N,
            mean_proj_at_ball0 = round(mean(proj_start),1),
            mean_actual_final  = round(mean(final_total),1),
            bias               = round(mean(proj_start - final_total),2),
            sd_bias            = round(sd(proj_start - final_total),1)), by = format])

cat("\n=== does TSA telescope to (end - start)? ===\n")
print(d[, .(mean_tsa_sum = round(mean(tsa_sum),2),
            mean_end_minus_start = round(mean(proj_end - proj_start),2)), by = format])

cat("\n=== starting bias by how the innings turned out ===\n")
cat("If the model is optimistic early it will overshoot low-scoring innings.\n")
d[, band := cut(final_total, quantile(final_total, 0:4/4), include.lowest = TRUE,
                labels = c("Q1 lowest","Q2","Q3","Q4 highest")), by = format]
print(d[, .(innings = .N, mean_final = round(mean(final_total)),
            mean_start_proj = round(mean(proj_start),1),
            bias = round(mean(proj_start - final_total),1)), by = .(format, band)][order(format, band)])
