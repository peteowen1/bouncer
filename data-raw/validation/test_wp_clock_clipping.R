# Does MAX_OVERS = 450 actually BITE, or is it inert for a tree model?
#
# For XGBoost, x/450 and x/299 give identical splits -- a constant divisor is a
# monotone reparameterisation. MAX_OVERS can only matter where it CLIPS
# (pmax(0, MAX-cum), pmin(1, cum/MAX)) or where overs_remaining is a DENOMINATOR
# (lead_per_over_remaining = abs_lead/overs_remaining, req_rate).
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown=TRUE))
d <- as.data.table(dbGetQuery(conn, "
  SELECT m.match_id, COUNT(*)/6.0 AS total_overs
  FROM cricsheet.matches m JOIN cricsheet.deliveries d ON d.match_id=m.match_id
  WHERE LOWER(m.match_type) IN ('test','mdm') AND m.outcome_type IS NOT NULL GROUP BY 1"))
cat(sprintf("matches: %d\n\n", nrow(d)))
for (MX in c(450, 350, 299)) {
  clip <- mean(d$total_overs >= MX)
  cat(sprintf("MAX_OVERS = %3d -> matches where cum_overs ever reaches it: %5.2f%% (%d matches)\n",
      MX, 100*clip, sum(d$total_overs >= MX)))
}
cat("\nSo with 450 the clips essentially never bind: the constant is INERT for the\n")
cat("tree except through overs_remaining as a denominator.\n\n")
cat("=== the denominator effect ===\n")
cat("lead_per_over_remaining = abs_lead / (MAX_OVERS - cum_overs).\n")
cat("Changing MAX changes the SHAPE of that ratio, not just its scale.\n")
for (MX in c(450, 299)) {
  co <- seq(0, 290, by = 10); lead <- 100
  r <- lead / pmax(1, MX - co)
  cat(sprintf("  MAX=%3d: ratio at 0 / 150 / 290 overs bowled = %.3f / %.3f / %.3f\n",
      MX, r[1], r[16], r[30]))
}
cat("\n=== era drift, which IS causal and IS absent from the model ===\n")
m2 <- as.datatable <- as.data.table(dbGetQuery(conn, "
  SELECT m.match_id, CAST(m.match_date AS DATE) AS md, COUNT(*)/6.0 AS total_overs
  FROM cricsheet.matches m JOIN cricsheet.deliveries d ON d.match_id=m.match_id
  WHERE LOWER(m.match_type) IN ('test','mdm') AND m.outcome_type IS NOT NULL GROUP BY 1,2"))
m2[, yr := as.integer(format(md, "%Y"))]
setorder(m2, md)
m2[, prior_median := {
  v <- numeric(.N)
  for (i in seq_len(.N)) v[i] <- if (i > 30) median(total_overs[1:(i-1)]) else NA_real_
  v
}]
ok <- m2[!is.na(prior_median)]
cat(sprintf("expanding-window prior median vs this match's overs: cor %.4f (n=%d)\n",
    cor(ok$prior_median, ok$total_overs), nrow(ok)))
cat(sprintf("year as a predictor of total overs: R^2 %.4f\n", summary(lm(total_overs ~ yr, m2))$r.squared))
