# Before building anything: is a per-match over budget even estimable?
#
# NOTE ON THE CONSTANT. time_pressure = projected_total_overs / MAX_OVERS and
# MAX_OVERS is a CONSTANT, so for a tree model dividing by 450 or by 299 gives
# IDENTICAL splits -- it is a pure reparameterisation. Changing the number alone
# cannot move the model. It only bites where it CLIPS (pmax/pmin) or where
# overs_remaining is a DENOMINATOR (lead_per_over_remaining, req_rate).
# So the real question is whether a per-MATCH budget is estimable causally.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown=TRUE))

m <- as.data.table(dbGetQuery(conn, "
  SELECT m.match_id, m.venue, m.match_date, m.match_type, m.outcome_type,
         COUNT(*)/6.0 AS total_overs
  FROM cricsheet.matches m JOIN cricsheet.deliveries d ON d.match_id = m.match_id
  WHERE LOWER(m.match_type) IN ('test','mdm') AND m.outcome_type IS NOT NULL
  GROUP BY 1,2,3,4,5"))
m[, match_date := as.Date(match_date)]
m[, era := as.integer(format(match_date, "%Y"))]
cat(sprintf("matches: %d | total overs: median %.0f  mean %.0f  sd %.0f  IQR %.0f-%.0f\n\n",
  nrow(m), median(m$total_overs), mean(m$total_overs), sd(m$total_overs),
  quantile(m$total_overs,.25), quantile(m$total_overs,.75)))

cat("=== how much of total-overs variance is BETWEEN venues? ===\n")
fit <- lm(total_overs ~ factor(venue), data = m)
cat(sprintf("  R^2 of venue identity on total overs: %.4f (in-sample, %d venues)\n",
    summary(fit)$r.squared, uniqueN(m$venue)))
cv <- time_causal_venue_mean(m, "total_overs", prior_weight = 5)
m <- merge(m, cv[, .(match_id, venue_exp_overs = venue_mean, n_prior)], by = "match_id")
cat(sprintf("  CAUSAL venue estimate vs actual: cor %.4f, R^2 %.4f\n",
    cor(m$venue_exp_overs, m$total_overs), cor(m$venue_exp_overs, m$total_overs)^2))
cat(sprintf("  with >=5 prior matches at the ground (n=%d): cor %.4f\n",
    m[n_prior>=5,.N], m[n_prior>=5, cor(venue_exp_overs, total_overs)]))

cat("\n=== does the ERA help? over rates have fallen ===\n")
print(m[, .(matches=.N, median_overs=round(median(total_overs))),
        by=.(decade = 10*(era %/% 10))][order(decade)])

cat("\n=== how much does the outcome depend on total overs at all? ===\n")
m[, is_result := as.integer(outcome_type != "draw")]
print(m[, .(matches=.N, result_rate=round(mean(is_result),3)),
        by=.(overs_band = cut(total_overs, c(0,200,250,300,350,400,600)))][order(overs_band)])
cat(sprintf("\ncor(total_overs, is_result) = %+.3f\n", cor(m$total_overs, m$is_result)))
cat("  (a LONG match means a draw; that is the signal the clock is for)\n")
