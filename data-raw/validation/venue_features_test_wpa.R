# #69: how much did the unsmoothed, self-inclusive venue features leak?
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown = TRUE))

v <- as.data.table(dbGetQuery(conn, "
  SELECT m.match_id, m.venue, m.match_date, m.outcome_type,
         MAX(CASE WHEN mi.innings = 1 THEN mi.total_runs END) AS inn1_total
  FROM cricsheet.matches m
  LEFT JOIN cricsheet.match_innings mi ON mi.match_id = m.match_id
  WHERE LOWER(m.match_type) IN ('test','mdm') GROUP BY 1,2,3,4"))
v[, `:=`(match_date = as.Date(match_date),
         decided = as.integer(!is.na(outcome_type)),
         is_result = as.integer(!is.na(outcome_type) & outcome_type != "draw"))]
cat(sprintf("Test/MDM matches: %d across %d venues\n", nrow(v), uniqueN(v$venue)))

# OLD: raw unsmoothed per-venue average over ALL matches
old <- v[, .(old_rate = mean(is_result), old_avg = mean(inn1_total, na.rm = TRUE),
             n = .N), by = venue]
v <- merge(v, old, by = "venue")
vr <- time_causal_venue_result_rate(v, prior_weight = 10)
va <- time_causal_venue_mean(v, "inn1_total", prior_weight = 5)
v <- merge(merge(v, vr[, .(match_id, new_rate = venue_result_rate, at_prior)], by="match_id"),
           va[, .(match_id, new_avg = venue_mean)], by = "match_id")

cat(sprintf("venues with a single match: %d (%.1f%% of venues)\n",
    uniqueN(old[n == 1, venue]), 100*mean(old$n == 1)))
cat(sprintf("matches at a one-match venue: %d\n\n", v[n == 1, .N]))

cat("=== correlation with the match's OWN outcome (result rate) ===\n")
cat(sprintf("%-16s %9s %9s %7s\n", "venue history", "OLD", "NEW", "n"))
for (lab in c("all", "n == 1", "n < 5", "n >= 30")) {
  d <- switch(lab, "all"=v, "n == 1"=v[n==1], "n < 5"=v[n<5], v[n>=30])
  if (nrow(d) < 5) next
  oc <- if (sd(d$old_rate) == 0) NA else cor(d$old_rate, d$is_result)
  cat(sprintf("%-16s %9s %9.3f %7d\n", lab,
      if (is.na(oc)) "  const" else sprintf("%.3f", oc), cor(d$new_rate, d$is_result), nrow(d)))
}
cat("\n=== correlation with the match's OWN innings-1 total (venue average) ===\n")
w <- v[!is.na(inn1_total)]
cat(sprintf("%-16s %9s %9s %7s\n", "venue history", "OLD", "NEW", "n"))
for (lab in c("all", "n == 1", "n < 5", "n >= 30")) {
  d <- switch(lab, "all"=w, "n == 1"=w[n==1], "n < 5"=w[n<5], w[n>=30])
  if (nrow(d) < 5) next
  oc <- if (sd(d$old_avg) == 0) NA else cor(d$old_avg, d$inn1_total)
  cat(sprintf("%-16s %9s %9.3f %7d\n", lab,
      if (is.na(oc)) "  const" else sprintf("%.3f", oc), cor(d$new_avg, d$inn1_total), nrow(d)))
}
cat(sprintf("\nfirst-at-ground, taking the prior: %d of %d (%.1f%%)\n",
    sum(v$at_prior), nrow(v), 100*mean(v$at_prior)))
