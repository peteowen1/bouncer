setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown = TRUE))

m <- as.data.table(dbGetQuery(conn, "
  SELECT m.match_id, m.venue, m.match_date, m.outcome_type
  FROM cricsheet.matches m
  WHERE LOWER(m.match_type) IN ('test','mdm') AND m.outcome_type IS NOT NULL"))
m[, `:=`(decided = 1L, is_result = as.integer(outcome_type != "draw"),
         match_date = as.Date(match_date))]
cat(sprintf("Test/MDM matches with an outcome: %d across %d venues\n", nrow(m), uniqueN(m$venue)))

# OLD: one rate per venue over ALL matches at that venue
pw <- 10
old <- m[, .(n = .N, r = sum(is_result)), by = venue]
pr <- old[, sum(r)/sum(n)]
old[, old_rate := (r + pw*pr)/(n + pw)]
m <- merge(m, old[, .(venue, old_rate, venue_n = n)], by = "venue")

# NEW
new <- time_causal_venue_result_rate(m, prior_weight = pw)
m <- merge(m, new, by = "match_id")

cat(sprintf("\nvenues with <10 decided matches: %d of %d (%.1f%%)\n",
    uniqueN(old[n < 10, venue]), nrow(old), 100*mean(old$n < 10)))
cat(sprintf("median matches per venue: %.0f\n", median(old$n)))
cat(sprintf("first-at-ground (falls back to prior): %d of %d (%.1f%%)\n",
    sum(m$at_prior), nrow(m), 100*mean(m$at_prior)))

cat("\n=== THE LEAK TEST: correlation of the feature with the match's OWN outcome ===\n")
cat("A time-causal feature cannot know the current result, so this should be\n")
cat("near zero at thin venues. The old one encodes it.\n\n")
cat(sprintf("%-22s %10s %10s %8s\n", "venue history", "OLD corr", "NEW corr", "n"))
for (lab in c("all", "venue_n < 5", "venue_n < 10", "venue_n >= 30")) {
  d <- switch(lab, "all" = m, "venue_n < 5" = m[venue_n < 5],
              "venue_n < 10" = m[venue_n < 10], m[venue_n >= 30])
  if (nrow(d) < 20) next
  cat(sprintf("%-22s %10.3f %10.3f %8d\n", lab,
      cor(d$old_rate, d$is_result), cor(d$venue_result_rate, d$is_result), nrow(d)))
}

cat("\n=== independent recomputation on 200 sampled matches ===\n")
set.seed(42); idx <- sort(sample(nrow(m), 200))
bad <- 0
for (i in idx) {
  row <- m[i]
  prior <- m[venue == row$venue & match_date < row$match_date]
  exp_rate <- (sum(prior$is_result) + pw*attr(new,"prior_rate"))/(nrow(prior) + pw)
  if (abs(exp_rate - row$venue_result_rate) > 1e-9) bad <- bad + 1
}
cat(sprintf("mismatches against a brute-force strictly-earlier recomputation: %d of 200\n", bad))

cat("\n=== distribution shift ===\n")
cat(sprintf("old: mean %.3f sd %.3f | new: mean %.3f sd %.3f\n",
    mean(m$old_rate), sd(m$old_rate), mean(m$venue_result_rate), sd(m$venue_result_rate)))
cat(sprintf("correlation old vs new: %.3f\n", cor(m$old_rate, m$venue_result_rate)))
