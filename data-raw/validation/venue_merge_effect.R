# Does merging aliases make the venue features better, or just tidier?
#
# Two things should improve if the merge is real:
#   1. n_prior rises -- a ground's history is no longer split, so estimates are
#      shrunk toward the prior less than they should have been.
#   2. the venue rate PREDICTS BETTER -- a cleaner estimate of a real effect
#      correlates more with the outcome it is meant to anticipate.
# The second is the test; the first alone could just be relabelling.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown=TRUE))
vmap <- build_venue_id_map(conn)

m <- as.data.table(dbGetQuery(conn, "
  SELECT m.match_id, m.venue, CAST(m.match_date AS DATE) AS match_date, m.outcome_type
  FROM cricsheet.matches m
  WHERE LOWER(m.match_type) IN ('test','mdm') AND m.outcome_type IS NOT NULL AND m.venue IS NOT NULL"))
m[, `:=`(decided = 1L, is_result = as.integer(outcome_type != "draw"))]

run <- function(d, lab) {
  r <- time_causal_venue_result_rate(d, prior_weight = 10)
  x <- merge(d, r, by = "match_id")
  cat(sprintf("%-10s venues %4d | mean n_prior %5.1f | at prior %4.1f%% | cor(rate, result) %+.4f\n",
      lab, uniqueN(d$venue), mean(x$n_prior), 100*mean(x$at_prior),
      cor(x$venue_result_rate, x$is_result)))
  x
}
cat("=== Test/MDM, time-causal venue result rate ===\n")
before <- run(copy(m), "aliased")
m2 <- copy(m); canonicalise_venues(m2, vmap)
after <- run(m2, "merged")

cat("\n=== restricted to matches whose venue name actually changed ===\n")
touched <- m[venue %in% vmap$venue, match_id]
b <- before[match_id %in% touched]; a <- after[match_id %in% touched]
cat(sprintf("affected Test matches: %d\n", length(touched)))
cat(sprintf("mean n_prior  %.1f -> %.1f\n", mean(b$n_prior), mean(a$n_prior)))
cat(sprintf("at prior      %.1f%% -> %.1f%%\n", 100*mean(b$at_prior), 100*mean(a$at_prior)))
if (length(touched) > 30)
  cat(sprintf("cor(rate, result) on those matches %+.4f -> %+.4f\n",
      cor(b$venue_result_rate, b$is_result), cor(a$venue_result_rate, a$is_result)))
