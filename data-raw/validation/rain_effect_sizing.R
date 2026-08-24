# #72 sizing: do the 1,818 orphaned weather rows show rain moving the draw rate?
# One query before anyone spends thousands of API calls (#71's lesson).
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(dbDisconnect(conn, shutdown=TRUE))

w <- as.data.table(dbGetQuery(conn, "SELECT * FROM main.weather_temp_110713"))
cat(sprintf("weather_temp_110713: %d rows, %d distinct matches\n", nrow(w), uniqueN(w$match_id)))
cat("match_type:\n"); print(w[, .N, by = match_type][order(-N)])
cat(sprintf("\ndate range: %s to %s\n", min(w$match_date, na.rm=TRUE), max(w$match_date, na.rm=TRUE)))
cat("\ncolumn coverage (non-NA %):\n")
for (cc in c("rain_days","precipitation_total","rain_total","match_days_weather","is_rain","temp_avg","wind_avg")) {
  if (cc %in% names(w)) cat(sprintf("  %-22s %5.1f%%\n", cc, 100*mean(!is.na(w[[cc]]))))
}
cat("\nrain_days distribution:\n"); print(w[, .N, by = rain_days][order(rain_days)])

# join to Test outcomes
m <- as.data.table(dbGetQuery(conn, "
  SELECT match_id, outcome_type, match_type AS mt FROM cricsheet.matches
  WHERE LOWER(match_type) IN ('test','mdm') AND outcome_type IS NOT NULL"))
j <- merge(w, m, by = "match_id")
cat(sprintf("\n=== joined to Test/MDM outcomes: %d matches ===\n", nrow(j)))
if (nrow(j) < 30) { cat("too few to size. STOP.\n"); quit(save="no") }
j[, is_draw := as.integer(outcome_type == "draw")]
cat(sprintf("draw rate overall: %.3f\n\n", mean(j$is_draw)))

cat("draw rate by rain days:\n")
print(j[, .(matches = .N, draw_rate = round(mean(is_draw), 3)), by = rain_days][order(rain_days)])

for (v in c("rain_days","precipitation_total","rain_total")) {
  if (!v %in% names(j)) next
  x <- j[[v]]; if (all(is.na(x)) || sd(x, na.rm=TRUE) == 0) next
  cc <- cor(x, j$is_draw, use = "complete.obs")
  cat(sprintf("\ncor(%s, is_draw) = %+.3f", v, cc))
  cat(sprintf("   -> explains %.2f%% of draw variance\n", 100*cc^2))
}
cat("\n=== effect size in the units that matter ===\n")
lo <- j[rain_days <= 0, mean(is_draw)]; hi <- j[rain_days >= 2, mean(is_draw)]
cat(sprintf("draw rate with 0 rain days: %.3f (n=%d)\n", lo, j[rain_days<=0,.N]))
cat(sprintf("draw rate with 2+ rain days: %.3f (n=%d)\n", hi, j[rain_days>=2,.N]))
cat(sprintf("difference: %+.3f\n", hi - lo))
