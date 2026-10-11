# Calibration of win_prob_after (P(batting-first team wins)), cricsheet limited overs.
# Seasons 2024-25 = the in-match models' test split (used for early stopping only),
# so mildly optimistic; there is no clean holdout (2026 was in training).
suppressMessages(library(data.table))
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only = TRUE)
d <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT w.match_id, w.format, w.gender, w.innings_number, w.over_number, w.win_prob_after AS p,
         m.season, CASE WHEN m.outcome_winner = i.batting_team THEN 1 ELSE 0 END AS y
  FROM main.bouncer_wp_from_cricsheet w
  JOIN cricsheet.matches m USING (match_id)
  JOIN cricsheet.match_innings i ON i.match_id = w.match_id AND i.innings = 1
  WHERE m.outcome_winner IS NOT NULL AND m.outcome_method IS NULL AND w.win_prob_after IS NOT NULL
    AND m.season IN ('2024', '2025', '2023/24', '2024/25')"))
DBI::dbDisconnect(conn, shutdown = TRUE)
stopifnot(nrow(d) > 0, all(d$p >= 0 & d$p <= 1))
cat(sprintf("%s deliveries, %d matches (decided, no D/L)\n", format(nrow(d), big.mark = ","), uniqueN(d$match_id)))
ece <- function(p, y, bins = 10) { b <- pmin(floor(p * bins), bins - 1); x <- data.table(b, p, y)[, .(n = .N, p = mean(p), y = mean(y)), by = b]; sum(x$n * abs(x$p - x$y)) / sum(x$n) }
ll <- function(p, y) { p <- pmin(pmax(p, 1e-6), 1 - 1e-6); -mean(y * log(p) + (1 - y) * log(1 - p)) }
s <- d[, .(n = .N, matches = uniqueN(match_id), brier = round(mean((p - y)^2), 4), logloss = round(ll(p, y), 4), ece = round(ece(p, y), 4)), by = .(format, gender)]
cat("\nBy bucket (lower is better for all three):\n"); print(s[order(format, gender)])
cat("\nReliability bins, men's T20 and ODI (mean stated vs actual; bins with n):\n")
for (f in c("T20", "ODI")) {
  x <- d[format == f & gender == "male"][, b := pmin(floor(p * 10), 9)][, .(n = .N, matches = uniqueN(match_id), stated = round(mean(p), 3), actual = round(mean(y), 3)), by = b][order(b)]
  x[, gap := actual - stated]; cat("--", f, "\n"); print(x)
}
cat("\nTails (stated < 0.05 or > 0.95), all buckets:\n")
print(d[p < 0.05 | p > 0.95, .(n = .N, stated = round(mean(ifelse(p > 0.5, p, 1 - p)), 4), actual_favourite_won = round(mean(ifelse(p > 0.5, y, 1 - y)), 4)), by = .(format, gender, tail = ifelse(p > 0.5, "high", "low"))][order(format, gender)])
cat("\nBy innings, men's T20 (ECE):\n"); print(d[format == "T20" & gender == "male", .(n = .N, ece = round(ece(p, y), 4)), by = innings_number])
