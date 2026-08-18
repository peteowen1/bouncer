# Independent cross-check: the run cost of a wicket measured as EXPECTED
# REMAINING INNINGS RUNS, which involves no win probability and no draws at all.
#
# Agreement between this and the WP-based lambda is meaningful in a way that
# agreement between three WP utilities is not (two of them turned out to be the
# same utility up to an affine transform).
#
# The naive form -- mean remaining runs at w vs w+1 over the whole innings -- is
# selection-biased: sides several down are systematically worse sides. So the
# comparison is made WITHIN a (innings, balls-elapsed-in-innings, lead) cell,
# and additionally by regression with those controls.
source("C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda/_preamble.R")
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

q <- "
WITH inn AS (
  SELECT match_id, innings, SUM(runs_total) AS inn_total, COUNT(*) AS inn_balls
  FROM cricsheet.deliveries
  WHERE match_type IN ('Test','MDM') AND gender='male'
  GROUP BY 1,2
),
d AS (
  SELECT d.match_id, d.innings,
    d.total_runs - d.runs_total AS runs_pre,
    d.wickets_fallen - CASE WHEN d.is_wicket THEN 1 ELSE 0 END AS wkts_pre,
    ROW_NUMBER() OVER (PARTITION BY d.match_id, d.innings
                       ORDER BY d.over, d.ball) - 1 AS inn_balls_elapsed,
    i.inn_total
  FROM cricsheet.deliveries d
  JOIN inn i ON i.match_id=d.match_id AND i.innings=d.innings
  WHERE d.match_type IN ('Test','MDM') AND d.gender='male'
)
SELECT innings, wkts_pre,
       CAST(ROUND(inn_balls_elapsed / 60.0) * 60 AS INTEGER) AS elapsed_bin,
       COUNT(*) AS n,
       AVG(inn_total - runs_pre) AS mean_remaining
FROM d
WHERE wkts_pre BETWEEN 0 AND 9
GROUP BY 1,2,3
ORDER BY 1,2,3"

cat("querying expected-remaining-runs surface...\n")
r <- dbGetQuery(conn, q)
cat(sprintf("cells %d, balls %s\n", nrow(r), format(sum(r$n), big.mark = ",")))
r <- r[r$n >= 50, ]

cat("\n=== face validity: mean remaining innings runs by wickets down (all innings, pooled) ===\n")
a <- aggregate(cbind(n) ~ wkts_pre, r, sum)
a$mean_rem <- round(sapply(a$wkts_pre, function(w) {
  s <- r[r$wkts_pre == w, ]; sum(s$mean_remaining * s$n) / sum(s$n) }), 1)
a$drop <- c(NA, round(-diff(a$mean_rem), 1))
print(a)
cat("  'drop' = naive per-wicket run cost. SELECTION-BIASED, shown for contrast only.\n")

# ---- within-cell paired comparison ----------------------------------------
# For each (innings, elapsed_bin) pair, compare adjacent wicket counts. Sides
# are still not identical, but they are at the same point of the same innings.
cat("\n=== within (innings, elapsed) cells: paired w -> w+1 run cost ===\n")
key <- paste(r$innings, r$elapsed_bin)
costs <- c(); wts <- c()
for (k in unique(key)) {
  s <- r[key == k, ]
  s <- s[order(s$wkts_pre), ]
  if (nrow(s) < 2) next
  for (j in seq_len(nrow(s) - 1)) {
    if (s$wkts_pre[j + 1] != s$wkts_pre[j] + 1) next
    costs <- c(costs, s$mean_remaining[j] - s$mean_remaining[j + 1])
    wts   <- c(wts, min(s$n[j], s$n[j + 1]))
  }
}
cat(sprintf("  pairs: %d   weighted mean run cost of a wicket: %.1f\n",
            length(costs), sum(costs * wts) / sum(wts)))
cat(sprintf("  unweighted median: %.1f   IQR [%.1f, %.1f]\n",
            median(costs), quantile(costs, .25), quantile(costs, .75)))

# ---- regression with controls ---------------------------------------------
cat("\n=== regression: mean_remaining ~ wkts_pre + controls (coef on wkts = run cost) ===\n")
r$inn <- factor(r$innings)
m <- lm(mean_remaining ~ wkts_pre + inn + splines::ns(elapsed_bin, 5) +
          inn:splines::ns(elapsed_bin, 3), data = r, weights = n)
co <- coef(summary(m))["wkts_pre", ]
cat(sprintf("  coef %.2f  (se %.2f)  => run cost of a wicket = %.1f\n",
            co[1], co[2], -co[1]))

cat("\n=== per innings ===\n")
for (i in 1:4) {
  s <- r[r$innings == i, ]
  mi <- lm(mean_remaining ~ wkts_pre + splines::ns(elapsed_bin, 5), data = s, weights = n)
  cat(sprintf("  innings %d: run cost %.1f\n", i, -coef(mi)["wkts_pre"]))
}

cat("\nWP-based lambda for comparison: 33.5 (U1/U2)\n")
