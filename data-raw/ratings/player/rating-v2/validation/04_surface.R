# Build the empirical actual-outcomes state surface for Test+MDM male.
#
# Deliberately NOT bouncer's own Test WP model: NEXT-STEPS records that it
# "cannot discriminate which matches will draw" mid-match, and the same reason
# ruled the T20/ODI WP models out of their own lambda fits -- two dominant tree
# features compress the wicket signal to 0.2-0.5% of split gain, so a lambda
# fitted from the model would inherit that bias.
#
# Pre-delivery state is derived from the row's OWN outcomes (total_runs and
# wickets_fallen are post-delivery, verified on 100% of 1,708,351 Test balls),
# never from an adjacent-row LAG.
#
# Cumulative runs key on batting_team, never innings parity: 342 matches have
# innings 3 batted by the same side as innings 2 (follow-ons).
source("C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda/_preamble.R")
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

q <- "
WITH inn AS (
  SELECT match_id, innings, batting_team, SUM(runs_total) AS inn_total
  FROM cricsheet.deliveries
  WHERE match_type IN ('Test','MDM') AND gender='male'
  GROUP BY 1,2,3
),
d AS (
  SELECT
    d.match_id, d.innings, d.batting_team,
    d.total_runs - d.runs_total                              AS runs_pre,
    d.wickets_fallen - CASE WHEN d.is_wicket THEN 1 ELSE 0 END AS wkts_pre,
    ROW_NUMBER() OVER (PARTITION BY d.match_id
                       ORDER BY d.innings, d.over, d.ball) - 1 AS balls_elapsed,
    m.outcome_type, m.outcome_winner
  FROM cricsheet.deliveries d
  JOIN cricsheet.matches m ON m.match_id = d.match_id
  WHERE d.match_type IN ('Test','MDM') AND d.gender='male'
),
s AS (
  SELECT
    d.innings, d.wkts_pre, d.balls_elapsed,
    d.runs_pre
      + COALESCE((SELECT SUM(i.inn_total) FROM inn i
                  WHERE i.match_id=d.match_id AND i.batting_team=d.batting_team
                    AND i.innings < d.innings), 0)
      - COALESCE((SELECT SUM(i.inn_total) FROM inn i
                  WHERE i.match_id=d.match_id AND i.batting_team<>d.batting_team
                    AND i.innings < d.innings), 0)                AS lead_pre,
    CASE WHEN d.outcome_type='normal' AND d.outcome_winner = d.batting_team THEN 'W'
         WHEN d.outcome_type='normal' AND d.outcome_winner IS NOT NULL     THEN 'L'
         ELSE 'D' END                                             AS res
  FROM d
)
SELECT
  innings,
  wkts_pre,
  CAST(ROUND(lead_pre / 20.0) * 20 AS INTEGER)          AS lead_bin,
  CAST(ROUND(balls_elapsed / 120.0) * 120 AS INTEGER)   AS elapsed_bin,
  SUM(CASE WHEN res='W' THEN 1 ELSE 0 END) AS n_w,
  SUM(CASE WHEN res='D' THEN 1 ELSE 0 END) AS n_d,
  SUM(CASE WHEN res='L' THEN 1 ELSE 0 END) AS n_l,
  COUNT(*)                                  AS n
FROM s
WHERE wkts_pre BETWEEN 0 AND 9
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4"

cat("Querying the surface (5.4M deliveries -> state cells)...\n")
t0 <- Sys.time()
g <- dbGetQuery(conn, q)
cat(sprintf("Done in %.0f s. cells: %d   balls covered: %s\n",
            as.numeric(difftime(Sys.time(), t0, units = "secs")),
            nrow(g), format(sum(g$n), big.mark = ",")))

cat("\n=== sanity: overall outcome shares (should match the corpus: Test 19% / MDM 38.7% draw) ===\n")
cat(sprintf("  W %.3f   D %.3f   L %.3f\n",
            sum(g$n_w)/sum(g$n), sum(g$n_d)/sum(g$n), sum(g$n_l)/sum(g$n)))
cat("  W and L should be near-equal by construction (every decided match has one of each)\n")

cat("\n=== cell occupancy ===\n")
print(summary(g$n))
cat("  cells with n >= 200:", sum(g$n >= 200), " covering",
    sprintf("%.1f%%", 100*sum(g$n[g$n>=200])/sum(g$n)), "of balls\n")

cat("\n=== face validity: P(win) by wickets_pre in innings 4 (chasing) ===\n")
i4 <- g[g$innings == 4, ]
agg <- aggregate(cbind(n_w, n_d, n_l, n) ~ wkts_pre, data = i4, FUN = sum)
agg$p_win <- round(agg$n_w / agg$n, 3); agg$p_draw <- round(agg$n_d / agg$n, 3)
print(agg[, c("wkts_pre", "n", "p_win", "p_draw")])

write_parquet(as.data.frame(g), file.path(OUT, "surface.parquet"))
cat("\nwrote surface.parquet\n")
