suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
library(DBI)
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

cat("=== 1. Is total_runs cumulative-within-innings and POST-delivery? ===\n")
cat("    (first 8 balls of one Test innings; total_runs should equal the running\n")
cat("     sum of runs_total INCLUDING the current ball)\n")
print(dbGetQuery(conn, "
  SELECT over, ball, runs_total, total_runs, is_wicket, wickets_fallen,
         SUM(runs_total) OVER (ORDER BY delivery_id
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_incl,
         SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) OVER (ORDER BY delivery_id
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wkts_incl
  FROM cricsheet.deliveries
  WHERE match_id = (SELECT MIN(match_id) FROM cricsheet.deliveries
                    WHERE match_type='Test' AND gender='male')
    AND innings = 1
  ORDER BY delivery_id LIMIT 8"))

cat("\n=== 2. Confirm the POST-delivery frame across the whole Test corpus ===\n")
print(dbGetQuery(conn, "
  WITH x AS (
    SELECT match_id, innings, delivery_id, runs_total, total_runs, is_wicket, wickets_fallen,
      SUM(runs_total) OVER (PARTITION BY match_id, innings ORDER BY delivery_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run_incl,
      SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) OVER (PARTITION BY match_id, innings ORDER BY delivery_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wkt_incl
    FROM cricsheet.deliveries WHERE match_type='Test' AND gender='male'
  )
  SELECT COUNT(*) AS balls,
         SUM(CASE WHEN total_runs = run_incl THEN 1 ELSE 0 END) AS runs_match_POST,
         SUM(CASE WHEN wickets_fallen = wkt_incl THEN 1 ELSE 0 END) AS wkts_match_POST
  FROM x"))

cat("\n=== 3. Does innings 3 ever share a batting team with innings 2? (follow-ons) ===\n")
print(dbGetQuery(conn, "
  WITH inn AS (
    SELECT DISTINCT match_id, innings, batting_team
    FROM cricsheet.deliveries WHERE match_type IN ('Test','MDM') AND gender='male'
  )
  SELECT COUNT(*) AS matches_where_inn3_team_eq_inn2_team
  FROM (SELECT a.match_id FROM inn a JOIN inn b
        ON a.match_id=b.match_id AND a.innings=2 AND b.innings=3
        WHERE a.batting_team = b.batting_team)"))
cat("  (non-zero => innings parity is NOT a safe proxy for which team is batting)\n")

cat("\n=== 4. outcome_winner vs batting_team: can we always label W/D/L? ===\n")
print(dbGetQuery(conn, "
  WITH inn AS (
    SELECT DISTINCT d.match_id, d.batting_team, m.outcome_type, m.outcome_winner
    FROM cricsheet.deliveries d JOIN cricsheet.matches m ON m.match_id=d.match_id
    WHERE d.match_type IN ('Test','MDM') AND d.gender='male'
  )
  SELECT outcome_type,
         COUNT(*) AS team_innings,
         SUM(CASE WHEN outcome_winner IS NULL THEN 1 ELSE 0 END) AS winner_null,
         SUM(CASE WHEN outcome_winner = batting_team THEN 1 ELSE 0 END) AS is_winner
  FROM inn GROUP BY 1 ORDER BY 1"))
