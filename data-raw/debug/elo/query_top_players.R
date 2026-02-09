library(DBI)
library(duckdb)

conn <- dbConnect(duckdb(), '../bouncerdata/bouncer.duckdb', read_only = TRUE)

cat('Total rows in mens_t20_3way_elo:', dbGetQuery(conn, 'SELECT COUNT(*) FROM mens_t20_3way_elo')[[1]], '\n')

cat('\n=== TOP 20 BATTERS (Run ELO) with Centrality ===\n')
top_batters <- dbGetQuery(conn, "
  WITH latest_elo AS (
    SELECT batter_id, batter_run_elo_after, batter_balls,
           ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY delivery_id DESC) as rn
    FROM mens_t20_3way_elo
  ),
  latest_centrality AS (
    SELECT player_id, percentile
    FROM mens_t20_player_centrality_history
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM mens_t20_player_centrality_history)
      AND role = 'batter'
  )
  SELECT p.player_name,
         ROUND(l.batter_run_elo_after, 1) as run_elo,
         l.batter_balls as balls,
         COALESCE(ROUND(c.percentile, 1), 0) as centrality_pct,
         CASE
           WHEN c.percentile < 30 THEN '⚠️ LOW'
           WHEN c.percentile < 60 THEN '~'
           ELSE '✓'
         END as flag
  FROM latest_elo l
  JOIN players p ON l.batter_id = p.player_id
  LEFT JOIN latest_centrality c ON l.batter_id = c.player_id
  WHERE l.rn = 1
  ORDER BY l.batter_run_elo_after DESC
  LIMIT 20
")
print(top_batters, row.names = FALSE)

cat('\n=== TOP 20 BOWLERS (Run ELO - Economy) with Centrality ===\n')
top_bowlers <- dbGetQuery(conn, "
  WITH latest_elo AS (
    SELECT bowler_id, bowler_run_elo_after, bowler_balls,
           ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY delivery_id DESC) as rn
    FROM mens_t20_3way_elo
  ),
  latest_centrality AS (
    SELECT player_id, percentile
    FROM mens_t20_player_centrality_history
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM mens_t20_player_centrality_history)
      AND role = 'bowler'
  )
  SELECT p.player_name,
         ROUND(l.bowler_run_elo_after, 1) as run_elo,
         l.bowler_balls as balls,
         COALESCE(ROUND(c.percentile, 1), 0) as centrality_pct,
         CASE
           WHEN c.percentile < 30 THEN '⚠️ LOW'
           WHEN c.percentile < 60 THEN '~'
           ELSE '✓'
         END as flag
  FROM latest_elo l
  JOIN players p ON l.bowler_id = p.player_id
  LEFT JOIN latest_centrality c ON l.bowler_id = c.player_id
  WHERE l.rn = 1
  ORDER BY l.bowler_run_elo_after DESC
  LIMIT 20
")
print(top_bowlers, row.names = FALSE)

cat('\n=== TOP 20 BOWLERS (Wicket ELO - Strike Rate) with Centrality ===\n')
top_wicket_bowlers <- dbGetQuery(conn, "
  WITH latest_elo AS (
    SELECT bowler_id, bowler_wicket_elo_after, bowler_balls,
           ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY delivery_id DESC) as rn
    FROM mens_t20_3way_elo
  ),
  latest_centrality AS (
    SELECT player_id, percentile
    FROM mens_t20_player_centrality_history
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM mens_t20_player_centrality_history)
      AND role = 'bowler'
  )
  SELECT p.player_name,
         ROUND(l.bowler_wicket_elo_after, 1) as wicket_elo,
         l.bowler_balls as balls,
         COALESCE(ROUND(c.percentile, 1), 0) as centrality_pct,
         CASE
           WHEN c.percentile < 30 THEN '⚠️ LOW'
           WHEN c.percentile < 60 THEN '~'
           ELSE '✓'
         END as flag
  FROM latest_elo l
  JOIN players p ON l.bowler_id = p.player_id
  LEFT JOIN latest_centrality c ON l.bowler_id = c.player_id
  WHERE l.rn = 1
  ORDER BY l.bowler_wicket_elo_after DESC
  LIMIT 20
")
print(top_wicket_bowlers, row.names = FALSE)

dbDisconnect(conn, shutdown = TRUE)
