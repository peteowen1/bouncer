library(DBI)
library(duckdb)

conn <- dbConnect(duckdb(), '../../../bouncerdata/bouncer.duckdb', read_only = TRUE)

cat('=== TOP 10 BATTERS (Run ELO) ===\n\n')
batters <- dbGetQuery(conn, "
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
         ROUND(c.percentile, 1) as centrality
  FROM latest_elo l
  JOIN players p ON l.batter_id = p.player_id
  LEFT JOIN latest_centrality c ON l.batter_id = c.player_id
  WHERE l.rn = 1
  ORDER BY l.batter_run_elo_after DESC
  LIMIT 10
")
print(batters, row.names = FALSE)

cat('\n=== TOP 10 BOWLERS (Run ELO - Economy) ===\n\n')
bowlers_econ <- dbGetQuery(conn, "
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
         ROUND(c.percentile, 1) as centrality
  FROM latest_elo l
  JOIN players p ON l.bowler_id = p.player_id
  LEFT JOIN latest_centrality c ON l.bowler_id = c.player_id
  WHERE l.rn = 1
  ORDER BY l.bowler_run_elo_after DESC
  LIMIT 10
")
print(bowlers_econ, row.names = FALSE)

cat('\n=== TOP 10 BOWLERS (Wicket ELO - Strike Rate) ===\n\n')
bowlers_sr <- dbGetQuery(conn, "
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
         ROUND(c.percentile, 1) as centrality
  FROM latest_elo l
  JOIN players p ON l.bowler_id = p.player_id
  LEFT JOIN latest_centrality c ON l.bowler_id = c.player_id
  WHERE l.rn = 1
  ORDER BY l.bowler_wicket_elo_after DESC
  LIMIT 10
")
print(bowlers_sr, row.names = FALSE)

dbDisconnect(conn, shutdown = TRUE)
