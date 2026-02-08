# Check if centrality threshold was actually 50 or 100
library(DBI)
devtools::load_all()
conn <- get_db_connection(read_only = TRUE)

cat("Current CENTRALITY_MIN_DELIVERIES:", CENTRALITY_MIN_DELIVERIES, "\n\n")

cat("=== Batters by ball count range ===\n")
ball_ranges <- dbGetQuery(conn, "
  WITH batter_balls AS (
    SELECT batter_id, MAX(batter_balls) as balls
    FROM mens_t20_3way_elo
    GROUP BY batter_id
  ),
  with_centrality AS (
    SELECT DISTINCT player_id FROM mens_t20_player_centrality_history WHERE role = 'batter'
  )
  SELECT
    CASE
      WHEN balls < 50 THEN '0-49'
      WHEN balls BETWEEN 50 AND 99 THEN '50-99'
      WHEN balls >= 100 THEN '100+'
    END as ball_range,
    COUNT(*) as total_batters,
    SUM(CASE WHEN c.player_id IS NOT NULL THEN 1 ELSE 0 END) as with_centrality,
    SUM(CASE WHEN c.player_id IS NULL THEN 1 ELSE 0 END) as without_centrality
  FROM batter_balls b
  LEFT JOIN with_centrality c ON b.batter_id = c.player_id
  GROUP BY ball_range
  ORDER BY ball_range
")
print(ball_ranges)

cat("\n=== Minimum deliveries in centrality table ===\n")
min_del <- dbGetQuery(conn, "
  SELECT role, MIN(deliveries) as min_deliveries, MAX(deliveries) as max_deliveries
  FROM mens_t20_player_centrality_history
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM mens_t20_player_centrality_history)
  GROUP BY role
")
print(min_del)

dbDisconnect(conn, shutdown = TRUE)
