# Check if centrality was updated with 50-ball threshold
library(DBI)
devtools::load_all()
conn <- get_db_connection(read_only = TRUE)

cat("=== TD Andrews (a0f612b6) centrality ===\n")
td <- dbGetQuery(conn, "
  SELECT * FROM mens_t20_player_centrality_history
  WHERE player_id = 'a0f612b6'
  ORDER BY snapshot_date DESC
  LIMIT 5
")
print(td)

cat("\n=== Players with 50-70 balls - do they have BATTER centrality? ===\n")
small_sample <- dbGetQuery(conn, "
  WITH batter_balls AS (
    SELECT batter_id, MAX(batter_balls) as balls
    FROM mens_t20_3way_elo
    GROUP BY batter_id
    HAVING MAX(batter_balls) BETWEEN 50 AND 70
  )
  SELECT b.batter_id, p.player_name, b.balls,
         CASE WHEN c.player_id IS NOT NULL THEN 'YES' ELSE 'NO' END as has_batter_centrality
  FROM batter_balls b
  JOIN players p ON b.batter_id = p.player_id
  LEFT JOIN (SELECT DISTINCT player_id FROM mens_t20_player_centrality_history WHERE role = 'batter') c
    ON b.batter_id = c.player_id
  ORDER BY b.balls DESC
  LIMIT 20
")
print(small_sample)

cat("\n=== Centrality table stats ===\n")
stats <- dbGetQuery(conn, "
  SELECT role, COUNT(DISTINCT player_id) as unique_players,
         MIN(snapshot_date) as earliest, MAX(snapshot_date) as latest
  FROM mens_t20_player_centrality_history
  GROUP BY role
")
print(stats)

dbDisconnect(conn, shutdown = TRUE)
