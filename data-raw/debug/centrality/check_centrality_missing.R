# Check why TD Andrews has no centrality
library(DBI)
devtools::load_all()
conn <- get_db_connection(read_only = TRUE)

cat("=== Check if TD Andrews exists in centrality table ===\n")
td_centrality <- dbGetQuery(conn, "
  SELECT * FROM mens_t20_player_centrality_history
  WHERE player_id = 'a0f612b6'
")
cat("Rows found:", nrow(td_centrality), "\n")
if (nrow(td_centrality) > 0) print(td_centrality)

cat("\n=== Check total players in centrality table ===\n")
centrality_count <- dbGetQuery(conn, "
  SELECT role, COUNT(DISTINCT player_id) as players
  FROM mens_t20_player_centrality_history
  GROUP BY role
")
print(centrality_count)

cat("\n=== Check snapshot dates ===\n")
snapshots <- dbGetQuery(conn, "
  SELECT DISTINCT snapshot_date FROM mens_t20_player_centrality_history ORDER BY snapshot_date DESC LIMIT 5
")
print(snapshots)

cat("\n=== Players with high ELO but no centrality ===\n")
missing_centrality <- dbGetQuery(conn, "
  WITH max_balls AS (
    SELECT batter_id, MAX(batter_balls) as max_balls
    FROM mens_t20_3way_elo
    GROUP BY batter_id
  ),
  elo_players AS (
    SELECT e.batter_id, p.player_name, e.batter_run_elo_after as elo, e.batter_balls
    FROM mens_t20_3way_elo e
    JOIN max_balls m ON e.batter_id = m.batter_id AND e.batter_balls = m.max_balls
    JOIN players p ON e.batter_id = p.player_id
    WHERE e.batter_balls >= 50
  )
  SELECT ep.batter_id, ep.player_name, ROUND(ep.elo, 0) as elo, ep.batter_balls,
         CASE WHEN c.player_id IS NULL THEN 'MISSING' ELSE 'EXISTS' END as centrality_status
  FROM elo_players ep
  LEFT JOIN (SELECT DISTINCT player_id FROM mens_t20_player_centrality_history WHERE role = 'batter') c
    ON ep.batter_id = c.player_id
  WHERE c.player_id IS NULL
  ORDER BY ep.elo DESC
  LIMIT 20
")
print(missing_centrality)

dbDisconnect(conn, shutdown = TRUE)
