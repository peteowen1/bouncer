# Check T20-specific ball counts
library(DBI)
devtools::load_all()
conn <- get_db_connection(read_only = TRUE)

cat("=== T20-only ball counts for key players ===\n")

t20_counts <- dbGetQuery(conn, "
  SELECT
    p.player_name,
    d.batter_id,
    COUNT(*) as t20_balls,
    SUM(runs_batter) as t20_runs,
    AVG(runs_batter) * 100 as strike_rate
  FROM deliveries d
  JOIN players p ON d.batter_id = p.player_id
  WHERE LOWER(match_type) IN ('t20', 'it20')
    AND d.batter_id IN ('5b8c830e', 'ba607b88', '8a75e999', 'c4487b84')
  GROUP BY p.player_name, d.batter_id
  ORDER BY t20_balls DESC
")
print(t20_counts)

cat("\n=== Check ELO table ball counts ===\n")
elo_counts <- dbGetQuery(conn, "
  SELECT batter_id, MAX(batter_balls) as max_balls
  FROM mens_t20_3way_elo
  WHERE batter_id IN ('5b8c830e', 'ba607b88', '8a75e999', 'c4487b84')
  GROUP BY batter_id
")
print(elo_counts)

cat("\n=== Check if there are multiple Babar Azams ===\n")
babars <- dbGetQuery(conn, "
  SELECT player_id, player_name
  FROM players
  WHERE LOWER(player_name) LIKE '%babar azam%'
")
print(babars)

cat("\n=== Babar Azam match_type distribution ===\n")
babar_formats <- dbGetQuery(conn, "
  SELECT match_type, COUNT(*) as balls
  FROM deliveries
  WHERE batter_id = '8a75e999'
  GROUP BY match_type
  ORDER BY balls DESC
")
print(babar_formats)

dbDisconnect(conn, shutdown = TRUE)
