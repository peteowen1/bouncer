# Check 3-Way ELO results
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)

cat("=== Table info ===\n")
cat("Row count:", dbGetQuery(conn, "SELECT COUNT(*) FROM mens_t20_3way_elo")[[1]], "\n\n")

cat("=== Schema ===\n")
schema <- dbGetQuery(conn, "SELECT column_name FROM information_schema.columns WHERE table_name = 'mens_t20_3way_elo'")
print(schema)

cat("\n=== Sample rows ===\n")
sample <- dbGetQuery(conn, "SELECT delivery_id, batter_id, batter_run_elo_after, bowler_id, bowler_run_elo_after FROM mens_t20_3way_elo LIMIT 5")
print(sample)

cat("\n=== Top 10 Batters (by final run ELO) ===\n")
top_batters <- dbGetQuery(conn, "
  WITH latest_elo AS (
    SELECT batter_id, batter_run_elo_after,
           ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY delivery_id DESC) as rn
    FROM mens_t20_3way_elo
  )
  SELECT l.batter_id, p.player_name, ROUND(l.batter_run_elo_after, 0) as elo
  FROM latest_elo l
  LEFT JOIN players p ON l.batter_id = p.player_id
  WHERE rn = 1
  ORDER BY l.batter_run_elo_after DESC
  LIMIT 10
")
print(top_batters)

cat("\n=== Top 10 Bowlers (by final run ELO - higher is better for bowlers economically) ===\n")
top_bowlers <- dbGetQuery(conn, "
  WITH latest_elo AS (
    SELECT bowler_id, bowler_run_elo_after,
           ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY delivery_id DESC) as rn
    FROM mens_t20_3way_elo
  )
  SELECT l.bowler_id, p.player_name, ROUND(l.bowler_run_elo_after, 0) as elo
  FROM latest_elo l
  LEFT JOIN players p ON l.bowler_id = p.player_id
  WHERE rn = 1
  ORDER BY l.bowler_run_elo_after DESC
  LIMIT 10
")
print(top_bowlers)

cat("\n=== ELO Distribution (batters) ===\n")
elo_dist <- dbGetQuery(conn, "
  WITH latest_elo AS (
    SELECT batter_id, batter_run_elo_after,
           ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY delivery_id DESC) as rn
    FROM mens_t20_3way_elo
  )
  SELECT
    ROUND(MIN(batter_run_elo_after), 0) as min_elo,
    ROUND(AVG(batter_run_elo_after), 0) as mean_elo,
    ROUND(MEDIAN(batter_run_elo_after), 0) as median_elo,
    ROUND(MAX(batter_run_elo_after), 0) as max_elo
  FROM latest_elo
  WHERE rn = 1
")
print(elo_dist)

dbDisconnect(conn, shutdown = TRUE)
