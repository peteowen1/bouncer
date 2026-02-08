# Get REAL top players by using MAX batter_balls to find final ELO
library(DBI)
devtools::load_all()
conn <- get_db_connection(read_only = TRUE)

cat("=== REAL Top 20 Batters (by final ELO using max balls) ===\n")
real_top_batters <- dbGetQuery(conn, "
  WITH max_balls AS (
    SELECT batter_id, MAX(batter_balls) as max_balls
    FROM mens_t20_3way_elo
    GROUP BY batter_id
  )
  SELECT e.batter_id, p.player_name,
         ROUND(e.batter_run_elo_after, 0) as run_elo,
         e.batter_balls, e.match_date
  FROM mens_t20_3way_elo e
  JOIN max_balls m ON e.batter_id = m.batter_id AND e.batter_balls = m.max_balls
  JOIN players p ON e.batter_id = p.player_id
  ORDER BY e.batter_run_elo_after DESC
  LIMIT 20
")
print(real_top_batters)

cat("\n=== REAL Top 20 Bowlers (by final ELO using max balls) ===\n")
real_top_bowlers <- dbGetQuery(conn, "
  WITH max_balls AS (
    SELECT bowler_id, MAX(bowler_balls) as max_balls
    FROM mens_t20_3way_elo
    GROUP BY bowler_id
  )
  SELECT e.bowler_id, p.player_name,
         ROUND(e.bowler_run_elo_after, 0) as run_elo,
         e.bowler_balls, e.match_date
  FROM mens_t20_3way_elo e
  JOIN max_balls m ON e.bowler_id = m.bowler_id AND e.bowler_balls = m.max_balls
  JOIN players p ON e.bowler_id = p.player_id
  ORDER BY e.bowler_run_elo_after DESC
  LIMIT 20
")
print(real_top_bowlers)

cat("\n=== ELO Distribution (REAL final ELOs) ===\n")
elo_dist <- dbGetQuery(conn, "
  WITH max_balls AS (
    SELECT batter_id, MAX(batter_balls) as max_balls
    FROM mens_t20_3way_elo
    GROUP BY batter_id
  ),
  final_elos AS (
    SELECT e.batter_run_elo_after
    FROM mens_t20_3way_elo e
    JOIN max_balls m ON e.batter_id = m.batter_id AND e.batter_balls = m.max_balls
  )
  SELECT
    ROUND(MIN(batter_run_elo_after), 0) as min_elo,
    ROUND(AVG(batter_run_elo_after), 0) as mean_elo,
    ROUND(MEDIAN(batter_run_elo_after), 0) as median_elo,
    ROUND(MAX(batter_run_elo_after), 0) as max_elo
  FROM final_elos
")
print(elo_dist)

cat("\n=== Known stars final ELOs ===\n")
known_stars <- dbGetQuery(conn, "
  WITH max_balls AS (
    SELECT batter_id, MAX(batter_balls) as max_balls
    FROM mens_t20_3way_elo
    GROUP BY batter_id
  )
  SELECT p.player_name,
         ROUND(e.batter_run_elo_after, 0) as run_elo,
         e.batter_balls
  FROM mens_t20_3way_elo e
  JOIN max_balls m ON e.batter_id = m.batter_id AND e.batter_balls = m.max_balls
  JOIN players p ON e.batter_id = p.player_id
  WHERE e.batter_id IN (
    '5b8c830e',  -- KH Pandya
    'ba607b88',  -- Kohli
    '8a75e999',  -- Babar
    'c4487b84',  -- AB de Villiers
    'bbd41817',  -- Russell
    'b681e71e',  -- Maxwell
    'db584dad'   -- Gayle
  )
  ORDER BY e.batter_run_elo_after DESC
")
print(known_stars)

dbDisconnect(conn, shutdown = TRUE)
