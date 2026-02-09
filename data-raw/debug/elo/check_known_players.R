# Check ELOs of known international stars
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)

cat("=== Known Top Batters ===\n")
known_batters <- dbGetQuery(conn, "
  WITH latest_elo AS (
    SELECT e.batter_id, p.player_name, e.batter_run_elo_after, e.batter_wicket_elo_after,
           ROW_NUMBER() OVER (PARTITION BY e.batter_id ORDER BY e.delivery_id DESC) as rn
    FROM mens_t20_3way_elo e
    JOIN players p ON e.batter_id = p.player_id
  )
  SELECT batter_id, player_name,
         ROUND(batter_run_elo_after, 0) as run_elo,
         ROUND(batter_wicket_elo_after, 0) as wicket_elo
  FROM latest_elo
  WHERE rn = 1
    AND (LOWER(player_name) LIKE '%kohli%'
      OR LOWER(player_name) LIKE '%babar%'
      OR LOWER(player_name) LIKE '%buttler%'
      OR LOWER(player_name) LIKE '%suryakumar%'
      OR LOWER(player_name) LIKE '%de villiers%'
      OR LOWER(player_name) LIKE '%warner%'
      OR LOWER(player_name) LIKE '%gayle%'
      OR LOWER(player_name) LIKE '%maxwell%'
      OR LOWER(player_name) LIKE '%russell%')
  ORDER BY batter_run_elo_after DESC
  LIMIT 20
")
print(known_batters)

cat("\n=== Known Top Bowlers ===\n")
known_bowlers <- dbGetQuery(conn, "
  WITH latest_elo AS (
    SELECT e.bowler_id, p.player_name, e.bowler_run_elo_after, e.bowler_wicket_elo_after,
           ROW_NUMBER() OVER (PARTITION BY e.bowler_id ORDER BY e.delivery_id DESC) as rn
    FROM mens_t20_3way_elo e
    JOIN players p ON e.bowler_id = p.player_id
  )
  SELECT bowler_id, player_name,
         ROUND(bowler_run_elo_after, 0) as run_elo,
         ROUND(bowler_wicket_elo_after, 0) as wicket_elo
  FROM latest_elo
  WHERE rn = 1
    AND (LOWER(player_name) LIKE '%bumrah%'
      OR LOWER(player_name) LIKE '%rashid%'
      OR LOWER(player_name) LIKE '%narine%'
      OR LOWER(player_name) LIKE '%archer%'
      OR LOWER(player_name) LIKE '%starc%'
      OR LOWER(player_name) LIKE '%rabada%'
      OR LOWER(player_name) LIKE '%shami%'
      OR LOWER(player_name) LIKE '%chahal%')
  ORDER BY bowler_run_elo_after DESC
  LIMIT 20
")
print(known_bowlers)

cat("\n=== Top 10 by balls faced (batters with most experience) ===\n")
exp_batters <- dbGetQuery(conn, "
  WITH latest AS (
    SELECT e.batter_id, p.player_name, e.batter_run_elo_after, e.batter_balls,
           ROW_NUMBER() OVER (PARTITION BY e.batter_id ORDER BY e.delivery_id DESC) as rn
    FROM mens_t20_3way_elo e
    JOIN players p ON e.batter_id = p.player_id
  )
  SELECT player_name, ROUND(batter_run_elo_after, 0) as elo, batter_balls
  FROM latest
  WHERE rn = 1
  ORDER BY batter_balls DESC
  LIMIT 20
")
print(exp_batters)

cat("\n=== Top 10 by balls bowled (most experienced bowlers) ===\n")
exp_bowlers <- dbGetQuery(conn, "
  WITH latest AS (
    SELECT e.bowler_id, p.player_name, e.bowler_run_elo_after, e.bowler_balls,
           ROW_NUMBER() OVER (PARTITION BY e.bowler_id ORDER BY e.delivery_id DESC) as rn
    FROM mens_t20_3way_elo e
    JOIN players p ON e.bowler_id = p.player_id
  )
  SELECT player_name, ROUND(bowler_run_elo_after, 0) as elo, bowler_balls
  FROM latest
  WHERE rn = 1
  ORDER BY bowler_balls DESC
  LIMIT 20
")
print(exp_bowlers)

cat("\n=== Who is KH Pandya? Check their matches ===\n")
pandya <- dbGetQuery(conn, "
  SELECT DISTINCT d.event_name, d.match_date
  FROM deliveries d
  WHERE d.batter_id = '5b8c830e'
  ORDER BY d.match_date DESC
  LIMIT 10
")
print(pandya)

dbDisconnect(conn, shutdown = TRUE)
