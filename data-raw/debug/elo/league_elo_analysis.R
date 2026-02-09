library(DBI)
library(duckdb)

conn <- dbConnect(duckdb(), '../../../bouncerdata/bouncer.duckdb', read_only = TRUE)

# Average ELO and runs per delivery by event
cat('=== AVERAGE ELO & RUNS BY LEAGUE (Top 30 by matches) ===\n\n')
league_stats <- dbGetQuery(conn, "
  WITH event_stats AS (
    SELECT
      m.event_name,
      COUNT(DISTINCT e.match_id) as n_matches,
      COUNT(*) as n_deliveries,
      AVG(d.runs_batter + d.runs_extras) as avg_runs_per_ball,
      AVG(e.batter_run_elo_after) as avg_batter_elo,
      AVG(e.bowler_run_elo_after) as avg_bowler_elo
    FROM mens_t20_3way_elo e
    JOIN matches m ON e.match_id = m.match_id
    JOIN deliveries d ON e.delivery_id = d.delivery_id
    WHERE m.event_name IS NOT NULL
    GROUP BY m.event_name
    HAVING COUNT(DISTINCT e.match_id) >= 20
  ),
  event_centrality AS (
    SELECT
      m.event_name,
      AVG(c.percentile) as avg_centrality
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    JOIN mens_t20_player_centrality_history c
      ON d.batter_id = c.player_id
      AND c.role = 'batter'
      AND c.snapshot_date = (SELECT MAX(snapshot_date) FROM mens_t20_player_centrality_history)
    WHERE m.match_type IN ('T20', 'IT20') AND m.gender = 'male'
    GROUP BY m.event_name
  )
  SELECT
    es.event_name,
    es.n_matches,
    ROUND(es.avg_runs_per_ball, 3) as runs_per_ball,
    ROUND(es.avg_batter_elo, 0) as avg_batter_elo,
    ROUND(es.avg_bowler_elo, 0) as avg_bowler_elo,
    ROUND(ec.avg_centrality, 1) as avg_centrality
  FROM event_stats es
  LEFT JOIN event_centrality ec ON es.event_name = ec.event_name
  ORDER BY es.n_matches DESC
  LIMIT 30
")
print(league_stats, row.names = FALSE)

cat('\n\n=== CORRELATION: Runs vs ELO vs Centrality ===\n\n')

# Check correlation
if (nrow(league_stats) > 5) {
  complete_cases <- league_stats[complete.cases(league_stats), ]
  if (nrow(complete_cases) > 5) {
    cat('Correlation between runs_per_ball and avg_batter_elo:',
        round(cor(complete_cases$runs_per_ball, complete_cases$avg_batter_elo), 3), '\n')
    cat('Correlation between avg_centrality and avg_batter_elo:',
        round(cor(complete_cases$avg_centrality, complete_cases$avg_batter_elo), 3), '\n')
    cat('Correlation between runs_per_ball and avg_centrality:',
        round(cor(complete_cases$runs_per_ball, complete_cases$avg_centrality), 3), '\n')
  }
}

cat('\n\n=== LOW CENTRALITY LEAGUES (potential inflation sources) ===\n\n')
low_cent <- dbGetQuery(conn, "
  WITH event_stats AS (
    SELECT
      m.event_name,
      COUNT(DISTINCT e.match_id) as n_matches,
      AVG(d.runs_batter + d.runs_extras) as avg_runs_per_ball,
      AVG(e.batter_run_elo_after) as avg_batter_elo,
      MAX(e.batter_run_elo_after) as max_batter_elo
    FROM mens_t20_3way_elo e
    JOIN matches m ON e.match_id = m.match_id
    JOIN deliveries d ON e.delivery_id = d.delivery_id
    WHERE m.event_name IS NOT NULL
    GROUP BY m.event_name
    HAVING COUNT(DISTINCT e.match_id) >= 5
  ),
  event_centrality AS (
    SELECT
      m.event_name,
      AVG(c.percentile) as avg_centrality
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    JOIN mens_t20_player_centrality_history c
      ON d.batter_id = c.player_id
      AND c.role = 'batter'
      AND c.snapshot_date = (SELECT MAX(snapshot_date) FROM mens_t20_player_centrality_history)
    WHERE m.match_type IN ('T20', 'IT20') AND m.gender = 'male'
    GROUP BY m.event_name
  )
  SELECT
    es.event_name,
    es.n_matches,
    ROUND(es.avg_runs_per_ball, 3) as runs_per_ball,
    ROUND(es.avg_batter_elo, 0) as avg_batter_elo,
    ROUND(es.max_batter_elo, 0) as max_batter_elo,
    ROUND(ec.avg_centrality, 1) as avg_centrality
  FROM event_stats es
  LEFT JOIN event_centrality ec ON es.event_name = ec.event_name
  WHERE ec.avg_centrality < 30 OR ec.avg_centrality IS NULL
  ORDER BY es.avg_batter_elo DESC
  LIMIT 20
")
print(low_cent, row.names = FALSE)

dbDisconnect(conn, shutdown = TRUE)
