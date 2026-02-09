library(DBI)
library(duckdb)

conn <- dbConnect(duckdb(), '../bouncerdata/bouncer.duckdb', read_only = TRUE)

# Sample T20 events
cat('=== T20 Events (sample) ===\n')
events <- dbGetQuery(conn, "
  SELECT DISTINCT event_name, COUNT(*) as matches
  FROM matches
  WHERE match_type IN ('T20', 'IT20') AND event_name IS NOT NULL
  GROUP BY event_name
  ORDER BY matches DESC
  LIMIT 30
")
print(events)

# Check centrality by event - calculate average centrality per event
cat('\n=== Average Centrality by Event (Top T20 events) ===\n')
event_centrality <- dbGetQuery(conn, "
  WITH player_debuts AS (
    SELECT
      d.batter_id as player_id,
      m.event_name,
      MIN(m.match_date) as debut_date
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    WHERE m.match_type IN ('T20', 'IT20')
      AND m.gender = 'male'
      AND m.event_name IS NOT NULL
    GROUP BY d.batter_id, m.event_name
  ),
  event_players AS (
    SELECT DISTINCT pd.event_name, pd.player_id
    FROM player_debuts pd
  ),
  latest_centrality AS (
    SELECT player_id, percentile
    FROM mens_t20_player_centrality_history
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM mens_t20_player_centrality_history)
      AND role = 'batter'
  )
  SELECT
    ep.event_name,
    COUNT(*) as n_players,
    ROUND(AVG(lc.percentile), 1) as avg_centrality,
    ROUND(MIN(lc.percentile), 1) as min_centrality,
    ROUND(MAX(lc.percentile), 1) as max_centrality
  FROM event_players ep
  LEFT JOIN latest_centrality lc ON ep.player_id = lc.player_id
  GROUP BY ep.event_name
  HAVING COUNT(*) >= 20
  ORDER BY avg_centrality DESC
  LIMIT 30
")
print(event_centrality)

# Now show LOW centrality events (isolated leagues)
cat('\n=== LOWEST Centrality Events (Isolated Leagues) ===\n')
low_centrality <- dbGetQuery(conn, "
  WITH player_debuts AS (
    SELECT
      d.batter_id as player_id,
      m.event_name,
      MIN(m.match_date) as debut_date
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    WHERE m.match_type IN ('T20', 'IT20')
      AND m.gender = 'male'
      AND m.event_name IS NOT NULL
    GROUP BY d.batter_id, m.event_name
  ),
  event_players AS (
    SELECT DISTINCT pd.event_name, pd.player_id
    FROM player_debuts pd
  ),
  latest_centrality AS (
    SELECT player_id, percentile
    FROM mens_t20_player_centrality_history
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM mens_t20_player_centrality_history)
      AND role = 'batter'
  )
  SELECT
    ep.event_name,
    COUNT(*) as n_players,
    ROUND(AVG(lc.percentile), 1) as avg_centrality,
    ROUND(MIN(lc.percentile), 1) as min_centrality,
    ROUND(MAX(lc.percentile), 1) as max_centrality
  FROM event_players ep
  LEFT JOIN latest_centrality lc ON ep.player_id = lc.player_id
  GROUP BY ep.event_name
  HAVING COUNT(*) >= 10
  ORDER BY avg_centrality ASC
  LIMIT 30
")
print(low_centrality)

# Major franchise leagues
cat('\n=== Major Franchise Leagues ===\n')
franchise <- dbGetQuery(conn, "
  WITH player_debuts AS (
    SELECT
      d.batter_id as player_id,
      m.event_name,
      MIN(m.match_date) as debut_date
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    WHERE m.match_type IN ('T20', 'IT20')
      AND m.gender = 'male'
      AND m.event_name IN (
        'Indian Premier League',
        'Big Bash League',
        'Pakistan Super League',
        'Caribbean Premier League',
        'Bangladesh Premier League',
        'SA20',
        'International League T20',
        'The Hundred Men''s Competition',
        'Lanka Premier League'
      )
    GROUP BY d.batter_id, m.event_name
  ),
  event_players AS (
    SELECT DISTINCT pd.event_name, pd.player_id
    FROM player_debuts pd
  ),
  latest_centrality AS (
    SELECT player_id, percentile
    FROM mens_t20_player_centrality_history
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM mens_t20_player_centrality_history)
      AND role = 'batter'
  )
  SELECT
    ep.event_name,
    COUNT(*) as n_players,
    ROUND(AVG(lc.percentile), 1) as avg_centrality
  FROM event_players ep
  LEFT JOIN latest_centrality lc ON ep.player_id = lc.player_id
  GROUP BY ep.event_name
  ORDER BY avg_centrality DESC
")
print(franchise)

dbDisconnect(conn, shutdown = TRUE)
