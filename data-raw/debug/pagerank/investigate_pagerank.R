# Investigate PageRank anomalies
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cli::cli_h1("Investigating PageRank Issues")

# 1. Compare Taranjit Singh vs Michael Neser
cli::cli_h2("Comparing Taranjit Singh (Cyprus) vs MG Neser (Australia)")

# Get their PageRank history
pr_compare <- DBI::dbGetQuery(conn, "
  SELECT
    p.player_name,
    p.country,
    pr.snapshot_date,
    pr.role,
    ROUND(pr.pagerank, 6) as pagerank,
    ROUND(pr.percentile, 1) as percentile,
    pr.deliveries
  FROM t20_player_pagerank_history pr
  JOIN players p ON pr.player_id = p.player_id
  WHERE p.player_name IN ('Taranjit Singh', 'MG Neser')
  ORDER BY p.player_name, pr.snapshot_date DESC
")
print(pr_compare)

# 2. Check who these players have faced
cli::cli_h2("Who has Taranjit Singh faced? (bowlers)")
taranjit_opponents <- DBI::dbGetQuery(conn, "
  SELECT
    p.player_name as bowler,
    p.country,
    COUNT(*) as deliveries,
    m.event_name
  FROM deliveries d
  JOIN players p ON d.bowler_id = p.player_id
  JOIN matches m ON d.match_id = m.match_id
  WHERE d.batter_id = (SELECT player_id FROM players WHERE player_name = 'Taranjit Singh' LIMIT 1)
    AND m.gender = 'male'
    AND LOWER(m.match_type) IN ('t20', 'it20')
  GROUP BY p.player_name, p.country, m.event_name
  ORDER BY deliveries DESC
  LIMIT 15
")
print(taranjit_opponents)

cli::cli_h2("Who has MG Neser faced? (bowlers)")
neser_opponents <- DBI::dbGetQuery(conn, "
  SELECT
    p.player_name as bowler,
    p.country,
    COUNT(*) as deliveries,
    m.event_name
  FROM deliveries d
  JOIN players p ON d.bowler_id = p.player_id
  JOIN matches m ON d.match_id = m.match_id
  WHERE d.batter_id = (SELECT player_id FROM players WHERE player_name = 'MG Neser' LIMIT 1)
    AND m.gender = 'male'
    AND LOWER(m.match_type) IN ('t20', 'it20')
  GROUP BY p.player_name, p.country, m.event_name
  ORDER BY deliveries DESC
  LIMIT 15
")
print(neser_opponents)

# 3. Explain NA PageRanks
cli::cli_h2("Why are some PageRanks NA?")

# Check PageRank snapshot date range
pr_dates <- DBI::dbGetQuery(conn, "
  SELECT MIN(snapshot_date) as earliest, MAX(snapshot_date) as latest
  FROM t20_player_pagerank_history
")
cli::cli_alert_info("PageRank snapshots range: {pr_dates$earliest} to {pr_dates$latest}")

# Check minimum deliveries threshold
cli::cli_alert_info("PageRank requires minimum deliveries threshold (typically 100)")

# Example: TD Andrews has NA - why?
cli::cli_h3("Example: TD Andrews (NA PageRank)")
td_andrews <- DBI::dbGetQuery(conn, "
  SELECT
    p.player_name,
    p.country,
    COUNT(*) as total_deliveries,
    MIN(m.match_date) as first_match,
    MAX(m.match_date) as last_match
  FROM deliveries d
  JOIN players p ON d.batter_id = p.player_id
  JOIN matches m ON d.match_id = m.match_id
  WHERE p.player_name = 'TD Andrews'
    AND m.gender = 'male'
    AND LOWER(m.match_type) IN ('t20', 'it20')
  GROUP BY p.player_name, p.country
")
print(td_andrews)

# Check if TD Andrews is in pagerank history at all
td_in_pr <- DBI::dbGetQuery(conn, "
  SELECT COUNT(*) as snapshots
  FROM t20_player_pagerank_history pr
  JOIN players p ON pr.player_id = p.player_id
  WHERE p.player_name = 'TD Andrews'
")
cli::cli_alert_info("TD Andrews has {td_in_pr$snapshots} PageRank snapshots")
