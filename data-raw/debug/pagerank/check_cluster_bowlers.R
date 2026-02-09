# Check if bowlers in isolated clusters also have inflated PageRank
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cli::cli_h1("Checking Bowler PageRank in Isolated Clusters")

# Get bowler PageRank for isolated nations
cli::cli_h2("Bowlers from isolated cricket nations")
isolated_bowlers <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, percentile, deliveries, pagerank,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'bowler'
  )
  SELECT p.player_name, p.country,
         ROUND(pr.pagerank * 1000000, 2) as raw_pr,
         ROUND(pr.percentile, 1) as pctl,
         pr.deliveries
  FROM latest_pr pr
  JOIN players p ON pr.player_id = p.player_id
  WHERE pr.rn = 1
    AND p.country IN ('Cyprus', 'Estonia', 'Bulgaria', 'Malta', 'Qatar', 'Kuwait', 'Sweden')
  ORDER BY pr.pagerank DESC
  LIMIT 20
")
print(isolated_bowlers, row.names = FALSE)

# Compare with elite bowlers
cli::cli_h2("Elite international bowlers")
elite_bowlers <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, percentile, deliveries, pagerank,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'bowler'
  )
  SELECT p.player_name, p.country,
         ROUND(pr.pagerank * 1000000, 2) as raw_pr,
         ROUND(pr.percentile, 1) as pctl,
         pr.deliveries
  FROM latest_pr pr
  JOIN players p ON pr.player_id = p.player_id
  WHERE pr.rn = 1
    AND p.player_name IN ('JJ Bumrah', 'Rashid Khan', 'JR Hazlewood', 'TA Boult',
                          'KA Pollard', 'DJ Bravo', 'SP Narine', 'Imran Tahir',
                          'YS Chahal', 'K Rabada')
  ORDER BY pr.pagerank DESC
")
print(elite_bowlers, row.names = FALSE)

# What's the bowler PageRank distribution?
cli::cli_h2("Bowler PageRank distribution (men's network)")
bowler_dist <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, percentile, deliveries, pagerank,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'bowler'
  )
  SELECT
    CASE
      WHEN pagerank * 1000000 >= 340 THEN '340+ (isolated?)'
      WHEN pagerank * 1000000 >= 335 THEN '335-340'
      WHEN pagerank * 1000000 >= 330 THEN '330-335'
      WHEN pagerank * 1000000 >= 325 THEN '325-330'
      ELSE 'Below 325'
    END as pagerank_band,
    COUNT(*) as players,
    ROUND(AVG(deliveries), 0) as avg_deliveries
  FROM latest_pr
  WHERE rn = 1 AND pagerank * 1000000 < 500  -- Exclude women's
  GROUP BY 1
  ORDER BY 1 DESC
")
print(bowler_dist, row.names = FALSE)
