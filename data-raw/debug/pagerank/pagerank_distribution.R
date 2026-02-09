# Analyze PageRank raw value distribution
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cli::cli_h1("PageRank Raw Value Distribution Analysis")

# Get raw PageRank distribution
cli::cli_h2("Raw PageRank Distribution (Men's T20 batters)")

# Filter to likely men's players (raw pagerank < 500 based on what we saw)
distribution <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, percentile, deliveries, pagerank,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'batter'
  )
  SELECT
    ROUND(pagerank * 1000000, 0) as raw_pr_x1m,
    COUNT(*) as players,
    ROUND(MIN(percentile), 1) as min_pctl,
    ROUND(MAX(percentile), 1) as max_pctl
  FROM latest_pr
  WHERE rn = 1 AND pagerank * 1000000 < 500  -- Men's players only
  GROUP BY 1
  ORDER BY 1 DESC
")
print(distribution[1:30, ], row.names = FALSE)

# Show players near Kohli's level (330-335) vs isolated cluster players (338-345)
cli::cli_h2("Players by PageRank band (Men's network)")

bands <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, percentile, deliveries, pagerank,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'batter'
  )
  SELECT
    CASE
      WHEN pagerank * 1000000 >= 340 THEN '340+ (isolated clusters)'
      WHEN pagerank * 1000000 >= 335 THEN '335-340'
      WHEN pagerank * 1000000 >= 330 THEN '330-335 (Kohli level)'
      WHEN pagerank * 1000000 >= 325 THEN '325-330'
      WHEN pagerank * 1000000 >= 320 THEN '320-325'
      ELSE 'Below 320'
    END as pagerank_band,
    COUNT(*) as players,
    ROUND(AVG(deliveries), 0) as avg_deliveries,
    ROUND(MIN(percentile), 1) as min_pctl,
    ROUND(MAX(percentile), 1) as max_pctl
  FROM latest_pr
  WHERE rn = 1 AND pagerank * 1000000 < 500  -- Men's players only
  GROUP BY 1
  ORDER BY 1 DESC
")
print(bands, row.names = FALSE)

# Who are the 340+ PageRank players (isolated clusters)?
cli::cli_h2("340+ PageRank players (isolated clusters)")
isolated <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, percentile, deliveries, pagerank,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'batter'
  )
  SELECT p.player_name, p.country,
         ROUND(pr.pagerank * 1000000, 2) as raw_pr,
         ROUND(pr.percentile, 1) as pctl,
         pr.deliveries
  FROM latest_pr pr
  JOIN players p ON pr.player_id = p.player_id
  WHERE pr.rn = 1
    AND pr.pagerank * 1000000 >= 340
    AND pr.pagerank * 1000000 < 500  -- Exclude women
  ORDER BY pr.pagerank DESC
  LIMIT 20
")
print(isolated, row.names = FALSE)

cli::cli_h2("Summary")
cli::cli_alert_info("The isolated cluster problem:")
cli::cli_alert_info("  - Main men's network: PageRank ~330 (Kohli, Gayle, Warner)")
cli::cli_alert_info("  - Isolated clusters: PageRank ~340+ (Cyprus, Estonia, Malta)")
cli::cli_alert_info("  - Small ~3% raw difference = large percentile gap")
cli::cli_alert_info("")
cli::cli_alert_info("The few players in isolated clusters (340+) get 99%+ percentile")
cli::cli_alert_info("because they sit above the entire main network distribution")
