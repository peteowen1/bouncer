# Analyze PageRank percentile distribution to understand why elite players rank lower than isolated cluster players
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cli::cli_h1("Understanding the PageRank Percentile Problem")

# How many players have PageRank above 99th percentile?
cli::cli_h2("Distribution of High PageRank Percentiles (Batters)")
high_pr <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, percentile, deliveries,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'batter'
  )
  SELECT
    CASE
      WHEN percentile >= 99 THEN '99%+'
      WHEN percentile >= 95 THEN '95-99%'
      WHEN percentile >= 90 THEN '90-95%'
      WHEN percentile >= 80 THEN '80-90%'
      ELSE 'Below 80%'
    END as percentile_band,
    COUNT(*) as players,
    ROUND(AVG(deliveries), 0) as avg_deliveries
  FROM latest_pr
  WHERE rn = 1
  GROUP BY 1
  ORDER BY 1 DESC
")
print(high_pr)

# Who are the 99%+ players?
cli::cli_h2("Players with 99%+ PageRank Percentile")
elite_pr <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, percentile, deliveries, pagerank,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'batter'
  )
  SELECT p.player_name, p.country,
         ROUND(pr.percentile, 1) as pct,
         pr.deliveries,
         ROUND(pr.pagerank * 1000000, 2) as pr_x1m
  FROM latest_pr pr
  JOIN players p ON pr.player_id = p.player_id
  WHERE pr.rn = 1 AND pr.percentile >= 99
  ORDER BY pr.percentile DESC, pr.deliveries DESC
  LIMIT 30
")
print(elite_pr, row.names = FALSE)

# What countries are these players from?
cli::cli_h2("Country breakdown of 99%+ percentile players")
country_breakdown <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, percentile, deliveries,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'batter'
  )
  SELECT p.country,
         COUNT(*) as players,
         ROUND(AVG(pr.deliveries), 0) as avg_deliveries
  FROM latest_pr pr
  JOIN players p ON pr.player_id = p.player_id
  WHERE pr.rn = 1 AND pr.percentile >= 99
  GROUP BY p.country
  ORDER BY players DESC
")
print(country_breakdown, row.names = FALSE)

# Compare with elite nations
cli::cli_h2("PageRank percentiles for known elite players")
elite_players <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, percentile, deliveries, pagerank,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'batter'
  )
  SELECT p.player_name, p.country,
         ROUND(pr.percentile, 1) as pct,
         pr.deliveries,
         ROUND(pr.pagerank * 1000000, 2) as pr_x1m
  FROM latest_pr pr
  JOIN players p ON pr.player_id = p.player_id
  WHERE pr.rn = 1
    AND p.player_name IN ('V Kohli', 'CH Gayle', 'DA Warner', 'JC Buttler', 'AB de Villiers',
                          'RG Sharma', 'KL Rahul', 'SK Raina', 'MS Dhoni', 'JE Root',
                          'SPD Smith', 'KS Williamson', 'BA Stokes', 'JM Bairstow')
  ORDER BY pr.percentile DESC
")
print(elite_players, row.names = FALSE)

# The real issue: how many players are in each "cluster"?
cli::cli_h2("Analyzing network connectivity")
cli::cli_alert_info("The issue: PageRank is calculated on disconnected graph components.")
cli::cli_alert_info("Players in small isolated clusters (Cyprus, Estonia) have inflated PageRank")
cli::cli_alert_info("because their importance circulates within a small closed loop.")
cli::cli_alert_info("")
cli::cli_alert_info("Solution options:")
cli::cli_alert_info("  1. Calculate percentiles WITHIN each connected component")
cli::cli_alert_info("  2. Weight PageRank by component size (penalize isolation)")
cli::cli_alert_info("  3. Filter to players with min deliveries against 'core' nations")
cli::cli_alert_info("  4. Use raw PageRank (not percentile) since it's naturally lower for isolated clusters")

# Check raw PageRank values
cli::cli_h2("Raw PageRank comparison (not percentile)")
raw_compare <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, percentile, deliveries, pagerank,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'batter'
  )
  SELECT p.player_name, p.country,
         pr.deliveries,
         ROUND(pr.pagerank * 1000000, 4) as raw_pr_x1m,
         ROUND(pr.percentile, 1) as percentile
  FROM latest_pr pr
  JOIN players p ON pr.player_id = p.player_id
  WHERE pr.rn = 1
    AND (p.player_name IN ('V Kohli', 'CH Gayle', 'Taranjit Singh', 'DA Warner', 'JC Buttler')
         OR pr.percentile >= 99.5)
  ORDER BY pr.pagerank DESC
  LIMIT 20
")
print(raw_compare, row.names = FALSE)
