# Top 20 Men's T20 batters with ELO and PageRank
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

# Check pagerank table columns first
pr_cols <- DBI::dbGetQuery(conn, "SELECT column_name FROM information_schema.columns WHERE table_name = 't20_player_pagerank_history'")
cli::cli_alert_info("PageRank columns: {paste(pr_cols$column_name, collapse = ', ')}")

# Get top 20 batters by ELO with their latest PageRank
result <- DBI::dbGetQuery(conn, "
  WITH latest_elo AS (
    SELECT
      batter_id,
      batter_run_elo_after as elo,
      match_date,
      batter_balls as balls_faced,
      ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
    FROM mens_t20_3way_elo
  ),
  latest_pagerank AS (
    SELECT
      player_id,
      percentile as batter_percentile,
      snapshot_date,
      ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'batter'
  )
  SELECT
    p.player_name as name,
    p.country,
    ROUND(e.elo, 0) as elo,
    e.balls_faced as balls,
    ROUND(pr.batter_percentile, 1) as pr_pct,
    e.match_date as last_match
  FROM latest_elo e
  LEFT JOIN players p ON e.batter_id = p.player_id
  LEFT JOIN latest_pagerank pr ON e.batter_id = pr.player_id AND pr.rn = 1
  WHERE e.rn = 1
  ORDER BY e.elo DESC
  LIMIT 200000
")

cli::cli_h1("TOP 20 MENS T20 BATTERS - ELO vs PageRank")
cat("\n")
print(result, row.names = FALSE)

cat("\n")
cli::cli_alert_info("pr_pct = PageRank percentile (99 = elite, NULL = no data)")
cli::cli_alert_info("balls = total deliveries faced in dataset")
