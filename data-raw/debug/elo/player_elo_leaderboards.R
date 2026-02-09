# Player ELO Leaderboards with Opponent Quality Metrics
# Run this script to get current player ELO rankings with centrality and opponent stats
#
# Outputs:
#   - batter_elos: Full batter leaderboard
#   - bowler_elos: Full bowler leaderboard
#   - summary stats printed to console

library(DBI)
devtools::load_all()

# Configuration ----
MIN_BALLS <- 50        # Minimum deliveries to include
TOP_N <- 500            # How many to show in console output
GENDER <- "mens"       # "mens" or "womens"
FORMAT <- "t20"        # "t20", "odi", or "test"

conn <- get_db_connection(read_only = TRUE)

cat("=== PLAYER ELO LEADERBOARDS ===\n")
cat(sprintf("Format: %s %s | Min balls: %d\n\n", toupper(GENDER), toupper(FORMAT), MIN_BALLS))

# Build table names dynamically
elo_table <- sprintf("%s_%s_3way_elo", GENDER, FORMAT)
centrality_table <- sprintf("%s_%s_player_centrality_history", GENDER, FORMAT)

# BATTERS ----
cat("Loading batter data...\n")
batter_query <- sprintf("
  WITH max_balls AS (
    SELECT batter_id, MAX(batter_balls) as max_balls
    FROM %s
    GROUP BY batter_id
  ),
  latest_centrality AS (
    SELECT player_id, percentile, quality_tier, unique_opponents,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM %s
    WHERE role = 'batter'
  ),
  -- Aggregate stats from deliveries
  batter_stats AS (
    SELECT batter_id,
           SUM(runs_batter) as total_runs,
           AVG(runs_batter) as avg_runs_per_ball,
           SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets
    FROM deliveries
    WHERE LOWER(match_type) IN ('t20', 'it20')
    GROUP BY batter_id
  ),
  -- Average opponent (bowler) ELO faced
  opponent_elo AS (
    SELECT batter_id,
           AVG(bowler_run_elo_before) as avg_opp_elo,
           COUNT(DISTINCT bowler_id) as unique_bowlers_faced
    FROM %s
    GROUP BY batter_id
  ),
  -- Average opponent (bowler) centrality
  opponent_centrality AS (
    SELECT d.batter_id,
           AVG(c.percentile) as avg_opp_centrality
    FROM (
      SELECT DISTINCT batter_id, bowler_id
      FROM %s
    ) d
    LEFT JOIN (
      SELECT player_id, percentile,
             ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
      FROM %s
      WHERE role = 'bowler'
    ) c ON d.bowler_id = c.player_id AND c.rn = 1
    GROUP BY d.batter_id
  )
  SELECT e.batter_id,
         p.player_name,
         ROUND(e.batter_run_elo_after, 0) as run_elo,
         ROUND(e.batter_wicket_elo_after, 0) as wicket_elo,
         ROUND(c.percentile, 1) as centrality_pct,
         c.quality_tier,
         e.batter_balls as balls,
         s.total_runs,
         ROUND(s.avg_runs_per_ball, 3) as avg_rpb,
         ROUND(s.avg_runs_per_ball * 100, 1) as strike_rate,
         s.wickets,
         ROUND(s.wickets * 1.0 / e.batter_balls, 4) as wickets_per_ball,
         c.unique_opponents,
         ROUND(oe.avg_opp_elo, 0) as avg_opp_elo,
         oe.unique_bowlers_faced,
         ROUND(oc.avg_opp_centrality, 1) as avg_opp_centrality
  FROM %s e
  JOIN max_balls m ON e.batter_id = m.batter_id AND e.batter_balls = m.max_balls
  JOIN players p ON e.batter_id = p.player_id
  LEFT JOIN latest_centrality c ON e.batter_id = c.player_id AND c.rn = 1
  LEFT JOIN batter_stats s ON e.batter_id = s.batter_id
  LEFT JOIN opponent_elo oe ON e.batter_id = oe.batter_id
  LEFT JOIN opponent_centrality oc ON e.batter_id = oc.batter_id
  WHERE e.batter_balls >= %d
  ORDER BY e.batter_run_elo_after DESC
", elo_table, centrality_table, elo_table, elo_table, centrality_table, elo_table, MIN_BALLS)

batter_elos <- dbGetQuery(conn, batter_query)

# BOWLERS ----
cat("Loading bowler data...\n")
bowler_query <- sprintf("
  WITH max_balls AS (
    SELECT bowler_id, MAX(bowler_balls) as max_balls
    FROM %s
    GROUP BY bowler_id
  ),
  latest_centrality AS (
    SELECT player_id, percentile, quality_tier, unique_opponents,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM %s
    WHERE role = 'bowler'
  ),
  -- Aggregate stats from deliveries
  bowler_stats AS (
    SELECT bowler_id,
           SUM(runs_batter + runs_extras) as runs_conceded,
           AVG(runs_batter + runs_extras) as avg_runs_per_ball,
           SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets
    FROM deliveries
    WHERE LOWER(match_type) IN ('t20', 'it20')
    GROUP BY bowler_id
  ),
  -- Average opponent (batter) ELO faced
  opponent_elo AS (
    SELECT bowler_id,
           AVG(batter_run_elo_before) as avg_opp_elo,
           COUNT(DISTINCT batter_id) as unique_batters_faced
    FROM %s
    GROUP BY bowler_id
  ),
  -- Average opponent (batter) centrality
  opponent_centrality AS (
    SELECT d.bowler_id,
           AVG(c.percentile) as avg_opp_centrality
    FROM (
      SELECT DISTINCT bowler_id, batter_id
      FROM %s
    ) d
    LEFT JOIN (
      SELECT player_id, percentile,
             ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
      FROM %s
      WHERE role = 'batter'
    ) c ON d.batter_id = c.player_id AND c.rn = 1
    GROUP BY d.bowler_id
  )
  SELECT e.bowler_id,
         p.player_name,
         ROUND(e.bowler_run_elo_after, 0) as run_elo,
         ROUND(e.bowler_wicket_elo_after, 0) as wicket_elo,
         ROUND(c.percentile, 1) as centrality_pct,
         c.quality_tier,
         e.bowler_balls as balls,
         s.runs_conceded,
         s.wickets,
         ROUND(s.avg_runs_per_ball, 3) as avg_rpb,
         ROUND(s.avg_runs_per_ball * 6, 2) as economy,
         c.unique_opponents,
         ROUND(oe.avg_opp_elo, 0) as avg_opp_elo,
         oe.unique_batters_faced,
         ROUND(oc.avg_opp_centrality, 1) as avg_opp_centrality
  FROM %s e
  JOIN max_balls m ON e.bowler_id = m.bowler_id AND e.bowler_balls = m.max_balls
  JOIN players p ON e.bowler_id = p.player_id
  LEFT JOIN latest_centrality c ON e.bowler_id = c.player_id AND c.rn = 1
  LEFT JOIN bowler_stats s ON e.bowler_id = s.bowler_id
  LEFT JOIN opponent_elo oe ON e.bowler_id = oe.bowler_id
  LEFT JOIN opponent_centrality oc ON e.bowler_id = oc.bowler_id
  WHERE e.bowler_balls >= %d
  ORDER BY e.bowler_run_elo_after DESC
", elo_table, centrality_table, elo_table, elo_table, centrality_table, elo_table, MIN_BALLS)

bowler_elos <- dbGetQuery(conn, bowler_query)

dbDisconnect(conn, shutdown = TRUE)

# Display Results ----
cat("\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")
cat(sprintf("TOP %d BATTERS BY RUN ELO\n", TOP_N))
cat("=" |> rep(60) |> paste(collapse = ""), "\n")

# Select key columns for display
batter_display <- batter_elos[1:min(TOP_N, nrow(batter_elos)),
  c("player_name", "run_elo", "centrality_pct", "balls", "strike_rate",
    "avg_opp_elo", "avg_opp_centrality")]
print(batter_display, row.names = FALSE)

cat("\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")
cat(sprintf("TOP %d BOWLERS BY RUN ELO (lower = better at restricting runs)\n", TOP_N))
cat("=" |> rep(60) |> paste(collapse = ""), "\n")

# For bowlers, lower run ELO is better - show bottom first
bowler_display <- bowler_elos[order(bowler_elos$run_elo),][1:min(TOP_N, nrow(bowler_elos)),
  c("player_name", "run_elo", "centrality_pct", "balls", "economy",
    "avg_opp_elo", "avg_opp_centrality")]
print(bowler_display, row.names = FALSE)

# Summary Statistics ----
cat("\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")
cat("SUMMARY STATISTICS\n")
cat("=" |> rep(60) |> paste(collapse = ""), "\n")

cat("\nBATTERS:\n")
cat(sprintf("  Total players: %d\n", nrow(batter_elos)))
cat(sprintf("  ELO range: %d to %d (mean: %d, median: %d)\n",
            round(min(batter_elos$run_elo)),
            round(max(batter_elos$run_elo)),
            round(mean(batter_elos$run_elo)),
            round(median(batter_elos$run_elo))))
cat(sprintf("  Avg opponent ELO range: %d to %d\n",
            round(min(batter_elos$avg_opp_elo, na.rm = TRUE)),
            round(max(batter_elos$avg_opp_elo, na.rm = TRUE))))

cat("\nBOWLERS:\n")
cat(sprintf("  Total players: %d\n", nrow(bowler_elos)))
cat(sprintf("  ELO range: %d to %d (mean: %d, median: %d)\n",
            round(min(bowler_elos$run_elo)),
            round(max(bowler_elos$run_elo)),
            round(mean(bowler_elos$run_elo)),
            round(median(bowler_elos$run_elo))))
cat(sprintf("  Avg opponent ELO range: %d to %d\n",
            round(min(bowler_elos$avg_opp_elo, na.rm = TRUE)),
            round(max(bowler_elos$avg_opp_elo, na.rm = TRUE))))

cat("\n=== Data loaded into: batter_elos, bowler_elos ===\n")
cat("Full columns available:\n")
cat("  Batters:", paste(names(batter_elos), collapse = ", "), "\n")
cat("  Bowlers:", paste(names(bowler_elos), collapse = ", "), "\n")
