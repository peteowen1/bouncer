# Test opponent quality approach to penalize isolated subgroups
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cli::cli_h1("Testing Opponent Quality Penalty Approach")

# Check: what is the average PageRank of opponents faced by each player?
# Players in isolated subgroups face mostly other isolated players (low PR)
# Elite players face elite opponents (high PR)

# First, get players and their countries
cli::cli_h2("Checking average opponent PageRank")

# Get latest PageRank for all batters (men's T20)
pr_data <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, pagerank, percentile,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'batter'
  )
  SELECT player_id, pagerank, percentile
  FROM latest_pr
  WHERE rn = 1 AND pagerank < 0.0005  -- Men's only (raw PR < 500 x 10^-6)
")

bowler_pr <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, pagerank,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'bowler'
  )
  SELECT player_id, pagerank
  FROM latest_pr
  WHERE rn = 1 AND pagerank < 0.0005  -- Men's only
")

# For each batter, calculate average PageRank of bowlers they faced
cli::cli_h2("Calculating average opponent PageRank for each batter")

# Get deliveries with bowler info
opponents <- DBI::dbGetQuery(conn, sprintf("
  SELECT d.batter_id, d.bowler_id, COUNT(*) as deliveries
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  WHERE m.gender = 'male'
    AND LOWER(m.match_type) IN ('t20', 'it20')
    AND d.batter_id IN (%s)
  GROUP BY d.batter_id, d.bowler_id
", paste(sprintf("'%s'", pr_data$player_id), collapse = ", ")))

# Join with bowler PageRank
opponents <- merge(opponents, bowler_pr, by.x = "bowler_id", by.y = "player_id", all.x = TRUE)
opponents$pagerank[is.na(opponents$pagerank)] <- 0  # Bowlers not in PR table

# Calculate weighted average opponent PR for each batter
avg_opp_pr <- aggregate(
  pagerank ~ batter_id,
  data = opponents,
  FUN = function(x) mean(x, na.rm = TRUE)
)
names(avg_opp_pr) <- c("player_id", "avg_opponent_pr")

# Merge with batter data
pr_data <- merge(pr_data, avg_opp_pr, by = "player_id", all.x = TRUE)

# Get player names
player_names <- DBI::dbGetQuery(conn, sprintf("
  SELECT player_id, player_name, country
  FROM players
  WHERE player_id IN (%s)
", paste(sprintf("'%s'", pr_data$player_id), collapse = ", ")))

pr_data <- merge(pr_data, player_names, by = "player_id")

# Sort by current percentile
pr_data <- pr_data[order(-pr_data$percentile), ]

cli::cli_h2("Top 20 by current percentile (with average opponent PR)")
print(pr_data[1:20, c("player_name", "country", "percentile", "avg_opponent_pr")], row.names = FALSE)

# Calculate normalized opponent quality
max_avg_opp <- max(pr_data$avg_opponent_pr, na.rm = TRUE)
pr_data$opp_quality <- pr_data$avg_opponent_pr / max_avg_opp

# Apply penalty: new_pr = raw_pr * opp_quality^0.5
pr_data$adjusted_pr <- pr_data$pagerank * (pr_data$opp_quality ^ 0.5)

# Recalculate percentile on adjusted PR
pr_data$new_percentile <- rank(pr_data$adjusted_pr) / nrow(pr_data) * 100

cli::cli_h2("Top 20 by ADJUSTED percentile (opponent quality penalty)")
pr_data_sorted <- pr_data[order(-pr_data$new_percentile), ]
print(pr_data_sorted[1:20, c("player_name", "country", "new_percentile", "avg_opponent_pr")], row.names = FALSE)

# Compare key players
cli::cli_h2("Key Player Comparison: Before vs After")
key_players <- c("V Kohli", "CH Gayle", "DA Warner", "JC Buttler", "AB de Villiers",
                 "Taranjit Singh", "Manan Bashir", "Faisal Javed", "FH Allen")
compare <- pr_data[pr_data$player_name %in% key_players,
                   c("player_name", "country", "percentile", "new_percentile", "avg_opponent_pr")]
compare$change <- round(compare$new_percentile - compare$percentile, 1)
compare <- compare[order(-compare$new_percentile), ]
print(compare, row.names = FALSE)
