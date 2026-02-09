# Test neighbor degree centrality - sum of opponents' degrees
# This captures: "Do you face opponents who are well-connected?"

library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cli::cli_h1("Testing Neighbor Degree Centrality")

# Step 1: Get each bowler's degree (unique batters faced)
cli::cli_h2("Calculating bowler degrees")
bowler_degrees <- DBI::dbGetQuery(conn, "
  SELECT
    d.bowler_id,
    COUNT(DISTINCT d.batter_id) as unique_batters
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  WHERE m.gender = 'male'
    AND LOWER(m.match_type) IN ('t20', 'it20')
  GROUP BY d.bowler_id
")
cli::cli_alert_info("Calculated degrees for {nrow(bowler_degrees)} bowlers")

# Step 2: For each batter, sum their opponents' degrees
cli::cli_h2("Calculating batter neighbor-degree centrality")

# Get batter-bowler matchups with deliveries
matchups <- DBI::dbGetQuery(conn, "
  SELECT
    d.batter_id,
    d.bowler_id,
    COUNT(*) as deliveries
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  WHERE m.gender = 'male'
    AND LOWER(m.match_type) IN ('t20', 'it20')
  GROUP BY d.batter_id, d.bowler_id
")

# Join with bowler degrees
matchups <- merge(matchups, bowler_degrees, by = "bowler_id")

# Aggregate per batter
batter_scores <- aggregate(
  cbind(
    total_deliveries = deliveries,
    unique_bowlers = rep(1, nrow(matchups)),  # count unique bowlers
    neighbor_degree_sum = unique_batters,      # sum of bowler degrees
    neighbor_degree_weighted = deliveries * unique_batters  # weighted by deliveries
  ) ~ batter_id,
  data = matchups,
  FUN = sum
)

# Calculate averages
batter_scores$avg_opponent_degree <- batter_scores$neighbor_degree_sum / batter_scores$unique_bowlers
batter_scores$avg_opponent_degree_weighted <- batter_scores$neighbor_degree_weighted / batter_scores$total_deliveries

# Filter to 100+ deliveries
batter_scores <- batter_scores[batter_scores$total_deliveries >= 100, ]

# Get player names
player_names <- DBI::dbGetQuery(conn, sprintf("
  SELECT player_id, player_name, country FROM players
  WHERE player_id IN (%s)
", paste(sprintf("'%s'", batter_scores$batter_id), collapse = ", ")))

batter_scores <- merge(batter_scores, player_names, by.x = "batter_id", by.y = "player_id")

# Calculate percentiles for different measures
batter_scores$pct_unique_bowlers <- rank(batter_scores$unique_bowlers) / nrow(batter_scores) * 100
batter_scores$pct_neighbor_sum <- rank(batter_scores$neighbor_degree_sum) / nrow(batter_scores) * 100
batter_scores$pct_avg_neighbor <- rank(batter_scores$avg_opponent_degree) / nrow(batter_scores) * 100

# Key players comparison
cli::cli_h2("Key Player Comparison")
key_players <- c("V Kohli", "CH Gayle", "DA Warner", "JC Buttler", "AB de Villiers",
                 "Taranjit Singh", "Manan Bashir", "Faisal Javed", "RG Sharma", "AD Hales")

key_df <- batter_scores[batter_scores$player_name %in% key_players, ]
key_df <- key_df[order(-key_df$avg_opponent_degree), ]

cli::cli_h3("Raw Values")
print(key_df[, c("player_name", "total_deliveries", "unique_bowlers",
                 "avg_opponent_degree")], row.names = FALSE)

cli::cli_h3("Percentile Rankings")
comparison <- data.frame(
  player = key_df$player_name,
  deliveries = key_df$total_deliveries,
  unique_opps = key_df$unique_bowlers,
  avg_opp_degree = round(key_df$avg_opponent_degree, 0),
  pct_unique = round(key_df$pct_unique_bowlers, 1),
  pct_neighbor = round(key_df$pct_avg_neighbor, 1)
)
print(comparison, row.names = FALSE)

# The key insight: average opponent degree
cli::cli_h2("Average Opponent Degree Distribution")
cli::cli_alert_info("Min avg opponent degree: {round(min(batter_scores$avg_opponent_degree), 0)}")
cli::cli_alert_info("Max avg opponent degree: {round(max(batter_scores$avg_opponent_degree), 0)}")
cli::cli_alert_info("Ratio: {round(max(batter_scores$avg_opponent_degree) / min(batter_scores$avg_opponent_degree), 1)}x")

# Top 15 by average opponent degree
cli::cli_h2("Top 15 by Average Opponent Degree")
top <- head(batter_scores[order(-batter_scores$avg_opponent_degree),
                          c("player_name", "country", "total_deliveries", "unique_bowlers",
                            "avg_opponent_degree", "pct_avg_neighbor")], 15)
names(top)[6] <- "percentile"
print(top, row.names = FALSE)

# Bottom 15 (isolated clusters)
cli::cli_h2("Bottom 15 by Average Opponent Degree (Isolated Clusters)")
bottom <- head(batter_scores[order(batter_scores$avg_opponent_degree),
                             c("player_name", "country", "total_deliveries", "unique_bowlers",
                               "avg_opponent_degree", "pct_avg_neighbor")], 15)
names(bottom)[6] <- "percentile"
print(bottom, row.names = FALSE)
