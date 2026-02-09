# Test combined centrality: your degree × your opponents' average degree
# This captures both breadth (unique opponents) and quality (opponent connectedness)

library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cli::cli_h1("Testing Combined Centrality Measures")

# Get bowler degrees
bowler_degrees <- DBI::dbGetQuery(conn, "
  SELECT d.bowler_id, COUNT(DISTINCT d.batter_id) as unique_batters
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  WHERE m.gender = 'male' AND LOWER(m.match_type) IN ('t20', 'it20')
  GROUP BY d.bowler_id
")

# Get batter-bowler matchups
matchups <- DBI::dbGetQuery(conn, "
  SELECT d.batter_id, d.bowler_id, COUNT(*) as deliveries
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  WHERE m.gender = 'male' AND LOWER(m.match_type) IN ('t20', 'it20')
  GROUP BY d.batter_id, d.bowler_id
")

matchups <- merge(matchups, bowler_degrees, by = "bowler_id")

# Aggregate per batter
batter_scores <- aggregate(
  cbind(
    total_deliveries = deliveries,
    unique_bowlers = rep(1, nrow(matchups)),
    neighbor_degree_sum = unique_batters
  ) ~ batter_id,
  data = matchups,
  FUN = sum
)

batter_scores$avg_opponent_degree <- batter_scores$neighbor_degree_sum / batter_scores$unique_bowlers

# Filter to 100+ deliveries
batter_scores <- batter_scores[batter_scores$total_deliveries >= 100, ]

# Get player names
player_names <- DBI::dbGetQuery(conn, sprintf("
  SELECT player_id, player_name, country FROM players
  WHERE player_id IN (%s)
", paste(sprintf("'%s'", batter_scores$batter_id), collapse = ", ")))

batter_scores <- merge(batter_scores, player_names, by.x = "batter_id", by.y = "player_id")

# Calculate combined measures
cli::cli_h2("Calculating Combined Centrality Measures")

# 1. Pure unique opponents (degree)
batter_scores$c1_degree <- batter_scores$unique_bowlers

# 2. Pure avg opponent degree (neighbor quality)
batter_scores$c2_neighbor <- batter_scores$avg_opponent_degree

# 3. Product: degree × avg_neighbor
batter_scores$c3_product <- batter_scores$unique_bowlers * batter_scores$avg_opponent_degree

# 4. Geometric mean: sqrt(degree × avg_neighbor) - Opsahl with α=0.5
batter_scores$c4_geometric <- sqrt(batter_scores$unique_bowlers * batter_scores$avg_opponent_degree)

# 5. Opsahl α=0.3 (favor degree more): degree^0.7 × neighbor^0.3
batter_scores$c5_opsahl_03 <- (batter_scores$unique_bowlers^0.7) * (batter_scores$avg_opponent_degree^0.3)

# 6. Opsahl α=0.7 (favor neighbor more): degree^0.3 × neighbor^0.7
batter_scores$c6_opsahl_07 <- (batter_scores$unique_bowlers^0.3) * (batter_scores$avg_opponent_degree^0.7)

# Calculate percentiles
for (col in c("c1_degree", "c2_neighbor", "c3_product", "c4_geometric", "c5_opsahl_03", "c6_opsahl_07")) {
  batter_scores[[paste0(col, "_pct")]] <- rank(batter_scores[[col]]) / nrow(batter_scores) * 100
}

# Key players comparison
cli::cli_h2("Key Player Comparison - Percentile Rankings")
key_players <- c("V Kohli", "CH Gayle", "DA Warner", "JC Buttler", "AB de Villiers",
                 "Taranjit Singh", "Manan Bashir", "Faisal Javed", "RG Sharma", "AD Hales")

key_df <- batter_scores[batter_scores$player_name %in% key_players, ]

comparison <- data.frame(
  player = key_df$player_name,
  unique_opps = key_df$unique_bowlers,
  avg_opp_deg = round(key_df$avg_opponent_degree, 0),
  `degree_only` = round(key_df$c1_degree_pct, 1),
  `neighbor_only` = round(key_df$c2_neighbor_pct, 1),
  `geometric` = round(key_df$c4_geometric_pct, 1),
  `opsahl_0.3` = round(key_df$c5_opsahl_03_pct, 1),
  check.names = FALSE
)
comparison <- comparison[order(-comparison$geometric), ]
print(comparison, row.names = FALSE)

# Analysis of separation
cli::cli_h2("Elite vs Isolated Separation")
elite <- c("V Kohli", "CH Gayle", "DA Warner", "JC Buttler", "AB de Villiers")
isolated <- c("Taranjit Singh", "Manan Bashir", "Faisal Javed")

for (col in c("c1_degree_pct", "c2_neighbor_pct", "c4_geometric_pct", "c5_opsahl_03_pct")) {
  elite_avg <- mean(batter_scores[batter_scores$player_name %in% elite, col])
  isolated_avg <- mean(batter_scores[batter_scores$player_name %in% isolated, col])
  gap <- elite_avg - isolated_avg
  name <- gsub("_pct", "", col)
  cli::cli_alert_info("{name}: Elite={round(elite_avg,1)}%, Isolated={round(isolated_avg,1)}%, Gap={round(gap,1)}")
}

# Top 20 by geometric mean
cli::cli_h2("Top 20 by Geometric Mean (sqrt(degree × avg_neighbor_degree))")
top <- head(batter_scores[order(-batter_scores$c4_geometric),
            c("player_name", "country", "unique_bowlers", "avg_opponent_degree", "c4_geometric_pct")], 20)
names(top) <- c("player", "country", "unique_opps", "avg_opp_degree", "percentile")
top$avg_opp_degree <- round(top$avg_opp_degree, 0)
top$percentile <- round(top$percentile, 1)
print(top, row.names = FALSE)

# Bottom 10
cli::cli_h2("Bottom 10 (Isolated Clusters)")
bottom <- head(batter_scores[order(batter_scores$c4_geometric),
               c("player_name", "country", "unique_bowlers", "avg_opponent_degree", "c4_geometric_pct")], 10)
names(bottom) <- c("player", "country", "unique_opps", "avg_opp_degree", "percentile")
bottom$avg_opp_degree <- round(bottom$avg_opp_degree, 0)
bottom$percentile <- round(bottom$percentile, 1)
print(bottom, row.names = FALSE)
