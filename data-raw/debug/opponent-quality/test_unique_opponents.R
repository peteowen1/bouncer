# Test unique opponents approach to penalize isolated clusters
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cli::cli_h1("Testing Unique Opponents Penalty Approach")

# Get unique opponent counts for each batter
cli::cli_h2("Calculating unique opponents for each batter")
unique_opps <- DBI::dbGetQuery(conn, "
  WITH batter_bowlers AS (
    SELECT d.batter_id, COUNT(DISTINCT d.bowler_id) as unique_bowlers
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    WHERE m.gender = 'male' AND LOWER(m.match_type) IN ('t20', 'it20')
    GROUP BY d.batter_id
  )
  SELECT b.*, p.player_name, p.country
  FROM batter_bowlers b
  JOIN players p ON b.batter_id = p.player_id
")

cli::cli_alert_info("Loaded {nrow(unique_opps)} batters with unique opponent counts")

# Get latest PageRank
pr_data <- DBI::dbGetQuery(conn, "
  WITH latest_pr AS (
    SELECT player_id, pagerank, percentile,
           ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) as rn
    FROM t20_player_pagerank_history
    WHERE role = 'batter'
  )
  SELECT player_id, pagerank, percentile
  FROM latest_pr
  WHERE rn = 1 AND pagerank < 0.0005
")

# Merge
combined <- merge(unique_opps, pr_data, by.x = "batter_id", by.y = "player_id")

cli::cli_h2("Unique Opponents Distribution")
summary(combined$unique_bowlers)

# Calculate opponent diversity penalty
max_opps <- max(combined$unique_bowlers)
combined$opp_diversity <- combined$unique_bowlers / max_opps

# Apply penalty with exponent 0.3
combined$adjusted_pr <- combined$pagerank * (combined$opp_diversity ^ 0.3)
combined$new_percentile <- rank(combined$adjusted_pr) / nrow(combined) * 100

cli::cli_h2("Key Players - Unique Opponents and Adjusted Percentile")
key_players <- c("V Kohli", "CH Gayle", "DA Warner", "JC Buttler", "AB de Villiers",
                 "Taranjit Singh", "Manan Bashir", "Faisal Javed", "RG Sharma")
key_df <- combined[combined$player_name %in% key_players,
                   c("player_name", "country", "unique_bowlers", "percentile", "new_percentile")]
key_df$change <- round(key_df$new_percentile - key_df$percentile, 1)
key_df <- key_df[order(-key_df$new_percentile), ]
print(key_df, row.names = FALSE)

# Top 15 after adjustment
cli::cli_h2("Top 15 by Adjusted Percentile")
top_adj <- head(combined[order(-combined$new_percentile),
                         c("player_name", "country", "unique_bowlers", "new_percentile")], 15)
print(top_adj, row.names = FALSE)

# Bottom of original top (isolated cluster players)
cli::cli_h2("Where isolated cluster players end up")
isolated <- combined[combined$player_name %in% c("Taranjit Singh", "Manan Bashir", "Faisal Javed", "Mehboob Ali"),
                     c("player_name", "country", "unique_bowlers", "percentile", "new_percentile", "change")]
print(isolated, row.names = FALSE)
