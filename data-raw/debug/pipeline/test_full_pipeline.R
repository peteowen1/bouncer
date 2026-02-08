# Test the full PageRank pipeline with opponent quality normalization
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cli::cli_h1("Testing Full PageRank Pipeline with Opponent Quality Normalization")

# Load men's T20 data
cli::cli_h2("Loading men's T20 delivery data")
deliveries <- DBI::dbGetQuery(conn, "
  SELECT
    d.batter_id,
    d.bowler_id,
    d.runs_batter,
    d.is_wicket,
    m.match_type
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  WHERE m.gender = 'male'
    AND LOWER(m.match_type) IN ('t20', 'it20')
    AND d.batter_id IS NOT NULL
    AND d.bowler_id IS NOT NULL
")
cli::cli_alert_info("Loaded {nrow(deliveries)} deliveries")

deliveries$is_wicket_delivery <- deliveries$is_wicket

# Build matrices
cli::cli_h2("Building matrices")
matrices <- build_pagerank_matrices(deliveries, format = "all", min_deliveries = 100)

# Compute PageRank
cli::cli_h2("Computing PageRank")
pr_result <- compute_cricket_pagerank(
  matchup_matrix = matrices$matchup_matrix,
  performance_matrix = matrices$performance_matrix,
  wicket_matrix = matrices$wicket_matrix,
  damping = PAGERANK_DAMPING,
  verbose = TRUE
)

# Calculate opponent quality
cli::cli_h2("Calculating average opponent PageRank")
avg_opp_pr <- calculate_avg_opponent_pagerank(
  matrices$matchup_matrix,
  pr_result$batter_pagerank,
  pr_result$bowler_pagerank
)

# Classify with opponent quality normalization
cli::cli_h2("Classifying with opponent quality normalization")
batter_df <- classify_pagerank_tiers(
  pr_result$batter_pagerank,
  n_players = length(matrices$batter_ids),
  avg_opponent_pr = avg_opp_pr$batter_avg_opp_pr
)
batter_df$deliveries <- matrices$batter_deliveries[batter_df$player_id]

# Get player names
player_names <- DBI::dbGetQuery(conn, sprintf("
  SELECT player_id, player_name, country
  FROM players
  WHERE player_id IN (%s)
", paste(sprintf("'%s'", batter_df$player_id), collapse = ", ")))

batter_df <- merge(batter_df, player_names, by = "player_id")

# Show results
cli::cli_h2("Top 20 batters by PageRank percentile (with opponent quality normalization)")
top_batters <- head(batter_df[order(-batter_df$percentile),
                              c("player_name", "country", "percentile", "deliveries")], 20)
print(top_batters, row.names = FALSE)

# Show key players
cli::cli_h2("Key players comparison")
key_players <- c("V Kohli", "CH Gayle", "DA Warner", "JC Buttler", "AB de Villiers",
                 "Taranjit Singh", "Manan Bashir", "Faisal Javed", "RG Sharma", "MS Dhoni")
key_df <- batter_df[batter_df$player_name %in% key_players,
                    c("player_name", "country", "percentile", "deliveries")]
key_df <- key_df[order(-key_df$percentile), ]
print(key_df, row.names = FALSE)
