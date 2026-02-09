# Test the component detection and normalization fix
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cli::cli_h1("Testing Component Detection and Normalization")

# Load sample men's T20 data
cli::cli_h2("Loading sample men's T20 delivery data")
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

# Build matrices
cli::cli_h2("Building matchup matrices")
deliveries$is_wicket_delivery <- deliveries$is_wicket
matrices <- build_pagerank_matrices(deliveries, format = "all", min_deliveries = 100)

# Find components
cli::cli_h2("Finding connected components")
components <- find_bipartite_components(matrices$matchup_matrix)

cli::cli_alert_success("Found {components$n_components} connected components")
cli::cli_alert_info("Component sizes:")
print(sort(components$component_sizes, decreasing = TRUE)[1:min(10, length(components$component_sizes))])

# Compute PageRank
cli::cli_h2("Computing PageRank")
pr_result <- compute_cricket_pagerank(
  matchup_matrix = matrices$matchup_matrix,
  performance_matrix = matrices$performance_matrix,
  wicket_matrix = matrices$wicket_matrix,
  damping = PAGERANK_DAMPING,
  verbose = FALSE
)

# Test WITHOUT normalization (old behavior)
cli::cli_h2("WITHOUT Component Normalization (old behavior)")
batter_df_old <- classify_pagerank_tiers(
  pr_result$batter_pagerank,
  n_players = length(matrices$batter_ids)
)
batter_df_old$deliveries <- matrices$batter_deliveries[batter_df_old$player_id]

# Join with player names
batter_names <- DBI::dbGetQuery(conn, sprintf("
  SELECT player_id, player_name, country
  FROM players
  WHERE player_id IN (%s)
", paste(sprintf("'%s'", batter_df_old$player_id), collapse = ", ")))

batter_df_old <- merge(batter_df_old, batter_names, by = "player_id")
top_old <- head(batter_df_old[order(-batter_df_old$percentile), c("player_name", "country", "percentile", "deliveries")], 15)
cli::cli_alert_info("Top 15 batters by percentile (OLD):")
print(top_old, row.names = FALSE)

# Test WITH normalization (new behavior)
cli::cli_h2("WITH Component Normalization (new behavior)")
batter_df_new <- classify_pagerank_tiers(
  pr_result$batter_pagerank,
  n_players = length(matrices$batter_ids),
  player_component = components$batter_component,
  component_sizes = components$component_sizes
)
batter_df_new$deliveries <- matrices$batter_deliveries[batter_df_new$player_id]
batter_df_new <- merge(batter_df_new, batter_names, by = "player_id")

top_new <- head(batter_df_new[order(-batter_df_new$percentile), c("player_name", "country", "percentile", "deliveries")], 15)
cli::cli_alert_info("Top 15 batters by percentile (NEW):")
print(top_new, row.names = FALSE)

# Compare key players
cli::cli_h2("Key Player Comparison")
key_players <- c("V Kohli", "CH Gayle", "DA Warner", "JC Buttler", "AB de Villiers",
                 "Taranjit Singh", "Manan Bashir", "Faisal Javed")
compare_old <- batter_df_old[batter_df_old$player_name %in% key_players, c("player_name", "country", "percentile")]
compare_new <- batter_df_new[batter_df_new$player_name %in% key_players, c("player_name", "country", "percentile")]
names(compare_old)[3] <- "pctl_old"
names(compare_new)[3] <- "pctl_new"

compare <- merge(compare_old, compare_new[, c("player_name", "pctl_new")], by = "player_name")
compare$change <- round(compare$pctl_new - compare$pctl_old, 1)
compare <- compare[order(-compare$pctl_new), ]
cli::cli_alert_info("Before vs After normalization:")
print(compare, row.names = FALSE)
