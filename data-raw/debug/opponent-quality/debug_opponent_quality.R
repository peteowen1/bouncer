# Debug why opponent quality normalization isn't having much effect
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cli::cli_h1("Debugging Opponent Quality Normalization")

# Load data and compute PageRank
deliveries <- DBI::dbGetQuery(conn, "
  SELECT d.batter_id, d.bowler_id, d.runs_batter, d.is_wicket, m.match_type
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  WHERE m.gender = 'male' AND LOWER(m.match_type) IN ('t20', 'it20')
    AND d.batter_id IS NOT NULL AND d.bowler_id IS NOT NULL
")
deliveries$is_wicket_delivery <- deliveries$is_wicket

matrices <- build_pagerank_matrices(deliveries, format = "all", min_deliveries = 100)
pr_result <- compute_cricket_pagerank(
  matchup_matrix = matrices$matchup_matrix,
  performance_matrix = matrices$performance_matrix,
  wicket_matrix = matrices$wicket_matrix,
  damping = PAGERANK_DAMPING,
  verbose = FALSE
)

# Calculate opponent quality
avg_opp_pr <- calculate_avg_opponent_pagerank(
  matrices$matchup_matrix,
  pr_result$batter_pagerank,
  pr_result$bowler_pagerank
)

cli::cli_h2("Average Opponent PageRank Distribution (Batters)")
summary(avg_opp_pr$batter_avg_opp_pr)

# Check the range
cli::cli_alert_info("Min avg opp PR: {signif(min(avg_opp_pr$batter_avg_opp_pr), 4)}")
cli::cli_alert_info("Max avg opp PR: {signif(max(avg_opp_pr$batter_avg_opp_pr), 4)}")
cli::cli_alert_info("Ratio (max/min): {signif(max(avg_opp_pr$batter_avg_opp_pr) / min(avg_opp_pr$batter_avg_opp_pr), 3)}")

# The issue: if the range is small, normalization won't have much effect
# Let's check specific players
player_names <- DBI::dbGetQuery(conn, sprintf("
  SELECT player_id, player_name, country FROM players
  WHERE player_id IN (%s)
", paste(sprintf("'%s'", names(avg_opp_pr$batter_avg_opp_pr)), collapse = ", ")))

opp_df <- data.frame(
  player_id = names(avg_opp_pr$batter_avg_opp_pr),
  avg_opp_pr = avg_opp_pr$batter_avg_opp_pr,
  raw_pr = pr_result$batter_pagerank[names(avg_opp_pr$batter_avg_opp_pr)]
)
opp_df <- merge(opp_df, player_names, by = "player_id")

cli::cli_h2("Key Players - Average Opponent PageRank")
key_players <- c("V Kohli", "CH Gayle", "DA Warner", "JC Buttler",
                 "Taranjit Singh", "Manan Bashir", "Faisal Javed")
key_df <- opp_df[opp_df$player_name %in% key_players, ]
key_df$opp_normalized <- key_df$avg_opp_pr / max(opp_df$avg_opp_pr)
key_df <- key_df[order(-key_df$avg_opp_pr), c("player_name", "country", "avg_opp_pr", "opp_normalized", "raw_pr")]
print(key_df, row.names = FALSE)

# The problem: check if isolated players face bowlers with LOW PageRank
cli::cli_h2("Problem Analysis")
cli::cli_alert_info("The issue: avg_opp_pr range is too narrow")
cli::cli_alert_info("Kohli vs Taranjit have similar avg_opp_pr because bowler PageRanks are similar")
cli::cli_alert_info("We need to use raw bowler PageRank, not the normalized one")

# Check bowler PageRank distribution
cli::cli_h2("Bowler PageRank Distribution")
summary(pr_result$bowler_pagerank)
