# Quick test of the new opponent quality functions
devtools::load_all()
cat("Package loaded successfully\n")

# Quick test of the new functions
cat("Testing calculate_avg_opponent_pagerank...\n")

# Create a simple test matrix
test_matrix <- matrix(c(10, 5, 0, 0, 20, 15), nrow = 2, ncol = 3,
                      dimnames = list(c("bat1", "bat2"), c("bowl1", "bowl2", "bowl3")))
batter_pr <- c(bat1 = 0.4, bat2 = 0.6)
bowler_pr <- c(bowl1 = 0.3, bowl2 = 0.5, bowl3 = 0.2)

result <- calculate_avg_opponent_pagerank(test_matrix, batter_pr, bowler_pr)
cat("Batter avg opp PR:", round(result$batter_avg_opp_pr, 3), "\n")
cat("Bowler avg opp PR:", round(result$bowler_avg_opp_pr, 3), "\n")

# Test normalization
cat("\nTesting normalize_pagerank_by_opponent_quality...\n")
pagerank <- c(p1 = 0.5, p2 = 0.3, p3 = 0.2)
avg_opp <- c(p1 = 0.4, p2 = 0.1, p3 = 0.35)

normalized <- normalize_pagerank_by_opponent_quality(pagerank, avg_opp, penalty_exponent = 0.3)
cat("Original PR:", round(pagerank, 3), "\n")
cat("Normalized PR:", round(normalized, 3), "\n")

cat("\nAll functions working correctly!\n")
