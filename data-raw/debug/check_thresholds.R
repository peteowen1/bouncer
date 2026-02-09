# Quick check of player counts at different ball thresholds
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

cat("Checking player counts at different thresholds (mens T20)...\n\n")

result <- DBI::dbGetQuery(conn, "
  WITH batter_counts AS (
    SELECT batter_id, COUNT(*) as balls
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    WHERE LOWER(d.match_type) IN ('t20', 'it20')
      AND m.gender = 'male'
    GROUP BY batter_id
  )
  SELECT
    COUNT(*) FILTER (WHERE balls >= 1) as t1,
    COUNT(*) FILTER (WHERE balls >= 6) as t6,
    COUNT(*) FILTER (WHERE balls >= 10) as t10,
    COUNT(*) FILTER (WHERE balls >= 20) as t20,
    COUNT(*) FILTER (WHERE balls >= 50) as t50,
    COUNT(*) FILTER (WHERE balls >= 100) as t100
  FROM batter_counts
")
cat("Batters:\n")
print(result)

result2 <- DBI::dbGetQuery(conn, "
  WITH bowler_counts AS (
    SELECT bowler_id, COUNT(*) as balls
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    WHERE LOWER(d.match_type) IN ('t20', 'it20')
      AND m.gender = 'male'
    GROUP BY bowler_id
  )
  SELECT
    COUNT(*) FILTER (WHERE balls >= 1) as t1,
    COUNT(*) FILTER (WHERE balls >= 6) as t6,
    COUNT(*) FILTER (WHERE balls >= 10) as t10,
    COUNT(*) FILTER (WHERE balls >= 20) as t20,
    COUNT(*) FILTER (WHERE balls >= 50) as t50,
    COUNT(*) FILTER (WHERE balls >= 100) as t100
  FROM bowler_counts
")
cat("\nBowlers:\n")
print(result2)

# Matrix size implications
cat("\n\nMatrix memory (dense, 3 matrices at 8 bytes each):\n")
bat <- as.numeric(result[1,])
bowl <- as.numeric(result2[1,])
thresholds <- c(1, 6, 10, 20, 50, 100)
for (i in seq_along(thresholds)) {
  size_gb <- (bat[i] * bowl[i] * 8 * 3) / 1e9
  cat(sprintf("Threshold %3d: %6d batters x %6d bowlers = %.2f GB\n",
              thresholds[i], bat[i], bowl[i], size_gb))
}
