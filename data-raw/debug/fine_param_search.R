# Fine-grained parameter search for MOV resource calculation
# Run from bouncer/ directory
#
# Validation approach:
#   For EVERY BALL, calculate projected final score = current_score / (1 - resources_remaining%)
#   Compare to ACTUAL final score for that innings
#   RMSE measures how well resources predict the final outcome at each point
#
# For Tests: only use wickets (balls aren't a constraint), only all-out innings

library(dplyr)
devtools::load_all()

cat("Parameter Search for MOV Resource Calculation\n")
cat("==============================================\n")
cat("Comparing projected vs actual final score at every delivery\n\n")

conn <- get_db_connection(read_only = TRUE)

# Get ball-by-ball data with innings context
# Need: current score, balls bowled, wickets fallen, and final innings score
delivery_query <- "
  SELECT
    d.match_id,
    d.innings,
    d.over,
    d.ball,
    d.runs_batter + d.runs_extras as ball_runs,
    d.is_wicket,
    CASE
      WHEN m.match_type IN ('T20', 'IT20') THEN 't20'
      WHEN m.match_type IN ('ODI', 'ODM') THEN 'odi'
      WHEN m.match_type IN ('Test', 'MDM') THEN 'test'
    END as format,
    m.overs_per_innings,
    mi.total_runs as final_score,
    mi.total_wickets as final_wickets
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  JOIN match_innings mi ON d.match_id = mi.match_id AND d.innings = mi.innings
  WHERE m.match_type IN ('T20', 'IT20', 'ODI', 'ODM', 'Test', 'MDM')
    AND mi.total_runs IS NOT NULL
    AND mi.total_wickets IS NOT NULL
  ORDER BY d.match_id, d.innings, d.over, d.ball
"

cat("Loading deliveries (this may take a minute)...\n")
all_deliveries <- DBI::dbGetQuery(conn, delivery_query)
DBI::dbDisconnect(conn)

cat(sprintf("Loaded %s deliveries\n\n", format(nrow(all_deliveries), big.mark = ",")))

# Calculate running totals per innings
cat("Calculating running totals...\n")
all_deliveries <- all_deliveries %>%
  group_by(match_id, innings) %>%
  mutate(
    ball_in_innings = row_number(),
    current_score = cumsum(ball_runs),
    wickets_fallen = cumsum(is_wicket),
    wickets_remaining = 10 - wickets_fallen
  ) %>%
  ungroup()

# For limited overs: calculate balls remaining
# For tests: only use all-out innings and only use wickets for resources
all_deliveries <- all_deliveries %>%
  mutate(
    max_balls = overs_to_balls(overs_per_innings),
    balls_remaining = pmax(0, max_balls - ball_in_innings)
  )

cat(sprintf("Format breakdown:\n"))
cat(sprintf("  T20: %s deliveries\n", format(sum(all_deliveries$format == "t20"), big.mark = ",")))
cat(sprintf("  ODI: %s deliveries\n", format(sum(all_deliveries$format == "odi"), big.mark = ",")))
cat(sprintf("  Test: %s deliveries\n\n", format(sum(all_deliveries$format == "test"), big.mark = ",")))

# RMSE calculation - compare projected final to actual final at every ball
calc_projection_rmse <- function(data, format, balls_power, wickets_power) {
  fmt_data <- data[data$format == format, ]

  # For tests, only use all-out innings (we know true final score)
  if (format == "test") {
    fmt_data <- fmt_data[fmt_data$final_wickets == 10, ]
  }

  # Filter out first 5 overs (30 balls) for T20/ODI - too much variance early
  if (format %in% c("t20", "odi")) {
    fmt_data <- fmt_data[fmt_data$ball_in_innings > 30, ]
  }

  if (nrow(fmt_data) < 1000) return(NA)

  # Calculate resource remaining at each ball
  wickets_pct <- fmt_data$wickets_remaining / 10
  wickets_factor <- wickets_pct ^ wickets_power

  if (format == "test") {
    # Tests: only wickets matter (time/balls not a hard constraint)
    resource_remaining <- wickets_factor
  } else {
    # Limited overs: balls and wickets both matter
    balls_pct <- fmt_data$balls_remaining / fmt_data$max_balls
    balls_factor <- balls_pct ^ balls_power
    resource_remaining <- balls_factor * wickets_factor
  }

  # Edge cases
  resource_remaining[fmt_data$wickets_remaining == 0] <- 0
  resource_remaining[fmt_data$balls_remaining == 0 & format != "test"] <- 0

  resource_used <- 1 - resource_remaining

  # Skip if too few resources used (unstable projection)
  valid <- resource_used > 0.05 & is.finite(resource_used)
  if (sum(valid) < 100) return(NA)

  # Project final score from current position
  projected_final <- fmt_data$current_score[valid] / resource_used[valid]
  actual_final <- fmt_data$final_score[valid]

  # RMSE as percentage of actual final score
  errors <- (projected_final - actual_final) / actual_final
  rmse_pct <- sqrt(mean(errors^2, na.rm = TRUE)) * 100

  return(rmse_pct)
}

# Store results for final summary
final_results <- list()

# Search each format
for (fmt in c("t20", "odi", "test")) {
  cat(sprintf("=== %s ===\n", toupper(fmt)))

  n_deliveries <- sum(all_deliveries$format == fmt)
  if (fmt == "test") {
    n_deliveries <- sum(all_deliveries$format == fmt & all_deliveries$final_wickets == 10)
    cat(sprintf("N = %s deliveries (all-out innings only)\n\n", format(n_deliveries, big.mark = ",")))
  } else {
    cat(sprintf("N = %s deliveries\n\n", format(n_deliveries, big.mark = ",")))
  }

  # Parameter ranges with cricket-logical constraints
  if (fmt == "test") {
    bp_range <- 1.0  # Not used for tests
    wp_range <- seq(1.00, 2.00, by = 0.05)  # Only wickets matter
  } else {
    bp_range <- seq(0.50, 1.00, by = 0.02)
    wp_range <- seq(1.00, 1.50, by = 0.02)
  }

  # Grid search with progress
  results <- expand.grid(bp = bp_range, wp = wp_range)
  n_combos <- nrow(results)
  cat(sprintf("Testing %d parameter combinations...\n", n_combos))

  results$rmse <- NA_real_
  for (i in seq_len(n_combos)) {
    results$rmse[i] <- calc_projection_rmse(all_deliveries, fmt, results$bp[i], results$wp[i])
    if (i %% 50 == 0 || i == n_combos) {
      cat(sprintf("\r  Progress: %d/%d (%.0f%%)", i, n_combos, 100 * i / n_combos))
    }
  }
  cat("\n\n")

  results <- results[order(results$rmse), ]

  best <- results[1, ]

  if (fmt == "test") {
    cat(sprintf("BEST: wp=%.2f, RMSE=%.2f%%\n\n", best$wp, best$rmse))
  } else {
    cat(sprintf("BEST: bp=%.2f, wp=%.2f, RMSE=%.2f%%\n\n", best$bp, best$wp, best$rmse))
  }

  # Show top 10
  cat("Top 10 parameter combinations:\n")
  if (fmt == "test") {
    cat(sprintf("%6s %8s\n", "WP", "RMSE%"))
    cat(paste(rep("-", 16), collapse = ""), "\n")
    for (i in 1:min(10, nrow(results))) {
      cat(sprintf("%6.2f %7.2f%%\n", results$wp[i], results$rmse[i]))
    }
  } else {
    cat(sprintf("%6s %6s %8s\n", "BP", "WP", "RMSE%"))
    cat(paste(rep("-", 22), collapse = ""), "\n")
    for (i in 1:min(10, nrow(results))) {
      cat(sprintf("%6.2f %6.2f %7.2f%%\n", results$bp[i], results$wp[i], results$rmse[i]))
    }
  }

  # Compare to linear baseline
  linear_rmse <- calc_projection_rmse(all_deliveries, fmt, 1.0, 1.0)
  cat(sprintf("\nLinear (1.0/1.0): %.2f%% RMSE\n", linear_rmse))
  cat(sprintf("Improvement: %.1f%% reduction vs linear\n", (1 - best$rmse/linear_rmse) * 100))

  cat(paste(rep("=", 40), collapse = ""), "\n\n")

  final_results[[fmt]] <- best
}

# Final summary
cat("\n")
cat("RECOMMENDED PARAMETERS\n")
cat("======================\n\n")
cat(sprintf("%-6s %6s %6s %8s\n", "Format", "BP", "WP", "RMSE%"))
cat(paste(rep("-", 30), collapse = ""), "\n")
for (fmt in c("t20", "odi", "test")) {
  r <- final_results[[fmt]]
  if (fmt == "test") {
    cat(sprintf("%-6s %6s %6.2f %7.2f%%\n", toupper(fmt), "n/a", r$wp, r$rmse))
  } else {
    cat(sprintf("%-6s %6.2f %6.2f %7.2f%%\n", toupper(fmt), r$bp, r$wp, r$rmse))
  }
}

cat("\n")
cat("INTERPRETATION\n")
cat("==============\n")
cat("RMSE% = average error in projected final score as % of actual\n")
cat("e.g., 15% RMSE means projections typically off by ~15% of final score\n\n")
cat("BALLS_POWER <= 1: Later balls worth more (scarcity)\n")
cat("WICKETS_POWER >= 1: Early wickets worth more (top order > tail)\n")
