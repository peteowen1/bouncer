# Comprehensive analysis of 3-Way ELO ratings across all format-gender combinations
library(DBI)
library(data.table)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

# Get player names for lookup
players <- DBI::dbGetQuery(conn, "SELECT player_id, player_name as name FROM players")
setDT(players)

cat("\n")
cli::cli_h1("3-Way ELO Analysis")
cat("\n")

# Define format-gender combinations
combos <- list(
  list(table = "mens_t20_3way_elo", label = "Men's T20"),
  list(table = "mens_odi_3way_elo", label = "Men's ODI"),
  list(table = "mens_test_3way_elo", label = "Men's Test"),
  list(table = "womens_t20_3way_elo", label = "Women's T20"),
  list(table = "womens_odi_3way_elo", label = "Women's ODI"),
  list(table = "womens_test_3way_elo", label = "Women's Test")
)

for (combo in combos) {
  cat("\n")
  cli::cli_h2(combo$label)

  # Get latest ELOs for each player (most recent delivery)
  latest_elos <- DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        batter_id,
        bowler_id,
        batter_run_elo_after,
        batter_wicket_elo_after,
        bowler_run_elo_after,
        bowler_wicket_elo_after,
        batter_balls,
        bowler_balls,
        ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as batter_rank,
        ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as bowler_rank
      FROM %s
    )
    SELECT * FROM ranked WHERE batter_rank = 1 OR bowler_rank = 1
  ", combo$table))
  setDT(latest_elos)

  # Separate batter and bowler latest ratings
  batter_latest <- latest_elos[batter_rank == 1, .(
    player_id = batter_id,
    run_elo = batter_run_elo_after,
    wicket_elo = batter_wicket_elo_after,
    balls = batter_balls
  )]

  bowler_latest <- latest_elos[bowler_rank == 1, .(
    player_id = bowler_id,
    run_elo = bowler_run_elo_after,
    wicket_elo = bowler_wicket_elo_after,
    balls = bowler_balls
  )]

  # Merge with player names
  batter_latest <- merge(batter_latest, players, by = "player_id", all.x = TRUE)
  bowler_latest <- merge(bowler_latest, players, by = "player_id", all.x = TRUE)

  # Filter to players with significant sample (500+ balls for batters, 300+ for bowlers)
  batter_qualified <- batter_latest[balls >= 500]
  bowler_qualified <- bowler_latest[balls >= 300]

  cat("\n--- BATTER RUN ELO (scoring ability) ---\n")
  cat(sprintf("Qualified batters (500+ balls): %d\n", nrow(batter_qualified)))
  cat(sprintf("Mean: %.1f | SD: %.1f | Range: %.1f to %.1f\n",
              mean(batter_qualified$run_elo), sd(batter_qualified$run_elo),
              min(batter_qualified$run_elo), max(batter_qualified$run_elo)))

  cat("\nTop 10 Batters (Run ELO):\n")
  top_batters <- batter_qualified[order(-run_elo)][1:10]
  for (i in 1:nrow(top_batters)) {
    cat(sprintf("  %2d. %-25s  %.0f  (%d balls)\n",
                i, top_batters$name[i], top_batters$run_elo[i], top_batters$balls[i]))
  }

  cat("\nBottom 5 Batters (Run ELO):\n")
  bottom_batters <- batter_qualified[order(run_elo)][1:5]
  for (i in 1:nrow(bottom_batters)) {
    cat(sprintf("  %2d. %-25s  %.0f  (%d balls)\n",
                i, bottom_batters$name[i], bottom_batters$run_elo[i], bottom_batters$balls[i]))
  }

  cat("\n--- BOWLER RUN ELO (economy) ---\n")
  cat(sprintf("Qualified bowlers (300+ balls): %d\n", nrow(bowler_qualified)))
  cat(sprintf("Mean: %.1f | SD: %.1f | Range: %.1f to %.1f\n",
              mean(bowler_qualified$run_elo), sd(bowler_qualified$run_elo),
              min(bowler_qualified$run_elo), max(bowler_qualified$run_elo)))

  # For bowlers, HIGHER run ELO = worse (gives up more runs)
  # So top bowlers have LOWER run ELO
  cat("\nTop 10 Bowlers (lowest Run ELO = most economical):\n")
  top_bowlers <- bowler_qualified[order(run_elo)][1:10]
  for (i in 1:nrow(top_bowlers)) {
    cat(sprintf("  %2d. %-25s  %.0f  (%d balls)\n",
                i, top_bowlers$name[i], top_bowlers$run_elo[i], top_bowlers$balls[i]))
  }

  cat("\n--- BOWLER WICKET ELO (strike rate) ---\n")
  # For bowlers, HIGHER wicket ELO = better (takes more wickets)
  cat("\nTop 10 Bowlers (highest Wicket ELO = best strike rate):\n")
  top_wicket_bowlers <- bowler_qualified[order(-wicket_elo)][1:10]
  for (i in 1:nrow(top_wicket_bowlers)) {
    cat(sprintf("  %2d. %-25s  %.0f  (%d balls)\n",
                i, top_wicket_bowlers$name[i], top_wicket_bowlers$wicket_elo[i], top_wicket_bowlers$balls[i]))
  }

  # Venue analysis
  cat("\n--- VENUE RUN ELO ---\n")
  venue_elos <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      venue,
      venue_perm_run_elo_after as perm_elo,
      venue_balls
    FROM %s
    WHERE (venue, match_date, delivery_id) IN (
      SELECT venue, MAX(match_date), MAX(delivery_id)
      FROM %s
      GROUP BY venue
    )
  ", combo$table, combo$table))
  setDT(venue_elos)
  venue_elos <- unique(venue_elos)
  venue_qualified <- venue_elos[venue_balls >= 1000]

  cat(sprintf("Qualified venues (1000+ balls): %d\n", nrow(venue_qualified)))
  cat(sprintf("Mean: %.1f | SD: %.1f | Range: %.1f to %.1f\n",
              mean(venue_qualified$perm_elo), sd(venue_qualified$perm_elo),
              min(venue_qualified$perm_elo), max(venue_qualified$perm_elo)))

  cat("\nHighest scoring venues (top 5):\n")
  high_venues <- venue_qualified[order(-perm_elo)][1:5]
  for (i in 1:nrow(high_venues)) {
    cat(sprintf("  %2d. %-40s  %.0f\n", i, high_venues$venue[i], high_venues$perm_elo[i]))
  }

  cat("\nLowest scoring venues (top 5):\n")
  low_venues <- venue_qualified[order(perm_elo)][1:5]
  for (i in 1:nrow(low_venues)) {
    cat(sprintf("  %2d. %-40s  %.0f\n", i, low_venues$venue[i], low_venues$perm_elo[i]))
  }

  # Prediction accuracy
  cat("\n--- PREDICTION ACCURACY ---\n")
  accuracy <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      CORR(exp_runs, actual_runs) as run_corr,
      AVG(ABS(exp_runs - actual_runs)) as run_mae,
      AVG(CASE WHEN is_wicket THEN exp_wicket ELSE 1 - exp_wicket END) as wicket_acc
    FROM %s
  ", combo$table))

  cat(sprintf("Run prediction: Correlation = %.4f | MAE = %.3f\n",
              accuracy$run_corr, accuracy$run_mae))
  cat(sprintf("Wicket prediction: Mean probability when correct = %.4f\n",
              accuracy$wicket_acc))
}

cat("\n")
cli::cli_h1("Analysis Complete")
cat("\n")
