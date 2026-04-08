# Step 13: Estimate Stat Ratings ----
#
# Bayesian per-stat rating estimation using Gamma-Poisson (rate stats)
# and Beta-Binomial (efficiency stats) conjugate models.

library(cli)
devtools::load_all()

conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

FORMATS <- c("t20", "odi", "test")

for (fmt in FORMATS) {
  cli::cli_h2("Estimating stat ratings for {toupper(fmt)}")

  # Load player game data
  pgd <- load_player_game_data(fmt, source = "local")
  if (nrow(pgd) == 0) {
    cli::cli_alert_warning("  No player game data for {toupper(fmt)}, skipping")
    next
  }

  # Prepare data
  stat_data <- prepare_stat_rating_data(pgd)

  # Estimate ratings
  ratings <- estimate_player_stat_ratings(stat_data)
  cli::cli_alert_info("  Rated {nrow(ratings)} players across {ncol(ratings)} columns")

  # Store to DB
  store_stat_ratings(conn, ratings, fmt)
}

cli::cli_alert_success("Stat rating estimation complete")
