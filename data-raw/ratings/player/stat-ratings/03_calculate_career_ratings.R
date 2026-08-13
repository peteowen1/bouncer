# Step 15-16: Calculate Career Ratings (EPR + BOUNCER) ----
#
# Computes EPR from player game data, PSR from stat ratings,
# and blends into BOUNCER composite.

library(cli)
devtools::load_all()

FORMATS <- c("t20", "odi", "test")

for (fmt in FORMATS) {
  cli::cli_h2("Calculating career ratings for {toupper(fmt)}")

  # EPR
  epr <- calculate_impact(fmt)
  cli::cli_alert_info("  EPR: {nrow(epr)} players")

  # BOUNCER (includes PSR if coefficients available)
  ratings <- bouncer_ratings(fmt)
  cli::cli_alert_info("  BOUNCER: {nrow(ratings)} players")

  # Show top 10
  cli::cli_h3("Top 10 {toupper(fmt)} BOUNCER ratings")
  top10 <- head(ratings, 10)
  for (i in seq_len(nrow(top10))) {
    r <- top10[i]
    cli::cli_alert_info("  {i}. {r$player_id} ({r$role_group}): {round(r$bouncer_rating, 3)}")
  }
}

cli::cli_alert_success("Career ratings complete")
