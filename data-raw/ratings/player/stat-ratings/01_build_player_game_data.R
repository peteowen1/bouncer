# Step 12: Build Player Game Data ----
#
# Aggregates Cricinfo ball-by-ball data into one row per player per match.
# This is the foundation for all value metrics.

library(cli)
library(data.table)
devtools::load_all()

conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

FORMATS <- c("t20", "odi", "test")

for (fmt in FORMATS) {
  cli::cli_h2("Building player game data for {toupper(fmt)}")

  pgd <- create_player_game_data(fmt, conn = conn)

  if (nrow(pgd) > 0) {
    store_player_game_data(conn, pgd, fmt)
    cli::cli_alert_info("  Rows: {nrow(pgd)}, Matches: {uniqueN(pgd$match_id)}, Players: {uniqueN(pgd$player_id)}")
  } else {
    cli::cli_alert_warning("  No data for {toupper(fmt)}")
  }
}

cli::cli_alert_success("Player game data build complete")
