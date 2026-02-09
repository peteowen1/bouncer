# Show top/bottom players by 3-Way ELO for all formats (gender-specific tables)
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

get_rankings <- function(conn, table_name) {
  # Top batters
  top_bat <- DBI::dbGetQuery(conn, sprintf("
    WITH latest AS (
      SELECT batter_id, batter_run_elo_after as elo,
             ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT l.batter_id, p.player_name as name, ROUND(l.elo, 1) as elo
    FROM latest l
    LEFT JOIN players p ON l.batter_id = p.player_id
    WHERE l.rn = 1
    ORDER BY l.elo DESC LIMIT 10
  ", table_name))

  # Bottom batters
  bot_bat <- DBI::dbGetQuery(conn, sprintf("
    WITH latest AS (
      SELECT batter_id, batter_run_elo_after as elo,
             ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT l.batter_id, p.player_name as name, ROUND(l.elo, 1) as elo
    FROM latest l
    LEFT JOIN players p ON l.batter_id = p.player_id
    WHERE l.rn = 1
    ORDER BY l.elo ASC LIMIT 10
  ", table_name))

  # Top bowlers (lowest run ELO = best)
  top_bowl <- DBI::dbGetQuery(conn, sprintf("
    WITH latest AS (
      SELECT bowler_id, bowler_run_elo_after as elo,
             ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT l.bowler_id, p.player_name as name, ROUND(l.elo, 1) as elo
    FROM latest l
    LEFT JOIN players p ON l.bowler_id = p.player_id
    WHERE l.rn = 1
    ORDER BY l.elo ASC LIMIT 10
  ", table_name))

  # Bottom bowlers
  bot_bowl <- DBI::dbGetQuery(conn, sprintf("
    WITH latest AS (
      SELECT bowler_id, bowler_run_elo_after as elo,
             ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT l.bowler_id, p.player_name as name, ROUND(l.elo, 1) as elo
    FROM latest l
    LEFT JOIN players p ON l.bowler_id = p.player_id
    WHERE l.rn = 1
    ORDER BY l.elo DESC LIMIT 10
  ", table_name))

  list(top_bat = top_bat, bot_bat = bot_bat, top_bowl = top_bowl, bot_bowl = bot_bowl)
}

tables <- c(
  "MENS T20" = "mens_t20_3way_elo",
  "MENS ODI" = "mens_odi_3way_elo",
  "MENS TEST" = "mens_test_3way_elo",
  "WOMENS T20" = "womens_t20_3way_elo",
  "WOMENS ODI" = "womens_odi_3way_elo",
  "WOMENS TEST" = "womens_test_3way_elo"
)

for (label in names(tables)) {
  cli::cli_h1(label)

  result <- tryCatch(get_rankings(conn, tables[[label]]), error = function(e) {
    cli::cli_alert_warning("Error: {e$message}")
    NULL
  })

  if (!is.null(result)) {
    cli::cli_h3("TOP 10 BATTERS")
    print(result$top_bat[, c("name", "elo")], row.names = FALSE)

    cat("\n")
    cli::cli_h3("BOTTOM 10 BATTERS")
    print(result$bot_bat[, c("name", "elo")], row.names = FALSE)

    cat("\n")
    cli::cli_h3("TOP 10 BOWLERS (lowest ELO = best)")
    print(result$top_bowl[, c("name", "elo")], row.names = FALSE)

    cat("\n")
    cli::cli_h3("BOTTOM 10 BOWLERS")
    print(result$bot_bowl[, c("name", "elo")], row.names = FALSE)
  }
  cat("\n\n")
}
