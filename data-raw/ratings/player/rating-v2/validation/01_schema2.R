suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
library(DBI)
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

cat("=== ALL cricsheet.matches columns ===\n")
print(dbGetQuery(conn, "
  SELECT column_name, data_type FROM information_schema.columns
  WHERE table_schema='cricsheet' AND table_name='matches' ORDER BY ordinal_position"))

cat("\n=== sample Test rows, outcome-related fields ===\n")
print(dbGetQuery(conn, "
  SELECT match_id, team1, team2, outcome_type, outcome_winner,
         outcome_by_runs, outcome_by_wickets, unified_margin, match_date
  FROM cricsheet.matches
  WHERE match_type='Test' AND gender='male'
  ORDER BY match_date DESC LIMIT 6"))
