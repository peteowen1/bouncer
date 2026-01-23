# Quick check of database counts
devtools::load_all()
conn <- get_db_connection(read_only = TRUE)
cat("Matches:", DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM matches")$n, "\n")
cat("Deliveries:", DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM deliveries")$n, "\n")
cat("Players:", DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM players")$n, "\n")
DBI::dbDisconnect(conn, shutdown = TRUE)
