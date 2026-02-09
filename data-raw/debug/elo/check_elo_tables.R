# Quick check of 3-Way ELO tables
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

# Check all 3way_elo tables
tables <- DBI::dbListTables(conn)
elo_tables <- grep("3way_elo", tables, value = TRUE)
cli::cli_alert_info("Tables found: {paste(elo_tables, collapse = ', ')}")

cat("\n")
for (tbl in elo_tables) {
  count <- DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) as n FROM %s", tbl))$n
  cli::cli_alert_success("{tbl}: {format(count, big.mark = ',')} rows")
}
