# Verify 3-Way ELO calculation completion across all format-gender combinations
library(DBI)
devtools::load_all()

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

# Get all 3-way ELO tables
tables <- DBI::dbGetQuery(conn, "
  SELECT table_name
  FROM information_schema.tables
  WHERE table_name LIKE '%3way_elo'
  ORDER BY table_name
")

cat("\n=== 3-Way ELO Tables Summary ===\n\n")

for (t in tables$table_name) {
  cat(toupper(t), ":\n", sep = "")
  q <- sprintf("SELECT COUNT(*) as n, MIN(match_date) as first, MAX(match_date) as last FROM %s", t)
  res <- DBI::dbGetQuery(conn, q)
  cat(sprintf("  Deliveries: %s | Date range: %s to %s\n\n",
              format(res$n, big.mark = ","), res$first, res$last))
}
