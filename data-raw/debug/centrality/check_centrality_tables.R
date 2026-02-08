# Check centrality table structure
library(DBI)
devtools::load_all()
conn <- get_db_connection(read_only = TRUE)

cat("=== Centrality-related tables ===\n")
tables <- dbListTables(conn)
centrality_tables <- tables[grepl("centrality|pagerank", tables, ignore.case = TRUE)]
print(centrality_tables)

if (length(centrality_tables) > 0) {
  for (tbl in centrality_tables[1:min(3, length(centrality_tables))]) {
    cat("\n=== Schema for", tbl, "===\n")
    schema <- dbGetQuery(conn, sprintf(
      "SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", tbl
    ))
    print(schema)

    cat("\nSample rows:\n")
    sample <- dbGetQuery(conn, sprintf("SELECT * FROM %s LIMIT 3", tbl))
    print(sample)
  }
}

dbDisconnect(conn, shutdown = TRUE)
