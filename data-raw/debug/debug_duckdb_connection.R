# Debug DuckDB Connection Issues
# Investigating segfaults when connecting to bouncer.duckdb

library(DBI)
library(duckdb)

cat("=== DuckDB Connection Debug ===\n\n")

# 1. Test in-memory first
cat("1. Testing in-memory connection...\n")
tryCatch({
  conn <- dbConnect(duckdb(), ":memory:")
  result <- dbGetQuery(conn, "SELECT 1 as test")
  cat("   In-memory works:", result$test == 1, "\n")
  dbDisconnect(conn, shutdown = TRUE)
}, error = function(e) {

  cat("   In-memory FAILED:", e$message, "\n")
})

# 2. Check database file
db_path <- "C:/Users/peteo/OneDrive/Documents/bouncerverse/bouncerdata/bouncer.duckdb"
cat("\n2. Database file info:\n")
cat("   Path:", db_path, "\n")
cat("   Exists:", file.exists(db_path), "\n")
if (file.exists(db_path)) {
  info <- file.info(db_path)
  cat("   Size:", round(info$size / 1024^3, 2), "GB\n")
  cat("   Modified:", as.character(info$mtime), "\n")
}

# Check for lock files
wal_path <- paste0(db_path, ".wal")
lock_path <- paste0(db_path, ".lock")
cat("   WAL exists:", file.exists(wal_path), "\n")
cat("   Lock exists:", file.exists(lock_path), "\n")

# 3. Try connecting with different options
cat("\n3. Attempting connection...\n")

# Try with explicit settings
tryCatch({
  cat("   Creating driver...\n")
  drv <- duckdb()
  cat("   Driver created\n")

  cat("   Connecting (read_only=TRUE)...\n")
  conn <- dbConnect(drv, db_path, read_only = TRUE)
  cat("   Connected!\n")

  cat("   Listing tables...\n")
  tables <- dbListTables(conn)
  cat("   Tables found:", length(tables), "\n")
  cat("   Sample:", paste(head(tables, 5), collapse = ", "), "\n")

  dbDisconnect(conn, shutdown = TRUE)
  cat("   Disconnected cleanly\n")
}, error = function(e) {
  cat("   Connection FAILED:", e$message, "\n")
})

# 4. Check agnostic predictions tables
cat("\n4. Agnostic predictions tables:\n")
for (fmt in c('t20', 'odi', 'test')) {
  tbl <- paste0('agnostic_predictions_', fmt)
  tryCatch({
    conn <- dbConnect(duckdb(), db_path, read_only = TRUE)
    if (tbl %in% dbListTables(conn)) {
      n <- dbGetQuery(conn, sprintf('SELECT COUNT(*) as n FROM %s', tbl))$n
      cat(sprintf("   %s: %s rows\n", tbl, format(n, big.mark = ',')))
    } else {
      cat(sprintf("   %s: NOT FOUND\n", tbl))
    }
    dbDisconnect(conn, shutdown = TRUE)
  }, error = function(e) {
    cat(sprintf("   %s: ERROR - %s\n", tbl, e$message))
  })
}

# 5. Check deliveries by format/gender
cat("\n5. Deliveries by format/gender:\n")
tryCatch({
  conn <- dbConnect(duckdb(), db_path, read_only = TRUE)
  counts <- dbGetQuery(conn, "
    SELECT
      CASE
        WHEN LOWER(d.match_type) IN ('t20', 'it20') THEN 'T20'
        WHEN LOWER(d.match_type) IN ('odi', 'odm') THEN 'ODI'
        WHEN LOWER(d.match_type) IN ('test', 'mdm') THEN 'Test'
        ELSE 'Other'
      END as format,
      CASE WHEN m.gender = 'male' THEN 'Men' ELSE 'Women' END as gender,
      COUNT(*) as deliveries
    FROM deliveries d
    JOIN matches m ON d.match_id = m.match_id
    GROUP BY 1, 2
    ORDER BY 1, 2
  ")
  for (i in seq_len(nrow(counts))) {
    cat(sprintf("   %s %s: %s deliveries\n",
                counts$gender[i], counts$format[i],
                format(counts$deliveries[i], big.mark = ',')))
  }
  dbDisconnect(conn, shutdown = TRUE)
}, error = function(e) {
  cat("   ERROR:", e$message, "\n")
})

cat("\n=== Debug Complete ===\n")
