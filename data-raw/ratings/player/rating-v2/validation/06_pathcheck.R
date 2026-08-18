suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages(library(DBI))

cat("cwd:", getwd(), "\n")
cat("find_bouncerdata_dir():", tryCatch(find_bouncerdata_dir(),
    error = function(e) paste("ERROR:", conditionMessage(e))), "\n")
cat("get_db_path():        ", tryCatch(get_db_path(),
    error = function(e) paste("ERROR:", conditionMessage(e))), "\n")

p <- tryCatch(get_db_path(), error = function(e) NA_character_)
cat("that path exists:", !is.na(p) && file.exists(p), "\n")
if (!is.na(p) && file.exists(p)) {
  cat("size:", format(file.info(p)$size, big.mark = ","), "bytes\n")
}

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cat("\nschemas visible on this connection:\n")
print(dbGetQuery(conn, "SELECT schema_name FROM information_schema.schemata ORDER BY 1"))
cat("\ncricsheet.matches row count:",
    tryCatch(dbGetQuery(conn, "SELECT COUNT(*) n FROM cricsheet.matches")$n,
             error = function(e) paste("ERROR:", conditionMessage(e))), "\n")
