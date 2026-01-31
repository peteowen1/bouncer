# Load 3-Way ELO Parquets into DuckDB ----
#
# Run after all parallel calculations complete.
# Loads parquet files into DuckDB tables.

library(DBI)
library(duckdb)
library(arrow)

# Find bouncerdata directory
find_bouncerdata_dir <- function() {
  candidates <- c(
    file.path(getwd(), "..", "bouncerdata"),
    file.path(getwd(), "bouncerdata"),
    "C:/Users/peteo/OneDrive/Documents/bouncerverse/bouncerdata"
  )
  for (path in candidates) {
    if (dir.exists(path)) return(normalizePath(path))
  }
  stop("Could not find bouncerdata directory")
}

bouncerdata_dir <- find_bouncerdata_dir()
temp_dir <- file.path(bouncerdata_dir, "temp_3way_elo")
db_path <- file.path(bouncerdata_dir, "bouncer.duckdb")

cat("\n")
cat("=== Loading 3-Way ELO Parquets into DuckDB ===\n\n")

# Check what parquet files exist
parquet_files <- list.files(temp_dir, pattern = "\\.parquet$", full.names = TRUE)
cat("Found", length(parquet_files), "parquet files:\n")
for (f in parquet_files) {
  cat("  -", basename(f), "\n")
}
cat("\n")

if (length(parquet_files) == 0) {
  stop("No parquet files found in ", temp_dir)
}

# Connect to DuckDB
cat("Connecting to database...\n")
conn <- dbConnect(duckdb(), db_path, read_only = FALSE)
on.exit(dbDisconnect(conn, shutdown = TRUE))

# Group files by format (t20, odi, test)
files_by_format <- list()
for (parquet_file in parquet_files) {
  filename <- basename(parquet_file)
  parts <- strsplit(tools::file_path_sans_ext(filename), "_")[[1]]
  format_name <- parts[2]  # t20, odi, or test
  if (is.null(files_by_format[[format_name]])) {
    files_by_format[[format_name]] <- c()
  }
  files_by_format[[format_name]] <- c(files_by_format[[format_name]], parquet_file)
}

# Process each format (combining men's and women's)
for (format_name in names(files_by_format)) {
  table_name <- paste0(format_name, "_3way_elo")
  cat("\n=== Processing format:", toupper(format_name), "->", table_name, "===\n")

  # Drop existing table first
  dbExecute(conn, paste0("DROP TABLE IF EXISTS ", table_name))

  # Load and combine all files for this format
  all_data <- NULL
  for (parquet_file in files_by_format[[format_name]]) {
    filename <- basename(parquet_file)
    cat("  Reading:", filename, "\n")
    data <- arrow::read_parquet(parquet_file)
    cat("    Rows:", format(nrow(data), big.mark = ","), "\n")
    if (is.null(all_data)) {
      all_data <- data
    } else {
      all_data <- rbind(all_data, data)
    }
  }

  cat("  Total rows:", format(nrow(all_data), big.mark = ","), "\n")

  # Write combined data to DuckDB
  dbWriteTable(conn, table_name, as.data.frame(all_data), overwrite = TRUE)

  # Verify
  count <- dbGetQuery(conn, paste0("SELECT COUNT(*) as n FROM ", table_name))$n
  cat("  Loaded:", format(count, big.mark = ","), "rows\n")

  # Create index on delivery_id
  dbExecute(conn, paste0("CREATE INDEX IF NOT EXISTS idx_", table_name, "_delivery ON ", table_name, "(delivery_id)"))
  cat("  Created index on delivery_id\n")
}

# Summary
cat("\n=== Summary ===\n")
for (tbl in c("t20_3way_elo", "odi_3way_elo", "test_3way_elo")) {
  tryCatch({
    cnt <- dbGetQuery(conn, paste0("SELECT COUNT(*) as n FROM ", tbl))$n
    cat(sprintf("%s: %s rows\n", tbl, format(cnt, big.mark = ",")))
  }, error = function(e) {
    cat(sprintf("%s: not loaded\n", tbl))
  })
}

cat("\n=== Done! ===\n")
cat("You can now delete the temp parquet files:\n")
cat("  rm -rf", temp_dir, "\n")
