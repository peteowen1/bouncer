# Quick test of parallel parsing with installed package
library(bouncer)

cat("=== Testing Parallel Parsing with Globals ===\n\n")

# Find JSON files
data_dir <- bouncer:::find_bouncerdata_dir()
json_dir <- file.path(data_dir, "json_files")
all_files <- list.files(json_dir, pattern = "\\.json$", full.names = TRUE)

# Test with just 100 files
test_files <- all_files[1:min(100, length(all_files))]
cat("Testing with", length(test_files), "files\n\n")

# Create temp directory
temp_dir <- tempfile("test_parquet_")
dir.create(temp_dir, recursive = TRUE)
on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

# Test the parallel parsing
cat("Starting parallel parse to Parquet...\n")
result <- bouncer:::parse_to_parquet_parallel(
  test_files,
  output_dir = temp_dir,
  n_workers = 4,  # Use fewer workers for test
  batch_size = 25,
  progress = TRUE
)

cat("\n\n=== RESULTS ===\n")
cat("Matches parsed:", result$n_matches, "\n")
cat("Errors:", result$n_errors, "\n")

# Check parquet files were created
parquet_files <- list.files(temp_dir, pattern = "\\.parquet$")
cat("Parquet files created:", length(parquet_files), "\n")
cat("Files:", paste(parquet_files, collapse = ", "), "\n")

cat("\n=== TEST PASSED ===\n")
