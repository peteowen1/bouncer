# Benchmark JSON Parsing Options
# Run this script to compare jsonlite vs RcppSimdJson performance
#
# RcppSimdJson uses SIMD (Single Instruction Multiple Data) CPU instructions
# and is typically 5-15x faster than jsonlite for large JSON files.

# Find a test file
data_dir <- file.path(dirname(getwd()), "bouncerdata")
json_dir <- file.path(data_dir, "json_files")
test_files <- list.files(json_dir, pattern = "\\.json$", full.names = TRUE)

if (length(test_files) == 0) {
 stop("No JSON files found in: ", json_dir)
}

# Pick a medium-sized file (T20 match ~500KB, Test match ~2MB)
file_sizes <- file.size(test_files)
test_file <- test_files[which.max(file_sizes)]  # Largest file for stress test

cat("=== JSON PARSER BENCHMARK ===\n\n")
cat("File:", basename(test_file), "\n")
cat("Size:", round(file.size(test_file) / 1024, 1), "KB\n\n")

# Benchmark jsonlite (current implementation)
cat("--- JSONLITE (current) ---\n")
n_iter <- 10
t1 <- system.time({
  for (i in 1:n_iter) {
    json_data <- jsonlite::fromJSON(test_file, simplifyVector = FALSE)
  }
})
jsonlite_ms <- t1["elapsed"] / n_iter * 1000
cat("Per file:", round(jsonlite_ms, 1), "ms\n")
cat("21k files estimate:", round(21000 * jsonlite_ms / 1000 / 60, 1), "minutes (single-threaded)\n\n")

# Check if RcppSimdJson is available
if (requireNamespace("RcppSimdJson", quietly = TRUE)) {
  cat("--- RcppSimdJson ---\n")
  t2 <- system.time({
    for (i in 1:n_iter) {
      json_data <- RcppSimdJson::fload(test_file)
    }
  })
  simd_ms <- t2["elapsed"] / n_iter * 1000
  cat("Per file:", round(simd_ms, 1), "ms\n")
  cat("21k files estimate:", round(21000 * simd_ms / 1000 / 60, 1), "minutes (single-threaded)\n")
  cat("Speedup:", round(jsonlite_ms / simd_ms, 1), "x faster\n\n")

  # Verify output is compatible
  cat("--- OUTPUT COMPATIBILITY CHECK ---\n")
  json_jsonlite <- jsonlite::fromJSON(test_file, simplifyVector = FALSE)
  json_simd <- RcppSimdJson::fload(test_file)

  cat("Both return list:", is.list(json_jsonlite) && is.list(json_simd), "\n")
  cat("Same top-level keys:", identical(names(json_jsonlite), names(json_simd)), "\n")
  cat("info section match:", identical(names(json_jsonlite$info), names(json_simd$info)), "\n")

} else {
  cat("--- RcppSimdJson NOT INSTALLED ---\n")
  cat("Install with: install.packages('RcppSimdJson')\n\n")
  cat("RcppSimdJson uses SIMD CPU instructions and is typically\n")
  cat("5-15x faster than jsonlite for large JSON files.\n")
}

cat("\n=== RECOMMENDATIONS ===\n")
cat("1. Install RcppSimdJson for 5-15x faster JSON parsing\n")
cat("2. Combined with furrr parallel processing, expect ~20-50x total speedup\n")
cat("3. On 8 cores: 21k files in ~2-5 minutes instead of ~30-60 minutes\n")
