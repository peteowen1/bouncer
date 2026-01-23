# Debug script to test new parser fields
# Location: data-raw/debug/debug_parser.R
devtools::load_all()

# Get a sample JSON file - find the bouncerdata directory
data_dir <- bouncer:::find_bouncerdata_dir()
json_dir <- file.path(data_dir, "json_files")  # Correct folder name
json_files <- list.files(json_dir, pattern = "\\.json$", full.names = TRUE)
cat("Total JSON files:", length(json_files), "\n")

if (length(json_files) == 0) {
  stop("No JSON files found in: ", json_dir)
}

# Parse a single file
test_file <- json_files[1]
cat("\nParsing:", basename(test_file), "\n")

result <- parse_cricsheet_json(test_file)

# Check powerplays
cat("\n=== POWERPLAYS ===\n")
cat("Rows:", nrow(result$powerplays), "\n")
if (!is.null(result$powerplays) && nrow(result$powerplays) > 0) {
  print(result$powerplays)
}

# Check innings new columns
cat("\n=== INNINGS NEW COLUMNS ===\n")
cat("Columns:", paste(names(result$innings), collapse = ", "), "\n")
new_innings_cols <- c("target_runs", "target_overs", "is_super_over", "absent_hurt")
available_cols <- intersect(new_innings_cols, names(result$innings))
if (length(available_cols) > 0) {
  print(result$innings[, c("innings", "batting_team", available_cols)])
} else {
  cat("None of the new innings columns found!\n")
}

# Check deliveries new columns
cat("\n=== DELIVERIES NEW COLUMNS ===\n")
new_cols <- c("has_review", "review_by", "fielder1_is_sub", "has_replacement")
for (col in new_cols) {
  if (col %in% names(result$deliveries)) {
    cat(col, "- present, non-NA:", sum(!is.na(result$deliveries[[col]])), "\n")
    if (col == "has_review") {
      cat("  TRUE count:", sum(result$deliveries[[col]] == TRUE, na.rm = TRUE), "\n")
    }
  } else {
    cat(col, "- MISSING!\n")
  }
}

# Read raw JSON to see what's actually there
cat("\n=== RAW JSON STRUCTURE ===\n")
raw_json <- jsonlite::fromJSON(test_file, simplifyVector = FALSE)

# Check if powerplays exist in raw JSON
if (!is.null(raw_json$innings) && length(raw_json$innings) > 0) {
  for (i in seq_along(raw_json$innings)) {
    inning <- raw_json$innings[[i]]
    cat("Innings", i, "- team:", inning$team, "\n")
    cat("  powerplays:", !is.null(inning$powerplays), "\n")
    if (!is.null(inning$powerplays)) {
      cat("  powerplays data:", jsonlite::toJSON(inning$powerplays, auto_unbox = TRUE), "\n")
    }
    cat("  target:", !is.null(inning$target), "\n")
    if (!is.null(inning$target)) {
      cat("  target data:", jsonlite::toJSON(inning$target, auto_unbox = TRUE), "\n")
    }
  }
}

# Look for a file with reviews (they're rare)
cat("\n=== SEARCHING FOR FILE WITH REVIEWS ===\n")
search_limit <- min(100, length(json_files))
found_review <- FALSE
for (f in json_files[1:search_limit]) {
  raw <- jsonlite::fromJSON(f, simplifyVector = FALSE)
  if (!is.null(raw$innings)) {
    for (inn in raw$innings) {
      if (!is.null(inn$overs)) {
        for (ov in inn$overs) {
          if (!is.null(ov$deliveries)) {
            for (del in ov$deliveries) {
              if (!is.null(del$review)) {
                found_review <- TRUE
                cat("Found review in:", basename(f), "\n")
                cat("  Review data:", jsonlite::toJSON(del$review, auto_unbox = TRUE), "\n")
                break
              }
            }
          }
          if (found_review) break
        }
      }
      if (found_review) break
    }
  }
  if (found_review) break
}
if (!found_review) {
  cat("No reviews found in first", search_limit, "files\n")
}

# Look for a file with powerplays
cat("\n=== SEARCHING FOR FILE WITH POWERPLAYS ===\n")
found_pp <- FALSE
for (f in json_files[1:search_limit]) {
  raw <- jsonlite::fromJSON(f, simplifyVector = FALSE)
  if (!is.null(raw$innings)) {
    for (inn in raw$innings) {
      if (!is.null(inn$powerplays)) {
        found_pp <- TRUE
        cat("Found powerplays in:", basename(f), "\n")
        cat("  Powerplays data:", jsonlite::toJSON(inn$powerplays, auto_unbox = TRUE), "\n")
        break
      }
    }
  }
  if (found_pp) break
}
if (!found_pp) {
  cat("No powerplays found in first", search_limit, "files\n")
}
