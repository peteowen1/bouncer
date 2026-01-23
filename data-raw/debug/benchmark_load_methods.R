# Benchmark Different Data Loading Methods
# Tests parsing and loading approaches on 1000 matches
# Location: data-raw/debug/benchmark_load_methods.R

library(dplyr)
devtools::load_all()

# =============================================================================
# SETUP
# =============================================================================

# Find JSON files
data_dir <- bouncer:::find_bouncerdata_dir()
json_dir <- file.path(data_dir, "json_files")
all_files <- list.files(json_dir, pattern = "\\.json$", full.names = TRUE)

# Take first 1000 files for benchmarking
n_test <- 1000
test_files <- all_files[1:n_test]

cat("=== BENCHMARK: Loading", n_test, "matches ===\n\n")
cat("JSON files location:", json_dir, "\n")
cat("Total files available:", length(all_files), "\n\n")

# Temp directory for outputs
temp_base <- file.path(tempdir(), "bouncer_benchmark")
dir.create(temp_base, showWarnings = FALSE, recursive = TRUE)

# Results storage
results <- list()

# =============================================================================
# METHOD 1: Sequential parsing (baseline)
# =============================================================================
cat("--- METHOD 1: Sequential parsing (baseline) ---\n")

t1 <- system.time({
  parsed_list <- vector("list", n_test)
  for (i in seq_along(test_files)) {
    parsed_list[[i]] <- tryCatch(
      parse_cricsheet_json(test_files[i]),
      error = function(e) NULL
    )
  }
  # Combine
  all_matches <- do.call(rbind, lapply(parsed_list, `[[`, "match_info"))
  all_deliveries <- do.call(rbind, lapply(parsed_list, `[[`, "deliveries"))
})

results$sequential <- list(
  method = "Sequential (in-memory)",
  time = t1["elapsed"],
  matches = nrow(all_matches),
  deliveries = nrow(all_deliveries)
)

cat("Time:", round(t1["elapsed"], 1), "sec\n")
cat("Matches:", nrow(all_matches), "| Deliveries:", nrow(all_deliveries), "\n\n")

rm(parsed_list, all_matches, all_deliveries)
gc()

# =============================================================================
# METHOD 2: Parallel parsing (current furrr approach)
# =============================================================================
if (bouncer:::has_parallel_support()) {
  cat("--- METHOD 2: Parallel parsing with furrr ---\n")

  n_workers <- max(1, parallel::detectCores() - 1)
  cat("Workers:", n_workers, "\n")

  t2 <- system.time({
    old_plan <- future::plan(future::multisession, workers = n_workers)
    on.exit(future::plan(old_plan), add = TRUE)

    parsed_list <- furrr::future_map(test_files, function(f) {
      tryCatch(parse_cricsheet_json(f), error = function(e) NULL)
    }, .options = furrr::furrr_options(seed = TRUE))

    # Combine
    all_matches <- do.call(rbind, lapply(parsed_list, `[[`, "match_info"))
    all_deliveries <- do.call(rbind, lapply(parsed_list, `[[`, "deliveries"))
  })

  results$parallel_furrr <- list(
    method = paste0("Parallel furrr (", n_workers, " workers)"),
    time = t2["elapsed"],
    matches = nrow(all_matches),
    deliveries = nrow(all_deliveries)
  )

  cat("Time:", round(t2["elapsed"], 1), "sec\n")
  cat("Matches:", nrow(all_matches), "| Deliveries:", nrow(all_deliveries), "\n\n")

  rm(parsed_list, all_matches, all_deliveries)
  gc()
}

# =============================================================================
# METHOD 3: Parquet intermediate (sequential batches)
# =============================================================================
if (bouncer:::has_arrow_support()) {
  cat("--- METHOD 3: Parquet intermediate (sequential, batch=100) ---\n")

  temp_dir <- file.path(temp_base, "parquet_seq")
  dir.create(temp_dir, showWarnings = FALSE, recursive = TRUE)

  batch_size <- 100

  t3 <- system.time({
    # Phase 1: Parse to Parquet in sequential batches
    batch_ids <- ceiling(seq_len(n_test) / batch_size)
    batches <- split(test_files, batch_ids)

    for (i in seq_along(batches)) {
      batch_files <- batches[[i]]

      matches_list <- list()
      deliveries_list <- list()

      for (f in batch_files) {
        result <- tryCatch(parse_cricsheet_json(f), error = function(e) NULL)
        if (!is.null(result) && !is.null(result$match_info)) {
          matches_list[[length(matches_list) + 1]] <- result$match_info
          deliveries_list[[length(deliveries_list) + 1]] <- result$deliveries
        }
      }

      if (length(matches_list) > 0) {
        all_m <- do.call(rbind, matches_list)
        all_d <- do.call(rbind, deliveries_list)
        arrow::write_parquet(all_m, file.path(temp_dir, sprintf("matches_%04d.parquet", i)))
        arrow::write_parquet(all_d, file.path(temp_dir, sprintf("deliveries_%04d.parquet", i)))
      }
    }

    # Phase 2: Read back
    matches_files <- list.files(temp_dir, "matches_.*\\.parquet$", full.names = TRUE)
    deliveries_files <- list.files(temp_dir, "deliveries_.*\\.parquet$", full.names = TRUE)

    all_matches <- arrow::open_dataset(matches_files) |> collect()
    all_deliveries <- arrow::open_dataset(deliveries_files) |> collect()
  })

  results$parquet_seq <- list(
    method = "Parquet sequential (batch=100)",
    time = t3["elapsed"],
    matches = nrow(all_matches),
    deliveries = nrow(all_deliveries)
  )

  cat("Time:", round(t3["elapsed"], 1), "sec\n")
  cat("Matches:", nrow(all_matches), "| Deliveries:", nrow(all_deliveries), "\n\n")

  unlink(temp_dir, recursive = TRUE)
  rm(all_matches, all_deliveries)
  gc()
}

# =============================================================================
# METHOD 4: Parallel furrr with 8 workers (less contention)
# =============================================================================
if (bouncer:::has_parallel_support()) {
  cat("--- METHOD 4: Parallel furrr (8 workers) ---\n")

  t4 <- system.time({
    old_plan <- future::plan(future::multisession, workers = 8)
    on.exit(future::plan(old_plan), add = TRUE)

    parsed_list <- furrr::future_map(test_files, function(f) {
      tryCatch(parse_cricsheet_json(f), error = function(e) NULL)
    }, .options = furrr::furrr_options(seed = TRUE))

    all_matches <- do.call(rbind, lapply(parsed_list, `[[`, "match_info"))
    all_deliveries <- do.call(rbind, lapply(parsed_list, `[[`, "deliveries"))
  })

  results$parallel_8 <- list(
    method = "Parallel furrr (8 workers)",
    time = t4["elapsed"],
    matches = nrow(all_matches),
    deliveries = nrow(all_deliveries)
  )

  cat("Time:", round(t4["elapsed"], 1), "sec\n")
  cat("Matches:", nrow(all_matches), "| Deliveries:", nrow(all_deliveries), "\n\n")

  rm(parsed_list, all_matches, all_deliveries)
  gc()
}

# =============================================================================
# METHOD 5: Parallel furrr with 4 workers
# =============================================================================
if (bouncer:::has_parallel_support()) {
  cat("--- METHOD 5: Parallel furrr (4 workers) ---\n")

  t5 <- system.time({
    old_plan <- future::plan(future::multisession, workers = 4)
    on.exit(future::plan(old_plan), add = TRUE)

    parsed_list <- furrr::future_map(test_files, function(f) {
      tryCatch(parse_cricsheet_json(f), error = function(e) NULL)
    }, .options = furrr::furrr_options(seed = TRUE))

    all_matches <- do.call(rbind, lapply(parsed_list, `[[`, "match_info"))
    all_deliveries <- do.call(rbind, lapply(parsed_list, `[[`, "deliveries"))
  })

  results$parallel_4 <- list(
    method = "Parallel furrr (4 workers)",
    time = t5["elapsed"],
    matches = nrow(all_matches),
    deliveries = nrow(all_deliveries)
  )

  cat("Time:", round(t5["elapsed"], 1), "sec\n")
  cat("Matches:", nrow(all_matches), "| Deliveries:", nrow(all_deliveries), "\n\n")

  rm(parsed_list, all_matches, all_deliveries)
  gc()
}

# =============================================================================
# METHOD 6: Direct Arrow write (no Parquet files, just memory)
# =============================================================================
if (bouncer:::has_arrow_support() && bouncer:::has_parallel_support()) {
  cat("--- METHOD 6: Parallel + Arrow concat (in-memory) ---\n")

  n_workers <- max(1, parallel::detectCores() - 1)

  t6 <- system.time({
    old_plan <- future::plan(future::multisession, workers = n_workers)
    on.exit(future::plan(old_plan), add = TRUE)

    parsed_list <- furrr::future_map(test_files, function(f) {
      tryCatch(parse_cricsheet_json(f), error = function(e) NULL)
    }, .options = furrr::furrr_options(seed = TRUE))

    # Use Arrow for combining (faster than rbind)
    valid <- Filter(Negate(is.null), parsed_list)

    matches_tables <- lapply(valid, function(x) arrow::as_arrow_table(x$match_info))
    deliveries_tables <- lapply(valid, function(x) arrow::as_arrow_table(x$deliveries))

    all_matches <- arrow::concat_tables(tables = matches_tables) |> as.data.frame()
    all_deliveries <- arrow::concat_tables(tables = deliveries_tables) |> as.data.frame()
  })

  results$arrow_concat <- list(
    method = paste0("Parallel + Arrow concat (", n_workers, " workers)"),
    time = t6["elapsed"],
    matches = nrow(all_matches),
    deliveries = nrow(all_deliveries)
  )

  cat("Time:", round(t6["elapsed"], 1), "sec\n")
  cat("Matches:", nrow(all_matches), "| Deliveries:", nrow(all_deliveries), "\n\n")

  rm(parsed_list, valid, matches_tables, deliveries_tables, all_matches, all_deliveries)
  gc()
}

# =============================================================================
# RESULTS SUMMARY
# =============================================================================
cat("\n", strrep("=", 60), "\n")
cat("BENCHMARK RESULTS SUMMARY\n")
cat(strrep("=", 60), "\n\n")

# Create results table
results_df <- do.call(rbind, lapply(results, function(r) {
  data.frame(
    Method = r$method,
    Time_sec = round(r$time, 1),
    Matches = r$matches,
    Deliveries = r$deliveries,
    stringsAsFactors = FALSE
  )
}))

# Sort by time
results_df <- results_df[order(results_df$Time_sec), ]
results_df$Rank <- seq_len(nrow(results_df))
results_df$vs_Sequential <- round(results_df$Time_sec[results_df$Method == "Sequential (in-memory)"] / results_df$Time_sec, 1)

print(results_df[, c("Rank", "Method", "Time_sec", "vs_Sequential")])

cat("\n")
cat("Winner:", results_df$Method[1], "at", results_df$Time_sec[1], "seconds\n")
cat("Speedup vs sequential:", results_df$vs_Sequential[1], "x\n")

# Extrapolate to full dataset
full_estimate <- results_df$Time_sec[1] * (length(all_files) / n_test) / 60
cat("\nEstimated time for full", length(all_files), "files:", round(full_estimate, 1), "minutes\n")

# Cleanup
unlink(temp_base, recursive = TRUE)
