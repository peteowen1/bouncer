# Benchmark Full Pipeline with Step-by-Step Timing
# Tests: Parse → Combine → Write → DuckDB Load
# Location: data-raw/debug/benchmark_full_pipeline.R

library(dplyr)
devtools::load_all()

# =============================================================================
# SETUP
# =============================================================================

data_dir <- bouncer:::find_bouncerdata_dir()
json_dir <- file.path(data_dir, "json_files")
all_files <- list.files(json_dir, pattern = "\\.json$", full.names = TRUE)

n_test <- 1000
test_files <- all_files[1:n_test]

cat("=== FULL PIPELINE BENCHMARK (with step timings) ===\n")
cat("Testing on", n_test, "matches\n\n")

temp_base <- file.path(tempdir(), "bouncer_benchmark")
dir.create(temp_base, showWarnings = FALSE, recursive = TRUE)

results <- list()

# Helper to format time
fmt_time <- function(t) paste0(round(t, 1), "s")

# =============================================================================
# METHOD 1: Sequential → Arrow → DuckDB
# =============================================================================
cat("--- METHOD 1: Sequential → Arrow → DuckDB ---\n")

test_db <- file.path(temp_base, "test1.duckdb")
if (file.exists(test_db)) file.remove(test_db)

# Step 1: Parse
t_parse <- system.time({
  parsed_list <- vector("list", n_test)
  for (i in seq_along(test_files)) {
    parsed_list[[i]] <- tryCatch(
      parse_cricsheet_json(test_files[i]),
      error = function(e) NULL
    )
  }
})["elapsed"]
cat("  Parse:", fmt_time(t_parse), "\n")

# Step 2: Combine
t_combine <- system.time({
  valid <- Filter(function(x) !is.null(x) && !is.null(x$match_info), parsed_list)
  all_matches <- do.call(rbind, lapply(valid, `[[`, "match_info"))
  all_deliveries <- do.call(rbind, lapply(valid, `[[`, "deliveries"))
  all_innings <- do.call(rbind, lapply(valid, `[[`, "innings"))
  all_players <- do.call(rbind, lapply(valid, `[[`, "players"))
  all_powerplays <- do.call(rbind, lapply(valid, `[[`, "powerplays"))

  all_matches <- all_matches[!duplicated(all_matches$match_id), ]
  all_deliveries <- all_deliveries[!duplicated(all_deliveries$delivery_id), ]
  all_players <- all_players[!duplicated(all_players$player_id), ]
})["elapsed"]
cat("  Combine:", fmt_time(t_combine), "\n")

# Step 3: DuckDB Load
t_load <- system.time({
  conn <- DBI::dbConnect(duckdb::duckdb(), test_db)
  bouncer:::create_schema(conn, verbose = FALSE)

  bouncer:::bulk_write_arrow(conn, "matches", all_matches)
  bouncer:::bulk_write_arrow(conn, "deliveries", all_deliveries)
  bouncer:::bulk_write_arrow(conn, "match_innings", all_innings)
  bouncer:::bulk_write_arrow(conn, "players", all_players)
  if (!is.null(all_powerplays) && nrow(all_powerplays) > 0) {
    bouncer:::bulk_write_arrow(conn, "innings_powerplays", all_powerplays)
  }

  DBI::dbDisconnect(conn, shutdown = TRUE)
})["elapsed"]
cat("  DuckDB Load:", fmt_time(t_load), "\n")

total_time <- t_parse + t_combine + t_load
cat("  TOTAL:", fmt_time(total_time), "\n\n")

results$seq_arrow <- list(
  method = "Sequential → Arrow → DuckDB",
  parse = t_parse, combine = t_combine, write = 0, load = t_load,
  total = total_time
)

file.remove(test_db)
rm(parsed_list, valid, all_matches, all_deliveries, all_innings, all_players, all_powerplays)
gc()

# =============================================================================
# METHOD 2: Parallel (all cores) → Arrow → DuckDB
# =============================================================================
if (bouncer:::has_parallel_support()) {
  cat("--- METHOD 2: Parallel (all cores) → Arrow → DuckDB ---\n")

  test_db <- file.path(temp_base, "test2.duckdb")
  if (file.exists(test_db)) file.remove(test_db)

  n_workers <- max(1, parallel::detectCores() - 1)
  cat("  Workers:", n_workers, "\n")

  # Step 1: Parse (parallel)
  t_parse <- system.time({
    old_plan <- future::plan(future::multisession, workers = n_workers)

    parsed_list <- furrr::future_map(test_files, function(f) {
      tryCatch(parse_cricsheet_json(f), error = function(e) NULL)
    }, .options = furrr::furrr_options(seed = TRUE))

    future::plan(old_plan)
  })["elapsed"]
  cat("  Parse:", fmt_time(t_parse), "\n")

  # Step 2: Combine
  t_combine <- system.time({
    valid <- Filter(function(x) !is.null(x) && !is.null(x$match_info), parsed_list)
    all_matches <- do.call(rbind, lapply(valid, `[[`, "match_info"))
    all_deliveries <- do.call(rbind, lapply(valid, `[[`, "deliveries"))
    all_innings <- do.call(rbind, lapply(valid, `[[`, "innings"))
    all_players <- do.call(rbind, lapply(valid, `[[`, "players"))
    all_powerplays <- do.call(rbind, lapply(valid, `[[`, "powerplays"))

    all_matches <- all_matches[!duplicated(all_matches$match_id), ]
    all_deliveries <- all_deliveries[!duplicated(all_deliveries$delivery_id), ]
    all_players <- all_players[!duplicated(all_players$player_id), ]
  })["elapsed"]
  cat("  Combine:", fmt_time(t_combine), "\n")

  # Step 3: DuckDB Load
  t_load <- system.time({
    conn <- DBI::dbConnect(duckdb::duckdb(), test_db)
    bouncer:::create_schema(conn, verbose = FALSE)

    bouncer:::bulk_write_arrow(conn, "matches", all_matches)
    bouncer:::bulk_write_arrow(conn, "deliveries", all_deliveries)
    bouncer:::bulk_write_arrow(conn, "match_innings", all_innings)
    bouncer:::bulk_write_arrow(conn, "players", all_players)
    if (!is.null(all_powerplays) && nrow(all_powerplays) > 0) {
      bouncer:::bulk_write_arrow(conn, "innings_powerplays", all_powerplays)
    }

    DBI::dbDisconnect(conn, shutdown = TRUE)
  })["elapsed"]
  cat("  DuckDB Load:", fmt_time(t_load), "\n")

  total_time <- t_parse + t_combine + t_load
  cat("  TOTAL:", fmt_time(total_time), "\n\n")

  results$parallel_all <- list(
    method = paste0("Parallel (", n_workers, ") → Arrow → DuckDB"),
    parse = t_parse, combine = t_combine, write = 0, load = t_load,
    total = total_time
  )

  file.remove(test_db)
  rm(parsed_list, valid, all_matches, all_deliveries, all_innings, all_players, all_powerplays)
  gc()
}

# =============================================================================
# METHOD 3: Parallel (8 workers) → Arrow → DuckDB
# =============================================================================
if (bouncer:::has_parallel_support()) {
  cat("--- METHOD 3: Parallel (8 workers) → Arrow → DuckDB ---\n")

  test_db <- file.path(temp_base, "test3.duckdb")
  if (file.exists(test_db)) file.remove(test_db)

  t_parse <- system.time({
    old_plan <- future::plan(future::multisession, workers = 8)

    parsed_list <- furrr::future_map(test_files, function(f) {
      tryCatch(parse_cricsheet_json(f), error = function(e) NULL)
    }, .options = furrr::furrr_options(seed = TRUE))

    future::plan(old_plan)
  })["elapsed"]
  cat("  Parse:", fmt_time(t_parse), "\n")

  t_combine <- system.time({
    valid <- Filter(function(x) !is.null(x) && !is.null(x$match_info), parsed_list)
    all_matches <- do.call(rbind, lapply(valid, `[[`, "match_info"))
    all_deliveries <- do.call(rbind, lapply(valid, `[[`, "deliveries"))
    all_innings <- do.call(rbind, lapply(valid, `[[`, "innings"))
    all_players <- do.call(rbind, lapply(valid, `[[`, "players"))
    all_powerplays <- do.call(rbind, lapply(valid, `[[`, "powerplays"))

    all_matches <- all_matches[!duplicated(all_matches$match_id), ]
    all_deliveries <- all_deliveries[!duplicated(all_deliveries$delivery_id), ]
    all_players <- all_players[!duplicated(all_players$player_id), ]
  })["elapsed"]
  cat("  Combine:", fmt_time(t_combine), "\n")

  t_load <- system.time({
    conn <- DBI::dbConnect(duckdb::duckdb(), test_db)
    bouncer:::create_schema(conn, verbose = FALSE)

    bouncer:::bulk_write_arrow(conn, "matches", all_matches)
    bouncer:::bulk_write_arrow(conn, "deliveries", all_deliveries)
    bouncer:::bulk_write_arrow(conn, "match_innings", all_innings)
    bouncer:::bulk_write_arrow(conn, "players", all_players)
    if (!is.null(all_powerplays) && nrow(all_powerplays) > 0) {
      bouncer:::bulk_write_arrow(conn, "innings_powerplays", all_powerplays)
    }

    DBI::dbDisconnect(conn, shutdown = TRUE)
  })["elapsed"]
  cat("  DuckDB Load:", fmt_time(t_load), "\n")

  total_time <- t_parse + t_combine + t_load
  cat("  TOTAL:", fmt_time(total_time), "\n\n")

  results$parallel_8 <- list(
    method = "Parallel (8) → Arrow → DuckDB",
    parse = t_parse, combine = t_combine, write = 0, load = t_load,
    total = total_time
  )

  file.remove(test_db)
  rm(parsed_list, valid, all_matches, all_deliveries, all_innings, all_players, all_powerplays)
  gc()
}

# =============================================================================
# METHOD 4: Parallel → Arrow concat (faster combine) → DuckDB
# =============================================================================
if (bouncer:::has_parallel_support() && bouncer:::has_arrow_support()) {
  cat("--- METHOD 4: Parallel (8) → data.table rbindlist → DuckDB ---\n")

  test_db <- file.path(temp_base, "test4.duckdb")
  if (file.exists(test_db)) file.remove(test_db)

  t_parse <- system.time({
    old_plan <- future::plan(future::multisession, workers = 8)

    parsed_list <- furrr::future_map(test_files, function(f) {
      tryCatch(parse_cricsheet_json(f), error = function(e) NULL)
    }, .options = furrr::furrr_options(seed = TRUE))

    future::plan(old_plan)
  })["elapsed"]
  cat("  Parse:", fmt_time(t_parse), "\n")

  # Use data.table rbindlist instead of rbind (much faster)
  t_combine <- system.time({
    valid <- Filter(function(x) !is.null(x) && !is.null(x$match_info), parsed_list)

    # data.table::rbindlist is much faster than do.call(rbind, ...)
    all_matches <- data.table::rbindlist(lapply(valid, `[[`, "match_info"), fill = TRUE) |> as.data.frame()
    all_deliveries <- data.table::rbindlist(lapply(valid, `[[`, "deliveries"), fill = TRUE) |> as.data.frame()
    all_innings <- data.table::rbindlist(lapply(valid, `[[`, "innings"), fill = TRUE) |> as.data.frame()
    all_players <- data.table::rbindlist(lapply(valid, `[[`, "players"), fill = TRUE) |> as.data.frame()

    pp_valid <- Filter(function(x) !is.null(x$powerplays) && nrow(x$powerplays) > 0, valid)
    if (length(pp_valid) > 0) {
      all_powerplays <- data.table::rbindlist(lapply(pp_valid, `[[`, "powerplays"), fill = TRUE) |> as.data.frame()
    } else {
      all_powerplays <- NULL
    }

    all_matches <- all_matches[!duplicated(all_matches$match_id), ]
    all_deliveries <- all_deliveries[!duplicated(all_deliveries$delivery_id), ]
    all_players <- all_players[!duplicated(all_players$player_id), ]
  })["elapsed"]
  cat("  Combine (data.table):", fmt_time(t_combine), "\n")

  t_load <- system.time({
    conn <- DBI::dbConnect(duckdb::duckdb(), test_db)
    bouncer:::create_schema(conn, verbose = FALSE)

    bouncer:::bulk_write_arrow(conn, "matches", all_matches)
    bouncer:::bulk_write_arrow(conn, "deliveries", all_deliveries)
    bouncer:::bulk_write_arrow(conn, "match_innings", all_innings)
    bouncer:::bulk_write_arrow(conn, "players", all_players)
    if (!is.null(all_powerplays) && nrow(all_powerplays) > 0) {
      bouncer:::bulk_write_arrow(conn, "innings_powerplays", all_powerplays)
    }

    DBI::dbDisconnect(conn, shutdown = TRUE)
  })["elapsed"]
  cat("  DuckDB Load:", fmt_time(t_load), "\n")

  total_time <- t_parse + t_combine + t_load
  cat("  TOTAL:", fmt_time(total_time), "\n\n")

  results$parallel_dt <- list(
    method = "Parallel (8) → data.table → DuckDB",
    parse = t_parse, combine = t_combine, write = 0, load = t_load,
    total = total_time
  )

  file.remove(test_db)
  rm(parsed_list, valid, all_matches, all_deliveries, all_innings, all_players, all_powerplays)
  gc()
}

# =============================================================================
# METHOD 5: Sequential → Parquet files → DuckDB read_parquet
# =============================================================================
if (bouncer:::has_arrow_support()) {
  cat("--- METHOD 5: Sequential → Parquet → DuckDB read_parquet ---\n")

  test_db <- file.path(temp_base, "test5.duckdb")
  parquet_dir <- file.path(temp_base, "parquet5")
  if (file.exists(test_db)) file.remove(test_db)
  dir.create(parquet_dir, showWarnings = FALSE, recursive = TRUE)

  batch_size <- 100

  # Step 1: Parse
  t_parse <- system.time({
    parsed_list <- vector("list", n_test)
    for (i in seq_along(test_files)) {
      parsed_list[[i]] <- tryCatch(
        parse_cricsheet_json(test_files[i]),
        error = function(e) NULL
      )
    }
  })["elapsed"]
  cat("  Parse:", fmt_time(t_parse), "\n")

  # Step 2: Combine in batches + Write Parquet
  t_write <- system.time({
    batch_ids <- ceiling(seq_len(n_test) / batch_size)
    batches <- split(seq_len(n_test), batch_ids)

    for (i in seq_along(batches)) {
      batch_idx <- batches[[i]]
      batch_parsed <- parsed_list[batch_idx]
      valid <- Filter(function(x) !is.null(x) && !is.null(x$match_info), batch_parsed)

      if (length(valid) > 0) {
        all_m <- do.call(rbind, lapply(valid, `[[`, "match_info"))
        all_d <- do.call(rbind, lapply(valid, `[[`, "deliveries"))
        all_i <- do.call(rbind, lapply(valid, `[[`, "innings"))
        all_p <- do.call(rbind, lapply(valid, `[[`, "players"))

        arrow::write_parquet(all_m, file.path(parquet_dir, sprintf("matches_%04d.parquet", i)))
        arrow::write_parquet(all_d, file.path(parquet_dir, sprintf("deliveries_%04d.parquet", i)))
        arrow::write_parquet(all_i, file.path(parquet_dir, sprintf("innings_%04d.parquet", i)))
        arrow::write_parquet(all_p, file.path(parquet_dir, sprintf("players_%04d.parquet", i)))

        pp_valid <- Filter(function(x) !is.null(x$powerplays) && nrow(x$powerplays) > 0, valid)
        if (length(pp_valid) > 0) {
          all_pp <- do.call(rbind, lapply(pp_valid, `[[`, "powerplays"))
          arrow::write_parquet(all_pp, file.path(parquet_dir, sprintf("powerplays_%04d.parquet", i)))
        }
      }
    }
  })["elapsed"]
  cat("  Combine+Write Parquet:", fmt_time(t_write), "\n")

  # Step 3: DuckDB load from Parquet
  t_load <- system.time({
    conn <- DBI::dbConnect(duckdb::duckdb(), test_db)
    bouncer:::create_schema(conn, verbose = FALSE)

    parquet_path <- gsub("\\\\", "/", parquet_dir)

    DBI::dbExecute(conn, sprintf("INSERT INTO matches SELECT * FROM read_parquet('%s/matches_*.parquet')", parquet_path))
    DBI::dbExecute(conn, sprintf("INSERT INTO deliveries SELECT * FROM read_parquet('%s/deliveries_*.parquet')", parquet_path))
    DBI::dbExecute(conn, sprintf("INSERT INTO match_innings SELECT * FROM read_parquet('%s/innings_*.parquet')", parquet_path))
    DBI::dbExecute(conn, sprintf("INSERT INTO players SELECT DISTINCT * FROM read_parquet('%s/players_*.parquet')", parquet_path))

    pp_files <- list.files(parquet_dir, "powerplays_.*\\.parquet$")
    if (length(pp_files) > 0) {
      DBI::dbExecute(conn, sprintf("INSERT INTO innings_powerplays SELECT * FROM read_parquet('%s/powerplays_*.parquet')", parquet_path))
    }

    DBI::dbDisconnect(conn, shutdown = TRUE)
  })["elapsed"]
  cat("  DuckDB Load (read_parquet):", fmt_time(t_load), "\n")

  total_time <- t_parse + t_write + t_load
  cat("  TOTAL:", fmt_time(total_time), "\n\n")

  results$seq_parquet <- list(
    method = "Sequential → Parquet → DuckDB",
    parse = t_parse, combine = 0, write = t_write, load = t_load,
    total = total_time
  )

  file.remove(test_db)
  unlink(parquet_dir, recursive = TRUE)
  rm(parsed_list)
  gc()
}

# =============================================================================
# RESULTS SUMMARY
# =============================================================================
cat("\n", strrep("=", 70), "\n")
cat("BENCHMARK RESULTS - STEP BY STEP TIMING\n")
cat(strrep("=", 70), "\n\n")

# Create detailed results table
results_df <- do.call(rbind, lapply(names(results), function(nm) {
  r <- results[[nm]]
  data.frame(
    Method = r$method,
    Parse = round(r$parse, 1),
    Combine = round(r$combine, 1),
    Write = round(r$write, 1),
    Load = round(r$load, 1),
    Total = round(r$total, 1),
    stringsAsFactors = FALSE
  )
}))

results_df <- results_df[order(results_df$Total), ]
results_df$Rank <- seq_len(nrow(results_df))

# Find sequential baseline
seq_total <- results_df$Total[grep("Sequential.*Arrow", results_df$Method)[1]]
results_df$Speedup <- paste0(round(seq_total / results_df$Total, 1), "x")

cat("Step timings (seconds):\n\n")
print(results_df[, c("Rank", "Method", "Parse", "Combine", "Write", "Load", "Total", "Speedup")], row.names = FALSE)

cat("\n")
cat("Winner:", results_df$Method[1], "\n")
cat("Total time:", results_df$Total[1], "seconds\n")

# Extrapolate
full_estimate <- results_df$Total[1] * (length(all_files) / n_test) / 60
cat("\nEstimated time for full", length(all_files), "files:", round(full_estimate, 1), "minutes\n")

# Show bottleneck analysis
cat("\n--- BOTTLENECK ANALYSIS ---\n")
for (i in 1:min(3, nrow(results_df))) {
  r <- results_df[i, ]
  total <- r$Total
  cat(sprintf("\n%s:\n", r$Method))
  cat(sprintf("  Parse:   %5.1fs (%2.0f%%)\n", r$Parse, r$Parse/total*100))
  cat(sprintf("  Combine: %5.1fs (%2.0f%%)\n", r$Combine, r$Combine/total*100))
  if (r$Write > 0) cat(sprintf("  Write:   %5.1fs (%2.0f%%)\n", r$Write, r$Write/total*100))
  cat(sprintf("  Load:    %5.1fs (%2.0f%%)\n", r$Load, r$Load/total*100))
}

unlink(temp_base, recursive = TRUE)
