# Benchmark Remote Data Loading Methods
# Tests different approaches for loading cricket data from GitHub releases
# Compares Arrow, DuckDB, and data.table for both loading and aggregation
# Location: data-raw/debug/benchmark_remote_loading.R

library(dplyr)
devtools::load_all()

# =============================================================================
# SETUP
# =============================================================================

cat("\n")
cat(strrep("=", 70), "\n")
cat("BENCHMARK: Remote Data Loading Methods\n")
cat(strrep("=", 70), "\n\n")

# Check for required packages
required_pkgs <- c("arrow", "duckdb", "data.table", "httr2")
optional_pkgs <- c("collapse")

missing_required <- setdiff(required_pkgs, rownames(installed.packages()))
if (length(missing_required) > 0) {
  stop("Missing required packages: ", paste(missing_required, collapse = ", "))
}

has_collapse <- "collapse" %in% rownames(installed.packages())
if (has_collapse) {
  library(collapse)
  cat("collapse package: AVAILABLE\n")
} else {
  cat("collapse package: NOT INSTALLED (will skip collapse benchmarks)\n")
}

# Get release metadata
cat("\nFetching release metadata...\n")
release <- get_latest_release(type = "cricsheet")
cat("Release tag:", release$tag_name, "\n")

# Build parquet URLs
base_url <- sprintf(

"https://github.com/peteowen1/bouncerdata/releases/download/%s",
  release$tag_name
)

# Test data files - note: actual release uses simplified naming
# matches.parquet (~600KB) - unified matches table
# deliveries_ODI_male.parquet (~8MB) - medium size for testing
# deliveries_T20_male.parquet (~17MB) - larger for stress testing
matches_url <- paste0(base_url, "/matches.parquet")
deliveries_url <- paste0(base_url, "/deliveries_ODI_male.parquet")
deliveries_large_url <- paste0(base_url, "/deliveries_T20_male.parquet")

cat("Test data:\n")
cat("  Matches (small):", matches_url, "\n")
cat("  Deliveries ODI (~8MB):", deliveries_url, "\n")
cat("  Deliveries T20 (~17MB):", deliveries_large_url, "\n\n")

# Benchmark configuration
n_iter <- 5
warmup <- 1

cat("Configuration:\n")
cat("  Iterations:", n_iter, "\n")
cat("  Warmup runs:", warmup, "\n\n")

# Results storage
results <- list()

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

#' Multi-iteration timing with warmup
#'
#' @param fn Function to benchmark (no arguments)
#' @param n_iter Number of iterations
#' @param warmup Number of warmup runs (not timed)
#' @return List with mean, sd, min, max times in seconds
benchmark_fn <- function(fn, n_iter = 5, warmup = 1) {
  # Warmup runs
  for (i in seq_len(warmup)) {
    try(fn(), silent = TRUE)
    gc(verbose = FALSE)
  }

  # Timed runs

times <- numeric(n_iter)
  for (i in seq_len(n_iter)) {
    gc(verbose = FALSE)
    times[i] <- system.time(fn())["elapsed"]
  }

  list(
    mean = mean(times),
    sd = sd(times),
    min = min(times),
    max = max(times),
    times = times
  )
}

#' Memory tracking wrapper
#'
#' @param expr Expression to evaluate
#' @return List with result and memory delta in MB
measure_memory <- function(expr) {
  gc(verbose = FALSE)
  mem_before <- sum(gc(verbose = FALSE)[, 2])

  result <- eval(expr)

  gc(verbose = FALSE)
  mem_after <- sum(gc(verbose = FALSE)[, 2])

  list(
    result = result,
    memory_mb = mem_after - mem_before
  )
}

#' Format timing result for display
format_timing <- function(timing, label) {
  sprintf(
    "%s: %.2fs (sd=%.2f, range=%.2f-%.2f)",
    label, timing$mean, timing$sd, timing$min, timing$max
  )
}

# =============================================================================
# SECTION 1: MATCHES TABLE - SMALL DATA (~2MB)
# =============================================================================

cat(strrep("=", 70), "\n")
cat("SECTION 1: Small Data Loading (matches_ODI_male_international)\n")
cat(strrep("=", 70), "\n\n")

# Method 1: DuckDB httpfs (current implementation)
cat("--- Method 1: DuckDB httpfs (current) ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    conn <- DBI::dbConnect(duckdb::duckdb())
    DBI::dbExecute(conn, "INSTALL httpfs")
    DBI::dbExecute(conn, "LOAD httpfs")
    sql <- sprintf("SELECT * FROM '%s'", matches_url)
    df <- DBI::dbGetQuery(conn, sql)
    DBI::dbDisconnect(conn, shutdown = TRUE)
    df
  }, n_iter = n_iter, warmup = warmup)

  results$matches_duckdb_httpfs <- list(
    method = "DuckDB httpfs",
    timing = timing,
    data_type = "matches"
  )
  cat(format_timing(timing, "Time"), "\n\n")
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

# Method 2: Arrow read_parquet
cat("--- Method 2: Arrow read_parquet ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    arrow::read_parquet(matches_url)
  }, n_iter = n_iter, warmup = warmup)

  results$matches_arrow_read <- list(
    method = "Arrow read_parquet",
    timing = timing,
    data_type = "matches"
  )
  cat(format_timing(timing, "Time"), "\n\n")
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

# Method 3: Arrow open_dataset + collect
cat("--- Method 3: Arrow open_dataset + collect ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    arrow::open_dataset(matches_url) |> collect()
  }, n_iter = n_iter, warmup = warmup)

  results$matches_arrow_dataset <- list(
    method = "Arrow open_dataset",
    timing = timing,
    data_type = "matches"
  )
  cat(format_timing(timing, "Time"), "\n\n")
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

# Method 4: Download to temp file + local read
cat("--- Method 4: Download + local read ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    temp_file <- tempfile(fileext = ".parquet")
    on.exit(unlink(temp_file), add = TRUE)

    httr2::request(matches_url) |>
      httr2::req_perform(path = temp_file)

    arrow::read_parquet(temp_file)
  }, n_iter = n_iter, warmup = warmup)

  results$matches_download_local <- list(
    method = "Download + local",
    timing = timing,
    data_type = "matches"
  )
  cat(format_timing(timing, "Time"), "\n\n")
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

# Method 5: DuckDB + Arrow (Arrow reads, register in DuckDB)
cat("--- Method 5: Arrow read + DuckDB register ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    tbl <- arrow::read_parquet(matches_url)
    conn <- DBI::dbConnect(duckdb::duckdb())
    duckdb::duckdb_register_arrow(conn, "matches", tbl)
    df <- DBI::dbGetQuery(conn, "SELECT * FROM matches")
    duckdb::duckdb_unregister_arrow(conn, "matches")
    DBI::dbDisconnect(conn, shutdown = TRUE)
    df
  }, n_iter = n_iter, warmup = warmup)

  results$matches_arrow_duckdb <- list(
    method = "Arrow + DuckDB register",
    timing = timing,
    data_type = "matches"
  )
  cat(format_timing(timing, "Time"), "\n\n")
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

gc()

# =============================================================================
# SECTION 2: DELIVERIES TABLE - LARGE DATA (~17MB T20)
# =============================================================================

cat(strrep("=", 70), "\n")
cat("SECTION 2: Large Data Loading (deliveries_T20_male ~17MB)\n")
cat(strrep("=", 70), "\n\n")

# Use larger T20 deliveries for this section
deliveries_url_large <- deliveries_large_url

# Reduce iterations for large data
n_iter_large <- 3

# Method 1: DuckDB httpfs
cat("--- Method 1: DuckDB httpfs (current) ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    conn <- DBI::dbConnect(duckdb::duckdb())
    DBI::dbExecute(conn, "INSTALL httpfs")
    DBI::dbExecute(conn, "LOAD httpfs")
    sql <- sprintf("SELECT * FROM '%s'", deliveries_url_large)
    df <- DBI::dbGetQuery(conn, sql)
    DBI::dbDisconnect(conn, shutdown = TRUE)
    df
  }, n_iter = n_iter_large, warmup = 1)

  results$deliveries_duckdb_httpfs <- list(
    method = "DuckDB httpfs",
    timing = timing,
    data_type = "deliveries"
  )
  cat(format_timing(timing, "Time"), "\n")

  # Also capture row count
  conn <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbExecute(conn, "INSTALL httpfs")
  DBI::dbExecute(conn, "LOAD httpfs")
  n_rows <- DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) as n FROM '%s'", deliveries_url_large))$n
  DBI::dbDisconnect(conn, shutdown = TRUE)
  cat("Rows:", format(n_rows, big.mark = ","), "\n\n")
  results$deliveries_duckdb_httpfs$rows <- n_rows
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

# Method 2: Arrow read_parquet
cat("--- Method 2: Arrow read_parquet ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    arrow::read_parquet(deliveries_url_large)
  }, n_iter = n_iter_large, warmup = 1)

  results$deliveries_arrow_read <- list(
    method = "Arrow read_parquet",
    timing = timing,
    data_type = "deliveries"
  )
  cat(format_timing(timing, "Time"), "\n\n")
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

# Method 3: Arrow open_dataset + collect
cat("--- Method 3: Arrow open_dataset + collect ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    arrow::open_dataset(deliveries_url_large) |> collect()
  }, n_iter = n_iter_large, warmup = 1)

  results$deliveries_arrow_dataset <- list(
    method = "Arrow open_dataset",
    timing = timing,
    data_type = "deliveries"
  )
  cat(format_timing(timing, "Time"), "\n\n")
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

# Method 4: Download + local read
cat("--- Method 4: Download + local read ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    temp_file <- tempfile(fileext = ".parquet")
    on.exit(unlink(temp_file), add = TRUE)

    httr2::request(deliveries_url_large) |>
      httr2::req_perform(path = temp_file)

    arrow::read_parquet(temp_file)
  }, n_iter = n_iter_large, warmup = 1)

  results$deliveries_download_local <- list(
    method = "Download + local",
    timing = timing,
    data_type = "deliveries"
  )
  cat(format_timing(timing, "Time"), "\n\n")
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

# Method 5: Arrow read + DuckDB register
cat("--- Method 5: Arrow read + DuckDB register ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    tbl <- arrow::read_parquet(deliveries_url_large)
    conn <- DBI::dbConnect(duckdb::duckdb())
    duckdb::duckdb_register_arrow(conn, "deliveries", tbl)
    df <- DBI::dbGetQuery(conn, "SELECT * FROM deliveries")
    duckdb::duckdb_unregister_arrow(conn, "deliveries")
    DBI::dbDisconnect(conn, shutdown = TRUE)
    df
  }, n_iter = n_iter_large, warmup = 1)

  results$deliveries_arrow_duckdb <- list(
    method = "Arrow + DuckDB register",
    timing = timing,
    data_type = "deliveries"
  )
  cat(format_timing(timing, "Time"), "\n\n")
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

gc()

# =============================================================================
# SECTION 3: AGGREGATION METHODS COMPARISON
# =============================================================================

cat(strrep("=", 70), "\n")
cat("SECTION 3: Aggregation Methods (bowling stats pattern)\n")
cat(strrep("=", 70), "\n\n")

# Load data once for aggregation tests (use ODI data - reasonable size)
cat("Loading deliveries data for aggregation tests (ODI ~8MB)...\n")
deliveries_df <- tryCatch({
  arrow::read_parquet(deliveries_url)
}, error = function(e) {
  cat("Failed to load test data:", conditionMessage(e), "\n")
  NULL
})

if (!is.null(deliveries_df)) {
  cat("Loaded", format(nrow(deliveries_df), big.mark = ","), "rows\n\n")

  # Convert to data.table for data.table tests
  deliveries_dt <- data.table::as.data.table(deliveries_df)

  # Method A: dplyr (current approach)
  cat("--- Method A: dplyr group_by + summarise ---\n")

  tryCatch({
    timing <- benchmark_fn(function() {
      deliveries_df |>
        group_by(bowler_id) |>
        summarise(
          balls_bowled = n(),
          runs_conceded = sum(runs_total, na.rm = TRUE),
          wickets = sum(is_wicket, na.rm = TRUE),
          dots = sum(runs_total == 0, na.rm = TRUE),
          .groups = "drop"
        )
    }, n_iter = n_iter, warmup = warmup)

    results$agg_dplyr <- list(
      method = "dplyr",
      timing = timing,
      data_type = "aggregation"
    )
    cat(format_timing(timing, "Time"), "\n\n")
  }, error = function(e) {
    cat("FAILED:", conditionMessage(e), "\n\n")
  })

  # Method B: data.table
  cat("--- Method B: data.table ---\n")

  tryCatch({
    timing <- benchmark_fn(function() {
      deliveries_dt[, .(
        balls_bowled = .N,
        runs_conceded = sum(runs_total, na.rm = TRUE),
        wickets = sum(is_wicket, na.rm = TRUE),
        dots = sum(runs_total == 0L, na.rm = TRUE)
      ), by = bowler_id]
    }, n_iter = n_iter, warmup = warmup)

    results$agg_data_table <- list(
      method = "data.table",
      timing = timing,
      data_type = "aggregation"
    )
    cat(format_timing(timing, "Time"), "\n\n")
  }, error = function(e) {
    cat("FAILED:", conditionMessage(e), "\n\n")
  })

  # Method C: DuckDB SQL (data already in memory, register and query)
  cat("--- Method C: DuckDB SQL ---\n")

  tryCatch({
    timing <- benchmark_fn(function() {
      conn <- DBI::dbConnect(duckdb::duckdb())
      duckdb::duckdb_register(conn, "deliveries", deliveries_df)

      sql <- "
        SELECT
          bowler_id,
          COUNT(*) as balls_bowled,
          SUM(runs_total) as runs_conceded,
          SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets,
          SUM(CASE WHEN runs_total = 0 THEN 1 ELSE 0 END) as dots
        FROM deliveries
        GROUP BY bowler_id
      "
      result <- DBI::dbGetQuery(conn, sql)

      DBI::dbDisconnect(conn, shutdown = TRUE)
      result
    }, n_iter = n_iter, warmup = warmup)

    results$agg_duckdb_sql <- list(
      method = "DuckDB SQL",
      timing = timing,
      data_type = "aggregation"
    )
    cat(format_timing(timing, "Time"), "\n\n")
  }, error = function(e) {
    cat("FAILED:", conditionMessage(e), "\n\n")
  })

  # Method D: Arrow + dplyr (lazy evaluation)
  cat("--- Method D: Arrow + dplyr (lazy) ---\n")

  tryCatch({
    # Convert to Arrow table
    arrow_tbl <- arrow::as_arrow_table(deliveries_df)

    timing <- benchmark_fn(function() {
      arrow_tbl |>
        group_by(bowler_id) |>
        summarise(
          balls_bowled = n(),
          runs_conceded = sum(runs_total, na.rm = TRUE),
          wickets = sum(is_wicket, na.rm = TRUE),
          dots = sum(runs_total == 0L, na.rm = TRUE)
        ) |>
        collect()
    }, n_iter = n_iter, warmup = warmup)

    results$agg_arrow_dplyr <- list(
      method = "Arrow + dplyr",
      timing = timing,
      data_type = "aggregation"
    )
    cat(format_timing(timing, "Time"), "\n\n")
  }, error = function(e) {
    cat("FAILED:", conditionMessage(e), "\n\n")
  })

  # Method E: collapse (if available)
  if (has_collapse) {
    cat("--- Method E: collapse fgroup_by + fsum ---\n")

    tryCatch({
      timing <- benchmark_fn(function() {
        g <- collapse::fgroup_by(deliveries_df, bowler_id)
        collapse::fsummarise(g,
          balls_bowled = collapse::fnobs(bowler_id),
          runs_conceded = collapse::fsum(runs_total, na.rm = TRUE),
          wickets = collapse::fsum(is_wicket, na.rm = TRUE),
          dots = collapse::fsum(runs_total == 0L, na.rm = TRUE)
        )
      }, n_iter = n_iter, warmup = warmup)

      results$agg_collapse <- list(
        method = "collapse",
        timing = timing,
        data_type = "aggregation"
      )
      cat(format_timing(timing, "Time"), "\n\n")
    }, error = function(e) {
      cat("FAILED:", conditionMessage(e), "\n\n")
    })
  } else {
    cat("--- Method E: collapse (SKIPPED - not installed) ---\n\n")
  }

  rm(deliveries_df, deliveries_dt)
  gc()
}

# =============================================================================
# SECTION 4: FULL WORKFLOW COMPARISON
# =============================================================================

cat(strrep("=", 70), "\n")
cat("SECTION 4: Full Workflow (load + aggregate bowling stats)\n")
cat(strrep("=", 70), "\n\n")

# Workflow 1: Current - DuckDB httpfs + dplyr
cat("--- Workflow 1: DuckDB httpfs + dplyr (current) ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    # Load via DuckDB httpfs
    conn <- DBI::dbConnect(duckdb::duckdb())
    DBI::dbExecute(conn, "INSTALL httpfs")
    DBI::dbExecute(conn, "LOAD httpfs")
    df <- DBI::dbGetQuery(conn, sprintf("SELECT * FROM '%s'", deliveries_url))
    DBI::dbDisconnect(conn, shutdown = TRUE)

    # Aggregate with dplyr
    df |>
      group_by(bowler_id) |>
      summarise(
        balls_bowled = n(),
        runs_conceded = sum(runs_total, na.rm = TRUE),
        wickets = sum(is_wicket, na.rm = TRUE),
        .groups = "drop"
      )
  }, n_iter = n_iter_large, warmup = 1)

  results$workflow_current <- list(
    method = "DuckDB httpfs + dplyr",
    timing = timing,
    data_type = "workflow"
  )
  cat(format_timing(timing, "Time"), "\n\n")
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

# Workflow 2: Arrow + data.table
cat("--- Workflow 2: Arrow read + data.table ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    # Load via Arrow
    df <- arrow::read_parquet(deliveries_url)

    # Convert and aggregate with data.table
    dt <- data.table::as.data.table(df)
    dt[, .(
      balls_bowled = .N,
      runs_conceded = sum(runs_total, na.rm = TRUE),
      wickets = sum(is_wicket, na.rm = TRUE)
    ), by = bowler_id]
  }, n_iter = n_iter_large, warmup = 1)

  results$workflow_arrow_dt <- list(
    method = "Arrow + data.table",
    timing = timing,
    data_type = "workflow"
  )
  cat(format_timing(timing, "Time"), "\n\n")
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

# Workflow 3: Download + DuckDB SQL
cat("--- Workflow 3: Download + DuckDB SQL ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    # Download to temp
    temp_file <- tempfile(fileext = ".parquet")
    on.exit(unlink(temp_file), add = TRUE)

    httr2::request(deliveries_url) |>
      httr2::req_perform(path = temp_file)

    # Query with DuckDB
    conn <- DBI::dbConnect(duckdb::duckdb())
    sql <- sprintf("
      SELECT
        bowler_id,
        COUNT(*) as balls_bowled,
        SUM(runs_total) as runs_conceded,
        SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets
      FROM '%s'
      GROUP BY bowler_id
    ", temp_file)
    result <- DBI::dbGetQuery(conn, sql)
    DBI::dbDisconnect(conn, shutdown = TRUE)
    result
  }, n_iter = n_iter_large, warmup = 1)

  results$workflow_download_sql <- list(
    method = "Download + DuckDB SQL",
    timing = timing,
    data_type = "workflow"
  )
  cat(format_timing(timing, "Time"), "\n\n")
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

# Workflow 4: Arrow + collapse (if available)
if (has_collapse) {
  cat("--- Workflow 4: Arrow + collapse ---\n")

  tryCatch({
    timing <- benchmark_fn(function() {
      # Load via Arrow
      df <- arrow::read_parquet(deliveries_url)

      # Aggregate with collapse
      g <- collapse::fgroup_by(df, bowler_id)
      collapse::fsummarise(g,
        balls_bowled = collapse::fnobs(bowler_id),
        runs_conceded = collapse::fsum(runs_total, na.rm = TRUE),
        wickets = collapse::fsum(is_wicket, na.rm = TRUE)
      )
    }, n_iter = n_iter_large, warmup = 1)

    results$workflow_arrow_collapse <- list(
      method = "Arrow + collapse",
      timing = timing,
      data_type = "workflow"
    )
    cat(format_timing(timing, "Time"), "\n\n")
  }, error = function(e) {
    cat("FAILED:", conditionMessage(e), "\n\n")
  })
} else {
  cat("--- Workflow 4: Arrow + collapse (SKIPPED) ---\n\n")
}

# Workflow 5: DuckDB httpfs with SQL aggregation (push down)
cat("--- Workflow 5: DuckDB httpfs + SQL aggregation (pushdown) ---\n")

tryCatch({
  timing <- benchmark_fn(function() {
    conn <- DBI::dbConnect(duckdb::duckdb())
    DBI::dbExecute(conn, "INSTALL httpfs")
    DBI::dbExecute(conn, "LOAD httpfs")

    # Aggregate directly in SQL - data never fully materialized in R
    sql <- sprintf("
      SELECT
        bowler_id,
        COUNT(*) as balls_bowled,
        SUM(runs_total) as runs_conceded,
        SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) as wickets
      FROM '%s'
      GROUP BY bowler_id
    ", deliveries_url)

    result <- DBI::dbGetQuery(conn, sql)
    DBI::dbDisconnect(conn, shutdown = TRUE)
    result
  }, n_iter = n_iter_large, warmup = 1)

  results$workflow_httpfs_sql <- list(
    method = "DuckDB httpfs + SQL agg",
    timing = timing,
    data_type = "workflow"
  )
  cat(format_timing(timing, "Time"), "\n\n")
}, error = function(e) {
  cat("FAILED:", conditionMessage(e), "\n\n")
})

gc()

# =============================================================================
# RESULTS SUMMARY
# =============================================================================

cat("\n")
cat(strrep("=", 70), "\n")
cat("RESULTS SUMMARY\n")
cat(strrep("=", 70), "\n\n")

# Build results data frame
build_results_df <- function(results, data_type_filter) {
  filtered <- Filter(function(r) r$data_type == data_type_filter, results)

  if (length(filtered) == 0) return(NULL)

  do.call(rbind, lapply(names(filtered), function(name) {
    r <- filtered[[name]]
    data.frame(
      Method = r$method,
      Mean_sec = round(r$timing$mean, 3),
      SD_sec = round(r$timing$sd, 3),
      Min_sec = round(r$timing$min, 3),
      Max_sec = round(r$timing$max, 3),
      stringsAsFactors = FALSE
    )
  }))
}

# Small data results (matches)
cat("--- Small Data Loading (matches ~2MB) ---\n")
matches_results <- build_results_df(results, "matches")
if (!is.null(matches_results) && nrow(matches_results) > 0) {
  matches_results <- matches_results[order(matches_results$Mean_sec), ]
  matches_results$Rank <- seq_len(nrow(matches_results))
  baseline <- matches_results$Mean_sec[matches_results$Method == "DuckDB httpfs"]
  if (length(baseline) > 0) {
    matches_results$vs_Current <- round(baseline / matches_results$Mean_sec, 2)
  }
  print(matches_results[, c("Rank", "Method", "Mean_sec", "vs_Current")])
  cat("\n")
}

# Large data results (deliveries)
cat("--- Large Data Loading (deliveries ~50-100MB) ---\n")
deliveries_results <- build_results_df(results, "deliveries")
if (!is.null(deliveries_results) && nrow(deliveries_results) > 0) {
  deliveries_results <- deliveries_results[order(deliveries_results$Mean_sec), ]
  deliveries_results$Rank <- seq_len(nrow(deliveries_results))
  baseline <- deliveries_results$Mean_sec[deliveries_results$Method == "DuckDB httpfs"]
  if (length(baseline) > 0) {
    deliveries_results$vs_Current <- round(baseline / deliveries_results$Mean_sec, 2)
  }
  print(deliveries_results[, c("Rank", "Method", "Mean_sec", "vs_Current")])
  cat("\n")
}

# Aggregation results
cat("--- Aggregation Methods (in-memory data) ---\n")
agg_results <- build_results_df(results, "aggregation")
if (!is.null(agg_results) && nrow(agg_results) > 0) {
  agg_results <- agg_results[order(agg_results$Mean_sec), ]
  agg_results$Rank <- seq_len(nrow(agg_results))
  baseline <- agg_results$Mean_sec[agg_results$Method == "dplyr"]
  if (length(baseline) > 0) {
    agg_results$vs_dplyr <- round(baseline / agg_results$Mean_sec, 2)
  }
  print(agg_results[, c("Rank", "Method", "Mean_sec", "vs_dplyr")])
  cat("\n")
}

# Full workflow results
cat("--- Full Workflow (load + aggregate) ---\n")
workflow_results <- build_results_df(results, "workflow")
if (!is.null(workflow_results) && nrow(workflow_results) > 0) {
  workflow_results <- workflow_results[order(workflow_results$Mean_sec), ]
  workflow_results$Rank <- seq_len(nrow(workflow_results))
  baseline <- workflow_results$Mean_sec[workflow_results$Method == "DuckDB httpfs + dplyr"]
  if (length(baseline) > 0) {
    workflow_results$vs_Current <- round(baseline / workflow_results$Mean_sec, 2)
  }
  print(workflow_results[, c("Rank", "Method", "Mean_sec", "vs_Current")])
  cat("\n")
}

# =============================================================================
# BOTTLENECK ANALYSIS
# =============================================================================

cat(strrep("=", 70), "\n")
cat("BOTTLENECK ANALYSIS\n")
cat(strrep("=", 70), "\n\n")

# Compare loading vs aggregation time
if (!is.null(deliveries_results) && !is.null(agg_results)) {
  best_load <- deliveries_results$Mean_sec[1]
  best_agg <- agg_results$Mean_sec[1]

  load_pct <- round(best_load / (best_load + best_agg) * 100, 1)
  agg_pct <- round(best_agg / (best_load + best_agg) * 100, 1)

  cat("Best loading time:", round(best_load, 2), "sec\n")
  cat("Best aggregation time:", round(best_agg, 3), "sec\n")
  cat("\n")
  cat("Time breakdown:\n")
  cat("  Loading:", load_pct, "%\n")
  cat("  Aggregation:", agg_pct, "%\n")
  cat("\n")

  if (load_pct > 90) {
    cat("BOTTLENECK: Network/Loading dominates. Aggregation optimization has minimal impact.\n")
  } else if (agg_pct > 50) {
    cat("BOTTLENECK: Aggregation is significant. Consider faster aggregation methods.\n")
  } else {
    cat("BALANCED: Both loading and aggregation contribute meaningfully.\n")
  }
  cat("\n")
}

# =============================================================================
# RECOMMENDATIONS
# =============================================================================

cat(strrep("=", 70), "\n")
cat("RECOMMENDATIONS\n")
cat(strrep("=", 70), "\n\n")

# Find winners
if (!is.null(deliveries_results) && nrow(deliveries_results) > 0) {
  best_loader <- deliveries_results$Method[1]
  best_loader_time <- deliveries_results$Mean_sec[1]

  cat("1. LOADING: Best method is '", best_loader, "' at ", round(best_loader_time, 2), "s\n", sep = "")

  current_time <- deliveries_results$Mean_sec[deliveries_results$Method == "DuckDB httpfs"]
  if (length(current_time) > 0 && best_loader != "DuckDB httpfs") {
    speedup <- round(current_time / best_loader_time, 1)
    cat("   -> ", speedup, "x faster than current DuckDB httpfs approach\n", sep = "")
  }
  cat("\n")
}

if (!is.null(agg_results) && nrow(agg_results) > 0) {
  best_agg <- agg_results$Method[1]
  best_agg_time <- agg_results$Mean_sec[1]

  cat("2. AGGREGATION: Best method is '", best_agg, "' at ", round(best_agg_time, 4), "s\n", sep = "")

  dplyr_time <- agg_results$Mean_sec[agg_results$Method == "dplyr"]
  if (length(dplyr_time) > 0 && best_agg != "dplyr") {
    speedup <- round(dplyr_time / best_agg_time, 1)
    cat("   -> ", speedup, "x faster than current dplyr approach\n", sep = "")
  }
  cat("\n")
}

if (!is.null(workflow_results) && nrow(workflow_results) > 0) {
  best_workflow <- workflow_results$Method[1]
  best_workflow_time <- workflow_results$Mean_sec[1]

  cat("3. FULL WORKFLOW: Best method is '", best_workflow, "' at ", round(best_workflow_time, 2), "s\n", sep = "")

  current_workflow_time <- workflow_results$Mean_sec[workflow_results$Method == "DuckDB httpfs + dplyr"]
  if (length(current_workflow_time) > 0 && best_workflow != "DuckDB httpfs + dplyr") {
    speedup <- round(current_workflow_time / best_workflow_time, 1)
    cat("   -> ", speedup, "x faster than current implementation\n", sep = "")
  }
  cat("\n")
}

cat("SUMMARY:\n")
cat("- For remote loading: Consider switching from DuckDB httpfs to Arrow read_parquet\n")
cat("- For aggregation: data.table or collapse offer significant speedups over dplyr\n")
cat("- For full workflow: Test Arrow + data.table or DuckDB SQL pushdown\n")
cat("- SQL pushdown (aggregating in DuckDB before fetching) may reduce memory usage\n")

cat("\n")
cat(strrep("=", 70), "\n")
cat("Benchmark complete!\n")
cat(strrep("=", 70), "\n")
