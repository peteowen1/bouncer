# Quick benchmark: Parallel 23 + data.table (expected winner)
devtools::load_all()

data_dir <- bouncer:::find_bouncerdata_dir()
json_dir <- file.path(data_dir, "json_files")
all_files <- list.files(json_dir, pattern = "\\.json$", full.names = TRUE)

n_test <- 1000
test_files <- all_files[1:n_test]

cat("=== BEST METHOD TEST: Parallel 23 + data.table ===\n\n")

temp_base <- file.path(tempdir(), "bouncer_best")
dir.create(temp_base, showWarnings = FALSE, recursive = TRUE)
test_db <- file.path(temp_base, "test.duckdb")

fmt_time <- function(t) paste0(round(t, 1), "s")

n_workers <- max(1, parallel::detectCores() - 1)
cat("Workers:", n_workers, "\n\n")

# Step 1: Parse (parallel with all cores)
t_parse <- system.time({
  old_plan <- future::plan(future::multisession, workers = n_workers)

  parsed_list <- furrr::future_map(test_files, function(f) {
    tryCatch(parse_cricsheet_json(f), error = function(e) NULL)
  }, .options = furrr::furrr_options(seed = TRUE))

  future::plan(old_plan)
})["elapsed"]
cat("Parse (", n_workers, " workers):", fmt_time(t_parse), "\n")

# Step 2: Combine with data.table
t_combine <- system.time({
  valid <- Filter(function(x) !is.null(x) && !is.null(x$match_info), parsed_list)

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
cat("Combine (data.table):", fmt_time(t_combine), "\n")

# Step 3: Load to DuckDB
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
cat("DuckDB Load:", fmt_time(t_load), "\n")

total_time <- t_parse + t_combine + t_load
cat("\n=== TOTAL:", fmt_time(total_time), "===\n")

# Verify
conn <- DBI::dbConnect(duckdb::duckdb(), test_db, read_only = TRUE)
n_matches <- DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM matches")$n
n_del <- DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM deliveries")$n
DBI::dbDisconnect(conn, shutdown = TRUE)

cat("\nLoaded:", n_matches, "matches,", n_del, "deliveries\n")

# Extrapolate
full_estimate <- total_time * (length(all_files) / n_test) / 60
cat("\nEstimated for full", length(all_files), "files:", round(full_estimate, 1), "minutes\n")

# Bottleneck analysis
cat("\n--- BOTTLENECK ---\n")
cat(sprintf("Parse:   %5.1fs (%2.0f%%)\n", t_parse, t_parse/total_time*100))
cat(sprintf("Combine: %5.1fs (%2.0f%%)\n", t_combine, t_combine/total_time*100))
cat(sprintf("Load:    %5.1fs (%2.0f%%)\n", t_load, t_load/total_time*100))

unlink(temp_base, recursive = TRUE)
