# 00 Compute Centrality Snapshots ----
#
# This script generates dated network centrality snapshots for use in 3-Way ELO calculation.
# Centrality is computed from delivery data UP TO each snapshot date only, preventing
# data leakage in the ELO system.
#
# Key concepts:
#   - Network centrality captures global player quality through the batter↔bowler network
#   - Centrality = sqrt(unique_opponents × avg_opponent_degree) [Opsahl's α=0.5]
#   - Snapshots are stored with dates for historical lookup
#   - During ELO calculation, we look up the most recent snapshot BEFORE match date
#
# This replaces the old PageRank algorithm which converged to similar values (~5%
# variation) in cricket's highly connected network. Centrality produces 90+
# percentile point gaps between elite and isolated players.
#
# This script should be run BEFORE 04_calculate_3way_elo.R to populate the
# centrality history tables.
#
# Output:
#   - {gender}_{format}_player_centrality_history tables populated in DuckDB

# 1. Setup ----
library(DBI)
library(data.table)

# Load bouncer package
bouncer_root <- tryCatch({
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- args[grep("^--file=", args)]
  if (length(file_arg) > 0) {
    script_path <- normalizePath(sub("^--file=", "", file_arg[1]))
    normalizePath(file.path(dirname(script_path), "..", "..", ".."))
  } else {
    getwd()
  }
}, error = function(e) getwd())

devtools::load_all(bouncer_root)

# 2. Configuration ----
FORMAT_GROUPS <- list(
  t20 = c("T20", "IT20"),
  odi = c("ODI", "ODM"),
  test = c("Test", "MDM")
)

GENDER_CATEGORIES <- list(
  mens = "male",
  womens = "female"
)

# Processing options - can be overridden via command line
# Usage: Rscript 00_compute_centrality_snapshots.R [gender] [format]
# Examples:
#   Rscript 00_compute_centrality_snapshots.R mens t20
#   Rscript 00_compute_centrality_snapshots.R womens odi
#   Rscript 00_compute_centrality_snapshots.R  # runs all (default)

args <- commandArgs(trailingOnly = TRUE)
GENDER_FILTER <- if (length(args) >= 1 && args[1] != "") args[1] else NULL
FORMAT_FILTER <- if (length(args) >= 2 && args[2] != "") args[2] else NULL

SNAPSHOT_MODE <- "weekly"  # "weekly", "games", or "full_rebuild"
GAMES_BETWEEN_SNAPSHOTS <- CENTRALITY_SNAPSHOT_INTERVAL_GAMES  # From constants

# Date range (NULL = process all available data)
START_DATE <- NULL         # e.g., "2020-01-01"
END_DATE <- NULL           # e.g., "2025-12-31"

cat("\n")
cli::cli_h1("Network Centrality Snapshot Generation")
cli::cli_alert_info("Computing dated centrality snapshots for ELO integration")
cli::cli_alert_info("Using Opsahl's weighted degree centrality (α = {CENTRALITY_ALPHA})")
cat("\n")

# Determine what to process
if (is.null(FORMAT_FILTER)) {
  formats_to_process <- names(FORMAT_GROUPS)
} else {
  formats_to_process <- FORMAT_FILTER
}

if (is.null(GENDER_FILTER)) {
  genders_to_process <- names(GENDER_CATEGORIES)
} else {
  genders_to_process <- GENDER_FILTER
}

cli::cli_alert_info("Genders: {paste(toupper(genders_to_process), collapse = ', ')}")
cli::cli_alert_info("Formats: {paste(toupper(formats_to_process), collapse = ', ')}")
cli::cli_alert_info("Snapshot mode: {SNAPSHOT_MODE}")
cat("\n")

# 3. Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cli::cli_alert_success("Connected to database")

# 4. Process Each Gender and Format ----
total_start_time <- Sys.time()

for (current_gender in genders_to_process) {

gender_db_value <- GENDER_CATEGORIES[[current_gender]]

cat("\n")
cli::cli_rule("{toupper(current_gender)} CRICKET")
cat("\n")

for (current_format in formats_to_process) {

format_start_time <- Sys.time()
cat("\n")
cli::cli_h1("{toupper(current_gender)} {toupper(current_format)} Centrality Snapshots")
cat("\n")

format_match_types <- FORMAT_GROUPS[[current_format]]
match_type_filter <- paste(sprintf("'%s'", tolower(format_match_types)), collapse = ", ")

# 4.1 Get Match Dates for Snapshots ----
cli::cli_h2("Determining snapshot dates")

# Get all unique match dates in chronological order
match_dates_query <- sprintf("
  SELECT DISTINCT m.match_date, COUNT(*) as delivery_count
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  WHERE LOWER(d.match_type) IN (%s)
    AND m.gender = '%s'
    AND d.batter_id IS NOT NULL
    AND d.bowler_id IS NOT NULL
  GROUP BY m.match_date
  ORDER BY m.match_date
", match_type_filter, gender_db_value)

match_dates <- DBI::dbGetQuery(conn, match_dates_query)
setDT(match_dates)

if (nrow(match_dates) == 0) {
  cli::cli_alert_warning("No matches found for {current_gender} {current_format}")
  next
}

cli::cli_alert_info("Found {nrow(match_dates)} unique match dates from {match_dates$match_date[1]} to {match_dates$match_date[.N]}")

# Determine which dates to create snapshots for
if (SNAPSHOT_MODE == "weekly") {
  # Take one snapshot per week (use last match date of each week)
  match_dates[, week := as.Date(cut(as.Date(match_date), "week"))]
  snapshot_dates <- match_dates[, .(snapshot_date = max(match_date)), by = week]$snapshot_date

} else if (SNAPSHOT_MODE == "games") {
  # Take snapshot every N games
  match_dates[, cumulative_deliveries := cumsum(delivery_count)]
  threshold_indices <- seq(1, nrow(match_dates), by = ceiling(GAMES_BETWEEN_SNAPSHOTS / 5))
  snapshot_dates <- match_dates$match_date[threshold_indices]

} else if (SNAPSHOT_MODE == "full_rebuild") {
  # Just create one snapshot at the end (for testing/initial setup)
  snapshot_dates <- match_dates$match_date[nrow(match_dates)]
}

cli::cli_alert_info("Will create {length(snapshot_dates)} snapshots in {SNAPSHOT_MODE} mode")

# 4.2 Check Existing Snapshots ----
existing_snapshots <- get_centrality_snapshot_dates(current_format, conn, gender = current_gender)

if (length(existing_snapshots) > 0) {
  new_snapshots <- snapshot_dates[!snapshot_dates %in% existing_snapshots]
  cli::cli_alert_info("Found {length(existing_snapshots)} existing snapshots, {length(new_snapshots)} new to create")
  snapshot_dates <- new_snapshots
} else {
  cli::cli_alert_info("No existing snapshots, will create all {length(snapshot_dates)}")
}

if (length(snapshot_dates) == 0) {
  cli::cli_alert_success("All snapshots up to date for {current_gender} {current_format}")
  next
}

# 4.3 Ensure Table Exists ----
ensure_centrality_history_table(current_format, conn, gender = current_gender)

# 4.4 OPTIMIZATION: Pre-load ALL deliveries once ----
cli::cli_h2("Loading all deliveries (one-time)")
load_start <- Sys.time()

all_deliveries_query <- sprintf("
  SELECT
    d.batter_id,
    d.bowler_id,
    d.runs_batter,
    CASE WHEN d.player_out_id IS NOT NULL AND d.player_out_id != '' THEN 1 ELSE 0 END as is_wicket,
    m.match_date
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  WHERE LOWER(d.match_type) IN (%s)
    AND m.gender = '%s'
    AND d.batter_id IS NOT NULL
    AND d.bowler_id IS NOT NULL
  ORDER BY m.match_date
", match_type_filter, gender_db_value)

all_deliveries <- DBI::dbGetQuery(conn, all_deliveries_query)
setDT(all_deliveries)
all_deliveries[, match_date := as.Date(match_date)]
all_deliveries[, is_wicket_delivery := is_wicket]

load_time <- round(difftime(Sys.time(), load_start, units = "secs"), 1)
cli::cli_alert_success("Loaded {format(nrow(all_deliveries), big.mark = ',')} deliveries in {load_time}s")

# 4.5 Generate Snapshots ----
cli::cli_h2("Generating centrality snapshots")

# Track timing for progress estimation
snapshot_times <- numeric(0)

# Set key for fast filtering (data already sorted by match_date from query)
setkey(all_deliveries, match_date)

cli::cli_progress_bar("Computing snapshots", total = length(snapshot_dates))

for (i in seq_along(snapshot_dates)) {
  iter_start <- Sys.time()
  snapshot_date <- as.Date(snapshot_dates[i])

  # Filter deliveries up to this date (setkey makes this fast)
  # Note: This creates a copy, but we clean it up at end of iteration
  deliveries <- all_deliveries[match_date <= snapshot_date]

  if (nrow(deliveries) < 1000) {
    cli::cli_progress_update()
    next  # Not enough data for meaningful centrality
  }

  # Build matrices and compute network centrality
  tryCatch({
    matrices <- build_matchup_matrices(
      deliveries,
      format = "all",  # Already filtered in query
      min_deliveries = CENTRALITY_MIN_DELIVERIES
    )

    # Calculate network centrality (replaces PageRank)
    cent_result <- calculate_network_centrality(
      matchup_matrix = matrices$matchup_matrix,
      alpha = CENTRALITY_ALPHA
    )

    # Classify into tiers
    batter_df <- classify_centrality_tiers(
      cent_result$batter_centrality,
      n_players = length(matrices$batter_ids)
    )
    batter_df$deliveries <- matrices$batter_deliveries[batter_df$player_id]
    batter_df$unique_opponents <- cent_result$batter_unique_opps[batter_df$player_id]
    batter_df$avg_opponent_degree <- cent_result$batter_avg_opp_degree[batter_df$player_id]
    batter_df$role <- "batter"

    bowler_df <- classify_centrality_tiers(
      cent_result$bowler_centrality,
      n_players = length(matrices$bowler_ids)
    )
    bowler_df$deliveries <- matrices$bowler_deliveries[bowler_df$player_id]
    bowler_df$unique_opponents <- cent_result$bowler_unique_opps[bowler_df$player_id]
    bowler_df$avg_opponent_degree <- cent_result$bowler_avg_opp_degree[bowler_df$player_id]
    bowler_df$role <- "bowler"

    centrality_result <- list(
      batters = batter_df,
      bowlers = bowler_df,
      algorithm = list(
        method = "network_centrality",
        alpha = CENTRALITY_ALPHA
      )
    )

    # Store snapshot with gender-specific table
    store_centrality_snapshot(
      centrality_result = centrality_result,
      snapshot_date = snapshot_date,
      format = current_format,
      conn = conn,
      gender = current_gender,
      verbose = FALSE
    )

    # MEMORY CLEANUP: Remove large intermediate objects
    rm(matrices, cent_result, batter_df, bowler_df, centrality_result)

    # Track timing (capture delivery count before cleanup)
    n_deliveries <- nrow(deliveries)
    iter_time <- as.numeric(difftime(Sys.time(), iter_start, units = "secs"))
    snapshot_times <- c(snapshot_times, iter_time)

    # Print timing every 10 snapshots
    if (i %% 10 == 0) {
      avg_time <- mean(snapshot_times)
      remaining <- length(snapshot_dates) - i
      eta_mins <- round((remaining * avg_time) / 60, 1)
      cli::cli_alert_info(
        "[{i}/{length(snapshot_dates)}] {snapshot_date}: {format(n_deliveries, big.mark = ',')} deliveries, {round(iter_time, 1)}s (avg {round(avg_time, 1)}s, ETA {eta_mins}min)"
      )
    }

  }, error = function(e) {
    cli::cli_alert_warning("Error computing centrality for {snapshot_date}: {e$message}")
  })

  # MEMORY CLEANUP: Remove deliveries subset and run gc() periodically
  rm(deliveries)
  if (i %% 20 == 0) {
    gc(verbose = FALSE)
  }

  cli::cli_progress_update()
}

# Clean up large object to free memory
rm(all_deliveries)
gc()

cli::cli_progress_done()

# 4.6 Cleanup Old Snapshots ----
cli::cli_h2("Cleaning up old snapshots")
deleted <- delete_old_centrality_snapshots(current_format, CENTRALITY_SNAPSHOT_KEEP_MONTHS, conn, gender = current_gender)

if (deleted == 0) {
  cli::cli_alert_info("No old snapshots to delete")
}

# 4.7 Summary with timing ----
format_elapsed <- round(difftime(Sys.time(), format_start_time, units = "mins"), 1)
final_snapshots <- get_centrality_snapshot_dates(current_format, conn, gender = current_gender)
cli::cli_alert_success(
  "{toupper(current_gender)} {toupper(current_format)}: {length(final_snapshots)} snapshots in {format_elapsed} minutes"
)

if (length(final_snapshots) > 0) {
  cli::cli_alert_info("Date range: {final_snapshots[length(final_snapshots)]} to {final_snapshots[1]}")
}

# MEMORY CLEANUP: Force garbage collection between formats
gc(verbose = FALSE)

cat("\n")

}  # End format loop

# MEMORY CLEANUP: Force garbage collection between genders
gc(verbose = FALSE)

}  # End gender loop

total_elapsed <- round(difftime(Sys.time(), total_start_time, units = "mins"), 1)
cli::cli_alert_success("Total runtime: {total_elapsed} minutes")

# 5. Final Summary ----
cat("\n")
cli::cli_h1("Centrality Snapshot Generation Complete")

for (fmt in formats_to_process) {
  for (gender in genders_to_process) {
    dates <- get_centrality_snapshot_dates(fmt, conn, gender = gender)
    cli::cli_alert_info("{toupper(gender)} {toupper(fmt)}: {length(dates)} snapshots")
  }
}

cat("\n")
cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "Run 01_calculate_3way_elo.R to use centrality in ELO calculation",
  "i" = "Snapshots stored in {{gender}}_{{format}}_player_centrality_history tables",
  "i" = "Use get_centrality_as_of() to look up centrality for a given date"
))
cat("\n")

cli::cli_h3("Query Example")
cat("
-- Get most recent centrality snapshot for a player
SELECT *
FROM mens_t20_player_centrality_history
WHERE player_id = 'some_player_id'
  AND role = 'batter'
  AND snapshot_date < '2024-01-15'
ORDER BY snapshot_date DESC
LIMIT 1
")
cat("\n")
