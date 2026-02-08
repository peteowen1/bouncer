# Resume 3-Way ELO Database Insertion from Checkpoint
# Uses smaller batch sizes to avoid memory issues that caused segfault

library(DBI)
library(data.table)
devtools::load_all()

# Configuration - use smaller batches to avoid memory issues
BATCH_SIZE <- 2000  # Reduced from 10000
GC_EVERY_N_BATCHES <- 25  # Force garbage collection periodically

# Find the checkpoint file
checkpoint_file <- file.path(find_bouncerdata_dir(), "checkpoints",
                              "mens_t20_3way_elo_checkpoint.qs")

if (!file.exists(checkpoint_file)) {
  cli::cli_abort("Checkpoint file not found: {checkpoint_file}")
}

cli::cli_h1("Resuming 3-Way ELO Database Insertion")
cli::cli_alert_info("Loading checkpoint from: {checkpoint_file}")

# Load checkpoint (saved with saveRDS despite .qs extension)
checkpoint <- readRDS(checkpoint_file)

# Extract checkpoint data
result_mat <- checkpoint$result_mat
delivery_ids <- checkpoint$delivery_ids
match_ids <- checkpoint$match_ids
match_dates <- checkpoint$match_dates
batter_ids <- checkpoint$batter_ids
bowler_ids <- checkpoint$bowler_ids
venues <- checkpoint$venues
n_deliveries <- checkpoint$n_deliveries
current_format <- checkpoint$current_format
current_category <- checkpoint$current_category

cli::cli_alert_success("Loaded checkpoint for {current_category}")
cli::cli_alert_info("Deliveries: {format(n_deliveries, big.mark = ',')}")

# Build the result data.table from checkpoint (same as in main script)
phase_map <- c("powerplay", "middle", "death")

cli::cli_h2("Building result data.table from checkpoint")

result_dt <- data.table(
  delivery_id = delivery_ids,
  match_id = match_ids,
  match_date = match_dates,
  batter_id = batter_ids,
  bowler_id = bowler_ids,
  venue = venues,

  batter_run_elo_before = result_mat[, "batter_run_before"],
  batter_run_elo_after = result_mat[, "batter_run_after"],
  bowler_run_elo_before = result_mat[, "bowler_run_before"],
  bowler_run_elo_after = result_mat[, "bowler_run_after"],

  batter_wicket_elo_before = result_mat[, "batter_wicket_before"],
  batter_wicket_elo_after = result_mat[, "batter_wicket_after"],
  bowler_wicket_elo_before = result_mat[, "bowler_wicket_before"],
  bowler_wicket_elo_after = result_mat[, "bowler_wicket_after"],

  venue_perm_run_elo_before = result_mat[, "venue_perm_run_before"],
  venue_perm_run_elo_after = result_mat[, "venue_perm_run_after"],
  venue_perm_wicket_elo_before = result_mat[, "venue_perm_wicket_before"],
  venue_perm_wicket_elo_after = result_mat[, "venue_perm_wicket_after"],

  venue_session_run_elo_before = result_mat[, "venue_session_run_before"],
  venue_session_run_elo_after = result_mat[, "venue_session_run_after"],
  venue_session_wicket_elo_before = result_mat[, "venue_session_wicket_before"],
  venue_session_wicket_elo_after = result_mat[, "venue_session_wicket_after"],

  k_batter_run = result_mat[, "k_batter_run"],
  k_bowler_run = result_mat[, "k_bowler_run"],
  k_venue_perm_run = result_mat[, "k_venue_perm_run"],
  k_venue_session_run = result_mat[, "k_venue_session_run"],
  k_batter_wicket = result_mat[, "k_batter_wicket"],
  k_bowler_wicket = result_mat[, "k_bowler_wicket"],
  k_venue_perm_wicket = result_mat[, "k_venue_perm_wicket"],
  k_venue_session_wicket = result_mat[, "k_venue_session_wicket"],

  exp_runs = result_mat[, "exp_runs"],
  exp_wicket = result_mat[, "exp_wicket"],
  actual_runs = as.integer(result_mat[, "actual_runs"]),
  is_wicket = as.logical(result_mat[, "is_wicket_num"]),

  batter_balls = as.integer(result_mat[, "batter_balls"]),
  bowler_balls = as.integer(result_mat[, "bowler_balls"]),
  venue_balls = as.integer(result_mat[, "venue_balls"]),
  balls_in_match = as.integer(result_mat[, "balls_in_match"]),
  days_inactive_batter = as.integer(result_mat[, "days_inactive_batter"]),
  days_inactive_bowler = as.integer(result_mat[, "days_inactive_bowler"]),

  is_knockout = as.logical(result_mat[, "is_knockout_num"]),
  event_tier = as.integer(result_mat[, "event_tier"]),
  phase = phase_map[as.integer(result_mat[, "phase_num"])]
)

cli::cli_alert_success("Built data.table with {format(nrow(result_dt), big.mark = ',')} rows")

# Free memory from checkpoint objects
rm(result_mat, checkpoint)
gc(verbose = FALSE)

# Connect to database
conn <- get_db_connection(read_only = FALSE)

# Determine the correct table name (must match insert_3way_elos function)
# insert_3way_elos uses: paste0(tolower(format), "_3way_elo")
# where format = current_category = "mens_t20"
table_name <- paste0(tolower(current_category), "_3way_elo")
cli::cli_alert_info("Target table: {table_name}")

# Clear existing data and recreate table
cli::cli_alert_info("Clearing existing data from {table_name}...")
tryCatch({
  # Drop and recreate is faster and cleaner than DELETE for large tables
  DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", table_name))
  cli::cli_alert_success("Table dropped")
}, error = function(e) {
  cli::cli_alert_warning("Could not drop table: {e$message}")
})

# Create the table (required before insert)
cli::cli_alert_info("Creating table {table_name}...")
create_3way_elo_table(current_category, conn, overwrite = FALSE)
cli::cli_alert_success("Table created")

# Insert in small batches
n_batches <- ceiling(n_deliveries / BATCH_SIZE)
cli::cli_h2("Inserting {format(n_deliveries, big.mark = ',')} deliveries in {n_batches} batches")
cli::cli_alert_info("Batch size: {BATCH_SIZE}, GC every: {GC_EVERY_N_BATCHES} batches")

cli::cli_progress_bar("Inserting batches", total = n_batches)

for (b in seq_len(n_batches)) {
  start_idx <- (b - 1) * BATCH_SIZE + 1
  end_idx <- min(b * BATCH_SIZE, n_deliveries)

  batch_df <- as.data.frame(result_dt[start_idx:end_idx])
  insert_3way_elos(batch_df, current_category, conn)

  # Periodic garbage collection to prevent memory fragmentation
  if (b %% GC_EVERY_N_BATCHES == 0) {
    gc(verbose = FALSE)
  }

  cli::cli_progress_update()
}

cli::cli_progress_done()

# Verify insertion before claiming success
verify_count <- DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) as n FROM %s", table_name))$n
if (verify_count != n_deliveries) {
  cli::cli_abort("Insertion verification failed: expected {n_deliveries}, got {verify_count}")
}
cli::cli_alert_success("Inserted and verified {format(n_deliveries, big.mark = ',')} deliveries")

# Store parameters
cli::cli_h2("Storing parameters")

current_params <- build_3way_elo_params(current_format)
current_params$format <- current_category

last_row <- result_dt[.N]
store_3way_elo_params(
  current_params,
  last_delivery_id = last_row$delivery_id,
  last_match_date = last_row$match_date,
  total_deliveries = n_deliveries,
  conn = conn
)

cli::cli_alert_success("Parameters stored")

# Clean up checkpoint
file.remove(checkpoint_file)
cli::cli_alert_success("Checkpoint file cleaned up")

# Summary statistics
cli::cli_h2("Summary Statistics")

tryCatch({
  stats <- get_3way_elo_stats(current_category, conn)

  if (!is.null(stats)) {
    cli::cli_alert_info("Total records: {format(stats$total_records, big.mark = ',')}")
    cli::cli_alert_info("Unique batters: {stats$unique_batters}")
    cli::cli_alert_info("Unique bowlers: {stats$unique_bowlers}")
    cli::cli_alert_info("Unique venues: {stats$unique_venues}")
    cli::cli_alert_info("Date range: {stats$first_date} to {stats$last_date}")

    cat("\nMean ELOs:\n")
    cat(sprintf("  Batter Run ELO:        %.1f\n", stats$mean_batter_run_elo))
    cat(sprintf("  Bowler Run ELO:        %.1f\n", stats$mean_bowler_run_elo))
    cat(sprintf("  Venue Perm Run ELO:    %.1f\n", stats$mean_venue_perm_run_elo))
  }
}, error = function(e) {
  cli::cli_alert_warning("Could not retrieve summary stats: {e$message}")
})

# Disconnect
DBI::dbDisconnect(conn, shutdown = TRUE)

cli::cli_h1("Database Insertion Complete!")
cli::cli_alert_success("Men's T20 3-Way ELO data successfully saved to database")
