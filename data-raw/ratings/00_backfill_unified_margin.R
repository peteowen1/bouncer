# Backfill Unified Margin
#
# Stores a projected run margin for every match missing one (pipeline step
# 1b). The rule lives in compute_projected_margins() (R/projected_margin.R,
# D-P72): runs wins use the official runs margin; wickets wins add the runs
# the chasing side would still have made, measured from real innings.
#
# Usage: Run this script after adding the unified_margin column to the database
# RECOMPUTE=true recalculates every stored margin, not only missing ones
# (needed once after the 2026-10-13 switch to projected run margins).

# 1. Setup ----

library(DBI)
library(dplyr)
if (!("bouncer" %in% loadedNamespaces())) devtools::load_all()

cat("\n")
cli::cli_h1("Backfill Unified Margin")
cat("\n")


# 2. Database Connection ----

cli::cli_h2("Connecting to database")

# Force shutdown any lingering DuckDB driver instances to avoid lock conflicts
# DuckDB caches driver instances which can cause issues on Windows
tryCatch({
  duckdb::duckdb_shutdown(duckdb::duckdb())
}, error = function(e) NULL)

conn <- get_db_connection(read_only = FALSE)

# Verify connection is valid
if (!DBI::dbIsValid(conn)) {
  cli::cli_alert_danger("Connection is not valid immediately after creation!")
  stop("Failed to establish valid database connection")
}

cli::cli_alert_success("Connected to database")


# 3. Add Column if Needed ----

cli::cli_h2("Checking database schema")

# Add unified_margin column to matches table if it doesn't exist
# (Inlined from add_margin_columns to avoid connection-passing issues)
matches_cols <- DBI::dbGetQuery(conn, "SELECT column_name FROM information_schema.columns WHERE table_schema = 'cricsheet' AND table_name = 'matches'")$column_name

if (!"unified_margin" %in% matches_cols) {
  cli::cli_alert_info("Adding unified_margin column to matches table...")
  DBI::dbExecute(conn, "ALTER TABLE cricsheet.matches ADD COLUMN unified_margin DOUBLE")
  cli::cli_alert_success("Added unified_margin column to matches")
} else {
  cli::cli_alert_info("unified_margin column already exists in matches")
}

# Check pre_match_features table too
pmf_cols <- tryCatch(
  DBI::dbGetQuery(conn, "SELECT column_name FROM information_schema.columns WHERE table_name = 'pre_match_features'")$column_name,
  error = function(e) character(0)
)

if (length(pmf_cols) > 0 && !"expected_margin" %in% pmf_cols) {
  cli::cli_alert_info("Adding expected_margin column to pre_match_features table...")
  DBI::dbExecute(conn, "ALTER TABLE pre_match_features ADD COLUMN expected_margin DOUBLE")
  cli::cli_alert_success("Added expected_margin column to pre_match_features")
}


# 4. Load stored margins ----

cli::cli_h2("Loading matches")
matches <- data.table::as.data.table(DBI::dbGetQuery(conn,
  "SELECT match_id, match_type, unified_margin FROM cricsheet.matches"))
cli::cli_alert_success("Loaded {nrow(matches)} matches")

RECOMPUTE <- identical(tolower(Sys.getenv("RECOMPUTE", "false")), "true")
if (RECOMPUTE) {
  cli::cli_alert_warning("RECOMPUTE=true: recalculating all {sum(!is.na(matches$unified_margin))} stored margins")
  matches$unified_margin <- NA_real_
}
cli::cli_alert_info("{sum(!is.na(matches$unified_margin))} matches already have unified_margin")


# 5. Projected run margin (D-P72) ----

cli::cli_h2("Calculating projected run margins")
need <- matches$match_id[is.na(matches$unified_margin)]
pm <- if (length(need)) compute_projected_margins(conn, match_ids = need) else
  data.table::data.table(match_id = character(), unified_margin = numeric(), margin_method = character())
matches <- merge(matches, pm[, .(match_id, calculated_margin = unified_margin, margin_method)],
                 by = "match_id", all.x = TRUE)
matches[!is.na(unified_margin), calculated_margin := unified_margin]
stopifnot(nrow(matches) == length(unique(matches$match_id)))


# 6. Summary Statistics ----

cli::cli_h2("Summary")
cli::cli_alert_success("Calculated margin for {sum(!is.na(pm$unified_margin))} of {length(need)} matches")
print(pm[, .N, by = margin_method][order(-N)])
print(matches[!is.na(calculated_margin) & calculated_margin != 0,
              .(n = .N, mean_abs = round(mean(abs(calculated_margin)), 1),
                median_abs = round(stats::median(abs(calculated_margin)), 1)), by = match_type][order(-n)])


# 7. Update Database ----

cli::cli_h2("Updating database")

# Filter to matches that need updating (have calculated margin but don't have stored margin)
# On RECOMPUTE every row is written, NA included: keeping an old-rule margin
# where the new rule has none would mix two rules in one column.
to_update <- as.data.frame(matches[if (RECOMPUTE) TRUE else is.na(unified_margin) & !is.na(calculated_margin),
                                   .(match_id, calculated_margin)])

if (nrow(to_update) > 0) {
  cli::cli_alert_info("Updating {nrow(to_update)} matches...")

  # Update in batches
  batch_size <- 500
  n_batches <- ceiling(nrow(to_update) / batch_size)

  cli::cli_progress_bar("Updating database", total = n_batches)

  for (b in seq_len(n_batches)) {
    start_idx <- (b - 1) * batch_size + 1
    end_idx <- min(b * batch_size, nrow(to_update))
    batch <- to_update[start_idx:end_idx, ]

    # Use temporary table for efficient batch update
    DBI::dbWriteTable(conn, "temp_margins", batch, temporary = TRUE, overwrite = TRUE)

    DBI::dbExecute(conn, "
      UPDATE cricsheet.matches
      SET unified_margin = (
        SELECT calculated_margin
        FROM temp_margins
        WHERE temp_margins.match_id = cricsheet.matches.match_id
      )
      WHERE match_id IN (SELECT match_id FROM temp_margins)
    ")

    cli::cli_progress_update()
  }

  cli::cli_progress_done()
  cli::cli_alert_success("Updated {nrow(to_update)} matches with unified_margin")

} else {
  cli::cli_alert_info("No matches need updating")
}


# 8. Verification ----

cli::cli_h2("Verification")

verify_query <- "
  SELECT
    match_type,
    COUNT(*) as total,
    COUNT(unified_margin) as with_margin,
    ROUND(AVG(ABS(unified_margin)), 1) as mean_abs_margin
  FROM cricsheet.matches
  GROUP BY match_type
  ORDER BY total DESC
"

verify_result <- DBI::dbGetQuery(conn, verify_query)
print(verify_result)

cat("\n")
cli::cli_alert_success("Unified margin backfill complete!")

# Clean up connection
DBI::dbDisconnect(conn, shutdown = TRUE)
cat("\n")
