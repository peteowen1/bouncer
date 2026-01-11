# Backfill Unified Margin for Historical Matches
#
# This script calculates and stores the unified margin for all matches
# in the database. Unified margin converts wickets-wins to runs-equivalent
# using DLS-style resource projection, enabling consistent comparison
# across all match outcomes.
#
# This only needs to be run once after the margin_calculation module is added.
# New matches will have their margin calculated during data loading.
#
# Usage: Run this script after adding the unified_margin column to the database

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
matches_cols <- DBI::dbGetQuery(conn, "SELECT column_name FROM information_schema.columns WHERE table_name = 'matches'")$column_name

if (!"unified_margin" %in% matches_cols) {
  cli::cli_alert_info("Adding unified_margin column to matches table...")
  DBI::dbExecute(conn, "ALTER TABLE matches ADD COLUMN unified_margin DOUBLE")
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


# 4. Load Match Data ----

cli::cli_h2("Loading matches")

query <- "
  SELECT
    m.match_id,
    m.match_type,
    m.outcome_type,
    m.outcome_by_runs,
    m.outcome_by_wickets,
    m.overs_per_innings,
    m.unified_margin,
    -- Get innings totals
    inn1.total_runs AS team1_score,
    inn1.total_overs AS team1_overs,
    inn2.total_runs AS team2_score,
    inn2.total_overs AS team2_overs
  FROM matches m
  LEFT JOIN match_innings inn1
    ON m.match_id = inn1.match_id AND inn1.innings = 1
  LEFT JOIN match_innings inn2
    ON m.match_id = inn2.match_id AND inn2.innings = 2
  WHERE m.outcome_type IS NOT NULL
  ORDER BY m.match_date
"

matches <- DBI::dbGetQuery(conn, query)
cli::cli_alert_success("Loaded {nrow(matches)} matches")

# Check how many already have margin
n_existing <- sum(!is.na(matches$unified_margin))
cli::cli_alert_info("{n_existing} matches already have unified_margin")


# 5. Calculate Unified Margin ----

cli::cli_h2("Calculating unified margins")

# Define format mapping
format_map <- c(
  "T20" = "t20", "IT20" = "t20",
  "ODI" = "odi", "ODM" = "odi",
  "Test" = "test", "MDM" = "test"
)

matches <- matches %>%
  mutate(
    format = format_map[match_type],
    # Determine win type
    win_type = case_when(
      !is.na(outcome_by_runs) & outcome_by_runs > 0 ~ "runs",
      !is.na(outcome_by_wickets) & outcome_by_wickets > 0 ~ "wickets",
      outcome_type %in% c("tie", "draw", "no result", "no_result") ~ outcome_type,
      TRUE ~ "unknown"
    ),
    # Calculate overs remaining for wickets wins (cricket notation)
    overs_remaining = case_when(
      win_type == "wickets" & !is.na(overs_per_innings) & !is.na(team2_overs) ~
        pmax(0, overs_per_innings - team2_overs),
      TRUE ~ 0
    ),
    # Use wickets from outcome (10 - wickets lost = wickets remaining)
    wickets_remaining = ifelse(is.na(outcome_by_wickets), 0L, as.integer(outcome_by_wickets))
  )

# Calculate margin for each match (only those missing it)
cli::cli_progress_bar("Calculating margins", total = nrow(matches))

calculated_margins <- numeric(nrow(matches))

for (i in seq_len(nrow(matches))) {
  m <- matches[i, ]

  # Skip if already has margin
  if (!is.na(m$unified_margin)) {
    calculated_margins[i] <- m$unified_margin
    cli::cli_progress_update()
    next
  }

  # Skip if missing data
  if (is.na(m$team1_score) || is.na(m$team2_score) ||
      is.na(m$format) || m$win_type == "unknown") {
    calculated_margins[i] <- NA_real_
    cli::cli_progress_update()
    next
  }

  # Calculate margin
  calculated_margins[i] <- calculate_unified_margin(
    team1_score = m$team1_score,
    team2_score = m$team2_score,
    wickets_remaining = m$wickets_remaining,
    overs_remaining = m$overs_remaining,
    win_type = m$win_type,
    format = m$format
  )

  cli::cli_progress_update()
}

cli::cli_progress_done()

matches$calculated_margin <- calculated_margins


# 6. Summary Statistics ----

cli::cli_h2("Summary")

n_calculated <- sum(!is.na(matches$calculated_margin))
n_runs_wins <- sum(matches$win_type == "runs", na.rm = TRUE)
n_wickets_wins <- sum(matches$win_type == "wickets", na.rm = TRUE)
n_draws <- sum(matches$win_type %in% c("tie", "draw"), na.rm = TRUE)

cli::cli_alert_success("Calculated margin for {n_calculated} matches")
cli::cli_alert_info("  - Runs wins: {n_runs_wins}")
cli::cli_alert_info("  - Wickets wins: {n_wickets_wins}")
cli::cli_alert_info("  - Ties/Draws: {n_draws}")

# Distribution by format
for (fmt in unique(matches$format[!is.na(matches$format)])) {
  fmt_matches <- matches[matches$format == fmt & !is.na(matches$calculated_margin), ]
  if (nrow(fmt_matches) > 0) {
    mean_margin <- mean(abs(fmt_matches$calculated_margin))
    median_margin <- median(abs(fmt_matches$calculated_margin))
    cli::cli_alert_info("  {toupper(fmt)}: Mean={round(mean_margin, 1)}, Median={round(median_margin, 1)}")
  }
}


# 7. Update Database ----

cli::cli_h2("Updating database")

# Filter to matches that need updating (have calculated margin but don't have stored margin)
to_update <- matches %>%
  filter(is.na(unified_margin) & !is.na(calculated_margin)) %>%
  select(match_id, calculated_margin)

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
      UPDATE matches
      SET unified_margin = (
        SELECT calculated_margin
        FROM temp_margins
        WHERE temp_margins.match_id = matches.match_id
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
  FROM matches
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
