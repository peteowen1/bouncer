# 01 Calculate Venue Skill Indices ----
#
# This script calculates venue skill indices using residual-based updates.
# Venue skills represent "how much the venue deviates from context-expected outcomes".
#
# Supports: T20, ODI, Test formats (configurable via FORMAT_FILTER)
#
# KEY FEATURE: Uses AGNOSTIC model predictions as baseline expectation
#   - Agnostic model predicts expected runs/wicket based on CONTEXT ONLY
#   - NO player, team, or venue identity in agnostic model
#   - Venue skills = "deviation from what any average venue would produce in this context"
#
# Venue Skill Indices (Residual-Based):
#   - Run Rate Skill: How many more/fewer runs compared to context-expected
#   - Wicket Rate Skill: How many more/fewer wickets compared to context-expected
#   - Boundary Rate: EMA of boundary (4s + 6s) probability (raw, no baseline)
#   - Dot Rate: EMA of dot ball probability (raw, no baseline)
#
# Update Formula (EMA on Residuals for Run/Wicket):
#   residual = actual - agnostic_expected
#   new_skill = (1 - alpha) * old_skill + alpha * residual
#
# This converges to venue's true deviation from expected (bounded and stable).
#
# REQUIRES: Run 01_train_agnostic_model.R first to train agnostic models
#
# Key Difference from Player Skills:
#   - Lower alpha (0.002 vs 0.01) because venues change slower than players
#   - Venues normalized via venue_aliases table before processing
#   - Single entity (venue) per delivery vs two entities (batter + bowler)
#
# Output:
#   - {format}_venue_skill tables populated in DuckDB

# 1. Setup ----
library(DBI)
library(data.table)
library(xgboost)
if (!("bouncer" %in% loadedNamespaces())) devtools::load_all()

# 2. Configuration ----
FORMAT_GROUPS <- list(
  t20 = c("T20", "IT20"),
  odi = c("ODI", "ODM"),
  test = c("Test", "MDM")
)

FORMAT_FILTER <- NULL    # NULL = all formats, or "t20", "odi", "test" for single format
BATCH_SIZE <- 10000      # Deliveries per batch insert
MATCH_LIMIT <- NULL      # Set to integer to limit matches (for testing)
FORCE_FULL <- FALSE      # If TRUE, always recalculate everything
BUILD_ALIASES <- TRUE    # If TRUE, build default venue aliases on first run

# Determine formats to process
if (is.null(FORMAT_FILTER)) {
  formats_to_process <- names(FORMAT_GROUPS)
} else {
  formats_to_process <- FORMAT_FILTER
}

cat("\n")
cli::cli_h1("Venue Skill Index Calculation")
cli::cli_alert_info("Formats to process: {paste(toupper(formats_to_process), collapse = ', ')}")
cli::cli_alert_info("Uses AGNOSTIC model for context-only baseline")
cat("\n")

# 3. Load Agnostic Models ----
agnostic_models <- list()
cli::cli_h2("Loading agnostic outcome models")

for (fmt in formats_to_process) {
  tryCatch({
    agnostic_models[[fmt]] <- load_agnostic_model(fmt)
    cli::cli_alert_success("Loaded agnostic {toupper(fmt)} model")
  }, error = function(e) {
    cli::cli_alert_danger("Agnostic {toupper(fmt)} model not found: {e$message}")
    cli::cli_alert_info("Run 01_train_agnostic_model.R to train agnostic models first")
    stop("Missing agnostic model")
  })
}

# 4. Database Connection ----
cli::cli_h2("Connecting to database")

# Ensure any previous DuckDB connections are closed
tryCatch({
  duckdb::duckdb_shutdown(duckdb::duckdb())
}, error = function(e) {
  # Ignore errors - just ensuring clean state
})

# Use the default database path (where install_all_bouncer_data loads data)
conn <- get_db_connection(read_only = FALSE)

# Verify we have data
n_matches <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM cricsheet.matches")$n
if (n_matches == 0) {
  cli::cli_abort("Database is empty. Run install_all_bouncer_data() first.")
}
cli::cli_alert_success("Connected to database ({format(n_matches, big.mark = ',')} matches)")

# 4. Ensure Venue Tables Exist ----
cli::cli_h2("Checking venue tables")

existing_tables <- DBI::dbListTables(conn)

# Create venue_aliases if it doesn't exist
if (!"venue_aliases" %in% existing_tables) {
  cli::cli_alert_info("Creating venue_aliases table...")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS venue_aliases (
      alias VARCHAR PRIMARY KEY,
      canonical_venue VARCHAR NOT NULL,
      country VARCHAR,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  ")
  DBI::dbExecute(conn, "CREATE INDEX IF NOT EXISTS idx_venue_aliases_canonical ON venue_aliases(canonical_venue)")
}

# Create venue_skill_calculation_params if it doesn't exist
if (!"venue_skill_calculation_params" %in% existing_tables) {
  cli::cli_alert_info("Creating venue_skill_calculation_params table...")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS venue_skill_calculation_params (
      format VARCHAR PRIMARY KEY,
      alpha DOUBLE,
      start_run_rate DOUBLE,
      start_wicket_rate DOUBLE,
      start_boundary_rate DOUBLE,
      start_dot_rate DOUBLE,
      last_delivery_id VARCHAR,
      last_match_date DATE,
      total_deliveries INTEGER,
      calculated_at TIMESTAMP
    )
  ")
}

cli::cli_alert_success("Venue tables ready")

# 5. Build Venue Aliases ----
if (BUILD_ALIASES) {
  cli::cli_h2("Setting up venue aliases")

  alias_count <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM venue_aliases")$n
  if (alias_count == 0) {
    build_default_venue_aliases(conn)
  } else {
    cli::cli_alert_success("Venue aliases already populated ({alias_count} aliases)")
  }
}

# 6. Process Each Format ----
for (current_format in formats_to_process) {

cat("\n")
cli::cli_h1("{toupper(current_format)} Venue Skill Index Calculation")
cat("\n")

# Build format-specific values
format_match_types <- FORMAT_GROUPS[[current_format]]
match_type_filter <- paste(sprintf("'%s'", tolower(format_match_types)), collapse = ", ")

# Get format-specific parameters
ALPHA <- get_venue_alpha(current_format)
START_VALUES <- get_venue_start_values(current_format)
START_RUN_RATE <- START_VALUES$run_rate
START_WICKET_RATE <- START_VALUES$wicket_rate
START_BOUNDARY_RATE <- START_VALUES$boundary_rate
START_DOT_RATE <- START_VALUES$dot_rate
MIN_BALLS <- get_venue_min_balls(current_format)

cli::cli_alert_info("Alpha (learning rate): {ALPHA}")
cli::cli_alert_info("Starting run rate: {START_RUN_RATE}")
cli::cli_alert_info("Starting wicket rate: {START_WICKET_RATE}")
cli::cli_alert_info("Starting boundary rate: {START_BOUNDARY_RATE}")
cli::cli_alert_info("Starting dot rate: {START_DOT_RATE}")
cli::cli_alert_info("Min balls for reliable: {MIN_BALLS}")

## 6.1 Check for existing data ----
cli::cli_h2("Checking existing data")

table_name <- paste0(current_format, "_venue_skill")
existing_tables <- DBI::dbListTables(conn)

if (!table_name %in% existing_tables || FORCE_FULL) {
  mode <- "full"
  if (FORCE_FULL) {
    cli::cli_alert_info("FORCE_FULL = TRUE, running full calculation")
  } else {
    cli::cli_alert_info("Table does not exist, running full calculation")
  }
  create_format_venue_skill_table(current_format, conn, overwrite = TRUE)
} else {
  # Check if we can do incremental
  last_processed <- get_last_processed_venue_skill_delivery(current_format, conn)
  if (is.null(last_processed)) {
    mode <- "full"
    cli::cli_alert_info("No existing records, running full calculation")
    create_format_venue_skill_table(current_format, conn, overwrite = TRUE)
  } else {
    mode <- "incremental"
    cli::cli_alert_success("Found existing data, running incremental update")
    cli::cli_alert_info("Last processed: {last_processed$match_date}")
  }
}

## 6.2 Load Deliveries ----
cli::cli_h2("Loading {toupper(current_format)} deliveries")

# Build query for deliveries with features needed for agnostic model
base_query <- sprintf("
  WITH innings_totals AS (
    SELECT
      match_id,
      innings,
      batting_team,
      MAX(total_runs) AS innings_total
    FROM cricsheet.deliveries
    WHERE LOWER(match_type) IN (%s)
    GROUP BY match_id, innings, batting_team
  ),
  cumulative_scores AS (
    SELECT
      d.*,
      d.total_runs AS batting_score,
      COALESCE(
        (SELECT SUM(it.innings_total)
         FROM innings_totals it
         WHERE it.match_id = d.match_id
           AND it.batting_team = d.bowling_team
           AND it.innings < d.innings),
        0
      ) AS bowling_score
    FROM cricsheet.deliveries d
    WHERE LOWER(d.match_type) IN (%s)
  ),
  match_context AS (
    SELECT DISTINCT
      m.match_id,
      CASE
        WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%final%%' THEN 1
        WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%qualifier%%' THEN 1
        WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%eliminator%%' THEN 1
        WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%playoff%%' THEN 1
        WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%semi%%' THEN 1
        ELSE 0
      END AS is_knockout,
      CASE
        WHEN LOWER(m.event_name) LIKE '%%world cup%%' THEN 1
        WHEN LOWER(m.event_name) LIKE '%%ipl%%' OR LOWER(m.event_name) LIKE '%%indian premier%%' THEN 1
        WHEN LOWER(m.event_name) LIKE '%%big bash%%' OR LOWER(m.event_name) LIKE '%%bbl%%' THEN 2
        WHEN LOWER(m.event_name) LIKE '%%psl%%' OR LOWER(m.event_name) LIKE '%%super league%%' THEN 2
        WHEN LOWER(m.event_name) LIKE '%%cpl%%' OR LOWER(m.event_name) LIKE '%%caribbean%%' THEN 2
        WHEN LOWER(m.match_type) IN ('test', 'odi', 't20i', 'it20') THEN 1
        ELSE 3
      END AS event_tier
    FROM cricsheet.matches m
  )
  SELECT
    cs.delivery_id,
    cs.match_id,
    cs.match_date,
    cs.venue,
    cs.runs_batter,
    cs.is_wicket,
    cs.innings,
    cs.over,
    cs.ball,
    cs.over_ball,
    -- FIX: wickets_fallen in Cricsheet is AFTER the delivery, so subtract is_wicket
    -- to get the count BEFORE this delivery (prevents data leakage)
    (cs.wickets_fallen - CAST(cs.is_wicket AS INT)) AS wickets_fallen,
    cs.gender,
    (cs.batting_score - cs.bowling_score) AS runs_difference,
    COALESCE(mc.is_knockout, 0) AS is_knockout,
    COALESCE(mc.event_tier, 3) AS event_tier,
    COALESCE(cs.is_four, FALSE) as is_four,
    COALESCE(cs.is_six, FALSE) as is_six
  FROM cumulative_scores cs
  LEFT JOIN match_context mc ON cs.match_id = mc.match_id
  WHERE 1=1
", match_type_filter, match_type_filter)

# Add date filter for incremental mode
if (mode == "incremental" && !is.null(last_processed)) {
  base_query <- paste0(base_query, sprintf("
    AND cs.match_date >= '%s'
  ", last_processed$match_date))
}

# Add match limit for testing
if (!is.null(MATCH_LIMIT)) {
  base_query <- paste0(base_query, sprintf("
    AND cs.match_id IN (
      SELECT DISTINCT match_id FROM cricsheet.deliveries
      WHERE LOWER(match_type) IN (%s)
      ORDER BY match_date
      LIMIT %d
    )
  ", match_type_filter, MATCH_LIMIT))
  cli::cli_alert_warning("MATCH_LIMIT = {MATCH_LIMIT}, processing limited matches only")
}

# Order by date and delivery_id for correct chronological processing
base_query <- paste0(base_query, "
  ORDER BY cs.match_date, cs.match_id, cs.delivery_id
")

cli::cli_alert_info("Querying deliveries...")
deliveries <- data.table::as.data.table(DBI::dbGetQuery(conn, base_query))

n_deliveries <- nrow(deliveries)
cli::cli_alert_success("Loaded {format(n_deliveries, big.mark = ',')} deliveries")

if (n_deliveries == 0) {
  cli::cli_alert_warning("No deliveries to process for {toupper(current_format)}")
  next
}

# Show date range
date_range <- deliveries[, .(min_date = min(match_date), max_date = max(match_date))]
cli::cli_alert_info("Date range: {date_range$min_date} to {date_range$max_date}")

## 6.3 Derive outcome columns ----
cli::cli_h2("Deriving outcome columns")

deliveries[, is_boundary := is_four | is_six]
deliveries[, is_dot := runs_batter == 0 & !is_wicket]

cli::cli_alert_info("Boundaries: {format(sum(deliveries$is_boundary), big.mark = ',')} ({round(mean(deliveries$is_boundary) * 100, 1)}%)")
cli::cli_alert_info("Dots: {format(sum(deliveries$is_dot), big.mark = ',')} ({round(mean(deliveries$is_dot) * 100, 1)}%)")
cli::cli_alert_info("Wickets: {format(sum(deliveries$is_wicket), big.mark = ',')} ({round(mean(deliveries$is_wicket) * 100, 1)}%)")

## 6.4 Normalize venue names ----
cli::cli_h2("Normalizing venue names")

unique_venues_before <- length(unique(deliveries$venue))

# Normalize venues using the aliases table
deliveries$venue_canonical <- normalize_venues(deliveries$venue, conn)

unique_venues_after <- length(unique(deliveries$venue_canonical))
cli::cli_alert_success("Venues: {unique_venues_before} raw -> {unique_venues_after} canonical")

# Use canonical venue for processing
venues_vec <- deliveries$venue_canonical

## 6.4.5 Generate agnostic model predictions ----
cli::cli_h2("Generating agnostic model predictions")
cli::cli_alert_info("Using CONTEXT-ONLY baseline (no player/team/venue identity)")

agnostic_model <- agnostic_models[[current_format]]
delivery_df <- as.data.frame(deliveries)

probs <- predict_agnostic_outcome(agnostic_model, delivery_df, current_format)
agnostic_exp_runs <- get_agnostic_expected_runs(probs)
agnostic_exp_wicket <- get_agnostic_expected_wicket(probs)

cli::cli_alert_success("Generated agnostic predictions for {n_deliveries} deliveries")
cli::cli_alert_info("Mean expected runs (agnostic): {round(mean(agnostic_exp_runs), 3)}")
cli::cli_alert_info("Mean expected wicket (agnostic): {round(mean(agnostic_exp_wicket) * 100, 2)}%")

rm(probs, delivery_df)  # Free memory

## 6.5 Initialize Venue Skill State ----
cli::cli_h2("Initializing venue skill state")

all_venues <- unique(venues_vec)
cli::cli_alert_info("{length(all_venues)} unique venues in data")

if (mode == "incremental") {
  cli::cli_alert_info("Loading existing venue skill state...")
  venue_state <- get_all_venue_skill_state(current_format, conn)

  if (!is.null(venue_state)) {
    venue_run_rate <- venue_state$run_rate
    venue_wicket_rate <- venue_state$wicket_rate
    venue_boundary_rate <- venue_state$boundary_rate
    venue_dot_rate <- venue_state$dot_rate
    venue_balls <- venue_state$balls

    # Initialize new venues
    new_venues <- 0
    for (v in all_venues) {
      if (!exists(v, envir = venue_run_rate)) {
        venue_run_rate[[v]] <- START_RUN_RATE
        venue_wicket_rate[[v]] <- START_WICKET_RATE
        venue_boundary_rate[[v]] <- START_BOUNDARY_RATE
        venue_dot_rate[[v]] <- START_DOT_RATE
        venue_balls[[v]] <- 0L
        new_venues <- new_venues + 1
      }
    }

    cli::cli_alert_success("Loaded state, initialized {new_venues} new venues")
  } else {
    mode <- "full"
    cli::cli_alert_warning("Could not load state, falling back to full calculation")
  }
}

if (mode == "full") {
  # Fresh state
  venue_run_rate <- new.env(hash = TRUE)
  venue_wicket_rate <- new.env(hash = TRUE)
  venue_boundary_rate <- new.env(hash = TRUE)
  venue_dot_rate <- new.env(hash = TRUE)
  venue_balls <- new.env(hash = TRUE)

  for (v in all_venues) {
    venue_run_rate[[v]] <- START_RUN_RATE
    venue_wicket_rate[[v]] <- START_WICKET_RATE
    venue_boundary_rate[[v]] <- START_BOUNDARY_RATE
    venue_dot_rate[[v]] <- START_DOT_RATE
    venue_balls[[v]] <- 0L
  }

  cli::cli_alert_success("Initialized {length(all_venues)} venues with starting values")
}

## 6.6 Pre-extract columns ----
delivery_ids <- deliveries$delivery_id
match_ids <- deliveries$match_id
match_dates <- deliveries$match_date
runs_vec <- deliveries$runs_batter
wicket_vec <- deliveries$is_wicket
boundary_vec <- deliveries$is_boundary
dot_vec <- deliveries$is_dot

## 6.7 Process deliveries ----
cli::cli_h2("Processing deliveries")
cli::cli_alert_info("Using RESIDUAL-BASED updates for run/wicket rates")

# Result matrix: 11 columns for venue data (added agnostic expectations)
result_mat <- matrix(NA_real_, nrow = n_deliveries, ncol = 11)
colnames(result_mat) <- c(
  "venue_run_rate", "venue_wicket_rate",
  "venue_boundary_rate", "venue_dot_rate",
  "venue_balls",
  "exp_runs_agnostic", "exp_wicket_agnostic",
  "actual_runs", "is_wicket",
  "is_boundary", "is_dot"
)

cli::cli_progress_bar("Calculating venue skill indices", total = n_deliveries)
update_freq <- max(1000, n_deliveries %/% 50)

for (i in seq_len(n_deliveries)) {
  venue <- venues_vec[i]
  runs <- runs_vec[i]
  is_wicket <- wicket_vec[i]
  is_boundary <- boundary_vec[i]
  is_dot <- dot_vec[i]

  # Get agnostic model expectations
  exp_runs <- agnostic_exp_runs[i]
  exp_wicket <- agnostic_exp_wicket[i]

  # Get current skill indices (BEFORE this delivery)
  run_rate <- venue_run_rate[[venue]]
  wicket_rate <- venue_wicket_rate[[venue]]
  boundary_rate <- venue_boundary_rate[[venue]]
  dot_rate <- venue_dot_rate[[venue]]
  balls <- venue_balls[[venue]]

  # Store current state (BEFORE update)
  result_mat[i, ] <- c(
    run_rate, wicket_rate,
    boundary_rate, dot_rate,
    balls,
    exp_runs, exp_wicket,
    runs, is_wicket,
    is_boundary, is_dot
  )

  # Update skill indices using PROPER EMA formula
  # Formula: new = (1 - alpha) * old + alpha * residual
  # Converges to venue's true deviation from expected (bounded and stable)

  # Run rate: residual-based EMA
  runs_residual <- runs - exp_runs
  new_run_rate <- (1 - ALPHA) * run_rate + ALPHA * runs_residual
  venue_run_rate[[venue]] <- new_run_rate

  # Wicket rate: residual-based EMA
  wicket_obs <- if (is_wicket) 1 else 0
  wicket_residual <- wicket_obs - exp_wicket
  new_wicket_rate <- (1 - ALPHA) * wicket_rate + ALPHA * wicket_residual
  venue_wicket_rate[[venue]] <- new_wicket_rate

  # Boundary rate: simple EMA (no agnostic baseline for boundaries)
  boundary_obs <- if (is_boundary) 1 else 0
  new_boundary_rate <- update_skill_index(boundary_rate, boundary_obs, ALPHA)
  venue_boundary_rate[[venue]] <- new_boundary_rate

  # Dot rate: simple EMA (no agnostic baseline for dots)
  dot_obs <- if (is_dot) 1 else 0
  new_dot_rate <- update_skill_index(dot_rate, dot_obs, ALPHA)
  venue_dot_rate[[venue]] <- new_dot_rate

  # Increment ball count
  venue_balls[[venue]] <- balls + 1L

  if (i %% update_freq == 0) {
    cli::cli_progress_update()
  }
}

cli::cli_progress_done()
cli::cli_alert_success("Processed {format(n_deliveries, big.mark = ',')} deliveries")

## 6.8 Build result data.table ----
cli::cli_h2("Building result dataset")

result_dt <- data.table::data.table(
  delivery_id = delivery_ids,
  match_id = match_ids,
  match_date = match_dates,
  venue = venues_vec,
  venue_run_rate = result_mat[, "venue_run_rate"],
  venue_wicket_rate = result_mat[, "venue_wicket_rate"],
  venue_boundary_rate = result_mat[, "venue_boundary_rate"],
  venue_dot_rate = result_mat[, "venue_dot_rate"],
  venue_balls = as.integer(result_mat[, "venue_balls"]),
  actual_runs = as.integer(result_mat[, "actual_runs"]),
  is_wicket = as.logical(result_mat[, "is_wicket"]),
  is_boundary = as.logical(result_mat[, "is_boundary"]),
  is_dot = as.logical(result_mat[, "is_dot"])
)

cli::cli_alert_success("Built result dataset with {ncol(result_dt)} columns")

## 6.9 Batch insert ----
cli::cli_h2("Inserting into database")

# For incremental mode, delete records from the overlap date to avoid duplicates
if (mode == "incremental" && !is.null(last_processed)) {
  overlap_date <- last_processed$match_date
  deleted <- DBI::dbExecute(conn, sprintf(
    "DELETE FROM %s WHERE match_date = '%s'",
    table_name, overlap_date
  ))
  cli::cli_alert_info("Deleted {format(deleted, big.mark = ',')} records from {overlap_date} (will be re-inserted)")
}

n_batches <- ceiling(n_deliveries / BATCH_SIZE)
cli::cli_alert_info("Inserting {format(n_deliveries, big.mark = ',')} records in {n_batches} batches")

# Wrap inserts in a transaction for consistency
DBI::dbBegin(conn)
tryCatch({
  for (b in seq_len(n_batches)) {
    start_idx <- (b - 1) * BATCH_SIZE + 1
    end_idx <- min(b * BATCH_SIZE, n_deliveries)

    batch_df <- as.data.frame(result_dt[start_idx:end_idx])
    insert_format_venue_skills(batch_df, current_format, conn)

    if (b %% 10 == 0 || b == n_batches) {
      cli::cli_alert_info("Inserted batch {b}/{n_batches}")
    }
  }
  DBI::dbCommit(conn)
  cli::cli_alert_success("Inserted all records into {table_name}")
}, error = function(e) {
  DBI::dbRollback(conn)
  cli::cli_abort("Insert failed, transaction rolled back: {e$message}")
})

## 6.10 Store calculation parameters ----
cli::cli_h2("Storing calculation parameters")

# Get actual total count from table (cumulative, not just this run)
total_in_table <- DBI::dbGetQuery(conn, sprintf(
  "SELECT COUNT(*) as n FROM %s", table_name
))$n

DBI::dbExecute(conn, sprintf("
  DELETE FROM venue_skill_calculation_params WHERE format = '%s'
", current_format))

DBI::dbExecute(conn, sprintf("
  INSERT INTO venue_skill_calculation_params
    (format, alpha, start_run_rate, start_wicket_rate, start_boundary_rate, start_dot_rate,
     last_delivery_id, last_match_date, total_deliveries, calculated_at)
  VALUES ('%s', %f, %f, %f, %f, %f, '%s', '%s', %d, CURRENT_TIMESTAMP)
", current_format, ALPHA, START_RUN_RATE, START_WICKET_RATE, START_BOUNDARY_RATE, START_DOT_RATE,
   tail(delivery_ids, 1), tail(match_dates, 1), total_in_table))

cli::cli_alert_success("Stored calculation parameters")

## 6.11 Summary Statistics ----
cli::cli_h2("{toupper(current_format)} Summary Statistics")

stats <- get_format_venue_skill_stats(current_format, conn)
if (!is.null(stats)) {
  cli::cli_alert_info("Total records: {format(stats$total_records, big.mark = ',')}")
  cli::cli_alert_info("Unique venues: {stats$unique_venues}")
  cli::cli_alert_info("Date range: {stats$first_date} to {stats$last_date}")
  cat("\n")
  cli::cli_alert_info("Mean venue run rate: {round(stats$mean_run_rate, 3)}")
  cli::cli_alert_info("Mean venue wicket rate: {round(stats$mean_wicket_rate * 100, 2)}%")
  cli::cli_alert_info("Mean venue boundary rate: {round(stats$mean_boundary_rate * 100, 1)}%")
  cli::cli_alert_info("Mean venue dot rate: {round(stats$mean_dot_rate * 100, 1)}%")
  cat("\n")
  cli::cli_alert_info("Actual mean runs: {round(stats$mean_actual_runs, 3)}")
  cli::cli_alert_info("Actual wicket rate: {round(stats$actual_wicket_rate * 100, 2)}%")
  cli::cli_alert_info("Actual boundary rate: {round(stats$actual_boundary_rate * 100, 1)}%")
  cli::cli_alert_info("Actual dot rate: {round(stats$actual_dot_rate * 100, 1)}%")
}

# Show top and bottom venues
cli::cli_h3("Top 5 Run-Scoring Venues")
top_venues <- get_venue_rankings(current_format, "run_rate", conn, limit = 5)
if (!is.null(top_venues) && nrow(top_venues) > 0) {
  for (i in seq_len(nrow(top_venues))) {
    cli::cli_alert_info("{i}. {top_venues$venue[i]}: {round(top_venues$run_rate[i], 2)} runs/ball ({top_venues$balls[i]} balls)")
  }
}

cli::cli_h3("Top 5 Highest Dot Rate Venues")
low_venues <- get_venue_rankings(current_format, "dot_rate", conn, limit = 5)
if (!is.null(low_venues) && nrow(low_venues) > 0) {
  for (i in seq_len(nrow(low_venues))) {
    cli::cli_alert_info("{i}. {low_venues$venue[i]}: {round(low_venues$dot_rate[i] * 100, 1)}% dots ({low_venues$balls[i]} balls)")
  }
}

# Clean up
rm(deliveries, result_dt, result_mat,
   venue_run_rate, venue_wicket_rate, venue_boundary_rate, venue_dot_rate, venue_balls)
gc()

}  # End format loop

# 7. Final Summary ----
cat("\n")
cli::cli_h1("Venue Skill Index Calculation Complete")
cli::cli_alert_success("Processed formats: {paste(toupper(formats_to_process), collapse = ', ')}")

# Show all format stats
for (fmt in formats_to_process) {
  stats <- get_format_venue_skill_stats(fmt, conn)
  if (!is.null(stats)) {
    cli::cli_alert_info("{toupper(fmt)}: {format(stats$total_records, big.mark = ',')} records, {stats$unique_venues} venues")
  }
}

cat("\n")
cli::cli_h3("Venue Skill Index Interpretation (Residual-Based)")
cli::cli_bullets(c(
  "i" = "Run/wicket rates now use RESIDUAL-BASED updates (vs agnostic baseline)",
  "i" = "Positive run_rate = venue produces MORE runs than context-expected",
  "i" = "Positive wicket_rate = venue produces MORE wickets than context-expected",
  "i" = "Boundary/dot rates remain raw EMAs (no agnostic baseline)"
))

cat("\n")
cli::cli_h3("Pipeline Dependencies")
cli::cli_bullets(c(
  "!" = "REQUIRES: Run 01_train_agnostic_model.R first to train agnostic models",
  "i" = "Run devtools::document() to update NAMESPACE",
  "i" = "Use add_venue_skill_features() to add venue indices to model data"
))
