# Venue Skill Index Functions
#
# Exponential Moving Average (EMA) based venue characteristic tracking.
# This approach:
#   - Is drift-proof by design (indices are actual averages)
#   - Handles continuous outcomes (runs) naturally
#   - Handles rare events (wickets) without imbalance
#   - Is directly interpretable (e.g., run rate of 1.5 = 1.5 runs/ball average at venue)
#
# Tracks four metrics per delivery:
#   - Run rate: EMA of runs per ball at venue
#   - Wicket rate: EMA of wicket probability per ball
#   - Boundary rate: EMA of boundary (4s + 6s) probability per ball
#   - Dot rate: EMA of dot ball probability per ball


#' Get Venue Alpha for Format
#'
#' Returns the EMA alpha (learning rate) for a given format.
#' Venue alphas are lower than player alphas because venues change slower.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#'
#' @return Numeric. Alpha value for EMA
#' @keywords internal
get_venue_alpha <- function(format = "t20") {
  fmt <- normalize_format(format)
  switch(fmt,
    "t20" = VENUE_ALPHA_T20,
    "odi" = VENUE_ALPHA_ODI,
    "test" = VENUE_ALPHA_TEST,
    VENUE_ALPHA_T20
  )
}


#' Get Starting Venue Skill Values for Format
#'
#' Returns starting venue skill index values for a format.
#'
#' For residual-based indices (run_rate, wicket_rate), starts at 0
#' (no deviation from context-expected).
#' For raw EMA indices (boundary_rate, dot_rate), uses calibrated averages.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#'
#' @return Named list with start values for all four metrics
#' @keywords internal
get_venue_start_values <- function(format = "t20") {
  # Residual-based indices start at 0 (no deviation from expected)
  # Raw EMA indices use calibrated format-specific values
  fmt <- normalize_format(format)
  switch(fmt,
    "t20" = list(
      run_rate = 0.0,
      wicket_rate = 0.0,
      boundary_rate = VENUE_START_BOUNDARY_RATE_T20,
      dot_rate = VENUE_START_DOT_RATE_T20
    ),
    "odi" = list(
      run_rate = 0.0,
      wicket_rate = 0.0,
      boundary_rate = VENUE_START_BOUNDARY_RATE_ODI,
      dot_rate = VENUE_START_DOT_RATE_ODI
    ),
    "test" = list(
      run_rate = 0.0,
      wicket_rate = 0.0,
      boundary_rate = VENUE_START_BOUNDARY_RATE_TEST,
      dot_rate = VENUE_START_DOT_RATE_TEST
    ),
    list(
      run_rate = 0.0,
      wicket_rate = 0.0,
      boundary_rate = VENUE_START_BOUNDARY_RATE_T20,
      dot_rate = VENUE_START_DOT_RATE_T20
    )
  )
}


#' Get Minimum Balls for Reliable Venue Index
#'
#' Returns the minimum balls required for a reliable venue index.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#'
#' @return Integer. Minimum balls for reliable index
#' @keywords internal
get_venue_min_balls <- function(format = "t20") {
  fmt <- normalize_format(format)
  switch(fmt,
    "t20" = VENUE_MIN_BALLS_T20,
    "odi" = VENUE_MIN_BALLS_ODI,
    "test" = VENUE_MIN_BALLS_TEST,
    VENUE_MIN_BALLS_T20
  )
}


# ==============================================================================
# VENUE NORMALIZATION FUNCTIONS
# ==============================================================================

#' Normalize Venue Name
#'
#' Looks up canonical venue name from aliases table.
#' Falls back to original name if not found.
#'
#' @param venue Character. Venue name to normalize
#' @param conn DBI connection. Database connection
#'
#' @return Character. Canonical venue name
#' @keywords internal
normalize_venue <- function(venue, conn) {
  if (is.na(venue) || venue == "") {
    return(venue)
  }

  # Check if venue_aliases table exists
  if (!"venue_aliases" %in% DBI::dbListTables(conn)) {
    return(venue)
  }

  # Look up in aliases table (using parameterized query for safety)
  result <- DBI::dbGetQuery(conn,
    "SELECT canonical_venue FROM venue_aliases WHERE alias = ? LIMIT 1",
    params = list(venue)
  )

  if (nrow(result) > 0) {
    return(result$canonical_venue[1])
  }

  # Return original if not found
  venue
}


#' Normalize Venue Names (Vectorized)
#'
#' Batch normalize venue names for efficiency.
#'
#' @param venues Character vector. Venue names to normalize
#' @param conn DBI connection. Database connection
#'
#' @return Character vector. Canonical venue names
#' @keywords internal
normalize_venues <- function(venues, conn) {
  if (length(venues) == 0) {
    return(character(0))
  }

  # Check if venue_aliases table exists
  if (!"venue_aliases" %in% DBI::dbListTables(conn)) {
    return(venues)
  }

  # Get unique venues
  unique_venues <- unique(venues)

  # Escape single quotes using helper for IN clause
  # (parameterized queries don't easily support variable-length IN clauses)
  escaped_venues <- escape_sql_quotes(unique_venues)

  # Query all aliases at once
  aliases <- DBI::dbGetQuery(conn, sprintf("
    SELECT alias, canonical_venue
    FROM venue_aliases
    WHERE alias IN ('%s')
  ", paste(escaped_venues, collapse = "','")))

  if (nrow(aliases) == 0) {
    return(venues)
  }

  # Create lookup
  alias_map <- stats::setNames(aliases$canonical_venue, aliases$alias)

  # Apply normalization
  normalized <- venues
  for (i in seq_along(venues)) {
    if (venues[i] %in% names(alias_map)) {
      normalized[i] <- alias_map[[venues[i]]]
    }
  }

  normalized
}


#' Build Default Venue Aliases
#'
#' Creates default alias mappings from the bundled venue_aliases.csv file.
#' The CSV file is located at inst/extdata/venue_aliases.csv and can be
#' edited to add new aliases without modifying code.
#'
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns number of aliases created
#' @keywords internal
build_default_venue_aliases <- function(conn) {
  cli::cli_h2("Building venue aliases")


  # Check if table exists
  if (!"venue_aliases" %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("venue_aliases table does not exist")
    cli::cli_alert_info("Run create_schema() or initialize database first")
    return(invisible(0))
  }

  # Load aliases from CSV file
  csv_path <- system.file("extdata", "venue_aliases.csv", package = "bouncer")

  if (csv_path == "") {
    cli::cli_alert_warning("venue_aliases.csv not found in package")
    return(invisible(0))
  }

  aliases_df <- utils::read.csv(csv_path, stringsAsFactors = FALSE)
  cli::cli_alert_info("Loaded {nrow(aliases_df)} aliases from CSV")

  # Insert aliases
  n_inserted <- 0
  for (i in seq_len(nrow(aliases_df))) {
    alias <- aliases_df$alias[i]
    canonical <- aliases_df$canonical_venue[i]
    country <- aliases_df$country[i]

    # Handle NA country
    country_sql <- if (is.na(country) || country == "") {
      "NULL"
    } else {
      sprintf("'%s'", escape_sql_quotes(country))
    }

    # Insert (skip if already exists)
    tryCatch({
      DBI::dbExecute(conn, sprintf("
        INSERT INTO venue_aliases (alias, canonical_venue, country)
        VALUES ('%s', '%s', %s)
        ON CONFLICT (alias) DO NOTHING
      ", escape_sql_quotes(alias),
         escape_sql_quotes(canonical),
         country_sql))
      n_inserted <- n_inserted + 1
    }, error = function(e) {
      # Silently skip duplicates or errors
    })
  }

  cli::cli_alert_success("Built {n_inserted} venue aliases")
  invisible(n_inserted)
}


#' Add Venue Alias
#'
#' Adds a single alias -> canonical mapping.
#'
#' @param alias Character. The variant name
#' @param canonical Character. The standardized name
#' @param country Character. Optional country name
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
add_venue_alias <- function(alias, canonical, country = NULL, conn) {
  if (!"venue_aliases" %in% DBI::dbListTables(conn)) {
    cli::cli_alert_danger("venue_aliases table does not exist")
    return(invisible(FALSE))
  }

  # Use parameterized query for safety
  if (is.null(country)) {
    DBI::dbExecute(conn,
      "INSERT INTO venue_aliases (alias, canonical_venue, country)
       VALUES (?, ?, NULL)
       ON CONFLICT (alias) DO UPDATE SET canonical_venue = EXCLUDED.canonical_venue",
      params = list(alias, canonical)
    )
  } else {
    DBI::dbExecute(conn,
      "INSERT INTO venue_aliases (alias, canonical_venue, country)
       VALUES (?, ?, ?)
       ON CONFLICT (alias) DO UPDATE SET canonical_venue = EXCLUDED.canonical_venue",
      params = list(alias, canonical, country)
    )
  }

  cli::cli_alert_success("Added alias: '{alias}' -> '{canonical}'")
  invisible(TRUE)
}


# ==============================================================================
# TABLE MANAGEMENT FUNCTIONS
# ==============================================================================

#' Create Format-Specific Venue Skill Table
#'
#' Creates a venue skill index table for a specific format.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#' @param overwrite Logical. If TRUE, drops and recreates table. Default FALSE.
#'
#' @return Invisibly returns TRUE on success
#' @keywords internal
create_format_venue_skill_table <- function(format = "t20", conn, overwrite = FALSE) {

  table_name <- paste0(format, "_venue_skill")

  # Check if table exists
  tables <- DBI::dbListTables(conn)

  if (table_name %in% tables) {
    if (!overwrite) {
      cli::cli_alert_info("Table '{table_name}' already exists")
      return(invisible(TRUE))
    }
    cli::cli_alert_warning("Dropping existing '{table_name}' table")
    DBI::dbExecute(conn, paste0("DROP TABLE ", table_name))
  }

  # Create table
  cli::cli_alert_info("Creating '{table_name}' table...")

  DBI::dbExecute(conn, sprintf("
    CREATE TABLE %s (
      delivery_id VARCHAR PRIMARY KEY,
      match_id VARCHAR,
      match_date DATE,
      venue VARCHAR,

      -- Venue skill indices (before this delivery)
      venue_run_rate DOUBLE,        -- EMA of runs per ball at venue
      venue_wicket_rate DOUBLE,     -- EMA of wickets per ball at venue
      venue_boundary_rate DOUBLE,   -- EMA of boundaries (4s + 6s) per ball
      venue_dot_rate DOUBLE,        -- EMA of dot balls per ball

      -- Ball count (for reliability)
      venue_balls INTEGER,

      -- Actual outcomes
      actual_runs INTEGER,
      is_wicket BOOLEAN,
      is_boundary BOOLEAN,
      is_dot BOOLEAN
    )
  ", table_name))

  # Create indexes
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_venue_skill_match ON %s(match_id)", format, table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_venue_skill_venue ON %s(venue)", format, table_name))
  DBI::dbExecute(conn, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_venue_skill_date ON %s(match_date)", format, table_name))

  cli::cli_alert_success("Created '{table_name}' table with indexes")

  invisible(TRUE)
}


#' Insert Format Venue Skill Records
#'
#' Batch inserts venue skill records into a format-specific table.
#'
#' @param skills_df Data frame. Venue skill records to insert
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns number of rows inserted
#' @keywords internal
insert_format_venue_skills <- function(skills_df, format = "t20", conn) {

  table_name <- paste0(format, "_venue_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_danger("Table '{table_name}' does not exist")
    cli::cli_alert_info("Run create_format_venue_skill_table('{format}', conn) first")
    return(invisible(0))
  }

  # Required columns
  required_cols <- c("delivery_id", "match_id", "match_date", "venue",
                     "venue_run_rate", "venue_wicket_rate",
                     "venue_boundary_rate", "venue_dot_rate",
                     "venue_balls", "actual_runs", "is_wicket",
                     "is_boundary", "is_dot")

  missing_cols <- setdiff(required_cols, names(skills_df))
  if (length(missing_cols) > 0) {
    cli::cli_alert_danger("Missing columns: {paste(missing_cols, collapse = ', ')}")
    return(invisible(0))
  }

  # Select only required columns
  skills_df <- skills_df[, required_cols]

  # Insert
  DBI::dbWriteTable(conn, table_name, skills_df, append = TRUE)

  invisible(nrow(skills_df))
}


# ==============================================================================
# STATE MANAGEMENT FUNCTIONS
# ==============================================================================

#' Get All Venues' Final Skill State
#'
#' Loads the final venue skill indices for all venues from existing data.
#' Used for incremental processing.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return List with environments for each skill type, or NULL if no data
#' @keywords internal
get_all_venue_skill_state <- function(format = "t20", conn) {
  table_name <- get_skill_table_name(format, "venue_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  # Get final skill indices for each venue from their most recent delivery

  venue_skills <- DBI::dbGetQuery(conn, sprintf("
    WITH ranked AS (
      SELECT
        venue,
        venue_run_rate,
        venue_wicket_rate,
        venue_boundary_rate,
        venue_dot_rate,
        venue_balls,
        ROW_NUMBER() OVER (PARTITION BY venue ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT venue, venue_run_rate, venue_wicket_rate, venue_boundary_rate, venue_dot_rate, venue_balls
    FROM ranked
    WHERE rn = 1
  ", table_name))

  if (nrow(venue_skills) == 0) {
    return(NULL)
  }

  # Create environments for fast lookup
  run_rate <- new.env(hash = TRUE)
  wicket_rate <- new.env(hash = TRUE)
  boundary_rate <- new.env(hash = TRUE)
  dot_rate <- new.env(hash = TRUE)
  balls <- new.env(hash = TRUE)

  # Populate from query results
  for (i in seq_len(nrow(venue_skills))) {
    v <- venue_skills$venue[i]
    run_rate[[v]] <- venue_skills$venue_run_rate[i]
    wicket_rate[[v]] <- venue_skills$venue_wicket_rate[i]
    boundary_rate[[v]] <- venue_skills$venue_boundary_rate[i]
    dot_rate[[v]] <- venue_skills$venue_dot_rate[i]
    balls[[v]] <- venue_skills$venue_balls[i]
  }

  n_venues <- length(ls(run_rate))
  cli::cli_alert_success("Loaded skill state for {n_venues} venues")

  list(
    run_rate = run_rate,
    wicket_rate = wicket_rate,
    boundary_rate = boundary_rate,
    dot_rate = dot_rate,
    balls = balls,
    n_venues = n_venues
  )
}


#' Get Last Processed Venue Skill Delivery
#'
#' Gets information about the last processed delivery for incremental updates.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with last_delivery_id and last_match_date, or NULL
#' @keywords internal
get_last_processed_venue_skill_delivery <- function(format = "t20", conn) {
  table_name <- paste0(format, "_venue_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  result <- DBI::dbGetQuery(conn, sprintf("
    SELECT delivery_id, match_date
    FROM %s
    ORDER BY match_date DESC, delivery_id DESC
    LIMIT 1
  ", table_name))

  if (nrow(result) == 0) {
    return(NULL)
  }

  result
}


# ==============================================================================
# QUERY AND JOIN FUNCTIONS
# ==============================================================================

#' Get Current Venue Skill Index
#'
#' Gets the most recent skill indices for a venue.
#'
#' @param venue Character. Venue name (will be normalized)
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Named list with skill indices, or NULL if not found
#' @keywords internal
get_venue_skill <- function(venue, format = "t20", conn) {

  table_name <- paste0(format, "_venue_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    return(NULL)
  }

  # Normalize venue name
  venue_canonical <- normalize_venue(venue, conn)

  # Use parameterized query for the venue filter (table_name is safe - from code)
  result <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      venue,
      venue_run_rate as run_rate,
      venue_wicket_rate as wicket_rate,
      venue_boundary_rate as boundary_rate,
      venue_dot_rate as dot_rate,
      venue_balls as balls,
      match_date
    FROM %s
    WHERE venue = ?
    ORDER BY match_date DESC, delivery_id DESC
    LIMIT 1
  ", table_name), params = list(venue_canonical))

  if (nrow(result) == 0) {
    return(NULL)
  }

  as.list(result)
}


#' Join Venue Skill Indices to Deliveries
#'
#' Joins venue skill indices from the skill table to a deliveries data frame.
#'
#' @param deliveries_df Data frame. Must contain delivery_id column.
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#' @param skill_columns Character vector. Which skill columns to join.
#'
#' @return Data frame with venue skill columns joined
#' @keywords internal
join_venue_skill_indices <- function(deliveries_df, format = "t20", conn,
                                      skill_columns = c("venue_run_rate",
                                                        "venue_wicket_rate",
                                                        "venue_boundary_rate",
                                                        "venue_dot_rate",
                                                        "venue_balls")) {

  table_name <- get_skill_table_name(format, "venue_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Table '{table_name}' does not exist")
    cli::cli_alert_info("Run data-raw/ratings/venue/01_calculate_venue_skill_indices.R first")
    return(deliveries_df)
  }

  if (!"delivery_id" %in% names(deliveries_df)) {
    cli::cli_alert_danger("deliveries_df must contain 'delivery_id' column")
    return(deliveries_df)
  }

  # Get venue skill indices for matching delivery_ids
  delivery_ids <- unique(deliveries_df$delivery_id)

  # Query in batches if too many
  batch_size <- 10000
  if (length(delivery_ids) > batch_size) {
    cli::cli_alert_info("Fetching venue skill indices in batches...")

    n_batches <- ceiling(length(delivery_ids) / batch_size)
    batch_results <- vector("list", n_batches)

    for (i in seq_len(n_batches)) {
      start_idx <- (i - 1) * batch_size + 1
      end_idx <- min(i * batch_size, length(delivery_ids))
      batch_ids <- delivery_ids[start_idx:end_idx]

      # Escape single quotes in delivery IDs (e.g., Durban's_Super_Giants)
      batch_ids_escaped <- escape_sql_quotes(batch_ids)

      batch_results[[i]] <- DBI::dbGetQuery(conn, sprintf("
        SELECT delivery_id, %s
        FROM %s
        WHERE delivery_id IN ('%s')
      ", paste(skill_columns, collapse = ", "),
         table_name,
         paste(batch_ids_escaped, collapse = "','")))

      if (i %% 10 == 0) {
        cli::cli_alert_info("Fetched {i}/{n_batches} batches...")
      }
    }

    skill_data <- fast_rbind(batch_results)
  } else {
    # Escape single quotes in delivery IDs
    delivery_ids_escaped <- escape_sql_quotes(delivery_ids)

    skill_data <- DBI::dbGetQuery(conn, sprintf("
      SELECT delivery_id, %s
      FROM %s
      WHERE delivery_id IN ('%s')
    ", paste(skill_columns, collapse = ", "),
       table_name,
       paste(delivery_ids_escaped, collapse = "','")))
  }

  if (nrow(skill_data) == 0) {
    cli::cli_alert_warning("No matching venue skill indices found")
    return(deliveries_df)
  }

  # Join to deliveries
  result <- dplyr::left_join(deliveries_df, skill_data, by = "delivery_id")

  n_matched <- sum(!is.na(result[[skill_columns[1]]]))
  cli::cli_alert_success("Joined venue skill indices: {n_matched}/{nrow(deliveries_df)} deliveries matched")

  result
}


#' Add Venue Skill Features to Model Data
#'
#' Convenience function that adds venue skill index features to a deliveries
#' data frame and fills missing values with starting values.
#'
#' @param deliveries_df Data frame. Must contain delivery_id column.
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#' @param fill_missing Logical. If TRUE, fills NA with starting values. Default TRUE.
#'
#' @return Data frame with venue skill features added
#' @keywords internal
add_venue_skill_features <- function(deliveries_df, format = "t20", conn,
                                      fill_missing = TRUE) {

  # Join venue skill indices
  result <- join_venue_skill_indices(
    deliveries_df,
    format = format,
    conn = conn
  )

  # Fill missing values with starting values
  if (fill_missing) {
    start_vals <- get_venue_start_values(format)
    min_balls <- get_venue_min_balls(format)

    # Count NAs BEFORE coalesce fill
    n_filled <- if ("venue_run_rate" %in% names(result)) {
      sum(is.na(result$venue_run_rate))
    } else {
      nrow(result)
    }

    result <- result %>%
      dplyr::mutate(
        venue_run_rate = dplyr::coalesce(venue_run_rate, start_vals$run_rate),
        venue_wicket_rate = dplyr::coalesce(venue_wicket_rate, start_vals$wicket_rate),
        venue_boundary_rate = dplyr::coalesce(venue_boundary_rate, start_vals$boundary_rate),
        venue_dot_rate = dplyr::coalesce(venue_dot_rate, start_vals$dot_rate),
        venue_balls = dplyr::coalesce(venue_balls, 0L),
        venue_reliable = as.integer(venue_balls >= min_balls)
      )

    if (n_filled > 0) {
      cli::cli_alert_info("Filled {n_filled} missing venue skill values with starting defaults")
    }
  }

  result
}


# ==============================================================================
# STATISTICS FUNCTIONS
# ==============================================================================

#' Get Format Venue Skill Statistics
#'
#' Retrieves summary statistics for venue skill indices.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with venue skill statistics
#' @keywords internal
get_format_venue_skill_stats <- function(format = "t20", conn) {

  table_name <- paste0(format, "_venue_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Table '{table_name}' does not exist")
    return(NULL)
  }

  DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(*) as total_records,
      COUNT(DISTINCT venue) as unique_venues,
      MIN(match_date) as first_date,
      MAX(match_date) as last_date,
      AVG(venue_run_rate) as mean_run_rate,
      AVG(venue_wicket_rate) as mean_wicket_rate,
      AVG(venue_boundary_rate) as mean_boundary_rate,
      AVG(venue_dot_rate) as mean_dot_rate,
      AVG(actual_runs) as mean_actual_runs,
      AVG(CAST(is_wicket AS DOUBLE)) as actual_wicket_rate,
      AVG(CAST(is_boundary AS DOUBLE)) as actual_boundary_rate,
      AVG(CAST(is_dot AS DOUBLE)) as actual_dot_rate
    FROM %s
  ", table_name))
}


#' Get Venue Rankings
#'
#' Returns venues ranked by a specific metric.
#'
#' @param format Character. Format: "t20", "odi", or "test"
#' @param metric Character. Metric to rank by: "run_rate", "wicket_rate",
#'   "boundary_rate", or "dot_rate"
#' @param conn DBI connection. Database connection
#' @param min_balls Integer. Minimum balls for inclusion. Default uses format-specific value.
#' @param limit Integer. Number of venues to return. Default 20.
#'
#' @return Data frame with venue rankings
#' @keywords internal
get_venue_rankings <- function(format = "t20", metric = "run_rate", conn,
                               min_balls = NULL, limit = 20) {

  table_name <- paste0(format, "_venue_skill")

  if (!table_name %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Table '{table_name}' does not exist")
    return(NULL)
  }

  if (is.null(min_balls)) {
    min_balls <- get_venue_min_balls(format)
  }

  # Map metric to column name
  metric_col <- switch(metric,
    "run_rate" = "venue_run_rate",
    "wicket_rate" = "venue_wicket_rate",
    "boundary_rate" = "venue_boundary_rate",
    "dot_rate" = "venue_dot_rate",
    stop("Unknown metric: ", metric)
  )

  DBI::dbGetQuery(conn, sprintf("
    WITH latest AS (
      SELECT
        venue,
        venue_run_rate,
        venue_wicket_rate,
        venue_boundary_rate,
        venue_dot_rate,
        venue_balls,
        ROW_NUMBER() OVER (PARTITION BY venue ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM %s
    )
    SELECT
      venue,
      venue_run_rate as run_rate,
      venue_wicket_rate as wicket_rate,
      venue_boundary_rate as boundary_rate,
      venue_dot_rate as dot_rate,
      venue_balls as balls
    FROM latest
    WHERE rn = 1
      AND venue_balls >= %d
    ORDER BY %s DESC
    LIMIT %d
  ", table_name, min_balls, metric_col, limit))
}
