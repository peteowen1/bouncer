# Save Agnostic Predictions to Database ----
#
# Pre-computes agnostic model predictions for ALL deliveries and saves them
# to a database table. This enables efficient lookup during ELO optimization.
#
# Output tables (one per format):
#   - agnostic_predictions_t20
#   - agnostic_predictions_odi
#   - agnostic_predictions_test
#
# Each table contains:
#   - delivery_id (primary key, joins to deliveries)
#   - agnostic_expected_runs (0-6 scale, weighted sum of outcome probabilities)
#   - agnostic_expected_wicket (0-1 probability, class 0 probability)
#
# Usage:
#   source("data-raw/models/ball-outcome/02_save_agnostic_predictions.R")
#
# Prerequisites:
#   - Run 01_train_agnostic_model.R first to train the models

# Setup ----
library(DBI)
library(dplyr)
library(xgboost)

if (!("bouncer" %in% loadedNamespaces())) {
  devtools::load_all()
}

# Configuration ----
FORMATS_TO_PROCESS <- c("t20", "odi", "test")
BATCH_SIZE <- 100000  # Process in batches to avoid memory issues

cat("\n")
cli::cli_h1("Save Agnostic Predictions to Database")
cli::cli_alert_info("Formats: {paste(FORMATS_TO_PROCESS, collapse = ', ')}")
cat("\n")

# Database Connection (WRITE mode) ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = FALSE)
cli::cli_alert_success("Connected in write mode")

# Models directory
models_dir <- file.path("..", "bouncerdata", "models")

# Process Each Format ----
for (format in FORMATS_TO_PROCESS) {

  cat("\n")
  cli::cli_rule("{toupper(format)} Format")
  cat("\n")

  # Load trained model ----
  cli::cli_h3("Loading trained model")
  model_path <- file.path(models_dir, sprintf("agnostic_outcome_%s.ubj", format))

  if (!file.exists(model_path)) {
    cli::cli_alert_warning("Model not found: {model_path}")
    cli::cli_alert_info("Run 01_train_agnostic_model.R first")
    next
  }

  xgb_model <- xgb.load(model_path)
  cli::cli_alert_success("Loaded model from {model_path}")

  # Determine format filters ----
  # Need different prefixes depending on which CTE we're in:
  # - format_filter_bare: standalone deliveries query (no join)
  # - format_filter_d: for d.match_type (deliveries aliased as d in joins)
  # - format_filter_m: for m.match_type (matches aliased as m in joins)
  if (format == "t20") {
    format_filter_bare <- "LOWER(match_type) IN ('t20', 'it20')"
    format_filter_d <- "LOWER(d.match_type) IN ('t20', 'it20')"
    format_filter_m <- "LOWER(m.match_type) IN ('t20', 'it20')"
  } else if (format == "odi") {
    format_filter_bare <- "LOWER(match_type) IN ('odi', 'odm')"
    format_filter_d <- "LOWER(d.match_type) IN ('odi', 'odm')"
    format_filter_m <- "LOWER(m.match_type) IN ('odi', 'odm')"
  } else {
    format_filter_bare <- "LOWER(match_type) IN ('test', 'mdm')"
    format_filter_d <- "LOWER(d.match_type) IN ('test', 'mdm')"
    format_filter_m <- "LOWER(m.match_type) IN ('test', 'mdm')"
  }

  # Get default expected values for format
  default_runs <- switch(format,
    "t20" = EXPECTED_RUNS_T20,
    "odi" = EXPECTED_RUNS_ODI,
    "test" = EXPECTED_RUNS_TEST,
    EXPECTED_RUNS_T20)
  default_wicket <- switch(format,
    "t20" = EXPECTED_WICKET_T20,
    "odi" = EXPECTED_WICKET_ODI,
    "test" = EXPECTED_WICKET_TEST,
    EXPECTED_WICKET_T20)

  # Count total deliveries ----
  count_query <- sprintf("
    SELECT COUNT(*) as n
    FROM cricsheet.deliveries d
    WHERE %s
      AND runs_batter NOT IN (5)
      AND runs_batter <= 6
  ", format_filter_d)
  total_count <- DBI::dbGetQuery(conn, count_query)$n
  cli::cli_alert_info("Total deliveries to process: {format(total_count, big.mark = ',')}")

  # Create/recreate output table ----
  table_name <- sprintf("agnostic_predictions_%s", format)
  cli::cli_h3("Creating table: {table_name}")

  # Drop if exists
  tryCatch({
    DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", table_name))
  }, error = function(e) NULL)

  # Create table
  DBI::dbExecute(conn, sprintf("
    CREATE TABLE %s (
      delivery_id VARCHAR PRIMARY KEY,
      agnostic_expected_runs DOUBLE,
      agnostic_expected_wicket DOUBLE
    )
  ", table_name))
  cli::cli_alert_success("Created table {table_name}")

  # Build query for features (same structure as training) ----
  # This query matches 01_train_agnostic_model.R feature extraction
  base_query <- sprintf("
    WITH innings_totals AS (
      SELECT
        match_id,
        innings,
        batting_team,
        MAX(total_runs) AS innings_total
      FROM cricsheet.deliveries
      WHERE %s
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
      WHERE %s
    ),
    match_context AS (
      SELECT DISTINCT
        m.match_id,
        m.event_name,
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
    ),
    league_stats AS (
      SELECT
        m.event_name,
        m.match_id,
        m.match_date,
        AVG(d.runs_batter + d.runs_extras) AS match_avg_runs,
        AVG(CAST(d.is_wicket AS DOUBLE)) AS match_wicket_rate
      FROM cricsheet.matches m
      JOIN cricsheet.deliveries d ON m.match_id = d.match_id
      WHERE %s
        AND m.event_name IS NOT NULL
      GROUP BY m.event_name, m.match_id, m.match_date
    ),
    league_running_avg AS (
      SELECT
        event_name,
        match_id,
        AVG(match_avg_runs) OVER (
          PARTITION BY event_name
          ORDER BY match_date, match_id
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS league_avg_runs,
        AVG(match_wicket_rate) OVER (
          PARTITION BY event_name
          ORDER BY match_date, match_id
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS league_avg_wicket
      FROM league_stats
    )
    SELECT
      cs.delivery_id,
      cs.match_id,
      cs.match_type,
      cs.innings,
      cs.over,
      cs.ball,
      cs.over_ball,
      cs.gender,
      (cs.wickets_fallen - CAST(cs.is_wicket AS INT)) AS wickets_fallen,
      (cs.batting_score - cs.bowling_score) AS runs_difference,
      COALESCE(mc.is_knockout, 0) AS is_knockout,
      COALESCE(mc.event_tier, 3) AS event_tier,
      COALESCE(lra.league_avg_runs, %f) AS league_avg_runs,
      COALESCE(lra.league_avg_wicket, %f) AS league_avg_wicket
    FROM cumulative_scores cs
    LEFT JOIN match_context mc ON cs.match_id = mc.match_id
    LEFT JOIN league_running_avg lra ON cs.match_id = lra.match_id
    WHERE cs.runs_batter NOT IN (5)
      AND cs.runs_batter <= 6
    ORDER BY cs.match_id, cs.delivery_id
  ", format_filter_bare, format_filter_d, format_filter_m, default_runs, default_wicket)

  # Process in batches ----
  cli::cli_h3("Processing deliveries in batches")

  n_batches <- ceiling(total_count / BATCH_SIZE)
  processed <- 0

  for (batch in 1:n_batches) {
    offset <- (batch - 1) * BATCH_SIZE

    batch_query <- sprintf("%s LIMIT %d OFFSET %d", base_query, BATCH_SIZE, offset)
    batch_data <- DBI::dbGetQuery(conn, batch_query)

    if (nrow(batch_data) == 0) break

    # Feature engineering (matching training script)
    if (format %in% c("t20", "odi")) {
      batch_data <- batch_data %>%
        mutate(
          overs_left = case_when(
            format == "t20" ~ pmax(0, 20 - over_ball),
            format == "odi" ~ pmax(0, 50 - over_ball),
            TRUE ~ NA_real_
          ),
          phase = case_when(
            format == "t20" & over < 6 ~ "powerplay",
            format == "t20" & over < 16 ~ "middle",
            format == "t20" ~ "death",
            format == "odi" & over < 10 ~ "powerplay",
            format == "odi" & over < 40 ~ "middle",
            format == "odi" ~ "death",
            TRUE ~ "middle"
          ),
          format_t20 = as.integer(format == "t20"),
          format_odi = as.integer(format == "odi"),
          phase_powerplay = as.integer(phase == "powerplay"),
          phase_middle = as.integer(phase == "middle"),
          phase_death = as.integer(phase == "death"),
          gender_male = as.integer(tolower(gender) == "male"),
          innings_num = as.integer(innings)
        )

      feature_matrix <- batch_data %>%
        select(
          format_t20, format_odi,
          innings_num, over, ball,
          wickets_fallen, runs_difference, overs_left,
          phase_powerplay, phase_middle, phase_death,
          gender_male,
          is_knockout, event_tier,
          league_avg_runs, league_avg_wicket
        ) %>%
        as.matrix()

    } else {
      # Test format
      batch_data <- batch_data %>%
        mutate(
          phase = case_when(
            over < 20 ~ "new_ball",
            over < 80 ~ "middle",
            TRUE ~ "old_ball"
          ),
          phase_new_ball = as.integer(phase == "new_ball"),
          phase_middle = as.integer(phase == "middle"),
          phase_old_ball = as.integer(phase == "old_ball"),
          gender_male = as.integer(tolower(gender) == "male"),
          innings_num = as.integer(innings)
        )

      feature_matrix <- batch_data %>%
        select(
          innings_num, over, ball,
          wickets_fallen, runs_difference,
          phase_new_ball, phase_middle, phase_old_ball,
          gender_male,
          is_knockout, event_tier,
          league_avg_runs, league_avg_wicket
        ) %>%
        as.matrix()
    }

    # Make predictions
    dmatrix <- xgb.DMatrix(data = feature_matrix)
    probs <- predict(xgb_model, dmatrix, reshape = TRUE)

    # probs is matrix: n_rows x 7 classes
    # Classes: 0=Wicket, 1=0runs, 2=1run, 3=2runs, 4=3runs, 5=4runs, 6=6runs

    # Calculate expected runs: weighted sum of outcome probabilities
    # Run values: 0, 0, 1, 2, 3, 4, 6 (wicket counts as 0 runs)
    run_values <- c(0, 0, 1, 2, 3, 4, 6)
    expected_runs <- probs %*% run_values

    # Expected wicket probability is class 0
    expected_wicket <- probs[, 1]

    # Prepare predictions data frame
    predictions_df <- data.frame(
      delivery_id = batch_data$delivery_id,
      agnostic_expected_runs = as.numeric(expected_runs),
      agnostic_expected_wicket = as.numeric(expected_wicket)
    )

    # Insert into database
    DBI::dbWriteTable(conn, table_name, predictions_df, append = TRUE, row.names = FALSE)

    processed <- processed + nrow(batch_data)
    cli::cli_progress_message("Processed {format(processed, big.mark = ',')} / {format(total_count, big.mark = ',')} deliveries")
  }

  cli::cli_alert_success("Saved {format(processed, big.mark = ',')} predictions to {table_name}")

  # Create index on delivery_id for fast lookups
  cli::cli_alert_info("Creating index on delivery_id...")
  tryCatch({
    DBI::dbExecute(conn, sprintf(
      "CREATE INDEX IF NOT EXISTS idx_%s_delivery_id ON %s(delivery_id)",
      format, table_name
    ))
  }, error = function(e) {
    cli::cli_alert_warning("Could not create index: {e$message}")
  })

  # Verify ----
  cli::cli_h3("Verification")
  verify <- DBI::dbGetQuery(conn, sprintf("
    SELECT
      COUNT(*) as n_rows,
      AVG(agnostic_expected_runs) as avg_runs,
      AVG(agnostic_expected_wicket) as avg_wicket,
      MIN(agnostic_expected_runs) as min_runs,
      MAX(agnostic_expected_runs) as max_runs
    FROM %s
  ", table_name))

  cli::cli_alert_info("Rows: {format(verify$n_rows, big.mark = ',')}")
  cli::cli_alert_info("Avg expected runs: {round(verify$avg_runs, 4)}")
  cli::cli_alert_info("Avg expected wicket: {round(verify$avg_wicket, 4)}")
  cli::cli_alert_info("Run range: [{round(verify$min_runs, 3)}, {round(verify$max_runs, 3)}]")
}

# Cleanup ----
cat("\n")
cli::cli_rule("Summary")
cat("\n")

# Close connection
if (exists("conn") && !is.null(conn)) {
  tryCatch({
    DBI::dbDisconnect(conn, shutdown = TRUE)
  }, error = function(e) NULL)
}

cli::cli_alert_success("Agnostic predictions saved to database!")
cli::cli_alert_info("Tables created: agnostic_predictions_t20, agnostic_predictions_odi, agnostic_predictions_test")
cli::cli_alert_info("Use: LEFT JOIN agnostic_predictions_t20 USING (delivery_id)")
cat("\n")
