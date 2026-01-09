# 02 Calculate Team Skill Indices ----
#
# This script calculates per-delivery team skill indices using residual-based updates.
# Team skills represent "how much better/worse the team performs than context-expected".
#
# Supports: T20, ODI, Test formats (configurable via FORMAT_FILTER)
#
# KEY FEATURE: Uses AGNOSTIC model predictions as baseline expectation
#   - Agnostic model predicts expected runs/wicket based on CONTEXT ONLY
#   - NO player, team, or venue identity in agnostic model
#   - Team skills = "deviation from what any average team would do in this situation"
#
# Team Skill Indices:
#   - Batting Team Runs Skill: Residual-based scoring ability
#   - Batting Team Wicket Skill: Residual-based survival ability
#   - Bowling Team Runs Skill: Residual-based economy (restricting runs)
#   - Bowling Team Wicket Skill: Residual-based strike ability (taking wickets)
#
# Update Formula (EMA on Residuals):
#   residual = actual - agnostic_expected
#   new_skill = (1 - alpha) * old_skill + alpha * residual
#
# This converges to team's true deviation from expected (bounded and stable).
#
# REQUIRES: Run 01_train_agnostic_model.R first to train agnostic models
#
# Output:
#   - {format}_team_skill tables populated in DuckDB

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

# Determine formats to process
if (is.null(FORMAT_FILTER)) {
  formats_to_process <- names(FORMAT_GROUPS)
} else {
  formats_to_process <- FORMAT_FILTER
}

cat("\n")
cli::cli_h1("Team Skill Index Calculation")
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
conn <- get_db_connection(read_only = FALSE)
cli::cli_alert_success("Connected to database")

# 5. Process Each Format ----
for (current_format in formats_to_process) {

cat("\n")
cli::cli_h1("{toupper(current_format)} Team Skill Index Calculation")
cat("\n")

# Build format-specific values
format_match_types <- FORMAT_GROUPS[[current_format]]
match_type_filter <- paste(sprintf("'%s'", tolower(format_match_types)), collapse = ", ")

# Get format-specific parameters
ALPHA <- get_team_skill_alpha(current_format)
START_VALUES <- get_team_skill_start_values(current_format)
START_RUNS_SKILL <- START_VALUES$runs_skill
START_WICKET_SKILL <- START_VALUES$wicket_skill

cli::cli_alert_info("Alpha (learning rate): {ALPHA}")
cli::cli_alert_info("Starting skill values: runs={START_RUNS_SKILL}, wicket={START_WICKET_SKILL}")
cli::cli_alert_info("(Residual-based: 0 = context-expected, positive = above, negative = below)")

## 5.1 Check for existing data ----
cli::cli_h2("Checking existing data")

table_name <- paste0(current_format, "_team_skill")
existing_tables <- DBI::dbListTables(conn)

if (!table_name %in% existing_tables || FORCE_FULL) {
  mode <- "full"
  if (FORCE_FULL) {
    cli::cli_alert_info("FORCE_FULL = TRUE, running full calculation")
  } else {
    cli::cli_alert_info("Table does not exist, running full calculation")
  }
  create_format_team_skill_table(current_format, conn, overwrite = TRUE)
} else {
  # Check if we can do incremental
  last_processed <- get_last_processed_team_skill_delivery(current_format, conn)
  if (is.null(last_processed)) {
    mode <- "full"
    cli::cli_alert_info("No existing records, running full calculation")
    create_format_team_skill_table(current_format, conn, overwrite = TRUE)
  } else {
    mode <- "incremental"
    cli::cli_alert_success("Found existing data, running incremental update")
    cli::cli_alert_info("Last processed: {last_processed$match_date}")
  }
}

# 5.2 Load Deliveries ----
cli::cli_h2("Loading {toupper(current_format)} deliveries")

# Build query with features needed for agnostic model
base_query <- sprintf("
  WITH innings_totals AS (
    SELECT
      match_id,
      innings,
      batting_team,
      MAX(total_runs) AS innings_total
    FROM deliveries
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
    FROM deliveries d
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
    FROM matches m
  )
  SELECT
    cs.delivery_id,
    cs.match_id,
    cs.match_date,
    cs.match_type,
    cs.batting_team,
    cs.bowling_team,
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
    COALESCE(mc.event_tier, 3) AS event_tier
  FROM cumulative_scores cs
  LEFT JOIN match_context mc ON cs.match_id = mc.match_id
  WHERE cs.batting_team IS NOT NULL
    AND cs.bowling_team IS NOT NULL
", match_type_filter, match_type_filter)

# Add incremental filter if applicable
if (mode == "incremental" && !is.null(last_processed)) {
  base_query <- paste0(base_query, sprintf("
    AND cs.match_date >= '%s'
  ", last_processed$match_date))
}

if (!is.null(MATCH_LIMIT)) {
  match_ids <- DBI::dbGetQuery(conn, sprintf("
    SELECT DISTINCT match_id FROM deliveries
    WHERE LOWER(match_type) IN (%s)
    ORDER BY match_date
    LIMIT %d
  ", match_type_filter, MATCH_LIMIT))$match_id

  placeholders <- paste(rep("?", length(match_ids)), collapse = ", ")
  query <- paste0(base_query, sprintf(" AND cs.match_id IN (%s)", placeholders))
  query <- paste0(query, " ORDER BY cs.match_date, cs.delivery_id")
  deliveries <- DBI::dbGetQuery(conn, query, params = as.list(match_ids))
} else {
  query <- paste0(base_query, " ORDER BY cs.match_date, cs.delivery_id")
  deliveries <- DBI::dbGetQuery(conn, query)
}

setDT(deliveries)

# Filter out already processed deliveries in incremental mode
if (mode == "incremental" && !is.null(last_processed)) {
  existing_ids <- DBI::dbGetQuery(conn, sprintf("
    SELECT delivery_id FROM %s WHERE match_date >= '%s'
  ", table_name, last_processed$match_date))$delivery_id

  if (length(existing_ids) > 0) {
    deliveries <- deliveries[!delivery_id %in% existing_ids]
    cli::cli_alert_info("Filtered out {length(existing_ids)} already processed deliveries")
  }
}

n_deliveries <- nrow(deliveries)
n_matches <- uniqueN(deliveries$match_id)

cli::cli_alert_success("Loaded {format(n_deliveries, big.mark = ',')} deliveries from {n_matches} matches")

if (n_deliveries == 0) {
  cli::cli_alert_success("No new {toupper(current_format)} deliveries to process")
  cat("\n")
  next
}

# 5.3 Generate agnostic model predictions ----
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

# 5.4 Initialize Team Skill State ----
cli::cli_h2("Initializing team skill state")

all_batting_teams <- unique(deliveries$batting_team)
all_bowling_teams <- unique(deliveries$bowling_team)
all_teams <- unique(c(all_batting_teams, all_bowling_teams))

cli::cli_alert_info("{length(all_teams)} unique teams in new data")

if (mode == "incremental") {
  cli::cli_alert_info("Loading existing team skill state...")
  team_state <- get_all_team_skill_state(current_format, conn)

  if (!is.null(team_state)) {
    batting_runs_skill <- team_state$batting_runs_skill
    batting_wicket_skill <- team_state$batting_wicket_skill
    batting_balls <- team_state$batting_balls
    bowling_runs_skill <- team_state$bowling_runs_skill
    bowling_wicket_skill <- team_state$bowling_wicket_skill
    bowling_balls <- team_state$bowling_balls

    # Initialize new teams
    new_teams <- 0
    for (t in all_teams) {
      if (!exists(t, envir = batting_runs_skill)) {
        batting_runs_skill[[t]] <- START_RUNS_SKILL
        batting_wicket_skill[[t]] <- START_WICKET_SKILL
        batting_balls[[t]] <- 0L
        bowling_runs_skill[[t]] <- START_RUNS_SKILL
        bowling_wicket_skill[[t]] <- START_WICKET_SKILL
        bowling_balls[[t]] <- 0L
        new_teams <- new_teams + 1
      }
    }

    cli::cli_alert_success("Loaded state, initialized {new_teams} new teams")
  } else {
    mode <- "full"
    cli::cli_alert_warning("Could not load state, falling back to full calculation")
  }
}

if (mode == "full") {
  # Fresh state
  batting_runs_skill <- new.env(hash = TRUE)
  batting_wicket_skill <- new.env(hash = TRUE)
  batting_balls <- new.env(hash = TRUE)
  bowling_runs_skill <- new.env(hash = TRUE)
  bowling_wicket_skill <- new.env(hash = TRUE)
  bowling_balls <- new.env(hash = TRUE)

  for (t in all_teams) {
    batting_runs_skill[[t]] <- START_RUNS_SKILL
    batting_wicket_skill[[t]] <- START_WICKET_SKILL
    batting_balls[[t]] <- 0L
    bowling_runs_skill[[t]] <- START_RUNS_SKILL
    bowling_wicket_skill[[t]] <- START_WICKET_SKILL
    bowling_balls[[t]] <- 0L
  }

  cli::cli_alert_success("Initialized {length(all_teams)} teams")
}

# 5.5 Pre-extract columns ----
delivery_ids <- deliveries$delivery_id
match_ids <- deliveries$match_id
match_dates <- deliveries$match_date
batting_teams <- deliveries$batting_team
bowling_teams <- deliveries$bowling_team
runs_vec <- deliveries$runs_batter
wicket_vec <- deliveries$is_wicket

# 5.6 Process deliveries ----
cli::cli_h2("Processing deliveries")

result_mat <- matrix(NA_real_, nrow = n_deliveries, ncol = 12)
colnames(result_mat) <- c(
  "batting_runs_skill", "batting_wicket_skill",
  "bowling_runs_skill", "bowling_wicket_skill",
  "exp_runs", "exp_wicket",
  "actual_runs", "is_wicket",
  "batting_balls", "bowling_balls",
  "runs_residual", "wicket_residual"
)

cli::cli_progress_bar("Calculating team skill indices", total = n_deliveries)

for (i in seq_len(n_deliveries)) {
  batting_team <- batting_teams[i]
  bowling_team <- bowling_teams[i]
  runs <- runs_vec[i]
  is_wicket <- wicket_vec[i]

  # Get current skill indices (BEFORE this delivery)
  bat_runs <- batting_runs_skill[[batting_team]]
  bat_wicket <- batting_wicket_skill[[batting_team]]
  bat_balls <- batting_balls[[batting_team]]

  bowl_runs <- bowling_runs_skill[[bowling_team]]
  bowl_wicket <- bowling_wicket_skill[[bowling_team]]
  bowl_balls <- bowling_balls[[bowling_team]]

  # Get expected values from agnostic model (context-only baseline)
  exp_runs <- agnostic_exp_runs[i]
  exp_wicket <- agnostic_exp_wicket[i]

  # Calculate residuals
  runs_residual <- runs - exp_runs
  survived <- if (is_wicket) 0 else 1
  survival_residual <- survived - (1 - exp_wicket)
  wicket_val <- if (is_wicket) 1 else 0
  wicket_residual <- wicket_val - exp_wicket

  # Store current state (BEFORE update)
  result_mat[i, ] <- c(
    bat_runs, bat_wicket,
    bowl_runs, bowl_wicket,
    exp_runs, exp_wicket,
    runs, is_wicket,
    bat_balls, bowl_balls,
    runs_residual, wicket_residual
  )

  # Update team skill indices using PROPER EMA formula
  # Formula: new = (1 - alpha) * old + alpha * residual
  # Converges to team's true deviation from expected (bounded and stable)

  # Batting team runs: positive residual = scored more than expected
  new_bat_runs <- (1 - ALPHA) * bat_runs + ALPHA * runs_residual
  batting_runs_skill[[batting_team]] <- new_bat_runs

  # Batting team wicket: survival residual (positive = survived when expected out)
  new_bat_wicket <- (1 - ALPHA) * bat_wicket + ALPHA * survival_residual
  batting_wicket_skill[[batting_team]] <- new_bat_wicket

  batting_balls[[batting_team]] <- bat_balls + 1L

  # Bowling team runs: positive residual = conceded more than expected (bad)
  new_bowl_runs <- (1 - ALPHA) * bowl_runs + ALPHA * runs_residual
  bowling_runs_skill[[bowling_team]] <- new_bowl_runs

  # Bowling team wicket: positive residual = took wicket when not expected
  new_bowl_wicket <- (1 - ALPHA) * bowl_wicket + ALPHA * wicket_residual
  bowling_wicket_skill[[bowling_team]] <- new_bowl_wicket

  bowling_balls[[bowling_team]] <- bowl_balls + 1L

  if (i %% 10000 == 0) {
    cli::cli_progress_update(set = i)
  }
}

cli::cli_progress_done()

# 5.7 Build result data.table ----
cli::cli_h2("Building output data")

result_dt <- data.table(
  delivery_id = delivery_ids,
  match_id = match_ids,
  match_date = match_dates,
  batting_team_id = batting_teams,
  bowling_team_id = bowling_teams,
  batting_team_runs_skill = result_mat[, 1],
  batting_team_wicket_skill = result_mat[, 2],
  bowling_team_runs_skill = result_mat[, 3],
  bowling_team_wicket_skill = result_mat[, 4],
  exp_runs_agnostic = result_mat[, 5],
  exp_wicket_agnostic = result_mat[, 6],
  actual_runs = as.integer(result_mat[, 7]),
  is_wicket = as.logical(result_mat[, 8]),
  batting_team_balls = as.integer(result_mat[, 9]),
  bowling_team_balls = as.integer(result_mat[, 10])
)

# 5.8 Batch insert ----
cli::cli_h2("Inserting to database")

n_batches <- ceiling(n_deliveries / BATCH_SIZE)
cli::cli_progress_bar("Inserting batches", total = n_batches)

for (b in seq_len(n_batches)) {
  start_idx <- (b - 1) * BATCH_SIZE + 1
  end_idx <- min(b * BATCH_SIZE, n_deliveries)

  batch_df <- as.data.frame(result_dt[start_idx:end_idx])
  insert_format_team_skills(batch_df, current_format, conn)

  cli::cli_progress_update()
}

cli::cli_progress_done()

cli::cli_alert_success("Inserted {format(n_deliveries, big.mark = ',')} deliveries")

# 5.9 Store parameters ----
cli::cli_h2("Storing parameters")

# Ensure table exists
if (!"team_skill_calculation_params" %in% DBI::dbListTables(conn)) {
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS team_skill_calculation_params (
      format VARCHAR PRIMARY KEY,
      alpha DOUBLE,
      start_runs_skill DOUBLE,
      start_wicket_skill DOUBLE,
      last_delivery_id VARCHAR,
      last_match_date DATE,
      total_deliveries INTEGER,
      calculated_at TIMESTAMP
    )
  ")
}

last_row <- result_dt[.N]
DBI::dbExecute(conn, "DELETE FROM team_skill_calculation_params WHERE format = ?",
               params = list(current_format))
DBI::dbExecute(conn, "
  INSERT INTO team_skill_calculation_params
  (format, alpha, start_runs_skill, start_wicket_skill, last_delivery_id, last_match_date, total_deliveries, calculated_at)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
", params = list(
  current_format, ALPHA, START_RUNS_SKILL, START_WICKET_SKILL,
  last_row$delivery_id, as.character(last_row$match_date),
  n_deliveries, Sys.time()
))

cli::cli_alert_success("Parameters stored for future incremental updates")

# 5.10 Summary Statistics ----
cat("\n")
cli::cli_h2("{toupper(current_format)} Team Skill Index Summary")

stats <- get_format_team_skill_stats(current_format, conn)

if (!is.null(stats)) {
  cli::cli_alert_info("Total records: {format(stats$total_records, big.mark = ',')}")
  cli::cli_alert_info("Unique batting teams: {stats$unique_batting_teams}")
  cli::cli_alert_info("Unique bowling teams: {stats$unique_bowling_teams}")
  cli::cli_alert_info("Date range: {stats$first_date} to {stats$last_date}")

  cat("\nMean Team Skill Indices (should be ~0 if well-calibrated):\n")
  cat(sprintf("  Batting Runs Skill:   %+.4f\n", stats$mean_batting_runs_skill))
  cat(sprintf("  Batting Wicket Skill: %+.4f\n", stats$mean_batting_wicket_skill))
  cat(sprintf("  Bowling Runs Skill:   %+.4f\n", stats$mean_bowling_runs_skill))
  cat(sprintf("  Bowling Wicket Skill: %+.4f\n", stats$mean_bowling_wicket_skill))

  cat("\nCalibration Check:\n")
  cat(sprintf("  Expected runs (agnostic): %.3f\n", stats$mean_exp_runs))
  cat(sprintf("  Actual runs:              %.3f\n", stats$mean_actual_runs))
  cat(sprintf("  Actual wicket rate:       %.3f (%.1f%%)\n",
              stats$actual_wicket_rate, stats$actual_wicket_rate * 100))
}

# Show top teams
cli::cli_h3("{toupper(current_format)} Top 10 Batting Teams by Runs Skill")
top_batting <- DBI::dbGetQuery(conn, sprintf("
  WITH latest AS (
    SELECT batting_team_id, batting_team_runs_skill, batting_team_wicket_skill, batting_team_balls,
           ROW_NUMBER() OVER (PARTITION BY batting_team_id ORDER BY match_date DESC, delivery_id DESC) as rn
    FROM %s
  )
  SELECT batting_team_id as team, batting_team_runs_skill as runs_skill,
         batting_team_wicket_skill as wicket_skill, batting_team_balls as balls
  FROM latest WHERE rn = 1 AND batting_team_balls >= 500
  ORDER BY batting_team_runs_skill DESC LIMIT 10
", table_name))

if (nrow(top_batting) > 0) {
  cat("\n")
  for (i in seq_len(nrow(top_batting))) {
    t <- top_batting[i, ]
    cli::cli_alert_info("{i}. {t$team}: {sprintf('%+.3f', t$runs_skill)} runs, {sprintf('%+.3f', t$wicket_skill)} wicket ({format(t$balls, big.mark=',')} balls)")
  }
}

cli::cli_h3("{toupper(current_format)} Top 10 Bowling Teams by Economy (Runs Skill)")
top_bowling <- DBI::dbGetQuery(conn, sprintf("
  WITH latest AS (
    SELECT bowling_team_id, bowling_team_runs_skill, bowling_team_wicket_skill, bowling_team_balls,
           ROW_NUMBER() OVER (PARTITION BY bowling_team_id ORDER BY match_date DESC, delivery_id DESC) as rn
    FROM %s
  )
  SELECT bowling_team_id as team, bowling_team_runs_skill as runs_skill,
         bowling_team_wicket_skill as wicket_skill, bowling_team_balls as balls
  FROM latest WHERE rn = 1 AND bowling_team_balls >= 500
  ORDER BY bowling_team_runs_skill ASC LIMIT 10
", table_name))

if (nrow(top_bowling) > 0) {
  cat("\n")
  for (i in seq_len(nrow(top_bowling))) {
    t <- top_bowling[i, ]
    cli::cli_alert_info("{i}. {t$team}: {sprintf('%+.3f', t$runs_skill)} runs, {sprintf('%+.3f', t$wicket_skill)} wicket ({format(t$balls, big.mark=',')} balls)")
  }
}

cat("\n")
cli::cli_alert_success("{toupper(current_format)} Team Skill Index calculation complete!")

}  # End of format loop

# 6. Final Summary ----
cat("\n")
cli::cli_h1("All Formats Complete")
cli::cli_alert_success("Processed formats: {paste(toupper(formats_to_process), collapse = ', ')}")
cat("\n")

cli::cli_h3("Team Skill Index Interpretation (Residual-Based)")
cli::cli_bullets(c(
  "i" = "Positive runs_skill = team scores/concedes MORE than context-expected",
  "i" = "Negative runs_skill = team scores/concedes LESS than context-expected",
  "i" = "For batting: positive = good (scores more), negative = bad",
  "i" = "For bowling: positive = bad (concedes more), negative = good",
  "i" = "All indices relative to AGNOSTIC baseline (context-only)"
))
cat("\n")

cli::cli_h3("Pipeline Dependencies")
cli::cli_bullets(c(
  "!" = "REQUIRES: Run 01_train_agnostic_model.R first to train agnostic models",
  "i" = "Team skill indices stored in {{format}}_team_skill tables",
  "i" = "Re-run this script for incremental updates (new matches)"
))
cat("\n")

cli::cli_h3("Query Example")
cat("
SELECT d.*, ts.batting_team_runs_skill, ts.batting_team_wicket_skill,
       ts.bowling_team_runs_skill, ts.bowling_team_wicket_skill,
       ts.exp_runs_agnostic, ts.actual_runs
FROM deliveries d
JOIN t20_team_skill ts ON d.delivery_id = ts.delivery_id
WHERE LOWER(d.match_type) IN ('t20', 'it20')
LIMIT 10
")
cat("\n")
