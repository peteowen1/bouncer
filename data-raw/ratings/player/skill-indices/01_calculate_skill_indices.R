# 03 Calculate Skill Indices ----
#
# This script calculates player skill indices using residual-based updates.
# Skill indices represent deviation FROM context-expected performance.
#
# Supports: T20, ODI, Test formats (configurable via FORMAT_FILTER)
#
# KEY FEATURE: Uses AGNOSTIC model predictions as baseline expectation
#   - Agnostic model predicts expected runs/wicket based on CONTEXT ONLY
#   - NO player, team, or venue identity in agnostic model
#   - Features: over, ball, wickets, runs_diff, phase, innings, format, gender, knockout, tier
#   - Skill indices = "deviation from what any average player would do in this situation"
#
# Skill Indices:
#   - Batter Scoring Index (BSI): Residual-based scoring ability
#   - Batter Survival Rate (BSR): Residual-based survival ability
#   - Bowler Economy Index (BEI): Residual-based economy (runs conceded)
#   - Bowler Strike Rate (BoSR): Residual-based strike ability (wickets taken)
#
# Update Formula (EMA on Residuals):
#   residual = actual - agnostic_expected
#   new_index = (1 - alpha) * old_index + alpha * residual
#
# This converges to player's true deviation from expected (bounded and stable).
# Positive index = performs BETTER than context-expected
# Negative index = performs WORSE than context-expected
#
# Output:
#   - {format}_player_skill tables populated in DuckDB (e.g., t20_player_skill)

# 1. Setup ----
library(DBI)
library(data.table)
library(xgboost)
# Note: When called from run_full_pipeline.R, package is already loaded
if (!("bouncer" %in% loadedNamespaces())) {
  devtools::load_all()
}

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
USE_MODEL <- TRUE        # If TRUE, use agnostic model for baseline expectations (all formats)

# Determine formats to process
if (is.null(FORMAT_FILTER)) {
  formats_to_process <- names(FORMAT_GROUPS)
} else {
  formats_to_process <- FORMAT_FILTER
}

cat("\n")
cli::cli_h1("Player Skill Index Calculation")
cli::cli_alert_info("Formats to process: {paste(toupper(formats_to_process), collapse = ', ')}")
cat("\n")

# 3. Load Agnostic Models ----
# Agnostic models provide context-only baseline expectations (no player/team/venue identity)
agnostic_models <- list()
if (USE_MODEL) {
  cli::cli_h2("Loading agnostic outcome models")

  for (fmt in formats_to_process) {
    tryCatch({
      agnostic_models[[fmt]] <- load_agnostic_model(fmt)
      cli::cli_alert_success("Loaded agnostic {toupper(fmt)} model")
    }, error = function(e) {
      cli::cli_alert_warning("Agnostic {toupper(fmt)} model not found: {e$message}")
      cli::cli_alert_info("Run 01_train_agnostic_model.R to train agnostic models first")
    })
  }

  if (length(agnostic_models) == 0) {
    cli::cli_alert_warning("No agnostic models loaded, falling back to calibrated expectations")
    USE_MODEL <- FALSE
  } else {
    cli::cli_alert_info("Agnostic models predict 7-category outcomes using CONTEXT ONLY")
    cli::cli_alert_info("Features: over, ball, wickets, runs_diff, phase, innings, format, gender, knockout, tier")
  }
}

# 4. Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = FALSE)
# Note: Don't use on.exit() here - it causes issues when sourced with local=TRUE
cli::cli_alert_success("Connected to database")

# 5. Process Each Format ----
for (current_format in formats_to_process) {

cat("\n")
cli::cli_h1("{toupper(current_format)} Player Skill Index Calculation")
cat("\n")

# Build format-specific values
format_match_types <- FORMAT_GROUPS[[current_format]]
match_type_filter <- paste(sprintf("'%s'", tolower(format_match_types)), collapse = ", ")

# Get format-specific parameters
ALPHA <- get_skill_alpha_params(current_format, gender = "male")$alpha_max
START_VALUES <- get_skill_start_values(current_format)

# INDICES start at 0 (neutral deviation from context-expected)
START_SCORING <- START_VALUES$scoring_index    # 0 for batter scoring
START_ECONOMY <- START_VALUES$economy_index    # 0 for bowler economy

# RATES start at format average probability
START_SURVIVAL <- START_VALUES$survival_rate   # ~0.95 for batter survival
START_STRIKE <- START_VALUES$strike_rate       # ~0.05 for bowler strike

# Expected values (fallback when agnostic model unavailable)
FALLBACK_EXP_RUNS <- switch(current_format,
  "t20" = EXPECTED_RUNS_T20,
  "odi" = EXPECTED_RUNS_ODI,
  "test" = EXPECTED_RUNS_TEST,
  EXPECTED_RUNS_T20
)
FALLBACK_EXP_WICKET <- switch(current_format,
  "t20" = EXPECTED_WICKET_T20,
  "odi" = EXPECTED_WICKET_ODI,
  "test" = EXPECTED_WICKET_TEST,
  EXPECTED_WICKET_T20
)

cli::cli_alert_info("Alpha (learning rate): {ALPHA}")
cli::cli_alert_info("Starting scoring/economy index: {START_SCORING} (neutral)")
cli::cli_alert_info("Starting survival rate: {START_SURVIVAL}")
cli::cli_alert_info("Starting strike rate: {START_STRIKE}")

# Determine if agnostic model should be used for this format
use_model_for_format <- USE_MODEL && current_format %in% names(agnostic_models)
if (!use_model_for_format && USE_MODEL) {
  cli::cli_alert_info("Agnostic model not available for {toupper(current_format)}, using calibrated averages")
}

## 5.1 Check for existing data ----
cli::cli_h2("Checking existing data")

table_name <- paste0(current_format, "_player_skill")
existing_tables <- DBI::dbListTables(conn)

if (!table_name %in% existing_tables || FORCE_FULL) {
  mode <- "full"
  if (FORCE_FULL) {
    cli::cli_alert_info("FORCE_FULL = TRUE, running full calculation")
  } else {
    cli::cli_alert_info("Table does not exist, running full calculation")
  }
  create_format_skill_table(current_format, conn, overwrite = TRUE)
} else {
  # Check if we can do incremental
  last_processed <- get_last_processed_skill_delivery(current_format, conn)
  if (is.null(last_processed)) {
    mode <- "full"
    cli::cli_alert_info("No existing records, running full calculation")
    create_format_skill_table(current_format, conn, overwrite = TRUE)
  } else {
    mode <- "incremental"
    cli::cli_alert_success("Found existing data, running incremental update")
    cli::cli_alert_info("Last processed: {last_processed$match_date}")
  }
}

# 5.2 Load Deliveries ----
cli::cli_h2("Loading {toupper(current_format)} deliveries")
cli::cli_alert_info("This may take a few minutes for large datasets...")

# Build query with features needed for agnostic model
if (use_model_for_format) {
  # Need additional features for agnostic model prediction
  # Includes: is_knockout, event_tier from matches table
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
      cs.match_type,
      cs.batter_id,
      cs.bowler_id,
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
    WHERE cs.batter_id IS NOT NULL
      AND cs.bowler_id IS NOT NULL
  ", match_type_filter, match_type_filter)
} else {
  # Simple query without model features
  base_query <- sprintf("
    SELECT
      delivery_id,
      match_id,
      match_date,
      batter_id,
      bowler_id,
      runs_batter,
      is_wicket
    FROM cricsheet.deliveries
    WHERE LOWER(match_type) IN (%s)
      AND batter_id IS NOT NULL
      AND bowler_id IS NOT NULL
  ", match_type_filter)
}

# Add incremental filter if applicable
if (mode == "incremental" && !is.null(last_processed)) {
  base_query <- paste0(base_query, sprintf("
    AND match_date >= '%s'
  ", last_processed$match_date))
}

if (!is.null(MATCH_LIMIT)) {
  match_ids <- DBI::dbGetQuery(conn, sprintf("
    SELECT DISTINCT match_id FROM cricsheet.deliveries
    WHERE LOWER(match_type) IN (%s)
    ORDER BY match_date
    LIMIT %d
  ", match_type_filter, MATCH_LIMIT))$match_id

  placeholders <- paste(rep("?", length(match_ids)), collapse = ", ")
  query <- paste0(base_query, sprintf(" AND match_id IN (%s)", placeholders))
  query <- paste0(query, " ORDER BY match_date, delivery_id")
  deliveries <- DBI::dbGetQuery(conn, query, params = as.list(match_ids))
} else {
  query <- paste0(base_query, " ORDER BY match_date, delivery_id")
  deliveries <- DBI::dbGetQuery(conn, query)
}

setDT(deliveries)
cli::cli_alert_success("Query complete, processing {format(nrow(deliveries), big.mark=',')} rows")

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
  cli::cli_alert_info("Skill indices are up to date for {toupper(current_format)}")

  # Still show current stats and leaderboards
  cat("\n")
  cli::cli_h2("Current {toupper(current_format)} Skill Index Summary")

  stats <- get_format_skill_stats(current_format, conn)
  if (!is.null(stats)) {
    cli::cli_alert_info("Total records: {format(stats$total_records, big.mark = ',')}")
    cli::cli_alert_info("Unique batters: {stats$unique_batters}, Unique bowlers: {stats$unique_bowlers}")
    cli::cli_alert_info("Date range: {stats$first_date} to {stats$last_date}")
    cat(sprintf("\n  Mean Scoring: %.3f runs/ball | Mean Economy: %.3f runs/ball\n",
                stats$mean_batter_scoring, stats$mean_bowler_economy))
  }

  # Format-specific minimum thresholds for Top 10 display (need substantial sample)
  min_batter_balls <- switch(current_format,
    "t20" = 1000, "odi" = 2000, "test" = 3000, 1000)
  min_bowler_balls <- switch(current_format,
    "t20" = 1000, "odi" = 2000, "test" = 3000, 1000)

  # Top Batters (filter to men's cricket via matches table)
  cli::cli_h3("{toupper(current_format)} Top 10 Batters by Scoring Index")
  top_batters <- DBI::dbGetQuery(conn, sprintf("
    WITH mens_skill AS (
      SELECT DISTINCT s.batter_id, s.batter_scoring_index, s.batter_survival_rate,
             s.batter_balls_faced, s.match_date, s.delivery_id
      FROM %s s
      JOIN cricsheet.matches m ON s.match_id = m.match_id
      WHERE m.gender = 'male'
    ),
    latest AS (
      SELECT batter_id, batter_scoring_index, batter_survival_rate, batter_balls_faced,
             ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM mens_skill
    )
    SELECT batter_id, batter_scoring_index as scoring, batter_survival_rate as survival, batter_balls_faced as balls
    FROM latest WHERE rn = 1 AND batter_balls_faced >= %d
    ORDER BY batter_scoring_index DESC LIMIT 10
  ", table_name, min_batter_balls))

  if (nrow(top_batters) > 0) {
    cat("\n")
    for (i in seq_len(nrow(top_batters))) {
      b <- top_batters[i, ]
      cli::cli_alert_info("{i}. {b$batter_id}: {round(b$scoring, 2)} runs/ball, {round(b$survival * 100, 1)}% survival ({format(b$balls, big.mark=',')} balls)")
    }
  }

  # Top Bowlers (filter to men's cricket via matches table)
  cli::cli_h3("{toupper(current_format)} Top 10 Bowlers by Economy Index")
  top_bowlers <- DBI::dbGetQuery(conn, sprintf("
    WITH mens_skill AS (
      SELECT DISTINCT s.bowler_id, s.bowler_economy_index, s.bowler_strike_rate,
             s.bowler_balls_bowled, s.match_date, s.delivery_id
      FROM %s s
      JOIN cricsheet.matches m ON s.match_id = m.match_id
      WHERE m.gender = 'male'
    ),
    latest AS (
      SELECT bowler_id, bowler_economy_index, bowler_strike_rate, bowler_balls_bowled,
             ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
      FROM mens_skill
    )
    SELECT bowler_id, bowler_economy_index as economy, bowler_strike_rate as strike, bowler_balls_bowled as balls
    FROM latest WHERE rn = 1 AND bowler_balls_bowled >= %d
    ORDER BY bowler_economy_index ASC LIMIT 10
  ", table_name, min_bowler_balls))

  if (nrow(top_bowlers) > 0) {
    cat("\n")
    for (i in seq_len(nrow(top_bowlers))) {
      b <- top_bowlers[i, ]
      cli::cli_alert_info("{i}. {b$bowler_id}: {round(b$economy, 2)} runs/ball, {round(b$strike * 100, 1)}% strike rate ({format(b$balls, big.mark=',')} balls)")
    }
  }

  cat("\n")
  next
}

# 5.3 Prepare agnostic model predictions ----
agnostic_exp_runs <- NULL
agnostic_exp_wicket <- NULL

if (use_model_for_format && current_format %in% names(agnostic_models)) {
  cli::cli_h2("Generating agnostic model predictions")
  cli::cli_alert_info("Using CONTEXT-ONLY baseline (no player/team/venue identity)")
  cli::cli_alert_info("Predicting for {format(n_deliveries, big.mark=',')} deliveries...")

  # Get the agnostic model for this format
  agnostic_model <- agnostic_models[[current_format]]

  # Convert data.table to data.frame for predict function
  # The predict_agnostic_outcome function handles feature preparation internally
  delivery_df <- as.data.frame(deliveries)

  # Generate predictions using the agnostic model functions
  pred_start <- Sys.time()
  probs <- predict_agnostic_outcome(agnostic_model, delivery_df, current_format)

  # Extract expected runs and wicket probability
  agnostic_exp_runs <- get_agnostic_expected_runs(probs)
  agnostic_exp_wicket <- get_agnostic_expected_wicket(probs)
  pred_time <- round(difftime(Sys.time(), pred_start, units = "secs"), 1)

  cli::cli_alert_success("Generated agnostic predictions for {format(n_deliveries, big.mark=',')} deliveries ({pred_time}s)")
  cli::cli_alert_info("Mean expected runs (agnostic): {round(mean(agnostic_exp_runs), 3)}")
  cli::cli_alert_info("Mean expected wicket (agnostic): {round(mean(agnostic_exp_wicket) * 100, 2)}%")
  cli::cli_alert_info("Actual mean runs: {round(mean(deliveries$runs_batter), 3)}")
  cli::cli_alert_info("Actual wicket rate: {round(mean(deliveries$is_wicket) * 100, 2)}%")

  rm(probs, delivery_df)  # Free memory
}

# 5.4 Initialize Player Skill State ----
cli::cli_h2("Initializing player skill state")

all_batters <- unique(deliveries$batter_id)
all_bowlers <- unique(deliveries$bowler_id)

cli::cli_alert_info("{length(all_batters)} unique batters, {length(all_bowlers)} unique bowlers in new data")

if (mode == "incremental") {
  cli::cli_alert_info("Loading existing player skill state...")
  player_state <- get_all_player_skill_state(current_format, conn)

  if (!is.null(player_state)) {
    batter_scoring <- player_state$batter_scoring
    batter_survival <- player_state$batter_survival
    batter_balls <- player_state$batter_balls
    bowler_economy <- player_state$bowler_economy
    bowler_strike <- player_state$bowler_strike
    bowler_balls <- player_state$bowler_balls

    # Initialize new players
    new_batters <- 0
    for (p in all_batters) {
      if (!exists(p, envir = batter_scoring)) {
        batter_scoring[[p]] <- START_SCORING    # 0 = neutral scoring deviation
        batter_survival[[p]] <- START_SURVIVAL  # Format average survival rate
        batter_balls[[p]] <- 0L
        new_batters <- new_batters + 1
      }
    }

    new_bowlers <- 0
    for (p in all_bowlers) {
      if (!exists(p, envir = bowler_economy)) {
        bowler_economy[[p]] <- START_ECONOMY    # 0 = neutral economy deviation
        bowler_strike[[p]] <- START_STRIKE      # Format average strike rate
        bowler_balls[[p]] <- 0L
        new_bowlers <- new_bowlers + 1
      }
    }

    cli::cli_alert_success("Loaded state, initialized {new_batters} new batters and {new_bowlers} new bowlers")
  } else {
    mode <- "full"
    cli::cli_alert_warning("Could not load state, falling back to full calculation")
  }
}

if (mode == "full") {
  # Fresh state
  batter_scoring <- new.env(hash = TRUE)
  batter_survival <- new.env(hash = TRUE)
  batter_balls <- new.env(hash = TRUE)
  bowler_economy <- new.env(hash = TRUE)
  bowler_strike <- new.env(hash = TRUE)
  bowler_balls <- new.env(hash = TRUE)

  for (p in all_batters) {
    batter_scoring[[p]] <- START_SCORING    # 0 = neutral scoring deviation
    batter_survival[[p]] <- START_SURVIVAL  # Format average survival rate
    batter_balls[[p]] <- 0L
  }

  for (p in all_bowlers) {
    bowler_economy[[p]] <- START_ECONOMY    # 0 = neutral economy deviation
    bowler_strike[[p]] <- START_STRIKE      # Format average strike rate
    bowler_balls[[p]] <- 0L
  }

  cli::cli_alert_success("Initialized {length(all_batters)} batters and {length(all_bowlers)} bowlers")
}

# 5.5 Pre-extract columns ----
delivery_ids <- deliveries$delivery_id
match_ids <- deliveries$match_id
match_dates <- deliveries$match_date
batter_ids <- deliveries$batter_id
bowler_ids <- deliveries$bowler_id
runs_vec <- deliveries$runs_batter
wicket_vec <- deliveries$is_wicket

# 5.6 Process deliveries ----
cli::cli_h2("Processing {format(n_deliveries, big.mark=',')} deliveries")
processing_start <- Sys.time()

result_mat <- matrix(NA_real_, nrow = n_deliveries, ncol = 10)
colnames(result_mat) <- c(
  "batter_scoring", "batter_survival",
  "bowler_economy", "bowler_strike",
  "exp_runs", "exp_wicket",
  "actual_runs", "is_wicket",
  "batter_balls", "bowler_balls"
)

cli::cli_progress_bar("Calculating skill indices", total = n_deliveries)

for (i in seq_len(n_deliveries)) {
  batter_id <- batter_ids[i]
  bowler_id <- bowler_ids[i]
  runs <- runs_vec[i]
  is_wicket <- wicket_vec[i]

  # Get current skill indices (BEFORE this delivery)
  bat_scoring <- batter_scoring[[batter_id]]
  bat_survival <- batter_survival[[batter_id]]
  bat_balls <- batter_balls[[batter_id]]

  bowl_economy <- bowler_economy[[bowler_id]]
  bowl_strike <- bowler_strike[[bowler_id]]
  bowl_balls <- bowler_balls[[bowler_id]]

  # Get expected values from agnostic model (context-only baseline)
  if (use_model_for_format && !is.null(agnostic_exp_runs)) {
    # Use agnostic model predictions as baseline (NO player/team/venue adjustment)
    # This is the pure context-expected outcome
    exp_runs <- agnostic_exp_runs[i]
    exp_wicket <- agnostic_exp_wicket[i]
  } else {
    # Fallback: use calibrated format averages
    exp_runs <- FALLBACK_EXP_RUNS
    exp_wicket <- FALLBACK_EXP_WICKET
  }

  # Store current state (BEFORE update)
  result_mat[i, ] <- c(
    bat_scoring, bat_survival,
    bowl_economy, bowl_strike,
    exp_runs, exp_wicket,
    runs, is_wicket,
    bat_balls, bowl_balls
  )

  # Update skill indices using PROPER EMA formulas
  # Indices: residual-based EMA → converges to player's true deviation from expected
  # Rates: absolute EMA → converges to player's true probability

  # Calculate values for update
  runs_residual <- runs - exp_runs                    # How much above/below expected runs
  survived <- if (is_wicket) 0 else 1                 # Binary survival outcome
  wicket_val <- if (is_wicket) 1 else 0               # Binary wicket outcome

  # Batter scoring INDEX: residual-based EMA (deviation from expected)
  # Converges to player's true scoring deviation (e.g., +0.5 = scores 0.5 runs/ball above expected)
  new_bat_scoring <- (1 - ALPHA) * bat_scoring + ALPHA * runs_residual
  batter_scoring[[batter_id]] <- new_bat_scoring

  # Batter survival RATE: absolute EMA on survival outcome (0/1)
  # Converges to player's true survival probability (e.g., 0.96 = survives 96% of balls)
  new_bat_survival <- (1 - ALPHA) * bat_survival + ALPHA * survived
  batter_survival[[batter_id]] <- new_bat_survival

  # Increment batter ball count
  batter_balls[[batter_id]] <- bat_balls + 1L

  # Bowler economy INDEX: residual-based EMA (deviation from expected)
  # Converges to bowler's true economy deviation (e.g., -0.3 = concedes 0.3 runs/ball less than expected)
  new_bowl_economy <- (1 - ALPHA) * bowl_economy + ALPHA * runs_residual
  bowler_economy[[bowler_id]] <- new_bowl_economy

  # Bowler strike RATE: absolute EMA on wicket outcome (0/1)
  # Converges to bowler's true wicket probability (e.g., 0.04 = takes wicket on 4% of balls)
  new_bowl_strike <- (1 - ALPHA) * bowl_strike + ALPHA * wicket_val
  bowler_strike[[bowler_id]] <- new_bowl_strike

  # Increment bowler ball count
  bowler_balls[[bowler_id]] <- bowl_balls + 1L

  if (i %% 5000 == 0) {
    cli::cli_progress_update(set = i)
  }
}

cli::cli_progress_done()
processing_time <- round(difftime(Sys.time(), processing_start, units = "mins"), 1)
cli::cli_alert_success("Skill indices calculated in {processing_time} minutes")

# 5.7 Build result data.table ----
cli::cli_h2("Building output data")

result_dt <- data.table(
  delivery_id = delivery_ids,
  match_id = match_ids,
  match_date = match_dates,
  batter_id = batter_ids,
  bowler_id = bowler_ids,
  batter_scoring_index = result_mat[, 1],
  batter_survival_rate = result_mat[, 2],
  bowler_economy_index = result_mat[, 3],
  bowler_strike_rate = result_mat[, 4],
  exp_runs = result_mat[, 5],
  exp_wicket = result_mat[, 6],
  actual_runs = as.integer(result_mat[, 7]),
  is_wicket = as.logical(result_mat[, 8]),
  batter_balls_faced = as.integer(result_mat[, 9]),
  bowler_balls_bowled = as.integer(result_mat[, 10])
)

# 5.8 Batch insert ----
cli::cli_h2("Inserting to database")

n_batches <- ceiling(n_deliveries / BATCH_SIZE)
cli::cli_progress_bar("Inserting batches", total = n_batches)

for (b in seq_len(n_batches)) {
  start_idx <- (b - 1) * BATCH_SIZE + 1
  end_idx <- min(b * BATCH_SIZE, n_deliveries)

  batch_df <- as.data.frame(result_dt[start_idx:end_idx])
  insert_format_skills(batch_df, current_format, conn)

  cli::cli_progress_update()
}

cli::cli_progress_done()

cli::cli_alert_success("Inserted {format(n_deliveries, big.mark = ',')} deliveries")

# 5.9 Store parameters ----
cli::cli_h2("Storing parameters")

# Ensure table exists
if (!"skill_calculation_params" %in% DBI::dbListTables(conn)) {
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS skill_calculation_params (
      format VARCHAR PRIMARY KEY,
      alpha DOUBLE,
      start_runs DOUBLE,
      start_survival DOUBLE,
      use_model BOOLEAN,
      last_delivery_id VARCHAR,
      last_match_date DATE,
      total_deliveries INTEGER,
      calculated_at TIMESTAMP
    )
  ")
}

last_row <- result_dt[.N]
DBI::dbExecute(conn, "DELETE FROM skill_calculation_params WHERE format = ?",
               params = list(current_format))
DBI::dbExecute(conn, "
  INSERT INTO skill_calculation_params
  (format, alpha, start_runs, start_survival, last_delivery_id, last_match_date, total_deliveries, calculated_at)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
", params = list(
  current_format, ALPHA, START_SCORING, START_SURVIVAL,
  last_row$delivery_id, as.character(last_row$match_date),
  n_deliveries, Sys.time()
))

cli::cli_alert_success("Parameters stored for future incremental updates")

# 5.10 Summary Statistics ----
cat("\n")
cli::cli_h2("{toupper(current_format)} Skill Index Summary")

stats <- get_format_skill_stats(current_format, conn)

if (!is.null(stats)) {
  cli::cli_alert_info("Total records: {format(stats$total_records, big.mark = ',')}")
  cli::cli_alert_info("Unique batters: {stats$unique_batters}")
  cli::cli_alert_info("Unique bowlers: {stats$unique_bowlers}")
  cli::cli_alert_info("Date range: {stats$first_date} to {stats$last_date}")

  cat("\nMean Skill Indices:\n")
  cat(sprintf("  Batter Scoring Index:  %.3f runs/ball\n", stats$mean_batter_scoring))
  cat(sprintf("  Bowler Economy Index:  %.3f runs/ball\n", stats$mean_bowler_economy))
  cat(sprintf("  Batter Survival Rate:  %.3f (%.1f%% dismissal rate)\n",
              stats$mean_batter_survival, (1 - stats$mean_batter_survival) * 100))
  cat(sprintf("  Bowler Strike Rate:    %.4f (%.1f%% of balls)\n",
              stats$mean_bowler_strike, stats$mean_bowler_strike * 100))

  cat("\nCalibration Check:\n")
  cat(sprintf("  Expected runs (avg):  %.3f\n", stats$mean_exp_runs))
  cat(sprintf("  Actual runs (avg):    %.3f\n", stats$mean_actual_runs))
  cat(sprintf("  Difference:           %+.3f\n", stats$mean_actual_runs - stats$mean_exp_runs))
  cat(sprintf("  Actual wicket rate:   %.3f (%.1f%%)\n",
              stats$actual_wicket_rate, stats$actual_wicket_rate * 100))

  # Warn if calibration drift is significant (>5% of actual)
  calibration_diff <- abs(stats$mean_actual_runs - stats$mean_exp_runs)
  calibration_pct <- calibration_diff / stats$mean_actual_runs * 100
  if (calibration_pct > 5) {
    cli::cli_alert_warning("Calibration drift detected: {round(calibration_pct, 1)}% difference between expected and actual runs")
    cli::cli_alert_info("Consider retraining the agnostic model (01_train_agnostic_model.R)")
  } else {
    cli::cli_alert_success("Calibration within acceptable range ({round(calibration_pct, 1)}% drift)")
  }

  if (use_model_for_format) {
    cli::cli_alert_success("Used AGNOSTIC model for context-only baseline (no player/team/venue)")
    cli::cli_alert_info("Skill indices represent deviation from context-expected performance")
  } else {
    cli::cli_alert_info("Used calibrated averages for baseline expectations")
  }
}

# 5.11 Format Complete ----
cat("\n")
cli::cli_alert_success("{toupper(current_format)} Skill Index calculation complete!")

# Format-specific minimum thresholds for meaningful sample sizes
min_batter_balls <- switch(current_format,
  "t20" = 200,
  "odi" = 500,
  "test" = 1000,
  200  # default
)
min_bowler_balls <- switch(current_format,
  "t20" = 300,
  "odi" = 600,
  "test" = 1500,
  300  # default
)

# Show top players for this format (men's cricket only for cleaner leaderboards)
cli::cli_h3("{toupper(current_format)} Top Batters by Scoring Index (min {min_batter_balls} balls)")
top_batters <- DBI::dbGetQuery(conn, sprintf("
  WITH mens_skill AS (
    SELECT DISTINCT s.batter_id, s.batter_scoring_index, s.batter_survival_rate,
           s.batter_balls_faced, s.match_date, s.delivery_id
    FROM %s s
    JOIN cricsheet.matches m ON s.match_id = m.match_id
    WHERE m.gender = 'male'
  ),
  latest AS (
    SELECT batter_id, batter_scoring_index, batter_survival_rate, batter_balls_faced,
           ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as rn
    FROM mens_skill
  )
  SELECT batter_id, batter_scoring_index as scoring, batter_survival_rate as survival, batter_balls_faced as balls
  FROM latest
  WHERE rn = 1 AND batter_balls_faced >= %d
  ORDER BY batter_scoring_index DESC
  LIMIT 10
", table_name, min_batter_balls))

cat("\n")
for (i in seq_len(nrow(top_batters))) {
  b <- top_batters[i, ]
  cli::cli_alert_info("{i}. {b$batter_id}: {round(b$scoring, 2)} runs/ball, {round(b$survival * 100, 1)}% survival ({format(b$balls, big.mark=',')} balls)")
}

cli::cli_h3("{toupper(current_format)} Top Bowlers by Economy Index (min {min_bowler_balls} balls)")
top_bowlers <- DBI::dbGetQuery(conn, sprintf("
  WITH mens_skill AS (
    SELECT DISTINCT s.bowler_id, s.bowler_economy_index, s.bowler_strike_rate,
           s.bowler_balls_bowled, s.match_date, s.delivery_id
    FROM %s s
    JOIN cricsheet.matches m ON s.match_id = m.match_id
    WHERE m.gender = 'male'
  ),
  latest AS (
    SELECT bowler_id, bowler_economy_index, bowler_strike_rate, bowler_balls_bowled,
           ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as rn
    FROM mens_skill
  )
  SELECT bowler_id, bowler_economy_index as economy, bowler_strike_rate as strike, bowler_balls_bowled as balls
  FROM latest
  WHERE rn = 1 AND bowler_balls_bowled >= %d
  ORDER BY bowler_economy_index ASC
  LIMIT 10
", table_name, min_bowler_balls))

cat("\n")
for (i in seq_len(nrow(top_bowlers))) {
  b <- top_bowlers[i, ]
  cli::cli_alert_info("{i}. {b$bowler_id}: {round(b$economy, 2)} runs/ball, {round(b$strike * 100, 1)}% strike rate ({format(b$balls, big.mark=',')} balls)")
}

cat("\n")

}  # End of format loop

# 6. Final Summary ----
cat("\n")
cli::cli_h1("All Formats Complete")
cli::cli_alert_success("Processed formats: {paste(toupper(formats_to_process), collapse = ', ')}")
cat("\n")

cli::cli_h3("Skill Index Interpretation (Residual-Based)")
cli::cli_bullets(c(
  "i" = "BSI > 0 means batter scores MORE than context-expected (good batter)",
  "i" = "BSI < 0 means batter scores LESS than context-expected (poor batter)",
  "i" = "BEI > 0 means bowler concedes MORE than context-expected (poor economy)",
  "i" = "BEI < 0 means bowler concedes LESS than context-expected (good economy)",
  "i" = "Indices are relative to AGNOSTIC baseline (context-only, no player/team/venue)"
))
cat("\n")

cli::cli_h3("Pipeline Dependencies")
cli::cli_bullets(c(
  "!" = "REQUIRES: Run 01_train_agnostic_model.R first to train agnostic models",
  "i" = "Skill indices stored in {{format}}_player_skill tables (t20, odi, test)",
  "i" = "Re-run this script for incremental updates (new matches)",
  "i" = "Join to deliveries via delivery_id"
))
cat("\n")

cli::cli_h3("Query Example")
cat("
SELECT d.*, s.batter_scoring_index, s.batter_survival_rate,
       s.bowler_economy_index, s.bowler_strike_rate,
       s.exp_runs, s.actual_runs,
       (s.actual_runs - s.exp_runs) as runs_residual
FROM cricsheet.deliveries d
JOIN t20_player_skill s ON d.delivery_id = s.delivery_id
WHERE LOWER(d.match_type) IN ('t20', 'it20')
LIMIT 10
")
cat("\n")
