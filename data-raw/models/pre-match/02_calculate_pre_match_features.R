# Calculate Pre-Match Features (FAST VERSION) ----
#
# This script calculates pre-match features for matches globally.
# Features use only data available BEFORE each match starts.
#
# OPTIMIZED: Uses bulk data loading + data.table for ~100x speedup
#
# Includes:
#   - Team ELO (result + roster-based)
#   - Player skill indices (batting/bowling aggregates)
#   - Form (last 5 matches)
#   - Head-to-head record
#   - Venue characteristics
#   - Toss information
#
# Output:
#   - pre_match_features table populated in DuckDB
#   - bouncerdata/models/{format}_prediction_features.rds

# 1. Setup ----
library(DBI)
library(dplyr)
library(data.table)
if (!("bouncer" %in% loadedNamespaces())) devtools::load_all()

# Check for optional parallel support
USE_PARALLEL <- requireNamespace("furrr", quietly = TRUE) &&
                requireNamespace("future", quietly = TRUE)
if (USE_PARALLEL) {
  library(furrr)
  library(future)
}


# 2. Configuration ----
EVENT_FILTER <- NULL
FORMAT_FILTER <- NULL  # NULL for all formats, or "t20", "odi", "test"

# Format groupings (match_type values in database)
FORMAT_GROUPS <- list(
  t20 = c("T20", "IT20"),
  odi = c("ODI", "ODM"),
  test = c("Test", "MDM")
)

BURNIN_YEARS <- 2005:2007
TEST_YEARS <- 2024:2025
N_WORKERS <- parallel::detectCores() - 2  # For parallel processing

# Output directory
output_dir <- file.path("..", "bouncerdata", "models")
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# Determine formats to process
if (is.null(FORMAT_FILTER)) {
  formats_to_process <- names(FORMAT_GROUPS)
  cli::cli_alert_info("Processing all formats: {paste(toupper(formats_to_process), collapse = ', ')}")
} else {
  formats_to_process <- FORMAT_FILTER
  cli::cli_alert_info("Processing single format: {toupper(FORMAT_FILTER)}")
}

cat("\n")
cli::cli_h1("Pre-Match Feature Calculation (Fast Version)")
cat("\n")

# 3. Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = TRUE)  # Read-only for bulk loading
cli::cli_alert_success("Connected to database")

# 4. Bulk Load All Data ----
cli::cli_h2("Bulk loading data into memory")

start_time <- Sys.time()

# Build format filter for SQL
format_msg <- if (is.null(FORMAT_FILTER)) "ALL" else toupper(FORMAT_FILTER)

# Load all matches (filter by format if specified) ----
cli::cli_alert_info("Loading {format_msg} matches...")

if (is.null(FORMAT_FILTER)) {
  # Load all formats
  matches_query <- "
    SELECT
      match_id, match_date, match_type, gender, team_type, season, event_name, venue,
      team1, team2, outcome_winner, toss_winner, toss_decision,
      event_match_number, event_group,
      unified_margin
    FROM matches
    WHERE gender IS NOT NULL
    ORDER BY match_date, match_id
  "
  all_matches_dt <- as.data.table(DBI::dbGetQuery(conn, matches_query))
} else {
  match_types <- FORMAT_GROUPS[[FORMAT_FILTER]]
  placeholders <- paste(rep("?", length(match_types)), collapse = ", ")
  matches_query <- sprintf("
    SELECT
      match_id, match_date, match_type, gender, team_type, season, event_name, venue,
      team1, team2, outcome_winner, toss_winner, toss_decision,
      event_match_number, event_group,
      unified_margin
    FROM matches
    WHERE LOWER(match_type) IN (%s)
      AND gender IS NOT NULL
    ORDER BY match_date, match_id
  ", paste(sprintf("'%s'", tolower(match_types)), collapse = ", "))
  all_matches_dt <- as.data.table(DBI::dbGetQuery(conn, matches_query))
}

all_matches_dt[, match_date := as.Date(match_date)]
cli::cli_alert_success("Loaded {nrow(all_matches_dt)} {format_msg} matches")

# Load all team_elo data ----
cli::cli_alert_info("Loading team ELO data...")
team_elo_dt <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT team_id, match_id, match_date, match_type, elo_result, elo_roster_combined
  FROM team_elo
"))
team_elo_dt[, match_date := as.Date(match_date)]
setkey(team_elo_dt, team_id, match_date)
cli::cli_alert_success("Loaded {nrow(team_elo_dt)} team ELO records")

# Load player participation for roster inference ----
cli::cli_alert_info("Loading player participation data...")
player_participation_dt <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT DISTINCT
    match_id, match_date, match_type, batting_team, bowling_team, batter_id, bowler_id
  FROM deliveries
"))
player_participation_dt[, match_date := as.Date(match_date)]
cli::cli_alert_success("Loaded player participation data")

# Load venue stats ----
cli::cli_alert_info("Loading venue statistics...")
venue_stats_dt <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT
    m.venue,
    m.match_date,
    m.match_type,
    m.outcome_winner,
    mi.innings,
    mi.total_runs,
    mi.batting_team
  FROM matches m
  JOIN match_innings mi ON m.match_id = mi.match_id
  WHERE m.outcome_winner IS NOT NULL
    AND m.outcome_winner != ''
"))
venue_stats_dt[, match_date := as.Date(match_date)]
cli::cli_alert_success("Loaded venue data")

load_time <- difftime(Sys.time(), start_time, units = "secs")
cli::cli_alert_success("Data loading complete in {round(load_time, 1)} seconds")

# 5. Calculate Features for Each Format ----
# Helper functions (get_team_elo_fast, get_team_form_fast, get_h2h_fast,
# get_venue_features_fast, get_team_skills_fast, calc_match_features,
# detect_knockout_match) are imported from package (R/pre_match_features.R)

# Results storage
all_format_results <- list()

# Build home lookups for neutral venue detection (once, before format loop)
cli::cli_alert_info("Building home venue lookups...")
home_lookups <- build_home_lookups(all_matches_dt)
cli::cli_alert_success("Built home lookups: {length(home_lookups$club_home)} club teams, {length(home_lookups$venue_country)} venues")

# Need write connection for storing (get it once before format loop)
DBI::dbDisconnect(conn, shutdown = TRUE)
# Fully shutdown DuckDB to release file lock before reconnecting
duckdb::duckdb_shutdown(duckdb::duckdb())
conn <- get_db_connection(read_only = FALSE)
add_skill_columns_to_features(conn = conn)

# Process each format
for (current_format in formats_to_process) {
  cat("\n")
  cli::cli_h1("Format: {toupper(current_format)}")
  cat("\n")

  # Get format-specific match types
  format_match_types <- FORMAT_GROUPS[[current_format]]

  # Filter matches for this format
  matches_dt <- all_matches_dt[toupper(match_type) %in% toupper(format_match_types)]

  if (nrow(matches_dt) < 10) {
    cli::cli_alert_warning("Only {nrow(matches_dt)} matches for {current_format} - skipping")
    next
  }

  # Create composite team IDs for this format
  # Uses make_team_id_vec() from R/team_ids.R for consistent snake_case IDs
  matches_dt[, `:=`(
    format = current_format,
    team1_id = make_team_id_vec(team1, gender, current_format, team_type),
    team2_id = make_team_id_vec(team2, gender, current_format, team_type),
    outcome_winner_id = ifelse(
      is.na(outcome_winner) | outcome_winner == "",
      NA_character_,
      make_team_id_vec(outcome_winner, gender, current_format, team_type)
    )
  )]

  cli::cli_alert_success("Processing {nrow(matches_dt)} {toupper(current_format)} matches")

  # Filter team ELO for this format
  format_team_elo_dt <- team_elo_dt[filter_team_ids_by_format(team_id, current_format)]
  cli::cli_alert_info("Team ELO records: {nrow(format_team_elo_dt)}")

  # Filter player participation for this format
  format_participation_dt <- player_participation_dt[toupper(match_type) %in% toupper(format_match_types)]

  # Filter venue stats for this format
  format_venue_stats_dt <- venue_stats_dt[toupper(match_type) %in% toupper(format_match_types)]

  # Load format-specific team skills (per-delivery skills)
  team_skill_table <- paste0(current_format, "_team_skill")
  if (team_skill_table %in% DBI::dbListTables(conn)) {
    cli::cli_alert_info("Loading team skill data from {team_skill_table}...")
    format_team_skill_dt <- as.data.table(DBI::dbGetQuery(conn, sprintf("
      WITH latest AS (
        SELECT
          batting_team_id, bowling_team_id, match_date,
          batting_team_runs_skill, batting_team_wicket_skill,
          bowling_team_runs_skill, bowling_team_wicket_skill,
          ROW_NUMBER() OVER (PARTITION BY batting_team_id ORDER BY match_date DESC, delivery_id DESC) as rn
        FROM %s
      )
      SELECT * FROM latest WHERE rn = 1
    ", team_skill_table)))
    format_team_skill_dt[, match_date := as.Date(match_date)]
    cli::cli_alert_success("Loaded skills for {nrow(format_team_skill_dt)} teams")
  } else {
    cli::cli_alert_warning("Team skill table {team_skill_table} not found - using defaults")
    format_team_skill_dt <- data.table(batting_team_id = character())
  }

  # Load format-specific venue skills
  venue_skill_table <- paste0(current_format, "_venue_skill")
  if (venue_skill_table %in% DBI::dbListTables(conn)) {
    cli::cli_alert_info("Loading venue skill data from {venue_skill_table}...")
    format_venue_skill_dt <- as.data.table(DBI::dbGetQuery(conn, sprintf("
      WITH latest AS (
        SELECT
          venue, match_date,
          venue_run_rate, venue_wicket_rate, venue_boundary_rate, venue_dot_rate,
          ROW_NUMBER() OVER (PARTITION BY venue ORDER BY match_date DESC, delivery_id DESC) as rn
        FROM %s
      )
      SELECT * FROM latest WHERE rn = 1
    ", venue_skill_table)))
    format_venue_skill_dt[, match_date := as.Date(match_date)]
    cli::cli_alert_success("Loaded skills for {nrow(format_venue_skill_dt)} venues")
  } else {
    cli::cli_alert_warning("Venue skill table {venue_skill_table} not found - using defaults")
    format_venue_skill_dt <- data.table(venue = character())
  }

  # Load format-specific player skills
  skill_table <- paste0(current_format, "_player_skill")
  if (skill_table %in% DBI::dbListTables(conn)) {
    cli::cli_alert_info("Loading player skill data from {skill_table}...")
    player_skill_dt <- as.data.table(DBI::dbGetQuery(conn, sprintf("
      WITH latest AS (
        SELECT
          batter_id, bowler_id, match_date,
          batter_scoring_index, batter_survival_rate, batter_balls_faced,
          bowler_economy_index, bowler_strike_rate, bowler_balls_bowled,
          ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) as batter_rn,
          ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) as bowler_rn
        FROM %s
      )
      SELECT * FROM latest WHERE batter_rn = 1 OR bowler_rn = 1
    ", skill_table)))
    player_skill_dt[, match_date := as.Date(match_date)]

    batter_skills <- player_skill_dt[batter_rn == 1, .(
      player_id = batter_id,
      skill_date = match_date,
      scoring_index = batter_scoring_index,
      survival_rate = batter_survival_rate,
      balls_faced = batter_balls_faced
    )]
    setkey(batter_skills, player_id)

    bowler_skills <- player_skill_dt[bowler_rn == 1, .(
      player_id = bowler_id,
      skill_date = match_date,
      economy_index = bowler_economy_index,
      strike_rate = bowler_strike_rate,
      balls_bowled = bowler_balls_bowled
    )]
    setkey(bowler_skills, player_id)
    cli::cli_alert_success("Loaded skills for {nrow(batter_skills)} batters, {nrow(bowler_skills)} bowlers")
  } else {
    cli::cli_alert_warning("Skill table {skill_table} not found - using defaults")
    batter_skills <- data.table(player_id = character(), scoring_index = numeric())
    bowler_skills <- data.table(player_id = character(), economy_index = numeric())
  }

  # Filter to matches with outcomes
  matches_with_outcome <- matches_dt[!is.na(outcome_winner) & outcome_winner != ""]
  n_matches <- nrow(matches_with_outcome)
  cli::cli_alert_info("Processing {n_matches} matches with outcomes")

  # Process matches
  start_calc <- Sys.time()

  if (USE_PARALLEL && n_matches > 100) {
    cli::cli_alert_info("Using parallel processing with {N_WORKERS} workers...")
    plan(multisession, workers = N_WORKERS)

    features_list <- future_map(
      seq_len(n_matches),
      ~calc_match_features(.x, matches_with_outcome, format_team_elo_dt, batter_skills,
                           bowler_skills, format_participation_dt, format_venue_stats_dt,
                           format_team_skill_dt, format_venue_skill_dt,
                           home_lookups = home_lookups),
      .progress = TRUE,
      .options = furrr_options(seed = TRUE, packages = "bouncer")
    ) %>% bind_rows()

    plan(sequential)  # Reset
  } else {
    cli::cli_alert_info("Processing sequentially...")
    cli::cli_progress_bar("Calculating features", total = n_matches)

    results <- vector("list", n_matches)
    for (i in seq_len(n_matches)) {
      results[[i]] <- calc_match_features(
        i, matches_with_outcome, format_team_elo_dt, batter_skills,
        bowler_skills, format_participation_dt, format_venue_stats_dt,
        format_team_skill_dt, format_venue_skill_dt,
        home_lookups = home_lookups
      )
      if (i %% 100 == 0) cli::cli_progress_update(set = i)
    }
    cli::cli_progress_done()
    features_list <- bind_rows(results)
  }

  calc_time <- difftime(Sys.time(), start_calc, units = "secs")
  cli::cli_alert_success("Feature calculation complete in {round(calc_time, 1)} seconds")
  cli::cli_alert_success("Calculated features for {nrow(features_list)} matches")

  # Add margin features
  cli::cli_h2("Adding margin features")

  features_list <- features_list %>%
    left_join(
      matches_with_outcome[, .(match_id, unified_margin)],
      by = "match_id"
    ) %>%
    mutate(
      # Calculate expected margin from ELO difference
      # Uses get_expected_margin() from R/margin_calculation.R
      expected_margin = mapply(
        function(t1_elo, t2_elo, fmt) {
          t1 <- if (is.na(t1_elo)) 1500 else t1_elo
          t2 <- if (is.na(t2_elo)) 1500 else t2_elo
          get_expected_margin(t1, t2, home_advantage = 0, format = fmt)
        },
        team1_elo_result, team2_elo_result, current_format
      ),
      # Actual margin from the match
      actual_margin = unified_margin
    ) %>%
    select(-unified_margin)

  n_with_margin <- sum(!is.na(features_list$actual_margin))
  cli::cli_alert_success("Added margins: {n_with_margin}/{nrow(features_list)} matches have actual margin")
  if (n_with_margin > 0) {
    margin_mae <- mean(abs(features_list$expected_margin - features_list$actual_margin), na.rm = TRUE)
    cli::cli_alert_info("Expected margin MAE: {round(margin_mae, 1)} runs")
  }

  # Add outcome labels
  cli::cli_h2("Adding outcome labels")

  features_list <- features_list %>%
    left_join(
      matches_with_outcome[, .(match_id, season, outcome_winner)],
      by = "match_id"
    ) %>%
    mutate(
      team1_wins = as.integer(outcome_winner == team1),
      match_year = as.integer(format(as.Date(match_date), "%Y"))
    )

  cli::cli_alert_info("Team 1 win rate: {round(mean(features_list$team1_wins, na.rm = TRUE) * 100, 1)}%")

  # Create Train/Test Splits
  cli::cli_h2("Creating train/test splits")

  burnin_data <- features_list %>% filter(match_year %in% BURNIN_YEARS)
  train_data <- features_list %>% filter(!match_year %in% BURNIN_YEARS & !match_year %in% TEST_YEARS)
  test_data <- features_list %>% filter(match_year %in% TEST_YEARS)

  cli::cli_alert_info("Burn-in (skipped): {nrow(burnin_data)} matches")
  if (nrow(train_data) > 0) {
    cli::cli_alert_info("Train: {nrow(train_data)} matches (years: {min(train_data$match_year)}-{max(train_data$match_year)})")
  }
  if (nrow(test_data) > 0) {
    cli::cli_alert_info("Test: {nrow(test_data)} matches (years: {paste(unique(test_data$match_year), collapse = ', ')})")
  }

  # Store in Database
  cli::cli_h2("Storing features in database")

  match_ids <- features_list$match_id
  if (length(match_ids) > 0) {
    batch_size <- 500
    for (batch_start in seq(1, length(match_ids), by = batch_size)) {
      batch <- match_ids[batch_start:min(batch_start + batch_size - 1, length(match_ids))]
      placeholders <- paste(rep("?", length(batch)), collapse = ", ")
      DBI::dbExecute(conn,
        sprintf("DELETE FROM pre_match_features WHERE match_id IN (%s)", placeholders),
        params = as.list(batch)
      )
    }
  }

  features_for_db <- features_list %>%
    select(-any_of(c("season", "outcome_winner", "team1_wins", "match_year")))

  DBI::dbWriteTable(conn, "pre_match_features", features_for_db, append = TRUE)
  cli::cli_alert_success("Stored {nrow(features_for_db)} feature records in database")

  # Save to RDS
  cli::cli_h2("Saving feature data")

  output_path <- file.path(output_dir, paste0(current_format, "_prediction_features.rds"))
  saveRDS(list(
    all = features_list,
    burnin = burnin_data,
    train = train_data,
    test = test_data,
    config = list(
      event_filter = EVENT_FILTER,
      format = current_format,
      burnin_years = BURNIN_YEARS,
      test_years = TEST_YEARS,
      n_total = nrow(features_list),
      n_burnin = nrow(burnin_data),
      n_train = nrow(train_data),
      n_test = nrow(test_data),
      created_at = Sys.time()
    )
  ), output_path)

  cli::cli_alert_success("Saved feature data to {output_path}")

  # Store summary for final output
  all_format_results[[current_format]] <- list(
    n_matches = n_matches,
    n_train = nrow(train_data),
    n_test = nrow(test_data),
    calc_time = calc_time
  )
} # End format loop

# 6. Summary ----
total_time <- difftime(Sys.time(), start_time, units = "secs")
cat("\n")
cli::cli_h1("Final Summary")
cat("\n")

cat(sprintf("%-10s %12s %10s %10s %12s\n",
            "Format", "Matches", "Train", "Test", "Time (s)"))
cat(paste(rep("-", 60), collapse = ""), "\n")

for (fmt in names(all_format_results)) {
  res <- all_format_results[[fmt]]
  cat(sprintf("%-10s %12d %10d %10d %12.1f\n",
              toupper(fmt),
              res$n_matches,
              res$n_train,
              res$n_test,
              as.numeric(res$calc_time)))
}

cat("\n")
cli::cli_alert_success("Total time: {round(total_time, 1)} seconds")
cli::cli_alert_info("Data loading: {round(load_time, 1)}s")

formats_processed <- paste(toupper(formats_to_process), collapse = ", ")
cli::cli_alert_success("All formats processed: {formats_processed}!")

cat("\n")
cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "Run 03_train_prediction_model.R to train prediction models",
  "i" = "Feature files saved to: {output_dir}/[format]_prediction_features.rds"
))
cat("\n")
