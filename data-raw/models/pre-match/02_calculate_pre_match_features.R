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

# NOTE: Parallel disabled because calc_match_features() is internal (not exported).
# Workers can't access unexported functions via library(bouncer) or devtools::load_all().
# To re-enable: export calc_match_features + helper functions from the package.
USE_PARALLEL <- FALSE


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

# Output directory (use package helper to find the correct bouncerdata path)
bouncerdata_root <- find_bouncerdata_dir(create = FALSE)
if (is.null(bouncerdata_root)) {
  stop("Cannot locate bouncerdata/ directory. Run from within the bouncer/ workspace with bouncerdata/ as sibling.")
}
# PM_OUTPUT_DIR / PM_WRITE_DB let a trial rebuild go to a staging folder without
# touching the live feature files or the pre_match_features table.
output_dir <- Sys.getenv("PM_OUTPUT_DIR", file.path(bouncerdata_root, "models"))
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}
WRITE_DB <- !identical(tolower(Sys.getenv("PM_WRITE_DB", "true")), "false")

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
conn <- get_db_connection(read_only = !WRITE_DB)
if (WRITE_DB) add_skill_columns_to_features(conn = conn)
cli::cli_alert_success("Connected to database (write = {WRITE_DB})")

start_time <- Sys.time()

# 4. Match labels and margins (all formats) ----
# Inputs to calc_match_features() come from load_pre_match_inputs(), the same
# loader the live prediction job uses, so training and serving read identical
# tables. This block only adds what training needs on top: labels, season,
# margins and home lookups.
all_matches_dt <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT match_id, match_date, match_type, gender, team_type, season, venue,
         team1, team2, outcome_winner, unified_margin
  FROM cricsheet.matches
  WHERE gender IS NOT NULL
"))
all_matches_dt[, match_date := as.Date(match_date)]

# unified_margin is from the side that batted first (D-P72); the margin model
# predicts it from team1's side, so flip it when team2 batted first.
bat_first <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT match_id, batting_team AS bat_first
  FROM cricsheet.match_innings
  WHERE innings = 1 AND NOT COALESCE(is_super_over, FALSE)
"))
all_matches_dt <- merge(all_matches_dt, unique(bat_first, by = "match_id"), by = "match_id", all.x = TRUE)
all_matches_dt[, team1_margin := fifelse(bat_first == team1, unified_margin, -unified_margin)]

cli::cli_alert_info("Building home venue lookups...")
home_lookups <- build_home_lookups(all_matches_dt)

all_format_results <- list()

# 5. Calculate Features for Each Format ----
for (current_format in formats_to_process) {
  cat("\n")
  cli::cli_h1("Format: {toupper(current_format)}")
  cat("\n")

  inputs <- load_pre_match_inputs(conn, current_format)
  matches_dt <- inputs$matches_dt
  matches_dt[, season := NULL]
  matches_dt <- merge(matches_dt, all_matches_dt[, .(match_id, season, unified_margin, team1_margin)],
                      by = "match_id", all.x = TRUE)
  setorder(matches_dt, match_date, match_id)

  if (nrow(matches_dt) < 10) {
    cli::cli_alert_warning("Only {nrow(matches_dt)} matches for {current_format} - skipping")
    next
  }
  cli::cli_alert_success("Processing {nrow(matches_dt)} {toupper(current_format)} matches")

  # Filter to processable matches
  # Test: include draws (no outcome_winner) since they're 33% of matches
  # T20/ODI: exclude no-results (rain, etc.) since they're uninformative
  if (current_format == "test") {
    # Keep all Test matches except those with no outcome_type at all
    matches_with_outcome <- matches_dt[!is.na(match_date)]
    # outcome_type is 'normal', 'draw', or 'tie' for completed Tests
    n_draws <- sum(is.na(matches_with_outcome$outcome_winner_id) | matches_with_outcome$outcome_winner_id == "")
    cli::cli_alert_info("Including {n_draws} draws/no-results out of {nrow(matches_with_outcome)} Test matches")
  } else {
    matches_with_outcome <- matches_dt[!is.na(outcome_winner) & outcome_winner != ""]
  }
  n_matches <- nrow(matches_with_outcome)
  cli::cli_alert_info("Processing {n_matches} matches")

  # Process matches
  start_calc <- Sys.time()

  if (USE_PARALLEL && n_matches > 100) {
    cli::cli_alert_info("Using parallel processing with {N_WORKERS} workers...")
    plan(multisession, workers = N_WORKERS)

    features_list <- future_map(
      seq_len(n_matches),
      ~calc_match_features(.x, matches_with_outcome, inputs$team_elo_dt, inputs$batter_skills,
                           inputs$bowler_skills, inputs$participation_dt, inputs$venue_stats_dt,
                           inputs$team_skill_dt, inputs$venue_skill_dt,
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
        i, matches_with_outcome, inputs$team_elo_dt, inputs$batter_skills,
        inputs$bowler_skills, inputs$participation_dt, inputs$venue_stats_dt,
        inputs$team_skill_dt, inputs$venue_skill_dt,
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
      matches_with_outcome[, .(match_id, team1_margin)],
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
      # Actual margin, from team1's side (unified_margin is from the side
      # that batted first; before 2026-10-13 it was used unflipped)
      actual_margin = team1_margin
    ) %>%
    select(-team1_margin)

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
      # team1_wins: 1 = team1 won, 0 = team2 won, NA = draw/no result
      team1_wins = case_when(
        is.na(outcome_winner) | outcome_winner == "" ~ NA_integer_,  # draw
        outcome_winner == team1 ~ 1L,
        TRUE ~ 0L
      ),
      match_year = as.integer(base::format(as.Date(match_date), "%Y"))
    )

  n_draws <- sum(is.na(features_list$team1_wins))
  n_results <- sum(!is.na(features_list$team1_wins))
  cli::cli_alert_info("Results: {n_results} win/loss, {n_draws} draws/no-result")
  if (n_results > 0) {
    cli::cli_alert_info("Team 1 win rate (excl draws): {round(mean(features_list$team1_wins, na.rm = TRUE) * 100, 1)}%")
  }

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

  match_ids <- if (WRITE_DB) features_list$match_id else character(0)
  if (!WRITE_DB) cli::cli_alert_info("PM_WRITE_DB=false: not writing pre_match_features")
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

  if (WRITE_DB) {
    DBI::dbWriteTable(conn, "pre_match_features", features_for_db, append = TRUE)
    cli::cli_alert_success("Stored {nrow(features_for_db)} feature records in database")
  }

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

formats_processed <- paste(toupper(formats_to_process), collapse = ", ")
cli::cli_alert_success("All formats processed: {formats_processed}!")

cat("\n")
cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "Run 03_train_prediction_model.R to train prediction models",
  "i" = "Feature files saved to: {output_dir}/[format]_prediction_features.rds"
))
cat("\n")
