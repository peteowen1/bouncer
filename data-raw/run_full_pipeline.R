# Run Full Bouncer Pipeline ----
#
# Master script that runs the complete bouncer pipeline in the correct order.
# This ensures all dependencies are satisfied and data flows correctly.
#
# Pipeline Architecture:
#
#   STEP 1: install_all_bouncer_data()
#              │
#   STEP 1b: Backfill Unified Margin (converts wickets wins to runs-equivalent)
#              │
#   STEP 2: Train Agnostic Model ──────────────────────┐
#              │                                        │
#              ├──> STEP 3: Player Skill Indices ───────┤
#              │           (uses agnostic baseline)     │
#              ├──> STEP 4: Team Skill Indices ─────────┼──> STEP 7: Full Model
#              │           (uses agnostic baseline)     │
#              └──> STEP 5: Venue Skill Indices ────────┤
#                          (uses agnostic baseline)     │
#                                                       │
#   STEP 6: Team ELO (game-level) ─────────────────────┤
#                                                       │
#                                       ┌───────────────┘
#                                       v
#                       STEP 8: Pre-Match Features (with team/venue skills)
#                                       │
#                       STEP 9: Pre-Match Model Training
#                                       │
#                       STEP 10: Optimize Projection Params
#                                       │
#                       STEP 11: Calculate Per-Delivery Projections
#
# Usage:
#   source("data-raw/run_full_pipeline.R")
#
# Or run individual steps by setting STEPS_TO_RUN

# 0. Configuration ----
library(cli)
devtools::load_all()

# Get path to data-raw directory (DON'T change working directory - it breaks DB paths)
DATA_RAW_DIR <- file.path(getwd(), "data-raw")
if (!dir.exists(DATA_RAW_DIR)) {
  # Maybe we're already in data-raw
  if (file.exists("run_full_pipeline.R")) {
    DATA_RAW_DIR <- getwd()
  } else {
    stop("Cannot find data-raw directory. Run from bouncer/ directory.")
  }
}
cli::cli_alert_info("Data-raw directory: {DATA_RAW_DIR}")
cli::cli_alert_info("Working directory: {getwd()}")

## Main Configuration ----

# Which steps to run?
# - NULL = run all steps (1, 1b, 2, 3, 4, 5, 6, 7, 8, 9)
# - c(3:9) = run only steps 3 through 9
STEPS_TO_RUN <- NULL

# Fresh start mode - CLEARS ALL DATA and recalculates from scratch
# - TRUE = delete all skill/elo tables and recalculate everything (takes hours)
# - FALSE = incremental mode, only process new data since last run (fast)
FRESH_START <- FALSE # TRUE

# Per-step fresh start (allows fresh start for specific steps only)
# - NULL = use global FRESH_START for all steps
# - c(6) = fresh start only for step 6 (team ELO)
# - c(3, 4, 5) = fresh start for player, team, venue skill indices
# This is useful when you want incremental mode for most steps but need to
# recalculate specific steps from scratch (e.g., after changing parameters)
FRESH_STEPS <- c(6) # NULL

# Formats to process (don't change unless you have a specific reason)
FORMATS <- c("t20", "odi", "test")

## Advanced Configuration ----

# For development: limit matches for faster testing
# - NULL = process all matches
# - 100 = only process first 100 matches (for testing)
MATCH_LIMIT <- NULL

# Smart caching (only relevant when FRESH_START = FALSE)
# When enabled, steps may be skipped if data hasn't changed enough
SKIP_IF_NO_CHANGE <- TRUE
MODEL_RETRAIN_THRESHOLD <- 5000      # Min new deliveries before retraining models
PREMATCH_RETRAIN_THRESHOLD <- 100    # Min new matches before retraining pre-match model
FORCE_STEPS <- NULL                  # Force these steps to run even with smart caching, e.g., c(3, 4)

cat("\n")
cli::cli_h1("Bouncer Full Pipeline")

# Show configuration summary
if (FRESH_START) {
  cli::cli_alert_warning("Mode: FRESH START - All tables will be cleared and recalculated!")
} else {
  cli::cli_alert_info("Mode: Incremental - Only processing new data")
}

if (!is.null(STEPS_TO_RUN)) {
  cli::cli_alert_info("Steps: {paste(STEPS_TO_RUN, collapse = ', ')}")
} else {
  cli::cli_alert_info("Steps: All (1, 1b, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)")
}
cat("\n")

# Track timing
pipeline_start <- Sys.time()
step_times <- list()
skipped_steps <- character()

# Helper functions (now in R/pipeline_helpers.R, loaded via devtools::load_all())
should_run <- function(step) pipeline_should_run(step, STEPS_TO_RUN)
print_step_complete <- pipeline_step_complete

# Check if step should use fresh start (global or per-step)
is_fresh_start <- function(step) {
  # Global fresh start applies to all steps
  if (FRESH_START) return(TRUE)
  # Check per-step fresh start
  if (!is.null(FRESH_STEPS) && step %in% FRESH_STEPS) return(TRUE)
  return(FALSE)
}

# Smart skip helper - checks if step should be skipped due to no/minimal data changes
check_smart_skip <- function(step_name, step_num, conn,
                              delivery_threshold = 0,
                              match_threshold = 0) {
  # Skip check if smart caching disabled

  if (!SKIP_IF_NO_CHANGE) return(list(should_skip = FALSE))

  # Never skip forced steps
  if (!is.null(FORCE_STEPS) && step_num %in% FORCE_STEPS) {
    cli::cli_alert_info("Step {step_num} forced to run")
    return(list(should_skip = FALSE))
  }

  should_skip_step(step_name, conn,
                   delivery_threshold = delivery_threshold,
                   match_threshold = match_threshold)
}


# Step 1: Data Download ----

if (should_run(1)) {
  cli::cli_h1("Step 1: Data Download")

  # Check if database exists and has data
  db_path <- get_default_db_path()
  if (file.exists(db_path)) {
    conn <- get_db_connection(read_only = TRUE)
    n_matches <- DBI::dbGetQuery(conn, "SELECT COUNT(*) as n FROM matches")$n
    DBI::dbDisconnect(conn, shutdown = TRUE)  # Must shutdown to avoid lock conflicts

    if (n_matches > 0) {
      cli::cli_alert_success("Database exists with {format(n_matches, big.mark = ',')} matches")
      cli::cli_alert_info("Skipping data download. Run install_all_bouncer_data() manually if needed.")
    } else {
      cli::cli_alert_warning("Database exists but is empty")
      cli::cli_alert_info("Run install_all_bouncer_data() to populate")
    }
  } else {
    cli::cli_alert_warning("No database found at {db_path}")
    cli::cli_alert_info("Run install_all_bouncer_data() to create and populate")
  }

  step_times[["1_data"]] <- 0  # Not timed - manual step
}


# Step 1b: Margin Backfill ----

# Run margin backfill after data download (required for pre-match features)
# This calculates unified_margin for all matches (converts wickets wins to runs-equivalent)
{
  cli::cli_h1("Step 1b: Backfill Unified Margin")

  # Check if database exists and has data
  db_path <- get_default_db_path()
  if (file.exists(db_path)) {
    conn <- get_db_connection(read_only = TRUE)

    # Check how many matches need margin
    margin_check <- DBI::dbGetQuery(conn, "
      SELECT
        COUNT(*) as total,
        SUM(CASE WHEN unified_margin IS NOT NULL THEN 1 ELSE 0 END) as with_margin
      FROM matches
      WHERE outcome_type IS NOT NULL
    ")
    # Must use shutdown = TRUE here because the backfill script needs a write connection
    # and DuckDB on Windows has file locking issues with lingering driver instances
    DBI::dbDisconnect(conn, shutdown = TRUE)

    n_need_margin <- margin_check$total - margin_check$with_margin

    if (n_need_margin == 0) {
      cli::cli_alert_success("All {format(margin_check$total, big.mark = ',')} matches have unified_margin")
      cli::cli_alert_info("Skipping margin backfill")
    } else {
      cli::cli_alert_warning("{format(n_need_margin, big.mark = ',')} matches need margin calculation")
      step_start <- Sys.time()

      # Force shutdown any lingering DuckDB driver instances to avoid lock conflicts
      # This is necessary because DuckDB caches driver instances and can have issues
      # when switching between read-only and write connections
      tryCatch({
        duckdb::duckdb_shutdown(duckdb::duckdb())
      }, error = function(e) NULL)

      # Source the backfill script
      source(file.path(DATA_RAW_DIR, "ratings/00_backfill_unified_margin.R"), local = TRUE)

      step_times[["1b_margin"]] <- difftime(Sys.time(), step_start, units = "mins")
      print_step_complete("Step 1b: Margin Backfill", step_times[["1b_margin"]])
    }
  } else {
    cli::cli_alert_warning("No database found - skipping margin backfill")
  }
}


# Step 2: Agnostic Model ----

if (should_run(2)) {
  cli::cli_h1("Step 2: Train Agnostic Model")

  # Check smart skip (threshold: 5000 deliveries)
  conn <- get_db_connection(read_only = TRUE)
  skip_check <- check_smart_skip("agnostic_model", 2, conn,
                                  delivery_threshold = MODEL_RETRAIN_THRESHOLD)
  DBI::dbDisconnect(conn, shutdown = FALSE)

  if (skip_check$should_skip) {
    cli::cli_alert_info("Skipped: {skip_check$reason}")
    skipped_steps <- c(skipped_steps, "2_agnostic_model")
  } else {
    step_start <- Sys.time()
    cli::cli_alert_success("Running: {skip_check$reason}")

    # Source the training script
    source(file.path(DATA_RAW_DIR, "models/ball-outcome/01_train_agnostic_model.R"), local = TRUE)

    # Update pipeline state
    conn <- get_db_connection(read_only = FALSE)
    update_pipeline_state("agnostic_model", conn, status = "success")
    DBI::dbDisconnect(conn, shutdown = TRUE)

    step_times[["2_agnostic_model"]] <- difftime(Sys.time(), step_start, units = "mins")
    print_step_complete("Step 2: Agnostic Model", step_times[["2_agnostic_model"]])
  }
}


# Step 3: Player Skill Indices ----

if (should_run(3)) {
  cli::cli_h1("Step 3: Calculate Player Skill Indices")

  # If FRESH_START, clear skill tables first
  if (FRESH_START) {
    cli::cli_alert_warning("FRESH_START: Clearing player skill tables...")
    conn <- get_db_connection(read_only = FALSE)
    for (fmt in c("t20", "odi", "test")) {
      tbl <- paste0(fmt, "_player_skill")
      if (tbl %in% DBI::dbListTables(conn)) {
        DBI::dbExecute(conn, paste0("DELETE FROM ", tbl))
        cli::cli_alert_info("Cleared {tbl}")
      }
    }
    DBI::dbDisconnect(conn, shutdown = TRUE)
  }

  # Check smart skip (no threshold - already incremental, skip only if zero new data)
  conn <- get_db_connection(read_only = TRUE)
  skip_check <- check_smart_skip("player_skill", 3, conn, delivery_threshold = 0)
  DBI::dbDisconnect(conn, shutdown = TRUE)

  if (skip_check$should_skip && !FRESH_START) {
    cli::cli_alert_info("Skipped: {skip_check$reason}")
    skipped_steps <- c(skipped_steps, "3_player_skill")
  } else {
    step_start <- Sys.time()
    if (FRESH_START) {
      cli::cli_alert_success("Running FRESH calculation from scratch")
    } else {
      cli::cli_alert_success("Running incremental: {skip_check$reason}")
    }

    # Source the calculation script (already supports incremental)
    source(file.path(DATA_RAW_DIR, "ratings/player/03_calculate_skill_indices.R"), local = TRUE)

    # Update pipeline state
    conn <- get_db_connection(read_only = FALSE)
    update_pipeline_state("player_skill", conn, status = "success")
    DBI::dbDisconnect(conn, shutdown = TRUE)

    step_times[["3_player_skill"]] <- difftime(Sys.time(), step_start, units = "mins")
    print_step_complete("Step 3: Player Skill Indices", step_times[["3_player_skill"]])
  }
}


# Step 4: Team Skill Indices ----

if (should_run(4)) {
  cli::cli_h1("Step 4: Calculate Team Skill Indices")

  # If FRESH_START, clear team skill tables first
  if (FRESH_START) {
    cli::cli_alert_warning("FRESH_START: Clearing team skill tables...")
    conn <- get_db_connection(read_only = FALSE)
    for (fmt in c("t20", "odi", "test")) {
      tbl <- paste0(fmt, "_team_skill")
      if (tbl %in% DBI::dbListTables(conn)) {
        DBI::dbExecute(conn, paste0("DELETE FROM ", tbl))
        cli::cli_alert_info("Cleared {tbl}")
      }
    }
    DBI::dbDisconnect(conn, shutdown = TRUE)
  }

  # Check smart skip
  conn <- get_db_connection(read_only = TRUE)
  skip_check <- check_smart_skip("team_skill", 4, conn, delivery_threshold = 0)
  DBI::dbDisconnect(conn, shutdown = TRUE)

  if (skip_check$should_skip && !FRESH_START) {
    cli::cli_alert_info("Skipped: {skip_check$reason}")
    skipped_steps <- c(skipped_steps, "4_team_skill")
  } else {
    step_start <- Sys.time()
    if (FRESH_START) {
      cli::cli_alert_success("Running FRESH calculation from scratch")
    } else {
      cli::cli_alert_success("Running incremental: {skip_check$reason}")
    }

    # Source the calculation script
    source(file.path(DATA_RAW_DIR, "ratings/team/02_calculate_team_skill_indices.R"), local = TRUE)

    # Update pipeline state
    conn <- get_db_connection(read_only = FALSE)
    update_pipeline_state("team_skill", conn, status = "success")
    DBI::dbDisconnect(conn, shutdown = TRUE)

    step_times[["4_team_skill"]] <- difftime(Sys.time(), step_start, units = "mins")
    print_step_complete("Step 4: Team Skill Indices", step_times[["4_team_skill"]])
  }
}


# Step 5: Venue Skill Indices ----

if (should_run(5)) {
  cli::cli_h1("Step 5: Calculate Venue Skill Indices")

  # If FRESH_START, clear venue skill tables first
  if (FRESH_START) {
    cli::cli_alert_warning("FRESH_START: Clearing venue skill tables...")
    conn <- get_db_connection(read_only = FALSE)
    for (fmt in c("t20", "odi", "test")) {
      tbl <- paste0(fmt, "_venue_skill")
      if (tbl %in% DBI::dbListTables(conn)) {
        DBI::dbExecute(conn, paste0("DELETE FROM ", tbl))
        cli::cli_alert_info("Cleared {tbl}")
      }
    }
    DBI::dbDisconnect(conn, shutdown = TRUE)
  }

  # Check smart skip
  conn <- get_db_connection(read_only = TRUE)
  skip_check <- check_smart_skip("venue_skill", 5, conn, delivery_threshold = 0)
  DBI::dbDisconnect(conn, shutdown = TRUE)

  if (skip_check$should_skip && !FRESH_START) {
    cli::cli_alert_info("Skipped: {skip_check$reason}")
    skipped_steps <- c(skipped_steps, "5_venue_skill")
  } else {
    step_start <- Sys.time()
    if (FRESH_START) {
      cli::cli_alert_success("Running FRESH calculation from scratch")
    } else {
      cli::cli_alert_success("Running incremental: {skip_check$reason}")
    }

    # Source the calculation script
    source(file.path(DATA_RAW_DIR, "ratings/venue/01_calculate_venue_skill_indices.R"), local = TRUE)

    # Update pipeline state
    conn <- get_db_connection(read_only = FALSE)
    update_pipeline_state("venue_skill", conn, status = "success")
    DBI::dbDisconnect(conn, shutdown = TRUE)

    step_times[["5_venue_skill"]] <- difftime(Sys.time(), step_start, units = "mins")
    print_step_complete("Step 5: Venue Skill Indices", step_times[["5_venue_skill"]])
  }
}


# Step 6: Team ELO ----

if (should_run(6)) {
  cli::cli_h1("Step 6: Calculate Team ELO")

  step6_fresh <- is_fresh_start(6)

  # If fresh start for this step, clear team_elo table first
  if (step6_fresh) {
    cli::cli_alert_warning("FRESH_START: Clearing team_elo table...")
    conn <- get_db_connection(read_only = FALSE)
    if ("team_elo" %in% DBI::dbListTables(conn)) {
      DBI::dbExecute(conn, "DELETE FROM team_elo")
      cli::cli_alert_info("Cleared team_elo")
    }
    DBI::dbDisconnect(conn, shutdown = TRUE)
  }

  # Check smart skip (match-based threshold)
  conn <- get_db_connection(read_only = TRUE)
  skip_check <- check_smart_skip("team_elo", 6, conn, match_threshold = 0)
  DBI::dbDisconnect(conn, shutdown = TRUE)

  if (skip_check$should_skip && !step6_fresh) {
    cli::cli_alert_info("Skipped: {skip_check$reason}")
    skipped_steps <- c(skipped_steps, "6_team_elo")
  } else {
    step_start <- Sys.time()
    if (step6_fresh) {
      cli::cli_alert_success("Running FRESH calculation from scratch")
    } else {
      cli::cli_alert_success("Running: {skip_check$reason}")
    }

    # Source the calculation script
    source(file.path(DATA_RAW_DIR, "ratings/team/01_calculate_team_elos.R"), local = TRUE)

    # Update pipeline state
    conn <- get_db_connection(read_only = FALSE)
    update_pipeline_state("team_elo", conn, status = "success")
    DBI::dbDisconnect(conn, shutdown = TRUE)

    step_times[["6_team_elo"]] <- difftime(Sys.time(), step_start, units = "mins")
    print_step_complete("Step 6: Team ELO", step_times[["6_team_elo"]])
  }
}


# Step 7: Full Model ----

if (should_run(7)) {
  cli::cli_h1("Step 7: Train Full Model")

  # Check smart skip (same threshold as agnostic model)
  conn <- get_db_connection(read_only = TRUE)
  skip_check <- check_smart_skip("full_model", 7, conn,
                                  delivery_threshold = MODEL_RETRAIN_THRESHOLD)
  DBI::dbDisconnect(conn, shutdown = TRUE)

  if (skip_check$should_skip) {
    cli::cli_alert_info("Skipped: {skip_check$reason}")
    skipped_steps <- c(skipped_steps, "7_full_model")
  } else {
    step_start <- Sys.time()
    cli::cli_alert_success("Running: {skip_check$reason}")

    # Source the training script
    source(file.path(DATA_RAW_DIR, "models/ball-outcome/02_train_full_model.R"), local = TRUE)

    # Update pipeline state
    conn <- get_db_connection(read_only = FALSE)
    update_pipeline_state("full_model", conn, status = "success")
    DBI::dbDisconnect(conn, shutdown = TRUE)

    step_times[["7_full_model"]] <- difftime(Sys.time(), step_start, units = "mins")
    print_step_complete("Step 7: Full Model", step_times[["7_full_model"]])
  }
}


# Step 8: Pre-Match Features ----

if (should_run(8)) {
  cli::cli_h1("Step 8: Calculate Pre-Match Features")

  # If FRESH_START, clear pre_match_features table first
  if (FRESH_START) {
    cli::cli_alert_warning("FRESH_START: Clearing pre_match_features table...")
    conn <- get_db_connection(read_only = FALSE)
    if ("pre_match_features" %in% DBI::dbListTables(conn)) {
      DBI::dbExecute(conn, "DELETE FROM pre_match_features")
      cli::cli_alert_info("Cleared pre_match_features")
    }
    DBI::dbDisconnect(conn, shutdown = TRUE)
  }

  # Check smart skip (skip only if no new matches)
  conn <- get_db_connection(read_only = TRUE)
  skip_check <- check_smart_skip("pre_match_features", 8, conn, match_threshold = 0)
  DBI::dbDisconnect(conn, shutdown = TRUE)

  if (skip_check$should_skip && !FRESH_START) {
    cli::cli_alert_info("Skipped: {skip_check$reason}")
    skipped_steps <- c(skipped_steps, "8_pre_match_features")
  } else {
    step_start <- Sys.time()
    if (FRESH_START) {
      cli::cli_alert_success("Running FRESH calculation from scratch")
    } else {
      cli::cli_alert_success("Running: {skip_check$reason}")
    }

    # Source the calculation script
    source(file.path(DATA_RAW_DIR, "models/pre-match/02_calculate_pre_match_features.R"), local = TRUE)

    # Update pipeline state
    conn <- get_db_connection(read_only = FALSE)
    update_pipeline_state("pre_match_features", conn, status = "success")
    DBI::dbDisconnect(conn, shutdown = TRUE)

    step_times[["8_pre_match_features"]] <- difftime(Sys.time(), step_start, units = "mins")
    print_step_complete("Step 8: Pre-Match Features", step_times[["8_pre_match_features"]])
  }
}


# Step 9: Pre-Match Model ----

if (should_run(9)) {
  cli::cli_h1("Step 9: Train Pre-Match Prediction Model")

  # Check smart skip (threshold: 100 new matches)
  conn <- get_db_connection(read_only = TRUE)
  skip_check <- check_smart_skip("pre_match_model", 9, conn,
                                  match_threshold = PREMATCH_RETRAIN_THRESHOLD)
  DBI::dbDisconnect(conn, shutdown = TRUE)

  if (skip_check$should_skip && !FRESH_START) {
    cli::cli_alert_info("Skipped: {skip_check$reason}")
    skipped_steps <- c(skipped_steps, "9_pre_match_model")
  } else {
    step_start <- Sys.time()
    if (FRESH_START) {
      cli::cli_alert_success("Running FRESH model training")
    } else {
      cli::cli_alert_success("Running: {skip_check$reason}")
    }

    # Source the training script
    source(file.path(DATA_RAW_DIR, "models/pre-match/03_train_prediction_model.R"), local = TRUE)

    # Update pipeline state
    conn <- get_db_connection(read_only = FALSE)
    update_pipeline_state("pre_match_model", conn, status = "success")
    DBI::dbDisconnect(conn, shutdown = TRUE)

    step_times[["9_pre_match_model"]] <- difftime(Sys.time(), step_start, units = "mins")
    print_step_complete("Step 9: Pre-Match Model", step_times[["9_pre_match_model"]])
  }
}


# Step 10: Projection Parameter Optimization ----

if (should_run(10)) {
  cli::cli_h1("Step 10: Optimize Projection Parameters")

  # Check if projection params already exist
  params_file <- file.path(dirname(DATA_RAW_DIR), "bouncerdata/models/projection_params_t20.rds")

  if (file.exists(params_file) && !FRESH_START) {
    cli::cli_alert_info("Projection parameters already exist at {params_file}")
    cli::cli_alert_info("Skipping optimization. Set FRESH_START = TRUE to re-optimize.")
    skipped_steps <- c(skipped_steps, "10_projection_params")
  } else {
    step_start <- Sys.time()
    if (FRESH_START) {
      cli::cli_alert_success("Running FRESH parameter optimization")
    } else {
      cli::cli_alert_success("Running initial parameter optimization")
    }

    # Source the optimization script
    source(file.path(DATA_RAW_DIR, "ratings/projection/01_optimize_projection_params.R"), local = TRUE)

    # Update pipeline state
    conn <- get_db_connection(read_only = FALSE)
    update_pipeline_state("projection_params", conn, status = "success")
    DBI::dbDisconnect(conn, shutdown = TRUE)

    step_times[["10_projection_params"]] <- difftime(Sys.time(), step_start, units = "mins")
    print_step_complete("Step 10: Projection Parameters", step_times[["10_projection_params"]])
  }
}


# Step 11: Calculate Per-Delivery Projections ----

if (should_run(11)) {
  cli::cli_h1("Step 11: Calculate Per-Delivery Projections")

  # If FRESH_START, clear projection tables first
  if (FRESH_START) {
    cli::cli_alert_warning("FRESH_START: Clearing projection tables...")
    conn <- get_db_connection(read_only = FALSE)
    for (fmt in c("t20", "odi", "test")) {
      tbl <- paste0(fmt, "_score_projection")
      if (tbl %in% DBI::dbListTables(conn)) {
        DBI::dbExecute(conn, paste0("DELETE FROM ", tbl))
        cli::cli_alert_info("Cleared {tbl}")
      }
    }
    DBI::dbDisconnect(conn, shutdown = TRUE)
  }

  # Check smart skip
  conn <- get_db_connection(read_only = TRUE)
  skip_check <- check_smart_skip("score_projection", 11, conn, delivery_threshold = 0)
  DBI::dbDisconnect(conn, shutdown = TRUE)

  if (skip_check$should_skip && !FRESH_START) {
    cli::cli_alert_info("Skipped: {skip_check$reason}")
    skipped_steps <- c(skipped_steps, "11_score_projection")
  } else {
    step_start <- Sys.time()
    if (FRESH_START) {
      cli::cli_alert_success("Running FRESH projection calculation")
    } else {
      cli::cli_alert_success("Running: {skip_check$reason}")
    }

    # Source the calculation script
    source(file.path(DATA_RAW_DIR, "ratings/projection/02_calculate_projections.R"), local = TRUE)

    # Update pipeline state
    conn <- get_db_connection(read_only = FALSE)
    update_pipeline_state("score_projection", conn, status = "success")
    DBI::dbDisconnect(conn, shutdown = TRUE)

    step_times[["11_score_projection"]] <- difftime(Sys.time(), step_start, units = "mins")
    print_step_complete("Step 11: Score Projections", step_times[["11_score_projection"]])
  }
}


# Summary ----

total_time <- difftime(Sys.time(), pipeline_start, units = "mins")

cat("\n")
cli::cli_h1("Pipeline Complete")
cat("\n")

cli::cli_alert_success("Total time: {round(total_time, 1)} minutes")
cat("\n")

if (length(step_times) > 0) {
  cli::cli_h2("Step Timing Summary")

  step_df <- data.frame(
    step = names(step_times),
    minutes = as.numeric(step_times),
    stringsAsFactors = FALSE
  )
  step_df$pct <- round(step_df$minutes / sum(step_df$minutes) * 100, 1)

  for (i in seq_len(nrow(step_df))) {
    cli::cli_alert_info("{step_df$step[i]}: {round(step_df$minutes[i], 1)} min ({step_df$pct[i]}%)")
  }
}

# Show skipped steps
if (length(skipped_steps) > 0) {
  cat("\n")
  cli::cli_h2("Skipped Steps (Smart Caching)")
  for (step in skipped_steps) {
    cli::cli_alert_warning("{step}")
  }
  cli::cli_alert_info("To force re-run, set FORCE_STEPS or SKIP_IF_NO_CHANGE = FALSE")
}

cat("\n")
cli::cli_h2("Output Files")
cli::cli_bullets(c(
  "i" = "Models: ../bouncerdata/models/",
  " " = "  - agnostic_outcome_{{format}}.ubj",
  " " = "  - full_outcome_{{format}}.ubj",
  " " = "  - projection_params_{{format}}.rds",
  "i" = "Features: ../bouncerdata/models/{{format}}_prediction_features.rds",
  "i" = "Database: ../bouncerdata/bouncer.duckdb",
  " " = "  - {{format}}_score_projection tables"
))

cat("\n")
cli::cli_h2("Next Steps")
cli::cli_bullets(c(
  "i" = "Load models: load_agnostic_model('t20'), load_full_model('t20')",
  "i" = "Simulate match: quick_match_simulation(model, 't20')",
  "i" = "Get predictions: get_pre_match_features(match_id, conn)",
  "i" = "Calculate attribution: calculate_player_attribution(model, deliveries, 't20')",
  "i" = "Score projection: calculate_projected_score(score, balls, wickets, 't20')"
))

cat("\n")
