# Optimize Run ELO Parameters ----
#
# Optimizes the 3-way Run ELO parameters using Poisson loss.
#
# Key differences from combined optimization (03_optimize_3way_params.R):
#   1. Uses POISSON LOSS for run prediction (treats runs as count data)
#   2. Uses DELIVERY-LEVEL agnostic baselines from pre-computed table
#   3. Optimizes ONLY run-related parameters (separate from wicket)
#
# Poisson Loss:
#   loss = mean(pred - actual * log(pred))
#   - Natural for count data (0, 1, 2, 3, 4, 6)
#   - Penalizes under-prediction more than over-prediction
#   - Handles zero inflation (dot balls) naturally
#
# Data:
#   - 200k train / 100k test split (most recent 100k as test)
#   - Men's T20 format
#   - Agnostic baselines from agnostic_predictions_t20 table
#
# Parameters optimized (~12):
#   - k_run_max, k_run_min, k_run_halflife (Player K-factors)
#   - k_venue_perm_max, k_venue_perm_min, k_venue_perm_halflife
#   - k_venue_session_base, k_venue_session_min, k_venue_session_halflife
#   - w_batter, w_bowler (attribution weights, w_venue = 1 - sum)
#   - runs_per_100_elo (ELO to runs conversion)
#
# Usage:
#   source("data-raw/ratings/player/3way-elo/optimization/02_optimize_run_elo.R")

# 1. Setup ----
library(DBI)
library(data.table)
devtools::load_all()

# 2. Configuration ----
# Accept command-line arguments: Rscript 02_optimize_run_elo.R [format] [gender]
args <- commandArgs(trailingOnly = TRUE)
FORMAT <- if (length(args) >= 1 && args[1] != "") tolower(args[1]) else "t20"
GENDER <- if (length(args) >= 2 && args[2] != "") tolower(args[2]) else "male"

# Sample sizes - smaller for formats with less data
# Women's Test (~44K) needs special handling
DEFAULT_TRAIN_SIZE <- 200000
DEFAULT_TEST_SIZE <- 100000

# Adjust for small datasets
if (GENDER == "female" && FORMAT == "test") {
  TRAIN_SIZE <- 30000   # Only ~44K available
  TEST_SIZE <- 10000
} else if (GENDER == "female") {
  TRAIN_SIZE <- min(DEFAULT_TRAIN_SIZE, 150000)  # Women's T20/ODI have 400-700K
  TEST_SIZE <- min(DEFAULT_TEST_SIZE, 75000)
} else {
  TRAIN_SIZE <- DEFAULT_TRAIN_SIZE
  TEST_SIZE <- DEFAULT_TEST_SIZE
}

MAX_ITERATIONS <- 50
VERBOSE <- TRUE

cat("\n")
cli::cli_h1("Run ELO Parameter Optimization (Poisson Loss)")
cli::cli_alert_info("Format: {toupper(FORMAT)}")
cli::cli_alert_info("Gender: {GENDER}")
cli::cli_alert_info("Train size: {format(TRAIN_SIZE, big.mark = ',')} deliveries")
cli::cli_alert_info("Test size: {format(TEST_SIZE, big.mark = ',')} deliveries")
cli::cli_alert_info("Max iterations: {MAX_ITERATIONS}")
cat("\n")

# 3. Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = TRUE)
# Note: on.exit() doesn't work reliably in sourced scripts, cleanup at end
cli::cli_alert_success("Connected to database")

# 4. Load Data with Agnostic Predictions ----
cli::cli_h2("Loading data with agnostic predictions")

# Get format-specific match types
format_match_types <- switch(FORMAT,
  "t20" = c("T20", "IT20"),
  "odi" = c("ODI", "ODM"),
  "test" = c("Test", "MDM"),
  c("T20", "IT20")  # default
)
match_type_filter <- paste(sprintf("'%s'", tolower(format_match_types)), collapse = ", ")

# Gender filter
gender_filter <- if (GENDER == "female") "'female'" else "'male'"

# Get format-specific expected values for fallback
expected_runs_fallback <- switch(FORMAT,
  "t20" = EXPECTED_RUNS_T20,
  "odi" = EXPECTED_RUNS_ODI,
  "test" = EXPECTED_RUNS_TEST,
  EXPECTED_RUNS_T20
)
expected_wicket_fallback <- switch(FORMAT,
  "t20" = EXPECTED_WICKET_T20,
  "odi" = EXPECTED_WICKET_ODI,
  "test" = EXPECTED_WICKET_TEST,
  EXPECTED_WICKET_T20
)

# Agnostic predictions table name
agnostic_table <- paste0("agnostic_predictions_", FORMAT)

# Load delivery data joined with pre-computed agnostic predictions
query <- sprintf("
  SELECT
    d.delivery_id,
    d.match_id,
    d.match_date,
    d.batter_id,
    d.bowler_id,
    d.venue,
    d.innings,
    d.over,
    d.runs_batter as actual_runs,
    d.is_wicket,
    d.is_boundary,
    m.event_name,
    m.outcome_type,
    -- Join agnostic predictions (pre-computed per-delivery baselines)
    COALESCE(ap.agnostic_expected_runs, %f) AS baseline_runs,
    COALESCE(ap.agnostic_expected_wicket, %f) AS baseline_wicket
  FROM deliveries d
  JOIN matches m ON d.match_id = m.match_id
  LEFT JOIN %s ap ON d.delivery_id = ap.delivery_id
  WHERE LOWER(d.match_type) IN (%s)
    AND m.gender = '%s'
    AND d.batter_id IS NOT NULL
    AND d.bowler_id IS NOT NULL
  ORDER BY d.match_date, d.match_id, d.delivery_id
", expected_runs_fallback, expected_wicket_fallback, agnostic_table, match_type_filter, GENDER)

deliveries <- DBI::dbGetQuery(conn, query)
setDT(deliveries)

# Add event_tier for context
cli::cli_alert_info("Computing event tiers...")
deliveries[, event_tier := sapply(event_name, get_event_tier)]

n_total <- nrow(deliveries)
cli::cli_alert_success("Loaded {format(n_total, big.mark = ',')} deliveries")

# Check for NULL baseline predictions
n_missing <- sum(is.na(deliveries$baseline_runs))
if (n_missing > 0) {
  cli::cli_alert_warning("{format(n_missing, big.mark = ',')} deliveries missing agnostic predictions (using format default)")
}

# 5. Train/Test Split ----
cli::cli_h2("Splitting data (200k train / 100k test)")

total_needed <- TRAIN_SIZE + TEST_SIZE

if (n_total < total_needed) {
  cli::cli_abort("Not enough data: need {total_needed}, have {n_total}")
}

# Test = most recent 100k, Train = 200k before that
test_data <- deliveries[(n_total - TEST_SIZE + 1):n_total]
train_data <- deliveries[(n_total - total_needed + 1):(n_total - TEST_SIZE)]

train_dates <- range(train_data$match_date)
test_dates <- range(test_data$match_date)

cli::cli_alert_info("Train: {format(nrow(train_data), big.mark = ',')} deliveries ({train_dates[1]} to {train_dates[2]})")
cli::cli_alert_info("Test: {format(nrow(test_data), big.mark = ',')} deliveries ({test_dates[1]} to {test_dates[2]})")

# 6. Define Run ELO Parameters ----
cli::cli_h2("Setting run ELO parameter bounds")

param_names <- c(
  # Player Run K (3)
  "k_run_max", "k_run_min", "k_run_halflife",
  # Venue Permanent K (3)
  "k_venue_perm_max", "k_venue_perm_min", "k_venue_perm_halflife",
  # Venue Session K (3)
  "k_venue_session_max", "k_venue_session_min", "k_venue_session_halflife",
  # Attribution weights (2) - W_VENUE_PERM and W_VENUE_SESSION derived
  "w_batter", "w_bowler",
  # Runs per ELO point (1)
  "runs_per_100_elo"
)

# Lower bounds
lower_bounds <- c(
  # Player Run K
  2, 0.5, 50,
  # Venue Permanent K
  0.5, 0.1, 1000,
  # Venue Session K
  2.0, 0.5, 30,
  # Attribution weights
  0.3, 0.1,
  # Runs per ELO
  0.02
)

# Upper bounds
upper_bounds <- c(
  # Player Run K
  25, 8, 500,
  # Venue Permanent K
  10.0, 2.0, 8000,
  # Venue Session K
  25.0, 5.0, 300,
  # Attribution weights (can sum up to 0.95, leaving 0.05 for venue)
  0.65, 0.35,
  # Runs per ELO
  0.20
)

# Starting values (from current constants)
start_params <- c(
  # Player Run K
  THREE_WAY_K_RUN_MAX_T20, THREE_WAY_K_RUN_MIN_T20, THREE_WAY_K_RUN_HALFLIFE_T20,
  # Venue Permanent K
  THREE_WAY_K_VENUE_PERM_MAX_T20, THREE_WAY_K_VENUE_PERM_MIN_T20, THREE_WAY_K_VENUE_PERM_HALFLIFE_T20,
  # Venue Session K
  THREE_WAY_K_VENUE_SESSION_MAX_T20, THREE_WAY_K_VENUE_SESSION_MIN_T20, THREE_WAY_K_VENUE_SESSION_HALFLIFE_T20,
  # Attribution weights
  THREE_WAY_W_BATTER, THREE_WAY_W_BOWLER,
  # Runs per ELO
  THREE_WAY_RUNS_PER_100_ELO_POINTS_T20
)

names(start_params) <- param_names
names(lower_bounds) <- param_names
names(upper_bounds) <- param_names

cli::cli_alert_success("Defined {length(param_names)} parameters to optimize")

# 7. Define Poisson Loss Function ----
cli::cli_h2("Defining Poisson loss function")

#' Prepare data for loss calculation
#' @param data data.table with delivery data including baseline_runs
prepare_run_elo_data <- function(data) {
  list(
    batter_ids = as.character(data$batter_id),
    bowler_ids = as.character(data$bowler_id),
    venues = as.character(data$venue),
    match_ids = as.character(data$match_id),
    match_dates = as.numeric(data$match_date),
    overs = as.integer(data$over),
    actual_runs = as.numeric(data$actual_runs),
    event_tiers = as.integer(data$event_tier),
    # Per-delivery agnostic baselines (key difference!)
    baseline_runs = as.numeric(data$baseline_runs),
    elo_start = THREE_WAY_ELO_START,
    elo_divisor = THREE_WAY_ELO_DIVISOR
  )
}

#' Calculate Poisson Loss for Run ELO
#'
#' Simulates ELO updates and calculates Poisson loss.
#' Poisson loss is natural for count data like runs.
#'
#' @param params Named numeric vector of parameters
#' @param prep_data List from prepare_run_elo_data()
#' @param verbose Logical. Print progress.
#' @return Numeric. Poisson loss value.
calculate_run_poisson_loss <- function(params, prep_data, verbose = FALSE) {
  tryCatch({
    # Validate params
    if (any(!is.finite(params))) return(1e10)

    # Extract params
    k_run_max <- params["k_run_max"]
    k_run_min <- params["k_run_min"]
    k_run_halflife <- params["k_run_halflife"]
    k_venue_perm_max <- params["k_venue_perm_max"]
    k_venue_perm_min <- params["k_venue_perm_min"]
    k_venue_perm_halflife <- params["k_venue_perm_halflife"]
    k_venue_session_max <- params["k_venue_session_max"]
    k_venue_session_min <- params["k_venue_session_min"]
    k_venue_session_halflife <- params["k_venue_session_halflife"]
    w_batter <- params["w_batter"]
    w_bowler <- params["w_bowler"]
    runs_per_100_elo <- params["runs_per_100_elo"]

    # Derive venue weights (what's left after player weights)
    w_venue_total <- max(0.01, 1 - w_batter - w_bowler)
    # Split venue weight 80/20 between session and permanent (from constants)
    w_venue_session <- w_venue_total * 0.8
    w_venue_perm <- w_venue_total * 0.2

    runs_per_elo <- runs_per_100_elo / 100

    n <- length(prep_data$actual_runs)

    # Initialize ELO stores (using R environments for O(1) lookup)
    batter_elos <- new.env(hash = TRUE, size = 10000)
    bowler_elos <- new.env(hash = TRUE, size = 5000)
    venue_perm_elos <- new.env(hash = TRUE, size = 500)

    # Track experience (deliveries)
    batter_balls <- new.env(hash = TRUE, size = 10000)
    bowler_balls <- new.env(hash = TRUE, size = 5000)
    venue_balls <- new.env(hash = TRUE, size = 500)

    # Track venue session ELOs (reset per match)
    current_match <- ""
    venue_session_elo <- prep_data$elo_start
    match_balls <- 0

    # Store predictions for loss calculation
    predictions <- numeric(n)
    actuals <- prep_data$actual_runs

    for (i in seq_len(n)) {
      # Get IDs
      batter_id <- prep_data$batter_ids[i]
      bowler_id <- prep_data$bowler_ids[i]
      venue <- prep_data$venues[i]
      match_id <- prep_data$match_ids[i]

      # Reset session ELO on new match
      if (match_id != current_match) {
        current_match <- match_id
        venue_session_elo <- prep_data$elo_start
        match_balls <- 0
      }

      # Get current ELOs (or start rating)
      batter_elo <- if (exists(batter_id, batter_elos)) get(batter_id, batter_elos) else prep_data$elo_start
      bowler_elo <- if (exists(bowler_id, bowler_elos)) get(bowler_id, bowler_elos) else prep_data$elo_start
      venue_perm_elo <- if (exists(venue, venue_perm_elos)) get(venue, venue_perm_elos) else prep_data$elo_start

      # Get experience counts
      batter_exp <- if (exists(batter_id, batter_balls)) get(batter_id, batter_balls) else 0
      bowler_exp <- if (exists(bowler_id, bowler_balls)) get(bowler_id, bowler_balls) else 0
      venue_exp <- if (exists(venue, venue_balls)) get(venue, venue_balls) else 0

      # Calculate expected runs using per-delivery baseline
      baseline <- prep_data$baseline_runs[i]

      # ELO contributions
      batter_contrib <- (batter_elo - prep_data$elo_start) * runs_per_elo
      bowler_contrib <- (prep_data$elo_start - bowler_elo) * runs_per_elo
      venue_perm_contrib <- (venue_perm_elo - prep_data$elo_start) * runs_per_elo
      venue_session_contrib <- (venue_session_elo - prep_data$elo_start) * runs_per_elo

      # Weighted expected runs
      expected_runs <- baseline +
        w_batter * batter_contrib +
        w_bowler * bowler_contrib +
        w_venue_perm * venue_perm_contrib +
        w_venue_session * venue_session_contrib

      # Bound to valid range and ensure positive for Poisson
      expected_runs <- max(0.01, min(6, expected_runs))
      predictions[i] <- expected_runs

      # Calculate residual (actual - expected)
      actual <- actuals[i]
      delta <- actual - expected_runs

      # Calculate K-factors
      k_batter <- k_run_min + (k_run_max - k_run_min) * exp(-batter_exp / k_run_halflife)
      k_bowler <- k_run_min + (k_run_max - k_run_min) * exp(-bowler_exp / k_run_halflife)
      k_venue_perm <- k_venue_perm_min + (k_venue_perm_max - k_venue_perm_min) * exp(-venue_exp / k_venue_perm_halflife)
      k_venue_session <- k_venue_session_max * exp(-match_balls / k_venue_session_halflife)
      k_venue_session <- max(k_venue_session_min, k_venue_session)

      # Update ELOs
      assign(batter_id, batter_elo + k_batter * delta, batter_elos)
      assign(bowler_id, bowler_elo - k_bowler * delta, bowler_elos)
      assign(venue, venue_perm_elo + k_venue_perm * delta, venue_perm_elos)
      venue_session_elo <- venue_session_elo + k_venue_session * delta

      # Update experience counts
      assign(batter_id, batter_exp + 1, batter_balls)
      assign(bowler_id, bowler_exp + 1, bowler_balls)
      assign(venue, venue_exp + 1, venue_balls)
      match_balls <- match_balls + 1
    }

    # Calculate Poisson deviance (loss)
    # Poisson deviance: 2 * sum(actual * log(actual/pred) - (actual - pred))
    # Simplified: sum(pred - actual * log(pred)) (up to constant)
    poisson_loss <- mean(predictions - actuals * log(predictions))

    if (verbose) {
      cat(sprintf("Poisson Loss: %.6f\n", poisson_loss))
    }

    poisson_loss

  }, error = function(e) {
    if (verbose) cat("Error:", conditionMessage(e), "\n")
    return(1e10)
  })
}

# Prepare data
cli::cli_alert_info("Preparing data for optimization...")
train_prep <- prepare_run_elo_data(train_data)
test_prep <- prepare_run_elo_data(test_data)
cli::cli_alert_success("Data prepared")

# 8. Calculate Baseline Loss ----
cli::cli_h2("Calculating baseline loss")

baseline_loss <- calculate_run_poisson_loss(start_params, train_prep, verbose = TRUE)
cli::cli_alert_info("Baseline Poisson loss: {round(baseline_loss, 6)}")

# 9. Run Optimization ----
cli::cli_h2("Running optimization")

MAX_FN_EVALS <- MAX_ITERATIONS * 25
cli::cli_alert_info("Using L-BFGS-B optimizer (max {MAX_FN_EVALS} evaluations)")

fn_eval_count <- 0
best_loss <- Inf
best_params <- start_params

optim_result <- optim(
  par = start_params,
  fn = function(p) {
    fn_eval_count <<- fn_eval_count + 1

    if (fn_eval_count > MAX_FN_EVALS) return(1e10)

    if (fn_eval_count %% 20 == 0) {
      cat(sprintf("Eval %d/%d (best Poisson loss: %.6f)...\n",
                  fn_eval_count, MAX_FN_EVALS, best_loss))
    }

    loss <- calculate_run_poisson_loss(p, train_prep, verbose = FALSE)

    if (loss < best_loss) {
      best_loss <<- loss
      best_params <<- p
    }

    loss
  },
  method = "L-BFGS-B",
  lower = lower_bounds,
  upper = upper_bounds,
  control = list(maxit = MAX_ITERATIONS, trace = 0)
)

if (optim_result$value > best_loss) {
  cli::cli_alert_warning("Using best params from search (loss={round(best_loss, 6)})")
  optim_result$par <- best_params
  optim_result$value <- best_loss
}

cli::cli_alert_success("Optimization complete")

# 10. Validation ----
cli::cli_h2("Validation")

train_loss_before <- baseline_loss
train_loss_after <- calculate_run_poisson_loss(optim_result$par, train_prep, verbose = TRUE)

test_loss_before <- calculate_run_poisson_loss(start_params, test_prep, verbose = TRUE)
test_loss_after <- calculate_run_poisson_loss(optim_result$par, test_prep, verbose = TRUE)

cat("\nPoisson Loss Summary:\n")
cat(sprintf("%-20s %12s %12s %12s\n", "Dataset", "Before", "After", "Improvement"))
cat(strrep("-", 60), "\n")
cat(sprintf("%-20s %12.6f %12.6f %+12.6f (%.2f%%)\n",
            "Train", train_loss_before, train_loss_after,
            train_loss_before - train_loss_after,
            100 * (train_loss_before - train_loss_after) / train_loss_before))
cat(sprintf("%-20s %12.6f %12.6f %+12.6f (%.2f%%)\n",
            "Test", test_loss_before, test_loss_after,
            test_loss_before - test_loss_after,
            100 * (test_loss_before - test_loss_after) / test_loss_before))

if (test_loss_after > test_loss_before * 1.02) {
  cli::cli_alert_warning("Warning: Test loss increased - possible overfitting")
} else {
  cli::cli_alert_success("No significant overfitting detected")
}

# 11. Report Results ----
cli::cli_h2("Optimization Results")

optimized_params <- optim_result$par

cat("\nParameter Changes:\n")
cat(sprintf("%-30s %12s %12s %12s\n", "Parameter", "Start", "Optimized", "Change"))
cat(strrep("-", 70), "\n")

for (p in param_names) {
  start_val <- start_params[p]
  opt_val <- optimized_params[p]
  change <- opt_val - start_val
  pct_change <- if (start_val != 0) (change / start_val) * 100 else 0

  cat(sprintf("%-30s %12.4f %12.4f %+12.4f (%+.1f%%)\n",
              p, start_val, opt_val, change, pct_change))
}

# 12. Generate Constants Code ----
cli::cli_h2("Generated Constants for constants_3way.R")

cat("\n# Run ELO Optimized Parameters (Poisson Loss, Feb 2026)\n")
cat(sprintf("THREE_WAY_K_RUN_MAX_T20 <- %.1f\n", optimized_params["k_run_max"]))
cat(sprintf("THREE_WAY_K_RUN_MIN_T20 <- %.1f\n", optimized_params["k_run_min"]))
cat(sprintf("THREE_WAY_K_RUN_HALFLIFE_T20 <- %.0f\n", optimized_params["k_run_halflife"]))
cat(sprintf("THREE_WAY_K_VENUE_PERM_MAX_T20 <- %.2f\n", optimized_params["k_venue_perm_max"]))
cat(sprintf("THREE_WAY_K_VENUE_PERM_MIN_T20 <- %.2f\n", optimized_params["k_venue_perm_min"]))
cat(sprintf("THREE_WAY_K_VENUE_PERM_HALFLIFE_T20 <- %.0f\n", optimized_params["k_venue_perm_halflife"]))
cat(sprintf("THREE_WAY_K_VENUE_SESSION_MAX_T20 <- %.2f\n", optimized_params["k_venue_session_max"]))
cat(sprintf("THREE_WAY_K_VENUE_SESSION_MIN_T20 <- %.2f\n", optimized_params["k_venue_session_min"]))
cat(sprintf("THREE_WAY_K_VENUE_SESSION_HALFLIFE_T20 <- %.0f\n", optimized_params["k_venue_session_halflife"]))
cat(sprintf("THREE_WAY_W_BATTER <- %.3f\n", optimized_params["w_batter"]))
cat(sprintf("THREE_WAY_W_BOWLER <- %.3f\n", optimized_params["w_bowler"]))
cat(sprintf("THREE_WAY_RUNS_PER_100_ELO_POINTS_T20 <- %.4f\n", optimized_params["runs_per_100_elo"]))

# 13. Save Results ----
cli::cli_h2("Saving results")

# Include gender in filename for format-gender-specific params
gender_suffix <- if (GENDER == "male") "mens" else "womens"
results_file <- file.path(find_bouncerdata_dir(), "models",
                          sprintf("run_elo_params_%s_%s.rds", gender_suffix, FORMAT))
dir.create(dirname(results_file), showWarnings = FALSE, recursive = TRUE)

results <- list(
  format = FORMAT,
  gender = GENDER,
  loss_type = "poisson",
  optimized_params = optimized_params,
  start_params = start_params,
  train_loss_before = train_loss_before,
  train_loss_after = train_loss_after,
  test_loss_before = test_loss_before,
  test_loss_after = test_loss_after,
  train_date_range = train_dates,
  test_date_range = test_dates,
  n_train = nrow(train_data),
  n_test = nrow(test_data),
  optimized_at = Sys.time()
)

saveRDS(results, results_file)
cli::cli_alert_success("Saved results to {results_file}")

# 14. Summary ----
cat("\n")
cli::cli_h1("Run ELO Optimization Complete")
cli::cli_alert_success("Loss function: Poisson deviance")
cli::cli_alert_success("Train Poisson loss improvement: {round(100 * (train_loss_before - train_loss_after) / train_loss_before, 2)}%")
cli::cli_alert_success("Test Poisson loss improvement: {round(100 * (test_loss_before - test_loss_after) / test_loss_before, 2)}%")
cat("\n")

cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "Run 03_optimize_wicket_elo.R for wicket parameters",
  "i" = "Copy generated constants to R/constants_3way.R",
  "i" = "Re-run full 3-way ELO calculation with optimized parameters"
))
cat("\n")

# Cleanup ----
if (exists("conn") && !is.null(conn)) {
  tryCatch({
    DBI::dbDisconnect(conn, shutdown = TRUE)
  }, error = function(e) NULL)
}
