# 05 Optimize 3-Way ELO Parameters ----
#
# This script optimizes the 3-way ELO parameters using historical data.
#
# Objective function:
#   combined_loss = 0.4 * wicket_log_loss + 0.6 * run_rmse
#
# Parameters to optimize (~20+ per format):
#   - Player K-factors: K_RUN_MAX/MIN/HALFLIFE, K_WICKET_MAX/MIN/HALFLIFE
#   - Venue K-factors: PERM_MAX/MIN/HALFLIFE, SESSION_BASE/MIN/HALFLIFE
#   - Attribution weights: W_BATTER, W_BOWLER (W_VENUE = 1 - others)
#   - Venue component weights: W_PERMANENT, W_SESSION
#   - Inactivity: HALFLIFE, K_BOOST
#   - Situational modifiers: PHASE_MULT, TIER_MULT, etc.
#
# Method:
#   - L-BFGS-B optimization with bounded parameters
#   - Vectorized ELO calculation for speed
#   - Train/validation split by time (last 6 months = validation)
#
# Prerequisites:
#   - Run 04_calculate_3way_elo.R first to populate 3-way ELO tables
#   - Sufficient historical data for meaningful optimization

# 1. Setup ----
library(DBI)
library(data.table)
devtools::load_all()

# 2. Configuration ----
FORMAT_TO_OPTIMIZE <- "t20"  # "t20", "odi", or "test"
TRAIN_SIZE <- 100000         # ~4-5 months of T20 (should capture venue variation)
VALIDATION_SIZE <- 50000     # ~2 months holdout
MAX_ITERATIONS <- 40         # L-BFGS-B iterations
VERBOSE <- TRUE              # Print progress

cat("\n")
cli::cli_h1("3-Way ELO Parameter Optimization")
cli::cli_alert_info("Format: {toupper(FORMAT_TO_OPTIMIZE)}")
cli::cli_alert_info("Train size: {format(TRAIN_SIZE, big.mark = ',')} deliveries")
cli::cli_alert_info("Validation size: {format(VALIDATION_SIZE, big.mark = ',')} deliveries")
cli::cli_alert_info("Max iterations: {MAX_ITERATIONS}")
cat("\n")

# 3. Database Connection ----
cli::cli_h2("Connecting to database")
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
cli::cli_alert_success("Connected to database")

# 4. Load Data ----
cli::cli_h2("Loading data")

# Get format-specific match types
format_match_types <- switch(FORMAT_TO_OPTIMIZE,
  "t20" = c("T20", "IT20"),
  "odi" = c("ODI", "ODM"),
  "test" = c("Test", "MDM")
)
match_type_filter <- paste(sprintf("'%s'", tolower(format_match_types)), collapse = ", ")

# Load delivery data with outcomes
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
    m.outcome_type
  FROM cricsheet.deliveries d
  JOIN cricsheet.matches m ON d.match_id = m.match_id
  WHERE LOWER(d.match_type) IN (%s)
    AND m.gender = 'male'
    AND d.batter_id IS NOT NULL
    AND d.bowler_id IS NOT NULL
  ORDER BY d.match_date, d.match_id, d.delivery_id
", match_type_filter)

deliveries <- DBI::dbGetQuery(conn, query)
setDT(deliveries)

# Add event_tier for C++ function (vectorized for speed)
cli::cli_alert_info("Computing event tiers...")
deliveries[, event_tier := sapply(event_name, get_event_tier)]

n_total <- nrow(deliveries)
cli::cli_alert_success("Loaded {format(n_total, big.mark = ',')} deliveries")

# 5. Train/Validation Split ----
cli::cli_h2("Splitting data")

# Use latest deliveries for validation, and the chunk before that for training
# This focuses optimization on recent, relevant data
n_total <- nrow(deliveries)
total_needed <- TRAIN_SIZE + VALIDATION_SIZE

if (n_total < total_needed) {
  cli::cli_abort("Not enough data: need {total_needed}, have {n_total}")
}

# Data is already ordered by date, so take the tail
validation_data <- deliveries[(n_total - VALIDATION_SIZE + 1):n_total]
train_data <- deliveries[(n_total - total_needed + 1):(n_total - VALIDATION_SIZE)]

# Get date ranges for logging
train_dates <- range(train_data$match_date)
val_dates <- range(validation_data$match_date)

cli::cli_alert_info("Train: {format(nrow(train_data), big.mark = ',')} deliveries ({train_dates[1]} to {train_dates[2]})")
cli::cli_alert_info("Validation: {format(nrow(validation_data), big.mark = ',')} deliveries ({val_dates[1]} to {val_dates[2]})")

# 6. Define Parameter Bounds ----
cli::cli_h2("Setting parameter bounds")

# Parameter names and bounds
# Order: player K-run, player K-wicket, venue K-perm, venue K-session, weights, inactivity, tier, blending
param_names <- c(
  # Player Run K (3)
  "k_run_max", "k_run_min", "k_run_halflife",
  # Player Wicket K (3)
  "k_wicket_max", "k_wicket_min", "k_wicket_halflife",
  # Venue Permanent K (3)
  "k_venue_perm_max", "k_venue_perm_min", "k_venue_perm_halflife",
  # Venue Session K (3)
  "k_venue_session_base", "k_venue_session_min", "k_venue_session_halflife",
  # Attribution weights (2) - W_VENUE is derived as 1 - W_BATTER - W_BOWLER
  "w_batter", "w_bowler",
  # Venue component weights (1) - W_SESSION = 1 - W_PERMANENT
  "venue_w_permanent",
  # Runs per ELO point (1)
  "runs_per_100_elo",
  # Inactivity (2)
  "inactivity_halflife", "inactivity_k_boost",
  # Tier multipliers (4)
  "tier_1_mult", "tier_2_mult", "tier_3_mult", "tier_4_mult",
  # Sample-size blending (1) - Bayesian prior strength
  "reliability_halflife"
)

# Lower bounds - UPDATED based on optimization run 1:
# - runs_per_100_elo hit lower bound (0.02), lowered to 0.005
# - w_batter + w_bowler hit upper limit (venue was 0.05), allow more player weight
lower_bounds <- c(
  # Player Run K - allow very low values
  2, 0.3, 200,
  # Player Wicket K - allow very low values
  1, 0.2, 200,
  # Venue Permanent K
  0.5, 0.05, 1000,
  # Venue Session K
  1.0, 0.2, 20,
  # Attribution weights - allow higher (sum can be up to 0.99, leaving 0.01 for venue)
  0.2, 0.2,
  # Venue component weights
  0.3,
  # Runs per ELO - lowered from 0.02 to 0.005 (hit bound last time)
  0.005,
  # Inactivity
  30, 0.1,
  # Tier multipliers
  0.7, 0.5, 0.3, 0.2,
  # Reliability halflife (smaller = more aggressive blending toward replacement)
  50
)

# Upper bounds - UPDATED: allow more extreme player weights
upper_bounds <- c(
  # Player Run K
  30, 8, 3000,
  # Player Wicket K
  12, 4, 3000,
  # Venue Permanent K
  4.0, 1.0, 8000,
  # Venue Session K
  8.0, 2.0, 200,
  # Attribution weights - allow up to 0.6 each (can sum to 1.2 but venue constraint will limit)
  0.6, 0.6,
  # Venue component weights
  0.9,
  # Runs per ELO
  0.15,
  # Inactivity
  400, 0.8,
  # Tier multipliers
  1.3, 1.2, 1.1, 1.0,
  # Reliability halflife (larger = less blending, more trust in raw ELO)
  2000
)

# Starting values - RESET to balanced starting point for larger sample
# Previous runs used tiny window (~27 days), may have over-fit
start_params <- c(
  # Player Run K
  10, 2, 1000,
  # Player Wicket K
  4, 1, 1000,
  # Venue Permanent K
  2.0, 0.3, 3000,
  # Venue Session K
  4.0, 1.0, 50,
  # Attribution weights - start balanced, let optimizer find true weights
  0.40, 0.40,
  # Venue component weights
  0.50,
  # Runs per ELO - start moderate
  0.05,
  # Inactivity
  180, 0.3,
  # Tier multipliers
  1.1, 1.0, 0.9, 0.8,
  # Reliability halflife
  500
)

names(start_params) <- param_names
names(lower_bounds) <- param_names
names(upper_bounds) <- param_names

cli::cli_alert_success("Defined {length(param_names)} parameters to optimize")

# 7. Define Loss Function ----
cli::cli_h2("Defining loss function")

# ============================================================================
# Using Rcpp C++ implementation for 10-50x speedup
# The C++ function calculate_3way_loss_cpp() is defined in:
#   src/three_way_elo_optim.cpp
# It uses std::unordered_map instead of R environments for O(1) lookups
# and compiled loops instead of interpreted R for-loops
# ============================================================================

#' Create pre-extracted data vectors for C++ loss function
#'
#' Prepares data in the format required by calculate_3way_loss_cpp().
#' This is done once before optimization to avoid repeated extraction.
#'
#' @param data data.table with delivery data
#' @param format Character. Match format for calibration values
#' @return List of vectors ready for C++ function
prepare_cpp_data <- function(data, format = "t20") {
  # Get calibration values (format means)
  mean_runs <- switch(format,
    "t20" = EXPECTED_RUNS_T20,
    "odi" = EXPECTED_RUNS_ODI,
    "test" = EXPECTED_RUNS_TEST,
    EXPECTED_RUNS_T20
  )
  mean_wicket <- switch(format,
    "t20" = EXPECTED_WICKET_T20,
    "odi" = EXPECTED_WICKET_ODI,
    "test" = EXPECTED_WICKET_TEST,
    EXPECTED_WICKET_T20
  )

  list(
    batter_ids = as.character(data$batter_id),
    bowler_ids = as.character(data$bowler_id),
    venues = as.character(data$venue),
    match_ids = as.character(data$match_id),
    match_dates = as.numeric(data$match_date),
    overs = as.integer(data$over),
    actual_runs = as.numeric(data$actual_runs),
    is_wickets = as.logical(data$is_wicket),
    is_boundaries = as.logical(ifelse(is.na(data$is_boundary), FALSE, data$is_boundary)),
    event_tiers = as.integer(data$event_tier),
    mean_runs = mean_runs,
    mean_wicket = mean_wicket,
    elo_start = THREE_WAY_ELO_START,
    elo_divisor = THREE_WAY_ELO_DIVISOR
  )
}

#' Calculate Loss for Parameter Set (C++ wrapper)
#'
#' Calls the fast C++ implementation for ELO calculation.
#'
#' @param params Named numeric vector of parameters.
#' @param cpp_data List from prepare_cpp_data().
#' @param verbose Logical. Print progress.
#'
#' @return Numeric. Combined loss value.
calculate_3way_loss <- function(params, cpp_data, verbose = FALSE) {
  # Wrap in tryCatch for safety
  result <- tryCatch({
    # Early validation - all params must be finite
    if (any(!is.finite(params))) {
      return(1e10)
    }

    # Call C++ function (10-50x faster than R version)
    loss <- calculate_3way_loss_cpp(
      params = as.numeric(params),
      batter_ids = cpp_data$batter_ids,
      bowler_ids = cpp_data$bowler_ids,
      venues = cpp_data$venues,
      match_ids = cpp_data$match_ids,
      match_dates = cpp_data$match_dates,
      overs = cpp_data$overs,
      actual_runs = cpp_data$actual_runs,
      is_wickets = cpp_data$is_wickets,
      is_boundaries = cpp_data$is_boundaries,
      event_tiers = cpp_data$event_tiers,
      mean_runs = cpp_data$mean_runs,
      mean_wicket = cpp_data$mean_wicket,
      elo_start = cpp_data$elo_start,
      elo_divisor = cpp_data$elo_divisor
    )

    if (verbose && is.finite(loss)) {
      # For verbose output, we'd need to modify C++ to return components
      # For now, just print combined loss
      cat(sprintf("Combined Loss: %.4f\n", loss))
    }

    loss
  }, error = function(e) {
    if (verbose) cat("Error in C++ loss function:", conditionMessage(e), "\n")
    return(1e10)
  })

  # Final check for non-finite values
  if (!is.finite(result)) {
    return(1e10)
  }

  result
}

# Pre-extract data for C++ function (do once, reuse in optimizer)
cli::cli_alert_info("Pre-extracting data for C++ optimizer...")
train_cpp_data <- prepare_cpp_data(train_data, FORMAT_TO_OPTIMIZE)
validation_cpp_data <- prepare_cpp_data(validation_data, FORMAT_TO_OPTIMIZE)
cli::cli_alert_success("Data prepared for C++ loss function")

# 8. Calculate Baseline Loss ----
cli::cli_h2("Calculating baseline loss")

baseline_loss <- calculate_3way_loss(start_params, train_cpp_data, verbose = TRUE)
cli::cli_alert_info("Baseline combined loss: {round(baseline_loss, 4)}")

# 9. Run Optimization ----
cli::cli_h2("Running optimization")

# Use L-BFGS-B optimization (reliable, works on Windows)
# Trade-off: less exploration than genetic algorithms, but stable
MAX_FN_EVALS <- MAX_ITERATIONS * 25
cli::cli_alert_info("Using L-BFGS-B optimizer (max {MAX_FN_EVALS} evaluations)")

fn_eval_count <- 0
best_loss <- Inf
best_params <- start_params

optim_result <- optim(
  par = start_params,
  fn = function(p) {
    fn_eval_count <<- fn_eval_count + 1

    if (fn_eval_count > MAX_FN_EVALS) {
      return(1e10)
    }

    if (fn_eval_count %% 20 == 0) {
      cat(sprintf("Eval %d/%d (best loss: %.4f)...\n", fn_eval_count, MAX_FN_EVALS, best_loss))
    }

    loss <- calculate_3way_loss(p, train_cpp_data, verbose = FALSE)

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

# Use best params found if optimizer didn't converge nicely
if (optim_result$value > best_loss) {
  cli::cli_alert_warning("Using best params from search (loss={round(best_loss, 4)}) instead of final")
  optim_result$par <- best_params
  optim_result$value <- best_loss
}

cli::cli_alert_success("Optimization complete")
cli::cli_alert_info("Final loss: {round(optim_result$value, 4)}")

# 10. Report Results ----
cli::cli_h2("Optimization Results")

optimized_params <- optim_result$par

cat("\nParameter Changes:\n")
cat(sprintf("%-25s %12s %12s %12s\n", "Parameter", "Start", "Optimized", "Change"))
cat(strrep("-", 65), "\n")

for (p in param_names) {
  start_val <- start_params[p]
  opt_val <- optimized_params[p]
  change <- opt_val - start_val
  pct_change <- if (start_val != 0) (change / start_val) * 100 else 0

  cat(sprintf("%-25s %12.4f %12.4f %+12.4f (%+.1f%%)\n",
              p, start_val, opt_val, change, pct_change))
}

# 11. Validation ----
cli::cli_h2("Validation")

train_loss_before <- baseline_loss
train_loss_after <- calculate_3way_loss(optimized_params, train_cpp_data, verbose = TRUE)

validation_loss_before <- calculate_3way_loss(start_params, validation_cpp_data, verbose = TRUE)
validation_loss_after <- calculate_3way_loss(optimized_params, validation_cpp_data, verbose = TRUE)

cat("\nLoss Summary:\n")
cat(sprintf("%-20s %12s %12s %12s\n", "Dataset", "Before", "After", "Improvement"))
cat(strrep("-", 60), "\n")
cat(sprintf("%-20s %12.4f %12.4f %+12.4f\n",
            "Train", train_loss_before, train_loss_after,
            train_loss_before - train_loss_after))
cat(sprintf("%-20s %12.4f %12.4f %+12.4f\n",
            "Validation", validation_loss_before, validation_loss_after,
            validation_loss_before - validation_loss_after))

# Check for overfitting
if (validation_loss_after > validation_loss_before * 1.05) {
  cli::cli_alert_warning("Warning: Validation loss increased - possible overfitting")
} else {
  cli::cli_alert_success("No significant overfitting detected")
}

# 12. Generate Constants Code ----
cli::cli_h2("Generated Constants")

cat("\n# Add to R/constants.R:\n\n")

w_venue <- 1 - optimized_params["w_batter"] - optimized_params["w_bowler"]
venue_w_session <- 1 - optimized_params["venue_w_permanent"]

format_upper <- toupper(FORMAT_TO_OPTIMIZE)

cat(sprintf("# 3-Way ELO Optimized Parameters for %s\n", format_upper))
cat(sprintf("THREE_WAY_K_RUN_MAX_%s <- %.1f\n", format_upper, optimized_params["k_run_max"]))
cat(sprintf("THREE_WAY_K_RUN_MIN_%s <- %.1f\n", format_upper, optimized_params["k_run_min"]))
cat(sprintf("THREE_WAY_K_RUN_HALFLIFE_%s <- %.0f\n", format_upper, optimized_params["k_run_halflife"]))
cat(sprintf("THREE_WAY_K_WICKET_MAX_%s <- %.1f\n", format_upper, optimized_params["k_wicket_max"]))
cat(sprintf("THREE_WAY_K_WICKET_MIN_%s <- %.1f\n", format_upper, optimized_params["k_wicket_min"]))
cat(sprintf("THREE_WAY_K_WICKET_HALFLIFE_%s <- %.0f\n", format_upper, optimized_params["k_wicket_halflife"]))
cat(sprintf("THREE_WAY_K_VENUE_PERM_MAX <- %.2f\n", optimized_params["k_venue_perm_max"]))
cat(sprintf("THREE_WAY_K_VENUE_PERM_MIN <- %.2f\n", optimized_params["k_venue_perm_min"]))
cat(sprintf("THREE_WAY_K_VENUE_PERM_HALFLIFE <- %.0f\n", optimized_params["k_venue_perm_halflife"]))
cat(sprintf("THREE_WAY_K_VENUE_SESSION_BASE <- %.2f\n", optimized_params["k_venue_session_base"]))
cat(sprintf("THREE_WAY_K_VENUE_SESSION_MIN <- %.2f\n", optimized_params["k_venue_session_min"]))
cat(sprintf("THREE_WAY_K_VENUE_SESSION_HALFLIFE <- %.0f\n", optimized_params["k_venue_session_halflife"]))
cat(sprintf("THREE_WAY_W_BATTER <- %.2f\n", optimized_params["w_batter"]))
cat(sprintf("THREE_WAY_W_BOWLER <- %.2f\n", optimized_params["w_bowler"]))
cat(sprintf("THREE_WAY_W_VENUE <- %.2f  # 1 - W_BATTER - W_BOWLER\n", w_venue))
cat(sprintf("THREE_WAY_VENUE_W_PERMANENT <- %.2f\n", optimized_params["venue_w_permanent"]))
cat(sprintf("THREE_WAY_VENUE_W_SESSION <- %.2f  # 1 - W_PERMANENT\n", venue_w_session))
cat(sprintf("THREE_WAY_RUNS_PER_100_ELO_POINTS_%s <- %.4f\n", format_upper, optimized_params["runs_per_100_elo"]))
cat(sprintf("THREE_WAY_INACTIVITY_HALFLIFE <- %.0f\n", optimized_params["inactivity_halflife"]))
cat(sprintf("THREE_WAY_INACTIVITY_K_BOOST_FACTOR <- %.2f\n", optimized_params["inactivity_k_boost"]))
cat(sprintf("THREE_WAY_TIER_1_MULT <- %.2f\n", optimized_params["tier_1_mult"]))
cat(sprintf("THREE_WAY_TIER_2_MULT <- %.2f\n", optimized_params["tier_2_mult"]))
cat(sprintf("THREE_WAY_TIER_3_MULT <- %.2f\n", optimized_params["tier_3_mult"]))
cat(sprintf("THREE_WAY_TIER_4_MULT <- %.2f\n", optimized_params["tier_4_mult"]))
cat(sprintf("THREE_WAY_RELIABILITY_HALFLIFE <- %.0f  # Sample-size blending\n", optimized_params["reliability_halflife"]))

# 13. Save Results ----
cli::cli_h2("Saving results")

# Save to file
results_file <- file.path(find_bouncerdata_dir(), "models",
                          sprintf("3way_elo_params_%s.rds", FORMAT_TO_OPTIMIZE))
dir.create(dirname(results_file), showWarnings = FALSE, recursive = TRUE)

results <- list(
  format = FORMAT_TO_OPTIMIZE,
  optimized_params = optimized_params,
  start_params = start_params,
  train_loss_before = train_loss_before,
  train_loss_after = train_loss_after,
  validation_loss_before = validation_loss_before,
  validation_loss_after = validation_loss_after,
  optim_result = optim_result,
  train_date_range = train_dates,
  validation_date_range = val_dates,
  n_train = nrow(train_data),
  n_validation = nrow(validation_data),
  optimized_at = Sys.time()
)

saveRDS(results, results_file)
cli::cli_alert_success("Saved results to {results_file}")

# 14. Final Summary ----
cat("\n")
cli::cli_h1("Optimization Complete")
cli::cli_alert_success("Format: {toupper(FORMAT_TO_OPTIMIZE)}")
cli::cli_alert_success("Train loss improvement: {round((train_loss_before - train_loss_after) / train_loss_before * 100, 1)}%")
cli::cli_alert_success("Validation loss improvement: {round((validation_loss_before - validation_loss_after) / validation_loss_before * 100, 1)}%")
cat("\n")

cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "Copy generated constants to R/constants.R",
  "i" = "Re-run 04_calculate_3way_elo.R with optimized parameters",
  "i" = "Validate predictions improve on held-out test data"
))
cat("\n")
