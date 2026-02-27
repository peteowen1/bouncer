# Optimize Wicket ELO Parameters ----
#
# Optimizes the 3-way Wicket ELO parameters using binary log-loss.
#
# Key differences from run optimization (02_optimize_run_elo.R):
#   1. Uses BINARY LOG-LOSS for wicket prediction (0/1 outcome)
#   2. Uses DELIVERY-LEVEL agnostic baselines from pre-computed table
#   3. Optimizes ONLY wicket-related parameters (separate from runs)
#
# Binary Log-Loss:
#   loss = -mean(actual * log(pred) + (1-actual) * log(1-pred))
#   - Standard for binary classification
#   - Heavily penalizes confident wrong predictions
#
# Data:
#   - 200k train / 100k test split (most recent 100k as test)
#   - Men's T20 format
#   - Agnostic baselines from agnostic_predictions_t20 table
#
# Parameters optimized (~12):
#   - k_wicket_max, k_wicket_min, k_wicket_halflife (Player K-factors)
#   - k_venue_perm_max, k_venue_perm_min, k_venue_perm_halflife
#   - k_venue_session_max, k_venue_session_min, k_venue_session_halflife
#   - w_batter, w_bowler (attribution weights)
#   - wicket_elo_divisor (ELO to probability conversion)
#
# Usage:
#   source("data-raw/ratings/player/3way-elo/optimization/03_optimize_wicket_elo.R")

# 1. Setup ----
library(DBI)
library(data.table)
devtools::load_all()

# 2. Configuration ----
# Accept command-line arguments: Rscript 03_optimize_wicket_elo.R [format] [gender]
args <- commandArgs(trailingOnly = TRUE)
FORMAT <- if (length(args) >= 1 && args[1] != "") tolower(args[1]) else "t20"
GENDER <- if (length(args) >= 2 && args[2] != "") tolower(args[2]) else "male"

# Sample sizes - smaller for formats with less data
DEFAULT_TRAIN_SIZE <- 200000
DEFAULT_TEST_SIZE <- 100000

if (GENDER == "female" && FORMAT == "test") {
  TRAIN_SIZE <- 30000   # Only ~44K available
  TEST_SIZE <- 10000
} else if (GENDER == "female") {
  TRAIN_SIZE <- min(DEFAULT_TRAIN_SIZE, 150000)
  TEST_SIZE <- min(DEFAULT_TEST_SIZE, 75000)
} else {
  TRAIN_SIZE <- DEFAULT_TRAIN_SIZE
  TEST_SIZE <- DEFAULT_TEST_SIZE
}

MAX_ITERATIONS <- 50
VERBOSE <- TRUE

cat("\n")
cli::cli_h1("Wicket ELO Parameter Optimization (Binary Log-Loss)")
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
  c("T20", "IT20")
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
    COALESCE(ap.agnostic_expected_runs, %f) AS baseline_runs,
    COALESCE(ap.agnostic_expected_wicket, %f) AS baseline_wicket
  FROM cricsheet.deliveries d
  JOIN cricsheet.matches m ON d.match_id = m.match_id
  LEFT JOIN %s ap ON d.delivery_id = ap.delivery_id
  WHERE LOWER(d.match_type) IN (%s)
    AND m.gender = '%s'
    AND d.batter_id IS NOT NULL
    AND d.bowler_id IS NOT NULL
  ORDER BY d.match_date, d.match_id, d.delivery_id
", expected_runs_fallback, expected_wicket_fallback, agnostic_table, match_type_filter, GENDER)

deliveries <- DBI::dbGetQuery(conn, query)
setDT(deliveries)

cli::cli_alert_info("Computing event tiers...")
deliveries[, event_tier := sapply(event_name, get_event_tier)]

n_total <- nrow(deliveries)
cli::cli_alert_success("Loaded {format(n_total, big.mark = ',')} deliveries")

# Wicket rate for context
wicket_rate <- mean(deliveries$is_wicket)
cli::cli_alert_info("Wicket rate: {round(wicket_rate * 100, 2)}%")

# 5. Train/Test Split ----
cli::cli_h2("Splitting data (200k train / 100k test)")

total_needed <- TRAIN_SIZE + TEST_SIZE

if (n_total < total_needed) {
  cli::cli_abort("Not enough data: need {total_needed}, have {n_total}")
}

test_data <- deliveries[(n_total - TEST_SIZE + 1):n_total]
train_data <- deliveries[(n_total - total_needed + 1):(n_total - TEST_SIZE)]

train_dates <- range(train_data$match_date)
test_dates <- range(test_data$match_date)

cli::cli_alert_info("Train: {format(nrow(train_data), big.mark = ',')} deliveries ({train_dates[1]} to {train_dates[2]})")
cli::cli_alert_info("Test: {format(nrow(test_data), big.mark = ',')} deliveries ({test_dates[1]} to {test_dates[2]})")
cli::cli_alert_info("Train wickets: {sum(train_data$is_wicket)} ({round(100*mean(train_data$is_wicket), 2)}%)")
cli::cli_alert_info("Test wickets: {sum(test_data$is_wicket)} ({round(100*mean(test_data$is_wicket), 2)}%)")

# 6. Define Wicket ELO Parameters ----
cli::cli_h2("Setting wicket ELO parameter bounds")

param_names <- c(
  # Player Wicket K (3)
  "k_wicket_max", "k_wicket_min", "k_wicket_halflife",
  # Venue Permanent K (3)
  "k_venue_perm_max", "k_venue_perm_min", "k_venue_perm_halflife",
  # Venue Session K (3)
  "k_venue_session_max", "k_venue_session_min", "k_venue_session_halflife",
  # Attribution weights (2)
  "w_batter", "w_bowler",
  # ELO divisor for probability conversion (1)
  "wicket_elo_divisor"
)

# Lower bounds
lower_bounds <- c(
  # Player Wicket K (smaller than run K since wickets are rarer)
  1, 0.2, 50,
  # Venue Permanent K
  0.5, 0.1, 1000,
  # Venue Session K
  1.0, 0.2, 30,
  # Attribution weights (Feb 2026: raised w_bowler minimum from 0.1 to 0.20)
  # This prevents optimizer from converging to local minimum with bowler weight too low
  0.25, 0.20,
  # ELO divisor (standard range)
  200
)

# Upper bounds
upper_bounds <- c(
  # Player Wicket K
  12, 4, 500,
  # Venue Permanent K
  6.0, 1.5, 8000,
  # Venue Session K
  15.0, 3.0, 300,
  # Attribution weights (Feb 2026: raised w_bowler maximum from 0.35 to 0.45)
  # Allows optimizer to explore higher bowler weights like other formats (31-35%)
  0.55, 0.45,
  # ELO divisor
  600
)

# Starting values (from current constants)
start_params <- c(
  # Player Wicket K
  THREE_WAY_K_WICKET_MAX_T20, THREE_WAY_K_WICKET_MIN_T20, THREE_WAY_K_WICKET_HALFLIFE_T20,
  # Venue Permanent K (same as run for now)
  THREE_WAY_K_VENUE_PERM_MAX_T20, THREE_WAY_K_VENUE_PERM_MIN_T20, THREE_WAY_K_VENUE_PERM_HALFLIFE_T20,
  # Venue Session K
  THREE_WAY_K_VENUE_SESSION_MAX_T20, THREE_WAY_K_VENUE_SESSION_MIN_T20, THREE_WAY_K_VENUE_SESSION_HALFLIFE_T20,
  # Attribution weights (same as run)
  THREE_WAY_W_BATTER, THREE_WAY_W_BOWLER,
  # ELO divisor
  THREE_WAY_ELO_DIVISOR
)

names(start_params) <- param_names
names(lower_bounds) <- param_names
names(upper_bounds) <- param_names

cli::cli_alert_success("Defined {length(param_names)} parameters to optimize")

# 7. Define Binary Log-Loss Function ----
cli::cli_h2("Defining binary log-loss function")

#' Prepare data for loss calculation
prepare_wicket_elo_data <- function(data) {
  list(
    batter_ids = as.character(data$batter_id),
    bowler_ids = as.character(data$bowler_id),
    venues = as.character(data$venue),
    match_ids = as.character(data$match_id),
    match_dates = as.numeric(data$match_date),
    overs = as.integer(data$over),
    is_wickets = as.integer(data$is_wicket),
    event_tiers = as.integer(data$event_tier),
    # Per-delivery agnostic baselines
    baseline_wicket = as.numeric(data$baseline_wicket),
    elo_start = THREE_WAY_ELO_START
  )
}

#' Calculate Binary Log-Loss for Wicket ELO
#'
#' Simulates ELO updates and calculates binary log-loss.
#'
#' @param params Named numeric vector of parameters
#' @param prep_data List from prepare_wicket_elo_data()
#' @param verbose Logical. Print progress.
#' @return Numeric. Binary log-loss value.
calculate_wicket_logloss <- function(params, prep_data, verbose = FALSE) {
  tryCatch({
    if (any(!is.finite(params))) return(1e10)

    # Extract params
    k_wicket_max <- params["k_wicket_max"]
    k_wicket_min <- params["k_wicket_min"]
    k_wicket_halflife <- params["k_wicket_halflife"]
    k_venue_perm_max <- params["k_venue_perm_max"]
    k_venue_perm_min <- params["k_venue_perm_min"]
    k_venue_perm_halflife <- params["k_venue_perm_halflife"]
    k_venue_session_max <- params["k_venue_session_max"]
    k_venue_session_min <- params["k_venue_session_min"]
    k_venue_session_halflife <- params["k_venue_session_halflife"]
    w_batter <- params["w_batter"]
    w_bowler <- params["w_bowler"]
    elo_divisor <- params["wicket_elo_divisor"]

    # Derive venue weights
    w_venue_total <- max(0.01, 1 - w_batter - w_bowler)
    w_venue_session <- w_venue_total * 0.8
    w_venue_perm <- w_venue_total * 0.2

    n <- length(prep_data$is_wickets)

    # Initialize ELO stores
    batter_elos <- new.env(hash = TRUE, size = 10000)
    bowler_elos <- new.env(hash = TRUE, size = 5000)
    venue_perm_elos <- new.env(hash = TRUE, size = 500)

    # Track experience
    batter_balls <- new.env(hash = TRUE, size = 10000)
    bowler_balls <- new.env(hash = TRUE, size = 5000)
    venue_balls <- new.env(hash = TRUE, size = 500)

    # Session tracking
    current_match <- ""
    venue_session_elo <- prep_data$elo_start
    match_balls <- 0

    # Store predictions
    predictions <- numeric(n)
    actuals <- prep_data$is_wickets

    for (i in seq_len(n)) {
      batter_id <- prep_data$batter_ids[i]
      bowler_id <- prep_data$bowler_ids[i]
      venue <- prep_data$venues[i]
      match_id <- prep_data$match_ids[i]

      # Reset session on new match
      if (match_id != current_match) {
        current_match <- match_id
        venue_session_elo <- prep_data$elo_start
        match_balls <- 0
      }

      # Get current ELOs
      batter_elo <- if (exists(batter_id, batter_elos)) get(batter_id, batter_elos) else prep_data$elo_start
      bowler_elo <- if (exists(bowler_id, bowler_elos)) get(bowler_id, bowler_elos) else prep_data$elo_start
      venue_perm_elo <- if (exists(venue, venue_perm_elos)) get(venue, venue_perm_elos) else prep_data$elo_start

      # Get experience
      batter_exp <- if (exists(batter_id, batter_balls)) get(batter_id, batter_balls) else 0
      bowler_exp <- if (exists(bowler_id, bowler_balls)) get(bowler_id, bowler_balls) else 0
      venue_exp <- if (exists(venue, venue_balls)) get(venue, venue_balls) else 0

      # Calculate expected wicket probability
      # Use logit scale for ELO adjustments
      baseline <- prep_data$baseline_wicket[i]
      baseline <- max(0.001, min(0.999, baseline))

      base_logit <- log(baseline / (1 - baseline))

      # ELO contributions on logit scale
      # Batter: higher ELO = survives better = LOWER wicket prob (subtract)
      # Bowler: higher ELO = takes more wickets = HIGHER wicket prob (add)
      # Venue: higher ELO = more wickets = HIGHER wicket prob (add)
      batter_contrib <- -w_batter * (batter_elo - prep_data$elo_start) / elo_divisor
      bowler_contrib <- w_bowler * (bowler_elo - prep_data$elo_start) / elo_divisor
      venue_perm_contrib <- w_venue_perm * (venue_perm_elo - prep_data$elo_start) / elo_divisor
      venue_session_contrib <- w_venue_session * (venue_session_elo - prep_data$elo_start) / elo_divisor

      adjusted_logit <- base_logit + batter_contrib + bowler_contrib +
                        venue_perm_contrib + venue_session_contrib

      # Convert to probability
      expected_wicket <- 1 / (1 + exp(-adjusted_logit))
      expected_wicket <- max(0.001, min(0.999, expected_wicket))

      predictions[i] <- expected_wicket

      # Calculate update delta
      actual <- actuals[i]

      # Calculate K-factors
      k_batter <- k_wicket_min + (k_wicket_max - k_wicket_min) * exp(-batter_exp / k_wicket_halflife)
      k_bowler <- k_wicket_min + (k_wicket_max - k_wicket_min) * exp(-bowler_exp / k_wicket_halflife)
      k_venue_perm <- k_venue_perm_min + (k_venue_perm_max - k_venue_perm_min) * exp(-venue_exp / k_venue_perm_halflife)
      k_venue_session <- k_venue_session_max * exp(-match_balls / k_venue_session_halflife)
      k_venue_session <- max(k_venue_session_min, k_venue_session)

      # Update ELOs
      # Batter: survives = good, so batter gains if survived (expected - actual)
      # Bowler: wicket = good, so bowler gains if got wicket (actual - expected)
      assign(batter_id, batter_elo + k_batter * (expected_wicket - actual), batter_elos)
      assign(bowler_id, bowler_elo + k_bowler * (actual - expected_wicket), bowler_elos)
      assign(venue, venue_perm_elo + k_venue_perm * (actual - expected_wicket), venue_perm_elos)
      venue_session_elo <- venue_session_elo + k_venue_session * (actual - expected_wicket)

      # Update experience
      assign(batter_id, batter_exp + 1, batter_balls)
      assign(bowler_id, bowler_exp + 1, bowler_balls)
      assign(venue, venue_exp + 1, venue_balls)
      match_balls <- match_balls + 1
    }

    # Calculate binary log-loss
    # log-loss = -mean(actual * log(pred) + (1-actual) * log(1-pred))
    eps <- 1e-15
    predictions <- pmax(eps, pmin(1 - eps, predictions))
    logloss <- -mean(actuals * log(predictions) + (1 - actuals) * log(1 - predictions))

    if (verbose) {
      cat(sprintf("Binary Log-Loss: %.6f\n", logloss))
    }

    logloss

  }, error = function(e) {
    if (verbose) cat("Error:", conditionMessage(e), "\n")
    return(1e10)
  })
}

# Prepare data
cli::cli_alert_info("Preparing data for optimization...")
train_prep <- prepare_wicket_elo_data(train_data)
test_prep <- prepare_wicket_elo_data(test_data)
cli::cli_alert_success("Data prepared")

# 8. Calculate Baseline Loss ----
cli::cli_h2("Calculating baseline loss")

baseline_loss <- calculate_wicket_logloss(start_params, train_prep, verbose = TRUE)
cli::cli_alert_info("Baseline log-loss: {round(baseline_loss, 6)}")

# For comparison: null model log-loss (always predict base rate)
base_rate <- mean(train_prep$is_wickets)
null_logloss <- -mean(train_prep$is_wickets * log(base_rate) +
                      (1 - train_prep$is_wickets) * log(1 - base_rate))
cli::cli_alert_info("Null model log-loss (base rate only): {round(null_logloss, 6)}")

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
      cat(sprintf("Eval %d/%d (best log-loss: %.6f)...\n",
                  fn_eval_count, MAX_FN_EVALS, best_loss))
    }

    loss <- calculate_wicket_logloss(p, train_prep, verbose = FALSE)

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
  cli::cli_alert_warning("Using best params from search")
  optim_result$par <- best_params
  optim_result$value <- best_loss
}

cli::cli_alert_success("Optimization complete")

# 10. Validation ----
cli::cli_h2("Validation")

train_loss_before <- baseline_loss
train_loss_after <- calculate_wicket_logloss(optim_result$par, train_prep, verbose = TRUE)

test_loss_before <- calculate_wicket_logloss(start_params, test_prep, verbose = TRUE)
test_loss_after <- calculate_wicket_logloss(optim_result$par, test_prep, verbose = TRUE)

cat("\nBinary Log-Loss Summary:\n")
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
cat(sprintf("%-20s %12.6f\n", "Null model", null_logloss))

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

cat("\n# Wicket ELO Optimized Parameters (Log-Loss, Feb 2026)\n")
cat(sprintf("THREE_WAY_K_WICKET_MAX_T20 <- %.1f\n", optimized_params["k_wicket_max"]))
cat(sprintf("THREE_WAY_K_WICKET_MIN_T20 <- %.1f\n", optimized_params["k_wicket_min"]))
cat(sprintf("THREE_WAY_K_WICKET_HALFLIFE_T20 <- %.0f\n", optimized_params["k_wicket_halflife"]))
cat(sprintf("# Wicket-specific venue K-factors (if different from run K)\n"))
cat(sprintf("THREE_WAY_K_VENUE_PERM_WICKET_MAX_T20 <- %.2f\n", optimized_params["k_venue_perm_max"]))
cat(sprintf("THREE_WAY_K_VENUE_PERM_WICKET_MIN_T20 <- %.2f\n", optimized_params["k_venue_perm_min"]))
cat(sprintf("THREE_WAY_K_VENUE_PERM_WICKET_HALFLIFE_T20 <- %.0f\n", optimized_params["k_venue_perm_halflife"]))
cat(sprintf("THREE_WAY_K_VENUE_SESSION_WICKET_MAX_T20 <- %.2f\n", optimized_params["k_venue_session_max"]))
cat(sprintf("THREE_WAY_K_VENUE_SESSION_WICKET_MIN_T20 <- %.2f\n", optimized_params["k_venue_session_min"]))
cat(sprintf("THREE_WAY_K_VENUE_SESSION_WICKET_HALFLIFE_T20 <- %.0f\n", optimized_params["k_venue_session_halflife"]))
cat(sprintf("THREE_WAY_W_BATTER_WICKET <- %.3f\n", optimized_params["w_batter"]))
cat(sprintf("THREE_WAY_W_BOWLER_WICKET <- %.3f\n", optimized_params["w_bowler"]))
cat(sprintf("THREE_WAY_WICKET_ELO_DIVISOR <- %.0f\n", optimized_params["wicket_elo_divisor"]))

# 13. Save Results ----
cli::cli_h2("Saving results")

# Include gender in filename for format-gender-specific params
gender_suffix <- if (GENDER == "male") "mens" else "womens"
results_file <- file.path(find_bouncerdata_dir(), "models",
                          sprintf("wicket_elo_params_%s_%s.rds", gender_suffix, FORMAT))
dir.create(dirname(results_file), showWarnings = FALSE, recursive = TRUE)

results <- list(
  format = FORMAT,
  gender = GENDER,
  loss_type = "binary_logloss",
  optimized_params = optimized_params,
  start_params = start_params,
  train_loss_before = train_loss_before,
  train_loss_after = train_loss_after,
  test_loss_before = test_loss_before,
  test_loss_after = test_loss_after,
  null_model_loss = null_logloss,
  train_date_range = train_dates,
  test_date_range = test_dates,
  n_train = nrow(train_data),
  n_test = nrow(test_data),
  train_wicket_rate = mean(train_data$is_wicket),
  test_wicket_rate = mean(test_data$is_wicket),
  optimized_at = Sys.time()
)

saveRDS(results, results_file)
cli::cli_alert_success("Saved results to {results_file}")

# 14. Summary ----
cat("\n")
cli::cli_h1("Wicket ELO Optimization Complete")
cli::cli_alert_success("Loss function: Binary log-loss")
cli::cli_alert_success("Train log-loss improvement: {round(100 * (train_loss_before - train_loss_after) / train_loss_before, 2)}%")
cli::cli_alert_success("Test log-loss improvement: {round(100 * (test_loss_before - test_loss_after) / test_loss_before, 2)}%")
cli::cli_alert_success("Improvement over null model: {round(100 * (null_logloss - test_loss_after) / null_logloss, 2)}%")
cat("\n")

cli::cli_h3("Next Steps")
cli::cli_bullets(c(
  "i" = "Copy generated constants to R/constants_3way.R",
  "i" = "Consider using separate wicket-specific venue K-factors",
  "i" = "Re-run full 3-way ELO calculation with optimized parameters"
))
cat("\n")

# Cleanup ----
if (exists("conn") && !is.null(conn)) {
  tryCatch({
    DBI::dbDisconnect(conn, shutdown = TRUE)
  }, error = function(e) NULL)
}
