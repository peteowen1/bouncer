# Generate Synthetic Data ----
#
# Creates synthetic cricket players, venues, and deliveries with KNOWN TRUE SKILLS.
# This allows us to validate whether the ELO system can recover those skills.
#
# Key insight: We generate outcomes using a formula that mirrors the ELO expected
# value calculation, then check if the ELO system can "reverse engineer" the
# true skills from the observed outcomes.
#
# Output: Data structures suitable for running through the ELO calculation loop.

# Load configuration
source(file.path(dirname(sys.frame(1)$ofile), "00_simulation_config.R"))

# ============================================================================
# PLAYER GENERATION
# ============================================================================

#' Generate Synthetic Players
#'
#' Creates batters and bowlers with known true skill values drawn from
#' configured distributions.
#'
#' @param n_batters Integer. Number of batters to generate.
#' @param n_bowlers Integer. Number of bowlers to generate.
#' @param seed Integer. Random seed for reproducibility.
#'
#' @return List with two data.tables:
#'   - batters: batter_id, true_run_skill, true_wicket_rate
#'   - bowlers: bowler_id, true_economy, true_wicket_rate
generate_synthetic_players <- function(n_batters = SIM_N_BATTERS,
                                        n_bowlers = SIM_N_BOWLERS,
                                        seed = SIM_SEED) {
  set.seed(seed)

  # Generate batter skills
  # true_run_skill: expected runs per ball (higher = better)
  # true_wicket_rate: expected wicket probability per ball (lower = better)
  batters <- data.table::data.table(
    batter_id = paste0("batter_", sprintf("%02d", seq_len(n_batters))),
    true_run_skill = rnorm(n_batters, SIM_BATTER_SR_MEAN, SIM_BATTER_SR_SD),
    true_wicket_rate = rnorm(n_batters, SIM_BATTER_WICKET_MEAN, SIM_BATTER_WICKET_SD)
  )

  # Bound to reasonable ranges
  batters[, true_run_skill := pmax(0.5, pmin(2.0, true_run_skill))]
  batters[, true_wicket_rate := pmax(0.01, pmin(0.20, true_wicket_rate))]

  # Generate bowler skills
  # true_economy: expected runs conceded per ball (lower = better)
  # true_wicket_rate: expected wicket probability per ball (higher = better)
  bowlers <- data.table::data.table(
    bowler_id = paste0("bowler_", sprintf("%02d", seq_len(n_bowlers))),
    true_economy = rnorm(n_bowlers, SIM_BOWLER_ECON_MEAN, SIM_BOWLER_ECON_SD),
    true_wicket_rate = rnorm(n_bowlers, SIM_BOWLER_WICKET_MEAN, SIM_BOWLER_WICKET_SD)
  )

  # Bound to reasonable ranges
  bowlers[, true_economy := pmax(0.8, pmin(1.6, true_economy))]
  bowlers[, true_wicket_rate := pmax(0.02, pmin(0.12, true_wicket_rate))]

  list(batters = batters, bowlers = bowlers)
}


# ============================================================================
# VENUE GENERATION
# ============================================================================

#' Generate Synthetic Venues
#'
#' Creates venues with known effects on run scoring and wicket-taking.
#'
#' @param n_venues Integer. Number of venues to generate.
#' @param seed Integer. Random seed for reproducibility.
#'
#' @return data.table with venue_id, true_run_effect, true_wicket_effect
generate_synthetic_venues <- function(n_venues = SIM_N_VENUES,
                                       seed = SIM_SEED) {
  set.seed(seed + 1)  # Different seed offset for venues

  venues <- data.table::data.table(
    venue = paste0("venue_", sprintf("%02d", seq_len(n_venues))),
    # true_run_effect: additive effect on runs (positive = high-scoring)
    true_run_effect = rnorm(n_venues, 0, SIM_VENUE_RUN_EFFECT_SD),
    # true_wicket_effect: additive effect on wicket probability (positive = bowler-friendly)
    true_wicket_effect = rnorm(n_venues, 0, SIM_VENUE_WICKET_EFFECT_SD)
  )

  # Bound to reasonable ranges
  venues[, true_run_effect := pmax(-0.3, pmin(0.3, true_run_effect))]
  venues[, true_wicket_effect := pmax(-0.03, pmin(0.03, true_wicket_effect))]

  venues
}


# ============================================================================
# EXPECTED VALUE CALCULATION (INVERSE OF ELO FORMULA)
# ============================================================================

#' Calculate Expected Runs from True Skills
#'
#' This is the DATA GENERATING process - the "ground truth" formula.
#' It mirrors how the ELO system calculates expected runs, so we can
#' validate that the ELO system correctly recovers these skills.
#'
#' Formula:
#'   expected_runs = baseline +
#'     w_batter * (batter_skill - baseline) +
#'     w_bowler * (baseline - bowler_skill) +  # Inverted for bowler
#'     w_venue * venue_effect
#'
#' @param batter_skill Numeric. Batter's true run skill (runs per ball).
#' @param bowler_skill Numeric. Bowler's true economy (runs per ball conceded).
#' @param venue_run_effect Numeric. Venue's additive effect on runs.
#' @param baseline Numeric. Format average runs per ball.
#' @param w_batter Numeric. Batter attribution weight.
#' @param w_bowler Numeric. Bowler attribution weight.
#' @param w_venue Numeric. Venue attribution weight.
#'
#' @return Numeric. Expected runs for this delivery.
calculate_expected_runs_from_true <- function(batter_skill,
                                               bowler_skill,
                                               venue_run_effect,
                                               baseline = SIM_BASELINE_RUNS,
                                               w_batter = SIM_W_BATTER,
                                               w_bowler = SIM_W_BOWLER,
                                               w_venue = SIM_W_VENUE_SESSION + SIM_W_VENUE_PERM) {
  # Batter contribution: how much above/below average this batter scores
  # Higher batter_skill = more runs expected (positive contribution)
  batter_contrib <- batter_skill - baseline

  # Bowler contribution: how much above/below average this bowler concedes
  # Higher economy (bowler_skill) = more runs expected (positive contribution)
  # This is NOT inverted because economy already measures "runs conceded"
  bowler_contrib <- bowler_skill - baseline

  # Venue contribution: direct effect
  venue_contrib <- venue_run_effect

  # Weighted combination
  expected <- baseline +
    w_batter * batter_contrib +
    w_bowler * bowler_contrib +
    w_venue * venue_contrib

  # Bound to valid range
  pmax(0.01, pmin(6, expected))
}


#' Calculate Expected Wicket Probability from True Skills
#'
#' Uses log-odds combination (matching the ELO wicket formula).
#'
#' Formula (on log-odds scale):
#'   adjusted_logit = base_logit +
#'     w_batter * log(bat_rate / base_rate) +  # Batter effect (higher = worse survival)
#'     w_bowler * log(bowl_rate / base_rate) +  # Bowler effect (higher = more wickets)
#'     w_venue * log((base_rate + venue_effect) / base_rate)  # Venue effect
#'
#' @param batter_wicket_rate Numeric. Batter's true wicket rate per ball.
#' @param bowler_wicket_rate Numeric. Bowler's true wicket rate per ball.
#' @param venue_wicket_effect Numeric. Venue's additive effect on wicket probability.
#' @param baseline Numeric. Format average wicket probability.
#'
#' @return Numeric. Expected wicket probability for this delivery.
calculate_expected_wicket_from_true <- function(batter_wicket_rate,
                                                 bowler_wicket_rate,
                                                 venue_wicket_effect,
                                                 baseline = SIM_BASELINE_WICKET) {
  # ADDITIVE model that matches the skill index prediction formula:
  # expected = baseline + batter_deviation + bowler_deviation + venue_effect
  #
  # This ensures the model can learn skills that exactly match the data-generating process.
  # Each entity's skill represents their DIRECT contribution to wicket probability.

  # Calculate deviations from baseline (what skill indices should learn)
  batter_deviation <- batter_wicket_rate - baseline  # Positive = gets out more
  bowler_deviation <- bowler_wicket_rate - baseline  # Positive = takes more wickets

  # Simple additive combination (matches our skill index prediction formula)
  combined <- baseline + batter_deviation + bowler_deviation + venue_wicket_effect

  # Bound to valid probability range
  pmax(0.001, pmin(0.5, combined))
}


# ============================================================================
# OUTCOME SAMPLING
# ============================================================================

#' Sample Actual Runs from Expected Runs
#'
#' Given expected runs, samples an actual run outcome (0, 1, 2, 3, 4, 6).
#' Uses a shifted probability distribution based on expected runs.
#'
#' @param expected_runs Numeric. Expected runs for this delivery.
#' @param seed Integer or NULL. Optional seed for single delivery (for testing).
#'
#' @return Integer. Sampled run outcome.
sample_runs <- function(expected_runs, seed = NULL) {
  if (!is.null(seed)) set.seed(seed)

  # Calculate shift from baseline
  shift <- expected_runs - SIM_BASELINE_RUNS

  # Base probabilities (from config)
  probs <- SIM_RUN_PROBS
  run_values <- as.integer(names(probs))

  # Adjust probabilities based on expected runs
  # Higher expected runs -> more 4s and 6s, fewer dots
  if (shift > 0) {
    # Increase boundary probability
    boundary_boost <- shift * 0.3  # 30% of shift goes to boundaries
    dot_reduction <- boundary_boost * 0.5
    probs["4"] <- probs["4"] + boundary_boost * 0.6
    probs["6"] <- probs["6"] + boundary_boost * 0.4
    probs["0"] <- probs["0"] - dot_reduction
    probs["1"] <- probs["1"] - (boundary_boost - dot_reduction)
  } else if (shift < 0) {
    # Increase dot ball probability
    dot_boost <- abs(shift) * 0.3
    probs["0"] <- probs["0"] + dot_boost
    probs["4"] <- probs["4"] - dot_boost * 0.4
    probs["6"] <- probs["6"] - dot_boost * 0.3
    probs["1"] <- probs["1"] - dot_boost * 0.3
  }

  # Ensure valid probabilities
  probs <- pmax(0.01, probs)
  probs <- probs / sum(probs)

  # Sample
  run_values[sample.int(length(run_values), 1, prob = probs)]
}


#' Sample Wicket from Expected Probability
#'
#' @param expected_wicket Numeric. Expected wicket probability.
#'
#' @return Integer. 1 if wicket, 0 otherwise.
sample_wicket <- function(expected_wicket) {
  as.integer(runif(1) < expected_wicket)
}


# ============================================================================
# DELIVERY GENERATION
# ============================================================================

#' Generate Synthetic Deliveries
#'
#' Creates a dataset of deliveries with outcomes sampled from true skill values.
#' Matches are created to simulate realistic match structure.
#'
#' @param players List from generate_synthetic_players().
#' @param venues data.table from generate_synthetic_venues().
#' @param n_deliveries Integer. Total number of deliveries to generate.
#' @param seed Integer. Random seed.
#' @param start_date Date. Starting date for matches.
#'
#' @return data.table with columns matching ELO scripts:
#'   delivery_id, match_id, match_date, batter_id, bowler_id, venue,
#'   actual_runs, is_wicket, baseline_runs, baseline_wicket, expected_runs, expected_wicket
generate_synthetic_deliveries <- function(players,
                                           venues,
                                           n_deliveries = SIM_N_TRAIN_DELIVERIES,
                                           seed = SIM_SEED,
                                           start_date = as.Date("2020-01-01")) {
  set.seed(seed + 100)

  n_batters <- nrow(players$batters)
  n_bowlers <- nrow(players$bowlers)
  n_venues <- nrow(venues)

  # Pre-allocate result vectors
  delivery_ids <- character(n_deliveries)
  match_ids <- character(n_deliveries)
  match_dates <- rep(start_date, n_deliveries)
  batter_ids <- character(n_deliveries)
  bowler_ids <- character(n_deliveries)
  venue_names <- character(n_deliveries)
  actual_runs_vec <- integer(n_deliveries)
  is_wicket_vec <- integer(n_deliveries)
  baseline_runs_vec <- rep(SIM_BASELINE_RUNS, n_deliveries)
  baseline_wicket_vec <- rep(SIM_BASELINE_WICKET, n_deliveries)
  expected_runs_vec <- numeric(n_deliveries)
  expected_wicket_vec <- numeric(n_deliveries)

  # Create lookup tables for skills
  batter_run_skill <- setNames(players$batters$true_run_skill, players$batters$batter_id)
  batter_wicket_rate <- setNames(players$batters$true_wicket_rate, players$batters$batter_id)
  bowler_economy <- setNames(players$bowlers$true_economy, players$bowlers$bowler_id)
  bowler_wicket <- setNames(players$bowlers$true_wicket_rate, players$bowlers$bowler_id)
  venue_run_effect <- setNames(venues$true_run_effect, venues$venue)
  venue_wicket_effect <- setNames(venues$true_wicket_effect, venues$venue)

  # Generate deliveries in match structure
  current_delivery <- 1
  match_num <- 1
  current_date <- start_date

  while (current_delivery <= n_deliveries) {
    # Determine match size (random within typical T20 range)
    match_size <- min(
      sample(200:280, 1),  # Typical T20 deliveries
      n_deliveries - current_delivery + 1
    )

    if (match_size <= 0) break

    # Select venue for this match
    venue_idx <- sample.int(n_venues, 1)
    current_venue <- venues$venue[venue_idx]

    # Generate match_id
    current_match_id <- sprintf("sim_match_%05d", match_num)

    # Generate deliveries for this match
    for (d in seq_len(match_size)) {
      if (current_delivery > n_deliveries) break

      # Sample batter and bowler (with some weighting for realism)
      batter_idx <- sample.int(n_batters, 1)
      bowler_idx <- sample.int(n_bowlers, 1)

      current_batter <- players$batters$batter_id[batter_idx]
      current_bowler <- players$bowlers$bowler_id[bowler_idx]

      # Calculate expected values from true skills
      exp_runs <- calculate_expected_runs_from_true(
        batter_skill = batter_run_skill[current_batter],
        bowler_skill = bowler_economy[current_bowler],
        venue_run_effect = venue_run_effect[current_venue]
      )

      exp_wicket <- calculate_expected_wicket_from_true(
        batter_wicket_rate = batter_wicket_rate[current_batter],
        bowler_wicket_rate = bowler_wicket[current_bowler],
        venue_wicket_effect = venue_wicket_effect[current_venue]
      )

      # Sample actual outcomes
      actual_runs <- sample_runs(exp_runs)
      is_wicket <- sample_wicket(exp_wicket)

      # Store results
      delivery_ids[current_delivery] <- sprintf("%s_%04d", current_match_id, d)
      match_ids[current_delivery] <- current_match_id
      match_dates[current_delivery] <- current_date
      batter_ids[current_delivery] <- current_batter
      bowler_ids[current_delivery] <- current_bowler
      venue_names[current_delivery] <- current_venue
      actual_runs_vec[current_delivery] <- actual_runs
      is_wicket_vec[current_delivery] <- is_wicket
      expected_runs_vec[current_delivery] <- exp_runs
      expected_wicket_vec[current_delivery] <- exp_wicket

      current_delivery <- current_delivery + 1
    }

    # Move to next match
    match_num <- match_num + 1
    current_date <- current_date + sample(1:3, 1)  # 1-3 days between matches
  }

  # Create data.table
  deliveries <- data.table::data.table(
    delivery_id = delivery_ids,
    match_id = match_ids,
    match_date = match_dates,
    batter_id = batter_ids,
    bowler_id = bowler_ids,
    venue = venue_names,
    actual_runs = actual_runs_vec,
    is_wicket = as.logical(is_wicket_vec),
    baseline_runs = baseline_runs_vec,
    baseline_wicket = baseline_wicket_vec,
    expected_runs_true = expected_runs_vec,
    expected_wicket_true = expected_wicket_vec
  )

  deliveries
}


# ============================================================================
# MAIN INTERFACE
# ============================================================================

#' Generate Complete Synthetic Dataset
#'
#' Convenience function that generates players, venues, and deliveries.
#'
#' @param config List. Configuration from get_simulation_config().
#'   If NULL, uses default configuration.
#' @param seed Integer. Random seed.
#'
#' @return List with:
#'   - players: List with batters and bowlers data.tables
#'   - venues: data.table with venue data
#'   - train_deliveries: data.table with training deliveries
#'   - test_deliveries: data.table with test deliveries
generate_synthetic_dataset <- function(config = NULL, seed = SIM_SEED) {
  if (is.null(config)) {
    config <- get_simulation_config()
  }

  cat("Generating synthetic dataset...\n")
  cat(sprintf("  Batters: %d, Bowlers: %d, Venues: %d\n",
              config$n_batters, config$n_bowlers, config$n_venues))

  # Generate entities
  players <- generate_synthetic_players(
    n_batters = config$n_batters,
    n_bowlers = config$n_bowlers,
    seed = seed
  )
  cat(sprintf("  Generated %d batters, %d bowlers\n",
              nrow(players$batters), nrow(players$bowlers)))

  venues <- generate_synthetic_venues(
    n_venues = config$n_venues,
    seed = seed
  )
  cat(sprintf("  Generated %d venues\n", nrow(venues)))

  # Generate training deliveries
  cat(sprintf("  Generating %s training deliveries...\n",
              format(config$n_train_deliveries, big.mark = ",")))
  train_deliveries <- generate_synthetic_deliveries(
    players = players,
    venues = venues,
    n_deliveries = config$n_train_deliveries,
    seed = seed,
    start_date = as.Date("2020-01-01")
  )

  # Generate test deliveries (starting after training data)
  train_max_date <- max(train_deliveries$match_date)
  cat(sprintf("  Generating %s test deliveries...\n",
              format(config$n_test_deliveries, big.mark = ",")))
  test_deliveries <- generate_synthetic_deliveries(
    players = players,
    venues = venues,
    n_deliveries = config$n_test_deliveries,
    seed = seed + 500,  # Different seed for test data
    start_date = train_max_date + 1
  )

  cat("  Done!\n")

  list(
    players = players,
    venues = venues,
    train_deliveries = train_deliveries,
    test_deliveries = test_deliveries,
    config = config
  )
}


# ============================================================================
# DIAGNOSTIC FUNCTIONS
# ============================================================================

#' Print Synthetic Data Summary
#'
#' @param dataset List from generate_synthetic_dataset().
print_synthetic_summary <- function(dataset) {
  cat("\n=== Synthetic Dataset Summary ===\n\n")

  cat("Batters:\n")
  cat(sprintf("  Count: %d\n", nrow(dataset$players$batters)))
  cat(sprintf("  True run skill: %.3f to %.3f (mean: %.3f)\n",
              min(dataset$players$batters$true_run_skill),
              max(dataset$players$batters$true_run_skill),
              mean(dataset$players$batters$true_run_skill)))
  cat(sprintf("  True wicket rate: %.3f to %.3f (mean: %.3f)\n",
              min(dataset$players$batters$true_wicket_rate),
              max(dataset$players$batters$true_wicket_rate),
              mean(dataset$players$batters$true_wicket_rate)))

  cat("\nBowlers:\n")
  cat(sprintf("  Count: %d\n", nrow(dataset$players$bowlers)))
  cat(sprintf("  True economy: %.3f to %.3f (mean: %.3f)\n",
              min(dataset$players$bowlers$true_economy),
              max(dataset$players$bowlers$true_economy),
              mean(dataset$players$bowlers$true_economy)))
  cat(sprintf("  True wicket rate: %.3f to %.3f (mean: %.3f)\n",
              min(dataset$players$bowlers$true_wicket_rate),
              max(dataset$players$bowlers$true_wicket_rate),
              mean(dataset$players$bowlers$true_wicket_rate)))

  cat("\nVenues:\n")
  cat(sprintf("  Count: %d\n", nrow(dataset$venues)))
  cat(sprintf("  Run effect: %.3f to %.3f\n",
              min(dataset$venues$true_run_effect),
              max(dataset$venues$true_run_effect)))
  cat(sprintf("  Wicket effect: %.4f to %.4f\n",
              min(dataset$venues$true_wicket_effect),
              max(dataset$venues$true_wicket_effect)))

  cat("\nDeliveries:\n")
  cat(sprintf("  Training: %s\n", format(nrow(dataset$train_deliveries), big.mark = ",")))
  cat(sprintf("  Test: %s\n", format(nrow(dataset$test_deliveries), big.mark = ",")))
  cat(sprintf("  Training date range: %s to %s\n",
              min(dataset$train_deliveries$match_date),
              max(dataset$train_deliveries$match_date)))
  cat(sprintf("  Test date range: %s to %s\n",
              min(dataset$test_deliveries$match_date),
              max(dataset$test_deliveries$match_date)))

  cat("\nOutcome distributions (training):\n")
  runs_table <- table(dataset$train_deliveries$actual_runs)
  cat("  Runs: ")
  cat(paste(names(runs_table), "=", format(runs_table / sum(runs_table) * 100, digits = 1), "%",
            collapse = ", "))
  cat("\n")
  cat(sprintf("  Wicket rate: %.2f%%\n",
              mean(dataset$train_deliveries$is_wicket) * 100))
}

NULL
