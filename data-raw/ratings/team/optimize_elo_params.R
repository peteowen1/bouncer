# Optimize Team ELO Parameters by Category
#
# Optimizes ELO parameters separately for 4 categories per format:
#   - mens_club: Male club/franchise leagues
#   - mens_international: Male internationals
#   - womens_club: Female club/franchise leagues
#   - womens_international: Female internationals
#
# Parameters optimized per category:
#   - K_MAX: Maximum K factor (for new teams)
#   - K_MIN: Minimum K factor (floor for established teams)
#   - K_HALFLIFE: Matches until K decays halfway to K_MIN
#   - CROSS_TIER_K_BOOST: K multiplier per tier difference
#   - OPP_K_BOOST: Winner K boost when beating strong opponent
#   - OPP_K_REDUCE: Loser penalty reduction when losing to strong
#   - HOME_ADVANTAGE: ELO points added to home team
#
# Output: Parameter files named team_elo_params_{format}_{category}.rds
#
# Usage: Set FORMAT_FILTER below, run this script, then run 01_calculate_team_elos.R


# 1. Setup ----

library(DBI)
library(dplyr)
devtools::load_all()

cat("\n")
cli::cli_h1("Team ELO Parameter Optimization by Category")
cat("\n")


# 2. Configuration ----

# Format to optimize: "t20", "odi", "test", or NULL for all
FORMAT_FILTER <- NULL

# Optional filters
EVENT_FILTER <- NULL
START_SEASON <- NULL

# Format groupings (match_type values in database)
FORMAT_GROUPS <- list(
  t20 = c("T20", "IT20"),
  odi = c("ODI", "ODM"),
  test = c("Test", "MDM")
)

ELO_DIVISOR <- 400  # Standard ELO divisor (not optimized)

CATEGORIES <- list(
  mens_club = list(gender = "male", team_type = "club"),
  mens_international = list(gender = "male", team_type = "international"),
  womens_club = list(gender = "female", team_type = "club"),
  womens_international = list(gender = "female", team_type = "international")
)

# Output directory for parameter files
params_dir <- file.path("..", "bouncerdata", "models")
if (!dir.exists(params_dir)) {
  dir.create(params_dir, recursive = TRUE)
}

# Determine formats to process
if (is.null(FORMAT_FILTER)) {
  formats_to_process <- names(FORMAT_GROUPS)  # c("t20", "odi", "test")
  cli::cli_alert_info("Optimizing all formats: {paste(toupper(formats_to_process), collapse = ', ')}")
} else {
  formats_to_process <- FORMAT_FILTER
  cli::cli_alert_info("Optimizing single format: {toupper(FORMAT_FILTER)}")
}


# 3. Database Connection ----

conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)


# 4. Load Match Data ----

format_msg <- if (is.null(FORMAT_FILTER)) "ALL" else toupper(FORMAT_FILTER)
cli::cli_h2("Loading {format_msg} match data")

# Build query - load all matches if FORMAT_FILTER is NULL
# Include margin data for MOV calculation
query <- "
  SELECT
    match_id,
    match_date,
    team1,
    team2,
    gender,
    team_type,
    event_name,
    event_group,
    event_match_number,
    outcome_winner,
    venue,
    match_type,
    -- Margin data for MOV calculation
    unified_margin,
    outcome_by_runs,
    outcome_by_wickets,
    overs_per_innings
  FROM matches
  WHERE outcome_winner IS NOT NULL
    AND outcome_winner != ''
    AND team_type IS NOT NULL
"

query_params <- list()

# Add format filter only if specific format requested
if (!is.null(FORMAT_FILTER)) {
  match_types <- FORMAT_GROUPS[[FORMAT_FILTER]]
  if (is.null(match_types)) {
    stop("Invalid FORMAT_FILTER: ", FORMAT_FILTER, ". Must be one of: t20, odi, test")
  }
  placeholders <- paste(rep("?", length(match_types)), collapse = ", ")
  query <- paste0(query, " AND match_type IN (", placeholders, ")")
  query_params <- c(query_params, as.list(match_types))
}

# Add optional event filter
if (!is.null(EVENT_FILTER)) {
  query <- paste0(query, " AND event_name LIKE ?")
  query_params <- c(query_params, paste0("%", EVENT_FILTER, "%"))
}

# Add optional season filter
if (!is.null(START_SEASON)) {
  query <- paste0(query, " AND season >= ?")
  query_params <- c(query_params, START_SEASON)
}

query <- paste0(query, " ORDER BY match_date, match_id")

if (length(query_params) > 0) {
  all_matches <- DBI::dbGetQuery(conn, query, params = query_params)
} else {
  all_matches <- DBI::dbGetQuery(conn, query)
}

cli::cli_alert_success("Loaded {nrow(all_matches)} total {format_msg} matches")

# Show category breakdown
cat_summary <- all_matches %>%
  group_by(gender, team_type) %>%
  summarise(n = n(), .groups = "drop")

for (i in seq_len(nrow(cat_summary))) {
  row <- cat_summary[i, ]
  cli::cli_alert_info("{row$gender}_{row$team_type}: {row$n} matches")
}

# Report margin data availability
n_with_margin <- sum(!is.na(all_matches$unified_margin))
cli::cli_alert_info("Matches with unified_margin: {n_with_margin}/{nrow(all_matches)} ({round(n_with_margin/nrow(all_matches)*100, 1)}%)")


# 5. Placeholder for Home Lookups (built per-format in loop) ----

# home_lookups built inside format loop using build_home_lookups()


# 6. Define Objective Function ----

# Uses functions from R/team_elo_optimization.R:
#   - calculate_elos_for_optimization()
#   - calculate_match_weights()
#   - calculate_weighted_log_loss()

# Objective function with all parameters (K + MOV)
# First 7 params are K-related, next 6 are MOV-related
objective_fn_full <- function(params, matches, base_weights, use_regularization = TRUE) {
  # K parameters
  k_max <- params[1]
  k_min <- params[2]
  k_halflife <- params[3]
  cross_tier_boost <- params[4]
  opp_k_boost <- params[5]
  opp_k_reduce <- params[6]
  home_advantage <- params[7]

  # MOV parameters
  mov_exponent <- params[8]
  mov_base_offset <- params[9]
  mov_denom_base <- params[10]
  mov_elo_factor <- params[11]
  mov_min <- params[12]
  mov_max <- params[13]

  # Hard constraints
  if (k_max < k_min || k_min < 1 || k_halflife < 1 || cross_tier_boost < 0) {
    return(Inf)
  }
  if (opp_k_boost < 0 || opp_k_reduce < 0 || opp_k_boost > 1 || opp_k_reduce > 1) {
    return(Inf)
  }
  if (home_advantage < 0 || home_advantage > 200) {
    return(Inf)
  }
  if (mov_exponent <= 0 || mov_exponent > 1.5) {
    return(Inf)
  }
  if (mov_min <= 0 || mov_max < mov_min || mov_max > 5) {
    return(Inf)
  }
  if (mov_base_offset < 0 || mov_denom_base < 1) {
    return(Inf)
  }

  results <- calculate_elos_for_optimization(
    matches, k_max, k_min, k_halflife, cross_tier_boost,
    opp_k_boost, opp_k_reduce, home_advantage,
    mov_exponent, mov_base_offset, mov_denom_base,
    mov_elo_factor, mov_min, mov_max,
    elo_divisor = ELO_DIVISOR
  )

  total_weights <- base_weights + results$cross_tier_weight
  log_loss <- calculate_weighted_log_loss(results$team1_wins, results$exp_win_prob, total_weights)

  if (use_regularization) {
    # Regularization penalties (soft constraints to avoid extreme values)
    penalty <- 0

    # Prefer moderate K_MAX (penalty increases above 60)
    penalty <- penalty + 0.0005 * max(0, k_max - 60)^2

    # Prefer smaller OPP modifiers (they amplify with MOV)
    penalty <- penalty + 0.002 * (opp_k_boost^2 + opp_k_reduce^2)

    # Prefer moderate MOV_MAX (extreme values cause volatility)
    penalty <- penalty + 0.001 * max(0, mov_max - 2.5)^2

    # Small penalty for large cross_tier boost
    penalty <- penalty + 0.001 * max(0, cross_tier_boost - 0.5)^2

    log_loss <- log_loss + penalty
  }

  log_loss
}

# Simplified objective for Stage 1 (K params only, default MOV)
objective_fn <- function(params, matches, base_weights) {
  # Use defaults for MOV params
  full_params <- c(params, 0.7, 5, 10, 0.004, 0.5, 2.5)
  objective_fn_full(full_params, matches, base_weights, use_regularization = TRUE)
}

# Objective for Stage 2 (MOV params only, fixed K)
objective_fn_mov <- function(mov_params, k_params, matches, base_weights) {
  full_params <- c(k_params, mov_params)
  objective_fn_full(full_params, matches, base_weights, use_regularization = TRUE)
}


# 7. Results Storage ----

all_results <- list()


# 8. Optimize Each Format and Category ----

for (current_format in formats_to_process) {
  cat("\n")
  cli::cli_h1("Format: {toupper(current_format)}")
  cat("\n")

  # Build format-specific home lookups
  home_lookups <- build_home_lookups(all_matches, current_format)
  cli::cli_alert_info("Club home venues: {length(home_lookups$club_home)} teams")
  cli::cli_alert_info("Venue->country mappings: {length(home_lookups$venue_country)} venues")

  for (category_name in names(CATEGORIES)) {
    cat_config <- CATEGORIES[[category_name]]

    cat("\n")
    cli::cli_h2("Category: {category_name}")
    cat("\n")

    # 8.1 Filter matches by format AND category ----
    format_match_types <- FORMAT_GROUPS[[current_format]]
    matches <- all_matches %>%
      filter(
        gender == cat_config$gender,
        team_type == cat_config$team_type,
        match_type %in% format_match_types
      )

    if (nrow(matches) < 100) {
      cli::cli_alert_warning("Only {nrow(matches)} matches - skipping optimization")
      next
    }

    cli::cli_alert_success("Processing {nrow(matches)} matches")

    # 8.2 Create team IDs and features ----
    matches <- matches %>%
      mutate(
        team1_id = make_team_id_vec(team1, gender, current_format, team_type),
        team2_id = make_team_id_vec(team2, gender, current_format, team_type),
        outcome_winner_id = make_team_id_vec(outcome_winner, gender, current_format, team_type),
        team1_wins = as.integer(outcome_winner_id == team1_id),
        event_tier = sapply(event_name, get_event_tier),
        is_knockout = grepl("final|semi|qualifier|eliminator|playoff|knockout",
                            paste(event_group, event_match_number), ignore.case = TRUE)
      ) %>%
      mutate(
        # detect_home_team() is from R/venue_country_lookup.R
        home_team = mapply(detect_home_team, team1, team2, team1_id, team2_id, venue, team_type,
                           MoreArgs = list(club_home_lookup = home_lookups$club_home,
                                           venue_country_lookup = home_lookups$venue_country))
      )

  n_teams <- length(unique(c(matches$team1_id, matches$team2_id)))
  cli::cli_alert_info("Unique teams: {n_teams}")

  # Show home detection stats
  n_home <- sum(matches$home_team != 0)
  cli::cli_alert_info("Home matches detected: {n_home}/{nrow(matches)} ({round(n_home/nrow(matches)*100, 1)}%)")

  # Calculate base weights (from R/team_elo_optimization.R)
  base_weights <- calculate_match_weights(matches, NULL)

  # 8.3 Grid search ----
  cli::cli_h2("Grid Search")

  k_max_grid <- c(50, 75, 100)
  k_min_grid <- c(15, 25)
  k_halflife_grid <- c(15, 25, 35)
  cross_tier_boost_grid <- c(0.2, 0.4, 0.6)
  opp_k_boost_grid <- c(0.0, 0.3, 0.5)
  opp_k_reduce_grid <- c(0.0, 0.2, 0.3)
  home_advantage_grid <- c(0, 40, 80, 120)

  param_grid <- expand.grid(
    k_max = k_max_grid,
    k_min = k_min_grid,
    k_halflife = k_halflife_grid,
    cross_tier_boost = cross_tier_boost_grid,
    opp_k_boost = opp_k_boost_grid,
    opp_k_reduce = opp_k_reduce_grid,
    home_advantage = home_advantage_grid,
    stringsAsFactors = FALSE
  ) %>% filter(k_max > k_min)

  cli::cli_alert_info("Testing {nrow(param_grid)} combinations...")

  results_grid <- data.frame()
  pb <- cli::cli_progress_bar("Grid search", total = nrow(param_grid))

  for (i in seq_len(nrow(param_grid))) {
    row <- param_grid[i, ]
    params <- c(row$k_max, row$k_min, row$k_halflife, row$cross_tier_boost,
                row$opp_k_boost, row$opp_k_reduce, row$home_advantage)
    log_loss <- objective_fn(params, matches, base_weights)

    results_grid <- rbind(results_grid, data.frame(
      k_max = row$k_max,
      k_min = row$k_min,
      k_halflife = row$k_halflife,
      cross_tier_boost = row$cross_tier_boost,
      opp_k_boost = row$opp_k_boost,
      opp_k_reduce = row$opp_k_reduce,
      home_advantage = row$home_advantage,
      log_loss = log_loss
    ))

    cli::cli_progress_update(id = pb)
  }
  cli::cli_progress_done(id = pb)

  results_grid <- results_grid %>% arrange(log_loss)
  best_k <- results_grid[1, ]

  cli::cli_alert_success("Best from grid: K_MAX={best_k$k_max}, K_MIN={best_k$k_min}, OPP_K={best_k$opp_k_boost}/{best_k$opp_k_reduce}, HomeAdv={best_k$home_advantage}, LogLoss={round(best_k$log_loss, 4)}")

  # 8.4 Fine-tuning Stage 1 (K parameters) ----
  cli::cli_h2("Stage 1: Fine-tuning K parameters")

  start_params <- c(best_k$k_max, best_k$k_min, best_k$k_halflife, best_k$cross_tier_boost,
                    best_k$opp_k_boost, best_k$opp_k_reduce, best_k$home_advantage)

  opt_result <- optim(
    par = start_params,
    fn = objective_fn,
    matches = matches,
    base_weights = base_weights,
    method = "Nelder-Mead",
    control = list(maxit = 100)
  )

  k_params <- opt_result$par
  k_params_loss <- opt_result$value

  cli::cli_alert_success("K params: K_MAX={round(k_params[1])}, K_MIN={round(k_params[2])}, OPP_K={round(k_params[5],2)}/{round(k_params[6],2)}, HomeAdv={round(k_params[7])}, LogLoss={round(k_params_loss, 4)}")

  # 8.4b Stage 2: Fine-tuning MOV parameters ----
  cli::cli_h2("Stage 2: Fine-tuning MOV parameters")

  # Grid search for MOV params (fewer combinations since these are less sensitive)
  mov_exponent_grid <- c(0.5, 0.7, 0.9)
  mov_base_offset_grid <- c(3, 5, 8)
  mov_max_grid <- c(2.0, 2.5, 3.0)

  # Keep other MOV params at defaults for grid
  default_mov_denom <- 10
  default_mov_elo_factor <- 0.004
  default_mov_min <- 0.5

  mov_grid <- expand.grid(
    mov_exponent = mov_exponent_grid,
    mov_base_offset = mov_base_offset_grid,
    mov_max = mov_max_grid,
    stringsAsFactors = FALSE
  )

  cli::cli_alert_info("Testing {nrow(mov_grid)} MOV combinations...")

  best_mov_loss <- Inf
  best_mov_params <- c(0.7, 5, 10, 0.004, 0.5, 2.5)

  for (i in seq_len(nrow(mov_grid))) {
    row <- mov_grid[i, ]
    mov_params <- c(row$mov_exponent, row$mov_base_offset, default_mov_denom,
                    default_mov_elo_factor, default_mov_min, row$mov_max)
    mov_loss <- objective_fn_mov(mov_params, k_params, matches, base_weights)

    if (mov_loss < best_mov_loss) {
      best_mov_loss <- mov_loss
      best_mov_params <- mov_params
    }
  }

  cli::cli_alert_success("Best MOV: exp={best_mov_params[1]}, offset={best_mov_params[2]}, max={best_mov_params[6]}, LogLoss={round(best_mov_loss, 4)}")

  # 8.4c Stage 3: Joint Nelder-Mead on all params ----
  cli::cli_h2("Stage 3: Joint fine-tuning (all parameters)")

  full_start <- c(k_params, best_mov_params)

  final_opt <- optim(
    par = full_start,
    fn = objective_fn_full,
    matches = matches,
    base_weights = base_weights,
    use_regularization = TRUE,
    method = "Nelder-Mead",
    control = list(maxit = 200)
  )

  fine_tuned <- final_opt$par
  fine_tuned_loss <- final_opt$value

  cli::cli_alert_success("Final: K_MAX={round(fine_tuned[1])}, K_MIN={round(fine_tuned[2])}, MOV_max={round(fine_tuned[13],2)}, LogLoss={round(fine_tuned_loss, 4)}")

  # 8.5 Calculate accuracy ----
  # Use full parameters (K + MOV)
  final_output <- calculate_elos_for_optimization(
    matches,
    k_max = round(fine_tuned[1]),
    k_min = round(fine_tuned[2]),
    k_halflife = round(fine_tuned[3]),
    cross_tier_boost = fine_tuned[4],
    opp_k_boost = fine_tuned[5],
    opp_k_reduce = fine_tuned[6],
    home_advantage = fine_tuned[7],
    mov_exponent = fine_tuned[8],
    mov_base_offset = fine_tuned[9],
    mov_denom_base = fine_tuned[10],
    mov_elo_factor = fine_tuned[11],
    mov_min = fine_tuned[12],
    mov_max = fine_tuned[13],
    return_final_elos = TRUE,
    elo_divisor = ELO_DIVISOR
  )

  final_results <- final_output$match_results
  final_results$predicted_team1_wins <- as.integer(final_results$exp_win_prob > 0.5)
  accuracy <- mean(final_results$predicted_team1_wins == final_results$team1_wins)

  cli::cli_alert_success("Accuracy: {round(accuracy * 100, 1)}%")

  # 8.6 Save parameters ----
  optimized_params <- list(
    # Metadata
    category = category_name,
    format = current_format,
    gender = cat_config$gender,
    team_type = cat_config$team_type,
    # K parameters
    K_MAX = round(fine_tuned[1]),
    K_MIN = round(fine_tuned[2]),
    K_HALFLIFE = round(fine_tuned[3]),
    CROSS_TIER_K_BOOST = round(fine_tuned[4], 3),
    OPP_K_BOOST = round(fine_tuned[5], 3),
    OPP_K_REDUCE = round(fine_tuned[6], 3),
    HOME_ADVANTAGE = round(fine_tuned[7]),
    # MOV parameters
    MOV_EXPONENT = round(fine_tuned[8], 3),
    MOV_BASE_OFFSET = round(fine_tuned[9], 2),
    MOV_DENOM_BASE = round(fine_tuned[10], 2),
    MOV_ELO_FACTOR = round(fine_tuned[11], 4),
    MOV_MIN = round(fine_tuned[12], 2),
    MOV_MAX = round(fine_tuned[13], 2),
    # Propagation settings
    PROPAGATION_FACTOR = 0.15,
    CORR_THRESHOLD = 0.15,
    CORR_W_OPPONENTS = 0.5,
    CORR_W_EVENTS = 0.3,
    CORR_W_DIRECT = 0.2,
    # Metrics
    log_loss = fine_tuned_loss,
    accuracy = accuracy,
    n_matches = nrow(matches),
    n_teams = n_teams,
    n_home_matches = n_home,
    n_margin_matches = sum(!is.na(matches$unified_margin)),
    optimized_at = Sys.time()
  )

  # File name: team_elo_params_{format}_{category}.rds
  params_path <- file.path(params_dir, paste0("team_elo_params_", current_format, "_", category_name, ".rds"))
  saveRDS(optimized_params, params_path)
  cli::cli_alert_success("Saved to {params_path}")

  # Store with format key for summary
  result_key <- paste0(current_format, "_", category_name)
  all_results[[result_key]] <- optimized_params

  # 8.7 Apply propagation ----
  cli::cli_h2("Applying Propagation")

  all_team_ids <- unique(c(matches$team1_id, matches$team2_id))
  team_opponents <- setNames(vector("list", length(all_team_ids)), all_team_ids)
  team_events_history <- setNames(vector("list", length(all_team_ids)), all_team_ids)
  team_matchups <- setNames(vector("list", length(all_team_ids)), all_team_ids)
  match_results_list <- list()

  for (i in seq_len(nrow(matches))) {
    t1 <- matches$team1_id[i]
    t2 <- matches$team2_id[i]
    ev <- matches$event_name[i]
    winner <- matches$outcome_winner_id[i]

    team_opponents[[t1]] <- c(team_opponents[[t1]], t2)
    team_opponents[[t2]] <- c(team_opponents[[t2]], t1)
    team_events_history[[t1]] <- c(team_events_history[[t1]], ev)
    team_events_history[[t2]] <- c(team_events_history[[t2]], ev)

    if (is.null(team_matchups[[t1]])) team_matchups[[t1]] <- list()
    if (is.null(team_matchups[[t2]])) team_matchups[[t2]] <- list()
    team_matchups[[t1]][[t2]] <- (team_matchups[[t1]][[t2]] %||% 0L) + 1L
    team_matchups[[t2]][[t1]] <- (team_matchups[[t2]][[t1]] %||% 0L) + 1L

    if (!is.na(winner) && winner != "") {
      loser <- if (winner == t1) t2 else t1
      match_results_list[[length(match_results_list) + 1]] <- data.frame(
        winner_id = winner, loser_id = loser, delta = 10,
        match_date = matches$match_date[i]
      )
    }
  }

  match_results_df <- do.call(rbind, match_results_list)

  pre_prop_elos <- final_output$final_elos
  team_elos_vec <- setNames(pre_prop_elos$elo, pre_prop_elos$team_id)

  cli::cli_alert_info("Building correlation matrix for {length(all_team_ids)} teams...")
  correlation_matrix <- build_correlation_matrix(
    all_team_ids = all_team_ids,
    team_opponents = team_opponents,
    team_events = team_events_history,
    team_matchups = team_matchups,
    threshold = optimized_params$CORR_THRESHOLD,
    w_opponents = optimized_params$CORR_W_OPPONENTS,
    w_events = optimized_params$CORR_W_EVENTS,
    w_direct = optimized_params$CORR_W_DIRECT
  )

  post_prop_elos <- apply_propagation_post(
    team_elos = team_elos_vec,
    match_results = match_results_df,
    correlation_matrix = correlation_matrix,
    propagation_factor = optimized_params$PROPAGATION_FACTOR
  )

  comparison <- data.frame(
    team_id = names(team_elos_vec),
    pre_elo = unname(team_elos_vec),
    post_elo = unname(post_prop_elos[names(team_elos_vec)]),
    stringsAsFactors = FALSE
  ) %>%
    mutate(
      adjustment = post_elo - pre_elo,
      team_name = gsub("_(male|female)_(t20|odi|test)_(club|international)$", "", team_id)
    ) %>%
    left_join(pre_prop_elos %>% select(team_id, matches_played), by = "team_id")

  # 8.8 Show top 20 teams ----
  cli::cli_h3("Top 20 Teams (Post-Propagation)")
  cat("\n")
  cat(sprintf("%-4s %-25s %8s %8s %10s %8s\n",
              "Rank", "Team", "Pre", "Post", "Adjust", "Matches"))
  cat(paste(rep("-", 70), collapse = ""), "\n")

  top_20_post <- comparison %>%
    arrange(desc(post_elo)) %>%
    head(20)

  for (i in seq_len(nrow(top_20_post))) {
    row <- top_20_post[i, ]
    cat(sprintf("%-4d %-25s %8.0f %8.0f %+10.1f %8d\n",
                i,
                substr(row$team_name, 1, 25),
                row$pre_elo,
                row$post_elo,
                row$adjustment,
                row$matches_played))
  }

  # 8.9 Show biggest adjustments ----
  cli::cli_h3("Biggest Adjustments")

  cat("\nTeams that DROPPED most:\n")
  dropped <- comparison %>% arrange(adjustment) %>% head(5)
  for (i in seq_len(nrow(dropped))) {
    row <- dropped[i, ]
    cat(sprintf("  %-25s: %+.1f (was #%d, now #%d)\n",
                substr(row$team_name, 1, 25),
                row$adjustment,
                which(comparison$team_id[order(-comparison$pre_elo)] == row$team_id),
                which(comparison$team_id[order(-comparison$post_elo)] == row$team_id)))
  }

  cat("\nTeams that ROSE most:\n")
  rose <- comparison %>% arrange(desc(adjustment)) %>% head(5)
  for (i in seq_len(nrow(rose))) {
    row <- rose[i, ]
    cat(sprintf("  %-25s: %+.1f (was #%d, now #%d)\n",
                substr(row$team_name, 1, 25),
                row$adjustment,
                which(comparison$team_id[order(-comparison$pre_elo)] == row$team_id),
                which(comparison$team_id[order(-comparison$post_elo)] == row$team_id)))
  }

  all_results[[result_key]]$comparison <- comparison

  # 8.10 Strength of schedule analysis ----
  cli::cli_h3("Strength of Schedule Analysis")

  avg_opp_elo <- sapply(all_team_ids, function(team) {
    opps <- team_opponents[[team]]
    if (length(opps) == 0) return(NA)
    mean(team_elos_vec[opps], na.rm = TRUE)
  })

  comparison$avg_opp_elo <- avg_opp_elo[comparison$team_id]
  comparison$elo_vs_opp <- comparison$pre_elo - comparison$avg_opp_elo

  cat("\nTop 15 teams by POST-propagation ELO with Strength of Schedule:\n")
  cat(sprintf("%-4s %-22s %7s %7s %8s %10s\n",
              "Rank", "Team", "Post", "AvgOpp", "Diff", "Diagnosis"))
  cat(paste(rep("-", 65), collapse = ""), "\n")

  top_15 <- comparison %>%
    arrange(desc(post_elo)) %>%
    head(15)

  for (i in seq_len(nrow(top_15))) {
    row <- top_15[i, ]
    diagnosis <- case_when(
      row$elo_vs_opp > 200 ~ "OVERRATED?",
      row$elo_vs_opp < 50 ~ "PROVEN",
      TRUE ~ "OK"
    )
    cat(sprintf("%-4d %-22s %7.0f %7.0f %+8.0f %10s\n",
                i,
                substr(row$team_name, 1, 22),
                row$post_elo,
                row$avg_opp_elo,
                row$elo_vs_opp,
                diagnosis))
  }

  cat("\n")
  } # End category loop
} # End format loop


# 9. Summary ----

PREVIOUS_RESULTS <- list(
  t20_mens_club = list(log_loss = 0.685, accuracy = 56.3),
  t20_mens_international = list(log_loss = 0.6224, accuracy = 64.8),
  t20_womens_club = list(log_loss = 0.6626, accuracy = 60.1),
  t20_womens_international = list(log_loss = 0.5597, accuracy = 72.8)
)

cat("\n")
cli::cli_h1("Optimization Summary")
cat("\n")

cat(sprintf("%-10s %-20s %5s %5s %5s %6s %6s %5s %8s %7s %7s\n",
            "Format", "Category", "K_MAX", "K_MIN", "Half", "OppB", "OppR", "Home", "LogLoss", "Acc", "Matches"))
cat(paste(rep("-", 110), collapse = ""), "\n")

for (result_key in names(all_results)) {
  params <- all_results[[result_key]]

  cat(sprintf("%-10s %-20s %5d %5d %5d %6.2f %6.2f %5d %8.4f %6.1f%% %7d\n",
              toupper(params$format),
              params$category,
              params$K_MAX,
              params$K_MIN,
              params$K_HALFLIFE,
              params$OPP_K_BOOST,
              params$OPP_K_REDUCE,
              params$HOME_ADVANTAGE,
              params$log_loss,
              params$accuracy * 100,
              params$n_matches))
}


# 10. Comparison with Previous (T20 only) ----

cat("\n")
cli::cli_h2("Comparison with Previous T20 (before home advantage)")
cat(sprintf("%-25s %12s %10s %12s %10s\n",
            "Category", "LL (prev)", "LL (new)", "Acc (prev)", "Acc (new)"))
cat(paste(rep("-", 75), collapse = ""), "\n")

for (result_key in names(all_results)) {
  params <- all_results[[result_key]]
  prev <- PREVIOUS_RESULTS[[result_key]]

  if (is.null(prev)) next  # Only T20 has previous results

  ll_change <- params$log_loss - prev$log_loss
  acc_change <- params$accuracy * 100 - prev$accuracy

  cat(sprintf("%-25s %12.4f %10.4f %11.1f%% %9.1f%%  %s%s\n",
              result_key,
              prev$log_loss,
              params$log_loss,
              prev$accuracy,
              params$accuracy * 100,
              ifelse(ll_change < 0, "v ", ""),
              ifelse(ll_change < -0.001, sprintf("LL improved by %.4f", -ll_change), "")
  ))
}

cat("\n")
formats_processed <- paste(toupper(formats_to_process), collapse = ", ")
cli::cli_alert_success("All parameter files saved for: {formats_processed}!")
cli::cli_alert_info("Run 01_calculate_team_elos.R to apply these parameters")
cat("\n")
