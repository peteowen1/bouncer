# Team ELO Optimization Functions
#
# Functions used for calculating and optimizing team ELO ratings.
# These are shared between the optimization script and the main ELO calculation.


#' Calculate Match Weights for ELO Optimization
#'
#' Assigns weights to matches based on importance (knockout stage, elite events).
#' Higher weights mean the match contributes more to the log-loss objective.
#'
#' @param matches Data frame with columns: is_knockout, event_tier
#' @param team_tiers (unused, kept for API compatibility)
#'
#' @return Numeric vector of weights (same length as matches)
#' @keywords internal
calculate_match_weights <- function(matches, team_tiers) {
  weights <- rep(1.0, nrow(matches))
  weights <- weights + ifelse(matches$is_knockout, 1.0, 0)
  weights <- weights + ifelse(matches$event_tier == 1, 0.5, 0)
  weights
}


#' Calculate ELOs for Optimization
#'
#' Calculates team ELO ratings for a set of matches using specified parameters.
#' Used during parameter optimization to evaluate different parameter combinations.
#'
#' Features:
#' - Opponent-weighted K-factor (asymmetric for winner/loser)
#' - Home advantage
#' - Cross-tier K boost
#' - Dynamic K decay based on matches played
#' - Margin of Victory (MOV) multiplier for rating changes
#'
#' @param matches Data frame of matches with columns:
#'   match_id, team1_id, team2_id, team1_wins, event_name, event_tier, home_team,
#'   unified_margin (optional, for MOV calculation)
#' @param k_max Maximum K factor for new teams
#' @param k_min Minimum K factor (floor for established teams)
#' @param k_halflife Matches until K decays halfway to k_min
#' @param cross_tier_boost K multiplier per tier difference
#' @param opp_k_boost Winner K boost when beating strong opponent (0-1)
#' @param opp_k_reduce Loser penalty reduction when losing to strong opponent (0-1)
#' @param home_advantage ELO points added to home team for probability calculation
#' @param mov_exponent Exponent for margin in MOV calculation (default 0.7)
#' @param mov_base_offset Base offset for MOV numerator (default 5)
#' @param mov_denom_base Denominator base for MOV (default 10)
#' @param mov_elo_factor ELO adjustment factor for favorite bias (default 0.004)
#' @param mov_min Minimum MOV multiplier (default 0.5)
#' @param mov_max Maximum MOV multiplier (default 2.5)
#' @param return_final_elos If TRUE, also returns final ELO values per team
#' @param elo_divisor ELO divisor (default 400, standard ELO)
#'
#' @return Data frame with match_id, team1_wins, exp_win_prob, team1_elo_before,
#'   team2_elo_before, cross_tier_weight, mov_multiplier. If return_final_elos=TRUE,
#'   returns a list with match_results and final_elos.
#' @keywords internal
calculate_elos_for_optimization <- function(matches, k_max, k_min, k_halflife, cross_tier_boost,
                                             opp_k_boost = 0, opp_k_reduce = 0,
                                             home_advantage = 0,
                                             mov_exponent = 0.7,
                                             mov_base_offset = 5,
                                             mov_denom_base = 10,
                                             mov_elo_factor = 0.004,
                                             mov_min = 0.5,
                                             mov_max = 2.5,
                                             return_final_elos = FALSE,
                                             elo_divisor = 400) {
  team_elos <- list()
  team_matches <- list()
  team_tier_ema <- list()

  n <- nrow(matches)
  exp_win_prob <- numeric(n)
  team1_elo_before <- numeric(n)
  team2_elo_before <- numeric(n)
  cross_tier_weight <- numeric(n)
  mov_multiplier <- numeric(n)  # Track MOV multipliers

  m_team1_id <- matches$team1_id
  m_team2_id <- matches$team2_id
  m_team1_wins <- matches$team1_wins
  m_event_name <- matches$event_name
  m_event_tier <- matches$event_tier
  m_home_team <- matches$home_team  # 1 = team1 home, -1 = team2 home, 0 = neutral

  # Extract unified_margin if present
  has_margin <- "unified_margin" %in% names(matches)
  m_margin <- if (has_margin) matches$unified_margin else rep(NA_real_, n)

  # Baseline for opponent strength calculation
  avg_elo <- 1500

  for (i in seq_len(n)) {
    t1 <- m_team1_id[i]
    t2 <- m_team2_id[i]
    event_tier <- m_event_tier[i]
    event_name <- m_event_name[i]

    if (is.null(team_elos[[t1]])) {
      team_elos[[t1]] <- get_tier_starting_elo(event_name)
      team_matches[[t1]] <- 0L
      team_tier_ema[[t1]] <- as.numeric(event_tier)
    }
    if (is.null(team_elos[[t2]])) {
      team_elos[[t2]] <- get_tier_starting_elo(event_name)
      team_matches[[t2]] <- 0L
      team_tier_ema[[t2]] <- as.numeric(event_tier)
    }

    t1_elo <- team_elos[[t1]]
    t2_elo <- team_elos[[t2]]
    t1_matches <- team_matches[[t1]]
    t2_matches <- team_matches[[t2]]
    t1_tier <- round(team_tier_ema[[t1]])
    t2_tier <- round(team_tier_ema[[t2]])

    team1_elo_before[i] <- t1_elo
    team2_elo_before[i] <- t2_elo

    tier_diff <- abs(t1_tier - t2_tier)
    cross_tier_weight[i] <- tier_diff * 1.0

    k_mult <- if (tier_diff == 0) 1.0 else min(1.0 + tier_diff * cross_tier_boost, 2.5)

    # Apply home advantage to expected probability calculation
    # m_home_team: 1 = team1 home, -1 = team2 home, 0 = neutral
    home_adj <- m_home_team[i] * home_advantage
    exp1 <- 1 / (1 + 10^((t2_elo - t1_elo - home_adj) / elo_divisor))
    exp_win_prob[i] <- exp1

    actual1 <- m_team1_wins[i]

    base_k1 <- k_min + (k_max - k_min) * exp(-t1_matches / k_halflife)
    base_k2 <- k_min + (k_max - k_min) * exp(-t2_matches / k_halflife)

    # Calculate opponent strength modifiers
    # Team 1's opponent is team 2, and vice versa
    t1_opp_strength <- (t2_elo - avg_elo) / 400  # t1's opponent is t2
    t2_opp_strength <- (t1_elo - avg_elo) / 400  # t2's opponent is t1

    # Asymmetric K modifiers based on who won
    if (actual1 == 1) {
      # Team 1 won: boost t1's K for beating strong opponent, reduce t2's penalty
      k1_modifier <- 1 + max(0, t1_opp_strength * opp_k_boost)
      k2_modifier <- 1 - max(0, t1_opp_strength * opp_k_reduce)
    } else {
      # Team 2 won: boost t2's K for beating strong opponent, reduce t1's penalty
      k1_modifier <- 1 - max(0, t2_opp_strength * opp_k_reduce)
      k2_modifier <- 1 + max(0, t2_opp_strength * opp_k_boost)
    }

    # Clamp modifiers to reasonable range [0.5, 2.0]
    k1_modifier <- max(0.5, min(2.0, k1_modifier))
    k2_modifier <- max(0.5, min(2.0, k2_modifier))

    k1 <- base_k1 * k_mult * k1_modifier
    k2 <- base_k2 * k_mult * k2_modifier

    # Calculate MOV multiplier (G-factor)
    margin <- m_margin[i]
    if (!is.na(margin)) {
      # Determine winner/loser for MOV calculation
      if (actual1 == 1) {
        winner_elo <- t1_elo
        loser_elo <- t2_elo
      } else {
        winner_elo <- t2_elo
        loser_elo <- t1_elo
      }

      # Calculate G-factor inline (avoid function call overhead in hot loop)
      elo_diff <- winner_elo - loser_elo
      margin_abs <- abs(margin)
      numerator <- (margin_abs + mov_base_offset)^mov_exponent
      denominator <- max(mov_denom_base + mov_elo_factor * elo_diff, 1)
      G <- max(mov_min, min(mov_max, numerator / denominator))
    } else {
      G <- 1.0  # No margin data, use neutral multiplier
    }
    mov_multiplier[i] <- G

    # Apply ELO update with MOV multiplier
    team_elos[[t1]] <- t1_elo + k1 * G * (actual1 - exp1)
    team_elos[[t2]] <- t2_elo + k2 * G * ((1 - actual1) - (1 - exp1))

    team_matches[[t1]] <- t1_matches + 1L
    team_matches[[t2]] <- t2_matches + 1L
    team_tier_ema[[t1]] <- 0.2 * event_tier + 0.8 * team_tier_ema[[t1]]
    team_tier_ema[[t2]] <- 0.2 * event_tier + 0.8 * team_tier_ema[[t2]]
  }

  match_results <- data.frame(
    match_id = matches$match_id,
    team1_wins = m_team1_wins,
    exp_win_prob = exp_win_prob,
    team1_elo_before = team1_elo_before,
    team2_elo_before = team2_elo_before,
    cross_tier_weight = cross_tier_weight,
    mov_multiplier = mov_multiplier
  )

  if (return_final_elos) {
    final_elos <- data.frame(
      team_id = names(team_elos),
      elo = unlist(team_elos),
      matches_played = unlist(team_matches[names(team_elos)]),
      primary_tier = sapply(team_tier_ema[names(team_elos)], round),
      stringsAsFactors = FALSE
    )
    return(list(match_results = match_results, final_elos = final_elos))
  }

  match_results
}


#' Calculate Weighted Log Loss
#'
#' Computes weighted log loss (cross-entropy) for probability predictions.
#' Used as the objective function for ELO parameter optimization.
#'
#' @param actual Vector of actual outcomes (0 or 1)
#' @param predicted Vector of predicted probabilities
#' @param weights Vector of weights for each observation
#' @param eps Small epsilon to avoid log(0)
#'
#' @return Scalar weighted log loss value
#' @keywords internal
calculate_weighted_log_loss <- function(actual, predicted, weights, eps = 1e-7) {
  predicted <- pmax(pmin(predicted, 1 - eps), eps)
  log_losses <- -(actual * log(predicted) + (1 - actual) * log(1 - predicted))
  sum(log_losses * weights) / sum(weights)
}


#' Get Dynamic K Factor
#'
#' Calculates the K factor for a team based on matches played.
#' Uses exponential decay from k_max to k_min.
#'
#' @param matches_played Number of matches the team has played
#' @param k_max Maximum K factor for new teams
#' @param k_min Minimum K factor floor
#' @param k_halflife Matches until K decays halfway
#'
#' @return K factor value
#'
#' @examples
#' get_dynamic_k(0, k_max = 40, k_min = 20, k_halflife = 50)
#' get_dynamic_k(100, k_max = 40, k_min = 20, k_halflife = 50)
#'
#' @export
get_dynamic_k <- function(matches_played, k_max, k_min, k_halflife) {
  k_min + (k_max - k_min) * exp(-matches_played / k_halflife)
}


#' Load Category-Specific ELO Parameters
#'
#' Loads optimized ELO parameters for a specific category from an RDS file.
#' If no optimized parameters exist, returns sensible defaults.
#'
#' @param category_name One of: "mens_club", "mens_international",
#'   "womens_club", "womens_international"
#' @param params_dir Directory containing parameter RDS files.
#'   Default is "bouncerdata/models" relative to project root.
#' @param format Match format: "t20", "odi", or "test". Default "t20".
#'
#' @return List with parameters:
#'   - K_MAX, K_MIN, K_HALFLIFE: Dynamic K factor settings
#'   - CROSS_TIER_K_BOOST: K multiplier for cross-tier matches
#'   - OPP_K_BOOST, OPP_K_REDUCE: Opponent-weighted K modifiers
#'   - HOME_ADVANTAGE: ELO points added for home team
#'   - MOV_EXPONENT, MOV_BASE_OFFSET, MOV_DENOM_BASE: MOV G-factor params
#'   - MOV_ELO_FACTOR, MOV_MIN, MOV_MAX: MOV bounds and ELO adjustment
#'   - PROPAGATION_ENABLED, PROPAGATION_FACTOR: Propagation settings
#'   - CORR_THRESHOLD, CORR_W_*: Correlation matrix settings
#'
#' @keywords internal
load_category_params <- function(category_name,
                                  params_dir = file.path("..", "bouncerdata", "models"),
                                  format = "t20") {
  # File name: team_elo_params_{format}_{category}.rds
  params_path <- file.path(params_dir, paste0("team_elo_params_", format, "_", category_name, ".rds"))

  if (file.exists(params_path)) {
    cli::cli_alert_info("Loading optimized parameters from {params_path}")
    opt_params <- readRDS(params_path)

    params <- list(
      K_MAX = opt_params$K_MAX,
      K_MIN = opt_params$K_MIN,
      K_HALFLIFE = opt_params$K_HALFLIFE,
      CROSS_TIER_K_BOOST = opt_params$CROSS_TIER_K_BOOST,
      OPP_K_BOOST = opt_params$OPP_K_BOOST %||% 0,
      OPP_K_REDUCE = opt_params$OPP_K_REDUCE %||% 0,
      HOME_ADVANTAGE = opt_params$HOME_ADVANTAGE %||% 0,
      # MOV parameters (use constants as defaults if not in saved params)
      MOV_EXPONENT = opt_params$MOV_EXPONENT %||% 0.7,
      MOV_BASE_OFFSET = opt_params$MOV_BASE_OFFSET %||% 5,
      MOV_DENOM_BASE = opt_params$MOV_DENOM_BASE %||% 10,
      MOV_ELO_FACTOR = opt_params$MOV_ELO_FACTOR %||% 0.004,
      MOV_MIN = opt_params$MOV_MIN %||% 0.5,
      MOV_MAX = opt_params$MOV_MAX %||% 2.5,
      PROPAGATION_ENABLED = FALSE,  # Disabled - causes ELO drift, normalization handles it
      PROPAGATION_FACTOR = opt_params$PROPAGATION_FACTOR,
      CORR_THRESHOLD = opt_params$CORR_THRESHOLD,
      CORR_W_OPPONENTS = opt_params$CORR_W_OPPONENTS,
      CORR_W_EVENTS = opt_params$CORR_W_EVENTS,
      CORR_W_DIRECT = opt_params$CORR_W_DIRECT
    )

    cli::cli_alert_success("K_MAX={params$K_MAX}, K_MIN={params$K_MIN}, K_HALFLIFE={params$K_HALFLIFE}")
    if (params$OPP_K_BOOST > 0 || params$OPP_K_REDUCE > 0) {
      cli::cli_alert_success("Opponent K: boost={params$OPP_K_BOOST}, reduce={params$OPP_K_REDUCE}")
    }
    if (params$HOME_ADVANTAGE > 0) {
      cli::cli_alert_success("Home advantage: {params$HOME_ADVANTAGE} ELO points")
    }
    if (!is.null(opt_params$optimized_at)) {
      cli::cli_alert_info("Optimized at: {opt_params$optimized_at} (LogLoss: {round(opt_params$log_loss, 4)})")
    }

    return(params)
  } else {
    cli::cli_alert_warning("No optimized parameters for {category_name} - using defaults")
    cli::cli_alert_info("Run analysis/optimize_elo_params.R to generate optimized parameters")

    return(list(
      K_MAX = 58,
      K_MIN = 16,
      K_HALFLIFE = 25,
      CROSS_TIER_K_BOOST = NULL,
      OPP_K_BOOST = 0,
      OPP_K_REDUCE = 0,
      HOME_ADVANTAGE = 0,
      # MOV parameters (defaults from constants)
      MOV_EXPONENT = 0.7,
      MOV_BASE_OFFSET = 5,
      MOV_DENOM_BASE = 10,
      MOV_ELO_FACTOR = 0.004,
      MOV_MIN = 0.5,
      MOV_MAX = 2.5,
      PROPAGATION_ENABLED = FALSE,  # Disabled - causes ELO drift, normalization handles it
      PROPAGATION_FACTOR = 0.15,
      CORR_THRESHOLD = 0.15,
      CORR_W_OPPONENTS = 0.5,
      CORR_W_EVENTS = 0.3,
      CORR_W_DIRECT = 0.2
    ))
  }
}
