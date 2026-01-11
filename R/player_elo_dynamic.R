# Player ELO Dynamic K-Factor System
#
# Functions for calculating dynamic K-factors and opponent adjustments
# to prevent ELO inflation from weak-vs-weak matchups.
#
# Key features:
#   1. Dynamic K based on deliveries (new players learn faster)
#   2. Opponent strength adjustment (beating strong players = more gain)
#   3. Tier-based starting ELOs (IPL players start higher than minor leagues)


#' Get Dynamic K-Factor for Run ELO
#'
#' Calculates the K-factor for Run ELO updates based on player experience.
#' New players have higher K-factors (faster learning), experienced players
#' have lower K-factors (more stable ratings).
#'
#' Formula: K = K_MIN + (K_MAX - K_MIN) * exp(-deliveries / K_HALFLIFE)
#'
#' @param deliveries Integer. Number of deliveries the player has faced/bowled.
#' @param format Character. Match format: "t20", "odi", or "test".
#'
#' @return Numeric. The K-factor to use for this delivery's ELO update.
#' @keywords internal
get_player_dynamic_k_run <- function(deliveries, format = "t20") {
  format <- tolower(format)

  # Get format-specific parameters
  params <- switch(format,
    "t20" = list(
      k_max = PLAYER_K_RUN_MAX_T20,
      k_min = PLAYER_K_RUN_MIN_T20,
      halflife = PLAYER_K_RUN_HALFLIFE_T20
    ),
    "odi" = list(
      k_max = PLAYER_K_RUN_MAX_ODI,
      k_min = PLAYER_K_RUN_MIN_ODI,
      halflife = PLAYER_K_RUN_HALFLIFE_ODI
    ),
    "test" = list(
      k_max = PLAYER_K_RUN_MAX_TEST,
      k_min = PLAYER_K_RUN_MIN_TEST,
      halflife = PLAYER_K_RUN_HALFLIFE_TEST
    ),
    # Default to T20
    list(
      k_max = PLAYER_K_RUN_MAX_T20,
      k_min = PLAYER_K_RUN_MIN_T20,
      halflife = PLAYER_K_RUN_HALFLIFE_T20
    )
  )

  # Exponential decay from max to min
  params$k_min + (params$k_max - params$k_min) * exp(-deliveries / params$halflife)
}


#' Get Dynamic K-Factor for Wicket ELO
#'
#' Calculates the K-factor for Wicket ELO updates based on player experience.
#' Wicket ELO has lower K-factors than Run ELO because wickets are rarer
#' but more decisive events.
#'
#' @param deliveries Integer. Number of deliveries the player has faced/bowled.
#' @param format Character. Match format: "t20", "odi", or "test".
#'
#' @return Numeric. The K-factor to use for this delivery's wicket ELO update.
#' @keywords internal
get_player_dynamic_k_wicket <- function(deliveries, format = "t20") {
  format <- tolower(format)

  params <- switch(format,
    "t20" = list(
      k_max = PLAYER_K_WICKET_MAX_T20,
      k_min = PLAYER_K_WICKET_MIN_T20,
      halflife = PLAYER_K_WICKET_HALFLIFE_T20
    ),
    "odi" = list(
      k_max = PLAYER_K_WICKET_MAX_ODI,
      k_min = PLAYER_K_WICKET_MIN_ODI,
      halflife = PLAYER_K_WICKET_HALFLIFE_ODI
    ),
    "test" = list(
      k_max = PLAYER_K_WICKET_MAX_TEST,
      k_min = PLAYER_K_WICKET_MIN_TEST,
      halflife = PLAYER_K_WICKET_HALFLIFE_TEST
    ),
    list(
      k_max = PLAYER_K_WICKET_MAX_T20,
      k_min = PLAYER_K_WICKET_MIN_T20,
      halflife = PLAYER_K_WICKET_HALFLIFE_T20
    )
  )

  params$k_min + (params$k_max - params$k_min) * exp(-deliveries / params$halflife)
}


#' Get Opponent Strength K-Factor Modifier
#'
#' Calculates a K-factor modifier based on opponent strength.
#' - Beating a strong opponent (high ELO) boosts your K-factor (bigger gain)
#' - Losing to a weak opponent (low ELO) increases your penalty
#' - Beating a weak opponent or losing to a strong one has reduced effect
#'
#' This prevents ELO inflation when weak players only face weak opponents.
#'
#' When opponent_team_elo is provided, the opponent strength is calculated as
#' a blend of individual player ELO (30%) and team ELO (70%). Team ELO is more
#' reliable because it's anchored through cross-league matches.
#'
#' @param player_elo Numeric. The player's current ELO.
#' @param opponent_elo Numeric. The opponent's current ELO.
#' @param player_won Logical. Whether this delivery outcome favored the player.
#'   For batters: TRUE if scored runs (not out). For bowlers: TRUE if got wicket.
#' @param opponent_team_elo Numeric or NULL. The opponent's team ELO (optional).
#'   When provided, blended with opponent player ELO for more accurate strength.
#' @param avg_elo Numeric. The average ELO (default 1500) for reference.
#' @param team_weight Numeric. Weight for team ELO in blend (default 0.7).
#'
#' @return Numeric. A K-factor multiplier (typically 0.5 to 1.5).
#' @keywords internal
get_player_opponent_k_modifier <- function(player_elo, opponent_elo,
                                            player_won,
                                            opponent_team_elo = NULL,
                                            avg_elo = 1500,
                                            team_weight = 0.7) {
  # Player component of opponent strength

  player_opp_strength <- (opponent_elo - avg_elo) / 400

  # Blend with team ELO if available
  if (!is.null(opponent_team_elo) && !is.na(opponent_team_elo)) {
    team_opp_strength <- (opponent_team_elo - avg_elo) / 400
    opp_strength <- (1 - team_weight) * player_opp_strength +
                    team_weight * team_opp_strength
  } else {
    opp_strength <- player_opp_strength
  }

  if (player_won) {
    # Good outcome: boost K for strong opponent, REDUCE for weak opponent
    # opp_strength > 0 (strong): k_modifier > 1 (bigger gain)
    # opp_strength < 0 (weak): k_modifier < 1 (smaller gain)
    k_modifier <- 1 + opp_strength * PLAYER_OPP_K_BOOST
  } else {
    # Bad outcome: reduce penalty for strong opponent, INCREASE for weak
    # opp_strength > 0 (strong): k_modifier < 1 (smaller penalty)
    # opp_strength < 0 (weak): k_modifier > 1 (bigger penalty)
    k_modifier <- 1 - opp_strength * PLAYER_OPP_K_REDUCE
  }

  # Clamp to reasonable range
  max(0.5, min(1.5, k_modifier))
}


#' Get Player Tier Starting ELO
#'
#' Returns the starting ELO for a new player based on the tier of the event
#' they first appear in. Players debuting in elite events (IPL, BBL) start
#' higher than players debuting in development tournaments.
#'
#' This leverages the existing event tier system from team ELO.
#'
#' @param event_name Character. The event/tournament name.
#'
#' @return Numeric. Starting ELO for the player (1400-1525).
#' @keywords internal
get_player_tier_starting_elo <- function(event_name) {
  tier <- get_event_tier(event_name)
  switch(tier,
    PLAYER_TIER_1_START_ELO,  # Tier 1: 1525
    PLAYER_TIER_2_START_ELO,  # Tier 2: 1500
    PLAYER_TIER_3_START_ELO,  # Tier 3: 1450
    PLAYER_TIER_4_START_ELO   # Tier 4: 1400
  )
}


#' Calculate Player K-Factor with All Adjustments
#'
#' Combines all K-factor adjustments into a single value:
#' 1. Base K from dynamic decay (experience)
#' 2. Opponent strength modifier
#'
#' @param deliveries Integer. Player's total deliveries faced/bowled.
#' @param player_elo Numeric. Player's current ELO.
#' @param opponent_elo Numeric. Opponent's current ELO.
#' @param player_won Logical. Whether this delivery favored the player.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param elo_type Character. "run" or "wicket" to select K parameters.
#'
#' @return Numeric. The final adjusted K-factor.
#' @keywords internal
get_player_adjusted_k <- function(deliveries, player_elo, opponent_elo,
                                   player_won, format = "t20",
                                   elo_type = "run") {
  # Get base K from experience
  base_k <- if (elo_type == "wicket") {
    get_player_dynamic_k_wicket(deliveries, format)
  } else {
    get_player_dynamic_k_run(deliveries, format)
  }

  # Get opponent strength modifier
  opp_modifier <- get_player_opponent_k_modifier(
    player_elo, opponent_elo, player_won
  )

  # Final K
  base_k * opp_modifier
}


#' Build Player ELO Parameters for Format
#'
#' Constructs a parameter list for player ELO calculation, including
#' both static parameters (scoring weights) and dynamic K-factor settings.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param use_dynamic_k Logical. If TRUE, includes dynamic K parameters.
#'   If FALSE, uses static K-factors from original system.
#'
#' @return List with all parameters needed for ELO calculation.
#' @keywords internal
build_player_elo_params <- function(format = "t20", use_dynamic_k = TRUE) {
 format <- tolower(format)

  # Static K-factors (original system, for backward compatibility)
  static_params <- switch(format,
    "t20" = list(
      k_run = K_RUN_T20,
      k_wicket = K_WICKET_T20,
      k_wicket_survival = K_WICKET_SURVIVAL_T20
    ),
    "odi" = list(
      k_run = K_RUN_ODI,
      k_wicket = K_WICKET_ODI,
      k_wicket_survival = K_WICKET_SURVIVAL_ODI
    ),
    "test" = list(
      k_run = K_RUN_TEST,
      k_wicket = K_WICKET_TEST,
      k_wicket_survival = K_WICKET_SURVIVAL_TEST
    ),
    list(k_run = K_RUN_T20, k_wicket = K_WICKET_T20, k_wicket_survival = K_WICKET_SURVIVAL_T20)
  )

  # Dynamic K parameters
  dynamic_params <- if (use_dynamic_k) {
    switch(format,
      "t20" = list(
        k_run_max = PLAYER_K_RUN_MAX_T20,
        k_run_min = PLAYER_K_RUN_MIN_T20,
        k_run_halflife = PLAYER_K_RUN_HALFLIFE_T20,
        k_wicket_max = PLAYER_K_WICKET_MAX_T20,
        k_wicket_min = PLAYER_K_WICKET_MIN_T20,
        k_wicket_halflife = PLAYER_K_WICKET_HALFLIFE_T20,
        opp_k_boost = PLAYER_OPP_K_BOOST,
        opp_k_reduce = PLAYER_OPP_K_REDUCE
      ),
      "odi" = list(
        k_run_max = PLAYER_K_RUN_MAX_ODI,
        k_run_min = PLAYER_K_RUN_MIN_ODI,
        k_run_halflife = PLAYER_K_RUN_HALFLIFE_ODI,
        k_wicket_max = PLAYER_K_WICKET_MAX_ODI,
        k_wicket_min = PLAYER_K_WICKET_MIN_ODI,
        k_wicket_halflife = PLAYER_K_WICKET_HALFLIFE_ODI,
        opp_k_boost = PLAYER_OPP_K_BOOST,
        opp_k_reduce = PLAYER_OPP_K_REDUCE
      ),
      "test" = list(
        k_run_max = PLAYER_K_RUN_MAX_TEST,
        k_run_min = PLAYER_K_RUN_MIN_TEST,
        k_run_halflife = PLAYER_K_RUN_HALFLIFE_TEST,
        k_wicket_max = PLAYER_K_WICKET_MAX_TEST,
        k_wicket_min = PLAYER_K_WICKET_MIN_TEST,
        k_wicket_halflife = PLAYER_K_WICKET_HALFLIFE_TEST,
        opp_k_boost = PLAYER_OPP_K_BOOST,
        opp_k_reduce = PLAYER_OPP_K_REDUCE
      ),
      list(
        k_run_max = PLAYER_K_RUN_MAX_T20,
        k_run_min = PLAYER_K_RUN_MIN_T20,
        k_run_halflife = PLAYER_K_RUN_HALFLIFE_T20,
        k_wicket_max = PLAYER_K_WICKET_MAX_T20,
        k_wicket_min = PLAYER_K_WICKET_MIN_T20,
        k_wicket_halflife = PLAYER_K_WICKET_HALFLIFE_T20,
        opp_k_boost = PLAYER_OPP_K_BOOST,
        opp_k_reduce = PLAYER_OPP_K_REDUCE
      )
    )
  } else {
    list(
      k_run_max = NULL,
      k_run_min = NULL,
      k_run_halflife = NULL,
      k_wicket_max = NULL,
      k_wicket_min = NULL,
      k_wicket_halflife = NULL,
      opp_k_boost = 0,
      opp_k_reduce = 0
    )
  }

  # Common parameters
  common_params <- list(
    format = format,
    use_dynamic_k = use_dynamic_k,
    elo_start = DUAL_ELO_START,
    elo_divisor = DUAL_ELO_DIVISOR,
    run_score_wicket = RUN_SCORE_WICKET,
    run_score_dot = RUN_SCORE_DOT,
    run_score_single = RUN_SCORE_SINGLE,
    run_score_two = RUN_SCORE_DOUBLE,
    run_score_three = RUN_SCORE_THREE,
    run_score_four = RUN_SCORE_FOUR,
    run_score_six = RUN_SCORE_SIX
  )

  c(static_params, dynamic_params, common_params)
}


NULL
