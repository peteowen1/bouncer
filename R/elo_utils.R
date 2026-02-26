# ELO Utility Functions
#
# Core ELO primitives: expected outcome, K-factor, update formula,
# dynamic K-factor decay, delivery outcome scoring, and format helpers.
# Consolidates functions from the former player_elo_core.R and elo_utils.R.

# ============================================================================
# DYNAMIC K-FACTOR CALCULATION
# ============================================================================

#' Calculate Dynamic K-Factor
#'
#' Computes a K-factor that decays exponentially with experience.
#' New players start with higher K (learn quickly), experienced players
#' have lower K (more stable ratings).
#'
#' Formula: K = k_min + (k_max - k_min) * exp(-experience / halflife)
#'
#' This function consolidates the K-factor decay pattern that was repeated
#' in player_elo_dynamic.R, team_elo_optimization.R, and three_way_elo.R.
#'
#' @param experience Numeric. Player/entity experience count (deliveries, matches, etc.)
#' @param k_max Numeric. Maximum K-factor for new players.
#' @param k_min Numeric. Minimum K-factor for experienced players.
#' @param halflife Numeric. Experience value at which K is halfway between max and min.
#'
#' @return Numeric. The calculated K-factor.
#'
#' @examples
#' \dontrun{
#' # Player K-factors (using T20 defaults)
#' calculate_dynamic_k(0, k_max = 200, k_min = 15, halflife = 500)    # 200 (new)
#' calculate_dynamic_k(500, k_max = 200, k_min = 15, halflife = 500)  # ~107.5 (half)
#' calculate_dynamic_k(5000, k_max = 200, k_min = 15, halflife = 500) # ~15 (experienced)
#'
#' # Team K-factors
#' calculate_dynamic_k(0, k_max = 60, k_min = 20, halflife = 50)      # 60 (new team)
#' calculate_dynamic_k(100, k_max = 60, k_min = 20, halflife = 50)    # ~26.5
#' }
#'
#' @keywords internal
calculate_dynamic_k <- function(experience, k_max, k_min, halflife) {
  if (is.null(experience) || is.na(experience)) {
    experience <- 0
  }

  # Ensure experience is non-negative
  experience <- max(0, experience)

  # Guard against division by zero in exponential decay
  if (halflife <= 0) return(k_min)

  # Exponential decay formula
  k_min + (k_max - k_min) * exp(-experience / halflife)
}


#' Calculate Dynamic K-Factor from Parameters List
#'
#' Convenience wrapper that extracts k_max, k_min, and halflife from a
#' named list (as used in three_way_elo.R and player_elo_dynamic.R).
#'
#' @param experience Numeric. Experience count.
#' @param params Named list. Must contain: k_max, k_min, halflife
#'
#' @return Numeric. The calculated K-factor.
#'
#' @examples
#' \dontrun{
#' params <- list(k_max = 200, k_min = 15, halflife = 500)
#' calculate_dynamic_k_from_params(100, params)  # ~169
#' }
#'
#' @keywords internal
calculate_dynamic_k_from_params <- function(experience, params) {
  required <- c("k_max", "k_min", "halflife")
  missing <- setdiff(required, names(params))
  if (length(missing) > 0) {
    cli::cli_abort("params missing required keys: {.field {missing}}")
  }
  calculate_dynamic_k(
    experience = experience,
    k_max = params$k_max,
    k_min = params$k_min,
    halflife = params$halflife
  )
}


# ============================================================================
# ELO EXPECTED SCORE
# ============================================================================

#' Calculate ELO Update
#'
#' Computes the new ELO rating after a match result.
#'
#' @param current_elo Numeric. Current ELO rating.
#' @param expected Numeric. Expected score (0 to 1).
#' @param actual Numeric. Actual score (0 to 1, or 0/0.5/1 for loss/draw/win).
#' @param k Numeric. K-factor for this update.
#'
#' @return Numeric. The new ELO rating.
#'
#' @examples
#' \dontrun{
#' # Player wins against equal opponent
#' calculate_elo_update(1500, expected = 0.5, actual = 1, k = 32)  # 1516
#'
#' # Player loses against equal opponent
#' calculate_elo_update(1500, expected = 0.5, actual = 0, k = 32)  # 1484
#' }
#'
#' @keywords internal
calculate_elo_update <- function(current_elo, expected, actual, k) {
  current_elo + k * (actual - expected)
}


# ============================================================================
# CORE ELO FUNCTIONS (from player_elo_core.R)
# ============================================================================

#' Calculate Expected Outcome
#'
#' Calculates the expected outcome for a player matchup using ELO ratings.
#' This is the standard ELO formula: E = 1 / (1 + 10^((R_opponent - R_player) / 400))
#'
#' @param player_elo Numeric. Player's current ELO rating
#' @param opponent_elo Numeric. Opponent's current ELO rating
#' @param divisor Numeric. ELO divisor constant (default 400)
#'
#' @return Numeric value between 0 and 1 representing expected outcome
#' @keywords internal
calculate_expected_outcome <- function(player_elo, opponent_elo, divisor = ELO_DIVISOR) {
  # Input validation - return 0.5 (neutral) for invalid inputs
  if (is.na(player_elo) || is.na(opponent_elo) || is.na(divisor) ||
      is.nan(player_elo) || is.nan(opponent_elo) || is.nan(divisor) ||
      is.infinite(player_elo) || is.infinite(opponent_elo) || divisor == 0) {
    return(0.5)
  }
  1 / (1 + 10^((opponent_elo - player_elo) / divisor))
}








#' Calculate Actual Outcome Score from Delivery
#'
#' Converts a delivery outcome (runs, wicket) into a score between 0 and 1
#' for ELO calculation.
#'
#' @param runs_batter Integer. Runs scored by batter (not including extras)
#' @param is_wicket Logical. Whether batter was dismissed
#' @param is_boundary Logical. Whether it was a boundary
#'
#' @return Numeric value between 0 and 1
#' @keywords internal
calculate_delivery_outcome_score <- function(runs_batter, is_wicket, is_boundary = FALSE) {
  # Wicket is complete failure for batter (0.0)
  if (is_wicket) {
    return(0.0)
  }

  # Base score from runs (normalized to 0-6 range)
  # 0 runs = 0.2, 1 run = 0.33, 2 = 0.47, 3 = 0.6, 4 = 0.73, 6 = 1.0
  base_score <- (runs_batter / 6)

  # Bonus for boundaries (dot balls get small credit)
  if (runs_batter == 0) {
    score <- 0.2  # Survived the ball
  } else {
    score <- 0.2 + (base_score * 0.8)
  }

  # Boundary bonus (4s and 6s are premium)
  if (is_boundary) {
    score <- min(score * 1.1, 1.0)
  }

  # Ensure in [0, 1] range
  score <- max(0, min(1, score))

  return(score)
}










