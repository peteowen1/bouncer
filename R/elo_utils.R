# ELO Utility Functions
#
# Common ELO calculation utilities used across rating systems.
# Consolidates the dynamic K-factor decay pattern that appeared in 5+ files.

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
#' # Player K-factors (using T20 defaults)
#' calculate_dynamic_k(0, k_max = 200, k_min = 15, halflife = 500)    # 200 (new)
#' calculate_dynamic_k(500, k_max = 200, k_min = 15, halflife = 500)  # ~107.5 (half)
#' calculate_dynamic_k(5000, k_max = 200, k_min = 15, halflife = 500) # ~15 (experienced)
#'
#' # Team K-factors
#' calculate_dynamic_k(0, k_max = 60, k_min = 20, halflife = 50)      # 60 (new team)
#' calculate_dynamic_k(100, k_max = 60, k_min = 20, halflife = 50)    # ~26.5
#'
#' @keywords internal
calculate_dynamic_k <- function(experience, k_max, k_min, halflife) {
  if (is.null(experience) || is.na(experience)) {
    experience <- 0
  }

  # Ensure experience is non-negative
  experience <- max(0, experience)

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
#' params <- list(k_max = 200, k_min = 15, halflife = 500)
#' calculate_dynamic_k_from_params(100, params)  # ~169
#'
#' @keywords internal
calculate_dynamic_k_from_params <- function(experience, params) {
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

#' Calculate Expected ELO Score
#'
#' Computes the expected score (probability of winning) given two ELO ratings.
#' Uses the standard ELO formula: E = 1 / (1 + 10^((opponent_elo - player_elo) / 400))
#'
#' @param player_elo Numeric. Player's current ELO rating.
#' @param opponent_elo Numeric. Opponent's current ELO rating.
#' @param scale Numeric. ELO scale factor. Default 400 (standard).
#'
#' @return Numeric. Expected score (0 to 1).
#'
#' @examples
#' calculate_elo_expected(1500, 1500)  # 0.5 (equal ratings)
#' calculate_elo_expected(1600, 1400)  # 0.76 (100 points advantage)
#' calculate_elo_expected(1400, 1600)  # 0.24 (100 points disadvantage)
#'
#' @keywords internal
calculate_elo_expected <- function(player_elo, opponent_elo, scale = 400) {
  1 / (1 + 10^((opponent_elo - player_elo) / scale))
}


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
#' # Player wins against equal opponent
#' calculate_elo_update(1500, expected = 0.5, actual = 1, k = 32)  # 1516
#'
#' # Player loses against equal opponent
#' calculate_elo_update(1500, expected = 0.5, actual = 0, k = 32)  # 1484
#'
#' @keywords internal
calculate_elo_update <- function(current_elo, expected, actual, k) {
  current_elo + k * (actual - expected)
}

