# ELO Utility Functions
#
# Core ELO primitives: expected outcome, K-factor, update formula,
# dynamic K-factor decay, delivery outcome scoring, and format helpers.

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
#' in team_elo_optimization.R and three_way_elo.R.
#'
#' @section The `halflife` argument is not a half-life:
#' It is the exponential decay constant (tau) in `exp(-experience / halflife)`.
#' At `experience == halflife`, K has fallen to `exp(-1)` = **36.8%** of the
#' `k_max - k_min` range, not to the halfway point. The true half-life is
#' `halflife * log(2)`, about 69% of it.
#'
#' The name and the documented examples both previously claimed "halfway",
#' which is wrong by a factor of `log(2)` and matters to anyone tuning it. The
#' argument keeps its name because callers pass it positionally and every
#' fitted rating in the package was produced with this formula; only the
#' description is corrected. All example values below are computed, not
#' estimated.
#'
#' @param experience Numeric vector. Player/entity experience count
#'   (deliveries, matches, etc.). NA and negative values are treated as 0.
#' @param k_max Numeric. Maximum K-factor for new players.
#' @param k_min Numeric. Minimum K-factor for experienced players.
#' @param halflife Numeric. Exponential decay constant -- see the section
#'   above. Non-finite or non-positive values yield `k_min`.
#'
#' @return Numeric vector, the same length as the recycled inputs.
#'
#' @examples
#' \dontrun{
#' # Player K-factors (using T20 defaults)
#' calculate_dynamic_k(0, k_max = 200, k_min = 15, halflife = 500)    # 200.00 (new)
#' calculate_dynamic_k(500, k_max = 200, k_min = 15, halflife = 500)  # 83.06 (exp(-1))
#' calculate_dynamic_k(5000, k_max = 200, k_min = 15, halflife = 500) # 15.01 (experienced)
#'
#' # Team K-factors
#' calculate_dynamic_k(0, k_max = 60, k_min = 20, halflife = 50)      # 60.00 (new team)
#' calculate_dynamic_k(100, k_max = 60, k_min = 20, halflife = 50)    # 25.41
#'
#' # Vectorised over experience
#' calculate_dynamic_k(c(0, 500, 5000), k_max = 200, k_min = 15, halflife = 500)
#' }
#'
#' @keywords internal
calculate_dynamic_k <- function(experience, k_max, k_min, halflife) {
  if (is.null(experience)) experience <- 0

  # Vectorised throughout: `max(0, x)` silently collapsed a vector to one
  # number and `if (is.na(x))` errors on one under R >= 4.2. Callers pass
  # scalars today, but they are column extractions one refactor away from
  # being vectors, and both failure modes are quiet or late.
  experience[is.na(experience)] <- 0
  experience <- pmax(0, experience)

  # Guard against division by zero in exponential decay.
  #
  # NOT ifelse(halflife <= 0, ...): ifelse() takes its result length from the
  # TEST, so a scalar halflife would truncate a vector of experiences back to
  # one element. Compute over the full length, then mask.
  out <- k_min + (k_max - k_min) * exp(-experience / halflife)
  degenerate <- rep_len(!is.finite(halflife) | halflife <= 0, length(out))
  out[degenerate] <- rep_len(k_min, length(out))[degenerate]
  out
}


#' Calculate Dynamic K-Factor from Parameters List
#'
#' Convenience wrapper that extracts k_max, k_min, and halflife from a
#' named list (as used in three_way_elo.R).
#'
#' @param experience Numeric. Experience count.
#' @param params Named list. Must contain: k_max, k_min, halflife
#'
#' @return Numeric. The calculated K-factor.
#'
#' @examples
#' \dontrun{
#' params <- list(k_max = 200, k_min = 15, halflife = 500)
#' calculate_dynamic_k_from_params(100, params)  # 166.48
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
# CORE ELO FUNCTIONS
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
  # Vectorised: the scalar `if (is.na(...) || ...)` chain read only the first
  # element of a vector argument, and team_predictions.R:175 passes a column
  # extraction that is one upstream change away from being length > 1.
  bad <- !is.finite(player_elo) | !is.finite(opponent_elo) |
    !is.finite(divisor) | divisor == 0

  # Returning 0.5 for unusable input means a broken ratings table looks
  # exactly like a perfectly balanced one. Say when it happens.
  if (any(bad)) {
    cli::cli_warn(paste(
      "{sum(bad)} of {length(bad)} expected-outcome input{?s} were NA, NaN or",
      "infinite; returning a neutral 0.5 for {?it/them}."
    ))
  }

  out <- 1 / (1 + 10^((opponent_elo - player_elo) / divisor))
  out[bad] <- 0.5
  out
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
  # Vectorised over deliveries: the scalar `if (is_wicket)` / `if (runs_batter
  # == 0)` form read only the first element, so calling this on a column
  # scored the whole vector as if every ball matched ball one.
  n <- max(length(runs_batter), length(is_wicket), length(is_boundary))
  runs_batter <- rep_len(runs_batter, n)
  is_wicket <- rep_len(is_wicket, n)
  is_boundary <- rep_len(is_boundary, n)

  # Base score from runs (normalized to 0-6 range)
  # 0 runs = 0.2, 1 run = 0.33, 2 = 0.47, 3 = 0.6, 4 = 0.73, 6 = 1.0
  # A dot ball still scores 0.2 for surviving the delivery.
  score <- ifelse(runs_batter == 0, 0.2, 0.2 + (runs_batter / 6) * 0.8)

  # Boundary bonus. Note this is asymmetric by construction: a four goes
  # 0.733 -> 0.783, but a six is already at the 1.0 cap and gains nothing,
  # so the bonus rewards fours only. Intentional or not, it is what the
  # ratings have always been fitted on -- changing it moves every skill index.
  score <- score + ifelse(is_boundary, 0.05, 0)

  # Wicket is complete failure for the batter, overriding everything above.
  score <- ifelse(is_wicket, 0, score)

  pmax(0, pmin(1, score))
}










