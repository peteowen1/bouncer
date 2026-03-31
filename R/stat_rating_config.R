# Player Stat Rating Estimation Configuration
# =============================================
# Stat definitions, role mapping, and default hyperparameters
# for Bayesian stat rating estimation pipeline.
#
# Adapted from torpverse/torp pattern for cricket:
# - Rate stats use Gamma-Poisson (per ball faced/bowled)
# - Efficiency stats use Beta-Binomial
# - Role groups instead of position groups (BATTER/BOWLER/ALL_ROUNDER/WICKETKEEPER)
# - Exposure is balls faced (batting) or balls bowled (bowling), not TOG


#' Stat definitions for cricket player stat rating estimation
#'
#' Returns a data.frame describing every stat to estimate. Each row specifies
#' how to extract the raw value from player game data and whether it's a rate stat
#' (Gamma-Poisson, scaled by balls) or an efficiency stat (Beta-Binomial).
#'
#' @return A data.frame with columns:
#'   \describe{
#'     \item{stat_name}{Short name used in output columns (e.g., "batting_runs")}
#'     \item{type}{"rate" or "efficiency"}
#'     \item{source_col}{Column name in player_game_data for the raw count}
#'     \item{exposure_col}{Column for the denominator (balls faced/bowled)}
#'     \item{category}{"batting", "bowling", "hawkeye_batting", "hawkeye_bowling", or "value"}
#'     \item{higher_is_better}{Logical. TRUE if higher values = better performance.}
#'     \item{success_col}{For efficiency stats: column for successes.}
#'     \item{attempts_col}{For efficiency stats: column for attempts.}
#'   }
#' @export
stat_rating_definitions <- function() {

  # --- Rate stats (Gamma-Poisson, per ball) ---
  rate_stats <- data.frame(
    stat_name = c(
      # Batting rate stats (per ball faced)
      "batting_runs",
      "batting_fours",
      "batting_sixes",
      "batting_boundaries",
      "batting_dot_balls",

      # Bowling rate stats (per ball bowled)
      "bowling_runs_conceded",
      "bowling_wickets",
      "bowling_dot_balls",
      "bowling_boundaries_conceded",
      "bowling_wides",

      # Value rate stats (per ball)
      "batting_wpa",
      "bowling_wpa",
      "batting_era",
      "bowling_era"
    ),
    source_col = c(
      # Batting
      "batting_runs",
      "batting_fours",
      "batting_sixes",
      "batting_boundaries",
      "batting_dot_balls",

      # Bowling
      "bowling_runs_conceded",
      "bowling_wickets",
      "bowling_dot_balls",
      "bowling_boundaries_conceded",
      "bowling_wides",

      # Value
      "batting_wpa",
      "bowling_wpa",
      "batting_era",
      "bowling_era"
    ),
    exposure_col = c(
      # Batting: per ball faced
      rep("batting_balls_faced", 5),
      # Bowling: per ball bowled
      rep("bowling_balls_bowled", 5),
      # Value: per ball in role
      "batting_balls_faced",
      "bowling_balls_bowled",
      "batting_balls_faced",
      "bowling_balls_bowled"
    ),
    category = c(
      rep("batting", 5),
      rep("bowling", 5),
      rep("value", 4)
    ),
    type = "rate",
    higher_is_better = c(
      # Batting: runs/4s/6s/boundaries good, dots bad
      TRUE, TRUE, TRUE, TRUE, FALSE,
      # Bowling: conceding runs/boundaries bad, wickets/dots good, wides bad
      FALSE, TRUE, TRUE, FALSE, FALSE,
      # Value: WPA/ERA always positive = good
      TRUE, TRUE, TRUE, TRUE
    ),
    success_col = NA_character_,
    attempts_col = NA_character_,
    stringsAsFactors = FALSE
  )

  # --- Efficiency stats (Beta-Binomial) ---
  efficiency_stats <- data.frame(
    stat_name = c(
      # Batting efficiency
      "batting_boundary_rate",
      "batting_dot_rate",
      "batting_survival_rate",

      # Bowling efficiency
      "bowling_dot_rate",
      "bowling_boundary_rate",

      # Hawkeye batting (proportion stats)
      "batting_control_rate",
      "batting_attack_rate",

      # Hawkeye bowling (proportion stats)
      "bowling_good_length_rate",
      "bowling_on_stump_rate",
      "bowling_beat_bat_rate"
    ),
    source_col = NA_character_,
    exposure_col = NA_character_,
    category = c(
      rep("batting", 3),
      rep("bowling", 2),
      rep("hawkeye_batting", 2),
      rep("hawkeye_bowling", 3)
    ),
    type = "efficiency",
    higher_is_better = c(
      # Batting: boundary good, dots bad, survival good
      TRUE, FALSE, TRUE,
      # Bowling: dots good, boundaries bad
      TRUE, FALSE,
      # Hawkeye batting: control good, attacking good
      TRUE, TRUE,
      # Hawkeye bowling: good length good, on stump good, beat bat good
      TRUE, TRUE, TRUE
    ),
    success_col = c(
      # Batting
      "batting_boundaries",
      "batting_dot_balls",
      "batting_balls_survived",  # computed: balls_faced - dismissed

      # Bowling
      "bowling_dot_balls",
      "bowling_boundaries_conceded",

      # Hawkeye batting (use hawkeye_balls * pct as successes)
      "batting_controlled_balls",
      "batting_attacking_balls",

      # Hawkeye bowling
      "bowling_good_length_balls",
      "bowling_on_stump_balls",
      "bowling_beat_bat_balls"
    ),
    attempts_col = c(
      # Batting
      "batting_balls_faced",
      "batting_balls_faced",
      "batting_balls_faced",

      # Bowling
      "bowling_balls_bowled",
      "bowling_balls_bowled",

      # Hawkeye batting
      "batting_hawkeye_balls",
      "batting_hawkeye_balls",

      # Hawkeye bowling
      "bowling_hawkeye_balls",
      "bowling_hawkeye_balls",
      "bowling_hawkeye_balls"
    ),
    stringsAsFactors = FALSE
  )

  rbind(rate_stats, efficiency_stats)
}


#' Role group mapping for cricket stat rating estimation
#'
#' Maps player roles to groups for computing role-specific priors.
#' Role detection uses career batting/bowling balance.
#'
#' @return A named list mapping role group names to descriptions.
#' @export
stat_rating_role_map <- function() {
  list(
    BATTER       = "Primary batter (bowls < 10% of team overs)",
    BOWLER       = "Primary bowler (bats below position 8 on average)",
    ALL_ROUNDER  = "Significant contributions in both batting and bowling",
    WICKETKEEPER = "Keeper-batter (detected from metadata)"
  )
}


#' Default hyperparameters for stat rating estimation
#'
#' Returns initial defaults for the Bayesian stat rating pipeline.
#' Per-stat optimized values are baked into \code{.stat_rating_params()}
#' after running the optimization script.
#'
#' @return A named list with elements:
#'   \describe{
#'     \item{lambda_rate}{Fallback decay rate for rate stats (per day).}
#'     \item{lambda_efficiency}{Fallback decay rate for efficiency stats (per day).}
#'     \item{prior_games_rate}{Fallback prior pseudo-games for Gamma-Poisson.}
#'     \item{prior_attempts_efficiency}{Fallback prior pseudo-attempts for Beta-Binomial.}
#'     \item{min_wt_matches}{Minimum weighted matches to appear in output.}
#'     \item{credible_level}{Width of credible interval (e.g., 0.80 for 80\%).}
#'     \item{stat_params}{Per-stat optimized lambda and prior_strength.}
#'   }
#' @export
default_stat_rating_params <- function() {
  list(
    lambda_rate             = 0.003,
    lambda_efficiency       = 0.002,
    prior_games_rate        = 5,
    prior_attempts_efficiency = 60,
    min_wt_matches          = 3,
    credible_level          = 0.80,
    stat_params             = .stat_rating_params()
  )
}


#' Optimized per-stat hyperparameters
#'
#' Baked-in results from optimization. Rate stats optimized via
#' exposure-weighted MSE, efficiency stats via attempt-weighted log-loss.
#' Each entry has \code{lambda} (decay per day) and \code{prior_strength}.
#'
#' Initially populated with reasonable defaults. After running
#' \code{data-raw/ratings/player/stat-ratings/02_optimize_stat_rating_params.R},
#' these values will be overwritten with optimized values.
#'
#' @keywords internal
.stat_rating_params <- function() {
  list(
    # Rate stats (Gamma-Poisson, optimized via multi-start MSE)
    batting_runs                = list(lambda = 0.00022, prior_strength = 100.00),
    batting_fours               = list(lambda = 0.00028, prior_strength = 99.16),
    batting_sixes               = list(lambda = 0.00062, prior_strength = 96.86),
    batting_boundaries          = list(lambda = 0.02596, prior_strength = 100.00),
    batting_dot_balls           = list(lambda = 0.00010, prior_strength = 100.00),
    bowling_runs_conceded       = list(lambda = 0.00080, prior_strength = 100.00),
    bowling_wickets             = list(lambda = 0.00773, prior_strength = 56.25),
    bowling_dot_balls           = list(lambda = 0.03068, prior_strength = 22.80),
    bowling_boundaries_conceded = list(lambda = 0.00319, prior_strength = 3.01),
    bowling_wides               = list(lambda = 0.00098, prior_strength = 100.00),
    batting_wpa                 = list(lambda = 0.00100, prior_strength = 0.50),
    bowling_wpa                 = list(lambda = 0.00100, prior_strength = 0.50),
    batting_era                 = list(lambda = 0.00112, prior_strength = 22.17),
    bowling_era                 = list(lambda = 0.00805, prior_strength = 99.99),
    # Efficiency stats (Beta-Binomial, optimized via multi-start log-loss)
    batting_boundary_rate       = list(lambda = 7e-05, prior_strength = 23.64),
    batting_dot_rate            = list(lambda = 1e-05, prior_strength = 121.78),
    batting_survival_rate       = list(lambda = 0.00305, prior_strength = 4.24),
    bowling_dot_rate            = list(lambda = 0.00246, prior_strength = 11.60),
    bowling_boundary_rate       = list(lambda = 0.00394, prior_strength = 4.12),
    batting_control_rate        = list(lambda = 0.01442, prior_strength = 0.10),
    batting_attack_rate         = list(lambda = 0.00319, prior_strength = 0.29),
    bowling_good_length_rate    = list(lambda = 0.01669, prior_strength = 0.10),
    bowling_on_stump_rate       = list(lambda = 9e-05, prior_strength = 0.44),
    bowling_beat_bat_rate       = list(lambda = 0.03640, prior_strength = 18.38)
  )
}
