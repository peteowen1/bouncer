# Player Career Ratings (EPR)
# ============================
# Rolling Bayesian-shrunk career value ratings from per-match
# WPA and ERA values. Cricket equivalent of torp's EPR.
#
# Two components:
#   - batting_epr: career batting value (WPA + ERA when batting)
#   - bowling_epr: career bowling value (WPA + ERA when bowling)
#   - total_epr: batting_epr + bowling_epr
#
# Each uses exponential time decay and Bayesian shrinkage
# toward a role-specific prior.


# ============================================================================
# EPR Constants (reasonable defaults, tunable per format)
# ============================================================================

# Decay in days (half-life ≈ 0.693 / lambda)
EPR_DECAY_BATTING_T20  <- 365
EPR_DECAY_BOWLING_T20  <- 365
EPR_DECAY_BATTING_ODI  <- 500
EPR_DECAY_BOWLING_ODI  <- 500
EPR_DECAY_BATTING_TEST <- 730
EPR_DECAY_BOWLING_TEST <- 730

# Prior pseudo-matches and prior rates
EPR_PRIOR_MATCHES   <- 10
EPR_PRIOR_RATE      <- 0    # Shrink toward zero (replacement level)


#' Calculate Expected Performance Rating (EPR)
#'
#' Computes rolling career value ratings for each player from per-match
#' WPA and ERA values. Uses exponential time decay and Bayesian shrinkage
#' toward a role-specific replacement-level prior.
#'
#' @param format Character. "t20", "odi", or "test".
#' @param player_game_data data.table from \code{\link{load_player_game_data}}.
#'   If NULL, loads automatically.
#' @param ref_date Date. Compute EPR as of this date (NULL = latest + 1 day).
#' @param decay_batting Numeric. Decay constant in days for batting component.
#' @param decay_bowling Numeric. Decay constant in days for bowling component.
#' @param prior_matches Numeric. Prior pseudo-matches for shrinkage.
#' @param prior_rate Numeric. Prior rate (typically 0 = replacement level).
#'
#' @return data.table with one row per player:
#'   \describe{
#'     \item{player_id}{Player identifier}
#'     \item{role_group}{BATTER, BOWLER, ALL_ROUNDER}
#'     \item{batting_epr}{Career batting value rating}
#'     \item{bowling_epr}{Career bowling value rating}
#'     \item{total_epr}{batting_epr + bowling_epr}
#'     \item{n_matches, wt_matches}{Match count and weighted count}
#'   }
#'
#' @export
calculate_epr <- function(format = c("t20", "odi", "test"),
                           player_game_data = NULL,
                           ref_date = NULL,
                           decay_batting = NULL,
                           decay_bowling = NULL,
                           prior_matches = EPR_PRIOR_MATCHES,
                           prior_rate = EPR_PRIOR_RATE) {

  format <- match.arg(format)

  # Format-specific decay defaults
  if (is.null(decay_batting)) {
    decay_batting <- switch(format,
      t20 = EPR_DECAY_BATTING_T20,
      odi = EPR_DECAY_BATTING_ODI,
      test = EPR_DECAY_BATTING_TEST
    )
  }
  if (is.null(decay_bowling)) {
    decay_bowling <- switch(format,
      t20 = EPR_DECAY_BOWLING_T20,
      odi = EPR_DECAY_BOWLING_ODI,
      test = EPR_DECAY_BOWLING_TEST
    )
  }

  # Load data if not provided
  if (is.null(player_game_data)) {
    player_game_data <- load_player_game_data(format)
  }

  dt <- data.table::as.data.table(player_game_data)

  if (!inherits(dt$match_date, "Date")) {
    dt[, match_date := as.Date(match_date)]
  }

  if (is.null(ref_date)) {
    ref_date <- max(dt$match_date, na.rm = TRUE) + 1L
  }
  ref_date <- as.Date(ref_date)

  # Filter to matches before ref_date
  dt <- dt[match_date < ref_date]
  if (nrow(dt) == 0) {
    cli::cli_warn("No matches before ref_date for EPR calculation")
    return(data.table::data.table())
  }

  dt[, days_diff := as.numeric(ref_date - match_date)]

  # Decay weights per component
  dt[, wt_bat := exp(-days_diff / decay_batting)]
  dt[, wt_bowl := exp(-days_diff / decay_bowling)]

  # Combined batting value = WPA + ERA (per match)
  dt[, bat_value := batting_wpa + batting_era]
  dt[, bowl_value := bowling_wpa + bowling_era]

  # Exposure weighting: scale by balls to give high-exposure games more weight
  # Normalize to a "full match" equivalent (e.g., 120 balls faced in T20)
  full_match_balls <- switch(format,
    t20 = 60,    # ~60 balls faced per batter in a full T20 innings
    odi = 100,   # ~100 balls in a full ODI innings
    test = 150   # ~150 balls in a full Test innings
  )

  dt[, bat_exposure := pmin(batting_balls_faced / full_match_balls, 1)]
  dt[, bowl_exposure := pmin(bowling_balls_bowled / (full_match_balls * 0.4), 1)]

  # Aggregate per player with Bayesian shrinkage
  result <- dt[, {
    # Batting EPR
    bat_sum <- sum(bat_value * wt_bat * bat_exposure, na.rm = TRUE)
    bat_denom <- sum(wt_bat * bat_exposure, na.rm = TRUE) + prior_matches
    batting_epr_val <- (bat_sum + prior_matches * prior_rate) / bat_denom

    # Bowling EPR
    bowl_sum <- sum(bowl_value * wt_bowl * bowl_exposure, na.rm = TRUE)
    bowl_denom <- sum(wt_bowl * bowl_exposure, na.rm = TRUE) + prior_matches
    bowling_epr_val <- (bowl_sum + prior_matches * prior_rate) / bowl_denom

    # Match counts
    n_m <- data.table::uniqueN(match_id)
    wt_m <- sum(wt_bat[!duplicated(match_id)], na.rm = TRUE)

    .(batting_epr = batting_epr_val,
      bowling_epr = bowling_epr_val,
      total_epr = batting_epr_val + bowling_epr_val,
      n_matches = n_m,
      wt_matches = round(wt_m, 2))
  }, by = player_id]

  # Assign role from most recent data
  roles <- dt[, .(role_group = data.table::last(role)), by = player_id]
  roles[role_group == "batter", role_group := "BATTER"]
  roles[role_group == "bowler", role_group := "BOWLER"]
  roles[role_group == "all_rounder", role_group := "ALL_ROUNDER"]
  result[roles, role_group := i.role_group, on = "player_id"]

  result[, ref_date := ref_date]

  data.table::setorder(result, -total_epr)

  cli::cli_alert_success("Calculated EPR for {nrow(result)} players (format={toupper(format)})")
  result
}
