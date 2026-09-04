# Player Career Ratings (Impact)
# ==============================
# Rolling Bayesian-shrunk career value ratings from per-match RAA and WPA:
#
#   per-match value = raa + kappa * wpa        (D-P11, bouncerverse#18)
#
# RAA is runs above the state-only (agnostic) expectation; WPA is win
# probability added to the player's own team, converted to run units by the
# fitted exchange rate kappa, so leverage counts at an honest scale instead of
# the raw-probability scale that made it 0.009% of the old EPR. Aggregation is
# exponential time decay + Bayesian shrinkage toward replacement level +
# exposure weighting, applied per component.
#
# The previous engine (`wpa + era`) is retired: ERA had three structural
# defects (D-P8) and the raw-scale sum was ERA in a costume (D-P7). The
# calculate_epr() name survives only as a deprecated alias.


# ============================================================================
# Impact Constants (reasonable defaults, tunable per format)
# ============================================================================

# Decay in days (half-life ~ 0.693 * decay)
IMPACT_DECAY_BATTING_T20  <- 365
IMPACT_DECAY_BOWLING_T20  <- 365
IMPACT_DECAY_BATTING_ODI  <- 500
IMPACT_DECAY_BOWLING_ODI  <- 500
IMPACT_DECAY_BATTING_TEST <- 730
IMPACT_DECAY_BOWLING_TEST <- 730

# Prior pseudo-matches and prior rates
IMPACT_PRIOR_MATCHES <- 10
IMPACT_PRIOR_RATE    <- 0    # Shrink toward zero (replacement level)

# Deprecated aliases -- the EPR names, kept so old callers and scripts do not
# break silently. New code uses the IMPACT_* names.
EPR_DECAY_BATTING_T20  <- IMPACT_DECAY_BATTING_T20
EPR_DECAY_BOWLING_T20  <- IMPACT_DECAY_BOWLING_T20
EPR_DECAY_BATTING_ODI  <- IMPACT_DECAY_BATTING_ODI
EPR_DECAY_BOWLING_ODI  <- IMPACT_DECAY_BOWLING_ODI
EPR_DECAY_BATTING_TEST <- IMPACT_DECAY_BATTING_TEST
EPR_DECAY_BOWLING_TEST <- IMPACT_DECAY_BOWLING_TEST
EPR_PRIOR_MATCHES      <- IMPACT_PRIOR_MATCHES
EPR_PRIOR_RATE         <- IMPACT_PRIOR_RATE


#' The Runs-per-Win-Probability Exchange Rate (kappa)
#'
#' Converts WPA into run units for the impact rating. Fitted from ACTUAL match
#' outcomes as 1 / (WP value of one run) with within-state controls (runs
#' needed / score, balls left, wickets in hand), 2026-08-14 (bouncerverse#18):
#' one unit of win probability is worth ~150 runs in T20 and ~272 in ODI.
#' Never refit from the WP model's own marginal effects -- the model is an
#' estimate of the thing, not the thing.
#'
#' @param format Character. "t20" and "odi" are fitted; "test" aborts until
#'   Test WP is trustworthy (bouncerverse#24) and a Test RAA lambda exists.
#'
#' @return Numeric scalar, runs per unit of win probability.
#'
#' @keywords internal
get_impact_kappa <- function(format = c("t20", "odi", "test")) {
  format <- match.arg(format)
  switch(format,
    t20 = 150,
    odi = 272,
    cli::cli_abort(c(
      "Impact kappa is not fitted for {.val {format}} yet.",
      "i" = "Blocked on trustworthy Test WP (bouncerverse#24) and a Test RAA lambda."
    ))
  )
}


#' Calculate Player Impact Ratings
#'
#' Computes rolling career value ratings for each player from per-match RAA
#' and WPA: `value = raa + kappa * wpa`, where kappa is the fitted
#' runs-per-win-probability exchange rate ([get_impact_kappa()]). Uses
#' exponential time decay, Bayesian shrinkage toward replacement level, and
#' exposure weighting -- per component, so batting and bowling decay
#' independently.
#'
#' Both inputs are bouncer's own: RAA from [build_cricinfo_raa()] (agnostic
#' baseline, fitted wicket value) and WPA from
#' [build_cricinfo_win_probability()] via `player_game_data.R`, own-team
#' signed (bouncerverse#25). A match with either component missing stays NA
#' and drops out of numerator AND denominator together -- the coverage warning
#' below is load-bearing, do not silence it.
#'
#' @param format Character. "t20", "odi", or "test". Test aborts until its
#'   inputs exist ([get_impact_kappa()]).
#' @param player_game_data data.table from \code{\link{load_player_game_data}}.
#'   If NULL, loads automatically.
#' @param ref_date Date. Compute the rating as of this date (NULL = latest + 1).
#' @param decay_batting Numeric. Decay constant in days for batting component.
#' @param decay_bowling Numeric. Decay constant in days for bowling component.
#' @param prior_matches Numeric. Prior pseudo-matches for shrinkage.
#' @param prior_rate Numeric. Prior rate (typically 0 = replacement level).
#'
#' @return data.table with one row per player:
#'   \describe{
#'     \item{player_id}{Player identifier}
#'     \item{role_group}{BATTER, BOWLER, ALL_ROUNDER}
#'     \item{batting_impact}{Career batting value rating (run units)}
#'     \item{bowling_impact}{Career bowling value rating (run units)}
#'     \item{total_impact}{batting_impact + bowling_impact}
#'     \item{n_matches, wt_matches}{Match count and weighted count}
#'   }
#'
#' @export
calculate_impact <- function(format = c("t20", "odi", "test"),
                             player_game_data = NULL,
                             ref_date = NULL,
                             decay_batting = NULL,
                             decay_bowling = NULL,
                             prior_matches = IMPACT_PRIOR_MATCHES,
                             prior_rate = IMPACT_PRIOR_RATE) {

  format <- match.arg(format)
  kappa <- get_impact_kappa(format)

  if (is.null(decay_batting)) {
    decay_batting <- switch(format,
      t20 = IMPACT_DECAY_BATTING_T20,
      odi = IMPACT_DECAY_BATTING_ODI,
      test = IMPACT_DECAY_BATTING_TEST
    )
  }
  if (is.null(decay_bowling)) {
    decay_bowling <- switch(format,
      t20 = IMPACT_DECAY_BOWLING_T20,
      odi = IMPACT_DECAY_BOWLING_ODI,
      test = IMPACT_DECAY_BOWLING_TEST
    )
  }

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

  dt <- dt[match_date < ref_date]
  if (nrow(dt) == 0) {
    cli::cli_warn("No matches before ref_date for impact calculation")
    return(data.table::data.table())
  }

  dt[, days_diff := as.numeric(ref_date - match_date)]
  dt[, wt_bat := exp(-days_diff / decay_batting)]
  dt[, wt_bowl := exp(-days_diff / decay_bowling)]

  # The rating's per-match value (D-P11)
  dt[, bat_value := batting_raa + kappa * batting_wpa]
  dt[, bowl_value := bowling_raa + kappa * bowling_wpa]

  # Coverage EVERY run. A missing component means the match contributes
  # nothing (correct), but a SHORTFALL means an upstream table is stale --
  # main.cricinfo_ball_raa or main.bouncer_wp_from_cricinfo needs
  # rebuilding -- and without this warning the rating still looks complete.
  ok_pct <- 100 * sum(!is.na(dt$bat_value) | !is.na(dt$bowl_value)) / max(1L, nrow(dt))
  if (ok_pct < 99) {
    lvl <- if (ok_pct < 50) cli::cli_warn else cli::cli_alert_info
    lvl(c(
      "Impact: a usable value exists for {round(ok_pct, 1)}% of {nrow(dt)} player-match rows ({toupper(format)}).",
      "!" = "The rest have RAA or WPA missing -- absent, not zero.",
      "i" = "Rebuild main.cricinfo_ball_raa / main.bouncer_wp_from_cricinfo if this is unexpected."
    ))
  }

  # Exposure weighting: scale by balls, normalised to one player's share of a
  # full innings (~60 balls for a T20 batter who bats deep, not the innings'
  # 120).
  full_match_balls <- switch(format,
    t20 = 60,
    odi = 100,
    test = 150
  )
  dt[, bat_exposure := pmin(batting_balls_faced / full_match_balls, 1)]
  dt[, bowl_exposure := pmin(bowling_balls_bowled / (full_match_balls * 0.4), 1)]

  # Aggregate per player with Bayesian shrinkage. A match whose value is NA
  # drops out of the NUMERATOR AND THE DENOMINATOR together -- summing the
  # numerator with na.rm while the denominator keeps the weight would shrink
  # the player toward the prior, making a data gap look like mediocrity.
  result <- dt[, {
    bat_ok <- !is.na(bat_value) & !is.na(wt_bat) & !is.na(bat_exposure)
    bat_sum <- sum((bat_value * wt_bat * bat_exposure)[bat_ok])
    bat_denom <- sum((wt_bat * bat_exposure)[bat_ok]) + prior_matches
    batting_val <- (bat_sum + prior_matches * prior_rate) / bat_denom

    bowl_ok <- !is.na(bowl_value) & !is.na(wt_bowl) & !is.na(bowl_exposure)
    bowl_sum <- sum((bowl_value * wt_bowl * bowl_exposure)[bowl_ok])
    bowl_denom <- sum((wt_bowl * bowl_exposure)[bowl_ok]) + prior_matches
    bowling_val <- (bowl_sum + prior_matches * prior_rate) / bowl_denom

    n_m <- data.table::uniqueN(match_id)
    wt_m <- sum(wt_bat[!duplicated(match_id)], na.rm = TRUE)

    .(batting_impact = batting_val,
      bowling_impact = bowling_val,
      total_impact = batting_val + bowling_val,
      n_matches = n_m,
      wt_matches = round(wt_m, 2))
  }, by = player_id]

  # Role from most recent data, ordered explicitly.
  roles <- dt[order(match_date), .(role_raw = data.table::last(role)),
              by = player_id]
  role_map <- c(batter = "BATTER", bowler = "BOWLER",
                all_rounder = "ALL_ROUNDER")
  roles[, role_key := tolower(trimws(as.character(role_raw)))]
  roles[, role_group := unname(role_map[role_key])]
  unmapped <- roles[is.na(role_group) & !is.na(role_key) & nzchar(role_key)]
  if (nrow(unmapped) > 0) {
    cli::cli_warn(c(
      "{nrow(unmapped)} player{?s} have a role outside {.val {names(role_map)}}.",
      "i" = "Unrecognised: {.val {sort(unique(unmapped$role_key))}}",
      "i" = "Recorded as {.val UNKNOWN} rather than passed through unmapped."
    ))
  }
  roles[is.na(role_group), role_group := "UNKNOWN"]
  result[roles, role_group := i.role_group, on = "player_id"]

  result[, ref_date := ref_date]
  data.table::setorder(result, -total_impact)

  cli::cli_alert_success("Calculated impact for {nrow(result)} players (format={toupper(format)})")
  result
}


#' Calculate Expected Performance Rating (EPR) -- Deprecated
#'
#' Deprecated alias for [calculate_impact()]. The `wpa + era` engine this name
#' referred to is retired (D-P8, D-P11): ERA left the rating and WPA enters at
#' the fitted run-unit exchange rate. This wrapper returns
#' [calculate_impact()]'s result with the columns renamed to the old
#' `batting_epr`/`bowling_epr`/`total_epr` names so existing callers keep
#' working while they migrate.
#'
#' @inheritParams calculate_impact
#' @return As [calculate_impact()], with the impact columns renamed to
#'   `batting_epr`, `bowling_epr`, `total_epr`.
#'
#' @export
calculate_epr <- function(format = c("t20", "odi", "test"),
                          player_game_data = NULL,
                          ref_date = NULL,
                          decay_batting = NULL,
                          decay_bowling = NULL,
                          prior_matches = IMPACT_PRIOR_MATCHES,
                          prior_rate = IMPACT_PRIOR_RATE) {
  cli::cli_warn(c(
    "{.fn calculate_epr} is deprecated; use {.fn calculate_impact}.",
    "i" = "The rating is now RAA + kappa*WPA (D-P11); the returned *_epr columns carry impact values."
  ))
  out <- calculate_impact(
    format = format, player_game_data = player_game_data, ref_date = ref_date,
    decay_batting = decay_batting, decay_bowling = decay_bowling,
    prior_matches = prior_matches, prior_rate = prior_rate
  )
  if (nrow(out) > 0) {
    data.table::setnames(out,
      c("batting_impact", "bowling_impact", "total_impact"),
      c("batting_epr", "bowling_epr", "total_epr"))
  }
  out[]
}
