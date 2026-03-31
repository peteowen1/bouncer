# Player Stat Value (PSV) and BatV/BowlV
# =========================================
# Cricket equivalent of torp's PSR/OSR/DSR system.
#
# PSR (Player Stat Rating): career-level composite from stat ratings
# PSV (Player Stat Value): per-game composite from single-match stats
# BatV (Batting Value): batting contribution to margin
# BowlV (Bowling Value): bowling contribution to margin
#
# Models trained via glmnet elastic-net on match margin:
#   - Margin model: margin ~ all_stat_ratings (authoritative total)
#   - Batting model: team_runs_scored ~ batting_ratings + opp_bowling_ratings
#   - Bowling model: team_runs_conceded ~ bowling_ratings + opp_batting_ratings


#' Calculate Player Stat Rating (PSR) from Stat Ratings
#'
#' Applies glmnet coefficients to per-stat Bayesian ratings to produce
#' a composite player rating. This is the career-level, predictive version
#' of player value.
#'
#' @param ratings data.table with \code{{stat}_rating} columns from
#'   \code{\link{estimate_player_stat_ratings}}.
#' @param coef_df data.frame with columns \code{stat_name} and \code{beta}.
#'   Optionally \code{sd} for standardization.
#' @param center Logical. If TRUE (default), subtract league mean so PSR
#'   represents value above average.
#'
#' @return data.table with identifier columns plus \code{psr_raw} and \code{psr}.
#' @export
calculate_psr <- function(ratings, coef_df, center = TRUE) {
  dt <- data.table::as.data.table(ratings)

  if (!all(c("stat_name", "beta") %in% names(coef_df))) {
    cli::cli_abort("{.arg coef_df} must have columns {.val stat_name} and {.val beta}")
  }

  coef_df <- coef_df[coef_df$beta != 0, , drop = FALSE]
  if (nrow(coef_df) == 0) {
    dt[, c("psr_raw", "psr") := 0]
    id_cols <- intersect(c("player_id", "role_group"), names(dt))
    return(dt[, c(id_cols, "psr_raw", "psr"), with = FALSE])
  }

  # Map stat_name to rating column
  rating_cols <- paste0(coef_df$stat_name, "_rating")
  available <- rating_cols %in% names(dt)

  if (sum(available) == 0) {
    cli::cli_abort("No matching _rating columns found in data")
  }
  if (any(!available)) {
    missing <- coef_df$stat_name[!available]
    cli::cli_warn("Missing rating columns for: {paste(missing, collapse = ', ')}")
  }

  coef_df <- coef_df[available, , drop = FALSE]
  rating_cols <- rating_cols[available]

  # Build rating matrix
  rating_mat <- as.matrix(dt[, rating_cols, with = FALSE])
  rating_mat[is.na(rating_mat)] <- 0

  # Standardize if SD column present
  if ("sd" %in% names(coef_df)) {
    sd_vec <- coef_df$sd
    sd_vec[sd_vec == 0 | is.na(sd_vec)] <- 1
    rating_mat <- sweep(rating_mat, 2, sd_vec, "/")
  }

  # Apply betas
  dt[, psr_raw := as.numeric(rating_mat %*% coef_df$beta)]

  if (center) {
    dt[, psr := psr_raw - mean(psr_raw, na.rm = TRUE)]
  } else {
    dt[, psr := psr_raw]
  }

  id_cols <- intersect(c("player_id", "role_group", "n_matches", "wt_matches"), names(dt))
  dt[, c(id_cols, "psr_raw", "psr"), with = FALSE]
}


#' Calculate PSR Components: PSR + BatV + BowlV
#'
#' Applies three sets of glmnet coefficients to decompose PSR into
#' batting value (BatV) and bowling value (BowlV).
#'
#' @inheritParams calculate_psr
#' @param batv_coef_df Coefficient data.frame for the batting model.
#' @param bowlv_coef_df Coefficient data.frame for the bowling model.
#'
#' @return data.table with \code{psr}, \code{batv}, \code{bowlv} columns.
#'   BatV + BowlV = PSR (enforced via additive shift).
#'
#' @export
calculate_psr_components <- function(ratings, coef_df, batv_coef_df,
                                      bowlv_coef_df, center = TRUE) {
  psr_result <- calculate_psr(ratings, coef_df, center = center)
  batv_result <- calculate_psr(ratings, batv_coef_df, center = center)
  bowlv_result <- calculate_psr(ratings, bowlv_coef_df, center = center)

  # Additive shift so BatV + BowlV = PSR exactly
  raw_batv <- batv_result$psr
  raw_bowlv <- bowlv_result$psr
  delta <- (psr_result$psr - raw_batv - raw_bowlv) / 2

  psr_result[, batv := raw_batv + delta]
  psr_result[, bowlv := raw_bowlv + delta]

  psr_result
}


#' Calculate Per-Game Player Stat Value (PSV)
#'
#' Applies glmnet coefficients to single-match box-score stats.
#' This is the per-game equivalent of PSR — how valuable was this
#' player's stat line in this specific match?
#'
#' @param player_game_data data.table from \code{\link{load_player_game_data}}.
#' @param coef_df data.frame with columns \code{stat_name} and \code{beta}.
#' @param exposure_adjust Logical. If TRUE (default), divide raw counts by
#'   balls to get per-ball rates matching the scale coefficients were trained on.
#' @param center Logical. If TRUE (default), subtract per-match mean so PSV
#'   represents value above average player in that match.
#'
#' @return data.table with identifier columns plus \code{psv_raw} and \code{psv}.
#' @export
calculate_psv <- function(player_game_data, coef_df,
                           exposure_adjust = TRUE, center = TRUE) {
  dt <- data.table::as.data.table(player_game_data)

  if (!all(c("stat_name", "beta") %in% names(coef_df))) {
    cli::cli_abort("{.arg coef_df} must have columns {.val stat_name} and {.val beta}")
  }

  coef_df <- coef_df[coef_df$beta != 0, , drop = FALSE]
  if (nrow(coef_df) == 0) {
    dt[, c("psv_raw", "psv") := 0]
    id_cols <- intersect(c("match_id", "player_id", "role"), names(dt))
    return(dt[, c(id_cols, "psv_raw", "psv"), with = FALSE])
  }

  # Map stat_name to raw stat columns (not _rating)
  stat_cols <- coef_df$stat_name
  available <- stat_cols %in% names(dt)
  if (any(!available)) {
    cli::cli_warn("Missing stat columns for PSV: {paste(stat_cols[!available], collapse = ', ')}")
  }
  coef_df <- coef_df[available, , drop = FALSE]
  stat_cols <- stat_cols[available]

  # Build stat matrix
  stat_mat <- as.matrix(dt[, stat_cols, with = FALSE])
  stat_mat[is.na(stat_mat)] <- 0

  # Exposure adjustment: convert counts to per-ball rates
  if (exposure_adjust) {
    stat_defs <- stat_rating_definitions()
    for (j in seq_along(stat_cols)) {
      sn <- stat_cols[j]
      sdef <- stat_defs[stat_defs$stat_name == sn, ]
      if (nrow(sdef) > 0 && !is.na(sdef$exposure_col[1])) {
        exp_col <- sdef$exposure_col[1]
        if (exp_col %in% names(dt)) {
          exp_vals <- as.numeric(dt[[exp_col]])
          exp_vals[exp_vals == 0 | is.na(exp_vals)] <- 1
          stat_mat[, j] <- stat_mat[, j] / exp_vals
        }
      }
    }
  }

  # Standardize if SD column present
  if ("sd" %in% names(coef_df)) {
    sd_vec <- coef_df$sd
    sd_vec[sd_vec == 0 | is.na(sd_vec)] <- 1
    stat_mat <- sweep(stat_mat, 2, sd_vec, "/")
  }

  dt[, psv_raw := as.numeric(stat_mat %*% coef_df$beta)]

  if (center) {
    dt[, psv := psv_raw - mean(psv_raw, na.rm = TRUE), by = match_id]
  } else {
    dt[, psv := psv_raw]
  }

  id_cols <- intersect(c("match_id", "player_id", "match_date", "role"), names(dt))
  dt[, c(id_cols, "psv_raw", "psv"), with = FALSE]
}


#' Calculate Per-Game PSV Components: PSV + BatV + BowlV
#'
#' @inheritParams calculate_psv
#' @param batv_coef_df Coefficient data.frame for the batting model.
#' @param bowlv_coef_df Coefficient data.frame for the bowling model.
#'
#' @return data.table with \code{psv}, \code{batv}, \code{bowlv} columns.
#' @export
calculate_psv_components <- function(player_game_data, coef_df,
                                      batv_coef_df, bowlv_coef_df,
                                      exposure_adjust = TRUE, center = TRUE) {
  psv_result <- calculate_psv(player_game_data, coef_df,
                               exposure_adjust = exposure_adjust, center = center)
  batv_result <- calculate_psv(player_game_data, batv_coef_df,
                                exposure_adjust = exposure_adjust, center = center)
  bowlv_result <- calculate_psv(player_game_data, bowlv_coef_df,
                                 exposure_adjust = exposure_adjust, center = center)

  raw_batv <- batv_result$psv
  raw_bowlv <- bowlv_result$psv
  delta <- (psv_result$psv - raw_batv - raw_bowlv) / 2

  psv_result[, batv := raw_batv + delta]
  psv_result[, bowlv := raw_bowlv + delta]

  psv_result
}


#' Aggregate Team Stat Ratings for Match Prediction
#'
#' Sums the top-11 players' stat ratings per team per match, producing
#' the feature matrix used by the glmnet margin model.
#'
#' @param stat_ratings data.table from \code{\link{estimate_player_stat_ratings}}.
#' @param match_data data.table with match_id, team1, team2 columns and
#'   player-team assignments.
#' @param stat_defs data.frame from \code{\link{stat_rating_definitions}}.
#'
#' @return data.table with one row per match, containing aggregated stat
#'   rating features for both teams.
#' @export
aggregate_team_stat_ratings <- function(stat_ratings, match_data, stat_defs = NULL) {
  if (is.null(stat_defs)) stat_defs <- stat_rating_definitions()

  dt <- data.table::as.data.table(stat_ratings)
  md <- data.table::as.data.table(match_data)

  rating_cols <- paste0(stat_defs$stat_name, "_rating")
  rating_cols <- intersect(rating_cols, names(dt))

  if (length(rating_cols) == 0) {
    cli::cli_abort("No rating columns found in stat_ratings")
  }

  # Sum ratings per team per match (top 11 by total weighted matches)
  team_agg <- dt[md, on = c("player_id", "match_id"), nomatch = 0]
  team_agg[, team_rank := frank(-wt_matches, ties.method = "first"), by = .(match_id, team)]

  top11 <- team_agg[team_rank <= 11]
  team_totals <- top11[, lapply(.SD, sum, na.rm = TRUE),
                        .SDcols = rating_cols,
                        by = .(match_id, team)]

  team_totals
}
