# Player Game Ratings
# ====================
# Display-ready per-game ratings combining box-score stats with value metrics.
# One row per player per match, showing WPA, ERA, PSV, BatV, BowlV.


#' Player Game Ratings
#'
#' Returns display-ready per-game player ratings for a specific match
#' or set of matches. Includes box-score stats, WPA, ERA, and
#' PSV/BatV/BowlV (if coefficients are available).
#'
#' @param format Character. "t20", "odi", or "test".
#' @param match_ids Character vector. Specific match IDs (NULL = all).
#' @param source Character. "local" or "remote".
#'
#' @return data.table with columns:
#'   match_id, player_id, role, batting/bowling stats,
#'   batting_wpa, bowling_wpa, total_wpa,
#'   batting_era, bowling_era, total_era,
#'   psv, batv, bowlv (if PSR model trained).
#'
#' @export
player_game_ratings <- function(format = c("t20", "odi", "test"),
                                 match_ids = NULL,
                                 source = c("local", "remote")) {
  format <- match.arg(format)
  source <- match.arg(source)

  pgd <- load_player_game_data(format, match_ids = match_ids, source = source)

  if (nrow(pgd) == 0) {
    cli::cli_warn("No player game data found")
    return(data.table::data.table())
  }

  # Try to add PSV if coefficients exist
  coef_path <- system.file("extdata", "psr_coefficients.csv", package = "bouncer")
  if (coef_path != "" && file.exists(coef_path)) {
    coef_df <- utils::read.csv(coef_path, stringsAsFactors = FALSE)
    psv <- tryCatch(
      calculate_psv(pgd, coef_df),
      error = function(e) NULL
    )
    if (!is.null(psv)) {
      pgd[psv, psv := i.psv, on = c("match_id", "player_id")]
    }

    # BatV/BowlV if available
    batv_path <- system.file("extdata", "batv_coefficients.csv", package = "bouncer")
    bowlv_path <- system.file("extdata", "bowlv_coefficients.csv", package = "bouncer")
    if (file.exists(batv_path) && file.exists(bowlv_path)) {
      batv_coef <- utils::read.csv(batv_path, stringsAsFactors = FALSE)
      bowlv_coef <- utils::read.csv(bowlv_path, stringsAsFactors = FALSE)
      psv_comp <- tryCatch(
        calculate_psv_components(pgd, coef_df, batv_coef, bowlv_coef),
        error = function(e) NULL
      )
      if (!is.null(psv_comp)) {
        pgd[psv_comp, `:=`(batv = i.batv, bowlv = i.bowlv),
            on = c("match_id", "player_id")]
      }
    }
  }

  # Sort by total_wpa within each match
  data.table::setorder(pgd, match_id, -total_wpa)

  pgd
}


#' Match MVP Rankings
#'
#' Returns player rankings for a specific match, ordered by total value
#' contribution (WPA + ERA or PSV if available).
#'
#' @param match_id Character. Single match ID.
#' @param format Character. "t20", "odi", or "test".
#' @param source Character. "local" or "remote".
#' @param n_top Integer. Number of top players to show (NULL = all).
#'
#' @return data.table with match MVP rankings.
#' @export
match_mvp <- function(match_id, format = c("t20", "odi", "test"),
                       source = c("local", "remote"), n_top = 5) {
  format <- match.arg(format)
  source <- match.arg(source)

  ratings <- player_game_ratings(format, match_ids = match_id, source = source)
  if (nrow(ratings) == 0) return(data.table::data.table())

  # Select display columns
  display_cols <- intersect(
    c("player_id", "role",
      "batting_runs", "batting_balls_faced", "batting_strike_rate",
      "bowling_wickets", "bowling_balls_bowled", "bowling_economy",
      "batting_wpa", "bowling_wpa", "total_wpa",
      "batting_era", "bowling_era", "total_era",
      "psv", "batv", "bowlv"),
    names(ratings)
  )

  result <- ratings[, display_cols, with = FALSE]
  data.table::setorder(result, -total_wpa)

  if (!is.null(n_top)) {
    result <- utils::head(result, n_top)
  }

  result
}
