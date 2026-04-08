# BOUNCER Composite Rating
# =========================
# Blends career delivery-value (EPR) with career stat-based (PSR) ratings.
# BOUNCER = weight_epr * EPR + (1 - weight_epr) * PSR
#
# Analogous to TORP = 0.5 * EPR + 0.5 * PSR in torpverse.


#' Calculate BOUNCER Composite Rating
#'
#' Blends EPR (delivery-level career value) with PSR (stat-based career value)
#' into a single composite player rating.
#'
#' @param epr data.table from \code{\link{calculate_epr}} with columns
#'   \code{player_id}, \code{total_epr}, \code{batting_epr}, \code{bowling_epr}.
#' @param psr data.table from \code{\link{calculate_psr}} with columns
#'   \code{player_id}, \code{psr}.
#' @param weight_epr Numeric. Weight for EPR component (default 0.5).
#'   PSR weight = 1 - weight_epr.
#'
#' @return data.table with columns:
#'   \code{player_id}, \code{role_group}, \code{bouncer_rating},
#'   \code{total_epr}, \code{psr}, \code{batting_epr}, \code{bowling_epr},
#'   \code{n_matches}, \code{wt_matches}.
#'
#' @export
calculate_bouncer <- function(epr, psr, weight_epr = 0.5) {
  epr_dt <- data.table::as.data.table(epr)
  psr_dt <- data.table::as.data.table(psr)

  # Merge on player_id
  result <- merge(
    epr_dt[, .(player_id, role_group, total_epr, batting_epr, bowling_epr,
               n_matches, wt_matches)],
    psr_dt[, .(player_id, psr)],
    by = "player_id",
    all = FALSE  # inner join: only players with both
  )

  # Standardize both components to same scale before blending
  epr_sd <- stats::sd(result$total_epr, na.rm = TRUE)
  psr_sd <- stats::sd(result$psr, na.rm = TRUE)

  if (epr_sd > 0 && psr_sd > 0) {
    result[, epr_z := total_epr / epr_sd]
    result[, psr_z := psr / psr_sd]
    result[, bouncer_z := weight_epr * epr_z + (1 - weight_epr) * psr_z]
    # Scale back to interpretable units (use EPR scale)
    result[, bouncer_rating := bouncer_z * epr_sd]
    result[, c("epr_z", "psr_z", "bouncer_z") := NULL]
  } else {
    result[, bouncer_rating := weight_epr * total_epr + (1 - weight_epr) * psr]
  }

  data.table::setorder(result, -bouncer_rating)
  result
}


#' BOUNCER Ratings Convenience Wrapper
#'
#' Loads EPR and PSR, computes BOUNCER composite, and returns a
#' user-friendly leaderboard.
#'
#' @param format Character. "t20", "odi", or "test".
#' @param player_game_data Optional pre-loaded player game data.
#' @param stat_ratings Optional pre-loaded stat ratings.
#' @param n_top Integer. Number of top players to show (NULL = all).
#'
#' @return data.table with BOUNCER ratings, sorted by bouncer_rating.
#' @export
bouncer_ratings <- function(format = c("t20", "odi", "test"),
                             player_game_data = NULL,
                             stat_ratings = NULL,
                             n_top = NULL) {
  format <- match.arg(format)

  # Compute EPR
  epr <- calculate_epr(format, player_game_data = player_game_data)

  # Compute PSR (needs stat ratings + coefficients)
  if (is.null(stat_ratings)) {
    stat_ratings <- tryCatch(
      load_stat_ratings(format),
      error = function(e) {
        cli::cli_warn("Could not load stat ratings: {e$message}. Using EPR only.")
        NULL
      }
    )
  }

  if (is.null(stat_ratings)) {
    # EPR-only mode (no PSR coefficients available yet)
    result <- epr[, .(player_id, role_group, bouncer_rating = total_epr,
                       total_epr, batting_epr, bowling_epr,
                       psr = NA_real_, n_matches, wt_matches)]
    data.table::setorder(result, -bouncer_rating)
  } else {
    # Load PSR coefficients
    coef_path <- system.file("extdata", "psr_coefficients.csv", package = "bouncer")
    if (coef_path == "" || !file.exists(coef_path)) {
      cli::cli_warn("PSR coefficient file not found. Using EPR only.")
      result <- epr[, .(player_id, role_group, bouncer_rating = total_epr,
                         total_epr, batting_epr, bowling_epr,
                         psr = NA_real_, n_matches, wt_matches)]
      data.table::setorder(result, -bouncer_rating)
    } else {
      coef_df <- utils::read.csv(coef_path, stringsAsFactors = FALSE)
      psr <- calculate_psr(stat_ratings, coef_df)
      result <- calculate_bouncer(epr, psr)
    }
  }

  if (!is.null(n_top)) {
    result <- utils::head(result, n_top)
  }

  result
}
