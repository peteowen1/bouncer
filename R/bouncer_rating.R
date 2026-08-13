# BOUNCER Composite Rating
# =========================
# Blends career delivery-value (impact: RAA + kappa*WPA, D-P11) with career
# stat-based (PSR) ratings.
# BOUNCER = weight_epr * impact + (1 - weight_epr) * PSR
#
# Analogous to TORP = 0.5 * EPR + 0.5 * PSR in torpverse. The delivery-value
# component was called EPR until 2026-08-14; the parameter name weight_epr is
# kept for API stability.


#' Calculate BOUNCER Composite Rating
#'
#' Blends impact (delivery-level career value, [calculate_impact()]) with PSR
#' (stat-based career value) into a single composite player rating.
#'
#' @param impact data.table from \code{\link{calculate_impact}} with columns
#'   \code{player_id}, \code{total_impact}, \code{batting_impact},
#'   \code{bowling_impact}.
#' @param psr data.table from \code{\link{calculate_psr}} with columns
#'   \code{player_id}, \code{psr}.
#' @param weight_epr Numeric. Weight for the impact component (default 0.5).
#'   PSR weight = 1 - weight_epr. (Name kept from the EPR era for API
#'   stability.)
#'
#' @return data.table with columns:
#'   \code{player_id}, \code{role_group}, \code{bouncer_rating},
#'   \code{total_impact}, \code{psr}, \code{batting_impact},
#'   \code{bowling_impact}, \code{n_matches}, \code{wt_matches}.
#'
#' @export
calculate_bouncer <- function(impact, psr, weight_epr = 0.5) {
  imp_dt <- data.table::as.data.table(impact)
  psr_dt <- data.table::as.data.table(psr)

  # calculate_impact() returns a zero-column data.table when no matches fall
  # before ref_date. Say so here rather than letting the column selection
  # below fail with "column(s) not found".
  needed <- c("player_id", "role_group", "total_impact", "batting_impact",
              "bowling_impact", "n_matches", "wt_matches")
  missing_cols <- setdiff(needed, names(imp_dt))
  if (nrow(imp_dt) == 0L || length(missing_cols) > 0L) {
    cli::cli_abort(c(
      "{.arg impact} has no usable rows.",
      "i" = if (nrow(imp_dt) == 0L) "It is empty -- check ref_date and the format filter."
            else "Missing column{?s}: {.field {missing_cols}}."
    ))
  }

  # Merge on player_id
  result <- merge(
    imp_dt[, .SD, .SDcols = needed],
    psr_dt[, .(player_id, psr)],
    by = "player_id",
    all = FALSE  # inner join: only players with both
  )

  if (nrow(result) == 0L) {
    cli::cli_abort("No players appear in both {.arg impact} and {.arg psr}.")
  }

  # Put PSR on impact's scale before blending. Note this divides by the SD
  # WITHOUT centring, so these are ratio-scaled values, not z-scores -- the
  # blend is a scale match, and bouncer_rating stays in impact (run) units.
  imp_sd <- stats::sd(result$total_impact, na.rm = TRUE)
  psr_sd <- stats::sd(result$psr, na.rm = TRUE)

  # sd() is NA for a single row and for an all-NA column, and `if (NA)` is an
  # error, not FALSE -- so isTRUE(), not a bare comparison.
  scalable <- isTRUE(imp_sd > 0) && isTRUE(psr_sd > 0)

  if (scalable) {
    result[, imp_scaled := total_impact / imp_sd]
    result[, psr_scaled := psr / psr_sd]
    result[, bouncer_rating :=
             (weight_epr * imp_scaled + (1 - weight_epr) * psr_scaled) * imp_sd]
    result[, c("imp_scaled", "psr_scaled") := NULL]
  } else {
    # Unscaled blend: a DIFFERENT quantity in different units, because impact
    # and PSR are no longer comparable. Flagged so a caller can tell them
    # apart.
    cli::cli_warn(c(
      "Cannot scale impact and PSR to a common spread; blending them unscaled.",
      "i" = "impact sd = {.val {imp_sd}}, PSR sd = {.val {psr_sd}} over {nrow(result)} player{?s}.",
      "!" = "bouncer_rating is not comparable with a scaled run."
    ))
    result[, bouncer_rating := weight_epr * total_impact + (1 - weight_epr) * psr]
  }
  data.table::setattr(result, "bouncer_scaled", scalable)

  data.table::setorder(result, -bouncer_rating)
  result
}


#' BOUNCER Ratings Convenience Wrapper
#'
#' Loads impact and PSR, computes the BOUNCER composite, and returns a
#' user-friendly leaderboard.
#'
#' @param format Character. "t20", "odi", or "test".
#' @param player_game_data Optional pre-loaded player game data.
#' @param stat_ratings Optional pre-loaded stat ratings.
#' @param weight_epr Numeric. Weight for the impact component, passed to
#'   \code{\link{calculate_bouncer}}. Ignored in impact-only mode.
#' @param n_top Integer. Number of top players to show (NULL = all).
#'
#' @return data.table with BOUNCER ratings, sorted by bouncer_rating.
#' @export
bouncer_ratings <- function(format = c("t20", "odi", "test"),
                             player_game_data = NULL,
                             stat_ratings = NULL,
                             weight_epr = 0.5,
                             n_top = NULL) {
  format <- match.arg(format)

  # Compute impact (delivery-level career value)
  impact <- calculate_impact(format, player_game_data = player_game_data)

  # Compute PSR (needs stat ratings + coefficients)
  if (is.null(stat_ratings)) {
    stat_ratings <- tryCatch(
      load_stat_ratings(format),
      error = function(e) {
        cli::cli_warn("Could not load stat ratings: {e$message}. Using impact only.")
        NULL
      }
    )
  }

  # Impact-only leaderboard: bouncer_rating IS total_impact, psr unavailable.
  impact_only <- function() {
    out <- impact[, .(player_id, role_group, bouncer_rating = total_impact,
                      total_impact, batting_impact, bowling_impact,
                      psr = NA_real_, n_matches, wt_matches)]
    data.table::setorder(out, -bouncer_rating)
    out
  }

  coef_path <- system.file("extdata", "psr_coefficients.csv", package = "bouncer")

  result <- if (is.null(stat_ratings)) {
    impact_only()
  } else if (coef_path == "" || !file.exists(coef_path)) {
    cli::cli_warn("PSR coefficient file not found. Using impact only.")
    impact_only()
  } else {
    coef_df <- utils::read.csv(coef_path, stringsAsFactors = FALSE)
    psr <- calculate_psr(stat_ratings, coef_df)
    calculate_bouncer(impact, psr, weight_epr = weight_epr)
  }

  if (!is.null(n_top)) {
    result <- utils::head(result, n_top)
  }

  result
}
