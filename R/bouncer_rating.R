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

  # calculate_epr() returns a zero-column data.table when no matches fall
  # before ref_date. Say so here rather than letting the column selection
  # below fail with "column(s) not found".
  needed <- c("player_id", "role_group", "total_epr", "batting_epr",
              "bowling_epr", "n_matches", "wt_matches")
  missing_cols <- setdiff(needed, names(epr_dt))
  if (nrow(epr_dt) == 0L || length(missing_cols) > 0L) {
    cli::cli_abort(c(
      "{.arg epr} has no usable rows.",
      "i" = if (nrow(epr_dt) == 0L) "It is empty -- check ref_date and the format filter."
            else "Missing column{?s}: {.field {missing_cols}}."
    ))
  }

  # Merge on player_id
  result <- merge(
    epr_dt[, .SD, .SDcols = needed],
    psr_dt[, .(player_id, psr)],
    by = "player_id",
    all = FALSE  # inner join: only players with both
  )

  if (nrow(result) == 0L) {
    cli::cli_abort("No players appear in both {.arg epr} and {.arg psr}.")
  }

  # Put PSR on EPR's scale before blending. Note this divides by the SD
  # WITHOUT centring, so these are ratio-scaled values, not z-scores -- the
  # blend is a scale match, and bouncer_rating stays in EPR units.
  epr_sd <- stats::sd(result$total_epr, na.rm = TRUE)
  psr_sd <- stats::sd(result$psr, na.rm = TRUE)

  # sd() is NA for a single row and for an all-NA column, and `if (NA)` is an
  # error, not FALSE -- so isTRUE(), not a bare comparison.
  scalable <- isTRUE(epr_sd > 0) && isTRUE(psr_sd > 0)

  if (scalable) {
    result[, epr_scaled := total_epr / epr_sd]
    result[, psr_scaled := psr / psr_sd]
    result[, bouncer_rating :=
             (weight_epr * epr_scaled + (1 - weight_epr) * psr_scaled) * epr_sd]
    result[, c("epr_scaled", "psr_scaled") := NULL]
  } else {
    # Unscaled blend: a DIFFERENT quantity in different units, because EPR and
    # PSR are no longer comparable. Flagged so a caller can tell them apart.
    cli::cli_warn(c(
      "Cannot scale EPR and PSR to a common spread; blending them unscaled.",
      "i" = "EPR sd = {.val {epr_sd}}, PSR sd = {.val {psr_sd}} over {nrow(result)} player{?s}.",
      "!" = "bouncer_rating is not comparable with a scaled run."
    ))
    result[, bouncer_rating := weight_epr * total_epr + (1 - weight_epr) * psr]
  }
  data.table::setattr(result, "bouncer_scaled", scalable)

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
#' @param weight_epr Numeric. Weight for the EPR component, passed to
#'   \code{\link{calculate_bouncer}}. Ignored in EPR-only mode.
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

  # EPR-only leaderboard: bouncer_rating IS total_epr, psr unavailable.
  epr_only <- function() {
    out <- epr[, .(player_id, role_group, bouncer_rating = total_epr,
                   total_epr, batting_epr, bowling_epr,
                   psr = NA_real_, n_matches, wt_matches)]
    data.table::setorder(out, -bouncer_rating)
    out
  }

  coef_path <- system.file("extdata", "psr_coefficients.csv", package = "bouncer")

  result <- if (is.null(stat_ratings)) {
    epr_only()
  } else if (coef_path == "" || !file.exists(coef_path)) {
    cli::cli_warn("PSR coefficient file not found. Using EPR only.")
    epr_only()
  } else {
    coef_df <- utils::read.csv(coef_path, stringsAsFactors = FALSE)
    psr <- calculate_psr(stat_ratings, coef_df)
    calculate_bouncer(epr, psr, weight_epr = weight_epr)
  }

  if (!is.null(n_top)) {
    result <- utils::head(result, n_top)
  }

  result
}
