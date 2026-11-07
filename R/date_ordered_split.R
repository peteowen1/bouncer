# Date-ordered train/test split for the ball-outcome trainers.
#
# ADDED 2026-11-07 (bouncerverse NEXT-STEPS, ODI retrain 2026-10-29). The two
# trainers split differently and neither split was usable for a comparison:
#   - 01_train_agnostic_model.R shuffled match ids (random split), so its
#     held-out matches were spread over all years;
#   - 02_train_full_model.R took unique(match_id) in ROW order, and DuckDB
#     returns rows in no fixed order, so three rebuilds held out 544,559 /
#     548,526 / 549,407 deliveries -- a different test set every run.
# The ODI comparison between them was therefore unfair AND unrepeatable. Both
# now hold out the most recent matches by date, which is also the question the
# models are used for: predicting matches that come after the training data.

#' Split Matches Into Train and Test by Match Date
#'
#' Orders matches by `match_date` (ties broken by `match_id`, so the split is
#' identical on every run whatever order the rows arrive in) and holds out
#' the latest `test_frac` of matches.
#'
#' @param match_id Vector of match ids, one per row (repeats allowed).
#' @param match_date Vector of match dates, aligned with `match_id`.
#' @param test_frac Numeric in (0, 1). Share of matches held out.
#' @return A list with `train_matches`, `test_matches` and `cutoff_date`
#'   (the first held-out match's date).
#' @keywords internal
date_ordered_match_split <- function(match_id, match_date, test_frac = 0.2) {
  if (length(match_id) != length(match_date)) {
    cli::cli_abort("{.arg match_id} and {.arg match_date} must be the same length.")
  }
  if (!is.numeric(test_frac) || length(test_frac) != 1 || test_frac <= 0 || test_frac >= 1) {
    cli::cli_abort("{.arg test_frac} must be a single number strictly between 0 and 1.")
  }
  m <- unique(data.table::data.table(match_id = match_id,
                                     match_date = .as_match_date(match_date)))
  if (anyDuplicated(m$match_id)) {
    cli::cli_abort("{sum(duplicated(m$match_id))} match id(s) carry more than one date.")
  }
  # A NULL date would sort first or last and silently move a match across the
  # cutoff, so an undated match is an error, not a guess.
  if (anyNA(m$match_date)) {
    cli::cli_abort(c(
      "{sum(is.na(m$match_date))} of {nrow(m)} matches have no match_date.",
      "i" = "The split orders by date, so every match needs one (cricsheet.matches.match_date)."))
  }
  if (nrow(m) < 2) cli::cli_abort("Need at least 2 matches to split, got {nrow(m)}.")
  data.table::setorder(m, match_date, match_id)
  n_train <- floor((1 - test_frac) * nrow(m))
  n_train <- min(max(n_train, 1L), nrow(m) - 1L)
  list(
    train_matches = m$match_id[seq_len(n_train)],
    test_matches  = m$match_id[(n_train + 1):nrow(m)],
    cutoff_date   = m$match_date[n_train + 1]
  )
}

# DuckDB returns DATE columns as Date, but a character column must not go
# through bare as.Date(): it picks one format from the first value and NAs the
# rest silently. Only ISO dates are accepted.
.as_match_date <- function(x) {
  if (inherits(x, "Date")) return(x)
  if (inherits(x, "POSIXt")) return(as.Date(x))
  if (is.character(x)) {
    bad <- !is.na(x) & !grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}", x)
    if (any(bad)) cli::cli_abort("{sum(bad)} match_date value(s) are not ISO dates, e.g. {.val {x[bad][1]}}.")
    return(as.Date(substr(x, 1, 10), format = "%Y-%m-%d"))
  }
  cli::cli_abort("match_date must be Date, POSIXct or ISO character, got {.cls {class(x)[1]}}.")
}
