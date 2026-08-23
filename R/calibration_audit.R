# Calibration-and-bias audit, generalised from the agnostic-model leak.
#
# The agnostic ball-outcome model carried a post-delivery leak for months.
# Aggregate calibration looked healthy throughout (0.856 predicted vs 0.909
# actual, over-level). It was caught by looking at the FIRST BALL OF AN
# INNINGS: every innings opens 0/0, so a model with no player identity must
# predict nearly the same value for all of them -- it predicted 0.005 to
# 5.499 runs, correlating 1.000 with the runs off that very ball. Found by
# reading a worked example by eye, twice. See
# docs/reference/MODEL-VALIDATION-PROTOCOL.md for the full write-up.
#
# Two things made that leak invisible until someone went looking on purpose:
# it never showed up in an average, and it needed a LOT of rows in the wrong
# bucket to be a coincidence rather than noise. So the two design rules here
# are not stylistic preferences -- they are the fix for how the incident hid:
#  1. Only flag a bucket once it has enough rows to mean something; report
#     small buckets separately rather than silently dropping them (a cut that
#     examined nothing must not read as a pass).
#  2. Surface the EXTREME buckets, not the average -- the leak was invisible
#     in the aggregate and obvious at the boundary.

#' Per-bucket calibration audit across arbitrary cuts
#'
#' For each cut variable supplied in `cuts`, buckets `predicted`/`actual` by
#' the distinct values of that variable and reports the mean prediction, mean
#' actual outcome, and bias (predicted minus actual) per bucket. This is the
#' generalised form of the check that caught the agnostic-model post-delivery
#' leak (bouncerverse, D-P38): aggregate calibration was healthy throughout,
#' and the leak was only visible in specific buckets (the first ball of an
#' innings).
#'
#' Every cut is optional -- pass whichever cut variables you have (ball
#' number, over, innings number, wickets fallen, phase, margin bucket,
#' competition, venue country, home/away, gender, format, season, ...). Each
#' element of `cuts` must be the same length as `predicted` and `actual`.
#'
#' A bucket with fewer than `min_n` rows is reported but flagged
#' `"not_judged"` rather than dropped -- a biased bucket of 12 balls is noise,
#' but a cut that examined nothing must not read as a pass either. A cut
#' whose values are constant (a single bucket) cannot compare anything and is
#' flagged `"single_bucket"` for every row of that cut, never `"ok"`.
#'
#' @param predicted Numeric vector of model predictions.
#' @param actual Numeric vector of realised outcomes, same length as
#'   `predicted`.
#' @param cuts A named list of cut vectors, each the same length as
#'   `predicted`/`actual`. Names become the `cut` column. Rows with `NA` in
#'   the cut, prediction or actual are excluded from that cut's buckets.
#' @param min_n Minimum bucket size to be judged (default 30). Buckets below
#'   this are flagged `"not_judged"`, not dropped.
#' @param flag_threshold Optional. If supplied, judged buckets with
#'   `abs(bias) > flag_threshold` are flagged `"high_bias"` instead of
#'   `"ok"`. Leave `NULL` (default) to rely on [worst_calibration_buckets()]
#'   for surfacing the worst offenders instead of a hard cutoff.
#'
#' @return A data frame (class `calibration_audit`) with one row per bucket
#'   per cut: `cut`, `bucket`, `n`, `judged`, `mean_predicted`, `mean_actual`,
#'   `bias` (predicted minus actual), `abs_bias`, `flag`
#'   (`"ok"`/`"high_bias"`/`"not_judged"`/`"single_bucket"`/`"no_data"`).
#' @seealso [worst_calibration_buckets()], [audit_low_information_state()]
#' @export
calibration_audit <- function(predicted, actual, cuts, min_n = 30,
                               flag_threshold = NULL) {
  if (!is.list(cuts) || length(cuts) == 0 || is.null(names(cuts)) ||
      any(!nzchar(names(cuts)))) {
    cli::cli_abort("{.arg cuts} must be a non-empty named list of cut vectors.")
  }
  n_rows <- length(predicted)
  if (length(actual) != n_rows) {
    cli::cli_abort("{.arg predicted} and {.arg actual} must be the same length.")
  }
  bad_len <- vapply(cuts, length, integer(1)) != n_rows
  if (any(bad_len)) {
    cli::cli_abort(c(
      "Every cut vector must be the same length as {.arg predicted}.",
      "x" = "{.val {names(cuts)[bad_len]}} {?is/are} the wrong length."))
  }

  rows <- lapply(names(cuts), function(cut_name) {
    .one_cut_audit(cut_name, cuts[[cut_name]], predicted, actual,
                    min_n = min_n, flag_threshold = flag_threshold)
  })

  out <- do.call(rbind, rows)
  rownames(out) <- NULL
  structure(out, class = c("calibration_audit", "data.frame"))
}

# One cut's worth of bucketed calibration. Split out so calibration_audit()
# stays readable as "loop over cuts, glue the results together".
.one_cut_audit <- function(cut_name, bucket, predicted, actual, min_n,
                            flag_threshold) {
  keep <- !is.na(bucket) & !is.na(predicted) & !is.na(actual)
  if (!any(keep)) {
    return(data.frame(
      cut = cut_name, bucket = NA_character_, n = 0L, judged = FALSE,
      mean_predicted = NA_real_, mean_actual = NA_real_, bias = NA_real_,
      abs_bias = NA_real_, flag = "no_data", stringsAsFactors = FALSE))
  }

  df <- data.frame(bucket = bucket[keep], predicted = predicted[keep],
                    actual = actual[keep])
  grouped <- split(df[, c("predicted", "actual")], df$bucket)
  bucket_names <- names(grouped)
  n <- vapply(grouped, nrow, integer(1))
  mean_predicted <- vapply(grouped, function(g) mean(g$predicted), numeric(1))
  mean_actual <- vapply(grouped, function(g) mean(g$actual), numeric(1))
  bias <- mean_predicted - mean_actual

  n_buckets <- length(bucket_names)
  if (n_buckets <= 1) {
    # A single-valued cut has nothing to compare -- reporting it as "ok"
    # would read as a pass on a check that never ran.
    flag <- rep("single_bucket", n_buckets)
  } else {
    flag <- ifelse(n < min_n, "not_judged", "ok")
    if (!is.null(flag_threshold)) {
      flag[flag == "ok" & abs(bias) > flag_threshold] <- "high_bias"
    }
  }

  data.frame(
    cut = cut_name, bucket = as.character(bucket_names), n = n,
    judged = flag %in% c("ok", "high_bias"),
    mean_predicted = mean_predicted, mean_actual = mean_actual,
    bias = bias, abs_bias = abs(bias), flag = flag,
    stringsAsFactors = FALSE, row.names = NULL)
}

#' Surface the most-biased buckets from a calibration audit
#'
#' Sorts judged buckets (`flag` `"ok"` or `"high_bias"`) by `abs(bias)`
#' descending. The leak this tool generalises from was invisible in the
#' aggregate and obvious at the boundary -- looking at the average calibration
#' would not have caught it; looking at the worst bucket did.
#'
#' @param audit A `calibration_audit` data frame from [calibration_audit()].
#' @param n Number of rows to return per cut (default 10). Use `Inf` for all.
#' @param cut Optional character vector restricting to specific cut names.
#'
#' @return A data frame, the worst `n` judged buckets per cut, sorted by
#'   `abs_bias` descending.
#' @export
worst_calibration_buckets <- function(audit, n = 10, cut = NULL) {
  if (!inherits(audit, "calibration_audit")) {
    cli::cli_abort("{.arg audit} must be the output of {.fn calibration_audit}.")
  }
  judged <- audit[audit$judged, , drop = FALSE]
  if (!is.null(cut)) {
    judged <- judged[judged$cut %in% cut, , drop = FALSE]
  }
  if (nrow(judged) == 0) {
    return(judged[order(judged$abs_bias, decreasing = TRUE), , drop = FALSE])
  }
  ordered <- judged[order(judged$cut, -judged$abs_bias), , drop = FALSE]
  do.call(rbind, lapply(split(ordered, ordered$cut), utils::head, n))
}

#' @export
print.calibration_audit <- function(x, n = 10, ...) {
  cli::cli_h1("Calibration audit")
  for (cut_name in unique(x$cut)) {
    sub <- x[x$cut == cut_name, , drop = FALSE]

    if (all(sub$flag == "no_data")) {
      cli::cli_alert_danger("{.field {cut_name}}: no rows with a non-NA cut value.")
      next
    }
    if (all(sub$flag == "single_bucket")) {
      cli::cli_alert_warning(
        "{.field {cut_name}}: only one distinct value ({sub$bucket[1]}) -- not a check.")
      next
    }

    n_buckets <- nrow(sub)
    n_judged <- sum(sub$judged)
    n_small <- sum(sub$flag == "not_judged")
    n_high <- sum(sub$flag == "high_bias")
    cli::cli_h2(cut_name)
    msg <- "{n_buckets} bucket{?s}, {n_judged} judged (n >= min_n), {n_small} too small to judge"
    if (n_high > 0) msg <- paste0(msg, ", {n_high} over the bias threshold")
    cli::cli_alert_info(msg)

    worst <- sub[sub$judged, , drop = FALSE]
    worst <- worst[order(-worst$abs_bias), , drop = FALSE]
    show <- utils::head(worst, n)
    if (nrow(show) > 0) {
      print(show[, c("bucket", "n", "mean_predicted", "mean_actual", "bias")],
            row.names = FALSE)
    } else {
      cli::cli_alert_warning("No bucket has n >= min_n -- nothing judged for {cut_name}.")
    }
  }
  invisible(x)
}

#' Report a calibration audit
#'
#' A thin, explicitly-named wrapper around `print.calibration_audit()` for
#' callers who want a human-readable report without relying on autoprint
#' (e.g. inside a pipeline script or an Rmd chunk).
#'
#' @param audit A `calibration_audit` data frame from [calibration_audit()].
#' @param n Number of worst buckets to show per cut (default 10).
#'
#' @return `audit`, invisibly.
#' @export
report_calibration_audit <- function(audit, n = 10) {
  print(audit, n = n)
}

#' Check a low-information state for the leak's signature
#'
#' Every innings opens 0/0: a model with no player identity should predict
#' nearly the same value for every first ball, because it has no information
#' to distinguish them. The leaked agnostic model predicted 0.005 to 5.499
#' runs for that exact state and correlated 1.000 with the runs actually
#' scored on the ball being predicted -- the model was looking at its own
#' answer. This function makes that check reusable for any state the caller
#' can nominate as "the model should not know much here" (first ball of an
#' innings, first over of a powerplay, a brand-new venue, etc).
#'
#' @param predicted Numeric vector of model predictions, over the FULL data
#'   set (not pre-filtered) -- `state` does the filtering.
#' @param actual Numeric vector of realised outcomes, same length.
#' @param state Logical vector, same length, `TRUE` for rows in the
#'   low-information state to check.
#' @param min_n Minimum number of matching rows to judge (default 30).
#' @param correlation_threshold Absolute correlation between `predicted` and
#'   `actual` at or above which the state is flagged `"leak_signature"`
#'   (default 0.2). A model with no information at this state should show
#'   close to zero correlation with that same row's outcome; the leaked
#'   model showed 1.000.
#'
#' @return A one-row data frame (class `low_information_audit`) with `n`,
#'   `judged`, `min_prediction`, `max_prediction`, `mean_prediction`,
#'   `sd_prediction`, `correlation_with_outcome`, and `flag`
#'   (`"ok"`/`"leak_signature"`/`"not_judged"`).
#' @export
audit_low_information_state <- function(predicted, actual, state, min_n = 30,
                                         correlation_threshold = 0.2) {
  if (length(predicted) != length(actual) || length(predicted) != length(state)) {
    cli::cli_abort("{.arg predicted}, {.arg actual} and {.arg state} must be the same length.")
  }
  state <- as.logical(state) & !is.na(predicted) & !is.na(actual)
  n <- sum(state, na.rm = TRUE)
  if (n == 0) {
    cli::cli_abort("No rows matched {.arg state} -- nothing to check.")
  }

  pred_sub <- predicted[state]
  act_sub <- actual[state]
  judged <- n >= min_n

  corr <- NA_real_
  if (judged && stats::sd(pred_sub) > 0 && stats::sd(act_sub) > 0) {
    corr <- stats::cor(pred_sub, act_sub)
  }

  flag <- if (!judged) {
    "not_judged"
  } else if (!is.na(corr) && abs(corr) >= correlation_threshold) {
    "leak_signature"
  } else {
    "ok"
  }

  structure(data.frame(
    n = n,
    judged = judged,
    min_prediction = min(pred_sub),
    max_prediction = max(pred_sub),
    mean_prediction = mean(pred_sub),
    sd_prediction = stats::sd(pred_sub),
    correlation_with_outcome = corr,
    flag = flag,
    stringsAsFactors = FALSE
  ), class = c("low_information_audit", "data.frame"))
}

#' @export
print.low_information_audit <- function(x, ...) {
  cli::cli_h1("Low-information-state check")
  if (x$flag == "not_judged") {
    cli::cli_alert_warning(
      "Only {x$n} matching row{?s} -- below min_n, not judged.")
    return(invisible(x))
  }
  cli::cli_alert_info(paste0(
    "{x$n} rows: predictions range [{signif(x$min_prediction, 4)}, ",
    "{signif(x$max_prediction, 4)}], mean {signif(x$mean_prediction, 4)}, ",
    "sd {signif(x$sd_prediction, 4)}."))
  cli::cli_alert_info(
    "corr(prediction, outcome) = {signif(x$correlation_with_outcome, 4)}")
  if (identical(x$flag, "leak_signature")) {
    cli::cli_alert_danger(paste0(
      "LEAK SIGNATURE: correlation with the row's own outcome exceeds the ",
      "threshold at a state the model should not be able to predict well."))
  } else {
    cli::cli_alert_success("No leak signature: low correlation at a low-information state.")
  }
  invisible(x)
}
