# Does the serving path give the full model the features it was trained on?
# (bouncerverse#65)
#
# THE TRAP: xgboost's predict() accepts a matrix with TOO FEW columns without
# error. It does not warn, it does not abort -- it returns plausible numbers
# computed from the wrong thing. So a serving path that drifts from its
# training frame fails silently and forever.
#
# This compares the booster's own recorded feature list against what
# prepare_full_features() actually builds, per format. Names and ORDER both:
# an xgb.DMatrix built from an unnamed matrix is positional, so two frames with
# the same columns in a different order predict nonsense equally quietly.
#
# Usage: Rscript data-raw/validation/full_model_serving_alignment.R
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages(library(xgboost))


# One synthetic delivery carrying every column prepare_full_features() reads.
# Values are irrelevant -- only the resulting COLUMN SET and ORDER matter.
.alignment_probe_row <- function(fmt) {
  data.frame(
    over = 5L, ball = 3L, innings = 1L, gender = "male",
    wickets_fallen = 2L, runs_difference = 45,
    is_knockout = 0L, event_tier = 2,
    batter_balls_faced = 500, bowler_balls_bowled = 500,
    batter_scoring_index = 1.2, batter_survival_rate = 0.95,
    bowler_economy_index = 1.0, bowler_strike_rate = 0.05,
    batting_team_runs_skill = 0, batting_team_wicket_skill = 0,
    bowling_team_runs_skill = 0, bowling_team_wicket_skill = 0,
    venue_run_rate = 1.3, venue_wicket_rate = 0.05,
    venue_boundary_rate = 0.15, venue_dot_rate = 0.35,
    stringsAsFactors = FALSE
  )
}

fail <- character(0); unnamed <- character(0)
for (fmt in c("t20", "odi", "test")) {
  cli::cli_h2(toupper(fmt))
  m <- tryCatch(load_full_model(fmt), error = function(e) NULL)
  if (is.null(m)) {
    cli::cli_alert_warning("no full model for {fmt}; skipping")
    next
  }
  trained <- m$feature_names
  # The boosters carry NO feature names, so a name-level check is impossible
  # and width is the only handle. xgb.attr("num_feature") is empty; the value
  # lives in the booster config.
  nfeat <- tryCatch({
    cfg <- xgb.config(m)
    if (is.character(cfg)) cfg <- jsonlite::fromJSON(cfg)
    as.integer(cfg$learner$learner_model_param$num_feature)
  }, error = function(e) NA_integer_)
  named <- !is.null(trained) && length(trained) > 0
  if (!named) {
    # A booster with no recorded names cannot be checked by name, and that is
    # itself worth reporting: the only thing between serving and silence is
    # then column COUNT, and predict() accepts a short matrix without error.
    cli::cli_alert_warning("{fmt}: booster records NO feature names -- width is the only check available")
    trained <- character(0)
  }
  width <- if (is.na(nfeat)) "" else paste0(", num_feature = ", nfeat)
  cli::cli_alert_info("model expects {length(trained)} named feature{?s}{width}")

  served <- tryCatch(names(prepare_full_features(.alignment_probe_row(fmt), fmt)),
                     error = function(e) {
                       cli::cli_alert_danger("prepare_full_features() failed: {conditionMessage(e)}")
                       NULL
                     })
  if (is.null(served)) { fail <- c(fail, sprintf("%s: serving path errored", fmt)); next }
  cli::cli_alert_info("serving path builds {length(served)} feature{?s}")

  # Width first, because it is the only check that works on these boosters.
  if (is.na(nfeat)) {
    cli::cli_alert_danger("{fmt}: could not read num_feature -- alignment is UNVERIFIABLE")
    fail <- c(fail, sprintf("%s: width unreadable", fmt))
  } else if (nfeat != length(served)) {
    cli::cli_alert_danger("WIDTH MISMATCH: model {nfeat}, serving {length(served)}")
    fail <- c(fail, sprintf("%s: model %d features, serving builds %d", fmt, nfeat, length(served)))
  } else {
    cli::cli_alert_success("width matches: {nfeat}")
  }
  if (!named) {
    # Do NOT fall through to the name comparison -- setdiff against an empty
    # vector finds nothing missing and would report success having checked
    # nothing. A vacuous pass is worse than no check.
    cli::cli_alert_warning("{fmt}: names unverified (booster carries none)")
    unnamed <- c(unnamed, fmt)
    next
  }

  missing <- setdiff(trained, served)
  extra   <- setdiff(served, trained)
  if (length(missing)) {
    cli::cli_alert_danger("MISSING at serving time: {.val {missing}}")
    fail <- c(fail, sprintf("%s: %d trained feature(s) not served", fmt, length(missing)))
  }
  if (length(extra)) {
    cli::cli_alert_warning("served but not trained on: {.val {extra}}")
  }
  if (!length(missing) && !length(extra) && !identical(trained, served)) {
    cli::cli_alert_danger("same features, DIFFERENT ORDER -- a positional DMatrix will silently misalign")
    fail <- c(fail, sprintf("%s: feature order differs", fmt))
  }
  if (!length(missing) && !length(extra) && identical(trained, served)) {
    cli::cli_alert_success("{fmt}: names and order match exactly")
  }
}

if (length(fail)) {
  cli::cli_abort(c("Serving alignment FAILED.",
                   stats::setNames(fail, rep("x", length(fail)))))
}
if (length(unnamed)) {
  cli::cli_alert_warning(c("Width matches for {.val {unnamed}}, but their boosters carry no feature names."))
  cli::cli_alert_info("Same width with the columns in a DIFFERENT ORDER predicts nonsense silently, and this check cannot see it. Stamping feature names at save time would close that.")
} else {
  cli::cli_alert_success("Every format's serving path matches its trained feature frame.")
}
