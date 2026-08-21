# The honest full-vs-agnostic comparison: BOTH models scored on ONE common
# held-out set (bouncerverse#65).
#
# WHY. 02_train_full_model.R compares its fresh test logloss against a number
# read out of a stored agnostic_model_results.rds -- a different run, a
# different split, an older corpus (t20 3,035,225 rows against 3,221,299).
# Whatever that difference is, it is not the feature gain. Here both saved
# models predict the SAME rows, so vintage and split cancel and only the
# feature set differs.
#
# NO PRIOR EXPECTATION IS ASSERTED HERE, deliberately. #16 recorded gains of
# T20 0.0% / ODI 0.8% / Test 0.8% and those were measured while the model's
# three ELO features were zero-filled for every row -- #16's own hypothesis
# (2), now confirmed. Using that as a ceiling to judge the first model with
# working ELO inputs would be citing the defect to discredit its fix. The
# s^2/2*sigma^2 bound is also narrower than it is usually quoted: it bounds
# what PLAYER IDENTITY buys per ball, and venue, team and ELO sit outside it.
#
# Usage: Rscript data-raw/validation/full_vs_agnostic_matched.R [t20 odi test]
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table); library(xgboost)})

fmts <- commandArgs(trailingOnly = TRUE)
if (!length(fmts)) fmts <- c("t20", "odi", "test")
SEED <- 42

conn <- get_db_connection(read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

logloss <- function(pr, y) {
  if (!is.matrix(pr)) pr <- matrix(pr, ncol = 7, byrow = TRUE)
  stopifnot(nrow(pr) == length(y))
  pr <- pmax(pr, 1e-15)
  -mean(log(pr[cbind(seq_len(nrow(pr)), y + 1L)]))
}

for (fmt in fmts) {
  cli::cli_h2(toupper(fmt))
  full <- tryCatch(load_full_model(fmt), error = function(e) NULL)
  agn  <- tryCatch(load_agnostic_model(fmt), error = function(e) NULL)
  if (is.null(full) || is.null(agn)) {
    cli::cli_alert_warning("missing a model for {fmt}; skipping"); next
  }

  d <- as.data.table(build_full_model_frame(conn, fmt))
  # Same label the models were trained against, from the shared declaration --
  # not rebuilt here, or this would score against a different target.
  d[, outcome := ball_outcome_class(runs_batter, is_wicket)]
  d <- d[!is.na(outcome)]
  # The frame must not arrive zero-filled -- that is the defect this whole
  # ticket is about, and it has appeared twice in this code path today.
  stopifnot("elo_run_diff" %in% names(d))
  nz <- mean(d$elo_run_diff != 0)
  if (nz < 0.5) cli::cli_abort("ELO features are {round(100*nz,1)}% non-zero -- the frame is zero-filled.")

  # SORT before splitting. DuckDB does not guarantee row order, so
  # unique(d$match_id) returns a different sequence between runs and the 80/20
  # split lands on different matches -- set.seed() on a query result reproduces
  # nothing. Two runs of this script gave T20 +1.625% and +1.804% on held-out
  # sets of 636,371 and 631,464 deliveries before this line existed.
  set.seed(SEED)
  mids <- sort(unique(d$match_id))
  te <- d[match_id %in% mids[(floor(0.8 * length(mids)) + 1L):length(mids)]]
  cli::cli_alert_info("held-out {format(nrow(te), big.mark=',')} deliveries, {format(uniqueN(te$match_id), big.mark=',')} matches; ELO non-zero {round(100*nz,1)}%")

  ff <- as.matrix(prepare_full_features(te, fmt))
  af <- as.matrix(prepare_agnostic_features(te, fmt))
  cli::cli_alert_info("features: full {ncol(ff)}, agnostic {ncol(af)}")

  ll_full <- logloss(predict(full, xgb.DMatrix(ff)), te$outcome)
  ll_agn  <- logloss(predict(agn,  xgb.DMatrix(af)), te$outcome)
  gain <- 100 * (ll_agn - ll_full) / ll_agn

  # Paired per-ball test: on this many rows a tiny mean difference can still be
  # certain, and a large one can still be noise. Report both.
  rf <- -log(pmax(predict(full, xgb.DMatrix(ff)), 1e-15)[cbind(seq_len(nrow(te)), te$outcome + 1L)])
  ra <- -log(pmax(predict(agn,  xgb.DMatrix(af)), 1e-15)[cbind(seq_len(nrow(te)), te$outcome + 1L)])
  dd <- ra - rf
  se <- sd(dd) / sqrt(length(dd))
  cli::cli_alert_info("full {round(ll_full,5)} | agnostic {round(ll_agn,5)} | gain {sprintf('%+.3f%%', gain)}")
  cli::cli_alert_info("per-ball paired mean {signif(mean(dd),3)}, SE {signif(se,3)}, t = {round(mean(dd)/se,1)}")
  # Per-ball intervals OVERSTATE precision here: every ball in a match shares
  # that match's conditions, so the effective sample is matches, not balls.
  # This repo measured the overstatement at ~220x on the WP benchmark and made
  # match-level bootstrapping the standard. The per-ball t above is reported
  # only so the gap between the two is visible.
  set.seed(SEED)
  mm <- unique(te$match_id)
  by_match <- data.table(match_id = te$match_id, d = dd)[, .(m = mean(d), n = .N), by = match_id]
  boot <- vapply(seq_len(2000), function(i) {
    s <- by_match[sample(.N, .N, replace = TRUE)]
    sum(s$m * s$n) / sum(s$n)
  }, numeric(1))
  ci <- quantile(boot, c(0.025, 0.975))
  cli::cli_alert_info("match-level bootstrap: mean {signif(mean(dd),3)}, 95% CI [{signif(ci[1],3)}, {signif(ci[2],3)}], {sum(boot > 0)}/2000 draws favour the full model")
}
