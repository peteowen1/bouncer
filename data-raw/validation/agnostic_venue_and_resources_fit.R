# One variant of #59, run alone so a long fit survives being interrupted.
# Usage: Rscript exp59_fit.R <variant>
suppressMessages({library(data.table); library(xgboost)})
# Scratch location for the cached split and the per-variant scores. Override
# with the first command-line argument; defaults to the session temp dir so the
# script is reproducible on any machine.
OUT <- commandArgs(trailingOnly = TRUE)[2]
if (is.na(OUT) || !nzchar(OUT)) OUT <- file.path(Sys.getenv("TEMP", unset = tempdir()), "agnostic_venue_resources")
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
SEED <- 42; MAX_ROUNDS <- 600; EARLY <- 20
which_v <- commandArgs(trailingOnly = TRUE)[1]

dat <- readRDS(file.path(OUT, "exp59_data.rds")); tr <- dat$tr; te <- dat$te
BASE <- c("over", "ball", "over_ball", "wickets_fallen", "runs_difference",
          "overs_left", "innings", "balls_bowled")

VARIANTS <- list(
  baseline  = BASE,
  venue     = c(BASE, "venue_avg_causal"),
  resources = c(BASE, "resources_left"),
  both      = c(BASE, "venue_avg_causal", "resources_left"))
feats <- VARIANTS[[which_v]]
stopifnot(!is.null(feats), all(feats %in% names(tr)))

dtr <- xgb.DMatrix(as.matrix(tr[, ..feats]), label = tr$outcome)
dte <- xgb.DMatrix(as.matrix(te[, ..feats]), label = te$outcome)
p <- list(objective = "multi:softprob", num_class = 7, eval_metric = "mlogloss",
          max_depth = 6, eta = 0.1, subsample = 0.8, colsample_bytree = 0.8, nthread = 0)
set.seed(SEED)
m <- xgb.train(p, dtr, nrounds = MAX_ROUNDS, early_stopping_rounds = EARLY,
               evals = list(test = dte), verbose = 0)
pr <- predict(m, dte)
if (!is.matrix(pr)) pr <- matrix(pr, ncol = 7, byrow = TRUE)
stopifnot(nrow(pr) == nrow(te), ncol(pr) == 7)
pr <- pmax(pr, 1e-15)
row_ll <- -log(pr[cbind(seq_len(nrow(pr)), te$outcome + 1L)])
ll <- mean(row_ll)
# A fitted model cannot be worse than uniform. It scored 2.88 against
# log(7) = 1.946 when predict()'s matrix was reshaped a second time.
if (ll >= log(7)) stop(sprintf("%s scored %.4f, worse than uniform.", which_v, ll))
saveRDS(list(variant = which_v, mlogloss = ll, n_feat = length(feats), row_ll = row_ll),
        file.path(OUT, paste0("exp59_", which_v, ".rds")))
cat(sprintf("%s: %.6f (%d features)
", which_v, ll, length(feats)))
