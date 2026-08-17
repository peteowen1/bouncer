# Fit the win/draw/loss surface and derive lambda = |dU/dwicket| / (dU/drun).
#
# Multinomial logit on the aggregated cells (counts response, so the weights are
# exact). Smooth in lead / elapsed / wickets, interacted with innings, because
# the marginal value of a run is not remotely constant across a Test.
#
# Marginal effects are WITHIN-STATE finite differences, not "mean outcome at w
# vs w+1 wickets" -- that naive form is selection-biased, since sides several
# down at a given point are systematically worse-placed sides.
suppressMessages({library(arrow); library(nnet); library(splines)})
OUT <- "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda"
g <- as.data.frame(read_parquet(file.path(OUT, "surface.parquet")))

cat(sprintf("cells %d, balls %s\n", nrow(g), format(sum(g$n), big.mark = ",")))

# Trim the sparsest cells: keep >= 30 balls, which is 97%+ of the corpus and
# stops single-ball cells dominating the spline tails.
g <- g[g$n >= 30, ]
cat(sprintf("after n>=30 trim: cells %d, balls %s (%.1f%% kept)\n",
            nrow(g), format(sum(g$n), big.mark = ","),
            100 * sum(g$n) / 5387988))

g$inn <- factor(g$innings)
Y <- as.matrix(g[, c("n_w", "n_d", "n_l")])

cat("\nfitting multinomial logit...\n")
fit <- multinom(
  Y ~ inn * (ns(lead_bin, 5) + ns(elapsed_bin, 4) + ns(wkts_pre, 4)),
  data = g, weights = rep(1, nrow(g)), maxit = 400, trace = FALSE)
cat("  converged:", fit$convergence == 0, "  edf:", fit$edf, "\n")

# ---- calibration check before trusting any marginal effect -----------------
pp <- predict(fit, newdata = g, type = "probs")
colnames(pp) <- c("W", "D", "L")
obs <- Y / rowSums(Y)
wt  <- g$n / sum(g$n)
cat("\n=== calibration (weighted mean |predicted - observed|) ===\n")
for (k in 1:3) cat(sprintf("  %s: %.4f   (mean obs %.3f, mean pred %.3f)\n",
    colnames(pp)[k], sum(wt * abs(pp[, k] - obs[, k])),
    sum(wt * obs[, k]), sum(wt * pp[, k])))

# ---- utilities -------------------------------------------------------------
U <- list(
  `U1 P(win)-P(loss)`   = function(p) p[, "W"] - p[, "L"],
  `U2 P(win)+0.5P(draw)`= function(p) p[, "W"] + 0.5 * p[, "D"],
  `U3 P(win)`           = function(p) p[, "W"]
)

pred <- function(df) {
  m <- predict(fit, newdata = df, type = "probs"); colnames(m) <- c("W","D","L"); m
}

# Within-state finite differences: +1 run, and +1 wicket, from each cell.
base   <- g
plus_r <- transform(g, lead_bin = lead_bin + 1)
plus_w <- transform(g, wkts_pre = wkts_pre + 1)
ok_w   <- g$wkts_pre <= 8            # cannot go to 10 wickets pre-delivery

p0 <- pred(base); pr <- pred(plus_r); pw <- pred(plus_w)

cat("\n=== LAMBDA by utility (weighted over the observed state distribution) ===\n")
res <- data.frame()
for (nm in names(U)) {
  f <- U[[nm]]
  d_run <- f(pr) - f(p0)
  d_wkt <- f(pw) - f(p0)
  w_all <- g$n / sum(g$n)
  w_ok  <- g$n[ok_w] / sum(g$n[ok_w])
  mr <- sum(w_all * d_run)
  mw <- sum(w_ok  * d_wkt[ok_w])
  lam <- -mw / mr
  cat(sprintf("  %-22s dU/run %+.6f   dU/wkt %+.5f   lambda %6.1f\n", nm, mr, mw, lam))
  res <- rbind(res, data.frame(utility = nm, d_run = mr, d_wkt = mw, lambda = lam))
}

cat("\n=== LAMBDA per innings (falsifier: innings-types disagreeing by >2x) ===\n")
cat(sprintf("  %-5s %8s %10s %10s %10s\n", "inn", "balls", "U1", "U2", "U3"))
for (i in 1:4) {
  sel <- g$innings == i
  row <- sprintf("  %-5d %8s", i, format(sum(g$n[sel]), big.mark = ","))
  for (nm in names(U)) {
    f <- U[[nm]]
    d_run <- f(pr) - f(p0); d_wkt <- f(pw) - f(p0)
    s2 <- sel & ok_w
    mr <- sum(g$n[sel] / sum(g$n[sel]) * d_run[sel])
    mw <- sum(g$n[s2]  / sum(g$n[s2])  * d_wkt[s2])
    row <- paste0(row, sprintf(" %10.1f", -mw / mr))
  }
  cat(row, "\n")
}

saveRDS(list(fit = fit, res = res), file.path(OUT, "lambda_fit.rds"))
cat("\nsaved lambda_fit.rds\n")
