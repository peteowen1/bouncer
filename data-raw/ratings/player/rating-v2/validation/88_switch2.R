# Take 2. The first attempt compared within-player SPREAD before and after
# dividing by factors -- not scale-invariant, since dividing by anything > 1
# shrinks a range mechanically. A shuffled-factor placebo beat the real factors,
# which is how that was caught.
#
# Same-scale design instead: predict a player's RAW mean in competition B from
# their RAW mean in competition A.
#   naive     : pred = raw_A                    (competitions are interchangeable)
#   adjusted  : pred = raw_A * (f_B / f_A)      (what the factors claim)
# Both predictions live on B's raw scale, so their errors are directly
# comparable and no rescaling can flatter either one.
#
# PRE-DECLARED: the factors earn their keep only if adjusted MAE is lower than
# naive MAE by a margin whose paired bootstrap CI excludes zero, AND the shuffled
# placebo does NOT achieve the same.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)

CUT <- "2024-01-01"; MINB <- 150L
fac <- fit_competition_factors(conn, "t20", "male", as_at = CUT)
fmap <- setNames(fac$factor, fac$comp)

b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.batter_id, r.raa, %s AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  WHERE r.format='T20' AND r.gender='male' AND r.match_date > DATE '%s'",
  bouncer:::.competition_sql("t20"), CUT)))
idmap <- build_player_id_map(conn); canonicalise_player_ids(b, idmap)
b[, cf := fmap[comp]]; b <- b[!is.na(cf)]
pc <- b[, .(balls=.N, raw=mean(raa)), by=.(batter_id, comp)][balls >= MINB]
pc <- pc[batter_id %in% pc[, .N, by=batter_id][N >= 2, batter_id]]

# every ordered pair of competitions within a player
pr <- merge(pc[, .(batter_id, cA=comp, rA=raw, nA=balls)],
            pc[, .(batter_id, cB=comp, rB=raw, nB=balls)], by="batter_id",
            allow.cartesian=TRUE)[cA != cB]
cat(sprintf("players %s | player-competition cells %s | ordered pairs %s\n\n",
    format(uniqueN(pc$batter_id), big.mark=","), format(nrow(pc), big.mark=","),
    format(nrow(pr), big.mark=",")))

score <- function(fm, label) {
  pr[, pred_adj := rA * (fm[cB] / fm[cA])]
  mae_n <- pr[, mean(abs(rB - rA))]
  mae_a <- pr[, mean(abs(rB - pred_adj))]
  d <- pr[, abs(rB - rA) - abs(rB - pred_adj)]
  set.seed(42)
  bs <- replicate(2000, mean(sample(d, length(d), replace=TRUE)))
  ci <- quantile(bs, c(.025,.975))
  cat(sprintf("%-22s naive MAE %.4f  adjusted MAE %.4f  gain %+.1f%%  CI [%+.4f, %+.4f] %s\n",
      label, mae_n, mae_a, 100*(mae_n-mae_a)/mae_n, ci[1], ci[2],
      ifelse(ci[1] > 0, "<- helps", ifelse(ci[2] < 0, "<- HURTS", "<- not distinguishable"))))
  invisible(mae_a)
}
cat("=== out-of-sample: predict raw mean in B from raw mean in A ===\n")
score(fmap, "real factors")
for (s in 1:5) {
  set.seed(100 + s); sh <- fmap; names(sh) <- sample(names(fmap))
  score(sh, sprintf("placebo shuffle %d", s))
}
cat("\n(placebos must NOT match the real factors, or the gain is an artefact)\n")
