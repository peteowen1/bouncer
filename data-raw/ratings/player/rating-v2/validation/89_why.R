# Why did the real factors come out "not distinguishable"? Two candidates:
#   (a) switchers move between competitions of similar strength, so the
#       correction is tiny and 150-ball noise swamps it -- the test had no power
#       for THESE pairs, and says nothing about pairs that differ a lot;
#   (b) the factors genuinely carry no information (ruled out: placebos hurt).
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
pr <- merge(pc[, .(batter_id, cA=comp, rA=raw)], pc[, .(batter_id, cB=comp, rB=raw)],
            by="batter_id", allow.cartesian=TRUE)[cA != cB]
pr[, ratio := fmap[cB] / fmap[cA]]
pr[, gap := abs(ratio - 1)]

cat("=== how different are the competitions switchers actually move between? ===\n")
cat("    (factor ratio f_B/f_A; 1.00 means identical strength)\n")
print(round(quantile(pr$ratio, c(0,.1,.25,.5,.75,.9,1)), 3))
cat(sprintf("\n  pairs within 10%% of equal strength : %s of %s (%.1f%%)\n",
    format(pr[gap < 0.10, .N], big.mark=","), format(nrow(pr), big.mark=","),
    100*pr[gap<0.10,.N]/nrow(pr)))
cat(sprintf("  pairs differing by >25%%             : %s (%.1f%%)\n",
    format(pr[gap > 0.25, .N], big.mark=","), 100*pr[gap>0.25,.N]/nrow(pr)))

cat("\n=== size the effect: correction the factors ask for, vs the noise ===\n")
pr[, correction := abs(rA*ratio - rA)]
pr[, err_naive := abs(rB - rA)]
cat(sprintf("  median correction requested : %.4f runs/ball\n", median(pr$correction)))
cat(sprintf("  median naive prediction error: %.4f runs/ball\n", median(pr$err_naive)))
cat(sprintf("  correction / error ratio     : %.2f\n",
    median(pr$correction)/median(pr$err_naive)))

cat("\n=== re-test on ONLY the pairs where factors disagree by >25% ===\n")
sub <- pr[gap > 0.25]
if (nrow(sub) >= 40) {
  sub[, pred := rA * ratio]
  mn <- sub[, mean(abs(rB - rA))]; ma <- sub[, mean(abs(rB - pred))]
  d <- sub[, abs(rB - rA) - abs(rB - pred)]
  set.seed(42); bs <- replicate(2000, mean(sample(d, length(d), replace=TRUE)))
  ci <- quantile(bs, c(.025,.975))
  cat(sprintf("  n=%s  naive MAE %.4f  adjusted MAE %.4f  gain %+.1f%%  CI [%+.4f, %+.4f] %s\n",
      format(nrow(sub), big.mark=","), mn, ma, 100*(mn-ma)/mn, ci[1], ci[2],
      ifelse(ci[1] > 0, "<- helps", ifelse(ci[2] < 0, "<- hurts", "<- not distinguishable"))))
} else cat(sprintf("  only %d such pairs -- too few to test\n", nrow(sub)))
