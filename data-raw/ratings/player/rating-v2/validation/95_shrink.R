# Do the factors earn their keep BEYOND simple shrinkage?
# Dividing by f_B/f_A < 1 shrinks a prediction toward zero, and shrinkage always
# helps a noisy near-zero target. So the fair control is not a shuffled factor
# (which sometimes INFLATES and is obviously worse) but a CONSTANT shrink that
# does the same average amount of shrinking with no per-competition knowledge.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
CUT <- "2024-01-01"; MINB <- 30L
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
pr[, ratio := fmap[cB]/fmap[cA]]
up <- pr[ratio < 1]
cat(sprintf("STEP UP: %s pairs, %s players. mean ratio applied = %.3f\n\n",
    format(nrow(up), big.mark=","), format(uniqueN(up$batter_id), big.mark=","), mean(up$ratio)))

mae <- function(p) mean(abs(up$rB - p))
res <- data.table(
  method = c("naive (no adjustment)", "FACTOR adjustment",
             sprintf("constant shrink %.2f", c(0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.0))),
  MAE = c(mae(up$rA), mae(up$rA*up$ratio),
          sapply(c(0.9,0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.0), function(k) mae(up$rA*k))))
res[, gain := 100*(res$MAE[1]-MAE)/res$MAE[1]]
setorder(res, MAE)
cat(sprintf("%-26s %8s %8s\n","method","MAE","gain"))
for (i in 1:nrow(res)) cat(sprintf("%-26s %8.4f %+7.1f%%\n", res$method[i], res$MAE[i], res$gain[i]))

best_k <- optimize(function(k) mae(up$rA*k), c(0,1))$minimum
cat(sprintf("\nbest constant shrink = %.3f (MAE %.4f)\n", best_k, mae(up$rA*best_k)))
cat(sprintf("factor adjustment       MAE %.4f\n", mae(up$rA*up$ratio)))

d <- abs(up$rB - up$rA*best_k) - abs(up$rB - up$rA*up$ratio)
set.seed(42); bs <- replicate(4000, mean(sample(d, length(d), replace=TRUE)))
ci <- quantile(bs, c(.025,.975))
cat(sprintf("\nFACTORS vs BEST CONSTANT SHRINK: %+.4f runs/ball, CI [%+.4f, %+.4f]\n", mean(d), ci[1], ci[2]))
cat(sprintf("  -> %s\n", ifelse(ci[1] > 0, "factors add REAL per-competition information beyond shrinkage",
  ifelse(ci[2] < 0, "factors are WORSE than a constant shrink -- the gain was shrinkage",
         "NOT DISTINGUISHABLE from a constant shrink -- the gain is shrinkage, not league knowledge"))))
