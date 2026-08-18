# Additive vs multiplicative league effect.
#   multiplicative (current): raw = skill * factor   -> pred_B = raw_A * fB/fA
#   additive (candidate):     raw = skill + offset   -> pred_B = raw_A - oA + oB
# Offsets are the mean RAA per competition, computed ONLY on pre-cutoff data, so
# they are as out-of-sample as the factors are. Shrinkage is applied on top of
# the skill estimate, since the shrink test showed it dominates everything.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
CUT <- "2024-01-01"; MINB <- 30L
fac <- fit_competition_factors(conn, "t20", "male", as_at = CUT)
fmap <- setNames(fac$factor, fac$comp)

q <- function(where) sprintf("
  SELECT r.batter_id, r.raa, %s AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  WHERE r.format='T20' AND r.gender='male' AND %s", bouncer:::.competition_sql("t20"), where)
idmap <- build_player_id_map(conn)
pre  <- as.data.table(DBI::dbGetQuery(conn, q(sprintf("r.match_date <= DATE '%s'", CUT))))
post <- as.data.table(DBI::dbGetQuery(conn, q(sprintf("r.match_date >  DATE '%s'", CUT))))
canonicalise_player_ids(pre, idmap); canonicalise_player_ids(post, idmap)

# league offsets from PRE data only
off <- pre[, .(off = mean(raa), n = .N), by = comp][n >= 500]
omap <- setNames(off$off, off$comp)
cat(sprintf("league offsets from pre-%s data: %d competitions\n", CUT, nrow(off)))
cat(sprintf("  offset range %.3f to %.3f runs/ball (spread %.3f)\n\n",
    min(off$off), max(off$off), max(off$off)-min(off$off)))

post[, `:=`(cf = fmap[comp], of = omap[comp])]
post <- post[!is.na(cf) & !is.na(of)]
pc <- post[, .(balls=.N, raw=mean(raa)), by=.(batter_id, comp)][balls >= MINB]
pc <- pc[batter_id %in% pc[, .N, by=batter_id][N >= 2, batter_id]]
pr <- merge(pc[, .(batter_id, cA=comp, rA=raw)], pc[, .(batter_id, cB=comp, rB=raw)],
            by="batter_id", allow.cartesian=TRUE)[cA != cB]
pr[, `:=`(ratio = fmap[cB]/fmap[cA], oA = omap[cA], oB = omap[cB])]
up <- pr[ratio < 1]
cat(sprintf("STEP UP pairs: %s over %s players\n\n", format(nrow(up), big.mark=","),
    format(uniqueN(up$batter_id), big.mark=",")))

mae <- function(p) mean(abs(up$rB - p))
sh <- function(x, k) x*k    # shrink the skill estimate toward zero
cands <- list(
  "naive"                        = up$rA,
  "multiplicative (current)"     = up$rA * up$ratio,
  "additive offsets"             = up$rA - up$oA + up$oB,
  "shrink only (k=0.20)"         = sh(up$rA, 0.20),
  "additive + shrink k=0.20"     = sh(up$rA - up$oA, 0.20) + up$oB,
  "additive + shrink k=0.40"     = sh(up$rA - up$oA, 0.40) + up$oB,
  "multiplicative + shrink 0.20" = sh(up$rA * up$ratio, 0.20)
)
r <- data.table(method=names(cands), MAE=sapply(cands, mae))
r[, gain := 100*(r$MAE[1]-MAE)/r$MAE[1]]
setorder(r, MAE)
cat(sprintf("%-30s %8s %8s\n","method","MAE","gain"))
for (i in 1:nrow(r)) cat(sprintf("%-30s %8.4f %+7.1f%%\n", r$method[i], r$MAE[i], r$gain[i]))

best <- r$method[1]
d <- abs(up$rB - cands[["shrink only (k=0.20)"]]) - abs(up$rB - cands[[best]])
set.seed(42); bs <- replicate(4000, mean(sample(d, length(d), replace=TRUE)))
ci <- quantile(bs, c(.025,.975))
cat(sprintf("\nbest ('%s') vs pure shrink: %+.4f, CI [%+.4f, %+.4f] -> %s\n", best, mean(d), ci[1], ci[2],
  ifelse(ci[1] > 0, "league information ADDS something beyond shrinkage",
  ifelse(ci[2] < 0, "worse than pure shrink", "not distinguishable from pure shrink"))))
