# Bigger sample via ROLLING ORIGINS, then calibration stratified by BALL COUNT.
# Each origin fits factors on data <= origin and tests only the window after it,
# so every row stays out of sample. Pooling origins multiplies the pairs.
#
# Shrinkage is held constant on BOTH sides: for every stratum each method gets
# its own optimal k, so the comparison is like-for-like and isolates whether
# league identity adds anything ON TOP of shrinkage. (k tuned on the test rows
# inflates both equally; it is a comparison, not an absolute claim.)
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
idmap <- build_player_id_map(conn)
CS <- bouncer:::.competition_sql("t20")
ORIGINS <- c("2018-01-01","2020-01-01","2022-01-01","2024-01-01")
WIN <- 730L  # two-year test window after each origin

all <- list()
for (o in ORIGINS) {
  fac <- fit_competition_factors(conn, "t20", "male", as_at = o)
  fmap <- setNames(fac$factor, fac$comp)
  post <- as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT r.batter_id, r.raa, %s AS comp FROM main.cricsheet_ball_raa r
    JOIN cricsheet.matches m ON m.match_id=r.match_id
    WHERE r.format='T20' AND r.gender='male'
      AND r.match_date > DATE '%s' AND r.match_date <= DATE '%s' + %d", CS, o, o, WIN)))
  if (!nrow(post)) next
  canonicalise_player_ids(post, idmap)
  post[, cf := fmap[comp]]; post <- post[!is.na(cf)]
  pc <- post[, .(balls=.N, raw=mean(raa)), by=.(batter_id, comp)][balls >= 30L]
  pc <- pc[batter_id %in% pc[, .N, by=batter_id][N >= 2, batter_id]]
  if (!nrow(pc)) next
  pr <- merge(pc[, .(batter_id, cA=comp, rA=raw, nA=balls)],
              pc[, .(batter_id, cB=comp, rB=raw, nB=balls)], by="batter_id",
              allow.cartesian=TRUE)[cA != cB]
  pr[, ratio := fmap[cB]/fmap[cA]]
  pr <- pr[ratio < 1]
  pr[, origin := o]
  all[[o]] <- pr
  cat(sprintf("origin %s -> %s step-UP pairs\n", o, format(nrow(pr), big.mark=",")))
}
up <- rbindlist(all)
cat(sprintf("\nPOOLED: %s pairs over %s players (was 3,427 / 538 at one origin)\n\n",
    format(nrow(up), big.mark=","), format(uniqueN(up$batter_id), big.mark=",")))

up[, nmin := pmin(nA, nB)]
up[, stratum := cut(nmin, breaks=c(30,60,100,200,1e9),
      labels=c("30-59","60-99","100-199","200+"), right=FALSE)]

bestk <- function(x, y) optimize(function(k) mean(abs(y - x*k)), c(0,2))$minimum
cat(sprintf("%-9s %7s %8s %9s %9s %9s %9s\n",
  "balls","n","slope","MAE naive","shrink","shrink+adj","adj gain"))
for (s in levels(up$stratum)) {
  d <- up[stratum == s]
  if (nrow(d) < 60) { cat(sprintf("%-9s %7s  (too few)\n", s, format(nrow(d), big.mark=","))); next }
  m <- lm(rB ~ rA, data=d)                       # calibration of the raw signal
  k1 <- bestk(d$rA, d$rB);            mae1 <- mean(abs(d$rB - d$rA*k1))
  k2 <- bestk(d$rA*d$ratio, d$rB);    mae2 <- mean(abs(d$rB - d$rA*d$ratio*k2))
  dd <- abs(d$rB - d$rA*k1) - abs(d$rB - d$rA*d$ratio*k2)
  set.seed(42); bs <- replicate(2000, mean(sample(dd, length(dd), replace=TRUE)))
  ci <- quantile(bs, c(.025,.975))
  cat(sprintf("%-9s %7s %8.3f %9.4f %9.4f %10.4f %+8.1f%% %s\n", s,
      format(nrow(d), big.mark=","), coef(m)["rA"], mean(abs(d$rB - d$rA)), mae1, mae2,
      100*(mae1-mae2)/mae1, ifelse(ci[1]>0,"*",ifelse(ci[2]<0,"(worse)",""))))
}
cat("\n slope = regression of actual on the RAW weak-league mean (1.00 = fully informative)\n")
cat(" shrink / shrink+adj each use their own optimal k, so the comparison is like-for-like\n")
cat(" * = adjustment beats shrink-alone with a bootstrap CI excluding zero\n")
