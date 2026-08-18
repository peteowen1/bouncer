# Is the weak-league -> strong-competition drop SELECTION or LEAGUE STRENGTH?
#
# Regress the strong-competition mean on the weak-league mean, with the weak
# mean CENTRED on its own population mean. Then:
#   slope < 1      -> regression to the mean (selection): the further above the
#                     weak-league average a player was, the more he gives back.
#   intercept < 0  -> genuine league strength: a player who was exactly AVERAGE
#                     in the weak league is still below average in the strong
#                     one, which selection cannot explain.
# Both can be true at once, and the intercept is the part that justifies
# discounting the league.
setwd("C:/dev/bouncerverse/bouncer")
suppressMessages(devtools::load_all(".", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- DBI::dbConnect(duckdb::duckdb(),
  dbdir="C:/dev/bouncerverse/bouncerdata/bouncer.duckdb", read_only=TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown=TRUE), add=TRUE)
fac <- fit_competition_factors(conn, "t20", "male")
fmap <- setNames(fac$factor, fac$comp)
b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.batter_id, r.raa_run, %s AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id=r.match_id
  WHERE r.format='T20' AND r.gender='male'", bouncer:::.competition_sql("t20"))))
idmap <- build_player_id_map(conn); canonicalise_player_ids(b, idmap)
b[, cf := fmap[comp]]; b <- b[!is.na(cf)]
pc <- b[, .(balls=.N, r=mean(raa_run)), by=.(batter_id, comp)]
pc[, cf := fmap[comp]]

for (MINB in c(50L, 100L)) {
  weak   <- pc[cf >= 1.30 & balls >= MINB, .(batter_id, wcf=cf, wb=balls, rA=r)]
  strong <- pc[cf <= 1.05 & balls >= MINB, .(batter_id, sb=balls, rB=r)]
  j <- merge(weak, strong, by="batter_id")
  if (nrow(j) < 40) { cat(sprintf("\nminB=%d: only %d moves\n", MINB, nrow(j))); next }
  mu <- mean(j$rA)
  j[, rA_c := rA - mu]
  m <- lm(rB ~ rA_c, data=j)
  ci_s <- confint(m)["rA_c",]; ci_i <- confint(m)["(Intercept)",]
  cat(sprintf("\n=== min %d balls each side: %d moves, %d players ===\n",
      MINB, nrow(j), uniqueN(j$batter_id)))
  cat(sprintf("  weak-league mean RAA %+.4f, strong-competition mean %+.4f (raw drop %+.4f)\n",
      mu, mean(j$rB), mean(j$rB) - mu))
  cat(sprintf("  SLOPE     %+.3f  CI [%+.3f, %+.3f]  -> %s\n", coef(m)["rA_c"], ci_s[1], ci_s[2],
      ifelse(ci_s[2] < 1, "regression to the mean IS present", "no evidence of regression")))
  cat(sprintf("  INTERCEPT %+.4f CI [%+.4f, %+.4f] -> %s\n", coef(m)[1], ci_i[1], ci_i[2],
      ifelse(ci_i[2] < 0, "LEAGUE STRENGTH IS REAL (an average weak-league player is still below par)",
      ifelse(ci_i[1] > 0, "average weak-league player is ABOVE par in the strong comp",
             "not distinguishable from zero -- drop is explained by selection alone"))))
  cat(sprintf("  i.e. of the %+.4f raw drop, ~%.0f%% is the intercept (league) and the rest scales with how far above average he was\n",
      mean(j$rB) - mu, 100*abs(coef(m)[1])/max(abs(mean(j$rB)-mu), 1e-9)))
}
