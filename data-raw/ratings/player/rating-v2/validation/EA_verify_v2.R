# Score the form that actually shipped.
#
# E8 measured PLAIN ADDITIVE (recentre only). Production ships RECENTRE THEN
# COMPRESS, which was chosen on an anchor failure after that measurement. A form
# chosen after the scoring run has not been scored, so this closes the loop --
# the alternative is shipping on the strength of a number belonging to a
# different estimator.
#
# Also re-checks the defect that started all of this: are below-average players
# still being IMPROVED by a weak-league adjustment?
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- dbConnect(duckdb::duckdb(), dbdir = "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb",
                  read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)
DECAY <- 1095; KBALLS <- 850; MIN_PRIOR <- 10L

id_map <- build_player_id_map(conn)
b <- as.data.table(dbGetQuery(conn, sprintf("
  SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, r.raa,
         COALESCE(%s,'unknown') AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id = r.match_id
  WHERE r.format='T20' AND r.gender='male'", .competition_sql("t20"))))
canonicalise_player_ids(b, id_map)
fac <- fit_competition_factors(conn, "t20", "male", id_map = id_map)
b[, cfactor := setNames(fac$factor, fac$comp)[comp]][is.na(cfactor), cfactor := 1]
eff <- fit_two_way_effects(b, prior_balls = 60, iterations = 20)
b[eff$bowler, on = "bowler_id", bowl_eff := i.eff][is.na(bowl_eff), bowl_eff := 0]
b[eff$batter, on = "batter_id", bat_eff  := i.eff][is.na(bat_eff),  bat_eff  := 0]
REF <- COMPETITION_REFERENCE_T20
b[, is_ref := comp %in% REF]

for (role in c("batter", "bowler")) {
  idc <- if (role == "batter") "batter_id" else "bowler_id"
  sgn <- if (role == "batter") 1 else -1
  ec  <- if (role == "batter") "bowl_eff" else "bat_eff"
  b[, v0 := raa - get(ec)]
  off <- fit_competition_offsets(b, idc, "v0", REF)
  b[, mh := setNames(off$m_here, off$comp)[comp]][is.na(mh), mh := 0]
  b[, mr := setNames(off$m_ref,  off$comp)[comp]][is.na(mr), mr := 0]
  b[, `:=`(a_none    = sgn * v0,
           a_factor  = sgn * v0 / cfactor,
           a_offset  = sgn * (mr + (v0 - mh)),
           a_shipped = sgn * (mr + (v0 - mh) / cfactor),
           tgt = sgn * v0)]
  base <- b[, .(balls = .N, target = mean(tgt), is_ref = all(is_ref),
                none = mean(a_none), factor = mean(a_factor),
                offset = mean(a_offset), shipped = mean(a_shipped)),
            by = c(player_id = idc, "match_id", "match_date")]
  setnames(base, 1L, "player_id")
  setorder(base, player_id, match_date, match_id)
  base[, wk := balls * (!is_ref)]
  base[, `:=`(prior_bal = cumsum(balls) - balls, prior_wk = cumsum(wk) - wk), by = player_id]
  base[, wshare := fifelse(prior_bal > 0, prior_wk / prior_bal, 0)]

  out <- list()
  for (nm in c("none", "factor", "offset", "shipped")) {
    pm <- copy(base); pm[, v := get(nm)]
    pop <- pm[is_ref == TRUE, weighted.mean(v, balls)]   # identical across arms
    r <- pm[, {
      n <- .N; pred <- rep(NA_real_, n)
      if (n >= 2L) for (i in 2:n) {
        if (i - 1L < MIN_PRIOR) next
        w <- exp(-as.numeric(match_date[i] - match_date[1:(i-1)]) / DECAY) * balls[1:(i-1)]
        pred[i] <- (sum(w * v[1:(i-1)]) + KBALLS * pop) / (sum(w) + KBALLS)
      }
      .(pred, target, is_ref, wshare)
    }, by = player_id]
    r <- r[!is.na(pred) & is_ref == TRUE]
    out[[nm]] <- data.table(adj = nm, n = nrow(r),
      all = stats::cor(r$pred, r$target, method = "spearman"),
      heavy = stats::cor(r[wshare > 0.6, pred], r[wshare > 0.6, target], method = "spearman"),
      n_heavy = r[wshare > 0.6, .N])
  }
  o <- rbindlist(out)
  o[, `:=`(g_all = 100*(all - all[adj=="none"])/all[adj=="none"],
           g_heavy = 100*(heavy - heavy[adj=="none"])/heavy[adj=="none"])]
  cat(sprintf("\n=== T20 men, %s: next-match Spearman on REFERENCE matches ===\n", role))
  cat(sprintf("%-9s %7s %8s %8s %8s %8s\n", "adj", "n", "all", "vs none", "60%+ weak", "vs none"))
  for (i in 1:nrow(o)) with(o[i], cat(sprintf("%-9s %7d %8.4f %+7.1f%% %8.4f %+7.1f%%\n",
      adj, n, all, g_all, heavy, g_heavy)))
}

# --- the defect that started this: are bad players still being helped? -------
b[, v0 := raa - bowl_eff]
off <- fit_competition_offsets(b, "batter_id", "v0", REF)
b[, mh := setNames(off$m_here, off$comp)[comp]][is.na(mh), mh := 0]
b[, mr := setNames(off$m_ref,  off$comp)[comp]][is.na(mr), mr := 0]
h <- b[, .(balls = .N, raw = mean(v0),
           old = mean(v0 / cfactor),
           new = mean(mr + (v0 - mh) / cfactor)), by = batter_id][balls >= 200]
cat(sprintf("\n=== the original defect, %d T20 male batters with 200+ balls ===\n", nrow(h)))
cat(sprintf("  below average before any adjustment: %d\n", h[raw < 0, .N]))
cat(sprintf("  OLD divisive form -- of those, made BETTER: %d (%.0f%%), mean %+.4f/ball\n",
    h[raw < 0 & old > raw, .N], 100*h[raw < 0 & old > raw, .N]/h[raw < 0, .N],
    h[raw < 0 & old > raw, mean(old - raw)]))
cat(sprintf("  NEW shipped form -- of those, made BETTER: %d (%.0f%%), mean %+.4f/ball\n",
    h[raw < 0 & new > raw, .N], 100*h[raw < 0 & new > raw, .N]/h[raw < 0, .N],
    if (h[raw < 0 & new > raw, .N]) h[raw < 0 & new > raw, mean(new - raw)] else 0))

# Decompose the residual. The ORIGINAL defect was specifically that an EASIER
# competition's discount improved a below-average player. Compression acting on
# a player who is below his own league's average is a different thing and is
# intended: if weak leagues stretch gaps by 1.35x, they stretch them downward
# too, so being 0.3 below a weak league's average really is less than 0.3 below
# the reference average. Split the two so the claim is measured, not assumed.
h2 <- b[, .(balls = .N, raw = mean(v0), mh = mean(mh), f = mean(cfactor),
            old = mean(v0 / cfactor), new = mean(mr + (v0 - mh) / cfactor)),
        by = batter_id][balls >= 200][raw < 0]
h2[, easier := mh > 0]
h2[, below_own := raw < mh]
cat("\n=== decomposing the below-average batters still improved ===\n")
cat(sprintf("%-34s %6s %8s %10s\n", "group", "n", "improved", "mean help"))
for (e in c(TRUE, FALSE)) for (bo in c(TRUE, FALSE)) {
  g <- h2[easier == e & below_own == bo]
  if (!nrow(g)) next
  cat(sprintf("%-34s %6d %7d%% %+10.4f\n",
      sprintf("%s league, %s own average",
              if (e) "easier" else "harder", if (bo) "below" else "above"),
      nrow(g), round(100 * g[new > raw, .N] / nrow(g)),
      if (g[new > raw, .N]) g[new > raw, mean(new - raw)] else 0))
}
cat("\nOLD form, same split (this is what the fix had to remove):\n")
for (e in c(TRUE, FALSE)) for (bo in c(TRUE, FALSE)) {
  g <- h2[easier == e & below_own == bo]
  if (!nrow(g)) next
  cat(sprintf("%-34s %6d %7d%% %+10.4f\n",
      sprintf("%s league, %s own average",
              if (e) "easier" else "harder", if (bo) "below" else "above"),
      nrow(g), round(100 * g[old > raw, .N] / nrow(g)),
      if (g[old > raw, .N]) g[old > raw, mean(old - raw)] else 0))
}
