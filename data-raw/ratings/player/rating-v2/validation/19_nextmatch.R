# Score Test on the SAME target the shipped T20/ODI work used: next-match
# Spearman (D-P17/D-P18/D-P19), and sweep the horizon to test D-P20's flip.
#
# Why this exists: the 12-month harness I pre-declared for Test is ~8-15 Tests,
# structurally D-P18's next-10 horizon -- the one D-P20 already measured as
# favouring a career mean over a decayed rating. So the -44.7% was measured on a
# metric the repo knew favours the baseline. This is the like-for-like number.
#
# Strictly forward-looking: at each player-match, both the rating and the
# baseline see only that player's STRICTLY EARLIER matches.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

DECAY <- 1095; PRIOR <- 20   # the settled v2 batting defaults (D-P20)
MIN_PRIOR <- 10L             # D-P18 required 10 prior matches

cat("Building per-match values for Test (opponent- and competition-adjusted)\n")
id_map <- build_player_id_map(conn)
b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, r.raa,
         COALESCE(%s,'unknown') AS comp
  FROM main.cricsheet_ball_raa r
  JOIN cricsheet.matches m ON m.match_id = r.match_id
  WHERE r.format='TEST' AND r.gender='male'", .competition_sql("test"))))
canonicalise_player_ids(b, id_map)

fac <- fit_competition_factors(conn, "test", "male", id_map = id_map)
fmap <- setNames(fac$factor, fac$comp)
b[, cfactor := fmap[comp]][is.na(cfactor), cfactor := 1]
eff <- fit_two_way_effects(b, prior_balls = 60, iterations = 20)
b[eff$bowler, on = "bowler_id", opp_eff := i.eff][is.na(opp_eff), opp_eff := 0]
b[, value := (raa - opp_eff) / cfactor]

# THREE per-match quantities, because the contrast matters:
#   v      opponent- AND competition-adjusted  (what the rating aggregates)
#   v_raw  neither adjustment                  (what "a plain career mean" means
#                                               in D-P18/D-P19, and the only
#                                               baseline that shows what the
#                                               adjustments actually buy)
# Comparing a decayed mean of v against a plain mean of v isolates decay and
# shrinkage alone, which D-P17 already measured at <1% -- so that comparison can
# only ever return "no difference", as it did.
pm <- b[, .(v = sum(value), v_raw = sum(raa), balls = .N),
        by = .(player_id = batter_id, match_id, match_date)]
setorder(pm, player_id, match_date, match_id)
cat(sprintf("  %s player-matches, %d players\n",
            format(nrow(pm), big.mark = ","), uniqueN(pm$player_id)))
pop <- pm[, mean(v)]

# For every player-match, build BOTH predictors from strictly earlier matches.
cat("\nBuilding strictly-prior predictors per player-match...\n")
pm[, idx := seq_len(.N), by = player_id]
res <- pm[, {
  n <- .N
  rating <- rep(NA_real_, n); career <- rep(NA_real_, n); career_raw <- rep(NA_real_, n)
  if (n >= 2L) for (i in 2:n) {
    j <- 1:(i - 1L)
    w <- exp(-as.numeric(match_date[i] - match_date[j]) / DECAY)
    rating[i]     <- (sum(v[j] * w) + PRIOR * pop) / (sum(w) + PRIOR)
    career[i]     <- mean(v[j])       # adjusted, undecayed -- isolates decay only
    career_raw[i] <- mean(v_raw[j])   # THE baseline: no adjustments at all
  }
  .(match_date, v, v_raw, idx, rating, career, career_raw, n_prior = idx - 1L)
}, by = player_id]

cat("\n=== HORIZON SWEEP: Spearman of each predictor against forward mean value ===\n")
cat(sprintf("  (players need %d prior matches, as D-P18 required)\n\n", MIN_PRIOR))
cat(sprintf("  %-9s %8s %10s %10s %10s %11s %11s\n", "horizon", "n",
            "rating", "adj mean", "RAW mean", "vs RAW", "vs adj"))
setorder(res, player_id, match_date)
for (H in c(1L, 3L, 5L, 10L, 20L)) {
  # forward mean of the next H matches, inclusive of the current one
  # Target is forward RAW output -- what the player actually did. Using forward
  # ADJUSTED value instead would be near-circular: the rating aggregates that
  # same adjusted quantity, so an adjusted predictor would win by construction
  # rather than by predicting anything.
  res[, fwd := {
    cs <- cumsum(v_raw); k <- pmin(.N, idx + H - 1L)
    (cs[k] - c(0, cs)[idx]) / (k - idx + 1L)
  }, by = player_id]
  d <- res[n_prior >= MIN_PRIOR & is.finite(rating) & is.finite(fwd) & is.finite(career_raw)]
  rr <- cor(d$rating, d$fwd, method = "spearman")
  ca <- cor(d$career, d$fwd, method = "spearman")
  cw <- cor(d$career_raw, d$fwd, method = "spearman")
  cat(sprintf("  next %-4d %8s %10.4f %10.4f %10.4f %+10.1f%% %+10.1f%%\n", H,
              format(nrow(d), big.mark = ","), rr, ca, cw,
              100 * (rr - cw) / abs(cw), 100 * (rr - ca) / abs(ca)))
}
cat("\n  'vs RAW' is the D-P19 contrast: what the opponent and competition\n")
cat("  adjustments buy over an unadjusted career mean. 'vs adj' isolates decay\n")
cat("  and shrinkage alone, which D-P17 already measured at <1%.\n")

cat("\n=== why the first two columns track each other so closely ===\n")
d <- res[n_prior >= MIN_PRIOR & is.finite(rating) & is.finite(career_raw)]
cat(sprintf("  spearman(rating, adjusted career mean) = %.4f\n",
            cor(d$rating, d$career, method = "spearman")))
cat(sprintf("  spearman(rating, RAW career mean)      = %.4f\n",
            cor(d$rating, d$career_raw, method = "spearman")))
cat("  If the first is ~0.99 the two ORDER players almost identically, so any\n")
cat("  correlation against a third quantity must come out nearly the same --\n")
cat("  that is the agreement, explained rather than assumed.\n")
