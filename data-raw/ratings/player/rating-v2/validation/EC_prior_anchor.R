# Should the shrinkage prior be the mean over ALL competitions, or over the
# REFERENCE competitions only?
#
# calculate_player_rating_v2() uses `pop <- pm[, mean(v)]` -- every player-match
# in the pool, which after the D-P42 adjustment is dominated by weak-league
# cricket. The rating is denominated in reference-equivalent runs, so an
# argument says the prior should be what an average player does in the
# REFERENCE: a higher bar, and therefore a heavier drag on a thin or
# weak-league-only record. That is the lever Pete reached for over Karanbir.
#
# The counter-argument is that a prior should be the mean of the pool being
# rated, and the pool IS every rated player, not just the reference ones. So
# this is a real question, not a formality, and it can go either way.
#
# NOTE ON WHY THIS IS A SEPARATE QUESTION FROM THE HARNESS BUG: the A/B harness
# needed a reference-anchored prior because a prior that MOVES BETWEEN ARMS is a
# second difference and confounds the comparison. Production has one arm, so
# there is no comparability problem there -- only the question of which prior is
# right. Do not conflate the two.
#
# Both arms use the SHIPPED adjustment and the SAME target. Only `pop` differs.
# Aggregation replicates production: per-match SUMS, decayed by match, shrunk by
# prior_matches -- not the ball-weighted form used in the earlier harnesses.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- dbConnect(duckdb::duckdb(), dbdir = file.path(find_bouncerdata_dir(), "bouncer.duckdb"),
                  read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)
DECAY <- 1095; MIN_PRIOR <- 10L

id_map <- build_player_id_map(conn)
b <- as.data.table(dbGetQuery(conn, sprintf("
  SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, r.raa,
         COALESCE(%s,'unknown') AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id = r.match_id
  WHERE r.format='T20' AND r.gender='male'", .competition_sql("t20"))))
canonicalise_player_ids(b, id_map)
fac <- fit_competition_factors(conn, "t20", "male", id_map = id_map)
b[, cfactor := setNames(fac$factor, fac$comp)[comp]]
b[is.na(cfactor) | !is.finite(cfactor) | cfactor <= 0, cfactor := 1]
eff <- fit_two_way_effects(b, prior_balls = 60, iterations = 20)
b[eff$bowler, on = "bowler_id", bowl_eff := i.eff][is.na(bowl_eff), bowl_eff := 0]
b[, v0 := raa - bowl_eff]
REF <- COMPETITION_REFERENCE_T20
b[, is_ref := comp %in% REF]
off <- fit_competition_offsets(b, "batter_id", "v0", REF)
b[, mh := setNames(off$m_here, off$comp)[comp]][is.na(mh), mh := 0]
b[, mr := setNames(off$m_ref,  off$comp)[comp]][is.na(mr), mr := 0]
b[, value := .competition_adjust(v0, mh, mr, cfactor)]

pm <- b[, .(v = sum(value), tgt = sum(v0), balls = .N, is_ref = all(is_ref)),
        by = .(player_id = batter_id, match_id, match_date)]
setorder(pm, player_id, match_date, match_id)
est <- derive_shrinkage_prior(pm)
K <- est$k
pop_all <- pm[, mean(v)]
pop_ref <- pm[is_ref == TRUE, mean(v)]
cat(sprintf("prior k = %.1f matches\n", K))
cat(sprintf("pop over ALL competitions : %+.4f per match\n", pop_all))
cat(sprintf("pop over REFERENCE only   : %+.4f per match\n", pop_ref))
cat(sprintf("difference                : %+.4f  (%s bar for a thin record)\n\n",
            pop_ref - pop_all, if (pop_ref > pop_all) "HIGHER" else "LOWER"))

pm[, `:=`(prior_m = seq_len(.N) - 1L), by = player_id]
score <- function(pop, label) {
  r <- pm[, {
    n <- .N; pred <- rep(NA_real_, n)
    if (n >= 2L) for (i in 2:n) {
      if (i - 1L < MIN_PRIOR) next
      w <- exp(-as.numeric(match_date[i] - match_date[1:(i-1)]) / DECAY)
      pred[i] <- (sum(w * v[1:(i-1)]) + K * pop) / (sum(w) + K)
    }
    .(pred, tgt, is_ref, prior_m)
  }, by = player_id]
  r <- r[!is.na(pred) & is_ref == TRUE]
  r[, stratum := cut(prior_m, c(-Inf, 20, 40, 80, Inf),
                     labels = c("10-20 prior", "21-40", "41-80", "80+"))]
  list(all = data.table(arm = label, n = nrow(r),
                        rho = stats::cor(r$pred, r$tgt, method = "spearman")),
       by = r[, .(n = .N, rho = stats::cor(pred, tgt, method = "spearman")),
              by = stratum][order(stratum)][, arm := label])
}
a <- score(pop_all, "all competitions (current)")
bb <- score(pop_ref, "reference only")
cat("=== T20 men batting: next-match Spearman on reference matches ===\n")
cat("Fixed target; the ONLY difference between arms is the shrinkage prior.\n\n")
res <- rbind(a$all, bb$all)
res[, gain := 100 * (rho - rho[1]) / rho[1]]
for (i in 1:nrow(res)) with(res[i], cat(sprintf("  %-28s n %6d  rho %.4f  %+.2f%%\n",
    arm, n, rho, gain)))
cat("\nby how many prior matches the player had (the prior only bites when thin):\n")
byr <- dcast(rbind(a$by, bb$by), stratum + n ~ arm, value.var = "rho")
setnames(byr, c("stratum", "n", "cur", "ref"))
for (i in 1:nrow(byr)) with(byr[i], cat(sprintf("  %-12s n %6d  current %.4f  reference %.4f  %+.2f%%\n",
    as.character(stratum), n, cur, ref, 100 * (ref - cur) / cur)))

# Invariant: a player with a very long record must be almost unmoved by the
# prior. If he is not, the change is doing something other than what it says.
final <- function(pop) {
  ref_date <- max(pm$match_date)
  pm[, .(m = .N, r = (sum(exp(-as.numeric(ref_date - match_date) / DECAY) * v) + K * pop) /
                     (sum(exp(-as.numeric(ref_date - match_date) / DECAY)) + K)),
     by = player_id]
}
fa <- final(pop_all); fb <- final(pop_ref)
# merge() suffixes BOTH shared columns, so `m` becomes m_cur/m_ref; keep one.
cmp <- merge(fa, fb[, .(player_id, r_ref = r)], by = "player_id")
setnames(cmp, "r", "r_cur")
cmp <- cmp[m >= 10]
cmp[, `:=`(k_cur = frank(-r_cur), k_ref = frank(-r_ref))]
cat(sprintf("\ninvariant check -- shift in rating by career length (%d players, 10+ matches):\n",
            nrow(cmp)))
for (g in list(c(10, 20), c(21, 50), c(51, 150), c(151, 1e6))) {
  x <- cmp[m >= g[1] & m <= g[2]]
  if (!nrow(x)) next
  cat(sprintf("  %4d-%-6s matches  n %5d  mean shift %+.4f  mean rank move %+.1f\n",
      g[1], if (g[2] > 1e5) "+" else as.character(g[2]), nrow(x),
      x[, mean(r_ref - r_cur)], x[, mean(k_cur - k_ref)]))
}
nm <- as.data.table(dbGetQuery(conn, "SELECT player_id, player_name FROM cricsheet.players"))
canonicalise_player_ids(nm, id_map)
cmp <- merge(cmp, unique(nm, by = "player_id"), by = "player_id", all.x = TRUE)
k <- cmp[grepl("Karanbir", player_name)]
cat("\nKaranbir Singh:\n")
if (nrow(k)) for (i in 1:nrow(k)) with(k[i], cat(sprintf(
  "  %d matches, rank %.0f under the current prior, %.0f under the reference prior\n",
  m, k_cur, k_ref))) else cat("  not in the 10+ match pool\n")
