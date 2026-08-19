# A/B the competition adjustment on the pipeline's own metric: next-match
# Spearman, strictly forward-looking (D-P17/D-P18/D-P19).
#
#   none    value = raa - opp_eff
#   factor  value = (raa - opp_eff) / f_L      <- current production
#   offset  value = (raa - opp_eff) - c_L      <- V1, the additive form
#
# TARGET IS FIXED ACROSS ARMS. The first cut of this script scored each arm
# against its OWN adjusted value, so every arm was compared to itself and the
# numbers meant nothing -- the same defect as the benchmark check that compared
# each run to itself. Here the target is always the observable per-ball value
# (opponent-adjusted, competition-unadjusted) and only the PREDICTOR varies.
#
# Everything is per BALL, not per match, so a 60-ball innings and a 3-ball
# cameo do not enter the rating with equal say.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
DECAY <- 1095; KBALLS <- 850; MIN_PRIOR <- 10L

id_map <- build_player_id_map(conn)
b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, r.raa,
         COALESCE(%s,'unknown') AS comp
  FROM main.cricsheet_ball_raa r JOIN cricsheet.matches m ON m.match_id = r.match_id
  WHERE r.format='T20' AND r.gender='male'", .competition_sql("t20"))))
canonicalise_player_ids(b, id_map)
fac <- fit_competition_factors(conn, "t20", "male", id_map = id_map)
fmap <- setNames(fac$factor, fac$comp)
b[, cfactor := fmap[comp]][is.na(cfactor), cfactor := 1]
eff <- fit_two_way_effects(b, prior_balls = 60, iterations = 20)
b[eff$bowler, on = "bowler_id", bowl_eff := i.eff][is.na(bowl_eff), bowl_eff := 0]
b[eff$batter, on = "batter_id", bat_eff  := i.eff][is.na(bat_eff),  bat_eff  := 0]
REF <- COMPETITION_REFERENCE_T20
b[, is_ref := comp %in% REF]

# Two scopes, because they answer different questions and only one is clean.
#
#   ref_only = FALSE  score every player-match, mapping the rating back onto
#     that match's competition. CONFOUNDED: pooled across competitions, the
#     target's variance is mostly BETWEEN leagues, so any arm that adds a
#     competition term is graded largely on how well it predicts which league
#     a match was played in. That is not what the rating is for.
#
#   ref_only = TRUE   score only matches in the REFERENCE competitions, where
#     every arm's adjustment is the identity (f = 1, c = 0) so no map-back is
#     needed and none is applied. The arms differ ONLY in how they treated a
#     player's earlier weak-league matches when building his rating. This is
#     the decision-relevant question -- does weak-league evidence, adjusted
#     this way, predict what a player does in the leagues that matter -- and it
#     is the same operation the E5/E6 bridge test scored.
run <- function(pm, back, ref_only) {
  setorder(pm, player_id, match_date, match_id)
  pop <- pm[, weighted.mean(v, balls)]
  r <- pm[, {
    n <- .N; pred <- rep(NA_real_, n)
    if (n >= 2L) for (i in 2:n) {
      if (i - 1L < MIN_PRIOR) next
      w <- exp(-as.numeric(match_date[i] - match_date[1:(i-1)]) / DECAY) * balls[1:(i-1)]
      pred[i] <- (sum(w * v[1:(i-1)]) + KBALLS * pop) / (sum(w) + KBALLS)
    }
    .(pred, target, f, cf, balls, is_ref)
  }, by = player_id]
  r <- r[!is.na(pred)]
  if (ref_only) r <- r[is_ref == TRUE]
  r[, p := if (ref_only) pred else back(pred, f, cf)]
  list(n = nrow(r), rho = stats::cor(r$p, r$target, method = "spearman"))
}

results <- list(); offs <- list()
for (role in c("batter", "bowler")) {
  idc <- if (role == "batter") "batter_id" else "bowler_id"
  sgn <- if (role == "batter") 1 else -1
  ec  <- if (role == "batter") "bowl_eff" else "bat_eff"
  b[, v0 := raa - get(ec)]
  off <- fit_competition_offsets(b, idc, "v0", REF)
  offs[[role]] <- off
  omap <- setNames(off$offset, off$comp)
  b[, coff := omap[comp]][is.na(coff), coff := 0]
  b[, `:=`(val_none = sgn * v0, val_factor = sgn * v0 / cfactor,
           val_offset = sgn * (v0 - coff), tgt = sgn * v0)]
  base <- b[, .(balls = .N, target = mean(tgt), f = mean(cfactor), cf = mean(coff),
                is_ref = all(is_ref), v0m = mean(v0), none = mean(val_none),
                factor = mean(val_factor), offset = mean(val_offset)),
            by = c(player_id = idc, "match_id", "match_date")]
  setnames(base, 1L, "player_id")
  backs <- list(none   = function(p, f, cf) p,
                factor = function(p, f, cf) p * f,
                offset = function(p, f, cf) p + sgn * cf)
  # How much of the offset does the metric actually want? lambda = 0 is no
  # adjustment at all, lambda = 1 is the full fitted offset. If the optimum sits
  # near 0 the honest reading is that the competition adjustment earns nothing
  # on this metric for this role, whatever its functional form.
  for (lam in c(0.25, 0.5, 0.75)) {
    pm <- copy(base); pm[, v := sgn * (v0m - lam * cf)]
    s <- run(pm, backs$none, TRUE)
    results[[length(results) + 1]] <- data.table(
      role, scope = "reference only", adj = sprintf("offset x%.2f", lam),
      n = s$n, rho = s$rho)
  }
  for (nm in c("none", "factor", "offset")) {
    pm <- copy(base); pm[, v := get(nm)]
    for (ro in c(FALSE, TRUE)) {
      s <- run(pm, backs[[nm]], ro)
      results[[length(results) + 1]] <- data.table(
        role, scope = if (ro) "reference only" else "all matches",
        adj = nm, n = s$n, rho = s$rho)
    }
  }
}
res <- rbindlist(results)
res[, gain := 100 * (rho - rho[adj == "none"]) / rho[adj == "none"], by = .(role, scope)]
setorder(res, role, scope, adj)
cat("\n=== T20 men, next-match Spearman, FIXED target (higher is better) ===\n")
cat("'reference only' is the clean scope: every arm's adjustment is the\n")
cat("identity there, so the arms differ only in how they treated a player's\n")
cat("earlier weak-league matches. 'all matches' is confounded by\n")
cat("between-competition variance and is shown for contrast.\n\n")
cat(sprintf("%-8s %-16s %-8s %8s %8s %9s\n", "role", "scope", "adj", "n", "rho", "vs none"))
for (i in 1:nrow(res)) with(res[i], cat(sprintf("%-8s %-16s %-8s %8d %8.4f %+8.1f%%\n",
    role, scope, adj, n, rho, gain)))
for (role in names(offs)) {
  o <- offs[[role]]
  cat(sprintf("\n%s offsets: %d competitions, %+.3f to %+.3f (reference = 0)\n",
      role, nrow(o), min(o$offset), max(o$offset)))
  print(head(o[order(-offset), .(comp, offset = round(offset, 3), n_bridges, step)], 5))
}
