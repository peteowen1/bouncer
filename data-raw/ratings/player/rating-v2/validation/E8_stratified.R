# The competition adjustment can only move a player whose record CONTAINS
# weak-league cricket. E7 scored it over all reference matches, where most rows
# are players whose entire prior record is reference cricket and the offset is
# exactly 0 -- so a real effect on the minority who bridge is diluted toward
# zero by the majority it cannot touch.
#
# Pete's objection is the right one: the measured gap between International
# (Developing) and the major leagues is +0.176 against +0.056 RVAA/ball, a
# batting average of 34.3 against 22.5, and a -0.227 per-ball drop across 39
# bridgers. That is not a null effect. So score it on the players it applies to.
#
# Same forward-looking next-match Spearman, same fixed target, reference matches
# only -- but STRATIFIED by how much of the player's own prior record was played
# outside the reference competitions.
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

for (role in c("batter", "bowler")) {
  idc <- if (role == "batter") "batter_id" else "bowler_id"
  sgn <- if (role == "batter") 1 else -1
  ec  <- if (role == "batter") "bowl_eff" else "bat_eff"
  b[, v0 := raa - get(ec)]
  off <- fit_competition_offsets(b, idc, "v0", REF)
  omap <- setNames(off$offset, off$comp)
  b[, coff := omap[comp]][is.na(coff), coff := 0]
  b[, `:=`(a_none = sgn * v0, a_factor = sgn * v0 / cfactor,
           a_offset = sgn * (v0 - coff), tgt = sgn * v0)]
  base <- b[, .(balls = .N, target = mean(tgt), is_ref = all(is_ref),
                none = mean(a_none), factor = mean(a_factor), offset = mean(a_offset)),
            by = c(player_id = idc, "match_id", "match_date")]
  setnames(base, 1L, "player_id")
  setorder(base, player_id, match_date, match_id)
  # share of the player's STRICTLY EARLIER balls played outside the reference
  base[, wk := balls * (!is_ref)]
  base[, `:=`(prior_bal = cumsum(balls) - balls,
              prior_wk  = cumsum(wk) - wk), by = player_id]
  base[, wshare := fifelse(prior_bal > 0, prior_wk / prior_bal, 0)]

  out <- list(); raw <- list()
  for (nm in c("none", "factor", "offset")) {
    pm <- copy(base); pm[, v := get(nm)]
    # Shrink toward the REFERENCE population mean, not the mean of this arm's
    # adjusted values over all competitions. In reference matches every arm
    # gives the same value (f = 1, c = 0), so this prior is identical across
    # arms by construction.
    #
    # Using each arm's own overall mean was a harness bug with a visible tell:
    # players with NO weak-league cricket scored -17.2% under the offset arm,
    # when their ratings must be identical to the no-adjustment arm by
    # construction. The offset lowers the population mean, so every thin-record
    # player is dragged toward a different number and the ordering shifts on
    # evidence rather than on skill -- in every stratum, including the one the
    # adjustment cannot touch. It is also the right choice on its own terms:
    # the rating is denominated in reference-equivalent runs, so its prior
    # should be what an average player does in the reference.
    pop <- pm[is_ref == TRUE, weighted.mean(v, balls)]
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
    r[, stratum := cut(wshare, c(-Inf, 0.001, 0.15, 0.35, 0.60, Inf),
                       labels = c("0% weak", "0-15%", "15-35%", "35-60%", "60%+"))]
    s <- r[, .(n = .N, rho = stats::cor(pred, target, method = "spearman")), by = stratum]
    s[, adj := nm]; out[[nm]] <- s; raw[[nm]] <- r
  }
  allr <- rbindlist(lapply(names(raw), function(nm) {
    x <- raw[[nm]]; data.table(adj = nm, n = nrow(x),
      rho = stats::cor(x$pred, x$target, method = "spearman")) }))
  allr[, gain := 100 * (rho - rho[adj == "none"]) / rho[adj == "none"]]
  cat(sprintf("
=== T20 men, %s: ALL reference matches (unstratified) ===
", role))
  for (i in 1:nrow(allr)) with(allr[i], cat(sprintf("  %-8s n %6d  rho %.4f  %+.1f%%
",
      adj, n, rho, gain)))
  o <- dcast(rbindlist(out), stratum + n ~ adj, value.var = "rho")
  o[, `:=`(fac_gain = 100*(factor-none)/none, off_gain = 100*(offset-none)/none)]
  setorder(o, stratum)
  cat(sprintf("\n=== T20 men, %s: next-match Spearman on REFERENCE matches ===\n", role))
  cat("Stratified by how much of the player's OWN prior record was weak-league.\n\n")
  cat(sprintf("%-10s %7s %8s %8s %8s %9s %9s\n",
      "prior weak","n","none","factor","offset","factor vs","offset vs"))
  for (i in 1:nrow(o)) with(o[i], cat(sprintf("%-10s %7d %8.4f %8.4f %8.4f %+8.1f%% %+8.1f%%\n",
      as.character(stratum), n, none, factor, offset, fac_gain, off_gain)))
}
