# #57 part 2: sweep the BOWLING decay.
#
# The default is 1825 days against batting's 1095, on the theory that bowling
# ratings are noisier per ball and so need a longer window. Never swept.
#
# Target: next-match Spearman, strictly forward-looking -- at each player-match
# the rating sees only that player's STRICTLY EARLIER matches. Same target the
# shipped v2 work used (D-P17/D-P18/D-P19).
#
# Prior from the repo: this family of knobs has been worth UNDER 1%. Expect a
# flat curve and be ready to say so.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
conn <- get_db_connection(read_only = TRUE); on.exit(DBI::dbDisconnect(conn, shutdown=TRUE))
id_map <- build_player_id_map(conn)
MIN_PRIOR <- 10L
PRIOR_MATCHES <- 40   # shrinkage toward the population, in matches

sweep_fmt <- function(fmt) {
  b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, r.raa
    FROM main.cricsheet_ball_raa r WHERE r.format='%s' AND r.gender='male'", toupper(fmt))))
  canonicalise_player_ids(b, id_map)
  eff <- fit_two_way_effects(b, prior_balls = 60, iterations = 20)
  b[eff$batter, on = "batter_id", opp := i.eff][is.na(opp), opp := 0]
  b[, v := -(raa - opp)]                       # bowling value per ball
  pm <- b[, .(val = mean(v), balls = .N, md = first(match_date)),
          by = .(pid = bowler_id, match_id)]
  pm <- pm[!is.na(pid) & balls >= 6]
  setorder(pm, pid, md)
  pm[, idx := seq_len(.N), by = pid]
  pop <- pm[, mean(val)]

  res <- data.table()
  for (dd in c(365, 730, 1095, 1460, 1825, 2555, 3650, 1e6)) {
    sc <- pm[, {
      n <- .N; out <- rep(NA_real_, n)
      if (n > MIN_PRIOR) for (i in (MIN_PRIOR+1):n) {
        w <- exp(-as.numeric(md[i] - md[1:(i-1)]) / dd) * balls[1:(i-1)]
        sw <- sum(w)
        out[i] <- (sum(w * val[1:(i-1)]) + PRIOR_MATCHES * 30 * pop) / (sw + PRIOR_MATCHES * 30)
      }
      .(idx, pred = out, actual = val)
    }, by = pid]
    sc <- sc[!is.na(pred)]
    res <- rbind(res, data.table(decay = dd, n = nrow(sc),
                                 rho = cor(sc$pred, sc$actual, method = "spearman")))
  }
  res[, fmt := toupper(fmt)][]
}

out <- rbindlist(lapply(c("t20","odi","test"), sweep_fmt))
for (f in unique(out$fmt)) {
  s <- out[fmt == f]
  best <- s[which.max(rho)]
  base <- s[decay == 1825]
  cat(sprintf("\n%s bowling (n = %s player-matches)\n", f, format(base$n, big.mark=",")))
  for (i in seq_len(nrow(s))) cat(sprintf("  decay %7s  rho %.4f%s\n",
      if (s$decay[i] > 1e5) "none" else format(s$decay[i]), s$rho[i],
      if (s$decay[i] == 1825) "   <- current default" else ""))
  cat(sprintf("  best %s vs default 1825: %+.4f (%+.2f%%)\n",
      if (best$decay > 1e5) "none" else format(best$decay),
      best$rho - base$rho, 100*(best$rho - base$rho)/abs(base$rho)))
}
