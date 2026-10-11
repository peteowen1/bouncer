# Experiment: shrink player_rating_v2 toward REPLACEMENT level instead of the
# bucket population mean (Pete, 2026-10-11). Read-only on bouncer.duckdb.
# Captures the exact per-match table the production function builds (via
# trace), so both priors see identical inputs; only the shrink target differs.
t0 <- Sys.time()
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
OUT <- file.path(tempdir(), "rp_out")
dir.create(OUT, showWarnings = FALSE)
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
names_dt <- as.data.table(dbGetQuery(conn,
  "SELECT player_id, ANY_VALUE(player_name) AS player_name FROM cricsheet.players GROUP BY player_id"))

f <- bouncer::calculate_player_rating_v2
steps <- as.list(body(f))
at <- which(vapply(steps, function(e) any(grepl("(w, exp(", deparse(e), fixed = TRUE)), logical(1)))
stopifnot(length(at) == 1L)
trace("calculate_player_rating_v2", at = at, where = asNamespace("bouncer"), print = FALSE,
      tracer = quote(assign(".cap", list(pm = data.table::copy(pm), k = prior_matches,
        decay = decay_days, ref = ref_date, pop = pop), envir = globalenv())))

buckets <- list()
for (fg in list(c("t20","male",""),c("t20","female",""),c("odi","male",""),c("odi","female",""),c("test","male",""),c("test","male","test")))
  for (ro in c("batter","bowler"))
    buckets[[length(buckets)+1]] <- list(label = paste0(fg[1], if (nzchar(fg[3])) "_intl" else "", "-", fg[2], " ", ro),
      format = fg[1], gender = fg[2], role = ro, mtf = if (nzchar(fg[3])) fg[3] else NULL, show = c("Bumrah", "Herath"))
summ <- list()
for (bk in buckets) {
  cat("\n==========", bk$label, "==========\n")
  t1 <- Sys.time()
  r <- get("calculate_player_rating_v2", asNamespace("bouncer"))(bk$format, bk$gender, role = bk$role, conn = conn,
                                  match_type_filter = bk$mtf)
  cap <- get(".cap", globalenv()); pm <- cap$pm
  cat(sprintf("rating fn: %.1f min; %s player-matches, %d players, k=%.1f, decay=%d, as_at=%s\n",
              as.numeric(difftime(Sys.time(), t1, units = "mins")),
              format(nrow(pm), big.mark = ","), uniqueN(pm$player_id), cap$k, cap$decay, cap$ref))

  # Replacement candidates, both stated in the same per-match units as pop.
  setorder(pm, player_id, match_date, match_id)
  pm[, idx := seq_len(.N), by = player_id]
  debut <- pm[idx <= 10, mean(v)]                     # what players do in their first 10 matches
  pmean <- pm[, .(n = .N, m = mean(v)), by = player_id]
  fringe <- pmean[n >= 20, quantile(m, 0.2)]          # 20th pct of established players' means
  cat(sprintf("shrink targets (per match): pop %.3f | debut-10 %.3f | fringe p20 %.3f\n",
              cap$pop, debut, fringe))

  # Anchor: recompute the production rating from the capture; must match.
  pm[, w := exp(-as.numeric(cap$ref - match_date) / cap$decay)]
  rate <- function(target) pm[, .(rating = (sum(v * w) + cap$k * target) / (sum(w) + cap$k),
                                  eff = sum(w), n = .N, last = max(match_date)), by = player_id]
  base <- rate(cap$pop)
  chk <- merge(r[, .(player_id, rating)], base[, .(player_id, mine = rating)], by = "player_id")
  stopifnot(nrow(chk) == nrow(r), max(abs(chk$rating - chk$mine)) < 1e-9)
  cat("anchor: recomputed pop-prior ratings match the function exactly (", nrow(chk), "players )\n")

  listed <- r$player_id    # same listing floor as production
  tab <- base[player_id %in% listed][, .(player_id, eff, n, last, r_pop = rating)]
  tab[rate(debut), on = "player_id", r_debut := i.rating]
  tab[rate(fringe), on = "player_id", r_fringe := i.rating]
  tab[, `:=`(rk_pop = frank(-r_pop), rk_debut = frank(-r_debut), rk_fringe = frank(-r_fringe))]
  tab[names_dt, on = "player_id", player := i.player_name]
  tab[, yrs := round(as.numeric(cap$ref - last) / 365.25, 1)]
  ex <- tab[Reduce(`|`, lapply(bk$show, function(s) grepl(s, player, fixed = TRUE)))]
  cat("\nExample players (rating per match; rank among", nrow(tab), "listed):\n")
  print(ex[order(rk_pop), .(player, n, eff = round(eff, 1), yrs, r_pop = round(r_pop, 2), rk_pop,
                            r_debut = round(r_debut, 2), rk_debut, r_fringe = round(r_fringe, 2), rk_fringe)])
  cat("\nBiggest movers in the top 50 under debut-10:\n")
  print(tab[rk_pop <= 50 | rk_debut <= 50][order(-abs(rk_debut - rk_pop))][1:10,
        .(player, n, eff = round(eff, 1), yrs, rk_pop, rk_debut)])
  fwrite(tab, file.path(OUT, paste0(gsub("[^a-z0-9]+", "_", bk$label), "_ratings.csv")))

  # Next-match test, strictly causal. Exponential decay is recursive, so the
  # rating before match i is built in O(n): S = sum v*w, W = sum w, both decayed
  # by the gap since the previous match.
  pm[, gap := c(NA, diff(as.numeric(match_date))), by = player_id]
  pm[, c("S", "W") := {
    S <- W <- numeric(.N); s <- 0; ww <- 0
    for (i in seq_len(.N)) {
      if (i > 1L) { d <- exp(-gap[i] / cap$decay); s <- (s + v[i - 1L]) * d; ww <- (ww + 1) * d }
      S[i] <- s; W[i] <- ww
    }
    list(S, W)
  }, by = player_id]
  pm[, fwd10 := { cs <- cumsum(v); k <- pmin(.N, idx + 9L); (cs[k] - c(0, cs)[idx]) / (k - idx + 1L) },
     by = player_id]
  d <- pm[idx >= 2L]
  for (nm in c("pop", "debut", "fringe")) {
    tg <- switch(nm, pop = cap$pop, debut = debut, fringe = fringe)
    set(d, j = paste0("p_", nm), value = (d$S + cap$k * tg) / (d$W + cap$k))
  }
  d[, band := fcase(idx <= 5L, "a 1-4 prior", idx <= 20L, "b 5-19 prior", default = "c 20+ prior")]
  d[, back := gap > 2 * 365.25]
  sp <- function(x, y) cor(x, y, method = "spearman")
  cat("\nNext-match Spearman (rank correlation of the pre-match rating with what he did next;",
      "higher is better). Pooled over all player-matches with 1+ prior match.\n")
  grid <- rbind(
    d[, .(cut = "ALL", n = .N, pop1 = sp(p_pop, v), deb1 = sp(p_debut, v), fr1 = sp(p_fringe, v),
          pop10 = sp(p_pop, fwd10), deb10 = sp(p_debut, fwd10), fr10 = sp(p_fringe, fwd10))],
    d[, .(n = .N, pop1 = sp(p_pop, v), deb1 = sp(p_debut, v), fr1 = sp(p_fringe, v),
          pop10 = sp(p_pop, fwd10), deb10 = sp(p_debut, fwd10), fr10 = sp(p_fringe, fwd10)),
      by = .(cut = band)][order(cut)],
    d[back == TRUE, .(cut = "back after 2y+ gap", n = .N, pop1 = sp(p_pop, v), deb1 = sp(p_debut, v),
          fr1 = sp(p_fringe, v), pop10 = sp(p_pop, fwd10), deb10 = sp(p_debut, fwd10), fr10 = sp(p_fringe, fwd10))])
  num <- names(grid)[-(1:2)]; grid[, (num) := lapply(.SD, round, 4), .SDcols = num]
  print(grid)
  lv <- d[band == "a 1-4 prior", .(actual = mean(v), pop = mean(p_pop), debut = mean(p_debut))]
  top <- tab[rk_pop <= 20, sum(rk_debut <= 20)]
  summ[[bk$label]] <- data.table(bucket = bk$label, n = grid$n[1], k = round(cap$k,1), pop10 = grid$pop10[1], deb10 = grid$deb10[1], pop1 = grid$pop1[1], deb1 = grid$deb1[1], new_actual = round(lv$actual,2), new_pred_pop = round(lv$pop,2), new_pred_debut = round(lv$debut,2), top20_kept = top)
  # Level check: does each prior predict the right AVERAGE for low-evidence players?
  cat("\nMean actual next-match value vs mean prediction, players with 1-4 prior matches:\n")
  print(d[band == "a 1-4 prior", .(n = .N, actual = round(mean(v), 3), pop = round(mean(p_pop), 3),
                                    debut = round(mean(p_debut), 3), fringe = round(mean(p_fringe), 3))])
}
cat(sprintf("\nTotal %.1f min\n", as.numeric(difftime(Sys.time(), t0, units = "mins"))))
