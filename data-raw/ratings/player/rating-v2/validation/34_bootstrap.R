# Bootstrap the next-1 comparison. The four predictors sit within 0.0024 of each
# other on a rho of ~0.05, which is not evidence of an ordering until the
# uncertainty is sized.
#
# Resampled by PLAYER, not by row: a player contributes many correlated
# player-matches, so a row bootstrap would understate the uncertainty badly.
# Paired throughout -- every replicate scores all four predictors on the SAME
# resampled players, so the DIFFERENCES are what get a confidence interval,
# which is the actual question.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

CUT <- as.Date("2018-01-01"); MIN_PRIOR <- 10L; B <- 400L
id_map <- build_player_id_map(conn)

b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
  SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id,
         r.raa_run, r.waa, r.tsa, r.raa, COALESCE(%s,'unknown') AS comp
  FROM main.cricsheet_ball_raa r
  JOIN cricsheet.matches m ON m.match_id = r.match_id
  WHERE r.format='T20' AND r.gender='male' AND r.tsa IS NOT NULL",
  .competition_sql("t20"))))
canonicalise_player_ids(b, id_map)
pre <- b[match_date < CUT]

fac_r <- fit_competition_factors(conn,"t20","male",id_map=id_map,as_at=CUT-1L,basis="runs")
fac_s <- fit_competition_factors(conn,"t20","male",id_map=id_map,as_at=CUT-1L,basis="survival")

mk <- function(col, fac) {
  fmap <- setNames(fac$factor, fac$comp)
  p <- copy(pre); p[, raa := get(col)]
  eff <- fit_two_way_effects(p, prior_balls = 60, iterations = 20)
  d <- copy(b); d[, cf := fmap[comp]][is.na(cf), cf := 1]
  d[eff$bowler, on="bowler_id", bo := i.eff][is.na(bo), bo := 0]
  d[, val := (get(col) - bo)/cf]
  x <- d[, .(v = sum(val), raw = sum(get(col))), by=.(player_id=batter_id, match_id, match_date)]
  setorder(x, player_id, match_date, match_id); x[, idx := seq_len(.N), by=player_id]; x }

dec <- function(v, dt, d, prior, pop) {
  n <- length(v); rt <- rep(NA_real_,n); sw <- 0; svw <- 0
  if (n >= 2L) for (i in 2:n) { a <- exp(-as.numeric(dt[i]-dt[i-1L])/d)
    svw <- a*(svw+v[i-1L]); sw <- a*(sw+1); rt[i] <- (svw+prior*pop)/(sw+prior) }
  rt }

pm <- list(runs = mk("raa_run", fac_r), wkt = mk("waa", fac_s),
           tsa = mk("tsa", fac_r), comp = mk("raa", fac_r))
for (k in names(pm)) { x <- pm[[k]]; pop <- x[, mean(v)]
  x[, rt := dec(v, match_date, 1095, 20, pop), by=player_id] }

# common target: next-1 raw team-score, plus its own career mean as baseline
t <- pm$tsa
t[, f := { cs <- cumsum(raw); k <- pmin(.N, idx); (cs[k]-c(0,cs)[idx])/(k-idx+1L) }, by=player_id]
t[, cw := { cs <- cumsum(raw); c(NA, cs[-.N]/seq_len(.N-1L)) }, by=player_id]

d <- t[, .(player_id, match_id, match_date, f, cw, idx)]
for (k in names(pm)) d <- merge(d, pm[[k]][, .(player_id, match_id, r = rt)],
                                by=c("player_id","match_id"), suffixes=c("", paste0("_",k)))
setnames(d, c("r","r_wkt","r_tsa","r_comp"), c("p_runs","p_wkt","p_tsa","p_comp"))
e <- d[idx-1L >= MIN_PRIOR & match_date >= CUT &
         is.finite(p_runs) & is.finite(p_wkt) & is.finite(p_tsa) &
         is.finite(p_comp) & is.finite(cw) & is.finite(f)]
cat(sprintf("rows %s, players %d\n\n", format(nrow(e), big.mark=","), uniqueN(e$player_id)))

preds <- c(runs="p_runs", wickets="p_wkt", team_score="p_tsa",
           composite="p_comp", baseline="cw")
pt <- sapply(preds, function(c0) cor(e[[c0]], e$f, method="spearman"))

players <- unique(e$player_id)
set.seed(20260818)
bs <- matrix(NA_real_, B, length(preds), dimnames = list(NULL, names(preds)))
for (i in seq_len(B)) {
  s <- sample(players, length(players), replace = TRUE)
  r <- e[data.table(player_id = s), on = "player_id", allow.cartesian = TRUE]
  bs[i, ] <- sapply(preds, function(c0) cor(r[[c0]], r$f, method="spearman"))
}

cat("=== next-1, bootstrap over 400 player resamples ===\n")
cat(sprintf("  %-12s %8s %8s %18s\n", "predictor", "rho", "se", "95% CI"))
for (k in names(preds))
  cat(sprintf("  %-12s %8.4f %8.4f   [%+.4f, %+.4f]\n", k, pt[k], sd(bs[,k]),
              quantile(bs[,k], .025), quantile(bs[,k], .975)))

cat("\n=== PAIRED differences vs the career-mean baseline ===\n")
for (k in setdiff(names(preds), "baseline")) {
  dif <- bs[,k] - bs[,"baseline"]
  cat(sprintf("  %-12s %+.4f   95%% CI [%+.4f, %+.4f]   %s\n", k,
              pt[k]-pt["baseline"], quantile(dif,.025), quantile(dif,.975),
              if (quantile(dif,.025) > 0) "beats baseline" else
              if (quantile(dif,.975) < 0) "worse" else "not distinguishable"))
}
cat("\n=== PAIRED: composite vs team_score (the apparent next-1 winner) ===\n")
dif <- bs[,"composite"] - bs[,"team_score"]
cat(sprintf("  %+.4f   95%% CI [%+.4f, %+.4f]   %s\n",
            pt["composite"]-pt["team_score"], quantile(dif,.025), quantile(dif,.975),
            if (quantile(dif,.025) > 0) "composite genuinely better" else
            if (quantile(dif,.975) < 0) "team_score genuinely better" else
            "TIED -- the ordering was noise"))
