# Per-format parameter sweep on an HONEST metric.
#
# Design notes that matter:
#
# 1. METRIC FIRST. The adjustments are refitted AT EACH ORIGIN (as_at = origin),
#    so at every evaluation point they know everything up to that point and
#    nothing after -- what a live system would actually have. The earlier runs
#    were an upper bound (fitted on all data) and a lower bound (frozen at
#    pre-2018). This is the middle, and the only version worth tuning against.
#
# 2. The levers are separable, which is what makes this affordable:
#      ridge prior  -> two-way fit only
#      clamp        -> competition factors only
#      decay/prior  -> aggregation only
#    So one two-way fit per (bucket, origin, ridge) serves both roles, and the
#    aggregation params can be swept on top for free. Clamp is swept separately
#    with the winning ridge, because it needs no two-way refit at all.
#
# 3. Target is forward RAW output. Never the adjusted value -- the rating
#    aggregates that, so an adjusted target flatters it by construction.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
OUT <- "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda"

ORIGINS   <- as.Date(c("2019-01-01", "2022-01-01", "2025-01-01"))
RIDGE     <- c(30, 60, 150, 400)
HORIZON   <- 5L          # mid-range; short horizons are dominated by noise
MIN_PRIOR <- 10L
id_map    <- build_player_id_map(conn)

buckets <- list(c("t20","male"), c("odi","male"), c("test","male"),
                c("t20","female"), c("odi","female"))

out <- data.table()
for (bk in buckets) {
  f <- bk[1]; g <- bk[2]; tag <- paste(f, g)
  cat("\n", strrep("=", 60), "\n", toupper(tag), "\n", strrep("=", 60), "\n", sep = "")

  ball <- as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, r.raa,
           COALESCE(%s,'unknown') AS comp
    FROM main.cricsheet_ball_raa r
    JOIN cricsheet.matches m ON m.match_id = r.match_id
    WHERE r.format='%s' AND r.gender='%s'",
    .competition_sql(f), toupper(f), g)))
  if (!nrow(ball)) { cat("  no rows\n"); next }
  canonicalise_player_ids(ball, id_map)

  for (T0 in as.list(ORIGINS)) {
    fac <- tryCatch(suppressMessages(fit_competition_factors(
             conn, f, g, id_map = id_map, as_at = T0 - 1L)), error = function(e) NULL)
    if (is.null(fac)) next
    fmap <- setNames(fac$factor, fac$comp)
    pre  <- ball[match_date < T0]
    if (nrow(pre) < 50000) next

    for (rg in RIDGE) {
      eff <- fit_two_way_effects(pre, prior_balls = rg, iterations = 20)
      d <- copy(ball)
      d[, cf := fmap[comp]][is.na(cf), cf := 1]
      d[eff$bowler, on = "bowler_id", bo := i.eff][is.na(bo), bo := 0]
      d[eff$batter, on = "batter_id", ba := i.eff][is.na(ba), ba := 0]

      for (role in c("batter","bowler")) {
        idc <- if (role == "batter") "batter_id" else "bowler_id"
        d[, val := if (role == "batter") (raa - bo)/cf else -(raa - ba)/cf]
        pm <- d[, .(v = sum(val),
                    v_raw = if (role == "batter") sum(raa) else -sum(raa)),
                by = c(idc, "match_id", "match_date")]
        setnames(pm, idc, "player_id")
        setorder(pm, player_id, match_date, match_id)
        pop <- pm[, mean(v)]; dk <- if (role == "batter") 1095 else 1825
        pm[, idx := seq_len(.N), by = player_id]
        rr <- pm[, {
          n <- .N; rt <- rep(NA_real_, n); cw <- rep(NA_real_, n)
          if (n >= 2L) for (i in 2:n) {
            j <- 1:(i-1L); w <- exp(-as.numeric(match_date[i]-match_date[j])/dk)
            rt[i] <- (sum(v[j]*w) + 20*pop)/(sum(w) + 20); cw[i] <- mean(v_raw[j])
          }
          .(match_date, v_raw, idx, rt, cw, np = idx - 1L)
        }, by = player_id]
        rr[, fwd := { cs <- cumsum(v_raw); k <- pmin(.N, idx + HORIZON - 1L)
                      (cs[k] - c(0,cs)[idx])/(k - idx + 1L) }, by = player_id]
        # score ONLY the 12 months after this origin -- each origin contributes
        # its own honest slice, refitted for that moment
        e <- rr[np >= MIN_PRIOR & match_date >= T0 & match_date < T0 + 365 &
                  is.finite(rt) & is.finite(cw) & is.finite(fwd)]
        if (nrow(e) < 150) next
        out <- rbind(out, data.table(bucket = tag, role = role, origin = T0,
          ridge = rg, n = nrow(e),
          gain = 100 * (cor(e$rt, e$fwd, method="spearman") -
                        cor(e$cw, e$fwd, method="spearman")) /
                 abs(cor(e$cw, e$fwd, method="spearman"))))
      }
      cat(sprintf("  %s ridge %4d done\n", format(T0), rg))
    }
  }
}

saveRDS(out, file.path(OUT, "ridge_sweep.rds"))
cat("\n\n=== GAIN over career mean (%), pooled across origins, horizon 5 ===\n")
agg <- out[, .(gain = round(weighted.mean(gain, n), 1), n = sum(n)),
           by = .(bucket, role, ridge)]
for (rl in c("batter","bowler")) {
  cat("\n---", toupper(rl), "--- (columns = ridge prior in balls)\n")
  print(dcast(agg[role == rl], bucket ~ ridge, value.var = "gain"))
}
cat("\ncurrent shipped value is ridge 60\n")
