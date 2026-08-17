# Per-format parameter sweep, properly powered.
#
# Changes from the last attempt, which was too noisy to conclude anything:
#  1. 5 origins with 2-year NON-OVERLAPPING slices covering 2016-2026 in full,
#     instead of 3 origins x 12 months. No player-match is counted twice and
#     none is skipped.
#  2. Ridge grid extended to 3200. Last time 6 of 10 cells put their optimum on
#     a grid edge, so the grid did not contain the answer.
#  3. The decayed mean is computed INCREMENTALLY, O(n) per player instead of
#     O(n^2):
#        S_w(i)  = a_i * (S_w(i-1)  + 1)
#        S_vw(i) = a_i * (S_vw(i-1) + v_{i-1}),   a_i = exp(-(t_i - t_{i-1})/d)
#     which is exact, not an approximation, and makes the two-way fits the only
#     real cost.
#
# Metric unchanged and deliberately so: adjustments refit at each origin,
# target is forward RAW output, predictors strictly prior per player.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
OUT <- "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda"

ORIGINS   <- as.Date(c("2016-01-01","2018-01-01","2020-01-01","2022-01-01","2024-01-01"))
SLICE     <- 365L * 2L
RIDGE     <- c(60, 200, 800, 3200)
HORIZON   <- 5L
MIN_PRIOR <- 10L
id_map    <- build_player_id_map(conn)

buckets <- list(c("t20","male"), c("odi","male"), c("test","male"),
                c("t20","female"), c("odi","female"))

# Exact incremental decayed mean, shrunk to `pop` with weight `prior`.
decayed_prior <- function(v, dates, d, prior, pop) {
  n <- length(v); rt <- rep(NA_real_, n)
  sw <- 0; svw <- 0
  if (n >= 2L) for (i in 2:n) {
    a   <- exp(-as.numeric(dates[i] - dates[i - 1L]) / d)
    svw <- a * (svw + v[i - 1L])
    sw  <- a * (sw + 1)
    rt[i] <- (svw + prior * pop) / (sw + prior)
  }
  rt
}

out <- data.table()
for (bk in buckets) {
  f <- bk[1]; g <- bk[2]; tag <- paste(f, g)
  cat("\n", strrep("=", 58), "\n", toupper(tag), "\n", strrep("=", 58), "\n", sep = "")
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
      d0 <- copy(ball)
      d0[, cf := fmap[comp]][is.na(cf), cf := 1]
      d0[eff$bowler, on = "bowler_id", bo := i.eff][is.na(bo), bo := 0]
      d0[eff$batter, on = "batter_id", ba := i.eff][is.na(ba), ba := 0]

      for (role in c("batter","bowler")) {
        idc <- if (role == "batter") "batter_id" else "bowler_id"
        d0[, val := if (role == "batter") (raa - bo)/cf else -(raa - ba)/cf]
        pm <- d0[, .(v = sum(val),
                     v_raw = if (role == "batter") sum(raa) else -sum(raa)),
                 by = c(idc, "match_id", "match_date")]
        setnames(pm, idc, "player_id")
        setorder(pm, player_id, match_date, match_id)
        pop <- pm[, mean(v)]; dk <- if (role == "batter") 1095 else 1825
        pm[, idx := seq_len(.N), by = player_id]
        pm[, rt := decayed_prior(v, match_date, dk, 20, pop), by = player_id]
        pm[, cw := { cs <- cumsum(v_raw); c(NA, cs[-.N] / seq_len(.N - 1L)) }, by = player_id]
        pm[, fwd := { cs <- cumsum(v_raw); k <- pmin(.N, idx + HORIZON - 1L)
                      (cs[k] - c(0, cs)[idx]) / (k - idx + 1L) }, by = player_id]
        e <- pm[idx - 1L >= MIN_PRIOR & match_date >= T0 & match_date < T0 + SLICE &
                  is.finite(rt) & is.finite(cw) & is.finite(fwd)]
        if (nrow(e) < 300) next
        a <- cor(e$rt, e$fwd, method = "spearman")
        b <- cor(e$cw, e$fwd, method = "spearman")
        out <- rbind(out, data.table(bucket = tag, role = role, origin = T0,
                                     ridge = rg, n = nrow(e),
                                     gain = 100 * (a - b) / abs(b)))
      }
    }
    cat(sprintf("  %s done\n", format(T0)))
  }
}

saveRDS(out, file.path(OUT, "full_sweep.rds"))
cat("\n\n=== SAMPLE SIZE (rows scored per bucket/role, all origins, one ridge) ===\n")
print(dcast(out[ridge == 60, .(n = sum(n)), by = .(bucket, role)], bucket ~ role, value.var = "n"))

cat("\n=== GAIN over career mean (%), n-weighted across origins ===\n")
agg <- out[, .(gain = round(weighted.mean(gain, n), 1)), by = .(bucket, role, ridge)]
for (rl in c("batter","bowler")) {
  cat("\n---", toupper(rl), "--- (columns = ridge prior, balls)\n")
  print(dcast(agg[role == rl], bucket ~ ridge, value.var = "gain"))
}

cat("\n=== STABILITY at each bucket's BEST ridge: spread across the 5 origins ===\n")
best <- agg[, .SD[which.max(gain)], by = .(bucket, role)][, .(bucket, role, ridge)]
s <- merge(out, best, by = c("bucket","role","ridge"))[
  , .(mean = round(weighted.mean(gain, n), 1),
      spread = round(max(gain) - min(gain), 1), origins = .N), by = .(bucket, role, ridge)]
print(s[order(role, -mean)])
