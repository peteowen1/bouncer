# The horizon sweep for EVERY bucket, leak-free.
#
# Two changes from the Test-only version:
#  1. Parameterised over all five buckets.
#  2. The competition factors and the two-way opponent effects are fitted with
#     as_at = CUTOFF and the evaluation uses only player-matches AFTER it. The
#     Test-only run fitted them on all data and then evaluated forward, which
#     lets the adjustment know the future -- the same leak family as the as_at
#     bug fixed earlier today. Per-player predictors were already strictly prior;
#     this closes the remaining hole.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

CUTOFF    <- as.Date("2018-01-01")
MIN_PRIOR <- 10L
HORIZONS  <- c(1L, 3L, 5L, 10L, 20L)
id_map    <- build_player_id_map(conn)

buckets <- list(
  list(f = "t20",  g = "male",   decay = 1095, prior = 20),
  list(f = "odi",  g = "male",   decay = 1095, prior = 20),
  list(f = "test", g = "male",   decay = 1095, prior = 20),
  list(f = "t20",  g = "female", decay = 1095, prior = 20),
  list(f = "odi",  g = "female", decay = 1095, prior = 20)
)

out <- data.table()
for (bk in buckets) {
  tag <- paste(bk$f, bk$g)
  cat("\n", strrep("=", 66), "\n", toupper(tag), "\n", strrep("=", 66), "\n", sep = "")

  b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, r.raa,
           COALESCE(%s,'unknown') AS comp
    FROM main.cricsheet_ball_raa r
    JOIN cricsheet.matches m ON m.match_id = r.match_id
    WHERE r.format='%s' AND r.gender='%s'",
    .competition_sql(bk$f), toupper(bk$f), bk$g)))
  canonicalise_player_ids(b, id_map)
  cat(sprintf("  %s deliveries\n", format(nrow(b), big.mark = ",")))

  # --- adjustments fitted on PRE-CUTOFF data only -------------------------
  pre <- b[match_date < CUTOFF]
  cat(sprintf("  fitting adjustments on %s pre-%s deliveries\n",
              format(nrow(pre), big.mark = ","), format(CUTOFF)))
  fac <- tryCatch(suppressMessages(fit_competition_factors(
           conn, bk$f, bk$g, id_map = id_map, as_at = CUTOFF - 1L)),
           error = function(e) NULL)
  if (is.null(fac)) { cat("  competition fit failed -- skipped\n"); next }
  fmap <- setNames(fac$factor, fac$comp)
  eff  <- fit_two_way_effects(pre, prior_balls = 60, iterations = 20)

  # apply those FIXED adjustments to the whole span
  b[, cfactor := fmap[comp]][is.na(cfactor), cfactor := 1]
  b[eff$bowler, on = "bowler_id", bo := i.eff][is.na(bo), bo := 0]
  b[eff$batter, on = "batter_id", ba := i.eff][is.na(ba), ba := 0]

  for (role in c("batter", "bowler")) {
    idc <- if (role == "batter") "batter_id" else "bowler_id"
    b[, val := if (role == "batter") (raa - bo) / cfactor else -(raa - ba) / cfactor]
    pm <- b[, .(v = sum(val), v_raw = if (role == "batter") sum(raa) else -sum(raa)),
            by = c(idc, "match_id", "match_date")]
    setnames(pm, idc, "player_id")
    setorder(pm, player_id, match_date, match_id)
    pop <- pm[, mean(v)]
    pm[, idx := seq_len(.N), by = player_id]

    res <- pm[, {
      n <- .N; rt <- rep(NA_real_, n); cw <- rep(NA_real_, n)
      if (n >= 2L) for (i in 2:n) {
        j <- 1:(i - 1L)
        w <- exp(-as.numeric(match_date[i] - match_date[j]) / bk$decay)
        rt[i] <- (sum(v[j] * w) + bk$prior * pop) / (sum(w) + bk$prior)
        cw[i] <- mean(v_raw[j])
      }
      .(match_date, v_raw, idx, rt, cw, n_prior = idx - 1L)
    }, by = player_id]

    for (H in HORIZONS) {
      res[, fwd := { cs <- cumsum(v_raw); k <- pmin(.N, idx + H - 1L)
                     (cs[k] - c(0, cs)[idx]) / (k - idx + 1L) }, by = player_id]
      d <- res[n_prior >= MIN_PRIOR & match_date >= CUTOFF &
                 is.finite(rt) & is.finite(cw) & is.finite(fwd)]
      if (nrow(d) < 200) next
      rr <- cor(d$rt, d$fwd, method = "spearman")
      cc <- cor(d$cw, d$fwd, method = "spearman")
      out <- rbind(out, data.table(bucket = tag, role = role, horizon = H,
                                   n = nrow(d), rating = rr, career = cc,
                                   gain = 100 * (rr - cc) / abs(cc)))
    }
    cat(sprintf("    %s done\n", role))
  }
}

cat("\n\n", strrep("=", 78), "\n RESULTS: rating vs plain career mean, adjustments fitted pre-2018 only\n",
    strrep("=", 78), "\n", sep = "")
for (rl in c("batter", "bowler")) {
  cat("\n---", toupper(rl), "---\n")
  w <- dcast(out[role == rl], bucket ~ horizon, value.var = "gain")
  setnames(w, as.character(HORIZONS), paste0("next", HORIZONS), skip_absent = TRUE)
  print(w[, lapply(.SD, function(x) if (is.numeric(x)) round(x, 1) else x)])
}
saveRDS(out, "C:/Users/peteo/AppData/Local/Temp/claude/C--dev-bouncerverse/635fc43f-1352-411b-8c7d-693d0ebc00b2/scratchpad/test_lambda/all_buckets.rds")
cat("\nsaved all_buckets.rds\n")
