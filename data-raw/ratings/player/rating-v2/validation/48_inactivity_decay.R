# Experiment: an extra decay that runs only while a player is NOT playing
# (Pete, 2026-10-11). Herath still ranked #21 7.7 years after his last Test:
# with 82 Tests his evidence fades slowly under the measured 5-year bowler
# decay. The extra term multiplies his own evidence by
#   exp(-max(0, gap - grace) / D2)
# for every gap between matches (and from his last match to as_at), so active
# players are untouched and long absences pull him toward the debut-level
# target. Testable only on players who came back after a long gap.
# Read-only on bouncer.duckdb. Uses the production shrink target (debut-10).
t0 <- Sys.time()
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
names_dt <- as.data.table(dbGetQuery(conn,
  "SELECT player_id, ANY_VALUE(player_name) AS player_name FROM cricsheet.players GROUP BY player_id"))

f <- get("calculate_player_rating_v2", asNamespace("bouncer"))
steps <- as.list(body(f))
at <- which(vapply(steps, function(e) any(grepl("(w, exp(", deparse(e), fixed = TRUE)), logical(1)))
stopifnot(length(at) == 1L)
trace("calculate_player_rating_v2", at = at, where = asNamespace("bouncer"), print = FALSE,
      tracer = quote(assign(".cap", list(pm = data.table::copy(pm), k = prior_matches,
        decay = decay_days, ref = ref_date, target = target), envir = globalenv())))

# Settings: grace (days of absence that cost nothing) x D2 (extra e-folding).
grid <- CJ(grace = c(365, 730), D2 = c(Inf, 365, 730, 1460))[!(is.infinite(D2) & grace == 730)]
grid[, name := ifelse(is.infinite(D2), "off", sprintf("g%.0f_D%.0f", grace, D2))]

buckets <- list()
for (fg in list(c("t20","male",""),c("t20","female",""),c("odi","male",""),c("odi","female",""),c("test","male",""),c("test","male","test")))
  for (ro in c("batter","bowler"))
    buckets[[length(buckets)+1]] <- list(label = paste0(fg[1], if (nzchar(fg[3])) "_intl" else "", "-", fg[2], " ", ro),
      format = fg[1], gender = fg[2], role = ro, mtf = if (nzchar(fg[3])) fg[3] else NULL)
sp <- function(x, y) cor(x, y, method = "spearman")
out <- list(); ex <- list()

for (bk in buckets) {
  r <- suppressMessages(get("calculate_player_rating_v2", asNamespace("bouncer"))(ageing = "none", bk$format, bk$gender, role = bk$role, conn = conn, match_type_filter = bk$mtf))
  cap <- get(".cap", globalenv()); pm <- cap$pm; tg <- cap$target
  setorder(pm, player_id, match_date, match_id)
  pm[, idx := seq_len(.N), by = player_id]
  pm[, gap := c(NA, diff(as.numeric(match_date))), by = player_id]
  pm[, fwd10 := { cs <- cumsum(v); k <- pmin(.N, idx + 9L); (cs[k] - c(0, cs)[idx]) / (k - idx + 1L) }, by = player_id]
  # anchor: "off" at as_at must reproduce production exactly
  last <- pm[, .(S = sum(v * exp(-as.numeric(cap$ref - match_date) / cap$decay)),
                 W = sum(exp(-as.numeric(cap$ref - match_date) / cap$decay)),
                 last = max(match_date)), by = player_id]
  chk <- merge(r[, .(player_id, rating)], last, by = "player_id")
  stopifnot(nrow(chk) == nrow(r), max(abs(chk$rating - (chk$S + cap$k * tg) / (chk$W + cap$k))) < 1e-9)

  for (gi in seq_len(nrow(grid))) {
    G <- grid$grace[gi]; D2 <- grid$D2[gi]; nm <- grid$name[gi]
    xd <- function(g) if (is.infinite(D2)) 1 else exp(-pmax(0, g - G) / D2)
    # causal prediction before each match, recursively
    pm[, pred := {
      p <- numeric(.N); s <- 0; ww <- 0
      for (i in seq_len(.N)) {
        if (i > 1L) { d <- exp(-gap[i] / cap$decay) * xd(gap[i]); s <- (s + v[i - 1L]) * d; ww <- (ww + 1) * d }
        p[i] <- (s + cap$k * tg) / (ww + cap$k)
      }
      p
    }, by = player_id]
    d <- pm[idx >= 2L]
    back <- d[gap > 2 * 365.25]
    # rating as at ref_date: extra decay applies to each historical gap AND the
    # final absence from last match to as_at
    at_ref <- pm[, {
      s <- 0; ww <- 0
      for (i in seq_len(.N)) {
        if (i > 1L) { dd <- exp(-gap[i] / cap$decay) * xd(gap[i]); s <- s * dd; ww <- ww * dd }
        s <- s + v[i]; ww <- ww + 1
      }
      g_end <- as.numeric(cap$ref - match_date[.N])
      dd <- exp(-g_end / cap$decay) * xd(g_end)
      .(rating = (s * dd + cap$k * tg) / (ww * dd + cap$k))
    }, by = player_id][player_id %in% r$player_id]
    at_ref[, rk := frank(-rating)]
    at_ref[names_dt, on = "player_id", player := i.player_name]
    at_ref[last, on = "player_id", yrs := round(as.numeric(cap$ref - i.last) / 365.25, 1)]
    out[[paste(bk$label, nm)]] <- data.table(bucket = bk$label, setting = nm,
      all10 = sp(d$pred, d$fwd10), all1 = sp(d$pred, d$v),
      n_back = nrow(back), back10 = sp(back$pred, back$fwd10), back1 = sp(back$pred, back$v),
      back_actual = mean(back$v), back_pred = mean(back$pred),
      gone3y_top20 = at_ref[yrs > 3 & rk <= 20, .N])
    ex[[paste(bk$label, nm)]] <- at_ref[grepl("Herath|Morkel|Sangakkara|Imran Tahir|Shrubsole|Bumrah", player)][, `:=`(bucket = bk$label, setting = nm)]
  }
  cat(sprintf("%s done (%.1f min)\n", bk$label, as.numeric(difftime(Sys.time(), t0, units = "mins"))))
}
res <- rbindlist(out); exs <- rbindlist(ex)
num <- c("all10", "all1", "back10", "back1", "back_actual", "back_pred")
res[, (num) := lapply(.SD, round, 4), .SDcols = num]
options(width = 220)
cat("\nNext-10 / next-1 Spearman, all player-matches and players back after a 2y+ gap. Higher is better.\n")
print(res, nrows = 200)
cat("\nExamples (rating and rank as at the bucket's as_at):\n")
print(exs[order(player, bucket, setting), .(player, bucket, setting, yrs, rating = round(rating, 2), rk)], nrows = 200)
fwrite(res, file.path(tempdir(), "inactivity_summary.csv"))
cat(sprintf("\nTotal %.1f min\n", as.numeric(difftime(Sys.time(), t0, units = "mins"))))
