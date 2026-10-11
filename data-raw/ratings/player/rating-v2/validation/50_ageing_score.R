# Score the ageing adjustment (Pete, 2026-10-11) on next-match Spearman across
# all 12 published buckets. Curve shape and walk-through: 49_ageing_curves.R.
#
# Leak control: the curve is fitted ONLY on matches before CUT, and predictions
# are scored ONLY on matches from CUT on. Each prediction still uses all of the
# player's strictly earlier matches, as in production.
#
# Prediction before match i, ageing on:
#   (sum_j (v_j + f(age_i) - f(age_j)) w_ij + k T) / (sum_j w_ij + k)
# built recursively from S = sum v w, W = sum w, F = sum f(age_j) w.
# Players with no date of birth get f = 0 throughout (identical to off).
# Read-only on bouncer.duckdb.
t0 <- Sys.time()
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
CUT <- as.Date("2018-01-01")
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
PD <- Sys.getenv("PLAYER_DETAILS", "C:/Users/peteo/AppData/Local/Temp/claude/C--dev/8edabc93-6381-43de-b41b-e2fda0fc6666/scratchpad/pd/bouncer_player_details.parquet")
stopifnot(file.exists(PD))
det <- unique(as.data.table(arrow::read_parquet(PD, col_select = c("player_id", "dob")))[
  , dob := as.Date(dob)][!is.na(dob)], by = "player_id")

trace("calculate_player_rating_v2", where = asNamespace("bouncer"), print = FALSE,
      at = which(vapply(as.list(body(calculate_player_rating_v2)),
                        function(e) any(grepl("(w, exp(", deparse(e), fixed = TRUE)), logical(1))),
      tracer = quote(assign(".cap", list(pm = data.table::copy(pm), k = prior_matches,
        decay = decay_days, ref = ref_date, target = target), envir = globalenv())))

fit_curve <- function(d, ages = 16:45, iters = 30L) {
  d <- d[!is.na(age)][, ab := pmin(pmax(floor(age), min(ages)), max(ages))]
  # Too little dated data to fit a curve: say so and return none.
  if (nrow(d) < 2000L || d[ab == 27, .N] < 200L) return(NULL)
  d[, f := 0]
  for (it in seq_len(iters)) {
    d[, a := mean(v - f), by = player_id]
    fa <- d[, .(f = mean(v - a), n = .N), by = ab]
    fa[, f := f - fa[ab == 27, f]]
    d[fa, on = "ab", f := i.f]
  }
  fa <- fa[order(ab)]
  fa[, f_s := vapply(ab, function(x) { w <- fa[abs(ab - x) <= 2]; sum(w$f * w$n) / sum(w$n) }, 0)]
  fa[, thin := n < 200L]
  fa[]
}
curve_at <- function(cv, age) {
  out <- rep(0, length(age)); ok <- !is.na(age)
  if (is.null(cv) || !any(!cv$thin)) return(out)
  a <- pmin(pmax(age[ok], min(cv$ab[!cv$thin])), max(cv$ab[!cv$thin]))
  out[ok] <- approx(cv$ab, cv$f_s, xout = a, rule = 2)$y
  out
}

buckets <- list()
for (fg in list(c("t20","male",""),c("t20","female",""),c("odi","male",""),c("odi","female",""),c("test","male",""),c("test","male","test")))
  for (ro in c("batter","bowler"))
    buckets[[length(buckets)+1]] <- list(label = paste0(fg[1], if (nzchar(fg[3])) "_intl" else "", "-", fg[2], " ", ro),
      format = fg[1], gender = fg[2], role = ro, mtf = if (nzchar(fg[3])) fg[3] else NULL)
sp <- function(x, y) if (length(x) > 30) cor(x, y, method = "spearman") else NA_real_
# Each bucket is saved as it finishes and skipped on a rerun, so a run stopped
# part-way (it has been, for memory) keeps what it finished.
RES_DIR <- Sys.getenv("AGEING_OUT", file.path(dirname(PD), "ageing_score"))
dir.create(RES_DIR, showWarnings = FALSE, recursive = TRUE)
for (bk in buckets) {
  res_file <- file.path(RES_DIR, paste0(gsub("[^a-z0-9]+", "_", bk$label), ".csv"))
  if (file.exists(res_file)) { cat(bk$label, "already scored, skipping
"); next }
  r <- suppressMessages(get("calculate_player_rating_v2", asNamespace("bouncer"))(
    bk$format, bk$gender, role = bk$role, conn = conn, match_type_filter = bk$mtf))
  cap <- get(".cap", globalenv()); pm <- cap$pm; tg <- cap$target
  pm[det, on = "player_id", dob := i.dob]
  pm[, age := as.numeric(match_date - dob) / 365.25][age < 14 | age > 50, age := NA]
  cv <- fit_curve(pm[match_date < CUT])
  if (is.null(cv)) cat(sprintf("%s: too few dated matches before %s; NO CURVE, ageing = off
", bk$label, format(CUT)))
  pm[, fa := curve_at(cv, age)]
  setorder(pm, player_id, match_date, match_id)
  pm[, idx := seq_len(.N), by = player_id]
  pm[, gap := c(NA, diff(as.numeric(match_date))), by = player_id]
  pm[, fwd10 := { cs <- cumsum(v); k <- pmin(.N, idx + 9L); (cs[k] - c(0, cs)[idx]) / (k - idx + 1L) }, by = player_id]
  pm[, c("p_off", "p_age") := {
    po <- pa <- numeric(.N); s <- 0; ww <- 0; ff <- 0
    for (i in seq_len(.N)) {
      if (i > 1L) { d <- exp(-gap[i] / cap$decay)
        s <- (s + v[i - 1L]) * d; ww <- (ww + 1) * d; ff <- (ff + fa[i - 1L]) * d }
      po[i] <- (s + cap$k * tg) / (ww + cap$k)
      pa[i] <- (s + fa[i] * ww - ff + cap$k * tg) / (ww + cap$k)
    }
    list(po, pa)
  }, by = player_id]
  d <- pm[idx >= 2L & match_date >= CUT]
  d[, band := fcase(is.na(age), "no dob", age < 24, "under 24", age < 34, "24-33", default = "34+")]
  one <- function(x, lab) data.table(bucket = bk$label, cut = lab, n = nrow(x),
    off10 = sp(x$p_off, x$fwd10), age10 = sp(x$p_age, x$fwd10),
    off1 = sp(x$p_off, x$v), age1 = sp(x$p_age, x$v),
    actual = mean(x$v), pred_off = mean(x$p_off), pred_age = mean(x$p_age))
  fwrite(rbind(one(d, "ALL"), rbindlist(lapply(c("under 24", "24-33", "34+", "no dob"),
    function(b) one(d[band == b], b)))), res_file)
  cat(sprintf("%s done (%.1f min; dob on %.0f%% of scored matches)\n", bk$label,
              as.numeric(difftime(Sys.time(), t0, units = "mins")), 100 * mean(!is.na(d$age))))
  rm(pm, d); gc(verbose = FALSE)
}
res <- rbindlist(lapply(list.files(RES_DIR, "[.]csv$", full.names = TRUE), fread))
num <- c("off10", "age10", "off1", "age1", "actual", "pred_off", "pred_age")
res[, (num) := lapply(.SD, round, 4), .SDcols = num]
res[, gain10_pct := round(100 * (age10 - off10) / abs(off10), 1)]
options(width = 220)
cat("\nNext-10 / next-1 Spearman on matches from", format(CUT), "on; higher is better.",
    "actual / pred_* = mean next-match value vs mean prediction.\n")
print(res, nrows = 200)
cat(sprintf("\nTotal %.1f min\n", as.numeric(difftime(Sys.time(), t0, units = "mins"))))
