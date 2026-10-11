# Score the ageing adjustment (Pete, 2026-10-11) on next-match Spearman across
# all 12 published buckets. Curve shape and walk-through: 49_ageing_curves.R.
#
# Dates of birth, in order of trust:
#   1. cricinfo.innings.player_dob, keyed through cricsheet's own id crosswalk
#      (cricsheet_people.csv key_cricinfo). Where both sources exist it matches
#      the details file exactly for 2,685 of 2,728 players; the 39 that differ
#      by over a year look like same-name mix-ups in the details file.
#   2. the player_details release (bouncer_player_details.parquet).
#   3. ESTIMATED: first match date minus the bucket's median debut age among
#      dated players. So no one is skipped and undated young players are not
#      left behind while dated peers get the boost.
#
# Variants scored against "off":
#   full   every match moved along the whole curve (rise and decline)
#   young  the curve is held flat after its peak: players improve, nobody declines
#
# Leak control: curves fitted ONLY on matches before CUT (and only on real
# dates); predictions scored ONLY on matches from CUT on, each using all
# strictly earlier matches. Each bucket is saved as it finishes and skipped on
# a rerun. Read-only on bouncer.duckdb.
t0 <- Sys.time()
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
CUT <- as.Date("2018-01-01")
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
PD <- Sys.getenv("PLAYER_DETAILS", "C:/Users/peteo/AppData/Local/Temp/claude/C--dev/8edabc93-6381-43de-b41b-e2fda0fc6666/scratchpad/pd/bouncer_player_details.parquet")
PEOPLE <- "C:/dev/bouncerverse/bouncerdata/source/cricsheet_people.csv"
stopifnot(file.exists(PD), file.exists(PEOPLE))
RES_DIR <- Sys.getenv("AGEING_OUT", file.path(dirname(PD), "ageing_score2"))
dir.create(RES_DIR, showWarnings = FALSE, recursive = TRUE)

# ---- dates of birth -------------------------------------------------------
parse_ci_dob <- function(x) {
  js <- jsonlite::parse_json(paste0("[", paste(gsub("None|NULL", "null", gsub("'", '"', x)), collapse = ","), "]"))
  as.Date(vapply(js, function(z)
    if (!is.null(z$year) && !is.null(z$month) && !is.null(z$date))
      sprintf("%04d-%02d-%02d", as.integer(z$year), as.integer(z$month), as.integer(z$date)) else NA_character_, ""))
}
ci <- unique(as.data.table(dbGetQuery(conn,
  "SELECT DISTINCT CAST(player_id AS VARCHAR) AS ci_id, player_dob FROM cricinfo.innings WHERE player_dob IS NOT NULL")), by = "ci_id")
ci[, dob := parse_ci_dob(player_dob)]
pp <- fread(PEOPLE, select = c("identifier", "key_cricinfo"))[!is.na(key_cricinfo)]
pp[, ci_id := as.character(key_cricinfo)]
dob_ci <- unique(merge(pp[, .(player_id = identifier, ci_id)], ci[!is.na(dob), .(ci_id, dob)], by = "ci_id"), by = "player_id")
dob_pd <- unique(as.data.table(arrow::read_parquet(PD, col_select = c("player_id", "dob")))[, dob := as.Date(dob)][!is.na(dob)], by = "player_id")
dobs <- rbind(dob_ci[, .(player_id, dob, src = "cricinfo")],
              dob_pd[!player_id %in% dob_ci$player_id, .(player_id, dob, src = "details")])
cat(sprintf("dates of birth: %d from cricinfo, %d from details only\n", dobs[src == "cricinfo", .N], dobs[src == "details", .N]))

trace("calculate_player_rating_v2", where = asNamespace("bouncer"), print = FALSE,
      at = which(vapply(as.list(body(calculate_player_rating_v2)),
                        function(e) any(grepl("(w, exp(", deparse(e), fixed = TRUE)), logical(1))),
      tracer = quote(assign(".cap", list(pm = data.table::copy(pm), k = prior_matches,
        decay = decay_days, ref = ref_date, target = target), envir = globalenv())))

fit_curve <- function(d, ages = 16:45, iters = 30L) {
  d <- d[!is.na(age)][, ab := pmin(pmax(floor(age), min(ages)), max(ages))]
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
curve_at <- function(cv, age, young = FALSE) {
  out <- rep(0, length(age)); ok <- !is.na(age)
  if (is.null(cv) || !any(!cv$thin)) return(out)
  ok_ab <- cv$ab[!cv$thin]
  hi <- if (young) cv[!(thin)][which.max(f_s), ab] else max(ok_ab)   # young: flat after the peak
  a <- pmin(pmax(age[ok], min(ok_ab)), hi)
  out[ok] <- approx(cv$ab, cv$f_s, xout = a, rule = 2)$y
  out
}

buckets <- list()
for (fg in list(c("t20","male",""),c("t20","female",""),c("odi","male",""),c("odi","female",""),c("test","male",""),c("test","male","test")))
  for (ro in c("batter","bowler"))
    buckets[[length(buckets)+1]] <- list(label = paste0(fg[1], if (nzchar(fg[3])) "_intl" else "", "-", fg[2], " ", ro),
      format = fg[1], gender = fg[2], role = ro, mtf = if (nzchar(fg[3])) fg[3] else NULL)
sp <- function(x, y) if (length(x) > 30) cor(x, y, method = "spearman") else NA_real_

for (bk in buckets) {
  res_file <- file.path(RES_DIR, paste0(gsub("[^a-z0-9]+", "_", bk$label), ".csv"))
  if (file.exists(res_file)) { cat(bk$label, "already scored, skipping\n"); next }
  r <- suppressMessages(get("calculate_player_rating_v2", asNamespace("bouncer"))(
    bk$format, bk$gender, role = bk$role, conn = conn, match_type_filter = bk$mtf))
  cap <- get(".cap", globalenv()); pm <- cap$pm; tg <- cap$target
  pm[dobs, on = "player_id", `:=`(dob = i.dob, src = i.src)]
  pm[, first := min(match_date), by = player_id]
  # estimated dob from the median debut age of dated players in this bucket
  deb <- unique(pm[!is.na(dob), .(player_id, a0 = as.numeric(first - dob) / 365.25)])[a0 >= 14 & a0 <= 45]
  med_debut <- median(deb$a0)
  pm[is.na(dob), `:=`(dob = first - round(med_debut * 365.25), src = "estimated")]
  pm[, age := as.numeric(match_date - dob) / 365.25][age < 14 | age > 50, age := NA]
  cv <- fit_curve(pm[match_date < CUT & src != "estimated"])     # curve from real dates only
  if (is.null(cv)) cat(sprintf("%s: too few dated matches before %s; NO CURVE\n", bk$label, format(CUT)))
  pm[, `:=`(f_full = curve_at(cv, age), f_young = curve_at(cv, age, young = TRUE))]
  setorder(pm, player_id, match_date, match_id)
  pm[, idx := seq_len(.N), by = player_id]
  pm[, gap := c(NA, diff(as.numeric(match_date))), by = player_id]
  pm[, fwd10 := { cs <- cumsum(v); k <- pmin(.N, idx + 9L); (cs[k] - c(0, cs)[idx]) / (k - idx + 1L) }, by = player_id]
  pm[, c("p_off", "p_full", "p_young") := {
    po <- pf <- py <- numeric(.N); s <- 0; ww <- 0; ff <- 0; fy <- 0
    for (i in seq_len(.N)) {
      if (i > 1L) { d <- exp(-gap[i] / cap$decay)
        s <- (s + v[i - 1L]) * d; ww <- (ww + 1) * d
        ff <- (ff + f_full[i - 1L]) * d; fy <- (fy + f_young[i - 1L]) * d }
      po[i] <- (s + cap$k * tg) / (ww + cap$k)
      pf[i] <- (s + f_full[i] * ww - ff + cap$k * tg) / (ww + cap$k)
      py[i] <- (s + f_young[i] * ww - fy + cap$k * tg) / (ww + cap$k)
    }
    list(po, pf, py)
  }, by = player_id]
  d <- pm[idx >= 2L & match_date >= CUT]
  d[, band := fcase(age < 24, "under 24", age < 34, "24-33", default = "34+")]
  one <- function(x, lab) data.table(bucket = bk$label, cut = lab, n = nrow(x),
    off10 = sp(x$p_off, x$fwd10), full10 = sp(x$p_full, x$fwd10), young10 = sp(x$p_young, x$fwd10),
    off1 = sp(x$p_off, x$v), full1 = sp(x$p_full, x$v), young1 = sp(x$p_young, x$v),
    actual = mean(x$v), pred_off = mean(x$p_off), pred_full = mean(x$p_full), pred_young = mean(x$p_young))
  fwrite(rbind(one(d, "ALL"),
               rbindlist(lapply(c("under 24", "24-33", "34+"), function(b) one(d[band == b], b))),
               one(d[src == "estimated"], "estimated dob")), res_file)
  cat(sprintf("%s done (%.1f min; dob source on scored matches: %s; median debut age %.1f; curve peak %s)\n",
              bk$label, as.numeric(difftime(Sys.time(), t0, units = "mins")),
              paste(d[, .N, by = src][, sprintf("%s %.0f%%", src, 100 * N / sum(N))], collapse = ", "),
              med_debut, if (is.null(cv)) "none" else cv[!(thin)][which.max(f_s), ab]))
  rm(pm, d); gc(verbose = FALSE)
}
res <- rbindlist(lapply(list.files(RES_DIR, "[.]csv$", full.names = TRUE), fread))
num <- setdiff(names(res), c("bucket", "cut", "n"))
res[, (num) := lapply(.SD, round, 4), .SDcols = num]
res[, `:=`(full_pct = round(100 * (full10 - off10) / abs(off10), 1), young_pct = round(100 * (young10 - off10) / abs(off10), 1))]
options(width = 220)
cat("\nNext-10 Spearman on matches from", format(CUT), "on; higher is better.\n")
print(res[, .(bucket, cut, n, off10, full10, young10, full_pct, young_pct, actual, pred_off, pred_full, pred_young)], nrows = 200)
cat(sprintf("\nTotal %.1f min\n", as.numeric(difftime(Sys.time(), t0, units = "mins"))))
