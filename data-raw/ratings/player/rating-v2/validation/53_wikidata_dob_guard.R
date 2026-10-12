# Does adding Wikidata dates of birth (gap-fill behind cricinfo) keep or improve
# next-10 Spearman? Same harness as 52_ageing_guard.R, three predictions per
# delivery-match: no ageing; two-part ageing with cricinfo dates only (as
# shipped in 0.8.0); two-part ageing with cricinfo + Wikidata. Each version
# fits its own curve on its own dated players before CUT, and applies the
# rating's date checks (age 10-60 at every match; Wikidata debut age 14-42).
#
# Read-only. DB_PATH defaults to the 2026-10-12 backup so it can run while the
# live DB is locked by an ingest. Each bucket is saved as it finishes.
t0 <- Sys.time()
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer-wt-dob", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
CUT <- as.Date("2018-01-01")
DB  <- Sys.getenv("DB_PATH", "C:/dev/bouncerverse/bouncerdata/bouncer_backup_2026_10_12.duckdb")
WD  <- "C:/dev/bouncerverse/bouncerdata/source/wikidata_cricinfo_dob.csv"
OUT <- Sys.getenv("WD_GUARD_OUT", file.path(tempdir(), "wikidata_guard"))
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
conn <- dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

# the real build_player_dob(), run in memory on copies of its two input tables
mem <- dbConnect(duckdb::duckdb())
dbExecute(mem, "CREATE SCHEMA cricsheet; CREATE SCHEMA cricinfo")
dbWriteTable(mem, Id(schema = "cricsheet", table = "people_register"),
             dbGetQuery(conn, "SELECT identifier, key_cricinfo, key_cricinfo_2, key_cricinfo_3 FROM cricsheet.people_register"))
dbWriteTable(mem, Id(schema = "cricinfo", table = "innings"),
             dbGetQuery(conn, "SELECT DISTINCT player_id, player_dob FROM cricinfo.innings WHERE player_dob IS NOT NULL"))
dobs <- as.data.table(suppressMessages(build_player_dob(mem, wikidata_path = WD)))
dbDisconnect(mem, shutdown = TRUE)
print(dobs[, .N, by = dob_source])

young <- get(".ageing_young", asNamespace("bouncer")); full <- get(".ageing_full", asNamespace("bouncer"))
trace("calculate_player_rating_v2", where = asNamespace("bouncer"), print = FALSE,
      at = which(vapply(as.list(body(calculate_player_rating_v2)),
                        function(e) any(grepl("(w, exp(", deparse(e), fixed = TRUE)), logical(1))),
      tracer = quote(assign(".cap", list(pm = data.table::copy(pm), k = prior_matches,
        decay = decay_days, ref = ref_date, target = target), envir = globalenv())))

buckets <- list()
for (fg in list(c("t20","male",""),c("t20","female",""),c("odi","male",""),c("odi","female",""),c("test","male",""),c("test","male","test")))
  for (ro in c("batter","bowler"))
    buckets[[length(buckets)+1]] <- list(label = paste0(fg[1], if (nzchar(fg[3])) "_intl" else "", "-", fg[2], " ", ro),
      format = fg[1], gender = fg[2], role = ro, mtf = if (nzchar(fg[3])) fg[3] else NULL)
sp <- function(x, y) cor(x, y, method = "spearman")

# causal two-part prediction for one set of ages (NA age = not aged)
predict_two <- function(pm, cv, agecol) {
  pm[, a := get(agecol)]
  pm[, fy := young(cv, a)]
  pm[, pr := {
    pt <- numeric(.N); s <- 0; ww <- 0; fs <- 0
    for (i in seq_len(.N)) {
      if (i > 1L) { d <- exp(-gap[i] / dec); s <- (s + v[i - 1L]) * d; ww <- (ww + 1) * d
                    fs <- (fs + (if (is.na(fy[i - 1L])) 0 else fy[i - 1L])) * d }
      a_last <- if (i > 1L) a[i - 1L] else NA_real_
      # an undated player gets shift 0 at every match: base = plain weighted mean
      base <- if (is.na(a_last)) (s + k * tg) / (ww + k) else (s + young(cv, a_last) * ww - fs + k * tg) / (ww + k)
      pt[i] <- base + if (!is.na(a_last) && !is.na(a[i])) full(cv, a[i]) - full(cv, a_last) else 0
    }
    pt
  }, by = player_id]
  out <- pm$pr; pm[, c("a", "fy", "pr") := NULL]; out
}

for (bk in buckets) {
  f_out <- file.path(OUT, paste0(gsub("[^a-z0-9]+", "_", bk$label), ".csv"))
  if (file.exists(f_out)) next
  invisible(suppressMessages(get("calculate_player_rating_v2", asNamespace("bouncer"))(
    ageing = "none", bk$format, bk$gender, role = bk$role, conn = conn, match_type_filter = bk$mtf)))
  cap <- get(".cap", globalenv()); pm <- cap$pm; tg <- cap$target; k <- cap$k; dec <- cap$decay
  pm[dobs, on = "player_id", `:=`(dob = as.Date(i.dob), src = i.dob_source)]
  pm[is.na(src), src := "none"]
  # the rating's date checks
  sp_ <- pm[, .(first = min(match_date), last = max(match_date)), by = .(player_id, dob, src)]
  bad <- sp_[!is.na(dob) & (as.numeric(first - dob) / 365.25 < 10 | as.numeric(last - dob) / 365.25 > 60 |
             (src == "wikidata" & (as.numeric(first - dob) / 365.25 < 14 | as.numeric(first - dob) / 365.25 > 42))), player_id]
  pm[player_id %in% bad, `:=`(dob = as.Date(NA), src = "none")]
  pm[, age_all := as.numeric(match_date - dob) / 365.25]
  pm[, age_ci := fifelse(src == "wikidata", NA_real_, age_all)]
  cv_ci  <- fit_ageing_curve(pm[!is.na(age_ci)  & match_date < CUT][, age := age_ci])
  cv_all <- fit_ageing_curve(pm[!is.na(age_all) & match_date < CUT][, age := age_all])
  if (is.null(cv_ci) || is.null(cv_all)) { cat(bk$label, ": no curve\n"); next }
  setorder(pm, player_id, match_date, match_id)
  pm[, `:=`(idx = seq_len(.N), gap = c(NA, diff(as.numeric(match_date)))), by = player_id]
  pm[, fwd10 := { cs <- cumsum(v); kk <- pmin(.N, idx + 9L); (cs[kk] - c(0, cs)[idx]) / (kk - idx + 1L) }, by = player_id]
  pm[, p_off := {
    po <- numeric(.N); s <- 0; ww <- 0
    for (i in seq_len(.N)) { if (i > 1L) { d <- exp(-gap[i] / dec); s <- (s + v[i - 1L]) * d; ww <- (ww + 1) * d }
                             po[i] <- (s + k * tg) / (ww + k) }
    po }, by = player_id]
  pm[, p_ci := predict_two(pm, cv_ci, "age_ci")]
  pm[, p_all := predict_two(pm, cv_all, "age_all")]
  g <- pm[idx >= 2L & match_date >= CUT]
  cuts <- list(ALL = g, `newly dated (wikidata)` = g[src == "wikidata"], `still undated` = g[src == "none"])
  res <- rbindlist(lapply(names(cuts), function(nm) cuts[[nm]][, .(cut = nm, n = .N, players = uniqueN(player_id),
    off = sp(p_off, fwd10), cricinfo_only = sp(p_ci, fwd10), with_wikidata = sp(p_all, fwd10))]))
  res[, `:=`(bucket = bk$label, peak_ci = cv_ci$peak, peak_all = cv_all$peak, wd_dropped = sum(sp_$src == "wikidata" & sp_$player_id %in% bad))]
  fwrite(res, f_out)
  cat(sprintf("%s done (%.1f min)\n", bk$label, as.numeric(difftime(Sys.time(), t0, units = "mins"))))
  rm(pm, g); gc(verbose = FALSE)
}
res <- rbindlist(lapply(list.files(OUT, "[.]csv$", full.names = TRUE), fread))
options(width = 220)
cat("\nNext-10 Spearman from", format(CUT), "(higher is better):\n")
print(res[, .(bucket, cut, n, players, peak_ci, peak_all, wd_dropped, off = round(off, 4),
              cricinfo_only = round(cricinfo_only, 4), with_wikidata = round(with_wikidata, 4))])
cat(sprintf("\nTotal %.1f min\n", as.numeric(difftime(Sys.time(), t0, units = "mins"))))
