# Guard check for the shipped ageing code (D-P69) on all 12 buckets: does the
# two-part adjustment, as implemented in R/ageing.R (not the experiment
# copies in 49-51), keep or improve next-10 Spearman for players who play on?
# The after-career part cannot be scored here (retirees never play a next
# match); this only checks it does not hurt the players who do.
#
# Same rules as production: real dates from main.player_dob, estimated from
# the bucket's median debut age otherwise, curve fitted on real dates only.
# Leak control: curve fitted before CUT, scored from CUT. Each bucket is
# saved as it finishes and skipped on a rerun. Read-only on bouncer.duckdb.
t0 <- Sys.time()
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
CUT <- as.Date("2018-01-01")
OUT <- Sys.getenv("AGEING_GUARD_OUT", file.path(tempdir(), "ageing_guard"))
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
conn <- get_db_connection(read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
dobs <- as.data.table(dbGetQuery(conn, "SELECT player_id, dob FROM main.player_dob"))[, dob := as.Date(dob)]
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
for (bk in buckets) {
  f_out <- file.path(OUT, paste0(gsub("[^a-z0-9]+", "_", bk$label), ".csv"))
  if (file.exists(f_out)) next
  invisible(suppressMessages(get("calculate_player_rating_v2", asNamespace("bouncer"))(
    ageing = "none", bk$format, bk$gender, role = bk$role, conn = conn, match_type_filter = bk$mtf)))
  cap <- get(".cap", globalenv()); pm <- cap$pm; tg <- cap$target; k <- cap$k; dec <- cap$decay
  pm[dobs, on = "player_id", dob := i.dob]
  pm[, real := !is.na(dob)]
  pm[, first := min(match_date), by = player_id]
  da <- unique(pm[real == TRUE, .(player_id, a0 = as.numeric(first - dob) / 365.25)])[a0 >= 14 & a0 <= 45, median(a0)]
  pm[real == FALSE, dob := first - round(da * 365.25)]
  pm[, age := as.numeric(match_date - dob) / 365.25][age < 10 | age > 60, age := NA]
  cv <- fit_ageing_curve(pm[real == TRUE & match_date < CUT])
  if (is.null(cv)) { cat(bk$label, ": no curve before", format(CUT), "\n"); next }
  setorder(pm, player_id, match_date, match_id)
  # as shipped: estimated ages get no rising-part credit
  pm[, `:=`(fy = fifelse(real, young(cv, age), 0), idx = seq_len(.N), gap = c(NA, diff(as.numeric(match_date)))), by = player_id]
  pm[, fwd10 := { cs <- cumsum(v); kk <- pmin(.N, idx + 9L); (cs[kk] - c(0, cs)[idx]) / (kk - idx + 1L) }, by = player_id]
  pm[, c("p_off", "p_two") := {
    po <- pt <- numeric(.N); s <- 0; ww <- 0; fs <- 0
    for (i in seq_len(.N)) {
      if (i > 1L) { d <- exp(-gap[i] / dec); s <- (s + v[i - 1L]) * d; ww <- (ww + 1) * d; fs <- (fs + fy[i - 1L]) * d }
      po[i] <- (s + k * tg) / (ww + k)
      a_last <- if (i > 1L) age[i - 1L] else NA_real_
      base <- (s + (if (is.na(a_last) || !real[i]) 0 else young(cv, a_last)) * ww - fs + k * tg) / (ww + k)
      pt[i] <- base + if (!is.na(a_last) && !is.na(age[i])) full(cv, age[i]) - full(cv, a_last) else 0
    }
    list(po, pt)
  }, by = player_id]
  g <- pm[idx >= 2L & match_date >= CUT]
  res <- rbind(g[, .(cut = "ALL", n = .N, off = sp(p_off, fwd10), two_part = sp(p_two, fwd10))],
               g[real == FALSE, .(cut = "estimated age", n = .N, off = sp(p_off, fwd10), two_part = sp(p_two, fwd10))],
               g[!is.na(age) & age >= 34, .(cut = "34+", n = .N, off = sp(p_off, fwd10), two_part = sp(p_two, fwd10))],
               g[!is.na(age) & age < 24, .(cut = "under 24", n = .N, off = sp(p_off, fwd10), two_part = sp(p_two, fwd10))])
  res[, `:=`(bucket = bk$label, peak = cv$peak)]
  fwrite(res, f_out)
  cat(sprintf("%s done (%.1f min, peak %d)\n", bk$label, as.numeric(difftime(Sys.time(), t0, units = "mins")), cv$peak))
  rm(pm, g); gc(verbose = FALSE)
}
res <- rbindlist(lapply(list.files(OUT, "[.]csv$", full.names = TRUE), fread))
res[, pct := round(100 * (two_part - off) / abs(off), 1)]
options(width = 200)
cat("\nNext-10 Spearman from", format(CUT), "(higher is better):\n")
print(res[, .(bucket, cut, n, peak, off = round(off, 4), two_part = round(two_part, 4), pct)])
cat(sprintf("\nTotal %.1f min\n", as.numeric(difftime(Sys.time(), t0, units = "mins"))))
