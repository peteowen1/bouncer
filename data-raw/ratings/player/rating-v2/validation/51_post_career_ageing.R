# Two-part age adjustment and rating histories (Pete, 2026-10-11).
#
#   rating(t) = base(t) + post(t)
#   base(t)   production rating at t (decay, debut-level shrinkage), with every
#             match moved along the YOUNG-ONLY curve to his age at his last match
#             before t: players improve to their peak, nobody declines while
#             still being picked (the tests showed active older players hold up)
#   post(t)   f_ext(age at t) - f_ext(age at last match): the full decline from
#             his last match to t. ~0 for an active player; grows after he stops.
#
# f_ext is the within-player curve fitted on ALL competitions in the bucket
# (domestic cricket keeps players into their 40s), smoothed, and past the last
# well-measured age EXTENDED as a downward parabola from the peak. The extension
# is an ASSUMPTION, not a measurement. post(t) sits outside the shrinkage, so an
# unknown 80-year-old is debut level plus 50+ years of decline: awful.
#
# Outputs: the curve, rating histories on a fixed monthly grid (1st of each
# month, panna#127's lesson), current ranks with and without, and a guard check
# that next-match Spearman for active players is not hurt.
# Read-only on bouncer.duckdb.
t0 <- Sys.time()
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
CUT <- as.Date("2018-01-01")
OUT <- Sys.getenv("POST_AGE_OUT", "C:/Users/peteo/AppData/Local/Temp/claude/C--dev/8edabc93-6381-43de-b41b-e2fda0fc6666/scratchpad/post_age")
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
PD <- Sys.getenv("PLAYER_DETAILS", "C:/Users/peteo/AppData/Local/Temp/claude/C--dev/8edabc93-6381-43de-b41b-e2fda0fc6666/scratchpad/pd/bouncer_player_details.parquet")
PEOPLE <- "C:/dev/bouncerverse/bouncerdata/source/cricsheet_people.csv"
stopifnot(file.exists(PD), file.exists(PEOPLE))

# ---- dates of birth: cricinfo first, then details (as 50_ageing_score.R) ----
parse_ci_dob <- function(x) {
  js <- jsonlite::parse_json(paste0("[", paste(gsub("None|NULL", "null", gsub("'", '"', x)), collapse = ","), "]"))
  as.Date(vapply(js, function(z)
    if (!is.null(z$year) && !is.null(z$month) && !is.null(z$date))
      sprintf("%04d-%02d-%02d", as.integer(z$year), as.integer(z$month), as.integer(z$date)) else NA_character_, ""))
}
ci <- unique(as.data.table(dbGetQuery(conn,
  "SELECT DISTINCT CAST(player_id AS VARCHAR) AS ci_id, player_dob FROM cricinfo.innings WHERE player_dob IS NOT NULL")), by = "ci_id")
ci[, dob := parse_ci_dob(player_dob)]
pp <- fread(PEOPLE, select = c("identifier", "key_cricinfo"))[!is.na(key_cricinfo)][, ci_id := as.character(key_cricinfo)]
dob_ci <- unique(merge(pp[, .(player_id = identifier, ci_id)], ci[!is.na(dob), .(ci_id, dob)], by = "ci_id"), by = "player_id")
dob_pd <- unique(as.data.table(arrow::read_parquet(PD, col_select = c("player_id", "dob")))[, dob := as.Date(dob)][!is.na(dob)], by = "player_id")
dobs <- rbind(dob_ci[, .(player_id, dob)], dob_pd[!player_id %in% dob_ci$player_id, .(player_id, dob)])
names_dt <- as.data.table(dbGetQuery(conn,
  "SELECT player_id, ANY_VALUE(player_name) AS player FROM cricsheet.players GROUP BY player_id"))

trace("calculate_player_rating_v2", where = asNamespace("bouncer"), print = FALSE,
      at = which(vapply(as.list(body(calculate_player_rating_v2)),
                        function(e) any(grepl("(w, exp(", deparse(e), fixed = TRUE)), logical(1))),
      tracer = quote(assign(".cap", list(pm = data.table::copy(pm), k = prior_matches,
        decay = decay_days, ref = ref_date, target = target), envir = globalenv())))

# ---- the curve ----------------------------------------------------------
fit_curve <- function(d, ages = 16:45, iters = 30L) {
  d <- d[!is.na(age)][, ab := pmin(pmax(floor(age), min(ages)), max(ages))]
  d[, f := 0]
  for (it in seq_len(iters)) {
    d[, a := mean(v - f), by = player_id]
    fa <- d[, .(f = mean(v - a), n = .N, players = uniqueN(player_id)), by = ab]
    fa[, f := f - fa[ab == 27, f]]
    d[fa, on = "ab", f := i.f]
  }
  fa <- fa[order(ab)]
  fa[, f_s := vapply(ab, function(x) { w <- fa[abs(ab - x) <= 2]; sum(w$f * w$n) / sum(w$n) }, 0)]
  fa[, thin := n < 200L]
  ok <- fa[!(thin)]
  peak <- ok[which.max(f_s), ab]; last <- max(ok$ab)
  post <- ok[ab > peak]
  # downward parabola from the peak, weighted by matches, through the measured
  # post-peak ages; used only beyond `last`, joined continuously there
  b <- if (nrow(post) >= 3) post[, sum(n * (ab - peak)^2 * (f_s - ok[ab == peak, f_s])) / sum(n * (ab - peak)^4)] else NA_real_
  if (!is.finite(b) || b >= 0) b <- -0.01 * abs(ok[ab == peak, f_s] - min(ok$f_s) + 1)   # never let it rise
  list(tab = fa, peak = peak, last = last, b = b, f_peak = ok[ab == peak, f_s], f_last = ok[ab == last, f_s],
       first = min(ok$ab))
}
f_young <- function(cv, age) {
  out <- rep(0, length(age)); ok <- !is.na(age)
  a <- pmin(pmax(age[ok], cv$first), cv$peak)
  out[ok] <- approx(cv$tab$ab, cv$tab$f_s, xout = a, rule = 2)$y
  out
}
f_ext <- function(cv, age) {
  out <- rep(0, length(age)); ok <- !is.na(age)
  a <- pmax(age[ok], cv$first)
  inside <- a <= cv$last
  o <- numeric(length(a))
  o[inside] <- approx(cv$tab$ab, cv$tab$f_s, xout = a[inside], rule = 2)$y
  o[!inside] <- cv$f_last + cv$b * ((a[!inside] - cv$peak)^2 - (cv$last - cv$peak)^2)
  out[ok] <- o
  out
}

# Rating at each date in `at` for ONE player's matches (sorted by date).
rate_path <- function(md, v, age_m, dob, at, cv, k, decay, tg) {
  vapply(seq_along(at), function(i) {
    j <- which(md <= at[i])
    if (!length(j)) return(c(NA, NA, NA, NA))
    w <- exp(-as.numeric(at[i] - md[j]) / decay)
    a_last <- age_m[max(j)]; a_now <- as.numeric(at[i] - dob) / 365.25
    shift <- if (is.na(a_last)) 0 else f_young(cv, a_last) - f_young(cv, age_m[j])
    shift[is.na(shift)] <- 0
    off <- (sum(v[j] * w) + k * tg) / (sum(w) + k)
    base <- (sum((v[j] + shift) * w) + k * tg) / (sum(w) + k)
    post <- if (is.na(a_last) || is.na(a_now)) 0 else f_ext(cv, a_now) - f_ext(cv, a_last)
    c(off, base, base + post, as.numeric(at[i] - md[max(j)]))
  }, numeric(4))
}

buckets <- list(
  list(label = "test-male bowler", format = "test", gender = "male", role = "bowler",
       show = c("JM Anderson", "R Ashwin", "N Wagner", "HMRKB Herath", "JJ Bumrah", "TJ Murtagh")),
  list(label = "odi-male batter", format = "odi", gender = "male", role = "batter",
       show = c("DA Warner", "V Kohli", "Mahmudullah", "DJ Malan")),
  list(label = "t20-male batter", format = "t20", gender = "male", role = "batter",
       show = c("Abhishek Sharma", "V Suryavanshi", "V Kohli", "KA Pollard")))
options(width = 220)
sp <- function(x, y) cor(x, y, method = "spearman")
for (bk in buckets) {
  cat("\n==========", bk$label, "==========\n")
  r <- suppressMessages(get("calculate_player_rating_v2", asNamespace("bouncer"))(
    ageing = "none", bk$format, bk$gender, role = bk$role, conn = conn))
  cap <- get(".cap", globalenv()); pm <- cap$pm; tg <- cap$target; k <- cap$k; dec <- cap$decay; ref <- cap$ref
  pm[dobs, on = "player_id", dob := i.dob]
  pm[, age := as.numeric(match_date - dob) / 365.25][age < 14 | age > 50, age := NA]
  setorder(pm, player_id, match_date, match_id)

  cv <- fit_curve(pm)
  cat(sprintf("curve on all competitions: peak %d, measured to %d, beyond that a parabola (b = %.4f per year^2)\n",
              cv$peak, cv$last, cv$b))
  ages <- c(20, 25, 27, 30, 33, 36, 39, 42, 45, 50, 60, 70, 80)
  print(data.table(age = ages, young_only = round(f_young(cv, ages), 2), full_extended = round(f_ext(cv, ages), 2),
                   measured = ages <= cv$last))

  # current ranks: every listed player at ref
  lst <- pm[player_id %in% r$player_id]
  now <- lst[, { x <- rate_path(match_date, v, age, dob[1], ref, cv, k, dec, tg)
                 .(off = x[1], base = x[2], new = x[3], days_since = x[4]) }, by = player_id]
  stopifnot(max(abs(now[r, on = "player_id", off - i.rating])) < 1e-9)          # anchor on production
  now[, `:=`(rk_off = frank(-off), rk_new = frank(-new))]
  now[names_dt, on = "player_id", player := i.player]
  cat("\nTop-20 overlap now vs new:", now[rk_off <= 20 & rk_new <= 20, .N], "of 20\n")
  cat("Players leaving the top 40 under the new rating:\n")
  print(now[rk_off <= 40 & rk_new > 40][order(rk_off), .(player, rk_off, rk_new, yrs_since = round(days_since / 365.25, 1))])

  # rating histories for the walk-through players, on the 1st of each month
  ids <- names_dt[player %in% bk$show, player_id]
  hist <- pm[player_id %in% ids, {
    grid <- seq(as.Date(format(min(match_date), "%Y-%m-01")), as.Date(format(ref, "%Y-%m-01")), by = "month")
    if (grepl("Herath", names_dt[player_id == .BY$player_id, player][1]) && !is.na(dob[1]))
      grid <- c(grid, seq(as.Date(format(ref, "%Y-%m-01")) + 31, dob[1] + round(80 * 365.25), by = "year"))
    x <- rate_path(match_date, v, age, dob[1], grid, cv, k, dec, tg)
    .(date = grid, age = round(as.numeric(grid - dob[1]) / 365.25, 1), off = x[1, ], base = x[2, ], new = x[3, ],
      days_since = x[4, ])
  }, by = player_id]
  hist[names_dt, on = "player_id", player := i.player]
  fwrite(hist, file.path(OUT, paste0(gsub("[^a-z0-9]+", "_", bk$label), "_history.csv")))
  cat("\nRating histories, yearly (each player's July, plus Herath on to 80). off = live rating, new = two-part age adjustment:\n")
  yr <- hist[format(date, "%m") == "07" | date > ref][, .(player, date, age, yrs_since = round(days_since / 365.25, 1),
                                                         off = round(off, 2), new = round(new, 2))]
  for (p in unique(yr$player)) { cat("--", p, "\n"); print(yr[player == p][seq(1, .N, by = max(1L, .N %/% 12L))], row.names = FALSE) }
  cat("\nNow:\n"); print(now[player_id %in% ids, .(player, off = round(off, 2), rk_off, new = round(new, 2), rk_new,
                                                   yrs_since = round(days_since / 365.25, 1))][order(rk_off)])

  # guard: next-match Spearman, curve fitted before CUT, scored from CUT
  cvg <- fit_curve(pm[match_date < CUT])
  pm[, `:=`(fy = f_young(cvg, age), idx = seq_len(.N), gap = c(NA, diff(as.numeric(match_date)))), by = player_id]
  pm[, fwd10 := { cs <- cumsum(v); kk <- pmin(.N, idx + 9L); (cs[kk] - c(0, cs)[idx]) / (kk - idx + 1L) }, by = player_id]
  pm[, c("p_off", "p_young", "p_new") := {
    po <- py <- pn <- numeric(.N); s <- 0; ww <- 0; fy_s <- 0
    for (i in seq_len(.N)) {
      if (i > 1L) { d <- exp(-gap[i] / dec); s <- (s + v[i - 1L]) * d; ww <- (ww + 1) * d; fy_s <- (fy_s + fy[i - 1L]) * d }
      po[i] <- (s + k * tg) / (ww + k)
      # young-only, referenced to his age at his previous match (his last match before i)
      a_last <- if (i > 1L) age[i - 1L] else NA
      ref_f <- if (is.na(a_last)) 0 else f_young(cvg, a_last)
      py[i] <- (s + ref_f * ww - fy_s + k * tg) / (ww + k)
      pn[i] <- py[i] + if (i > 1L && !is.na(a_last) && !is.na(age[i])) f_ext(cvg, age[i]) - f_ext(cvg, a_last) else 0
    }
    list(po, py, pn)
  }, by = player_id]
  g <- pm[idx >= 2L & match_date >= CUT]
  g[, band := fcase(is.na(age), "no age", age < 34, "under 34", default = "34+")]
  gg <- rbind(g[, .(cut = "ALL", n = .N, off = sp(p_off, fwd10), young = sp(p_young, fwd10), young_plus_post = sp(p_new, fwd10))],
              g[, .(n = .N, off = sp(p_off, fwd10), young = sp(p_young, fwd10), young_plus_post = sp(p_new, fwd10)), by = .(cut = band)],
              g[gap > 365, .(cut = "back after 1y+ gap", n = .N, off = sp(p_off, fwd10), young = sp(p_young, fwd10), young_plus_post = sp(p_new, fwd10))])
  cat("\nGuard: next-10 Spearman from", format(CUT), "(higher is better):\n"); print(gg[, lapply(.SD, function(x) if (is.numeric(x)) round(x, 4) else x)])
  rm(pm, g, lst); gc(verbose = FALSE)
}
cat(sprintf("\nTotal %.1f min\n", as.numeric(difftime(Sys.time(), t0, units = "mins"))))
