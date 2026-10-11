# Ageing curves for player rating v2 (Pete, 2026-10-11): should a young
# player's old matches count for more, and an old player's for less, by how
# much they have aged since? Step 1 of the model-design walk: fit the curve and
# show real players BEFORE scoring anything.
#
# Curve: v_ij = a_i + f(age_ij) + e, fitted by alternating means (player
# effect, then age effect), so each player is compared only with himself.
# Comparing across players instead would be dominated by survivorship: the
# players still around at 36 are the ones who were good enough to stay.
# The within-player fit reduces that bias but does not remove it: a player who
# declines and is dropped contributes no decline years.
# Read-only on bouncer.duckdb.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
# cricsheet.players.dob is empty; dates of birth come from the player_details
# release (the same file the blog's enrichment reads), keyed on player_id.
PD <- Sys.getenv("PLAYER_DETAILS", "C:/Users/peteo/AppData/Local/Temp/claude/C--dev/8edabc93-6381-43de-b41b-e2fda0fc6666/scratchpad/pd/bouncer_player_details.parquet")
stopifnot(file.exists(PD))
ppl <- as.data.table(dbGetQuery(conn,
  "SELECT player_id, ANY_VALUE(player_name) AS player_name FROM cricsheet.players GROUP BY player_id"))
det <- as.data.table(arrow::read_parquet(PD, col_select = c("player_id", "dob")))
det[, dob := as.Date(dob)]
ppl[unique(det[!is.na(dob)], by = "player_id"), on = "player_id", dob := i.dob]
cat(sprintf("players registry: %d, with dob: %d\n", nrow(ppl), sum(!is.na(ppl$dob))))

trace("calculate_player_rating_v2", where = asNamespace("bouncer"), print = FALSE,
      at = which(vapply(as.list(body(calculate_player_rating_v2)),
                        function(e) any(grepl("(w, exp(", deparse(e), fixed = TRUE)), logical(1))),
      tracer = quote(assign(".cap", list(pm = data.table::copy(pm), k = prior_matches,
        decay = decay_days, ref = ref_date, target = target), envir = globalenv())))

fit_curve <- function(pm, ages = 16:45, min_n = 200L, iters = 30L) {
  d <- pm[!is.na(age)][, ab := pmin(pmax(floor(age), min(ages)), max(ages))]
  d[, f := 0]
  for (it in seq_len(iters)) {
    d[, a := mean(v - f), by = player_id]
    fa <- d[, .(f = mean(v - a), n = .N, players = uniqueN(player_id)), by = ab]
    fa[, f := f - fa[ab == 27, f]]                       # anchor: 0 at age 27
    d[fa, on = "ab", f := i.f]
  }
  fa <- fa[order(ab)]
  # Smooth across neighbouring ages, weighted by matches, so a thin year cannot
  # make a spike; ages with < min_n matches take the smoothed value only.
  fa[, f_s := vapply(ab, function(x) { w <- fa[abs(ab - x) <= 2]; sum(w$f * w$n) / sum(w$n) }, 0)]
  fa[, thin := n < min_n]
  fa[]
}
curve_at <- function(cv, age) {
  a <- pmin(pmax(age, min(cv$ab)), max(cv$ab[!cv$thin]))   # clamp at the last well-measured age
  approx(cv$ab, cv$f_s, xout = a, rule = 2)$y
}

buckets <- list(
  list(label = "t20-male batter", format = "t20", role = "batter", mtf = NULL,
       show = c("Suryavanshi", "V Kohli", "KL Rahul", "Abhishek Sharma", "DA Warner")),
  list(label = "test-male bowler", format = "test", role = "bowler", mtf = NULL,
       show = c("JM Anderson", "Herath", "JJ Bumrah", "NM Lyon", "S Joseph")))
options(width = 200)
for (bk in buckets) {
  cat("\n==========", bk$label, "==========\n")
  r <- suppressMessages(get("calculate_player_rating_v2", asNamespace("bouncer"))(
    bk$format, "male", role = bk$role, conn = conn, match_type_filter = bk$mtf))
  cap <- get(".cap", globalenv()); pm <- cap$pm
  pm[ppl, on = "player_id", dob := i.dob]
  pm[, age := as.numeric(match_date - dob) / 365.25]
  pm[age < 14 | age > 50, age := NA]                    # registry typos
  cat(sprintf("player-matches with an age: %d of %d (%.1f%%); rated players with a dob: %d of %d\n",
              sum(!is.na(pm$age)), nrow(pm), 100 * mean(!is.na(pm$age)),
              sum(r$player_id %in% ppl[!is.na(dob), player_id]), nrow(r)))
  cv <- fit_curve(pm)
  cat("\nAgeing curve: expected per-match value at each age relative to age 27, same player (raw / smoothed).\n")
  print(cv[, .(age = ab, raw = round(f, 2), smoothed = round(f_s, 2), matches = n, players, thin)])

  # Ratings as at ref_date with every past match moved to the player's age now.
  pm[, w := exp(-as.numeric(cap$ref - match_date) / cap$decay)]
  pm[, age_now := as.numeric(cap$ref - dob) / 365.25]
  pm[, shift := fifelse(is.na(age), 0, curve_at(cv, age_now) - curve_at(cv, age))]
  rt <- pm[, .(base = (sum(v * w) + cap$k * cap$target) / (sum(w) + cap$k),
               aged = (sum((v + shift) * w) + cap$k * cap$target) / (sum(w) + cap$k),
               age_now = age_now[1], last = max(match_date),
               shift_w = sum(shift * w) / sum(w), eff = sum(w)), by = player_id][player_id %in% r$player_id]
  stopifnot(max(abs(rt[r, on = "player_id", base - i.rating])) < 1e-9)   # anchor on production
  rt[, `:=`(rk_base = frank(-base), rk_aged = frank(-aged))]
  rt[ppl, on = "player_id", player := i.player_name]
  cat("\nWalk-through players (rating per match; rank among", nrow(rt), "listed):\n")
  ex <- rt[Reduce(`|`, lapply(bk$show, function(s) grepl(s, player, fixed = TRUE)))]
  young <- rt[!is.na(age_now) & age_now < 23][order(rk_base)][1]
  print(rbind(ex, young)[, .(player, age_now = round(age_now, 1), yrs_since = round(as.numeric(cap$ref - last) / 365.25, 1),
                             eff = round(eff, 1), avg_shift = round(shift_w, 2), base = round(base, 2), rk_base,
                             aged = round(aged, 2), rk_aged)])
  cat("\nTop-20 overlap base vs aged:", rt[rk_base <= 20 & rk_aged <= 20, .N], "of 20\n")
  cat("Biggest movers among the top 50:\n")
  print(rt[rk_base <= 50 | rk_aged <= 50][order(-abs(rk_aged - rk_base))][1:8,
        .(player, age_now = round(age_now, 1), rk_base, rk_aged, avg_shift = round(shift_w, 2))])
}
