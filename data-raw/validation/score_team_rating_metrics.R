# #61 arms 4 and 5: does a team rating built from RVAA or TSA beat the one
# built from RAA -- and does any of them beat the result-ELO?
#
# THE BAR, unchanged from the ticket: held-out match prediction, per format,
# with the MATCH as the independent unit, scored under BOTH targets:
#
#   who wins -- glm, log loss, against a base-rate constant
#   margin   -- lm, RMSE on unified_margin, against a mean constant
#
# WHY THESE ARMS. The rating tested so far is calculate_player_value_v2(),
# whose per-ball quantity is `raa - opponent_effect`: RUNS ONLY, no wicket
# term. In Test cricket wickets are the currency, so a bowling contribution
# credited purely through runs prevented may be measuring the wrong thing.
#   RVAA (metric "composite") = RAA + lambda*WAA, so wickets count directly.
#   TSA  (metric "team_score") = the ball's effect on the projected final
#        score, which prices a wicket by what it costs the innings.
#
# ADDITIVITY IS NOT SHARED ACROSS SOURCES, and this is the trap.
# calculate_player_value_v2() is contribution (quality x opportunity) and its
# own docs say the two components CAN be added. calculate_player_rating_v2() is
# per-role QUALITY and its @return says outright they "must not be added". So
# RAA enters as a summed total AND as a split; RVAA and TSA enter ONLY as a
# split, as two separate model features. Summing them would be a units error
# that still produces a number.
#
# TSA HAS NO TEST ARM. A Test innings has no fixed ball allocation, so a
# projected final score has no denominator (validation/30_tsa_persist.R). The
# arm is absent for Test rather than silently scored on nothing.
#
# ONE MATCH SET FOR EVERY ARM. Each source rates a different set of players, so
# scoring each arm on whatever matches it happens to cover would compare arms
# on different data and call the difference a result. Every arm here is fitted
# and scored on the intersection: matches where EVERY source rates at least
# MIN_RATED players a side.
#
# Usage: Rscript data-raw/validation/score_team_rating_metrics.R [t20 odi test]
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})

fmts <- commandArgs(trailingOnly = TRUE)
if (!length(fmts)) fmts <- c("t20", "odi", "test")
SEED <- 42
MIN_RATED <- 6L
BOOT <- 2000L

conn <- get_db_connection(read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)
types <- list(t20 = "'t20','it20'", odi = "'odi','odm'", test = "'test','mdm'")

# ---- per-side features from one snapshot source ------------------------------
# Returns one row per (match_id, team) with a batting part, a bowling part and
# a rated count, or NULL if the source has nothing for this format.
side_from_value <- function(fmt, app, scorable) {
  s <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT as_at, player_id, bat_value, bowl_value FROM main.player_value_v2_snapshots
    WHERE format = '%s' AND gender = 'male'", fmt)))
  if (!nrow(s)) return(NULL)
  dates <- sort(unique(as.Date(s$as_at)))
  a <- copy(app)
  a[, snap := pick_snapshot(match_date, dates)]
  a <- a[!is.na(snap)]
  a <- merge(a, s[, .(snap = as.Date(as_at), player_id, bat_value, bowl_value)],
             by = c("snap", "player_id"), all.x = TRUE)
  a[, debutant := is.na(bat_value) & is.na(bowl_value)]
  repl <- s[, .(rb = stats::quantile(bat_value, 0.10, na.rm = TRUE),
                rw = stats::quantile(bowl_value, 0.10, na.rm = TRUE)),
            by = .(snap = as.Date(as_at))]
  a <- merge(a, repl, by = "snap", all.x = TRUE)
  a[debutant == TRUE, `:=`(bat_value = rb, bowl_value = rw)]
  if (sum(is.na(a$bat_value) & is.na(a$bowl_value)) > 0) {
    cli::cli_abort("value: a fill left rows with no value; it covered a cause it was not meant to.")
  }
  a[, .(bat_part = sum(bat_value, na.rm = TRUE),
        bowl_part = sum(bowl_value, na.rm = TRUE),
        n_rated = sum(!debutant)), by = .(match_id, team)]
}

side_from_metric <- function(fmt, metric, app, scorable) {
  tbl <- paste0("player_metric_snapshots_", metric)
  if (!table_exists(conn, tbl)) return(NULL)
  s <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT as_at, role, player_id, rating FROM %s
    WHERE format = '%s'", tbl, fmt)))
  if (!nrow(s)) return(NULL)
  # Per role, NEVER summed -- the two roles are not on a common scale.
  w <- dcast(s, as_at + player_id ~ role, value.var = "rating",
             fun.aggregate = function(x) mean(x, na.rm = TRUE), fill = NA_real_)
  if (!all(c("batter", "bowler") %in% names(w))) return(NULL)
  dates <- sort(unique(as.Date(w$as_at)))
  a <- copy(app)
  a[, snap := pick_snapshot(match_date, dates)]
  a <- a[!is.na(snap)]
  a <- merge(a, w[, .(snap = as.Date(as_at), player_id, bat_r = batter, bowl_r = bowler)],
             by = c("snap", "player_id"), all.x = TRUE)
  a[, debutant := is.na(bat_r) & is.na(bowl_r)]
  repl <- w[, .(rb = stats::quantile(batter, 0.10, na.rm = TRUE),
                rw = stats::quantile(bowler, 0.10, na.rm = TRUE)),
            by = .(snap = as.Date(as_at))]
  a <- merge(a, repl, by = "snap", all.x = TRUE)
  # A player rated in one role only is NOT a debutant -- he is a specialist.
  # Filling only the missing role keeps him, and keeps the count honest.
  a[is.na(bat_r), bat_r := rb]
  a[is.na(bowl_r), bowl_r := rw]
  if (anyNA(a$bat_r) || anyNA(a$bowl_r)) {
    cli::cli_abort("{metric}: rows still unrated after the replacement fill.")
  }
  a[, .(bat_part = sum(bat_r, na.rm = TRUE),
        bowl_part = sum(bowl_r, na.rm = TRUE),
        n_rated = sum(!debutant)), by = .(match_id, team)]
}

for (fmt in fmts) {
  cli::cli_h1(toupper(fmt))

  m <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT m.match_id, CAST(m.match_date AS DATE) AS match_date,
           m.team1, m.team2, m.unified_margin, m.team_type, m.outcome_winner,
           (SELECT MIN(batting_team) FROM cricsheet.match_innings i
            WHERE i.match_id = m.match_id AND i.innings = 1) AS bat_first,
           (SELECT MAX(batting_team) FROM cricsheet.match_innings i
            WHERE i.match_id = m.match_id AND i.innings = 1) AS bat_first_max
    FROM cricsheet.matches m
    WHERE LOWER(m.match_type) IN (%s) AND m.gender = 'male'
      AND m.unified_margin IS NOT NULL AND m.unified_margin <> 0", types[[fmt]])))
  amb <- sum(m$bat_first != m$bat_first_max, na.rm = TRUE)
  if (amb > 0) cli::cli_abort("{amb} match{?es} have two first-innings batting teams; the margin sign would be arbitrary.")
  m[, bat_first_max := NULL]
  m <- m[!is.na(bat_first)]
  m[, chasing := fifelse(bat_first == team1, team2, team1)]
  scorable <- m

  app <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT s.match_id, s.team, s.player_id FROM main.match_squads s
    JOIN cricsheet.matches m ON m.match_id = s.match_id
    WHERE LOWER(m.match_type) IN (%s) AND m.gender = 'male'", types[[fmt]])))
  app <- app[match_id %in% scorable$match_id]
  if (!nrow(app)) { cli::cli_alert_warning("no squads for {fmt}"); next }
  id_map <- build_player_id_map(conn)
  canonicalise_player_ids(app, id_map)
  app <- merge(app, scorable[, .(match_id, match_date)], by = "match_id")

  sources <- list(RAA = function() side_from_value(fmt, app, scorable),
                  RVAA = function() side_from_metric(fmt, "composite", app, scorable),
                  TSA = function() side_from_metric(fmt, "team_score", app, scorable))
  sides <- list()
  for (nm in names(sources)) {
    s <- sources[[nm]]()
    if (is.null(s)) { cli::cli_alert_warning("{nm}: no snapshots for {fmt}, arm absent"); next }
    s <- s[n_rated >= MIN_RATED]
    sides[[nm]] <- s
    cli::cli_alert_info("{nm}: {format(nrow(s), big.mark=',')} sides pass the {MIN_RATED}-rated floor")
  }
  if (!"RAA" %in% names(sides)) { cli::cli_alert_warning("no RAA reference for {fmt}"); next }

  d <- scorable[, .(match_id, match_date, bat_first, chasing, unified_margin,
                    team_type, outcome_winner)]
  for (nm in names(sides)) {
    s <- sides[[nm]]
    bf <- s[, .(match_id, bat_first = team, bb = bat_part, bw = bowl_part)]
    ch <- s[, .(match_id, chasing  = team, cb = bat_part, cw = bowl_part)]
    setnames(bf, c("bb", "bw"), paste0(nm, c("_bf_bat", "_bf_bowl")))
    setnames(ch, c("cb", "cw"), paste0(nm, c("_ch_bat", "_ch_bowl")))
    d <- merge(d, bf, by = c("match_id", "bat_first"))
    d <- merge(d, ch, by = c("match_id", "chasing"))
    d[, (paste0(nm, "_bat_diff")) := get(paste0(nm, "_bf_bat")) - get(paste0(nm, "_ch_bat"))]
    d[, (paste0(nm, "_bowl_diff")) := get(paste0(nm, "_bf_bowl")) - get(paste0(nm, "_ch_bowl"))]
  }
  d[, RAA_diff := (RAA_bf_bat + RAA_bf_bowl) - (RAA_ch_bat + RAA_ch_bowl)]
  cli::cli_alert_info("{format(nrow(d), big.mark=',')} matches rated by EVERY source present ({paste(names(sides), collapse=', ')})")

  te <- as.data.table(dbGetQuery(conn, "
    SELECT match_id, team_id, elo_before FROM main.team_elo WHERE played_in_match"))
  d[, `:=`(bf_id = make_team_id_vec(bat_first, "male", fmt, team_type),
           ch_id = make_team_id_vec(chasing,  "male", fmt, team_type))]
  d <- merge(d, te[, .(match_id, bf_id = team_id, bf_elo = elo_before)],
             by = c("match_id", "bf_id"), all.x = TRUE)
  d <- merge(d, te[, .(match_id, ch_id = team_id, ch_elo = elo_before)],
             by = c("match_id", "ch_id"), all.x = TRUE)
  if (sum(!is.na(d$bf_elo) & !is.na(d$ch_elo)) == 0) {
    cli::cli_abort(c("No match joined an ELO -- the team id join found nothing.",
                     "i" = "Built: {.val {utils::head(d$bf_id, 1)}}",
                     "i" = "In team_elo: {.val {utils::head(te$team_id, 1)}}"))
  }
  d <- d[!is.na(bf_elo) & !is.na(ch_elo)]
  d[, elo_diff := bf_elo - ch_elo]
  d[, bf_won := as.integer(outcome_winner == bat_first)]
  d <- d[!is.na(bf_won)]

  # A seeded split needs a TOTAL order. match_date alone leaves ties, and the
  # row order DuckDB returns is not guaranteed stable between runs, so the same
  # seed would silently reproduce a different split.
  setorder(d, match_date, match_id)
  cut <- floor(0.8 * nrow(d))
  if (cut < 50 || nrow(d) - cut < 50) {
    cli::cli_alert_warning("{nrow(d)} matches is too few to split; skipping {fmt}"); next
  }
  tr <- d[1:cut]; te_set <- d[(cut + 1):nrow(d)]

  logloss <- function(p, y) { p <- pmin(pmax(p, 1e-15), 1 - 1e-15)
    -mean(y * log(p) + (1 - y) * log(1 - p)) }
  rmse <- function(p, a) sqrt(mean((p - a)^2))
  base_rate <- mean(tr$bf_won)
  ll_null <- logloss(rep(base_rate, nrow(te_set)), te_set$bf_won)
  rm_null <- rmse(rep(mean(tr$unified_margin), nrow(te_set)), te_set$unified_margin)

  # ARMS DECLARED BEFORE FITTING. RAA appears as a total and a split because it
  # is additive; RVAA and TSA only as splits because they are not.
  rhs <- list("ELO alone" = "elo_diff",
              "ELO + RAA total" = "elo_diff + RAA_diff",
              "ELO + RAA split" = "elo_diff + RAA_bat_diff + RAA_bowl_diff")
  if ("RVAA" %in% names(sides)) {
    rhs[["ELO + RVAA split"]] <- "elo_diff + RVAA_bat_diff + RVAA_bowl_diff"
    rhs[["RVAA split alone"]] <- "RVAA_bat_diff + RVAA_bowl_diff"
  }
  if ("TSA" %in% names(sides)) {
    rhs[["ELO + TSA split"]] <- "elo_diff + TSA_bat_diff + TSA_bowl_diff"
    rhs[["TSA split alone"]] <- "TSA_bat_diff + TSA_bowl_diff"
  }
  if (all(c("RVAA", "TSA") %in% names(sides))) {
    rhs[["ELO + RVAA + TSA"]] <- "elo_diff + RVAA_bat_diff + RVAA_bowl_diff + TSA_bat_diff + TSA_bowl_diff"
  }

  cli::cli_alert_info("n={nrow(d)} train={nrow(tr)} test={nrow(te_set)} | batting-first wins {round(100*base_rate,1)}% | BASE logloss {round(ll_null,4)} | BASE margin RMSE {round(rm_null,2)}")
  cli::cli_alert_info("  {sprintf('%-18s','arm')} {sprintf('%9s','logloss')} {sprintf('%8s','vs base')} {sprintf('%7s','acc')} | {sprintf('%9s','RMSE')} {sprintf('%8s','vs base')}")
  res <- list()
  for (nm in names(rhs)) {
    fw <- glm(stats::as.formula(paste("bf_won ~", rhs[[nm]])), family = binomial, data = tr)
    fm <- stats::lm(stats::as.formula(paste("unified_margin ~", rhs[[nm]])), data = tr)
    pw <- predict(fw, te_set, type = "response")
    ll <- logloss(pw, te_set$bf_won)
    ac <- mean((pw > 0.5) == (te_set$bf_won == 1))
    rm <- rmse(predict(fm, te_set), te_set$unified_margin)
    res[[nm]] <- list(pw = pw, ll = ll, rm = rm, fm = fm)
    cli::cli_alert_info("  {sprintf('%-18s', nm)} {sprintf('%9.4f', ll)} {sprintf('%+7.1f%%', 100*(ll_null-ll)/ll_null)} {sprintf('%6.1f%%', 100*ac)} | {sprintf('%9.2f', rm)} {sprintf('%+7.1f%%', 100*(rm_null-rm)/rm_null)}")
  }

  # Every arm against ELO alone, bootstrapped BY MATCH under both targets.
  # Per-ball resampling would overstate precision roughly 220-fold; the match
  # is the independent unit because that is what is being predicted.
  set.seed(SEED)
  idx <- replicate(BOOT, sample(nrow(te_set), nrow(te_set), replace = TRUE), simplify = FALSE)
  base <- res[["ELO alone"]]
  cli::cli_alert_info("  -- vs ELO alone, {BOOT} bootstrap resamples by match --")
  for (nm in setdiff(names(rhs), "ELO alone")) {
    a <- res[[nm]]
    bw <- vapply(idx, function(i)
      logloss(base$pw[i], te_set$bf_won[i]) - logloss(a$pw[i], te_set$bf_won[i]), numeric(1))
    bm <- vapply(idx, function(i) { s2 <- te_set[i]
      rmse(predict(base$fm, s2), s2$unified_margin) - rmse(predict(a$fm, s2), s2$unified_margin) },
      numeric(1))
    cw <- quantile(bw, c(0.025, 0.975)); cm <- quantile(bm, c(0.025, 0.975))
    v <- function(ci) if (ci[1] > 0) "BEATS" else if (ci[2] < 0) "worse" else "n.d."
    cli::cli_alert_info("  {sprintf('%-18s', nm)} win {sprintf('%+.4f', mean(bw))} [{sprintf('%+.4f', cw[1])},{sprintf('%+.4f', cw[2])}] {sprintf('%4d', sum(bw>0))}/{BOOT} {v(cw)} | margin {sprintf('%+.3f', mean(bm))} [{sprintf('%+.3f', cm[1])},{sprintf('%+.3f', cm[2])}] {sprintf('%4d', sum(bm>0))}/{BOOT} {v(cm)}")
  }
}
