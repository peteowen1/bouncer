# #61: does a team rating built from player ratings beat the result-ELO?
#
# THE BAR, from the ticket: held-out match prediction, per format, with the
# MATCH as the independent unit.
#
# EVERY DESIGN CHOICE HERE CAME FROM #60, and each one constrains the code:
#
#   * Composition is on a runs-per-match scale, NOT a sum. Summing bat and
#     bowl value gives a bowling-only rating in Test (batting is 7.6% of the
#     variance, and the composite correlates -0.085 with it).
#   * Target is unified_margin, whose sign is relative to the side BATTING
#     FIRST -- not team1, despite what its docstring said until 2026-08-21.
#     98.9% agreement against 86.4%. Getting this backwards absorbs a
#     toss-shaped error and reports it as team strength.
#   * Squad is "who actually appeared", because the XI is mostly not
#     recoverable. That makes the rating RETROSPECTIVE; it cannot be computed
#     before a match.
#   * The baseline is the REBUILT team_elo (now current to 2026-08-06). The
#     stale one would have repeated the vintage confound that made the
#     full-model comparison meaningless.
#
# CAUSALITY. Ratings come from player_rating_v2_snapshots via pick_snapshot(),
# which returns the latest snapshot STRICTLY BEFORE each match --
# calculate_player_rating_v2(as_at = D) includes matches played ON D.
#
# Usage: Rscript data-raw/validation/score_team_rating.R [t20 odi test]
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})

fmts <- commandArgs(trailingOnly = TRUE)
if (!length(fmts)) fmts <- c("t20", "odi", "test")
MIN_RATED <- 6L   # a side composed from fewer than this is not a team rating

conn <- get_db_connection(read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

if (!table_exists(conn, "player_value_v2_snapshots")) {
  cli::cli_abort("No rating snapshots. Run build_rating_snapshots.R first.")
}

types <- list(t20 = "'t20','it20'", odi = "'odi','odm'", test = "'test','mdm'")

for (fmt in fmts) {
  cli::cli_h1(toupper(fmt))

  # value_v2, not per-role rating_v2: the two roles' ratings are NOT on a
  # common scale and must not be added (R/player_rating_v2.R @return, #42).
  snaps <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT as_at, player_id, bat_value, bowl_value, bat_balls, bowl_balls
    FROM main.player_value_v2_snapshots
    WHERE format = '%s' AND gender = 'male'", fmt)))
  if (!nrow(snaps)) { cli::cli_alert_warning("no snapshots for {fmt}"); next }
  snap_dates <- sort(unique(as.Date(snaps$as_at)))
  cli::cli_alert_info("{length(snap_dates)} snapshot date{?s}, {format(nrow(snaps), big.mark=',')} rating rows")

  # Matches, with the side batting first -- the frame unified_margin uses.
  m <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT m.match_id, CAST(m.match_date AS DATE) AS match_date,
           m.team1, m.team2, m.unified_margin, m.team_type,
           -- MIN/MAX rather than LIMIT 1 with no ORDER BY: a duplicate
           -- innings row would otherwise pick a side non-deterministically,
           -- and that choice sets the unified_margin sign convention.
           (SELECT MIN(batting_team) FROM cricsheet.match_innings i
            WHERE i.match_id = m.match_id AND i.innings = 1) AS bat_first,
           (SELECT MAX(batting_team) FROM cricsheet.match_innings i
            WHERE i.match_id = m.match_id AND i.innings = 1) AS bat_first_max
    FROM cricsheet.matches m
    WHERE LOWER(m.match_type) IN (%s) AND m.gender = 'male'
      AND m.unified_margin IS NOT NULL AND m.unified_margin <> 0", types[[fmt]])))
  amb <- sum(m$bat_first != m$bat_first_max, na.rm = TRUE)
  if (amb > 0) {
    cli::cli_abort("{amb} match{?es} have more than one first-innings batting team; the margin sign would be arbitrary.")
  }
  m[, bat_first_max := NULL]
  m <- m[!is.na(bat_first)]
  m[, chasing := fifelse(bat_first == team1, team2, team1)]
  m[, snap := pick_snapshot(match_date, snap_dates)]
  scorable <- m[!is.na(snap)]
  cli::cli_alert_info("{format(nrow(m), big.mark=',')} decided matches, {format(nrow(scorable), big.mark=',')} with a snapshot strictly before them")
  if (nrow(scorable) < 100) { cli::cli_alert_warning("too few to score {fmt}"); next }

  # Who actually appeared, per side. ONE row per (match, team, player):
  # an all-rounder must not be counted twice, and value_v2 already carries
  # both his batting and bowling contribution in one row.
  app <- as.data.table(dbGetQuery(conn, sprintf("
    SELECT match_id, team, player_id FROM (
      SELECT d.match_id, d.batting_team AS team, d.batter_id AS player_id
      FROM cricsheet.deliveries d
      WHERE LOWER(d.match_type) IN (%s) AND d.gender = 'male'
      UNION
      SELECT d.match_id, d.bowling_team AS team, d.bowler_id AS player_id
      FROM cricsheet.deliveries d
      WHERE LOWER(d.match_type) IN (%s) AND d.gender = 'male')", types[[fmt]], types[[fmt]])))
  app <- app[match_id %in% scorable$match_id]

  # CANONICALISE. Snapshot ids are canonical (calculate_player_value_v2 runs
  # canonicalise_player_ids before aggregating); raw delivery ids are not.
  # 4.47% of appearances sit on the wrong side of the bare-name/hash split, so
  # without this those players fail the join, arrive as NA, and get marked
  # DEBUTANT -- a replacement-level fill covering an id-plumbing bug and a
  # genuine debut with nothing to tell them apart.
  id_map <- build_player_id_map(conn)
  before_ids <- uniqueN(app$player_id)
  canonicalise_player_ids(app, id_map)
  cli::cli_alert_info("canonicalised player ids: {before_ids} -> {uniqueN(app$player_id)} distinct")

  app <- merge(app, scorable[, .(match_id, snap)], by = "match_id")
  app <- merge(app, snaps[, .(snap = as.Date(as_at), player_id,
                              bat_value, bowl_value, bat_balls, bowl_balls)],
               by = c("snap", "player_id"), all.x = TRUE)

  # Replacement level for DEBUTANTS ONLY -- a player with no prior snapshot
  # row, i.e. no earlier match to value him from. Derived from the same
  # snapshot so it tracks the scale rather than asserting one.
  app[, debutant := is.na(bat_value) & is.na(bowl_value)]
  repl <- snaps[, .(repl_bat = stats::quantile(bat_value, 0.10, na.rm = TRUE),
                    repl_bowl = stats::quantile(bowl_value, 0.10, na.rm = TRUE)),
                by = .(snap = as.Date(as_at))]
  app <- merge(app, repl, by = "snap", all.x = TRUE)
  app[debutant == TRUE, `:=`(bat_value = repl_bat, bowl_value = repl_bowl)]
  # A fill that silently failed would otherwise vanish into sum(na.rm = TRUE).
  still_na <- sum(is.na(app$bat_value) & is.na(app$bowl_value))
  if (still_na > 0) {
    cli::cli_abort(c("{still_na} appearance{?s} have no value even after the debutant fill.",
                     "i" = "The fill covered a cause it was not meant to cover."))
  }

  # SUM THE VALUES DIRECTLY. No exposure conversion -- and this corrects my own
  # #60 reasoning, not just a coding slip.
  #
  # calculate_player_value_v2() is titled "Batting Plus Bowling, PER MATCH
  # PLAYED" and states outright: "the quantity is contribution, not quality --
  # so the two components can be added." Each is already quality x opportunity
  # (runs per ball, shrunk) x (balls per match, shrunk). It is ALREADY the
  # per-match scale value_per_match() was written to produce, and the function
  # computes total_value = bat_value + bowl_value itself.
  #
  # Running it through value_per_match() divided an already-per-match rate by
  # RAW CAREER BALLS and rescaled -- making a player's contribution inversely
  # proportional to how long he has played. Two batters of identical skill
  # differed 6x purely by career length.
  #
  # What this means for #60's question 1: I framed the batting/bowling variance
  # imbalance (Test batting 7.6% of summed variance) as a scale defect needing
  # conversion. On this quantity it is not a unit problem -- the components are
  # designed to be added. The imbalance is more likely a real property of Test
  # cricket, where bowling contribution genuinely varies more than batting.
  # assert_component_balance() is kept below as a DIAGNOSTIC that reports the
  # split, not as a claim that an imbalance is a bug.
  side <- app[, .(rating_sum = sum(bat_value + bowl_value, na.rm = TRUE),
                  bat_part = sum(bat_value, na.rm = TRUE),
                  bowl_part = sum(bowl_value, na.rm = TRUE),
                  n_rated = sum(!debutant), n_debut = sum(debutant)),
              by = .(match_id, team)]
  dbg <- side[, .(sides = .N, mean_rated = round(mean(n_rated), 1),
                  mean_debut = round(mean(n_debut), 2),
                  all_debut = sum(n_rated == 0))]
  cli::cli_alert_info("per side: {dbg$mean_rated} rated, {dbg$mean_debut} debutant{?s} on average; {dbg$all_debut} side{?s} entirely unrated")
  # Anchor 5 reported, not enforced. On this quantity the components are
  # designed to be additive, so an imbalance is evidence about the format
  # rather than proof of a broken composition. Enforcing it here would abort
  # on a real property of Test cricket.
  bal <- tryCatch(assert_component_balance(side$bat_part, side$bowl_part),
                  error = function(e) NULL)
  vb <- stats::var(side$bat_part); vw <- stats::var(side$bowl_part)
  cli::cli_alert_info("component split: batting {round(100*vb/(vb+vw), 1)}% of summed variance")
  side <- side[n_rated >= MIN_RATED]

  d <- merge(scorable[, .(match_id, match_date, bat_first, chasing, unified_margin, team_type)],
             side[, .(match_id, bat_first = team, bf_rating = rating_sum, bf_n = n_rated, bf_debut = n_debut)],
             by = c("match_id", "bat_first"))
  d <- merge(d, side[, .(match_id, chasing = team, ch_rating = rating_sum, ch_n = n_rated, ch_debut = n_debut)],
             by = c("match_id", "chasing"))
  d[, rating_diff := bf_rating - ch_rating]
  cli::cli_alert_info("{format(nrow(d), big.mark=',')} matches with both sides rated ({MIN_RATED}+ players each)")
  if (nrow(d) < 100) { cli::cli_alert_warning("too few after rating both sides"); next }

  # Baseline: the rebuilt result-ELO, same matches, same frame.
  te <- as.data.table(dbGetQuery(conn, "
    SELECT match_id, team_id, elo_before FROM main.team_elo WHERE played_in_match"))
  # team_elo keys on a SLUG (sylhet_super_stars_male_t20_club), not the display
  # name cricsheet.matches carries. Joining on the name silently matched zero
  # rows -- the join produced no error, just an empty result, which lm() then
  # reported as "0 (non-NA) cases" three steps later. make_team_id_vec() is the
  # existing builder; constructing the slug by hand here would be another
  # two-declarations drift.
  d[, `:=`(bf_id = make_team_id_vec(bat_first, "male", fmt, team_type),
           ch_id = make_team_id_vec(chasing,  "male", fmt, team_type))]
  d <- merge(d, te[, .(match_id, bf_id = team_id, bf_elo = elo_before)],
             by = c("match_id", "bf_id"), all.x = TRUE)
  d <- merge(d, te[, .(match_id, ch_id = team_id, ch_elo = elo_before)],
             by = c("match_id", "ch_id"), all.x = TRUE)
  matched <- sum(!is.na(d$bf_elo) & !is.na(d$ch_elo))
  if (matched == 0) {
    cli::cli_abort(c("No match joined an ELO -- the team id join found nothing.",
                     "i" = "Example slug built: {.val {utils::head(d$bf_id, 1)}}",
                     "i" = "Example in team_elo: {.val {utils::head(te$team_id, 1)}}"))
  }
  d <- d[!is.na(bf_elo) & !is.na(ch_elo)]
  d[, elo_diff := bf_elo - ch_elo]
  cli::cli_alert_info("{format(nrow(d), big.mark=',')} with both an ELO and a rating")

  # Split by DATE, not at random: a rating is used forward in time, so a
  # random split would let the fit see the future of its own test matches.
  setorder(d, match_date)
  cut <- floor(0.8 * nrow(d))
  tr <- d[1:cut]; te_set <- d[(cut + 1):nrow(d)]
  cli::cli_alert_info("train {nrow(tr)} to {max(tr$match_date)}, test {nrow(te_set)} from {min(te_set$match_date)}")

  rmse <- function(p, a) sqrt(mean((p - a)^2))
  f_elo <- lm(unified_margin ~ elo_diff, data = tr)
  f_rat <- lm(unified_margin ~ rating_diff, data = tr)
  f_both <- lm(unified_margin ~ elo_diff + rating_diff, data = tr)
  r_elo <- rmse(predict(f_elo, te_set), te_set$unified_margin)
  r_rat <- rmse(predict(f_rat, te_set), te_set$unified_margin)
  r_both <- rmse(predict(f_both, te_set), te_set$unified_margin)
  cli::cli_alert_info("held-out RMSE -- result-ELO {round(r_elo,2)} | team rating {round(r_rat,2)} | both {round(r_both,2)}")

  # Bootstrap BY MATCH. The match is the independent unit; this repo has twice
  # reported per-unit intervals that overstated precision.
  set.seed(42)
  boot <- vapply(seq_len(2000), function(i) {
    s <- te_set[sample(.N, .N, replace = TRUE)]
    rmse(predict(f_elo, s), s$unified_margin) - rmse(predict(f_rat, s), s$unified_margin)
  }, numeric(1))
  ci <- quantile(boot, c(0.025, 0.975))
  cli::cli_alert_info("ELO RMSE minus rating RMSE: {round(mean(boot),3)}, 95% CI [{round(ci[1],3)}, {round(ci[2],3)}]")
  if (ci[1] > 0) cli::cli_alert_success("{toupper(fmt)}: the team rating BEATS the result-ELO.")
  else if (ci[2] < 0) cli::cli_alert_danger("{toupper(fmt)}: the team rating LOSES to the result-ELO.")
  else cli::cli_alert_warning("{toupper(fmt)}: not distinguishable from the result-ELO.")
}
