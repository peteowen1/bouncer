# Player Rating v2: opponent- and competition-adjusted, tuned to predict the
# NEXT match.
#
# Target (D-P17, Pete 2026-08-15): "who would you rather have out of all players
# next match". Everything here was selected by out-of-sample Spearman against a
# player's next match, not by how the leaderboard looks.
#
# Pipeline, with the measured contribution of each stage:
#
#   RAA per ball (build_cricsheet_raa)
#     -> two-way batter/bowler adjustment          +40.5%   (D-P19)
#     -> competition discount                       +5.2%   (D-P22)
#     -> decayed, shrunk weighted mean              +3%     (D-P20, vs no decay)
#
# For scale, tuning kappa / decay / shrinkage in isolation moved the same metric
# by under 1% (D-P17, D-P18). The adjustments are where the value is.
#
# Two things deliberately NOT here, both measured and rejected:
#   - venue as a crossed effect (-1%, D-P19) and venue in the baseline (0.03% of
#     per-ball variance, so not worth a model retrain)
#   - situational wicket value from the resource surface (-6.6%, #40)

#' Competition Difficulty Factors
#'
#' How much a competition inflates batting averages relative to a reference set
#' of major leagues. A factor of 2 means the same player averages twice as much
#' there, so his value in it should be divided by 2.
#'
#' Estimated from players who appear in both, so the player's own ability
#' cancels. Competitions with no direct bridge are reached by chaining: an
#' unrated competition inherits its ratio against a rated neighbour times that
#' neighbour's factor, iterated outward. Austrian club cricket shares no player
#' with the IPL but does share players with the Europe Qualifiers, which do.
#'
#' @param conn DBI connection; opened read-only and closed on exit if NULL.
#' @param format Character. Currently "t20".
#' @param gender Character. "male" or "female".
#' @param reference Character vector of competitions defining the 1.0 scale.
#' @param min_here,min_ref Integer. Balls required in the competition being
#'   rated, and in the reference set, for a player to count as a bridge.
#'   `min_here` was 60 until 2026-08-16 (D-P23); 60 left 4.1% of deliveries in
#'   competitions with no factor, almost all of them short bilateral T20I
#'   series where no batter reaches 60 balls, and rating those is worth +2.2%
#'   next-game Spearman. The metric is flat from 10 to 40 and falls off only at
#'   60, so this sits mid-plateau rather than at an edge.
#' @param min_players Integer. Bridges required before a competition is rated.
#' @param max_steps Integer. Chaining passes.
#' @param clamp Numeric length 2. Factors are clipped to this range so one thin
#'   cell cannot dominate.
#'
#' @return data.table of `comp`, `factor`, `n_bridges`, `step` (0 = direct).
#' @export
fit_competition_factors <- function(conn = NULL,
                                    format = "t20",
                                    gender = "male",
                                    reference = COMPETITION_REFERENCE_T20,
                                    min_here = 30L,
                                    min_ref = 150L,
                                    min_players = 3L,
                                    max_steps = 6L,
                                    clamp = c(0.5, 4)) {

  own <- is.null(conn)
  if (own) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }
  types <- if (format == "t20") "'t20','it20'" else "'odi','odm'"

  d <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT d.batter_id, COALESCE(m.event_name,'unknown') AS comp,
           SUM(d.runs_batter) AS runs, SUM(CAST(d.is_wicket AS INT)) AS outs,
           COUNT(*) AS balls
    FROM cricsheet.deliveries d
    JOIN cricsheet.matches m ON m.match_id = d.match_id
    WHERE LOWER(d.match_type) IN (%s) AND m.gender = '%s'
      AND COALESCE(m.balls_per_over, 6) = 6 AND COALESCE(d.wides, 0) = 0
    GROUP BY d.batter_id, m.event_name", types, gender)))
  if (!nrow(d)) cli::cli_abort("No deliveries for {format}/{gender}.")

  ref <- d[comp %in% reference,
           .(r_runs = sum(runs), r_outs = sum(outs), r_balls = sum(balls)),
           by = batter_id][r_balls >= min_ref]
  if (nrow(ref) < 20) {
    cli::cli_abort(c("Only {nrow(ref)} players clear the reference threshold.",
                     "i" = "Check {.arg reference} names against {.field cricsheet.matches.event_name}."))
  }

  avg <- function(r, o) sum(r) / pmax(sum(o), 1)
  j <- merge(d[!comp %in% reference & balls >= min_here], ref, by = "batter_id")
  direct <- j[, .(n_bridges = .N,
                  factor = avg(runs, outs) / avg(r_runs, r_outs)),
              by = comp][n_bridges >= min_players]
  direct[, step := 0L]
  out <- rbind(direct,
               data.table::data.table(comp = reference, factor = 1, n_bridges = NA_integer_,
                                      step = 0L), fill = TRUE)
  out <- out[!duplicated(comp)]

  for (s in seq_len(max_steps)) {
    known <- out$comp
    cand <- d[balls >= min_here]
    a <- cand[comp %in% known, .(batter_id, rcomp = comp, r_runs = runs, r_outs = outs)]
    u <- cand[!comp %in% known, .(batter_id, comp, runs, outs)]
    if (!nrow(a) || !nrow(u)) break
    jj <- merge(u, a, by = "batter_id", allow.cartesian = TRUE)
    if (!nrow(jj)) break
    fmap <- stats::setNames(out$factor, out$comp)
    jj[, rfac := fmap[rcomp]]
    nw <- jj[, .(n_bridges = data.table::uniqueN(batter_id),
                 factor = avg(runs, outs) / avg(r_runs, r_outs) * stats::median(rfac)),
             by = comp][n_bridges >= min_players]
    if (!nrow(nw)) break
    nw[, step := s]
    out <- rbind(out, nw)
  }

  out <- out[is.finite(factor) & factor > 0]
  out[, factor := pmin(pmax(factor, clamp[1]), clamp[2])]
  data.table::setorder(out, -factor)
  cli::cli_alert_success(
    "Rated {nrow(out)} competition{?s} ({sum(out$step == 0)} directly, {sum(out$step > 0)} by chaining).")
  out[]
}

#' Reference Competitions Defining the 1.0 Difficulty Scale
#' @export
COMPETITION_REFERENCE_T20 <- c(
  "Indian Premier League", "Big Bash League", "Pakistan Super League",
  "SA20", "Caribbean Premier League", "International League T20",
  "ICC Men's T20 World Cup", "Vitality Blast", "NatWest T20 Blast"
)

#' Two-Way Batter and Bowler Effects
#'
#' Alternating ridge fit of per-ball RAA onto batter and bowler identity. Each
#' side is estimated net of the other, which is what makes the bowler effect a
#' measure of bowling rather than of who he happened to bowl at.
#'
#' Joint fitting is legitimate here and was NOT for competition strength
#' (2026-08-06): batters and bowlers are crossed within matches, so both are
#' identified from the same deliveries. Players and competitions are not
#' crossed, which is why those need the bridge construction above.
#'
#' @param balls data.table with `batter_id`, `bowler_id`, `raa`.
#' @param prior_balls Numeric ridge prior, in balls. 60 measured best; heavier
#'   priors converge faster to a worse answer.
#' @param iterations Integer. 20 is converged (20 to 50 moves rho by 0.002).
#' @return list of `batter` and `bowler` data.tables with `eff` and `n`.
#' @export
fit_two_way_effects <- function(balls, prior_balls = 60, iterations = 20L) {
  stopifnot(all(c("batter_id", "bowler_id", "raa") %in% names(balls)))
  z <- data.table::as.data.table(balls)[, .(batter_id, bowler_id, raa)]
  z[, `:=`(ae = 0, be = 0)]
  bat <- bwl <- NULL
  for (i in seq_len(iterations)) {
    bat <- z[, .(eff = sum(raa - be) / (.N + prior_balls), n = .N), by = batter_id]
    z[bat, on = "batter_id", ae := i.eff]
    bwl <- z[, .(eff = sum(raa - ae) / (.N + prior_balls), n = .N), by = bowler_id]
    z[bwl, on = "bowler_id", be := i.eff]
  }
  list(batter = bat[], bowler = bwl[])
}

#' Player Rating v2
#'
#' The full pipeline: per-ball RAA, adjusted for the opponent faced and for the
#' competition, aggregated as a decayed weighted mean shrunk toward the
#' population mean.
#'
#' Defaults are the values selected by out-of-sample next-match Spearman, not
#' by inspection of the leaderboard (D-P17 to D-P24).
#'
#' Batting and bowling are the same construction with the roles swapped: a
#' batter's value is RAA net of the bowler he faced, a bowler's is the negation
#' of RAA net of the batter he bowled to. Both are then divided by the
#' competition factor. Because the two share every aggregation setting, their
#' ratings are on one scale and may be added — component sd is 1.358 (batting)
#' against 1.347 (bowling), a ratio of 0.99. See D-P24: the legacy
#' [calculate_impact()] path reported 0.61, which was its exposure weighting,
#' not a property of the game.
#'
#' @param format,gender Bucket to rate. Ratings are never pooled across either:
#'   men and women do not play each other, and formats are separate skills.
#' @param role "batter" or "bowler".
#' @param conn DBI connection; opened read-only and closed on exit if NULL.
#' @param as_at Date. Rate as of this date; NULL uses the latest delivery.
#'   Decay is measured back from here, so an inactive player falls on his own
#'   rather than needing an activity filter.
#' @param decay_days Numeric, or NULL to use the role's measured default —
#'   1095 for batting, 1825 for bowling. Bowlers hold their value longer:
#'   sweeping decay against a fixed target gives 0.1027 / 0.1101 / 0.1121 /
#'   0.1130 / 0.1130 / 0.1115 at 365 / 730 / 1095 / 1825 / 2555 / none.
#'   Shorter decays are worse at every horizon for both roles.
#' @param prior_matches Numeric shrinkage toward the population mean.
#' @param prior_balls,iterations Passed to [fit_two_way_effects()].
#' @param factors Output of [fit_competition_factors()]; NULL fits them.
#' @param min_balls Integer. Career balls required to appear in the result.
#'
#' @return data.table of `player_id`, `player_name`, `rating`, `matches`,
#'   `balls`, `last_match`, ordered best first. `matches` counts innings batted
#'   for a batter and matches played for a bowler — the two roles use different
#'   inclusion rules, each measured (D-P26), so the two ratings rank correctly
#'   within a role but are NOT on a common per-match scale and must not be
#'   added. A combined total is blocked on #42.
#' @export
calculate_player_rating_v2 <- function(format = "t20",
                                       gender = "male",
                                       role = c("batter", "bowler"),
                                       conn = NULL,
                                       as_at = NULL,
                                       decay_days = NULL,
                                       prior_matches = 20,
                                       prior_balls = 60,
                                       iterations = 20L,
                                       factors = NULL,
                                       min_balls = 500L) {

  role <- match.arg(role)
  if (is.null(decay_days)) decay_days <- if (role == "batter") 1095 else 1825

  own <- is.null(conn)
  if (own) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }

  b <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, r.raa,
           COALESCE(m.event_name, 'unknown') AS comp
    FROM main.cricsheet_ball_raa r
    JOIN cricsheet.matches m ON m.match_id = r.match_id
    WHERE r.format = '%s' AND r.gender = '%s'", toupper(format), gender)))
  if (!nrow(b)) {
    cli::cli_abort(c("No rows in {.field main.cricsheet_ball_raa} for {format}/{gender}.",
                     "i" = "Run {.fn build_cricsheet_raa} first."))
  }

  if (is.null(factors)) factors <- fit_competition_factors(conn, format, gender)
  fmap <- stats::setNames(factors$factor, factors$comp)
  b[, cfactor := fmap[comp]]
  # An unrated competition keeps 1.0. "Unrated implies weak" was tested and
  # rejected (D-P23): most of what went unrated was short bilateral T20I series
  # between full members, which rate at a median 1.05 and as low as 0.69 once
  # the bridge threshold admits them. Assuming 1.6 there would have discounted
  # elite international cricket. With min_here = 30 the residue is ~0.5% of
  # deliveries, too small to move a rating either way. Report it regardless.
  unrated <- b[is.na(cfactor), .N]
  b[is.na(cfactor), cfactor := 1]
  if (unrated > 0) {
    cli::cli_alert_info(
      "{round(100 * unrated / nrow(b), 1)}% of deliveries are in competitions with no factor; treated as reference difficulty.")
  }

  eff <- fit_two_way_effects(b, prior_balls = prior_balls, iterations = iterations)
  if (role == "batter") {
    b[eff$bowler, on = "bowler_id", opp_eff := i.eff]
    b[is.na(opp_eff), opp_eff := 0]
    b[, value := (raa - opp_eff) / cfactor]
    id_col <- "batter_id"
  } else {
    # RAA is signed from the batting side, so negate: a bowler wants it low.
    # The competition factor divides here exactly as it does for batting --
    # tested, not assumed. Applying it the other way round (multiplying) costs
    # 10.2% against the same target, so the direction is established rather
    # than merely plausible.
    b[eff$batter, on = "batter_id", opp_eff := i.eff]
    b[is.na(opp_eff), opp_eff := 0]
    b[, value := -(raa - opp_eff) / cfactor]
    id_col <- "bowler_id"
  }

  pm <- b[, .(v = sum(value), balls = .N),
          by = c(player_id = id_col, "match_id", "match_date")]

  # Which appearances count is measured per role, not assumed (D-P26). Both
  # rules were scored against one uncensored target: the player's actual value
  # in his next match, counting zero when he does not perform the role.
  if (role == "bowler") {
    # A match he played but did not bowl is a real zero -- he was in the side
    # and contributed nothing with the ball -- not missing data. Treating it as
    # absent costs 8.3%. Appearance is inferred from the ball record, so a
    # player who neither batted nor bowled is invisible; that undercounts a
    # pure specialist's matches slightly and cannot inflate him.
    app <- unique(data.table::rbindlist(list(
      b[, .(player_id = batter_id, match_id, match_date)],
      b[, .(player_id = bowler_id, match_id, match_date)])))
    pm <- merge(app, pm[, .(player_id, match_id, v, balls)],
                by = c("player_id", "match_id"), all.x = TRUE)
    pm[is.na(v), `:=`(v = 0, balls = 0)]
    stopifnot(!anyNA(pm$match_date))
  }
  # Batting keeps every innings he batted. The old `balls >= 6` floor was a
  # survivorship filter: a tailender's innings are mostly 1-5 balls, so it
  # deleted his failures and kept his survivals -- Bumrah went from 35 innings
  # to 5, and those 5 averaged +4.85. Removing it is worth 8.8%.
  ref_date <- if (is.null(as_at)) max(pm$match_date) else as.Date(as_at)
  pm <- pm[match_date <= ref_date]
  pop <- pm[, mean(v)]
  pm[, w := exp(-as.numeric(ref_date - match_date) / decay_days)]

  r <- pm[, .(rating = (sum(v * w) + prior_matches * pop) / (sum(w) + prior_matches),
              matches = .N, balls = sum(balls),
              effective_matches = round(sum(w), 1),
              last_match = max(match_date)), by = player_id][balls >= min_balls]

  # ANY_VALUE, not a bare SELECT: a duplicated registry row would otherwise
  # duplicate the player in the leaderboard rather than erroring.
  nm <- data.table::as.data.table(DBI::dbGetQuery(conn,
    "SELECT player_id, ANY_VALUE(player_name) AS player_name
     FROM cricsheet.players GROUP BY player_id"))
  r <- merge(r, nm, by = "player_id", all.x = TRUE)
  data.table::setorder(r, -rating)
  r[, rank := seq_len(.N)]
  cli::cli_alert_success(
    "Rated {nrow(r)} {gender} {toupper(format)} {role}s as at {ref_date}.")
  r[, .(rank, player_id, player_name, rating, matches, balls,
        effective_matches, last_match)][]
}

#' Combined Player Value: Batting Plus Bowling, Per Match Played
#'
#' What a player is worth to a side across both disciplines, in runs per match
#' played. This is a different question from [calculate_player_rating_v2()],
#' which answers "how good a batter is he" on a per-innings basis. Here a
#' specialist bowler's batting term is near zero because he barely bats — the
#' quantity is contribution, not quality — so the two components can be added.
#'
#' Each component is `quality x opportunity`:
#' \itemize{
#'   \item quality = runs per ball, shrunk toward the population rate
#'   \item opportunity = balls per match played, shrunk toward the population
#'     mean
#' }
#'
#' The two shrinkage constants must DIFFER or the product cancels algebraically
#' back to the plain per-match mean — with `Kq == Ko` the `(balls + Ko * N)`
#' factor appears in both numerator and denominator. That identity is why an
#' earlier attempt measured byte-identical to the baseline.
#'
#' `opp_prior = 2` deliberately trusts a player's own participation. Sweeping
#' it against the BATTING-only target prefers 320 (assume everyone bats about
#' as often), which is true for the regulars that dominate the evaluation set
#' and false for specialists — and it re-creates the defect this function
#' exists to fix, paying Bumrah +0.95 with the bat. Against the COMBINED target
#' the ordering reverses and 2 wins (0.0870 vs 0.0854), so no trade is being
#' made here; see D-P27.
#'
#' @param format,gender,conn,as_at,factors,prior_balls,iterations As in
#'   [calculate_player_rating_v2()].
#' @param bat_prior,bowl_prior Quality shrinkage, in population-average matches.
#' @param opp_prior Opportunity shrinkage. See the note above before raising it.
#' @param min_balls Integer. Career balls, both roles combined.
#' @param min_calibrated Numeric 0-1. Minimum share of a player's career spent
#'   in competitions rated DIRECTLY against the reference set (5+ bridge
#'   players), as opposed to reached by chaining. Players below it are reported
#'   and dropped; set to 0 to keep everyone.
#'
#'   This exists because the associate-league residue is not estimable, and
#'   three attempts to estimate through it failed (D-P28). A player whose entire
#'   career is in one weak league offers nothing that separates "he is good"
#'   from "that league is easy" — the 2026-08-06 identifiability constraint —
#'   and the harness cannot adjudicate it either, because his next match is in
#'   that same league, which his inflated rating predicts perfectly well.
#'   Measuring the uncertainty is honest where estimating it is not: the
#'   population median share is 100% and only 130 of 1,127 players fall below
#'   50%, so this is a narrow exclusion, not a blunt one.
#'
#' @return data.table of `player_id`, `player_name`, `total_value`,
#'   `bat_value`, `bowl_value`, `matches`, `bat_balls`, `bowl_balls`,
#'   ordered best first.
#' @export
calculate_player_value_v2 <- function(format = "t20",
                                      gender = "male",
                                      conn = NULL,
                                      as_at = NULL,
                                      factors = NULL,
                                      bat_prior = 40,
                                      bowl_prior = 5,
                                      opp_prior = 2,
                                      prior_balls = 60,
                                      iterations = 20L,
                                      min_balls = 1000L,
                                      min_calibrated = 0.5) {

  own <- is.null(conn)
  if (own) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }

  b <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, r.raa,
           COALESCE(m.event_name, 'unknown') AS comp
    FROM main.cricsheet_ball_raa r
    JOIN cricsheet.matches m ON m.match_id = r.match_id
    WHERE r.format = '%s' AND r.gender = '%s'", toupper(format), gender)))
  if (!nrow(b)) {
    cli::cli_abort(c("No rows in {.field main.cricsheet_ball_raa} for {format}/{gender}.",
                     "i" = "Run {.fn build_cricsheet_raa} first."))
  }

  if (is.null(factors)) factors <- fit_competition_factors(conn, format, gender)
  fmap <- stats::setNames(factors$factor, factors$comp)
  b[, cfactor := fmap[comp]][is.na(cfactor), cfactor := 1]

  eff <- fit_two_way_effects(b, prior_balls = prior_balls, iterations = iterations)
  b[eff$bowler, on = "bowler_id", be := i.eff][is.na(be), be := 0]
  b[eff$batter, on = "batter_id", ae := i.eff][is.na(ae), ae := 0]
  b[, v_bat := (raa - be) / cfactor]
  b[, v_bowl := -(raa - ae) / cfactor]

  bb <- b[, .(vb = sum(v_bat),  nb = .N), by = .(player_id = batter_id, match_id, match_date)]
  ww <- b[, .(vw = sum(v_bowl), nw = .N), by = .(player_id = bowler_id, match_id, match_date)]
  pm <- merge(bb, ww, by = c("player_id", "match_id", "match_date"), all = TRUE)
  for (cc in c("vb", "nb", "vw", "nw")) {
    data.table::set(pm, which(is.na(pm[[cc]])), cc, 0)
  }
  ref_date <- if (is.null(as_at)) max(pm$match_date) else as.Date(as_at)
  pm <- pm[match_date <= ref_date]

  # population rate and participation, on a per-match-PLAYED basis
  par <- list(
    bat  = list(r = pm[, sum(vb) / sum(nb)], n = pm[, mean(nb)], decay = 1095),
    bowl = list(r = pm[, sum(vw) / sum(nw)], n = pm[, mean(nw)], decay = 1825))

  out <- NULL
  for (tag in c("bat", "bowl")) {
    p  <- par[[tag]]
    vc <- if (tag == "bat") "vb" else "vw"
    nc <- if (tag == "bat") "nb" else "nw"
    kq <- if (tag == "bat") bat_prior else bowl_prior
    pm[, w := exp(-as.numeric(ref_date - match_date) / p$decay)]
    a <- pm[, .(sv = sum(get(vc) * w), sn = sum(get(nc) * w), sw = sum(w),
                balls = sum(get(nc)), matches = .N), by = player_id]
    a[, value := ((sv + kq * p$n * p$r) / (sn + kq * p$n)) *
                 ((sn + opp_prior * p$n) / (sw + opp_prior))]
    data.table::setnames(a, c("value", "balls"), paste0(tag, c("_value", "_balls")))
    out <- if (is.null(out)) a[, .(player_id, matches, bat_value, bat_balls)] else
      merge(out, a[, .(player_id, bowl_value, bowl_balls)], by = "player_id", all = TRUE)
  }
  out[, total_value := bat_value + bowl_value]
  out <- out[bat_balls + bowl_balls >= min_balls]

  # How much of each career is in cricket we can actually calibrate. A chained
  # factor inherits its neighbour's error at every hop, so only competitions
  # rated directly against the reference set count.
  solid <- c(factors[step == 0L & n_bridges >= 5L, comp],
             intersect(factors$comp, COMPETITION_REFERENCE_T20))
  cal <- data.table::rbindlist(list(
    b[, .(player_id = batter_id, ok = comp %in% solid)],
    b[, .(player_id = bowler_id, ok = comp %in% solid)]
  ))[, .(calibrated = mean(ok)), by = player_id]
  out <- merge(out, cal, by = "player_id", all.x = TRUE)
  out[is.na(calibrated), calibrated := 0]
  if (min_calibrated > 0) {
    drop <- out[calibrated < min_calibrated]
    if (nrow(drop)) {
      cli::cli_alert_info(
        "Dropped {nrow(drop)} player{?s} with under {round(100*min_calibrated)}% of their career in directly-rated competitions (e.g. {drop[order(-total_value)][seq_len(min(2, .N))]$player_name}).")
    }
    out <- out[calibrated >= min_calibrated]
  }

  nm <- data.table::as.data.table(DBI::dbGetQuery(conn,
    "SELECT player_id, ANY_VALUE(player_name) AS player_name
     FROM cricsheet.players GROUP BY player_id"))
  out <- merge(out, nm, by = "player_id", all.x = TRUE)
  data.table::setorder(out, -total_value)
  out[, rank := seq_len(.N)]
  cli::cli_alert_success(
    "Valued {nrow(out)} {gender} {toupper(format)} players as at {ref_date}.")
  out[, .(rank, player_id, player_name, total_value, bat_value, bowl_value,
          matches, bat_balls, bowl_balls, calibrated)][]
}
