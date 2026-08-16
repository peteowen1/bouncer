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
                                    min_here = 60L,
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
#' The full pipeline: per-ball RAA, adjusted for the bowler faced and for the
#' competition, aggregated as a decayed weighted mean shrunk toward the
#' population mean.
#'
#' Defaults are the values selected by out-of-sample next-match Spearman, not
#' by inspection of the leaderboard (D-P17 to D-P22).
#'
#' @param format,gender Bucket to rate. Ratings are never pooled across either:
#'   men and women do not play each other, and formats are separate skills.
#' @param conn DBI connection; opened read-only and closed on exit if NULL.
#' @param as_at Date. Rate as of this date; NULL uses the latest delivery.
#'   Decay is measured back from here, so an inactive player falls on his own
#'   rather than needing an activity filter.
#' @param decay_days Numeric. 1095 measured best at the next-match horizon;
#'   shorter decays are worse at every horizon, and no decay only wins when
#'   predicting 5+ matches ahead.
#' @param prior_matches Numeric shrinkage toward the population mean.
#' @param prior_balls,iterations Passed to [fit_two_way_effects()].
#' @param factors Output of [fit_competition_factors()]; NULL fits them.
#' @param min_balls Integer. Career balls required to appear in the result.
#'
#' @return data.table of `player_id`, `player_name`, `rating`, `matches`,
#'   `balls`, `last_match`, ordered best first.
#' @export
calculate_player_rating_v2 <- function(format = "t20",
                                       gender = "male",
                                       conn = NULL,
                                       as_at = NULL,
                                       decay_days = 1095,
                                       prior_matches = 20,
                                       prior_balls = 60,
                                       iterations = 20L,
                                       factors = NULL,
                                       min_balls = 500L) {

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
  # An unrated competition keeps 1.0 -- it is NOT assumed weak, because that
  # would be a guess. Report the share so the assumption stays visible.
  unrated <- b[is.na(cfactor), .N]
  b[is.na(cfactor), cfactor := 1]
  if (unrated > 0) {
    cli::cli_alert_info(
      "{round(100 * unrated / nrow(b), 1)}% of deliveries are in competitions with no factor; treated as reference difficulty.")
  }

  eff <- fit_two_way_effects(b, prior_balls = prior_balls, iterations = iterations)
  b[eff$bowler, on = "bowler_id", bowl_eff := i.eff]
  b[is.na(bowl_eff), bowl_eff := 0]
  b[, value := (raa - bowl_eff) / cfactor]

  pm <- b[, .(v = sum(value), balls = .N),
          by = .(player_id = batter_id, match_id, match_date)][balls >= 6]
  ref_date <- if (is.null(as_at)) max(pm$match_date) else as.Date(as_at)
  pm <- pm[match_date <= ref_date]
  pop <- pm[, mean(v)]
  pm[, w := exp(-as.numeric(ref_date - match_date) / decay_days)]

  r <- pm[, .(rating = (sum(v * w) + prior_matches * pop) / (sum(w) + prior_matches),
              matches = .N, balls = sum(balls),
              effective_matches = round(sum(w), 1),
              last_match = max(match_date)), by = player_id][balls >= min_balls]

  nm <- data.table::as.data.table(DBI::dbGetQuery(conn,
    "SELECT player_id, player_name FROM cricsheet.players"))
  r <- merge(r, nm, by = "player_id", all.x = TRUE)
  data.table::setorder(r, -rating)
  r[, rank := seq_len(.N)]
  cli::cli_alert_success(
    "Rated {nrow(r)} {gender} {toupper(format)} batters as at {ref_date}.")
  r[, .(rank, player_id, player_name, rating, matches, balls,
        effective_matches, last_match)][]
}
