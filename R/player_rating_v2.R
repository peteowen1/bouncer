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
#     -> competition discount                       +4.9%   (D-P22)
#     -> decayed, shrunk weighted mean              +3%     (D-P20, vs no decay)
#
# For scale, tuning kappa / decay / shrinkage in isolation moved the same metric
# by under 1% (D-P17, D-P18). The adjustments are where the value is.
#
# Two things deliberately NOT here, both measured and rejected:
#   - venue as a crossed effect (-1%, D-P19) and venue in the baseline (0.03% of
#     per-ball variance, so not worth a model retrain)
#   - situational wicket value from the resource surface (-6.6%, #40)

# Which cricsheet match_types a rating bucket covers, as a SQL literal list.
#
# This was written twice as `if (format == "t20") "'t20','it20'" else
# "'odi','odm'"`, which is not a two-way choice -- it is a t20 branch and a
# catch-all. `format = "test"` does not fail there, it silently selects ODI and
# ODM deliveries and returns ODI numbers labelled Test. The only reason that
# has never bitten is that no caller passes "test" yet, which is exactly the
# state in which the trap is easiest to walk into: Test is the one bucket
# currently queued to be added.
#
# Aborting matches what get_raa_lambda() already does for "test" -- it refuses
# rather than guessing a wicket value -- so the two now fail the same way
# instead of one aborting and the other quietly answering the wrong question.
.rating_match_types <- function(format) {
  switch(tolower(format),
    t20  = "'t20','it20'",
    odi  = "'odi','odm'",
    # Test pairs with MDM (domestic first-class) exactly as ODI pairs with ODM,
    # which is also what raa_cricsheet.R's own switch does. Without MDM there is
    # almost no bridge network: 187 Test event_names against 10 MDM ones, but
    # the MDM side carries 2,161 of the 3,047 matches.
    test = "'test','mdm'",
    cli::cli_abort(c(
      "No rating match-types defined for format {.val {format}}.",
      "i" = "Supported: {.val t20}, {.val odi}, {.val test}."))
  )
}

# The SQL expression that identifies a "competition" for a bucket.
#
# For T20 and ODI, raw `event_name` IS the competition -- the IPL and the BBL
# are genuinely different leagues that genuinely different players play in.
#
# For Test it is not, and using it would quietly wreck the bridge network. Test
# cricket is one competition split across ~187 bilateral series names, often
# several for the SAME contest: India v Australia appears as both
# "Border-Gavaskar Trophy" (24 matches) and "India tour of Australia" (16), and
# England v India as "England in India Test Series" (11), "England tour of
# India" (12) AND "Pataudi Trophy" (9). English county cricket is split four
# ways by sponsor across eras. Fitting a factor per name would estimate separate
# strengths for synonyms and split every bridge between them.
#
# competition_units.R already solved this for exactly this pool (2026-08-06) and
# was simply never wired into the rating. The CASE below is GENERATED from
# COMPETITION_UNIT_MAP so that map stays the single source of truth rather than
# being restated in SQL where the two could drift.
.competition_sql <- function(format) {
  if (tolower(format) != "test") return("COALESCE(m.event_name,'unknown')")

  esc <- function(x) gsub("'", "''", x, fixed = TRUE)   # 'LV=' is fine; be safe anyway
  whens <- paste(sprintf("WHEN m.event_name = '%s' THEN '%s'",
                         esc(names(COMPETITION_UNIT_MAP)),
                         esc(unname(COMPETITION_UNIT_MAP))),
                 collapse = "\n             ")
  # Any Test row is the "Test" unit whatever its series is called; an
  # unrecognised first-class event returns NULL rather than a guess, so a new
  # competition surfaces as unrated instead of being folded into a neighbour.
  sprintf("CASE WHEN LOWER(m.match_type) = 'test' THEN 'Test'\n             %s\n             ELSE NULL END",
          whens)
}

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
#' @param format Character. "t20" or "odi"; the match types and the default
#'   reference set both follow from it.
#' @param gender Character. "male" or "female".
#' @param reference Character vector of competitions defining the 1.0 scale;
#'   NULL resolves per bucket via [default_competition_reference()].
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
#' @param id_map Output of [build_player_id_map()]; NULL builds it. Pass one in
#'   to avoid rebuilding it per call.
#' @param as_at Date or NULL. Estimate strength from deliveries on or before
#'   this date only. NULL uses everything, which is correct for a current
#'   rating and WRONG for a backtest: a rolling-origin harness that let this
#'   default would score a rating which already knew how the competitions
#'   turned out, against baselines restricted to pre-origin data.
#'
#' @return data.table of `comp`, `factor`, `n_bridges`, `step` (0 = direct).
#' @export
fit_competition_factors <- function(conn = NULL,
                                    format = "t20",
                                    gender = "male",
                                    reference = NULL,
                                    min_here = 30L,
                                    min_ref = 150L,
                                    min_players = 3L,
                                    max_steps = 6L,
                                    clamp = c(0.5, 4),
                                    id_map = NULL,
                                    as_at = NULL,
                                    basis = c("runs", "survival")) {
  basis <- match.arg(basis)

  own <- is.null(conn)
  if (own) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }
  if (is.null(reference)) reference <- default_competition_reference(format, gender)
  types <- .rating_match_types(format)

  comp_sql <- .competition_sql(format)
  d <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT d.batter_id, %1$s AS comp,
           SUM(d.runs_batter) AS runs, SUM(CAST(d.is_wicket AS INT)) AS outs,
           COUNT(*) AS balls
    FROM cricsheet.deliveries d
    JOIN cricsheet.matches m ON m.match_id = d.match_id
    WHERE LOWER(d.match_type) IN (%2$s) AND m.gender = '%3$s'
      AND COALESCE(m.balls_per_over, 6) = 6 AND COALESCE(d.wides, 0) = 0
      %4$s
    GROUP BY d.batter_id, %1$s", comp_sql, types, gender,
    # Competition strength must be estimated from pre-origin cricket only, or a
    # backtest scores a rating that already knows how the leagues turned out.
    if (is.null(as_at)) "" else
      sprintf("AND d.match_date <= DATE '%s'", format(as.Date(as_at))))))
  if (!nrow(d)) cli::cli_abort("No deliveries for {format}/{gender}.")

  # An unrecognised first-class competition maps to NULL rather than a guess, so
  # it must be reported and dropped here rather than aggregated into one giant
  # NA "competition" that would then bridge against everything.
  if (anyNA(d$comp)) {
    lost <- d[is.na(comp), sum(balls)]
    cli::cli_warn(c(
      "{format(lost, big.mark = ',')} ball{?s} are in a competition with no normalised unit; excluded.",
      "i" = "Add it to {.field COMPETITION_UNIT_MAP} in {.file R/competition_units.R} if it belongs in the pool."))
    d <- d[!is.na(comp)]
  }

  # A split career is counted as two bridge players at half weight each, which
  # weakens exactly the bridges this scale rests on (#43).
  if (is.null(id_map)) id_map <- build_player_id_map(conn)
  canonicalise_player_ids(d, id_map)
  d <- d[, .(runs = sum(runs), outs = sum(outs), balls = sum(balls)),
         by = .(batter_id, comp)]

  ref <- d[comp %in% reference,
           .(r_runs = sum(runs), r_outs = sum(outs), r_balls = sum(balls)),
           by = batter_id][r_balls >= min_ref]
  if (nrow(ref) < 20) {
    cli::cli_abort(c("Only {nrow(ref)} players clear the reference threshold.",
                     "i" = "Check {.arg reference} names against {.field cricsheet.matches.event_name}."))
  }

  clip <- function(x) pmin(pmax(x, clamp[1]), clamp[2])

  # What "easier" means depends on the metric being adjusted, and a batting
  # average is the wrong yardstick for a survival metric.
  #   runs      sum(runs)  / sum(outs)  -- batting average
  #   survival  sum(balls) / sum(outs)  -- balls faced per dismissal
  # Both are ratios where larger means an easier competition, so the chaining,
  # clamping and reference anchoring below are unchanged.
  avg <- if (basis == "runs") {
    function(r, o, b) sum(r) / pmax(sum(o), 1)
  } else {
    function(r, o, b) sum(b) / pmax(sum(o), 1)
  }
  j <- merge(d[!comp %in% reference & balls >= min_here], ref, by = "batter_id")
  direct <- j[, .(n_bridges = .N,
                  factor = avg(runs, outs, balls) / avg(r_runs, r_outs, r_balls)),
              by = comp][n_bridges >= min_players]
  # Clamped before the chaining loop reads them as neighbour values, not after.
  direct[, `:=`(factor = clip(factor), step = 0L)]
  out <- rbind(direct,
               data.table::data.table(comp = reference, factor = 1, n_bridges = NA_integer_,
                                      step = 0L), fill = TRUE)
  out <- out[!duplicated(comp)]
  for (s in seq_len(max_steps)) {
    known <- out$comp
    cand <- d[balls >= min_here]
    a <- cand[comp %in% known,
              .(batter_id, rcomp = comp, r_runs = runs, r_outs = outs, r_balls = balls)]
    u <- cand[!comp %in% known, .(batter_id, comp, runs, outs, balls)]
    if (!nrow(a) || !nrow(u)) break
    jj <- merge(u, a, by = "batter_id", allow.cartesian = TRUE)
    if (!nrow(jj)) break
    fmap <- stats::setNames(out$factor, out$comp)
    jj[, rfac := fmap[rcomp]]

    # ONE ROW PER (player, unrated competition). The cartesian join emits one
    # row per known competition the player also appears in, each carrying the
    # SAME `runs`/`outs` for the unrated comp -- so a pooled sum(runs)/sum(outs)
    # counted a player once per bridge he happened to have rather than once
    # per player, and `median(rfac)` became a median over player-neighbour
    # EDGES rather than over neighbours. Both silently gave more say to players
    # who straddle more rated leagues, which has nothing to do with the
    # competition being rated. Keep each player's best-evidenced neighbour.
    data.table::setorder(jj, comp, batter_id, -r_balls)
    jj <- jj[, .SD[1L], by = .(comp, batter_id)]

    nw <- jj[, .(n_bridges = data.table::uniqueN(batter_id),
                 factor = avg(runs, outs, balls) / avg(r_runs, r_outs, r_balls) * stats::median(rfac)),
             by = comp][n_bridges >= min_players]
    if (!nrow(nw)) break
    # Clamp NOW, not once at the end. `fmap` reads these factors as the
    # neighbour value on the next pass, so an unclamped extreme from a thin
    # cell otherwise propagates through every competition that chains via it
    # and can land back inside the range, uncorrectable.
    nw[, factor := clip(factor)]
    nw[, step := s]
    out <- rbind(out, nw)
  }

  out <- out[is.finite(factor) & factor > 0]
  out[, factor := clip(factor)]
  data.table::setorder(out, -factor)
  cli::cli_alert_success(
    "Rated {nrow(out)} competition{?s} ({sum(out$step == 0)} directly, {sum(out$step > 0)} by chaining).")
  out[]
}

#' Reference Competitions Defining the 1.0 Difficulty Scale
#'
#' One set per bucket. The scale is arbitrary but must be ANCHORED to something
#' that is both hard and well populated, because every other competition is
#' expressed as a ratio against it and reached by chaining outward from it.
#'
#' The four buckets do not take the same kind of anchor, which is why these are
#' not one list with a filter. Men's T20 is anchored on the major franchise
#' leagues, because that is where the best T20 players actually play. ODI has
#' no franchise tier -- its biggest competitions by volume are DOMESTIC one-day
#' cups (Royal London is 14% of all ODI-format balls) which are county
#' standard, so the anchor has to be the elite international tournaments even
#' though they are only about a tenth of the data. Women's cricket is anchored
#' on its top franchise and international events together, since neither alone
#' carries enough volume.
#' @name competition_reference
#' @export
COMPETITION_REFERENCE_T20 <- c(
  "Indian Premier League", "Big Bash League", "Pakistan Super League",
  "SA20", "Caribbean Premier League", "International League T20",
  "ICC Men's T20 World Cup", "Vitality Blast", "NatWest T20 Blast"
)

#' @rdname competition_reference
#' @export
COMPETITION_REFERENCE_ODI <- c(
  "ICC Cricket World Cup", "ICC World Cup", "ICC Champions Trophy",
  "NatWest Series", "ICC Men's Cricket World Cup Super League"
)

#' @rdname competition_reference
#' @export
COMPETITION_REFERENCE_T20_FEMALE <- c(
  "Women's Big Bash League", "Women's Premier League",
  "Women's Cricket Super League", "Charlotte Edwards Cup",
  "Vitality Blast Women", "ICC Women's T20 World Cup",
  "Women's Super Smash"
)

#' @rdname competition_reference
#' @export
COMPETITION_REFERENCE_ODI_FEMALE <- c(
  "ICC Women's World Cup", "ICC Women's Championship",
  "Rachael Heyhoe Flint Trophy", "Women's Ashes",
  "ICC Women's Cricket World Cup"
)

#' @rdname competition_reference
#'
#' @details
#' Test takes a **one-element** reference set, which looks wrong next to the
#' others and is not. The competition key for Test is a normalised unit from
#' [normalise_competition()], not a series name, and every Test match collapses
#' to the single unit `"Test"` -- the ~187 bilateral series names are naming
#' variants of one competition, several of them for the same contest. So there
#' are only six units in the whole pool (Test plus five domestic first-class
#' programmes), and the elite one is the anchor. This is the same logic as ODI's
#' "anchor on the elite tier even though domestic carries the volume", taken to
#' its limit: Test is 31.7% of the balls and County Championship alone is more.
#' @export
COMPETITION_REFERENCE_TEST <- c("Test")

#' Default Reference Set for a Bucket
#' @param format,gender Bucket.
#' @return Character vector of competition names.
#' @export
default_competition_reference <- function(format = "t20", gender = "male") {
  key <- paste(tolower(format), tolower(gender))
  switch(key,
    "t20 male"    = COMPETITION_REFERENCE_T20,
    "odi male"    = COMPETITION_REFERENCE_ODI,
    "test male"   = COMPETITION_REFERENCE_TEST,
    "t20 female"  = COMPETITION_REFERENCE_T20_FEMALE,
    "odi female"  = COMPETITION_REFERENCE_ODI_FEMALE,
    # Deliberately no "test female": 24 matches and zero MDM female rows, so
    # there is no domestic bridge network to place it against. See
    # docs/plans/TEST-LAMBDA-PREDECLARATION.md.
    cli::cli_abort("No reference set defined for {format}/{gender}."))
}

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
#' @param prior_matches Numeric shrinkage toward the population mean, in
#'   matches. NULL derives it per bucket via [derive_shrinkage_prior()], which
#'   is the default because the old hand-set 20 was a men's-T20 number reused
#'   everywhere and is roughly half what that bucket actually wants.
#' @param prior_balls,iterations Passed to [fit_two_way_effects()].
#' @param factors Output of [fit_competition_factors()]; NULL fits them.
#' @param min_balls Integer. Career balls required to appear in the result.
#' @param id_map Output of [build_player_id_map()]; NULL builds it. Player
#'   careers split across a bare-name id and a hash id are merged first (#43),
#'   which affects 2,845 players and 4% of appearances.
#'
#' @return data.table of `rank`, `player_id`, `player_name`, `rating`,
#'   `average`, `main_comp`, `matches`, `balls`, `effective_matches`,
#'   `last_match`, ordered best first. `average` and `main_comp` come from
#'   [player_career_context()] and are context beside the rating, never inputs
#'   to it. `matches` counts innings batted
#'   for a batter and matches played for a bowler — the two roles use different
#'   inclusion rules, each measured (D-P26), so the two ratings rank correctly
#'   within a role but are NOT on a common per-match scale and must not be
#'   added. Use [calculate_player_value_v2()] for a combinable per-match-played
#'   scale (#42).
#' @export
calculate_player_rating_v2 <- function(format = "t20",
                                       gender = "male",
                                       role = c("batter", "bowler"),
                                       conn = NULL,
                                       as_at = NULL,
                                       decay_days = NULL,
                                       prior_matches = NULL,
                                       prior_balls = 60,
                                       iterations = 20L,
                                       factors = NULL,
                                       min_balls = 500L,
                                       id_map = NULL,
                                       metric = c("composite", "runs", "wickets")) {
  metric <- match.arg(metric)

  role <- match.arg(role)
  if (is.null(decay_days)) decay_days <- if (role == "batter") 1095 else 1825

  own <- is.null(conn)
  if (own) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }

  # MUST be the same competition key fit_competition_factors() produced. Those
  # factors are keyed on normalised UNITS for Test, so joining them onto raw
  # event_name silently leaves most deliveries un-discounted: measured at 60.5%,
  # namely every Test series plus the sponsor-named county seasons, all of which
  # then default to reference difficulty and undo the whole adjustment. The
  # coverage warning below is what caught it -- do not silence it.
  # The chosen metric is aliased to `raa` so every stage below -- the two-way
  # opponent fit, the competition divide, the decay and the shrinkage -- is
  # identical whichever is picked. A wickets rating is in WICKETS, not runs.
  #
  # The three L1 metrics (docs/reference/RATING-ARCHITECTURE.md). "composite" is
  # the DEFAULT and is what shipped: raa_run + lambda * waa, on the runs scale.
  # Changing that default would silently move every published rating.
  metric_col <- switch(metric,
    composite = "r.raa",       # runs scale, wicket priced at a flat lambda
    runs      = "r.raa_run",   # runs above average alone
    wickets   = "r.waa")       # wickets above average, unpriced
  b <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, %s AS raa,
           COALESCE(%s, 'unknown') AS comp
    FROM main.cricsheet_ball_raa r
    JOIN cricsheet.matches m ON m.match_id = r.match_id
    WHERE r.format = '%s' AND r.gender = '%s'",
    metric_col, .competition_sql(format), toupper(format), gender)))
  if (is.null(id_map)) id_map <- build_player_id_map(conn)
  canonicalise_player_ids(b, id_map)
  if (!nrow(b)) {
    cli::cli_abort(c("No rows in {.field main.cricsheet_ball_raa} for {format}/{gender}.",
                     "i" = "Run {.fn build_cricsheet_raa} first."))
  }
  if (anyNA(b$raa)) {
    cli::cli_abort(c("{sum(is.na(b$raa))} NA values in {.field {metric_col}}.",
                     "i" = "Rebuild with {.fn build_cricsheet_raa}, or backfill the column."))
  }

  # Truncate BEFORE fitting anything, not after aggregating.
  #
  # `as_at` used to filter only the per-match table at the end, which is
  # harmless at the default (there is no future beyond the last match) and a
  # LEAK for any backtest: at an origin of 2017 the opponent effects and the
  # competition factors were still fitted on 2017-2026 deliveries, so the rating
  # was scored against baselines that only ever saw pre-origin data. Same family
  # as the venue_result_rate self-inclusion leak (#29). A rolling-origin harness
  # is the whole reason as_at exists, so it has to bind here.
  if (!is.null(as_at)) {
    n0 <- nrow(b)
    b <- b[match_date <= as.Date(as_at)]
    if (!nrow(b)) {
      cli::cli_abort("No {format}/{gender} deliveries on or before {as_at}.")
    }
    cli::cli_alert_info(
      "as_at {as_at}: fitting on {format(nrow(b), big.mark = ',')} of {format(n0, big.mark = ',')} deliveries.")
  }

  if (is.null(factors)) {
    # A batting average is the wrong yardstick for a survival metric, so WAA
    # gets its competition strength from balls-per-dismissal instead of
    # runs-per-dismissal. Same construction, same anchors, different numerator.
    factors <- fit_competition_factors(conn, format, gender, id_map = id_map,
                                      as_at = as_at,
                                      basis = if (metric == "wickets") "survival" else "runs")
  }
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
    # tested, not assumed. Applying it the other way round (multiplying)
    # scores 0.0944 against the two-way-adjusted arm's 0.1051 -- a 10.2% LOSS
    # where dividing is a 6.6% gain -- so the direction is established rather
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
  if (is.null(prior_matches)) {
    est <- derive_shrinkage_prior(pm)
    prior_matches <- est$k
    # `share` is NA on the thin-bucket fallback, which printed "NA% of
    # single-match variance is the player" -- say which of the two happened.
    cli::cli_alert_info(if (is.na(est$share)) {
      "Shrinkage prior {round(prior_matches, 1)} match{?es} -- NOT derived: only {est$players} player{?s} cleared the estimation threshold, so this is the hardcoded fallback, not this bucket's own number."
    } else {
      "Derived shrinkage prior {round(prior_matches, 1)} match{?es} ({round(100 * est$share, 2)}% of single-match variance is the player)."
    })
  }
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

  # Where he did it, and what the traditional number says. Carried alongside
  # every rating so the two can be eyeballed together without a second query.
  ctx <- player_career_context(conn, format, gender, role, id_map = id_map)
  r <- merge(r, ctx, by = "player_id", all.x = TRUE)

  data.table::setorder(r, -rating)
  r[, rank := seq_len(.N)]
  cli::cli_alert_success(
    "Rated {nrow(r)} {gender} {toupper(format)} {role}s as at {ref_date}.")
  r[, .(rank, player_id, player_name, rating, average, main_comp,
        matches, balls, effective_matches, last_match)][]
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
#' @param id_map Output of [build_player_id_map()]; NULL builds it. Player
#'   careers split across a bare-name id and a hash id are merged first (#43),
#'   which affects 2,845 players and 4% of appearances.
#' @param min_calibrated Numeric 0-1. Minimum share of a player's career spent
#'   in competitions rated DIRECTLY against the reference set (5+ bridge
#'   players), as opposed to reached by chaining. **Defaults to 0: everyone is
#'   returned and the `calibrated` column is reported instead.**
#'
#'   It defaults off deliberately. Dropping a player is not a rating, and the
#'   case that motivated it is not yet settled: chasing why Karanbir Singh would
#'   not respond to a competition-factor change turned up a data defect —
#'   **31% of his appearances are on a second `player_id`** (#43), so he was
#'   being rated on two-thirds of his career. Re-open the question after the ids
#'   are merged. Raise this above 0 only for a display where you would rather
#'   omit a player than show a number you cannot calibrate.
#'
#' @return data.table of `rank`, `player_id`, `player_name`, `total_value`,
#'   `bat_value`, `bowl_value`, `matches`, `bat_balls`, `bowl_balls` and
#'   `calibrated`, ordered best first.
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
                                      min_calibrated = 0,
                                      id_map = NULL) {

  own <- is.null(conn)
  if (own) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }

  # MUST be the same competition key fit_competition_factors() produced. Those
  # factors are keyed on normalised UNITS for Test, so joining them onto raw
  # event_name silently leaves most deliveries un-discounted: measured at 60.5%,
  # namely every Test series plus the sponsor-named county seasons, all of which
  # then default to reference difficulty and undo the whole adjustment. The
  # coverage warning below is what caught it -- do not silence it.
  b <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, r.raa,
           COALESCE(%s, 'unknown') AS comp
    FROM main.cricsheet_ball_raa r
    JOIN cricsheet.matches m ON m.match_id = r.match_id
    WHERE r.format = '%s' AND r.gender = '%s'",
    .competition_sql(format), toupper(format), gender)))
  if (is.null(id_map)) id_map <- build_player_id_map(conn)
  canonicalise_player_ids(b, id_map)
  if (!nrow(b)) {
    cli::cli_abort(c("No rows in {.field main.cricsheet_ball_raa} for {format}/{gender}.",
                     "i" = "Run {.fn build_cricsheet_raa} first."))
  }

  # Truncate BEFORE fitting anything, not after aggregating.
  #
  # `as_at` used to filter only the per-match table at the end, which is
  # harmless at the default (there is no future beyond the last match) and a
  # LEAK for any backtest: at an origin of 2017 the opponent effects and the
  # competition factors were still fitted on 2017-2026 deliveries, so the rating
  # was scored against baselines that only ever saw pre-origin data. Same family
  # as the venue_result_rate self-inclusion leak (#29). A rolling-origin harness
  # is the whole reason as_at exists, so it has to bind here.
  if (!is.null(as_at)) {
    n0 <- nrow(b)
    b <- b[match_date <= as.Date(as_at)]
    if (!nrow(b)) {
      cli::cli_abort("No {format}/{gender} deliveries on or before {as_at}.")
    }
    cli::cli_alert_info(
      "as_at {as_at}: fitting on {format(nrow(b), big.mark = ',')} of {format(n0, big.mark = ',')} deliveries.")
  }

  if (is.null(factors)) {
    factors <- fit_competition_factors(conn, format, gender, id_map = id_map,
                                      as_at = as_at)
  }
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
             intersect(factors$comp, default_competition_reference(format, gender)))
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
  res <- out[, .(rank, player_id, player_name, total_value, bat_value, bowl_value,
                 matches, bat_balls, bowl_balls, calibrated)][]
  # Carried so a caller storing this cannot silently stamp it with today's date
  # instead of the date the data actually runs to.
  data.table::setattr(res, "as_at", ref_date)
  res
}

#' Career Context for a Rating Table
#'
#' The competition a player has played most of his cricket in, and his
#' conventional average. Both are for orientation beside a rating, not inputs
#' to it — a rating says how good he is, these say where he did it and what
#' the traditional number looks like.
#'
#' Averages follow the ordinary cricket definitions rather than anything
#' bespoke:
#' \itemize{
#'   \item batting = runs / dismissals, where retired hurt and retired not out
#'     are NOT dismissals but retired out is.
#'   \item bowling = runs conceded / wickets, counting only wickets CREDITED to
#'     the bowler (caught, bowled, lbw, caught and bowled, stumped, hit
#'     wicket — never a run out), and runs off the bat plus wides and no-balls
#'     but not byes or leg-byes.
#' }
#' Returns `NA` for a bowler who has never taken a wicket, rather than
#' infinity or a fabricated zero.
#'
#' @param conn DBI connection.
#' @param format,gender,role Bucket.
#' @param id_map Output of [build_player_id_map()], so a split career is not
#'   counted as two players.
#' @return data.table of `player_id`, `main_comp`, `main_comp_share`, `average`.
#' @export
player_career_context <- function(conn, format = "t20", gender = "male",
                                  role = c("batter", "bowler"), id_map = NULL) {
  role <- match.arg(role)
  types <- .rating_match_types(format)
  who <- if (role == "batter") "batter_id" else "bowler_id"

  # Bowler-credited dismissals only; a run out is nobody's wicket.
  bowler_kinds <- "'caught','bowled','lbw','caught and bowled','stumped','hit wicket'"
  # Retirements that are not dismissals for batting-average purposes.
  not_out_kinds <- "'retired hurt','retired not out'"

  runs_expr <- if (role == "batter") "d.runs_batter" else
    "d.runs_batter + COALESCE(d.wides,0) + COALESCE(d.noballs,0)"
  outs_expr <- if (role == "batter") {
    sprintf("CASE WHEN d.player_out_id = d.%s AND COALESCE(d.wicket_kind,'') NOT IN (%s) THEN 1 ELSE 0 END",
            who, not_out_kinds)
  } else {
    sprintf("CASE WHEN COALESCE(d.wicket_kind,'') IN (%s) THEN 1 ELSE 0 END", bowler_kinds)
  }

  # Same normalisation as the competition fit: for Test, `main_comp` should read
  # "Test" or "County Championship", not whichever of three names that series
  # happened to be filed under.
  comp_sql <- .competition_sql(format)
  x <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT d.%1$s AS player_id, COALESCE(%2$s, 'unknown') AS comp,
           COUNT(*) AS balls, SUM(%3$s) AS runs, SUM(%4$s) AS outs
    FROM cricsheet.deliveries d
    JOIN cricsheet.matches m ON m.match_id = d.match_id
    WHERE LOWER(d.match_type) IN (%5$s) AND m.gender = '%6$s'
      AND COALESCE(m.balls_per_over, 6) = 6 AND d.%1$s IS NOT NULL
    GROUP BY d.%1$s, %2$s", who, comp_sql, runs_expr, outs_expr, types, gender)))
  # Typed, not bare: the caller merges this by "player_id", and a zero-COLUMN
  # data.table fails that merge with an error naming the missing key rather
  # than the empty query behind it.
  if (!nrow(x)) return(data.table::data.table(
    player_id = character(), main_comp = character(),
    main_comp_share = numeric(), average = numeric()))

  if (is.null(id_map)) id_map <- build_player_id_map(conn)
  data.table::setnames(x, "player_id", "batter_id")
  canonicalise_player_ids(x, id_map, cols = "batter_id")
  data.table::setnames(x, "batter_id", "player_id")
  x <- x[, .(balls = sum(balls), runs = sum(runs), outs = sum(outs)),
         by = .(player_id, comp)]

  data.table::setorder(x, player_id, -balls)
  main <- x[, .(main_comp = comp[1],
                main_comp_share = balls[1] / sum(balls)), by = player_id]
  tot <- x[, .(runs = sum(runs), outs = sum(outs)), by = player_id]
  tot[, average := ifelse(outs > 0L, runs / outs, NA_real_)]
  merge(main, tot[, .(player_id, average)], by = "player_id")[]
}

#' Derive the Shrinkage Prior from the Data
#'
#' How many matches of a player's own record it takes to outweigh the
#' population. In the standard empirical-Bayes form an estimate is shrunk by
#' `n / (n + k)` with `k = sigma^2_within / sigma^2_between` — so `k` is
#' exactly what `prior_matches` means, and it is derivable rather than a free
#' parameter.
#'
#' Deriving it matters because the hand-set 20 was tuned on men's T20 and
#' applied unchanged to ODI and to women's cricket, which carry different
#' information per match. For men's T20 batting the derivation returns **39.9**
#' where the next-match harness independently prefers **40** — two unrelated
#' lines of evidence agreeing, and both saying the shipped 20 was half what it
#' should be.
#'
#' Estimated by unbalanced one-way ANOVA (method of moments). The tempting
#' shortcut — `var(player means) - sigma^2_within / harmonic_n` — is badly
#' biased when group sizes are skewed: it drove `sigma^2_between` to zero in
#' every bucket, implying players do not differ at all, and produced a "prior"
#' of 145 billion matches. The sanity check that catches this is the implied
#' player share of single-match variance, which comes out 2.4–5.5% here against
#' the 2.2% measured independently in D-P17.
#'
#' That check is now **in the code**, not just in this note. It was described
#' here as though it had been built, while the function floored
#' `sigma^2_between` at `1e-9` and returned whatever fell out — which
#' reproduces the 145-billion prior exactly, since `msw` is ~148 on men's T20.
#' A prior that large is not a visibly broken number: every player collapses
#' onto the population mean, so the leaderboard still ranks in the right order
#' with fabricated spread, and the rank-based anchor check cannot see it.
#' `derive_shrinkage_prior()` therefore **aborts** when `msb <= msw` (the
#' between-player variance is not identified) and **warns** when the implied
#' share falls outside 0.5–25%, a band far wider than anything measured.
#' Covered by `tests/testthat/test-player-rating-v2-prior.R`, which also
#' recovers a known `k` from simulated data with known variances.
#'
#' Note the harness prefers a much SMALLER prior in ODI and women's buckets
#' (usually 5, the edge of the grid). That is not a contradiction: the harness
#' only scores players with 10+ prior matches, so it sees established players
#' for whom shrinkage is pure bias, while this minimises error across all
#' players including thin ones. An optimum sitting on a grid boundary is a
#' warning, not a result.
#'
#' @param pm data.table of per-match values with `player_id` and `v`.
#' @param min_matches Integer. Players below this are ignored for estimation
#'   only; they are still rated.
#' @return list with `k`, `s2_within`, `s2_between`, `players`, `share`.
#' @export
derive_shrinkage_prior <- function(pm, min_matches = 5L) {
  s <- pm[, .(n = .N, m = mean(v), ss = sum((v - mean(v))^2)), by = player_id]
  s <- s[n >= min_matches]
  if (nrow(s) < 30L) {
    cli::cli_warn("Only {nrow(s)} player{?s} clear {min_matches} matches; falling back to 20.")
    return(list(k = 20, s2_within = NA_real_, s2_between = NA_real_,
                players = nrow(s), share = NA_real_))
  }
  N <- sum(s$n); K <- nrow(s)
  grand <- sum(s$n * s$m) / N
  msw <- sum(s$ss) / (N - K)
  msb <- sum(s$n * (s$m - grand)^2) / (K - 1)
  n0 <- (N - sum(s$n^2) / N) / (K - 1)
  s2b_raw <- (msb - msw) / n0

  # The sanity check the notes above credit for catching the 145-billion prior
  # -- it was described but never actually written down, and a bare
  # `max(s2b_raw, 1e-9)` floor RECREATES that incident exactly: msw is ~148 on
  # men's T20, so a floored s2b returns k = 148/1e-9 = 1.5e11. At that k every
  # player collapses onto the population mean, and the result is not an
  # obviously broken number -- it is a full leaderboard in the right ORDER with
  # fake spread, which the rank-based anchor check in 01_build_player_ratings_v2.R
  # cannot see. Refuse to return a prior rather than return that.
  if (!is.finite(s2b_raw) || s2b_raw <= 0) {
    cli::cli_abort(c(
      "Between-player variance is not identified for this bucket; refusing to derive a prior.",
      "x" = "msb {round(msb, 3)} <= msw {round(msw, 3)} (n0 {round(n0, 2)}, {K} players).",
      "i" = "Every player would shrink onto the population mean and the leaderboard would rank correctly with fabricated spread.",
      "i" = "Pass an explicit {.arg prior_matches} if you intend to rate this bucket anyway."))
  }

  out <- list(k = msw / s2b_raw, s2_within = msw, s2_between = s2b_raw,
              players = K, share = s2b_raw / (s2b_raw + msw))

  # Measured 2.2-5.5% across the six buckets that have enough data. Outside a
  # band far wider than that, the estimate is telling you something about the
  # bucket, not about the players -- so say so rather than quietly using it.
  if (out$share < 0.005 || out$share > 0.25) {
    cli::cli_warn(c(
      "Implied player share of single-match variance is {round(100 * out$share, 2)}%, outside the plausible 0.5-25% band.",
      "!" = "Derived prior is {round(out$k, 1)} matches on {K} players; treat this bucket's spread as unverified."))
  }
  out
}
