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
  if (tolower(format) != "test") {
    # T20 and ODI DO have genuinely distinct competitions, so unlike Test this
    # is a rename rather than a partition -- but a competition that changes
    # sponsor changes its event_name while staying the same competition, and
    # fitting a factor per name splits every bridge between the variants.
    # England's domestic T20 alone is 1,554 matches across three names, more
    # than the IPL. Generated from COMPETITION_ALIASES so that map stays the
    # single source of truth.
    esc <- function(x) gsub("'", "''", x, fixed = TRUE)
    whens <- paste(sprintf("WHEN m.event_name = '%s' THEN '%s'",
                           esc(names(COMPETITION_ALIASES)),
                           esc(unname(COMPETITION_ALIASES))),
                   collapse = "
             ")
    # Bilateral tours and short series collapse into four buckets by playing
    # standard. Fitting one factor per series meant 326 competitions off a median
    # of 5 matches, which put Williamson and McCullum in the "weakest
    # competition on record" and rated a 5-match series in Bangladesh harder
    # than the IPL. Named tournaments are unaffected -- see competition_units.R.
    fm <- paste(sprintf("'%s'", gsub("'", "''", COMPETITION_TOP_NATIONS, fixed = TRUE)),
                collapse = ", ")
    wc <- paste(sprintf("'%s'", gsub("'", "''", COMPETITION_WC_ASSOCIATES, fixed = TRUE)),
                collapse = ", ")
    tours <- sprintf(
      "WHEN m.team_type = 'international' AND (%s) THEN
                 CASE WHEN m.team1 IN (%s) AND m.team2 IN (%s)
                        THEN 'International (Top Nations)'
                      WHEN (m.team1 IN (%s) OR m.team2 IN (%s))
                       AND (m.team1 IN (%s) OR m.team1 IN (%s))
                       AND (m.team2 IN (%s) OR m.team2 IN (%s))
                        THEN 'International (Mixed)'
                      WHEN (m.team1 IN (%s) OR m.team1 IN (%s))
                       AND (m.team2 IN (%s) OR m.team2 IN (%s))
                        THEN 'International (Associate)'
                      ELSE 'International (Developing)' END",
      COMPETITION_TOUR_PATTERN_SQL,
      fm, fm,
      fm, fm, fm, wc, fm, wc,
      fm, wc, fm, wc)
    pathway <- sprintf(
      "WHEN m.team_type = 'international' AND (%s) THEN 'ICC Qualifying Pathway'",
      COMPETITION_PATHWAY_PATTERN_SQL)
    # Pathway is tested BEFORE the tour patterns: a name like "ICC Men's T20
    # World Cup Sub Regional Europe Qualifier Group C" would otherwise be caught
    # by the bilateral shapes and land in the wrong bucket.
    return(sprintf("COALESCE(CASE %s
             %s
             %s ELSE m.event_name END, 'unknown')",
                   whens, pathway, tours))
  }

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


# Report competitions the rating could not price, distinguishing the two very
# different reasons a delivery has no factor.
#
# `comp` is NA only when .competition_sql() deliberately returned NULL -- a
# first-class competition absent from COMPETITION_UNIT_MAP. That is a MAP GAP:
# the fit already warns and drops those rows, and a consumer that quietly rates
# them at reference difficulty reproduces, in shape, the 60.5% un-discounted
# incident this machinery was written to fix. Ranji Trophy is not in the map
# today, so the next cricsheet refresh can trigger this.
#
# `comp` present but absent from `fmap` is the ordinary thin-bridge case, which
# D-P23 measured and deliberately leaves at 1.0.
.report_unrated <- function(b, where, col = "cfactor") {
  nmap <- b[is.na(comp), .N]
  if (nmap > 0) {
    cli::cli_warn(c(
      "{where}: {format(nmap, big.mark = ',')} deliver{?y/ies} are in a first-class competition with no normalised unit.",
      "x" = "They are being rated at REFERENCE difficulty, which is almost certainly wrong for a competition nobody has classified.",
      "i" = "Add it to {.field COMPETITION_UNIT_MAP} in {.file R/competition_units.R}."))
  }
  unrated <- b[!is.na(comp) & is.na(get(col)), .N]
  if (unrated > 0) {
    cli::cli_alert_info(
      "{where}: {round(100 * unrated / nrow(b), 1)}% of deliveries are in competitions with no factor; treated as reference difficulty.")
  }
  invisible(NULL)
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
#'   `min_here` is 1 as of 2026-08-19: bridges are now weighted by the harmonic
#'   mean of their two ball counts, so a one-ball bridge earns a weight near zero
#'   on its own and no cutoff is needed. `min_evidence` and `shrink_balls` do the
#'   real work. The plateau note below describes the OLD regime, when the default
#'   was a hard 30 and the cutoff was the only defence.
#'   `min_here` was 60 until 2026-08-16 (D-P23); 60 left 4.1% of deliveries in
#'   competitions with no factor, almost all of them short bilateral T20I
#'   series where no batter reaches 60 balls, and rating those is worth +2.2%
#'   next-game Spearman. The metric is flat from 10 to 40 and falls off only at
#'   60, so this sits mid-plateau rather than at an edge.
#' @param min_evidence Numeric. Effective paired evidence, in balls, required
#'   before a competition is rated. Bridges are weighted by the harmonic mean of
#'   their ball counts in the two competitions (inverse-variance weighting), so
#'   this is a real evidence floor where a headcount is not: three players with
#'   one ball each contribute almost nothing and must not rate a league. The
#'   default is 200; a single match is roughly 230 balls, so this alone does not
#'   stop a one-game competition being rated -- `shrink_balls` is what pulls such
#'   a factor back toward neutral (the HRV Cup, 231 balls over one match, rated
#'   0.82 before shrinkage and 0.99 after). `min_ref` keeps a light
#'   floor because runs-per-dismissal is degenerate for a player with a handful
#'   of reference balls and no dismissal.
#' @param shrink_balls Numeric. Evidence, in balls, at which a fitted factor is
#'   pulled halfway to 1.0. Replaces a hard evidence cutoff with a smooth one, so
#'   a thin competition degrades toward neutral instead of either being trusted
#'   in full or dropped entirely.
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
                                    min_here = 1L,
                                    min_ref = 30L,
                                    min_players = 3L,
                                    min_evidence = 200,
                                    shrink_balls = 1500,
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

  # Shrink each factor toward 1.0 by how much paired evidence stands behind it.
  # A hard evidence cutoff is a step function: 1,499 balls says nothing and
  # 1,501 balls says everything. This is the smooth version, and it is the same
  # medicine the player rating already takes via derive_shrinkage_prior(). A
  # competition with `shrink_balls` of evidence lands halfway between its fitted
  # value and neutral; a well-evidenced league is barely touched.
  shrink <- function(f, evidence) {
    (evidence * f + shrink_balls * 1) / (evidence + shrink_balls)
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
  # Symmetric bridge weighting -------------------------------------------------
  #
  # The factor is a ratio of two pooled averages: these players' record HERE
  # against the same players' record in the reference. Pooling runs and outs on
  # each side separately weights every player by his volume ON THAT SIDE, and
  # the two sides are wildly unequal -- Bopara brings 393 balls to the Nepal
  # Premier League and 4,181 to the reference, while Paudel brings 489 and 114.
  # The reference average therefore reflects career franchise professionals and
  # the local average reflects local players, so the ratio measures the
  # difference between two populations rather than the difficulty of a league.
  # Nepal came out at 0.835 -- harder to score in than the IPL -- and EVERY
  # competition tested was biased the same way, which is also why weak leagues
  # were being discounted far too little downstream.
  #
  # Weighting both sides of each bridge by the harmonic mean of its two ball
  # counts makes every player
  # contribute the same evidence to numerator and denominator. Deliberately NOT
  # a mean of per-player ratios: a player with few dismissals has an explosive
  # average, and averaging such ratios is the D-P37 defect.
  balance <- function(x) {
    x <- data.table::copy(x)
    # HARMONIC MEAN of the two ball counts, which is inverse-variance weighting:
    # the variance of a player's between-league difference goes as
    # (1/n_here + 1/n_ref), so its precision is the harmonic mean over two. A
    # player thin on either side earns almost no say, smoothly, so no arbitrary
    # ball cutoff is needed -- one ball in a league carries a weight near zero
    # rather than being either excluded or counted in full.
    #
    # Bopara has 393 Nepal Premier League balls and 4,181 reference balls;
    # Paudel has 489 and 114. Harmonic weights are 718 and 185, so Bopara has
    # about 4x the say. Under the old pooling the reference side saw 4,181
    # against 114 -- a 37x imbalance that measured squad composition, not
    # league difficulty.
    x[, w := data.table::fifelse(balls > 0 & r_balls > 0,
                                 2 * balls * r_balls / (balls + r_balls), 0)]
    x <- x[w > 0]
    x[, `:=`(runs   = runs   * w / pmax(balls, 1),
             outs   = outs   * w / pmax(balls, 1),
             r_runs = r_runs * w / pmax(r_balls, 1),
             r_outs = r_outs * w / pmax(r_balls, 1),
             balls = w, r_balls = w)]
    x[]
  }

  j <- merge(d[!comp %in% reference & balls >= min_here], ref, by = "batter_id")
  j <- balance(j)
  # `avg()` floors its denominator with pmax(sum(outs), 1). A competition can
  # clear the ball-based min_evidence gate while its weighted DISMISSAL total
  # sits below 1, at which point the floor invents the denominator and the
  # resulting factor still lands inside the [0.5, 4] clamp -- plausible-looking
  # and fabricated, the same shape as the 1.5e11 shrinkage-prior incident.
  #
  # This is not hypothetical: on 2026-08-19 the ECA Men's European Cup (775
  # balls) cleared the gate on ball volume with under one weighted dismissal
  # behind it. Refuse to rate such a competition rather than publish a number
  # resting on a floor, which is how derive_shrinkage_prior() already handles
  # its own unidentified case. An unrated competition falls back to reference
  # difficulty and is reported by .report_unrated().
  .starved <- j[, .(o = sum(outs), ro = sum(r_outs)), by = comp][o < 1 | ro < 1, comp]
  if (length(.starved)) {
    cli::cli_alert_info(paste(
      "Dropping {length(.starved)} competition{?s} with under one weighted dismissal:",
      "{paste(utils::head(.starved, 5), collapse = ', ')}.",
      "A factor there would rest on a floored denominator."))
    j <- j[!comp %in% .starved]
  }
  # `balls` holds the harmonic weight after balance(), so its sum is the
  # effective paired evidence behind the competition, in balls. That replaces a
  # raw headcount: three players with one ball each is not three bridges.
  direct <- j[, .(n_bridges = .N, evidence = sum(balls),
                  factor = avg(runs, outs, balls) / avg(r_runs, r_outs, r_balls)),
              by = comp][n_bridges >= min_players & evidence >= min_evidence]
  # Clamped before the chaining loop reads them as neighbour values, not after.
  direct[, `:=`(factor = clip(shrink(factor, evidence)), step = 0L)]
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

    jj <- balance(jj)
    # Same starvation check as the direct pass. Without it a competition
    # rejected for having under one weighted dismissal against the REFERENCE
    # simply reappears here bridged against a neighbour -- the ECA Men's
    # European Cup did exactly that, dropped at step 0 and rated 1.27 at step 1.
    .starved_c <- jj[, .(o = sum(outs), ro = sum(r_outs)), by = comp][o < 1 | ro < 1, comp]
    if (length(.starved_c)) {
      cli::cli_alert_info(paste(
        "Chaining: dropping {length(.starved_c)} competition{?s} with under one",
        "weighted dismissal: {paste(utils::head(.starved_c, 5), collapse = ', ')}."))
      jj <- jj[!comp %in% .starved_c]
      if (!nrow(jj)) break
    }
    nw <- jj[, .(n_bridges = data.table::uniqueN(batter_id), evidence = sum(balls),
                 factor = avg(runs, outs, balls) / avg(r_runs, r_outs, r_balls) * stats::median(rfac)),
             by = comp][n_bridges >= min_players & evidence >= min_evidence]
    if (!nrow(nw)) break
    # Clamp NOW, not once at the end. `fmap` reads these factors as the
    # neighbour value on the next pass, so an unclamped extreme from a thin
    # cell otherwise propagates through every competition that chains via it
    # and can land back inside the range, uncorrectable.
    nw[, factor := clip(shrink(factor, evidence))]
    nw[, step := s]
    out <- rbind(out, nw)
  }

  out <- out[is.finite(factor) & factor > 0]
  out[, factor := clip(factor)]
  data.table::setorder(out, -factor)
  # Stamp the basis so a caller reusing this object across metrics can be told
  # when it does not match. A runs-basis factor applied to a wickets rating is
  # correctly ordered, plausible, and wrongly calibrated -- the exact shape this
  # codebase keeps getting caught by.
  data.table::setattr(out, "basis", basis)
  cli::cli_alert_success(
    "Rated {nrow(out)} competition{?s} ({sum(out$step == 0)} directly, {sum(out$step > 0)} by chaining).")
  out[]
}

# Map a per-ball value onto the reference scale: RECENTRE, then COMPRESS.
#
#   .competition_adjust(v0, m_here, m_ref, cfactor) = m_ref + (v0 - m_here) / cfactor
#
# One definition used by both the rating and the value function, and by the
# tests. It exists as a function rather than an inline expression because the
# property that matters is not obvious by reading it, and was wrong in
# production until 2026-08-19: for ANY two players with the same raw value, the
# one in the EASIER competition must come out lower -- at negative values as
# well as positive. The old form (`v0 / cfactor`) satisfied that only for
# positive v0 and inverted it for negative, so a below-average batter was made
# better by a weak-league discount. See test-competition-adjust.R.
#
# `m_here` is what an average bridge player scores in the competition and
# `m_ref` what the same players score in the reference, so subtracting m_here
# and adding m_ref moves a player onto the reference scale. Dividing the
# remaining deviation by `cfactor` then compresses it -- see the OPEN QUESTION
# in calculate_player_rating_v2() before treating that second step as settled.
.competition_adjust <- function(v0, m_here, m_ref, cfactor) {
  m_ref + (v0 - m_here) / cfactor
}

#' Competition Difficulty Offsets
#'
#' How much a competition inflates per-ball value relative to the reference set,
#' as an ADDITIVE shift on the same scale the rating aggregates.
#'
#' This replaces the divisive competition factor for the rating, and the reason
#' is a defect rather than a preference. [fit_competition_factors()] estimates a
#' ratio of batting AVERAGES -- a non-negative quantity, where a ratio is the
#' natural form. The rating then divided RVAA by it, and RVAA is a SIGNED
#' deviation. Dividing a negative by 1.6 moves it toward zero, so a below-average
#' batter in a weak league was made to look BETTER by the weak-league discount.
#' On 2026-08-19, 671 of 1,039 below-average male T20 batters with 200+ balls
#' were being helped this way, by a mean of +0.032 RVAA/ball and up to +0.201.
#'
#' The fix is to recentre rather than rescale: subtract what an average bridge
#' player scores in that competition. Three forms were tested against a
#' bridge-prediction target on 1,788 player-competition pairs (T20 men) --
#' additive, additive-then-multiplicative, and a multiplicative form on a
#' non-negative level scale. With the pipeline's shrinkage applied to all three
#' they are indistinguishable (RMSE 0.1400 / 0.1401 / 0.1400) and all beat both
#' the divisive form (0.1423) and no adjustment (0.1439). Additive is taken
#' because it is the only one of the three with no free parameter.
#'
#' Weak leagues DO also spread players out -- the same players' RVAA has SD
#' 0.304 in a weak competition against 0.226 in the reference, a ratio of 1.35 --
#' so a multiplier below 1 on the deviation is real. It is not applied because
#' shrinkage downstream already compresses far harder (the best multiplier
#' against this target is 0.107, and every form independently chose a shrinkage
#' of 850 balls), leaving the extra term worth 0.0001 RMSE.
#'
#' @section Why this is fitted here and not in fit_competition_factors:
#' The offset is subtracted from `raa - opp_eff`, so it must be ESTIMATED on
#' `raa - opp_eff`. Weak competitions are full of weak bowlers, and
#' [fit_two_way_effects()] already removes part of the competition's strength
#' as an opponent effect. An offset fitted on raw RVAA would re-remove what the
#' opponent adjustment has already taken out, and double-discount weak leagues.
#' That is why this takes an already-adjusted ball table rather than a
#' connection.
#'
#' @param b data.table of deliveries carrying `comp`, the bridge id column, and
#'   the adjusted per-ball value. Not modified; only read.
#' @param id_col Character. Column bridging players -- "batter_id" for a batting
#'   offset, "bowler_id" for a bowling one. The two are fitted separately
#'   because a competition can have weak bowling and ordinary batting.
#' @param value_col Character. The already-opponent-adjusted per-ball value.
#' @param reference Character vector of competitions defining the 0.0 anchor.
#' @param min_evidence,shrink_balls,min_players,max_steps,clamp As in
#'   [fit_competition_factors()], and deliberately the same defaults: the two
#'   estimators face the same thin-bridge problem and drifting them apart would
#'   mean two sets of tuning to reason about.
#' @return data.table of `comp`, `offset`, `m_here`, `m_ref`, `n_bridges`,
#'   `evidence`, `step`, where `offset == m_here - m_ref`. `m_here` is what an
#'   average bridge player scores in that competition and `m_ref` is what the
#'   same players score in the reference, so a caller can recentre exactly
#'   (subtract `m_here`, add `m_ref`) rather than only shift. Reference
#'   competitions are present at offset 0, step 0.
#' @export
fit_competition_offsets <- function(b, id_col, value_col, reference,
                                    min_evidence = 200, shrink_balls = 1500,
                                    min_players = 3L, max_steps = 6L,
                                    clamp = c(-0.75, 0.75)) {
  stopifnot(is.data.frame(b), id_col %in% names(b), value_col %in% names(b),
            "comp" %in% names(b), length(clamp) == 2L, clamp[1] < clamp[2])
  x <- data.table::as.data.table(b)[, .(balls = .N, s = sum(get(value_col))),
                                    by = c("comp", id_col)]
  data.table::setnames(x, id_col, "pid")
  x <- x[!is.na(pid) & !is.na(comp) & balls > 0]
  x[, m := s / balls]

  # Shrink toward 0 -- no adjustment -- on the same evidence scale the factor
  # fit uses, so a competition resting on one thin bridge barely moves.
  shrink <- function(c_hat, evidence) evidence * c_hat / (evidence + shrink_balls)
  clip <- function(v) pmin(pmax(v, clamp[1]), clamp[2])

  # Reference competitions anchor the scale: no shift, and both means 0 so a
  # caller's recentring is the identity there.
  out <- data.table::data.table(comp = unique(reference[reference %in% x$comp]),
                                offset = 0, m_here = 0, m_ref = 0,
                                n_bridges = NA_integer_,
                                evidence = NA_real_, step = 0L)
  if (!nrow(out)) {
    cli::cli_abort(c("No reference competition appears in the supplied deliveries.",
                     "i" = "Check {.arg reference} against the {.field comp} column."))
  }

  for (s in seq_len(max_steps)) {
    cmap <- stats::setNames(out$offset, out$comp)
    a <- x[comp %in% out$comp, .(pid, ncomp = comp, n_m = m, n_balls = balls)]
    u <- x[!comp %in% out$comp, .(pid, comp, m, balls)]
    if (!nrow(a) || !nrow(u)) break
    jj <- merge(u, a, by = "pid", allow.cartesian = TRUE)
    if (!nrow(jj)) break

    # ONE ROW PER (player, unrated competition), keeping his best-evidenced
    # neighbour. The cartesian join otherwise gives more say to players who
    # happen to straddle more rated leagues, which is a property of the player
    # and not of the competition being rated -- the same defect the factor
    # chaining loop fixes for the same reason.
    data.table::setorder(jj, comp, pid, -n_balls)
    jj <- jj[, .SD[1L], by = .(comp, pid)]

    # The neighbour's OWN offset comes off first, so what is left is this
    # competition's shift relative to the reference rather than to the
    # neighbour. This is how an offset chains: additively, where a factor
    # chains multiplicatively.
    jj[, n_adj := n_m - cmap[ncomp]]
    # Harmonic mean of the two ball counts = inverse-variance weighting: the
    # variance of a player's between-competition DIFFERENCE goes as
    # (1/n_here + 1/n_there), so its precision is the harmonic mean. A player
    # thin on either side earns almost no say, smoothly, which is why there is
    # no ball cutoff here.
    jj[, w := 2 * balls * n_balls / (balls + n_balls)]
    jj <- jj[w > 0 & is.finite(n_adj)]
    if (!nrow(jj)) break

    nw <- jj[, .(n_bridges = data.table::uniqueN(pid), evidence = sum(w),
                 m_here = sum(w * m) / sum(w),
                 m_ref  = sum(w * n_adj) / sum(w)), by = comp]
    nw[, offset := m_here - m_ref]
    nw <- nw[n_bridges >= min_players & evidence >= min_evidence & is.finite(offset)]
    if (!nrow(nw)) break
    # Clamp NOW, not once at the end: `cmap` reads these as the neighbour value
    # on the next pass, so an unclamped extreme from a thin cell would
    # propagate through everything that chains via it and could land back
    # inside the range, uncorrectable.
    nw[, offset := clip(shrink(offset, evidence))]
    # Keep the identity offset == m_here - m_ref after shrinkage, so a caller
    # recentring with these two means gets exactly the shrunk offset and not a
    # slightly different one. Anchoring on m_here (an observed quantity) rather
    # than m_ref keeps the recentre exact for a player at his league's average.
    nw[, m_ref := m_here - offset]
    # `s` counts loop passes, but the FIRST pass bridges straight to the
    # reference, which is a direct fit and not a chain. Recording it as step 0
    # keeps the same meaning `step` has for factors, where 0 means "measured
    # against the reference itself".
    nw[, step := s - 1L]
    out <- rbind(out, nw)
  }

  data.table::setorder(out, -offset)
  # Stamp the bridge column. A batting offset applied to a bowling rating is
  # correctly ordered, plausible and wrongly calibrated -- the same hazard the
  # `basis` attribute guards for factors, and the reason that guard exists.
  data.table::setattr(out, "id_col", id_col)
  cli::cli_alert_success(
    "Offset {nrow(out)} competition{?s} ({sum(out$step == 0)} directly, {sum(out$step > 0)} by chaining).")
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
  "ICC Men's T20 World Cup", "Vitality Blast"
)

#' @rdname competition_reference
#' @export
COMPETITION_REFERENCE_ODI <- c(
  "ICC Cricket World Cup", "ICC Champions Trophy",
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
  "ECB Women's One-Day Cup", "Women's Ashes",
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
#' @param factors Output of [fit_competition_factors()]; NULL fits them on the
#'   basis `metric` requires. Used ONLY to compress the within-competition
#'   deviation, never to scale the uncentred value -- that was the defect this
#'   replaces. A supplied object must carry the matching `basis` attribute.
#' @param offsets Output of [fit_competition_offsets()]; NULL fits them. Must
#'   have been fitted on the same side as `role` -- checked via its `id_col`
#'   attribute, because a batting offset on a bowling rating is plausible and
#'   wrong.
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
                                       offsets = NULL,
                                       factors = NULL,
                                       min_balls = 500L,
                                       id_map = NULL,
                                       metric = c("composite", "runs", "wickets",
                                                  "team_score")) {
  metric <- match.arg(metric)

  # Validate BEFORE opening a connection or querying 2M rows: a bad argument
  # should fail in milliseconds, not after the expensive part. (A test for this
  # initially passed only because the working directory happened to resolve a
  # database -- the check ran after the query.)
  role <- match.arg(role)
  id_col <- if (role == "batter") "batter_id" else "bowler_id"

  # The factor no longer scales the raw value -- it compresses the deviation
  # from a competition's own mean -- but it is STILL a ratio of per-dismissal
  # rates, and which rate depends on the metric. `wickets` reads r.waa, a
  # survival quantity, so it needs balls-per-dismissal; everything else reads a
  # runs quantity and needs runs-per-dismissal. Dropping this argument when the
  # application site was rewritten silently compressed WAA deviations with a
  # batting-average factor: correctly ordered, plausible, wrongly calibrated.
  want_basis <- if (metric == "wickets") "survival" else "runs"
  if (!is.null(factors)) {
    # `factors` exists to be reused across calls, which is exactly how a
    # runs-basis object reaches a wickets rating. Nothing in the returned table
    # records its basis for a reader, so without this the mismatch is
    # undetectable at the call site.
    got <- attr(factors, "basis")
    if (is.null(got)) {
      cli::cli_warn(c(
        "Supplied {.arg factors} carries no basis attribute, so it cannot be checked against {.val {metric}}.",
        "i" = "Refit with {.fn fit_competition_factors} to stamp it, or pass {.code NULL} to fit here."))
    } else if (!identical(got, want_basis)) {
      cli::cli_abort(c(
        "Supplied {.arg factors} were fitted on the {.val {got}} basis; {.val {metric}} needs {.val {want_basis}}.",
        "x" = "A runs-basis factor on a wickets rating is correctly ordered, plausible and wrongly calibrated.",
        "i" = "Pass {.code factors = NULL} to fit the right basis."))
    }
  }

  # The runs/survival basis split guarded the FACTOR, which was a ratio of
  # averages and so needed a different numerator for a survival metric. An
  # offset is estimated directly on the value being adjusted, so there is no
  # basis to get wrong. The analogous hazard is a BATTING offset reaching a
  # BOWLING rating -- correctly ordered, plausible, wrongly calibrated -- and
  # that is what this checks.
  #
  # Deliberately here, before the connection is opened and before 2M rows are
  # queried: a bad argument should fail in milliseconds. The equivalent check
  # for `factors` was written after the query once, and the test for it passed
  # only because the working directory happened to resolve a database.
  if (!is.null(offsets) && !identical(attr(offsets, "id_col"), id_col)) {
    cli::cli_abort(c(
      "Supplied {.arg offsets} were not fitted on the {.val {role}} side.",
      "x" = "A batting offset on a bowling rating is correctly ordered, plausible and wrongly calibrated.",
      "i" = "Pass {.code offsets = NULL} to fit the right side, or refit with {.fn fit_competition_offsets}."))
  }
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
    composite  = "r.raa",       # runs scale, wicket priced at a flat lambda
    runs       = "r.raa_run",   # runs above average alone
    wickets    = "r.waa",       # wickets above average, unpriced
    team_score = "r.tsa")       # effect on the team's projected final score

  # TSA exists for innings 1 of limited-overs cricket only: a chase truncates
  # the innings so "projected final score" stops being the modelled quantity,
  # and Test has no fixed ball allocation at all. Those rows are NULL and must
  # be excluded rather than treated as zero contribution.
  metric_filter <- if (metric == "team_score") " AND r.tsa IS NOT NULL" else ""
  b <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT r.match_id, r.match_date, r.batter_id, r.bowler_id, %s AS raa,
           %s AS comp
    FROM main.cricsheet_ball_raa r
    JOIN cricsheet.matches m ON m.match_id = r.match_id
    WHERE r.format = '%s' AND r.gender = '%s'%s",
    metric_col, .competition_sql(format), toupper(format), gender, metric_filter)))
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

  # The opponent effect comes off FIRST, because the competition offset is
  # estimated on `raa - opp_eff` and so must be applied to it. Weak competitions
  # are full of weak bowlers and fit_two_way_effects() already removes part of a
  # competition's strength as an opponent effect; subtracting an offset fitted
  # on raw RVAA on top of that would discount weak leagues twice.
  eff <- fit_two_way_effects(b, prior_balls = prior_balls, iterations = iterations)
  if (role == "batter") {
    b[eff$bowler, on = "bowler_id", opp_eff := i.eff]
    sgn <- 1
  } else {
    # RAA is signed from the batting side, so negate: a bowler wants it low.
    b[eff$batter, on = "batter_id", opp_eff := i.eff]
    sgn <- -1
  }
  b[is.na(opp_eff), opp_eff := 0]
  b[, v0 := raa - opp_eff]

  # SUBTRACT a competition offset; do not divide by a competition factor.
  #
  # The factor is a ratio of batting AVERAGES -- non-negative, where a ratio is
  # the natural form -- and RVAA is a SIGNED deviation. Dividing a negative by
  # 1.6 moves it toward zero, so until 2026-08-19 the weak-league discount made
  # a BELOW-average batter look better: 671 of 1,039 below-average male T20
  # batters with 200+ balls were being helped, by up to +0.201 RVAA/ball.
  #
  # Three forms were tested (additive; additive then multiplicative; and
  # multiplicative on a non-negative level scale). Written properly all three
  # are "recentre, then scale the deviation" and differ only in the multiplier;
  # once the shrinkage below is applied they are indistinguishable, so additive
  # is taken as the only one with no free parameter. On next-match Spearman over
  # reference matches this is +2.6% for batters, and +19.2% for the players
  # whose records are 60%+ weak-league cricket -- the ones it exists for.
  if (is.null(factors)) {
    factors <- fit_competition_factors(conn, format, gender, id_map = id_map,
                                       as_at = as_at, basis = want_basis)
  }
  if (is.null(offsets)) {
    offsets <- fit_competition_offsets(
      b, id_col, "v0", default_competition_reference(format, gender))
  }
  b[, m_here := stats::setNames(offsets$m_here, offsets$comp)[comp]]
  b[, m_ref  := stats::setNames(offsets$m_ref,  offsets$comp)[comp]]
  b[, cfactor := stats::setNames(factors$factor, factors$comp)[comp]]
  # An unrated competition keeps the identity -- no shift, no compression.
  # "Unrated implies weak" was tested and rejected (D-P23): most of what went
  # unrated was short bilateral T20I series between full members.
  .report_unrated(b, "calculate_player_rating_v2", "m_here")
  b[is.na(m_here), m_here := 0]
  b[is.na(m_ref),  m_ref  := 0]
  b[is.na(cfactor) | !is.finite(cfactor) | cfactor <= 0, cfactor := 1]

  # RECENTRE, then COMPRESS the deviation.
  #
  #   value = m_ref + (v0 - m_here) / f
  #
  # The recentring is what fixes the sign defect. The compression is why the
  # form is `additive THEN multiplicative` rather than plain additive, and it
  # was added on 2026-08-19 after the plain-additive build was scored:
  #
  # A flat offset is not progressive. Dividing by 1.6 costs a player in
  # proportion to how far above average he is; subtracting 0.26 per ball costs
  # everyone the same. So plain additive correctly stopped rewarding
  # below-average weak-league batters -- the ten biggest fallers were all
  # 0%-reference players dropping 400 to 610 places -- while simultaneously
  # EASING the discount on the best one, moving a batter with 1,354 balls and
  # no reference cricket at all from 7th to 4th in the world.
  #
  # OPEN QUESTION -- the compression term's MECHANISM is not established.
  #
  # It was justified on a measured 1.35x spread ratio (SD 0.304 in a weak
  # competition against 0.226 in the reference for the same players). That
  # number does not survive a within-competition measurement: centring each
  # competition on its own bridge mean first, weak-competition spreads come out
  # SMALLER than reference spreads, not larger (0.198 vs 0.236 above the mean,
  # 0.180 vs 0.252 below). The 1.35 was between-competition variance leaking in
  # -- the pooled estimate never centred each competition. See EB_symmetry.R.
  #
  # What IS established is that the term earns its place empirically:
  #   - it scores best or joint-best in every cell of the next-match Spearman
  #     table (batters +2.6% overall and +19.3% on 60%+ weak-league records;
  #     bowlers +0.9% and +5.8%), against plain recentring and against the old
  #     divisive form;
  #   - without it, plain recentring moved a batter with 1,354 balls and NO
  #     reference cricket at all from 7th to 4th in the world, because a flat
  #     offset is not progressive: dividing by 1.6 costs a player in proportion
  #     to how far above average he is, subtracting 0.26 per ball costs everyone
  #     the same. With it he sits 28th.
  #
  # So it is kept on an anchor plus a metric, with its stated cause withdrawn.
  # The known cost is at the bottom of the range: below a crossover value a
  # weak-competition return is rated ABOVE the same return in the reference.
  # test-competition-adjust.R pins where that crossover is rather than
  # pretending it is not there.
  #
  # A TWO-SIDED multiplier was investigated and is NOT justified. An earlier
  # note here claimed the two sides regress at 0.153 and 0.077; that came from
  # splitting players on the sign of their own noisy deviation and then
  # regressing on that same deviation, which biases each half's slope toward
  # zero by different amounts and manufactured the gap. Redone properly --
  # classifying on one half of a player's record and measuring on the other, so
  # classification noise is independent of measurement noise -- the slopes are
  # 0.137 above and 0.050 below, a difference of 0.087 with se 0.050 (z = 1.74,
  # p = 0.082, n = 601). Not distinguishable, and a quadratic term on the
  # unsplit data earns nothing (F = 0.30, p = 0.585). One multiplier stands.
  # Underpowered rather than refuted, so worth revisiting on more data.
  b[, value := sgn * .competition_adjust(v0, m_here, m_ref, cfactor)]

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
           %s AS comp
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

  # `factors` compresses the within-competition deviation and decides which
  # competitions count as directly calibrated for the `calibrated` share below.
  # It is never applied to the uncentred value -- see the note in
  # calculate_player_rating_v2() for why that was wrong.
  if (is.null(factors)) {
    factors <- fit_competition_factors(conn, format, gender, id_map = id_map,
                                      as_at = as_at)
  }

  eff <- fit_two_way_effects(b, prior_balls = prior_balls, iterations = iterations)
  b[eff$bowler, on = "bowler_id", be := i.eff][is.na(be), be := 0]
  b[eff$batter, on = "batter_id", ae := i.eff][is.na(ae), ae := 0]

  # Two offsets, not one. A competition can have weak bowling and ordinary
  # batting, so the batting and bowling shifts are separate quantities -- and
  # in T20 men they are: the batting offsets span -0.086 to +0.257 while the
  # bowling offsets span -0.187 to +0.087. Each is fitted on exactly the
  # opponent-adjusted value it is subtracted from.
  ref <- default_competition_reference(format, gender)
  b[, v0_bat := raa - be]
  b[, v0_bowl := raa - ae]
  off_bat  <- fit_competition_offsets(b, "batter_id", "v0_bat",  ref)
  off_bowl <- fit_competition_offsets(b, "bowler_id", "v0_bowl", ref)
  b[, h_bat  := stats::setNames(off_bat$m_here,  off_bat$comp)[comp]]
  b[, r_bat  := stats::setNames(off_bat$m_ref,   off_bat$comp)[comp]]
  b[, h_bowl := stats::setNames(off_bowl$m_here, off_bowl$comp)[comp]]
  b[, r_bowl := stats::setNames(off_bowl$m_ref,  off_bowl$comp)[comp]]
  b[, cfactor := stats::setNames(factors$factor, factors$comp)[comp]]
  .report_unrated(b, "calculate_player_value_v2", "h_bat")
  for (cc in c("h_bat", "r_bat", "h_bowl", "r_bowl")) b[is.na(get(cc)), (cc) := 0]
  b[is.na(cfactor) | !is.finite(cfactor) | cfactor <= 0, cfactor := 1]
  # Recentre then compress, exactly as calculate_player_rating_v2() does; see
  # the note there for why the compression term is not optional.
  b[, v_bat  :=  .competition_adjust(v0_bat,  h_bat,  r_bat,  cfactor)]
  b[, v_bowl := -.competition_adjust(v0_bowl, h_bowl, r_bowl, cfactor)]

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
    SELECT d.%1$s AS player_id, %2$s AS comp,
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
derive_shrinkage_prior <- function(pm, min_matches = 5L,
                                   sh_min_matches = 20L, sh_min_players = 40L) {
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
              players = K, share = s2b_raw / (s2b_raw + msw), method = "anova")

  # SPLIT-HALF is preferred where it is measurable, because the ANOVA estimate
  # is systematically too small. Split each player's matches in two, correlate
  # the half-means across players, and the prior follows from
  # k = n_half * (1 - r) / r with no distributional assumption at all.
  #
  # Measured 2026-08-19 on every bucket, split-half against ANOVA:
  #   t20 male batter 35.1 / 25.0    t20 male bowler 57.5 / 39.5
  #   odi male batter 38.0 / 28.5    odi male bowler 35.0 / 26.9
  #   t20 fem batter  24.9 / 14.6    t20 fem bowler  41.4 / 24.5
  #   odi fem batter  19.8 / 15.5    odi fem bowler  16.9 / 20.4
  #   test male batter 23.3 / 18.1   test male bowler 13.6 / 9.9
  # Higher in NINE of ten, by 28-71%, the exception being the smallest bucket.
  # So every rating in the system was under-shrunk, and low-volume players were
  # over-credited -- which is what put a 48-match associate-cricket batter 6th
  # among T20 men.
  #
  # The split alternates over a STABLE sort rather than sampling. That is
  # deterministic without needing a seed, and sidesteps the trap that a seeded
  # split over an unordered query result reproduces nothing.
  # Two separate things, which an earlier version of this comment conflated:
  #   CONTROL FLOW is inherited -- the abort above still governs, so a bucket
  #     whose between-player variance is not identified never reaches here.
  #     That is deliberate: split-half would report r <= 0 anyway, and the
  #     abort carries the better message.
  #   THE ESTIMATE IS REPLACED, not blended. When split-half succeeds, out$k is
  #     overwritten outright and the ANOVA value survives only as k_anova for
  #     diagnostics. Everything downstream reads out$k.
  sh <- tryCatch({
    d <- pm[, .(player_id, match_id, v)]
    keep <- d[, .N, by = player_id][N >= sh_min_matches, player_id]
    d <- d[player_id %in% keep]
    if (data.table::uniqueN(d$player_id) < sh_min_players) NULL else {
      data.table::setorder(d, player_id, match_id)
      d[, .half := rep_len(c(1L, 2L), .N), by = player_id]
      hm <- data.table::dcast(d[, .(m = mean(v), n = .N), by = .(player_id, .half)],
                              player_id ~ .half, value.var = c("m", "n"))
      hm <- hm[stats::complete.cases(hm)]
      r <- suppressWarnings(stats::cor(hm$m_1, hm$m_2))
      nh <- mean(c(hm$n_1, hm$n_2))
      if (!is.finite(r) || r <= 0 || r >= 1) NULL
      else list(k = nh * (1 - r) / r, r = r, nh = nh, players = nrow(hm))
    }
  }, error = function(e) {
    # Never let a genuine failure look like "not enough data". Both used to
    # return NULL, so a dcast collision, a renamed column or an OOM would
    # silently drop the pipeline back onto the estimator we know under-shrinks,
    # with no signal at all.
    cli::cli_warn(c("Split-half prior estimation failed; falling back to ANOVA.",
                    "x" = conditionMessage(e)))
    NULL
  })

  if (!is.null(sh) && (!is.finite(sh$k) || sh$k < 1 || sh$k > 500)) {
    cli::cli_warn(c(
      "Split-half prior {round(sh$k, 1)} is outside the plausible 1-500 band; using ANOVA instead.",
      "i" = "r = {round(sh$r, 3)} on {sh$players} players."))
  }
  if (!is.null(sh) && is.finite(sh$k) && sh$k >= 1 && sh$k <= 500) {
    cli::cli_alert_info(paste(
      "Split-half prior {round(sh$k, 1)} matches (r = {round(sh$r, 3)} on",
      "{sh$players} players, {round(sh$nh)} matches per half);",
      "ANOVA would have given {round(out$k, 1)}."))
    out$k_anova <- out$k
    out$share_anova <- out$share
    out$k <- sh$k
    # Keep `share` consistent with the prior actually in force. share and k are
    # two views of the same quantity (share = 1 / (1 + k)), so leaving the ANOVA
    # share next to a split-half k reported "35.4 matches (3.84%)" when 3.84%
    # is the share implied by k = 25.
    out$share <- 1 / (1 + sh$k)
    out$split_half_r <- sh$r
    out$method <- "split_half"
  }

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
