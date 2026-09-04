# Bouncer's Own Win Probability, Computed for Every Cricinfo Delivery
#
# This is the producer side of DECISIONS.md D-P6. player_game_data.R has always
# built batting_wpa/bowling_wpa by LEAD()-differencing
# cricinfo.balls.win_probability -- ESPNcricinfo's scraped forecast, not ours.
# That column covers 15% of the corpus (T20 42.8%, ODI 7.7%, Test 0.0%) and is
# missing whole-match rather than scattered, so 2,711 of 3,757 matches arrive at
# calculate_epr() as NA.
#
# This file computes our own number for every T20 and ODI delivery and stores it
# so the aggregation can join to it instead.


# One source of truth for the table shape (mirrors .cricsheet_wp_schema in
# cricsheet_win_probability.R). See store_cricinfo_win_probability()'s own
# comment for why this table's writer no longer drops it on a shape mismatch.
.cricinfo_wp_schema <- c(
  id = "VARCHAR", match_id = "VARCHAR", innings_number = "INTEGER",
  over_number = "DOUBLE", ball_number = "INTEGER", format = "VARCHAR",
  win_prob_before = "DOUBLE", win_prob_after = "DOUBLE", delta_wp = "DOUBLE",
  proj_score_before = "DOUBLE", proj_score_after = "DOUBLE", delta_ps = "DOUBLE"
)


#' Build Bouncer Win Probability for Every Cricinfo Delivery
#'
#' Scores every delivery of a limited-overs format from `cricinfo.balls` with
#' bouncer's own in-match models and writes the result to
#' `main.bouncer_wp_from_cricinfo`.
#'
#' @section Why the join key is `id`:
#' `(match_id, innings_number, over_number, ball_number)` is **not** unique in
#' `cricinfo.balls`: 546,034 T20/ODI rows collapse to 546,028 distinct
#' composites, six of them duplicated in match `1099000` innings 1 over 30.
#' Joining on the composite would silently multiply those rows in any
#' downstream aggregate. `id` is unique across all 546,034 and is what this
#' table keys on.
#'
#' @section Momentum comes from the training helper, zero-fill included:
#' The 14 momentum features are computed with [calculate_rolling_features()] --
#' the same function `data-raw/models/in-match/01_prepare_all_formats.R` uses to
#' build the training set. That function zero-fills the incomplete windows at
#' the start of an innings (`feature_engineering.R:93-97`), which in isolation
#' is the exact hazard that broke the serving path: zero means "no runs and no
#' wickets in the last N balls", a real and extreme state.
#'
#' It is nonetheless correct to keep it here, because the models were *trained*
#' on those zero-filled early-innings rows. Reproducing training's treatment is
#' what makes train and serve agree; substituting the scalar path's run-rate
#' imputation would introduce a skew rather than remove one. Note that this
#' means [predict_win_probability()]'s no-history fallback and this path
#' deliberately differ for the first 24 balls of an innings.
#'
#' @param format Character. "t20" or "odi". Test is not supported -- it runs
#'   through the decomposed `predict_test_win_probability()`, which
#'   [predict_win_probability_batch()] rejects rather than mishandles.
#' @param conn DBI connection. If NULL, opens one (write access when
#'   `write = TRUE`) and closes it on exit.
#' @param models_path Character. Directory holding the in-match models. NULL
#'   resolves via [load_in_match_models()], which derives it from the database
#'   path -- pass it explicitly when running against a dev checkout whose
#'   models are not in the user data directory.
#' @param write Logical. Write the table, or just return the scored frame.
#' @param table_name Character. Target table in the `main` schema.
#' @param return_pre_states Logical. When TRUE, attaches the already-built
#'   pre-delivery state frame (and the loaded models) as attributes on the
#'   returned data.table -- `attr(x, "pre_states")`, `attr(x, "models")`,
#'   `attr(x, "scoreable")`, `attr(x, "mom_cols")`, `attr(x, "balls_out")`
#'   (the full internal `balls` table, pre-state columns included). Lets a
#'   caller (e.g. [build_ball_leverage()]) reuse this function's pre-state
#'   construction -- momentum, venue stats, target derivation, gap handling --
#'   instead of a second, drifting copy of ~150 lines of state-building logic.
#'   Default FALSE: zero effect on the return value or on `write`'s output for
#'   every existing caller.
#'
#' @return data.table with `id`, `match_id`, `innings_number`, `over_number`,
#'   `ball_number`, `format` and `win_probability` (P(batting-first team wins),
#'   0-1), invisibly when `write = TRUE`.
#'
#' @export
build_cricinfo_win_probability <- function(format = c("t20", "odi"),
                                           conn = NULL,
                                           models_path = NULL,
                                           write = TRUE,
                                           table_name = "bouncer_wp_from_cricinfo",
                                           return_pre_states = FALSE) {

  format <- match.arg(format)
  db_format <- toupper(format)

  own_conn <- is.null(conn)
  if (own_conn) {
    conn <- get_db_connection(read_only = !write)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }

  models <- load_in_match_models(format, models_path = models_path)
  if (is.null(models)) {
    cli::cli_abort(c(
      "Could not load in-match models for {.val {format}}.",
      "i" = "Run {.path data-raw/models/in-match/} first, or pass {.arg models_path}."
    ))
  }

  cli::cli_alert_info("Loading {db_format} deliveries from cricinfo.balls...")

  balls <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT b.id,
           b.match_id,
           b.innings_number        AS innings,
           b.over_number           AS over,
           b.ball_number           AS ball,
           b.overs_actual,
           b.total_innings_runs    AS score,
           b.total_innings_wickets AS wickets,
           COALESCE(b.batsman_runs, 0) + COALESCE(b.wides, 0) + COALESCE(b.noballs, 0)
             + COALESCE(b.byes, 0) + COALESCE(b.legbyes, 0) + COALESCE(b.penalties, 0)
             AS runs_total,
           COALESCE(b.is_four,   FALSE) AS is_four,
           COALESCE(b.is_six,    FALSE) AS is_six,
           COALESCE(b.is_wicket, FALSE) AS is_wicket,
           CASE WHEN m.gender = 'female' THEN 0 ELSE 1 END AS gender_male,
           m.ground_name
    FROM cricinfo.balls b
    JOIN cricinfo.matches m ON m.match_id = b.match_id
    WHERE m.format = '%s'
      AND b.innings_number IN (1, 2)
      AND b.over_number IS NOT NULL
      AND b.ball_number IS NOT NULL
      AND b.overs_actual IS NOT NULL
      AND b.total_innings_runs IS NOT NULL
      AND b.total_innings_wickets BETWEEN 0 AND 10
  ", db_format)))

  if (nrow(balls) == 0) {
    cli::cli_abort("No scoreable {db_format} deliveries found in {.field cricinfo.balls}.")
  }

  if (anyDuplicated(balls$id)) {
    cli::cli_abort("{.field cricinfo.balls.id} is not unique for {db_format} -- it is this table's join key.")
  }

  cli::cli_alert_info("{nrow(balls)} deliveries across {data.table::uniqueN(balls$match_id)} matches.")

  # Momentum, exactly as training computes it (see @section above).
  balls <- data.table::as.data.table(calculate_rolling_features(balls))
  mom_cols <- grep("_last_", names(balls), value = TRUE)

  # Chase target: the first innings total plus one, per match.
  #
  # Two sources, in this order:
  #   1. The ball sequence -- max(total_innings_runs) over innings 1.
  #   2. cricinfo.innings.total_runs, the scorecard's own innings total.
  #
  # Source 2 exists because 371 T20/ODI matches have second-innings deliveries
  # but no first-innings ones, so source 1 cannot produce a target and their
  # whole chase went unscored. All 371 have a scorecard total.
  #
  # Source 1 is preferred where both exist so that adding the fallback changes
  # no ball that already scored. They agree exactly for 2,531 of 2,583 matches;
  # the 52 that disagree are reported rather than silently resolved, because a
  # scorecard total ABOVE the ball-derived one means the ball data is truncated,
  # and a chase scored against a too-low target is wrong in a way nothing else
  # would surface.
  inn1_balls <- balls[innings == 1L, .(target_balls = max(score, na.rm = TRUE) + 1),
                      by = match_id]

  inn1_card <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT i.match_id, MAX(i.total_runs) + 1 AS target_card
    FROM cricinfo.innings i
    JOIN cricinfo.matches m ON m.match_id = i.match_id
    WHERE m.format = '%s' AND i.innings_number = 1 AND i.total_runs IS NOT NULL
    GROUP BY i.match_id
  ", db_format)))

  targets <- merge(inn1_balls, inn1_card, by = "match_id", all = TRUE)
  targets[, target := data.table::fifelse(is.na(target_balls), target_card, target_balls)]

  disagree <- targets[!is.na(target_balls) & !is.na(target_card) &
                        abs(target_card - target_balls) > 5]
  if (nrow(disagree) > 0) {
    cli::cli_warn(c(
      "{nrow(disagree)} match{?es} disagree by more than 5 runs between the ball-derived and scorecard first-innings total.",
      "i" = "Ball-derived is used. A scorecard total that is HIGHER suggests truncated ball data and a chase scored against a too-low target.",
      "i" = "Worst: {.val {disagree[which.max(abs(target_card - target_balls)), match_id]}} ({disagree[which.max(abs(target_card - target_balls)), target_balls - 1]} balls vs {disagree[which.max(abs(target_card - target_balls)), target_card - 1]} card)."
    ))
  }

  recovered <- targets[is.na(target_balls) & !is.na(target_card), .N]
  if (recovered > 0) {
    cli::cli_alert_info(
      "{recovered} match{?es} had no first-innings deliveries; target recovered from the scorecard."
    )
  }

  balls <- merge(balls, targets[, .(match_id, target)], by = "match_id", all.x = TRUE)

  unscoreable <- balls[innings == 2L & is.na(target), .N]
  if (unscoreable > 0) {
    cli::cli_warn(c(
      "{unscoreable} second-innings deliveries have no first-innings total from either source and are left unscored.",
      "i" = "They will be NA in {.field {table_name}} rather than scored against an assumed target."
    ))
  }

  scoreable <- balls[innings == 1L | !is.na(target), which = TRUE]

  # Real per-venue statistics, computed from cricinfo's own history rather than
  # joined to the training-side venue table: only 74 of 232 cricinfo ODI ground
  # names match those 170 venues, so the join would silently default two thirds
  # of grounds back to the format average. Computing them here keeps the
  # identifier space consistent at the cost of being cricinfo-only.
  venue <- balls[innings == 1L, .(inn1_total = max(score, na.rm = TRUE)),
                 by = .(ground_name, match_id)][
                   , .(venue_avg_score = mean(inn1_total, na.rm = TRUE),
                       venue_matches = .N), by = ground_name]

  outcomes <- merge(
    balls[innings == 1L, .(i1 = max(score, na.rm = TRUE)), by = .(ground_name, match_id)],
    balls[innings == 2L, .(i2 = max(score, na.rm = TRUE)), by = .(ground_name, match_id)],
    by = c("ground_name", "match_id"))
  chase_rate <- outcomes[i1 != i2, .(venue_chase_success_rate = mean(i2 > i1),
                                     venue_avg_second_innings = mean(i2)), by = ground_name]
  venue <- merge(venue, chase_rate, by = "ground_name", all.x = TRUE)

  # A ground with a handful of matches gives a chase rate of 0 or 1, which is a
  # confident lie. Shrink toward the format default by match count.
  defaults <- get_default_venue_stats(format)
  PRIOR <- 10
  venue[, w := venue_matches / (venue_matches + PRIOR)]
  venue[, venue_avg_score := w * venue_avg_score + (1 - w) * defaults$avg_first_innings]
  venue[, venue_chase_success_rate := data.table::fifelse(
    is.na(venue_chase_success_rate), defaults$chase_win_rate %||% 0.45,
    w * venue_chase_success_rate + (1 - w) * (defaults$chase_win_rate %||% 0.45))]
  venue[, venue_avg_second_innings := data.table::fifelse(
    is.na(venue_avg_second_innings), defaults$avg_second_innings %||% defaults$avg_first_innings,
    w * venue_avg_second_innings + (1 - w) * (defaults$avg_second_innings %||% defaults$avg_first_innings))]

  balls <- merge(balls, venue[, .(ground_name, venue_avg_score,
                                  venue_chase_success_rate, venue_avg_second_innings)],
                 by = "ground_name", all.x = TRUE)
  data.table::setorder(balls, match_id, innings, over, ball, id)

  # Real first-innings wickets, not the assumed 10.
  i1w <- balls[innings == 1L, .(innings1_wickets = max(wickets, na.rm = TRUE)), by = match_id]
  balls <- merge(balls, i1w, by = "match_id", all.x = TRUE)
  balls[is.na(innings1_wickets), innings1_wickets := 10]
  data.table::setorder(balls, match_id, innings, over, ball, id)
  scoreable <- balls[innings == 1L | !is.na(target), which = TRUE]

  states <- data.frame(
    current_score = balls$score[scoreable],
    wickets       = balls$wickets[scoreable],
    overs         = balls$overs_actual[scoreable],
    innings       = balls$innings[scoreable],
    target        = balls$target[scoreable],
    gender_male   = balls$gender_male[scoreable],
    venue_avg_score          = balls$venue_avg_score[scoreable],
    venue_chase_success_rate = balls$venue_chase_success_rate[scoreable],
    venue_avg_second_innings = balls$venue_avg_second_innings[scoreable],
    innings1_wickets         = balls$innings1_wickets[scoreable]
  )
  states <- cbind(states, as.data.frame(balls[scoreable, ..mom_cols]))

  cli::cli_alert_info("Scoring...")
  t0 <- Sys.time()
  scored <- predict_win_probability_batch(states, format = format, models = models,
                                          detail = TRUE)
  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))

  balls[, `:=`(win_prob_after = NA_real_, proj_score_after = NA_real_)]
  balls[scoreable, `:=`(win_prob_after   = scored$win_prob,
                        proj_score_after = scored$projected_score)]

  n_scored <- sum(!is.na(balls$win_prob_after))
  cli::cli_alert_success(
    "Scored {n_scored}/{nrow(balls)} deliveries in {round(elapsed, 1)}s ({round(1000 * elapsed / max(n_scored, 1), 3)} ms/ball)."
  )

  # --- Pre-delivery state, and the delta that follows from it ---------------
  #
  # The scored number above is the state AFTER the delivery: it is built from
  # total_innings_runs and total_innings_wickets, both of which already include
  # the current ball (verified: on the first ball of an innings the cumulative
  # total equals that ball's runs, and total_innings_wickets jumps by 0.993 on
  # wicket balls and 0.000 otherwise).
  #
  # The "before" number is the model evaluated at THIS ball's own pre-delivery
  # state -- score minus this ball's runs, wickets minus this ball's wicket,
  # one ball earlier on the clock -- the same epv_delta construction torp and
  # panna use. It is NOT the previous row's "after". Differencing adjacent
  # rows looks equivalent and is not: cricinfo ball data has gaps (match
  # 1384429 is missing overs 31-32 of its chase), and a LAG across a gap
  # charges every unrecorded ball's drift to whoever bowls next -- one dot
  # ball there carried a -0.537 delta and put an ICC top-10 bowler 94th of 98
  # on the impact rating (bouncerverse#28). Under the own-pre-state
  # construction a gap's drift lands on no delivery at all.
  #
  # LEAD(wp) - wp is more wrong still -- it credits each swing to the previous
  # delivery's batter and bowler (the 2026-08-13 off-by-one, d7ffbf5).
  #
  # Momentum for the pre-state is the previous ball's window (lagged one ball,
  # zero at innings start, matching calculate_rolling_features() there). Across
  # a data gap the lagged window is stale by the gap's length -- bounded and
  # in-distribution, unlike the state, which is exact from the ball's own row.
  data.table::setorder(balls, match_id, innings, over, ball, id)

  pre_mom_cols <- paste0(mom_cols, "_pre")
  for (nm in mom_cols) {
    balls[, (paste0(nm, "_pre")) := data.table::shift(get(nm), 1L, type = "lag", fill = 0),
          by = .(match_id, innings)]
  }
  balls[, `:=`(
    score_pre   = score - runs_total,
    wickets_pre = pmax(wickets - as.integer(is_wicket), 0L),
    # overs in cricket notation, one ball earlier; ball 1 of an over rolls
    # back to the completed previous over
    overs_pre   = (over - 1) + (pmin(ball, 6) - 1) / 10
  )]

  pre_states <- data.frame(
    current_score = balls$score_pre[scoreable],
    wickets       = balls$wickets_pre[scoreable],
    overs         = balls$overs_pre[scoreable],
    innings       = balls$innings[scoreable],
    target        = balls$target[scoreable],
    gender_male   = balls$gender_male[scoreable],
    venue_avg_score          = balls$venue_avg_score[scoreable],
    venue_chase_success_rate = balls$venue_chase_success_rate[scoreable],
    venue_avg_second_innings = balls$venue_avg_second_innings[scoreable],
    innings1_wickets         = balls$innings1_wickets[scoreable]
  )
  pre_mom <- as.data.frame(balls[scoreable, ..pre_mom_cols])
  names(pre_mom) <- mom_cols
  pre_states <- cbind(pre_states, pre_mom)

  pre_scored <- predict_win_probability_batch(pre_states, format = format,
                                              models = models, detail = TRUE)

  balls[, `:=`(win_prob_before = NA_real_, proj_score_before = NA_real_)]
  balls[scoreable, `:=`(win_prob_before   = pre_scored$win_prob,
                        proj_score_before = pre_scored$projected_score)]

  balls[, delta_wp := win_prob_after - win_prob_before]
  balls[, delta_ps := proj_score_after - proj_score_before]

  n_delta <- sum(!is.na(balls$delta_wp))
  cli::cli_alert_success("{n_delta}/{nrow(balls)} deliveries have a win probability delta.")

  out <- balls[, .(
    id,
    match_id,
    innings_number = innings,
    over_number    = over,
    ball_number    = ball,
    format         = db_format,
    win_prob_before,
    win_prob_after,
    delta_wp,
    proj_score_before,
    proj_score_after,
    delta_ps
  )]

  if (return_pre_states) {
    data.table::setattr(out, "pre_states", pre_states)
    data.table::setattr(out, "models", models)
    data.table::setattr(out, "scoreable", scoreable)
    data.table::setattr(out, "mom_cols", mom_cols)
    data.table::setattr(out, "balls_out", balls)
  }

  if (!write) return(out[])

  store_cricinfo_win_probability(conn, out, format = format, table_name = table_name)
  invisible(out[])
}


#' Store Bouncer Win Probability for Cricinfo Deliveries
#'
#' Replaces this format's rows in `main.<table_name>`, creating the table on
#' first use. Per-format replacement rather than a full truncate, so rebuilding
#' T20 does not delete ODI.
#'
#' @param conn DBI connection with write access.
#' @param data data.table as returned by [build_cricinfo_win_probability()].
#' @param format Character. Format whose rows are being replaced.
#' @param table_name Character. Target table in the `main` schema.
#'
#' @return Number of rows inserted, invisibly.
#'
#' @keywords internal
store_cricinfo_win_probability <- function(conn, data, format,
                                           table_name = "bouncer_wp_from_cricinfo") {

  db_format <- toupper(format)
  wanted <- names(.cricinfo_wp_schema)

  data <- as.data.frame(data)
  extra <- setdiff(names(data), wanted)
  if (length(extra)) {
    cli::cli_abort(c(
      "{.arg data} carries {length(extra)} column{?s} the table has no home for: {.field {extra}}.",
      "i" = "Add them to {.code .cricinfo_wp_schema} deliberately, so the migration can create them."
    ))
  }
  data <- data[, wanted]

  duckdb::duckdb_register(conn, "cwp_staging", data)
  on.exit(duckdb::duckdb_unregister(conn, "cwp_staging"), add = TRUE)
  col_list <- paste(wanted, collapse = ", ")

  # Per-format replacement on a table that holds every format, migrated in
  # place (never dropped) on a shape change, DELETE+INSERT sharing one
  # transaction -- fixes bouncerverse#45's whole-table-drop defect, already
  # found and fixed on this table's cricsheet-sourced twin
  # (store_cricsheet_wp()) but not previously ported here. See
  # .migrate_schema()'s docstring (player_rating_v2_storage.R) for the full
  # story: dropping this table on any shape mismatch destroyed every OTHER
  # format's rows, not just the one being rebuilt, and this table feeds the
  # WPA that reaches the player ratings (D-P6).
  n <- .in_transaction(conn, function() {
    DBI::dbExecute(conn, sprintf(
      "CREATE TABLE IF NOT EXISTS main.%s (\n%s\n    )",
      table_name, .schema_ddl(.cricinfo_wp_schema)))
    .migrate_schema(conn, table_name, .cricinfo_wp_schema)
    DBI::dbExecute(conn, sprintf("DELETE FROM main.%s WHERE format = '%s'",
                                 table_name, db_format))
    DBI::dbExecute(conn, sprintf(
      "INSERT INTO main.%s (%s) SELECT %s FROM cwp_staging",
      table_name, col_list, col_list))
  })

  cli::cli_alert_success("Stored {n} {db_format} rows in {.field main.{table_name}}.")
  invisible(n)
}


#' Parse a Cricinfo Match Result From Its Status Text
#'
#' Turns `cricinfo.matches.status_text` into a usable outcome. This is the
#' correct label for any win-probability evaluation in this package.
#'
#' @section Do not derive the result from the scores:
#' `innings2_total <= innings1_total` looks like the obvious label and is wrong
#' for every rain-affected match, where the chase wins on a reduced target and
#' therefore reads as a batting-first win. Measured on cricsheet ODI male
#' 2014-2026, which carries a trustworthy `outcome_method`:
#'
#' | | matches | true bf rate | score-derived bf rate |
#' |---|---|---|---|
#' | normal | 1,143 | 0.4829 | 0.4838 |
#' | D/L | 108 | 0.4815 | **0.8148** |
#'
#' D/L is 8.6% of matches and the derived label is 33 points wrong on them,
#' which was enough to make a correctly-calibrated model look badly
#' miscalibrated (ODI male chase ECE 0.1064 against a true 0.0781).
#'
#' @section Do not use `winner_team_id` either:
#' That column names a team which is not in the match for most rows — 71.7% of
#' T20, 56.5% of ODI, and every Hundred match. Match `1513717` is India U19
#' (`1803`) against UAE U19 (`3675`) with `winner_team_id` `1854`.
#'
#' @param status_text Character vector of `cricinfo.matches.status_text`.
#'
#' @return A data.frame with one row per input:
#'   \itemize{
#'     \item `result` — "batting_first", "chasing", "tied", "drawn",
#'       "no_result", or NA when the text is absent or unrecognised
#'     \item `bf_won` — 1 if the side batting first won, 0 if the chasing side
#'       won, NA otherwise. Ties are NA even when a Super Over decided them:
#'       the innings themselves were level.
#'     \item `margin`, `margin_type` — "runs", "wickets" or "innings_and_runs"
#'     \item `is_dls` — TRUE when the result was reached by DLS/D-L
#'     \item `super_over` — TRUE when a Super Over or one-over eliminator
#'       decided a tie
#'   }
#'
#' @export
cricinfo_match_outcome <- function(status_text) {

  n <- length(status_text)
  st <- as.character(status_text)

  result      <- rep(NA_character_, n)
  bf_won      <- rep(NA_integer_, n)
  margin      <- rep(NA_real_, n)
  margin_type <- rep(NA_character_, n)

  is_dls <- grepl("DLS|D/L|Duckworth", st, ignore.case = TRUE) & !is.na(st)
  super_over <- grepl("super over|one-over eliminator", st, ignore.case = TRUE) & !is.na(st)

  # A tie is a tie whatever settled it afterwards, so this is checked before
  # the margin patterns -- "Match tied (India won the Super Over)" contains no
  # margin, but the guard keeps the intent explicit.
  tied  <- grepl("^\\s*Match tied", st, ignore.case = TRUE) & !is.na(st)
  drawn <- grepl("^\\s*Match drawn", st, ignore.case = TRUE) & !is.na(st)
  nores <- grepl("^\\s*No result|abandoned", st, ignore.case = TRUE) & !is.na(st)

  result[tied]  <- "tied"
  result[drawn] <- "drawn"
  result[nores] <- "no_result"

  undecided <- tied | drawn | nores | is.na(st)

  # "won by an innings and 47 runs" -- the side that batted once won, which is
  # the side that batted first. Checked before the plain runs pattern, which
  # would otherwise match the same text.
  RE_INNS <- "won by an innings and\\s+(\\d+)\\s+runs?"
  RE_RUNS <- "won by\\s+(\\d+)\\s+runs?"
  RE_WKTS <- "won by\\s+(\\d+)\\s+wickets?"
  grab <- function(x, re) as.numeric(sub(paste0(".*", re, ".*"), "\\1", x, ignore.case = TRUE))

  inns <- grepl(RE_INNS, st, ignore.case = TRUE) & !undecided
  if (any(inns)) {
    result[inns] <- "batting_first"
    bf_won[inns] <- 1L
    margin_type[inns] <- "innings_and_runs"
    margin[inns] <- grab(st[inns], RE_INNS)
  }

  runs <- grepl(RE_RUNS, st, ignore.case = TRUE) & !undecided & !inns
  if (any(runs)) {
    result[runs] <- "batting_first"
    bf_won[runs] <- 1L
    margin_type[runs] <- "runs"
    margin[runs] <- grab(st[runs], RE_RUNS)
  }

  wkts <- grepl(RE_WKTS, st, ignore.case = TRUE) & !undecided
  if (any(wkts)) {
    result[wkts] <- "chasing"
    bf_won[wkts] <- 0L
    margin_type[wkts] <- "wickets"
    margin[wkts] <- grab(st[wkts], RE_WKTS)
  }

  data.frame(
    result = result,
    bf_won = bf_won,
    margin = margin,
    margin_type = margin_type,
    is_dls = is_dls,
    super_over = super_over,
    stringsAsFactors = FALSE
  )
}
