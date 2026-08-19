# Bouncer's Test Win Probability, Computed for Every Cricinfo Delivery
#
# The Test companion to build_cricinfo_win_probability(). Test cricket runs
# through the decomposed v3 models (A: will there be a result; B: who wins
# given one), which until this file existed only behind the scalar
# predict_test_win_probability() -- so 355,962 Test deliveries had no win
# probability from any source (the scraped column is 0.0% populated for Test).
#
# Feature construction here is VECTORIZED TRAINING SEMANTICS
# (data-raw/models/in-match/08_test_win_probability_v3.R), not a transcription
# of the scalar path -- the scalar path carried two divergences from training
# (projection denominator unclamped; innings-2 projected_lead collapsed to the
# raw lead) which were fixed to match training when this builder was written,
# so the two paths now agree. Known remaining gaps vs training, accepted and
# documented:
#
#   - innings-1 projected_innings_total: CLOSED in bouncerverse#24. Training
#     used the Stage 1 XGBoost projection where available; it was worth 0.0004
#     of holdout mlogloss, so training dropped it and both paths now use the
#     rate projection everywhere.
#   - rain_days_so_far is 0 at serving (no weather join for cricinfo matches);
#     the Tier-1 derived rain proxies are still supplied.
#   - "team1" is defined as the side batting innings 1, and innings 1/3 are
#     attributed to it. Training attributed completed-innings totals by the
#     same alternation, so follow-on matches carry the same approximation the
#     models were trained with.

#' Build Test Win Probability for Every Cricinfo Delivery
#'
#' Scores every Test delivery in `cricinfo.balls` with the decomposed v3
#' models and writes P(team batting first wins) to
#' `main.cricinfo_ball_win_probability` (format `TEST`), alongside a detail
#' table carrying the full three-way decomposition (win / draw / loss and
#' P(result)) that the scalar column cannot hold.
#'
#' @param conn DBI connection. If NULL, opens one (write access when
#'   `write = TRUE`) and closes it on exit.
#' @param models_path Character. Directory holding the in-match models; NULL
#'   resolves via [load_in_match_models()].
#' @param write Logical. Write the tables, or just return the scored frame.
#' @param table_name Character. Scalar target table in the `main` schema.
#' @param detail_table_name Character. Three-way detail table.
#'
#' @return data.table with one row per scored delivery, including
#'   `p_team1_win`, `p_draw`, `p_team2_win`, `p_result`, the before/after/
#'   delta triple on P(team1 wins), and the projected-innings-total triple.
#'   Invisibly when `write = TRUE`.
#'
#' @export
build_cricinfo_test_win_probability <- function(conn = NULL,
                                                models_path = NULL,
                                                write = TRUE,
                                                table_name = "cricinfo_ball_win_probability",
                                                detail_table_name = "cricinfo_ball_test_wp_detail") {

  own_conn <- is.null(conn)
  if (own_conn) {
    conn <- get_db_connection(read_only = !write)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }

  models <- load_in_match_models("test", models_path = models_path)
  if (is.null(models) || is.null(models$result_model) || is.null(models$conditional_model)) {
    cli::cli_abort(c(
      "Could not load the decomposed Test models (v3).",
      "i" = "Run data-raw/models/in-match/08_test_win_probability_v3.R first, or pass {.arg models_path}."
    ))
  }

  cli::cli_alert_info("Loading TEST deliveries from cricinfo.balls...")

  balls <- data.table::as.data.table(DBI::dbGetQuery(conn, "
    SELECT b.id,
           b.match_id,
           b.innings_number        AS innings,
           b.over_number,
           b.ball_number,
           b.total_innings_runs    AS score,
           b.total_innings_wickets AS wickets,
           b.total_runs            AS ball_runs,
           b.is_wicket,
           m.ground_name
    FROM cricinfo.balls b
    JOIN cricinfo.matches m ON m.match_id = b.match_id
    WHERE m.format = 'TEST'
      AND b.innings_number BETWEEN 1 AND 4
      AND b.over_number IS NOT NULL
      AND b.ball_number IS NOT NULL
      AND b.total_innings_runs IS NOT NULL
      AND b.total_innings_wickets BETWEEN 0 AND 10
  "))

  if (nrow(balls) == 0) {
    cli::cli_abort("No scoreable TEST deliveries found in {.field cricinfo.balls}.")
  }
  if (anyDuplicated(balls$id)) {
    cli::cli_abort("{.field cricinfo.balls.id} is not unique for TEST -- it is this table's join key.")
  }

  cli::cli_alert_info(
    "{nrow(balls)} deliveries across {data.table::uniqueN(balls$match_id)} matches."
  )

  # Overs bowled in the current innings, true fraction (ball_number counts
  # re-bowled deliveries past 6; clamp like every other reader).
  balls[, overs_frac := (over_number - 1) + pmin(ball_number, 6) / 6]

  # ---- Completed-innings state, ball-derived with a scorecard fallback ------
  inn_balls <- balls[, .(
    runs_b = max(score, na.rm = TRUE),
    wkts_b = max(wickets, na.rm = TRUE),
    overs_b = max(overs_frac, na.rm = TRUE)
  ), by = .(match_id, innings)]

  inn_card <- data.table::as.data.table(DBI::dbGetQuery(conn, "
    SELECT i.match_id, i.innings_number AS innings,
           MAX(i.total_runs)    AS runs_c,
           MAX(i.total_wickets) AS wkts_c,
           MAX(i.total_overs)   AS overs_c_raw
    FROM cricinfo.innings i
    JOIN cricinfo.matches m ON m.match_id = i.match_id
    WHERE m.format = 'TEST'
    GROUP BY i.match_id, i.innings_number
  "))
  # total_overs is cricket notation (78.3 = 78 overs 3 balls)
  inn_card[, overs_c := floor(overs_c_raw) + (overs_c_raw %% 1) * 10 / 6]

  inn <- merge(inn_balls, inn_card, by = c("match_id", "innings"), all = TRUE)
  inn[, `:=`(
    runs  = data.table::fifelse(is.na(runs_b), as.numeric(runs_c), as.numeric(runs_b)),
    wkts  = data.table::fifelse(is.na(wkts_b), as.numeric(wkts_c), as.numeric(wkts_b)),
    overs = data.table::fifelse(is.na(overs_b), overs_c, overs_b)
  )]

  wide <- data.table::dcast(inn, match_id ~ innings,
                            value.var = c("runs", "wkts", "overs"))
  for (nm in c("runs_1", "runs_2", "runs_3", "wkts_1", "wkts_2", "wkts_3",
               "overs_1", "overs_2", "overs_3")) {
    if (!nm %in% names(wide)) wide[, (nm) := NA_real_]
  }
  balls <- merge(balls, wide[, .(match_id, runs_1, runs_2, runs_3,
                                 wkts_1, wkts_2, wkts_3,
                                 overs_1, overs_2, overs_3)],
                 by = "match_id", all.x = TRUE)

  # ---- Venue statistics from cricinfo's own Test history --------------------
  venue_avg_dt <- balls[innings == 1, .(inn1 = max(score, na.rm = TRUE)),
                        by = .(ground_name, match_id)][
    , .(venue_avg = mean(inn1), n = .N), by = ground_name]
  venue_avg_dt[n < 3, venue_avg := NA_real_]

  # Time-causal, per MATCH rather than per venue -- see R/venue_rates.R. This
  # was a per-venue rate computed over every match at the ground including the
  # one being scored, which is label information a live prediction cannot have
  # (#29). The merge key changes from ground_name to match_id accordingly.
  outcomes <- data.table::as.data.table(DBI::dbGetQuery(conn, "
    SELECT m.match_id, m.ground_name, m.match_date, m.status_text
    FROM cricinfo.matches m WHERE m.format = 'TEST'
  "))
  oc <- cricinfo_match_outcome(outcomes$status_text)
  outcomes[, is_result := as.integer(oc$result %in% c("batting_first", "chasing"))]
  outcomes[, decided := as.integer(!is.na(oc$result) & oc$result != "no_result")]
  vr <- time_causal_venue_result_rate(
    outcomes[, .(match_id, venue = ground_name,
                 match_date = as.Date(match_date), decided, is_result)],
    prior_weight = 10)
  prior_rate <- attr(vr, "prior_rate")
  cli::cli_alert_info(
    "Venue result rate: {sum(vr$at_prior)} of {nrow(vr)} matches
     ({round(100 * mean(vr$at_prior), 1)}%) are the first at their ground and
     fall back to the prior ({round(prior_rate, 3)}).")

  balls <- merge(balls, venue_avg_dt[, .(ground_name, venue_avg)],
                 by = "ground_name", all.x = TRUE)
  balls <- merge(balls, vr[, .(match_id, venue_result_rate)],
                 by = "match_id", all.x = TRUE)
  balls[is.na(venue_avg), venue_avg := 340]
  balls[is.na(venue_result_rate), venue_result_rate := prior_rate]

  data.table::setorder(balls, match_id, innings, over_number, ball_number, id)

  # ---- The feature construction, exactly as training built it ---------------
  feats <- .test_wp_features(balls)

  cli::cli_alert_info("Scoring with the decomposed Test models...")
  t0 <- Sys.time()
  p_result <- predict_with_features(models$result_model, feats,
                                    models$result_features)
  p_t1_given <- predict_with_features(models$conditional_model, feats,
                                      models$conditional_features)
  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))

  balls[, `:=`(
    p_result = p_result,
    p_team1_win = p_result * p_t1_given,
    p_draw = 1 - p_result,
    p_team2_win = p_result * (1 - p_t1_given),
    proj_innings_total = feats$projected_innings_total
  )]
  cli::cli_alert_success(
    "Scored {nrow(balls)} deliveries in {round(elapsed, 1)}s ({round(1000 * elapsed / nrow(balls), 3)} ms/ball)."
  )

  # ---- The "before" state: each ball's OWN pre-delivery state ---------------
  # The scored number above is the state AFTER the delivery -- score and
  # wickets both come from cumulative columns that already include this ball
  # (verified empirically: on the first ball of an innings the cumulative
  # total equals that ball's runs, and total_innings_wickets equals is_wicket).
  #
  # So "before" is this ball's own row rolled back one delivery -- score minus
  # this ball's runs, wickets minus this ball's wicket, one ball earlier on the
  # clock -- the epv_delta construction the limited-overs builder moved to in
  # 0e802dc. It is NOT the previous row's "after". Differencing adjacent rows
  # looks equivalent and is not: cricinfo Test ball data has gaps (1511663 is
  # missing a whole innings), and a LAG across a gap charges every unrecorded
  # ball's drift to whoever bowls next. Under the own-pre-state construction a
  # gap's drift lands on no delivery at all.
  #
  # This also retires the separate innings-start scoring: the first ball of an
  # innings rolls back to score 0, wickets 0, 0 overs, which IS the innings
  # start, so it now falls out of the same construction.
  data.table::setorder(balls, match_id, innings, over_number, ball_number, id)

  pre <- data.table::copy(balls)
  # ball_runs/is_wicket are not in the WHERE clause's NOT NULL set; a missing
  # one means "we do not know what this ball did", and the honest roll-back is
  # then no roll-back at all rather than an NA that would poison the features.
  pre[, `:=`(
    score      = pmax(score - data.table::fifelse(is.na(ball_runs), 0L, ball_runs), 0L),
    wickets    = pmax(wickets - data.table::fifelse(is.na(is_wicket), 0L,
                                                   as.integer(is_wicket)), 0L),
    overs_frac = (over_number - 1) + (pmin(ball_number, 6) - 1) / 6
  )]
  pf <- .test_wp_features(pre)
  b_res <- predict_with_features(models$result_model, pf, models$result_features)
  b_t1  <- predict_with_features(models$conditional_model, pf,
                                 models$conditional_features)

  balls[, `:=`(
    win_prob_before   = b_res * b_t1,
    proj_score_before = pf$projected_innings_total
  )]
  balls[, delta_wp := p_team1_win - win_prob_before]
  balls[, delta_ps := proj_innings_total - proj_score_before]

  out <- balls[, .(
    id, match_id,
    innings_number = innings,
    over_number, ball_number,
    format = "TEST",
    win_prob_before,
    win_prob_after = p_team1_win,
    delta_wp,
    proj_score_before,
    proj_score_after = proj_innings_total,
    delta_ps,
    p_result, p_team1_win, p_draw, p_team2_win
  )]

  if (!write) return(out[])

  store_cricinfo_win_probability(
    conn,
    out[, .(id, match_id, innings_number, over_number, ball_number, format,
            win_prob_before, win_prob_after, delta_wp,
            proj_score_before, proj_score_after, delta_ps)],
    format = "test", table_name = table_name
  )

  DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS main.%s", detail_table_name))
  DBI::dbExecute(conn, sprintf("
    CREATE TABLE main.%s (
      id VARCHAR, match_id VARCHAR, innings_number INTEGER,
      p_result DOUBLE, p_team1_win DOUBLE, p_draw DOUBLE, p_team2_win DOUBLE
    )", detail_table_name))
  duckdb::duckdb_register(conn, "twp_staging",
    as.data.frame(out[, .(id, match_id, innings_number,
                          p_result, p_team1_win, p_draw, p_team2_win)]))
  on.exit(duckdb::duckdb_unregister(conn, "twp_staging"), add = TRUE)
  n_detail <- DBI::dbExecute(conn, sprintf(
    "INSERT INTO main.%s SELECT * FROM twp_staging", detail_table_name))
  cli::cli_alert_success("Stored {n_detail} rows in {.field main.{detail_table_name}}.")

  invisible(out[])
}


#' Vectorized Test WP Feature Construction (Training Semantics)
#'
#' Builds every Model A and Model B feature for a frame of Test states, as
#' `08_test_win_probability_v3.R` defines them. `dt` needs: innings,
#' over_number (1-indexed), ball_number, overs_frac, score, wickets,
#' venue_avg, venue_result_rate, and the completed-innings columns
#' runs_/wkts_/overs_{1,2,3}.
#'
#' @param dt data.table of states.
#' @return data.frame of features.
#' @keywords internal
.test_wp_features <- function(dt) {

  z <- function(x) data.table::fifelse(is.na(x), 0, as.numeric(x))
  innings <- dt$innings
  score <- as.numeric(dt$score)
  wickets <- as.numeric(dt$wickets)
  overs_frac <- dt$overs_frac
  over0 <- dt$over_number - 1  # training's 0-indexed over

  team1_completed <- data.table::fcase(
    innings == 1, 0,
    innings %in% c(2, 3), z(dt$runs_1),
    innings == 4, z(dt$runs_1) + z(dt$runs_3)
  )
  team2_completed <- data.table::fcase(
    innings <= 2, 0,
    innings == 3, z(dt$runs_2),
    innings == 4, z(dt$runs_2)
  )
  batting_is_team1 <- as.integer(innings %in% c(1, 3))
  team1_lead <- data.table::fifelse(
    batting_is_team1 == 1L,
    team1_completed + score - team2_completed,
    team1_completed - (team2_completed + score)
  )

  completed_overs <- data.table::fcase(
    innings == 1, 0,
    innings == 2, data.table::fifelse(is.na(dt$overs_1), 90, dt$overs_1),
    innings == 3, data.table::fifelse(is.na(dt$overs_1), 90, dt$overs_1) +
                  data.table::fifelse(is.na(dt$overs_2), 90, dt$overs_2),
    innings == 4, data.table::fifelse(is.na(dt$overs_1), 90, dt$overs_1) +
                  data.table::fifelse(is.na(dt$overs_2), 90, dt$overs_2) +
                  data.table::fifelse(is.na(dt$overs_3), 90, dt$overs_3)
  )
  cum_overs <- over0 + completed_overs  # training used whole current overs
  MAX_OVERS <- 450
  overs_remaining <- pmax(0, MAX_OVERS - cum_overs)
  match_progress <- pmin(1, cum_overs / MAX_OVERS)
  approx_day <- pmin(5, floor(cum_overs / 90) + 1)

  total_wickets_match <- data.table::fcase(
    innings == 1, wickets,
    innings == 2, z(dt$wkts_1) + wickets,
    innings == 3, z(dt$wkts_1) + z(dt$wkts_2) + wickets,
    innings == 4, z(dt$wkts_1) + z(dt$wkts_2) + z(dt$wkts_3) + wickets
  )
  total_runs_match <- data.table::fcase(
    innings == 1, score,
    innings == 2, z(dt$runs_1) + score,
    innings == 3, z(dt$runs_1) + z(dt$runs_2) + score,
    innings == 4, z(dt$runs_1) + z(dt$runs_2) + z(dt$runs_3) + score
  )
  runs_per_over_match <- data.table::fifelse(cum_overs > 0, total_runs_match / cum_overs, 3.0)

  wickets_in_hand <- 10 - wickets
  overs_per_wicket_current <- data.table::fifelse(wickets > 0, overs_frac / wickets, 30)
  current_innings_projected_overs <- pmin(
    150,
    data.table::fifelse(wickets > 0,
                        overs_frac + wickets_in_hand * overs_per_wicket_current,
                        90)
  )
  completed_innings_overs <- data.table::fcase(
    innings == 1, 0,
    innings == 2, z(dt$overs_1),
    innings == 3, z(dt$overs_1) + z(dt$overs_2),
    innings == 4, z(dt$overs_1) + z(dt$overs_2) + z(dt$overs_3)
  )
  o80 <- function(x) data.table::fifelse(is.na(x), 80, x)
  avg_overs_per_innings <- data.table::fcase(
    innings == 1, 80,
    innings == 2, o80(dt$overs_1),
    innings == 3, (o80(dt$overs_1) + o80(dt$overs_2)) / 2,
    innings == 4, (o80(dt$overs_1) + o80(dt$overs_2) + o80(dt$overs_3)) / 3
  )
  avg_overs_per_innings[is.na(avg_overs_per_innings)] <- 80
  remaining_innings_count <- 4 - innings
  projected_total_overs <- pmin(600, pmax(50,
    completed_innings_overs + current_innings_projected_overs +
      remaining_innings_count * avg_overs_per_innings))
  time_pressure <- projected_total_overs / MAX_OVERS

  abs_lead <- abs(team1_lead)
  lead_per_over_remaining <- data.table::fifelse(
    overs_remaining > 0, abs_lead / overs_remaining, abs_lead)

  # 0 until innings 2 is complete, honest from innings 3 on. Training used to
  # compute this from the innings-2 FINAL total, which on an innings-2 row is
  # future information ("this side finished 200+ behind"), and Model A leaned
  # on it -- serving refused to reproduce the leak, which is most of why the
  # two paths scored so differently. Training was brought to these semantics
  # in bouncerverse#24; the two now agree, and this is the reference.
  follow_on_possible <- as.integer(
    innings >= 3 & !is.na(dt$runs_1) & !is.na(dt$runs_2) &
      (z(dt$runs_1) - z(dt$runs_2)) >= 200
  )

  is4 <- innings == 4
  target <- data.table::fifelse(is4, team1_completed - team2_completed + 1, 0)
  runs_needed <- data.table::fifelse(is4, pmax(0, target - score), 0)
  req_rate <- data.table::fifelse(
    is4, data.table::fifelse(overs_remaining > 0, runs_needed / overs_remaining, 99), 0)
  overs_per_wicket <- data.table::fifelse(
    is4, data.table::fifelse(wickets_in_hand > 0, overs_remaining / wickets_in_hand, 0), 0)

  current_run_rate <- data.table::fifelse(over0 > 0, score / overs_frac, 0)

  projected_innings_total <- score * (90 / pmax(overs_frac, 1))
  projected_lead <- data.table::fcase(
    batting_is_team1 == 1L & innings == 1, projected_innings_total - dt$venue_avg,
    batting_is_team1 == 1L, team1_completed + projected_innings_total - team2_completed - dt$venue_avg,
    batting_is_team1 == 0L & innings == 2, team1_completed - (team2_completed + projected_innings_total),
    default = team1_lead
  )

  overs_per_day <- data.table::fifelse(approx_day > 0, cum_overs / approx_day, 90)
  overs_deficit <- pmax(0, approx_day * 90 - cum_overs)

  data.frame(
    overs_remaining = overs_remaining,
    match_progress = match_progress,
    approx_day = as.double(approx_day),
    time_pressure = time_pressure,
    projected_total_overs = projected_total_overs,
    venue_result_rate = dt$venue_result_rate,
    total_wickets_match = total_wickets_match,
    runs_per_over_match = runs_per_over_match,
    abs_lead = abs_lead,
    lead_per_over_remaining = lead_per_over_remaining,
    innings_num = as.double(innings),
    follow_on_possible = follow_on_possible,
    overs_per_day = overs_per_day,
    overs_deficit = overs_deficit,
    rain_days_so_far = 0,
    team1_lead = team1_lead,
    projected_lead = projected_lead,
    projected_innings_total = projected_innings_total,
    batting_is_team1 = batting_is_team1,
    wickets_in_hand = wickets_in_hand,
    cum_overs = cum_overs,
    venue_avg = dt$venue_avg,
    target = target,
    runs_needed = runs_needed,
    req_rate = req_rate,
    overs_per_wicket = overs_per_wicket,
    current_run_rate = current_run_rate
  )
}
