# Runs Above Average (RAA), Computed for Every Cricinfo Delivery
#
# The replacement for ERA specified in
# bouncerverse/docs/plans/2026-08-13-REPLACE-ERA-WITH-RAA.md. Per delivery a
# batter faced:
#
#   raa = (runs_actual - E[runs | state]) - lambda * (wicket_actual - P[wicket | state])
#
# where the expectations come from the agnostic outcome model -- state-only,
# deliberately blind to batter and bowler identity, which is exactly the
# "average batter" baseline the metric needs. Unlike ERA, nothing constrains an
# innings' total RAA to a team-level pot, the expectation conditions on phase
# and wickets so batting position is priced in, and a dismissal costs a bounded
# lambda-weighted surprise rather than a projected-score collapse.
#
# Train/serve consistency notes (the agnostic model was trained on cricsheet
# deliveries by data-raw/models/ball-outcome/01_train_agnostic_model.R; this
# file serves it on cricinfo balls):
#
#   - `over` is 0-indexed in training; cricinfo over_number is 1-indexed, so 1
#     is subtracted here.
#   - `ball` counts deliveries within the over INCLUDING re-bowled illegal
#     ones in both sources (cricsheet reaches 19, cricinfo 18), so it is
#     passed through unchanged.
#   - `wickets_fallen` was leak-fixed at training to the count BEFORE the
#     delivery; cricinfo total_innings_wickets includes the current ball, so
#     is_wicket is subtracted here.
#   - `runs_difference` at training was the batting side's cumulative score
#     INCLUDING the current delivery minus the opponent's completed-innings
#     total. That inclusion is reproduced here on purpose: serving must match
#     training even where training's convention is arguable.
#   - Training did not exclude wides (batter runs 0 by definition), so the
#     expectation averages over them; scoring only batter-faced legal balls
#     against it adds a uniform ~+0.02 runs/ball drift that cancels in any
#     across-player comparison at the same states.

#' The Run Value of a Wicket for RAA
#'
#' Returns lambda, the run cost of a dismissal in the RAA formula.
#'
#' Fitted, not assumed (see the RAA plan, "The wicket value, fitted"): both
#' the WP cost of a wicket and the WP value of a run are estimated from actual
#' match outcomes with within-state controls (runs needed / score, balls left,
#' wickets in hand), and lambda is their ratio, averaged across the two
#' innings so identical acts score identically in both. T20 fitted 7.8-10.2
#' by innings (9.0 in use); ODI fitted 22.5 (innings 1) and 23.4 (chase) on
#' 2026-08-14 over 227,628 male deliveries -- a wicket is worth ~2.5x more
#' runs in a 300-ball innings (bouncerverse#19). The naive state-difference
#' estimator is selection-biased and must never be used to refit these.
#'
#' **Test fitted 2026-08-17 at 33**, over Test+MDM male (3,047 matches,
#' 5,388,418 deliveries). Same method, with one addition the shorter formats
#' never needed: a Test can be drawn, so the utility a wicket is priced against
#' has to say what a draw is worth. Two of the three candidates turn out to be
#' the same utility — since `pW+pD+pL=1`, `pW+0.5pD` is `0.5 + 0.5(pW-pL)`, an
#' affine transform, and lambda is a ratio of derivatives — so the only real
#' choice is whether a draw has value at all. It does: pricing a draw as a loss
#' returns 22.7, i.e. *below ODI*, which cannot be right for a format whose
#' innings ends on wickets and time rather than on balls, and it is far less
#' consistent across innings (1.75x spread vs 1.25x). Corroborated by an
#' independent estimator using innings run totals and no win probability at
#' all: 30.5. Full working in
#' `docs/reviews/2026-08-17-TEST-LAMBDA-FIT.md`.
#'
#' @param format Character. All three are fitted: "t20", "odi", "test".
#'
#' @return Numeric scalar, runs per wicket.
#'
#' @keywords internal
get_raa_lambda <- function(format = c("t20", "odi", "test")) {
  format <- match.arg(format)
  switch(format,
    t20  = 9.0,
    odi  = 23.0,
    test = 33.0,
    # Unreachable while match.arg() gates the argument, but kept so that adding
    # a format to the signature without fitting its lambda fails loudly rather
    # than inheriting another format's wicket value by falling through.
    cli::cli_abort(c(
      "RAA lambda is not fitted for {.val {format}} yet.",
      "i" = "Fit it from actual outcomes as the RAA plan specifies; do not reuse another format's value."
    ))
  )
}


#' Build Runs Above Average for Every Cricinfo Delivery
#'
#' Scores every batter-faced delivery of a format from `cricinfo.balls` with
#' the agnostic outcome model and writes per-ball RAA to
#' `main.cricinfo_ball_raa`, keyed on `cricinfo.balls.id` (the composite
#' (match, innings, over, ball) is not unique -- see
#' [build_cricinfo_win_probability()]).
#'
#' Wides are excluded: the batter does not face a wide, and the batting
#' aggregation in `player_game_data.R` excludes them for every other batting
#' stat. No-balls are faced and are included. Deliveries where the batter ran
#' 5 or 7 are scored as-is even though the outcome model's classes stop at 6;
#' they are ~1 in 7,000 balls and their expectation is still state-correct.
#'
#' @param format Character. "t20" for now; other formats need their lambda
#'   fitted first ([get_raa_lambda()]).
#' @param conn DBI connection. If NULL, opens one (write access when
#'   `write = TRUE`) and closes it on exit.
#' @param model Optional pre-loaded agnostic model from
#'   [load_agnostic_model()]; loaded if NULL.
#' @param write Logical. Write the table, or just return the scored frame.
#' @param table_name Character. Target table in the `main` schema.
#'
#' @return data.table with one row per scored delivery: `id`, `match_id`,
#'   `innings_number`, `over_number`, `ball_number`, `format`,
#'   `batsman_player_id`, `exp_runs`, `exp_wicket`, `actual_runs`,
#'   `is_wicket`, `raa_run`, `raa_wicket`, `raa`. Invisibly when
#'   `write = TRUE`.
#'
#' @export
build_cricinfo_raa <- function(format = c("t20", "odi", "test"),
                               conn = NULL,
                               model = NULL,
                               write = TRUE,
                               table_name = "cricinfo_ball_raa") {

  format <- match.arg(format)
  db_format <- toupper(format)
  lambda <- get_raa_lambda(format)

  own_conn <- is.null(conn)
  if (own_conn) {
    conn <- get_db_connection(read_only = !write)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }

  if (is.null(model)) model <- load_agnostic_model(format)

  cli::cli_alert_info("Loading {db_format} batter-faced deliveries from cricinfo.balls...")

  # Event tier reproduces training's CASE on cricsheet event names as closely
  # as cricinfo's fields allow: any international (class id set) and the IPL
  # are tier 1, the established franchise leagues tier 2, the rest tier 3.
  # is_knockout comes from the match title, standing in for cricsheet's
  # event_match_number.
  balls <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT b.id,
           b.match_id,
           b.batsman_player_id,
           b.innings_number         AS innings,
           b.over_number,
           b.ball_number,
           COALESCE(b.batsman_runs, 0)  AS actual_runs,
           CAST(COALESCE(b.is_wicket, FALSE) AS INT) AS is_wicket,
           b.total_innings_runs,
           b.total_innings_wickets,
           m.gender,
           CASE
             WHEN LOWER(COALESCE(m.title, '')) LIKE '%%final%%'
               OR LOWER(COALESCE(m.title, '')) LIKE '%%qualifier%%'
               OR LOWER(COALESCE(m.title, '')) LIKE '%%eliminator%%'
               OR LOWER(COALESCE(m.title, '')) LIKE '%%playoff%%'
               OR LOWER(COALESCE(m.title, '')) LIKE '%%semi%%'
             THEN 1 ELSE 0
           END AS is_knockout,
           CASE
             WHEN LOWER(COALESCE(m.series_name, '')) LIKE '%%world cup%%' THEN 1
             WHEN LOWER(COALESCE(m.series_name, '')) LIKE '%%indian premier%%' THEN 1
             WHEN m.international_class_id IS NOT NULL THEN 1
             WHEN LOWER(COALESCE(m.series_name, '')) LIKE '%%big bash%%' THEN 2
             WHEN LOWER(COALESCE(m.series_name, '')) LIKE '%%super league%%' THEN 2
             WHEN LOWER(COALESCE(m.series_name, '')) LIKE '%%caribbean premier%%' THEN 2
             ELSE 3
           END AS event_tier
    FROM cricinfo.balls b
    JOIN cricinfo.matches m ON m.match_id = b.match_id
    WHERE m.format = '%s'
      AND m.gender IS NOT NULL
      AND b.innings_number IN (1, 2)
      AND b.batsman_player_id IS NOT NULL
      AND b.over_number IS NOT NULL
      AND b.ball_number IS NOT NULL
      AND b.total_innings_runs IS NOT NULL
      AND b.total_innings_wickets BETWEEN 0 AND 10
      AND (b.wides IS NULL OR b.wides = 0)
  ", db_format)))

  if (nrow(balls) == 0) {
    cli::cli_abort("No scoreable {db_format} deliveries found in {.field cricinfo.balls}.")
  }
  if (anyDuplicated(balls$id)) {
    cli::cli_abort("{.field cricinfo.balls.id} is not unique for {db_format} -- it is this table's join key.")
  }

  cli::cli_alert_info(
    "{nrow(balls)} deliveries across {data.table::uniqueN(balls$match_id)} matches."
  )

  # First-innings totals for runs_difference, ball-derived with a scorecard
  # fallback -- the same two-source pattern (and the same 371-match gap it
  # closes) as build_cricinfo_win_probability().
  inn1_balls <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT b.match_id, MAX(b.total_innings_runs) AS inn1_balls
    FROM cricinfo.balls b
    JOIN cricinfo.matches m ON m.match_id = b.match_id
    WHERE m.format = '%s' AND b.innings_number = 1
    GROUP BY b.match_id
  ", db_format)))
  inn1_card <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT i.match_id, MAX(i.total_runs) AS inn1_card
    FROM cricinfo.innings i
    JOIN cricinfo.matches m ON m.match_id = i.match_id
    WHERE m.format = '%s' AND i.innings_number = 1 AND i.total_runs IS NOT NULL
    GROUP BY i.match_id
  ", db_format)))
  inn1 <- merge(inn1_balls, inn1_card, by = "match_id", all = TRUE)
  inn1[, inn1_total := data.table::fifelse(is.na(inn1_balls), inn1_card, inn1_balls)]

  balls <- merge(balls, inn1[, .(match_id, inn1_total)], by = "match_id", all.x = TRUE)

  # Second-innings balls with no first-innings total from either source have
  # no defensible runs_difference and are left unscored rather than guessed.
  unscoreable <- balls[innings == 2L & is.na(inn1_total), .N]
  if (unscoreable > 0) {
    cli::cli_warn(
      "{unscoreable} second-innings deliveries have no first-innings total and are left unscored."
    )
  }
  balls <- balls[innings == 1L | !is.na(inn1_total)]

  # League running averages, built from cricinfo itself the way training built
  # them from cricsheet: per-match mean total runs per delivery (extras
  # included, wides included) and wicket rate, averaged over the league's
  # PRIOR matches only. "League" is series_name, the closest cricinfo analogue
  # of cricsheet's event_name. A league's first match has no history and falls
  # through to training's format default inside prepare_agnostic_features().
  league <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    WITH match_stats AS (
      SELECT b.match_id,
             COALESCE(m.series_name, 'unknown') AS league,
             MIN(m.start_date) AS start_date,
             AVG(COALESCE(b.batsman_runs, 0) + COALESCE(b.wides, 0)
                 + COALESCE(b.noballs, 0) + COALESCE(b.byes, 0)
                 + COALESCE(b.legbyes, 0) + COALESCE(b.penalties, 0)) AS match_avg_runs,
             AVG(CAST(COALESCE(b.is_wicket, FALSE) AS DOUBLE)) AS match_wicket_rate
      FROM cricinfo.balls b
      JOIN cricinfo.matches m ON m.match_id = b.match_id
      WHERE m.format = '%s'
      GROUP BY b.match_id, COALESCE(m.series_name, 'unknown')
    )
    SELECT match_id,
           AVG(match_avg_runs) OVER (
             PARTITION BY league ORDER BY start_date, match_id
             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS league_avg_runs,
           AVG(match_wicket_rate) OVER (
             PARTITION BY league ORDER BY start_date, match_id
             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS league_avg_wicket
    FROM match_stats
  ", db_format)))
  balls <- merge(balls, league, by = "match_id", all.x = TRUE)

  features <- data.frame(
    innings = balls$innings,
    over = balls$over_number - 1L,
    ball = balls$ball_number,
    wickets_fallen = pmax(balls$total_innings_wickets - balls$is_wicket, 0L),
    runs_difference = data.table::fifelse(
      balls$innings == 1L,
      as.numeric(balls$total_innings_runs),
      as.numeric(balls$total_innings_runs - balls$inn1_total)
    ),
    gender = balls$gender,
    is_knockout = balls$is_knockout,
    event_tier = balls$event_tier,
    league_avg_runs = balls$league_avg_runs,
    league_avg_wicket = balls$league_avg_wicket
  )

  cli::cli_alert_info("Scoring with the agnostic {format} model...")
  t0 <- Sys.time()
  probs <- predict_agnostic_outcome(model, features, format)
  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  cli::cli_alert_success(
    "Scored {nrow(balls)} deliveries in {round(elapsed, 1)}s."
  )

  balls[, exp_runs := get_agnostic_expected_runs(probs)]
  balls[, exp_wicket := get_agnostic_expected_wicket(probs)]
  balls[, raa_run := actual_runs - exp_runs]
  balls[, raa_wicket := -lambda * (is_wicket - exp_wicket)]
  balls[, raa := raa_run + raa_wicket]

  out <- balls[, .(
    id,
    match_id,
    innings_number = innings,
    over_number,
    ball_number,
    format = db_format,
    batsman_player_id,
    exp_runs,
    exp_wicket,
    actual_runs,
    is_wicket,
    raa_run,
    raa_wicket,
    raa
  )]

  if (!write) return(out[])

  store_cricinfo_raa(conn, out, format = format, table_name = table_name)
  invisible(out[])
}


#' Store Per-Ball RAA for Cricinfo Deliveries
#'
#' Replaces this format's rows in `main.<table_name>`, creating the table on
#' first use. Per-format replacement, so rebuilding T20 does not delete ODI.
#'
#' @param conn DBI connection with write access.
#' @param data data.table as returned by [build_cricinfo_raa()].
#' @param format Character. Format whose rows are being replaced.
#' @param table_name Character. Target table in the `main` schema.
#'
#' @return Number of rows inserted, invisibly.
#'
#' @keywords internal
store_cricinfo_raa <- function(conn, data, format,
                               table_name = "cricinfo_ball_raa") {

  db_format <- toupper(format)

  wanted <- c("id", "match_id", "innings_number", "over_number", "ball_number",
              "format", "batsman_player_id", "exp_runs", "exp_wicket",
              "actual_runs", "is_wicket", "raa_run", "raa_wicket", "raa")

  existing <- DBI::dbGetQuery(conn, sprintf("
    SELECT column_name FROM information_schema.columns
    WHERE table_schema = 'main' AND table_name = '%s'", table_name))$column_name

  if (length(existing) > 0 && !setequal(existing, wanted)) {
    cli::cli_alert_warning(
      "{.field main.{table_name}} has an outdated shape ({length(existing)} column{?s}); recreating it."
    )
    DBI::dbExecute(conn, sprintf("DROP TABLE main.%s", table_name))
  }

  DBI::dbExecute(conn, sprintf("
    CREATE TABLE IF NOT EXISTS main.%s (
      id                VARCHAR,
      match_id          VARCHAR,
      innings_number    INTEGER,
      over_number       DOUBLE,
      ball_number       INTEGER,
      format            VARCHAR,
      batsman_player_id VARCHAR,
      exp_runs          DOUBLE,
      exp_wicket        DOUBLE,
      actual_runs       INTEGER,
      is_wicket         INTEGER,
      raa_run           DOUBLE,
      raa_wicket        DOUBLE,
      raa               DOUBLE
    )", table_name))

  duckdb::duckdb_register(conn, "raa_staging", as.data.frame(data))
  on.exit(duckdb::duckdb_unregister(conn, "raa_staging"), add = TRUE)

  removed <- DBI::dbExecute(conn, sprintf(
    "DELETE FROM main.%s WHERE format = '%s'", table_name, db_format
  ))

  col_list <- paste(wanted, collapse = ", ")
  n <- DBI::dbExecute(conn, sprintf(
    "INSERT INTO main.%s (%s) SELECT %s FROM raa_staging",
    table_name, col_list, col_list
  ))

  cli::cli_alert_success(
    "Stored {n} {db_format} rows in {.field main.{table_name}}{if (removed > 0) paste0(' (replaced ', removed, ')') else ''}."
  )
  invisible(n)
}
