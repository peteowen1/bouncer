# Per-Ball RAA over Cricsheet Deliveries
#
# The companion to build_cricinfo_raa(), against the source the agnostic model
# was actually TRAINED on. Cricsheet carries 13,038 T20 matches to cricinfo's
# 1,977, with every IPL season 2008-2025 complete where cricinfo has none at
# all for 2017-2019 -- Kohli has 425 T20 matches here against 59 there
# (bouncerverse#33).
#
# Feature construction is lifted from
# data-raw/models/ball-outcome/01_train_agnostic_model.R verbatim, because that
# is what the model saw. build_cricinfo_raa()'s own comment concedes its
# event-tier CASE only "reproduces training's CASE on cricsheet event names as
# closely as cricinfo's fields allow" -- here no approximation is needed.
#
# Verified against the data before this was written (2026-08-15), not assumed:
#
#   - `total_runs` is POST-delivery (100% of 26,072 first balls equal that
#     ball's runs) and its running sum reconciles exactly (46,516/46,516).
#   - `wickets_fallen` is POST-delivery (100%). Training subtracts `is_wicket`
#     to get the PRE count while using `total_runs` POST, so the two halves of
#     the state sit in different frames. That is REPRODUCED here, not fixed:
#     the model was fitted on it, and build_cricinfo_raa() does the same.
#   - `runs_total = runs_batter + runs_extras` exactly (100%), and
#     `runs_extras` equals its five components exactly (100%).
#   - `ball` is NOT capped at 6 -- it counts every delivery including extras and
#     reaches 19. 97 overs contain 7 legal balls (umpire miscounts). Training
#     feeds raw `over`/`ball`, so this does too.
#   - super overs exist (T20 innings 3-8, 1,066 deliveries; ODI innings 3-4),
#     and are excluded.
#   - 453 deliveries have `runs_batter = 5` and 3 have 7; training excludes
#     both, so this does.
#   - `delivery_id` is unique and no key or value column contains a NULL.

#' Per-Ball Runs Above Average over Cricsheet Deliveries
#'
#' Scores every batter-faced Cricsheet delivery with the agnostic ball-outcome
#' model and writes runs-above-average to `main.cricsheet_ball_raa`.
#'
#' @param format Character. "t20", "odi" or "test".
#' @param conn DBI connection. If NULL, opens one and closes it on exit.
#' @param model Agnostic model; NULL resolves via [load_agnostic_model()].
#' @param write Logical. Write the table, or just return the scored frame.
#' @param table_name Character. Target table in the `main` schema.
#' @param exclude_short_overs Logical. Drop matches whose `balls_per_over` is
#'   not 6 -- The Hundred is filed as `match_type = 'T20'` with 5-ball overs
#'   (167 men's + 155 women's matches), where `over`/`ball` mean something
#'   structurally different. Training did NOT exclude them; this is a
#'   deliberate divergence, and turning it off reproduces training's population.
#'
#' @return data.table with one row per scored delivery. Invisibly when
#'   `write = TRUE`.
#'
#' @export
build_cricsheet_raa <- function(format = c("t20", "odi", "test"),
                                conn = NULL,
                                model = NULL,
                                write = TRUE,
                                table_name = "cricsheet_ball_raa",
                                exclude_short_overs = TRUE) {

  format <- match.arg(format)
  db_format <- toupper(format)
  lambda <- get_raa_lambda(format)

  own_conn <- is.null(conn)
  if (own_conn) {
    conn <- get_db_connection(read_only = !write)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }

  if (is.null(model)) model <- load_agnostic_model(format)

  types <- switch(format,
    t20  = c("t20", "it20"),
    odi  = c("odi", "odm"),
    test = c("test", "mdm")
  )
  type_list <- paste0("'", types, "'", collapse = ", ")
  max_innings <- if (format == "test") 4L else 2L

  short_over_filter <- if (exclude_short_overs) {
    "AND COALESCE(m.balls_per_over, 6) = 6"
  } else ""

  cli::cli_alert_info("Loading {db_format} batter-faced deliveries from cricsheet.deliveries...")

  # Innings totals for the opponent's completed innings -- training's
  # bowling_score. General over innings count, so Test's 4 innings work.
  balls <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    WITH innings_totals AS (
      SELECT d.match_id, d.innings, d.batting_team, MAX(d.total_runs) AS innings_total
      FROM cricsheet.deliveries d
      WHERE LOWER(d.match_type) IN (%1$s)
      GROUP BY d.match_id, d.innings, d.batting_team
    ),
    match_context AS (
      SELECT DISTINCT
        m.match_id,
        m.event_name,
        CASE
          WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%final%%' THEN 1
          WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%qualifier%%' THEN 1
          WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%eliminator%%' THEN 1
          WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%playoff%%' THEN 1
          WHEN LOWER(CAST(m.event_match_number AS VARCHAR)) LIKE '%%semi%%' THEN 1
          ELSE 0
        END AS is_knockout,
        CASE
          WHEN LOWER(m.event_name) LIKE '%%world cup%%' THEN 1
          WHEN LOWER(m.event_name) LIKE '%%ipl%%' OR LOWER(m.event_name) LIKE '%%indian premier%%' THEN 1
          WHEN LOWER(m.event_name) LIKE '%%big bash%%' OR LOWER(m.event_name) LIKE '%%bbl%%' THEN 2
          WHEN LOWER(m.event_name) LIKE '%%psl%%' OR LOWER(m.event_name) LIKE '%%super league%%' THEN 2
          WHEN LOWER(m.event_name) LIKE '%%cpl%%' OR LOWER(m.event_name) LIKE '%%caribbean%%' THEN 2
          WHEN LOWER(m.match_type) IN ('test', 'odi', 't20i', 'it20') THEN 1
          ELSE 3
        END AS event_tier
      FROM cricsheet.matches m
    ),
    league_stats AS (
      SELECT m.event_name, m.match_id, m.match_date,
             AVG(d.runs_batter + d.runs_extras) AS match_avg_runs,
             AVG(CAST(d.is_wicket AS DOUBLE)) AS match_wicket_rate
      FROM cricsheet.matches m
      JOIN cricsheet.deliveries d ON m.match_id = d.match_id
      WHERE LOWER(m.match_type) IN (%1$s) AND m.event_name IS NOT NULL
      GROUP BY m.event_name, m.match_id, m.match_date
    ),
    league_running_avg AS (
      SELECT event_name, match_id,
             AVG(match_avg_runs) OVER (
               PARTITION BY event_name ORDER BY match_date, match_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS league_avg_runs,
             AVG(match_wicket_rate) OVER (
               PARTITION BY event_name ORDER BY match_date, match_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS league_avg_wicket
      FROM league_stats
    )
    SELECT
      d.delivery_id,
      d.match_id,
      d.match_date,
      d.batter_id,
      d.bowler_id,
      d.innings,
      d.over,
      d.ball,
      d.gender,
      d.runs_batter                              AS actual_runs,
      CAST(d.is_wicket AS INT)                   AS is_wicket,
      (d.wickets_fallen - CAST(d.is_wicket AS INT)) AS wickets_pre,
      -- FIX: total_runs is the innings score AFTER this delivery (the parser writes
      -- the running total post-ball). Subtract the ball's own runs to get the score
      -- BEFORE it, or runs_difference leaks the target it is used to predict.
      (d.total_runs - (d.runs_batter + d.runs_extras)) AS batting_score,
      COALESCE((SELECT SUM(it.innings_total) FROM innings_totals it
                WHERE it.match_id = d.match_id
                  AND it.batting_team = d.bowling_team
                  AND it.innings < d.innings), 0) AS bowling_score,
      COALESCE(mc.is_knockout, 0)                AS is_knockout,
      COALESCE(mc.event_tier, 3)                 AS event_tier,
      lra.league_avg_runs,
      lra.league_avg_wicket,
      d.is_free_hit
    FROM cricsheet.deliveries d
    JOIN cricsheet.matches m ON m.match_id = d.match_id
    LEFT JOIN match_context mc ON mc.match_id = d.match_id
    LEFT JOIN league_running_avg lra ON lra.match_id = d.match_id
    WHERE LOWER(d.match_type) IN (%1$s)
      %2$s
      AND d.innings BETWEEN 1 AND %3$d
      -- Stays, deliberately (#81/D-P50 stage 4). The agnostic model now
      -- trains on wides as their own category, but a wide is not a ball
      -- FACED -- cricket convention (and R/player_game_data.R's own
      -- batting_balls_faced filter) never counts one. Scoring wides here
      -- would credit/debit batters for balls that were never theirs.
      AND COALESCE(d.wides, 0) = 0
      AND d.runs_batter <> 5
      AND d.runs_batter <= 6
      AND d.batter_id IS NOT NULL
      AND d.bowler_id IS NOT NULL
  ", type_list, short_over_filter, max_innings)))

  if (nrow(balls) == 0) {
    cli::cli_abort("No scoreable {db_format} deliveries found in {.field cricsheet.deliveries}.")
  }
  if (anyDuplicated(balls$delivery_id)) {
    cli::cli_abort("{.field cricsheet.deliveries.delivery_id} is not unique for {db_format} -- it is this table's join key.")
  }
  cli::cli_alert_info(
    "{nrow(balls)} deliveries across {data.table::uniqueN(balls$match_id)} matches."
  )

  # Exactly training's feature block. league_avg_* stay NA where a league has no
  # prior match; prepare_agnostic_features() fills them with the format default,
  # which is what training's COALESCE did.
  features <- data.frame(
    innings = balls$innings,
    over = balls$over,
    ball = balls$ball,
    wickets_fallen = pmax(balls$wickets_pre, 0L),
    runs_difference = as.numeric(balls$batting_score - balls$bowling_score),
    gender = balls$gender,
    is_knockout = balls$is_knockout,
    event_tier = balls$event_tier,
    league_avg_runs = balls$league_avg_runs,
    league_avg_wicket = balls$league_avg_wicket,
    # #81/D-P50 stage 3 added is_free_hit as a training feature; real value
    # available here since stage 1 backfilled it onto cricsheet.deliveries.
    is_free_hit = balls$is_free_hit
  )

  cli::cli_alert_info("Scoring with the agnostic {format} model...")
  t0 <- Sys.time()
  probs <- predict_agnostic_outcome(model, features, format)
  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  cli::cli_alert_success("Scored {nrow(balls)} deliveries in {round(elapsed, 1)}s.")

  balls[, exp_runs := get_agnostic_expected_runs(probs)]
  balls[, exp_wicket := get_agnostic_expected_wicket(probs)]
  # Three quantities, deliberately kept apart (docs/reference/RATING-ARCHITECTURE.md):
  #   raa_run  runs above average -- the RAA rating's input
  #   waa      wickets above average, in WICKETS. Positive means the batter
  #            survived a ball the model expected him to lose, negative on
  #            dismissal. This is the WAA rating's input and it carries no
  #            lambda, so it is not committed to any run price.
  #   raa      RVAA -- the composite, raa_run + lambda * waa, on the runs scale.
  #            Distinct from TSA (team score added), which is the change in the
  #            team's PROJECTED INNINGS TOTAL across a delivery, post minus pre.
  # Keeping waa unpriced is the point: lambda belongs at the aggregate, where it
  # can be made situational, not baked into every ball at a flat rate.
  balls[, raa_run := actual_runs - exp_runs]
  balls[, waa := exp_wicket - is_wicket]
  balls[, raa_wicket := lambda * waa]
  balls[, raa := raa_run + raa_wicket]

  out <- balls[, .(
    delivery_id, match_id, match_date,
    innings_number = innings, over_number = over, ball_number = ball,
    format = db_format, gender, batter_id, bowler_id,
    exp_runs, exp_wicket, actual_runs, is_wicket, raa_run, waa, raa_wicket, raa
  )]

  if (!write) return(out[])
  store_cricsheet_raa(conn, out, format = format, table_name = table_name)
  invisible(out[])
}


#' Store Per-Ball RAA for Cricsheet Deliveries
#'
#' Replaces this format's rows in `main.<table_name>`, creating the table on
#' first use. Per-format replacement, so rebuilding T20 does not delete ODI.
#'
#' @param conn DBI connection with write access.
#' @param data data.table from [build_cricsheet_raa()].
#' @param format Character. Format whose rows are being replaced.
#' @param table_name Character. Target table in the `main` schema.
#' @return Number of rows inserted, invisibly.
#' @keywords internal
store_cricsheet_raa <- function(conn, data, format,
                                table_name = "cricsheet_ball_raa") {
  db_format <- toupper(format)
  wanted <- c("delivery_id", "match_id", "match_date", "innings_number",
              "over_number", "ball_number", "format", "gender", "batter_id",
              "bowler_id",
              "exp_runs", "exp_wicket", "actual_runs", "is_wicket",
              "raa_run", "waa", "raa_wicket", "raa")

  existing <- DBI::dbGetQuery(conn, sprintf("
    SELECT column_name FROM information_schema.columns
    WHERE table_schema = 'main' AND table_name = '%s'", table_name))$column_name
  if (length(existing) > 0 && !setequal(existing, wanted)) {
    cli::cli_alert_warning(
      "{.field main.{table_name}} has an outdated shape ({length(existing)} column{?s}); recreating it.")
    DBI::dbExecute(conn, sprintf("DROP TABLE main.%s", table_name))
  }

  DBI::dbExecute(conn, sprintf("
    CREATE TABLE IF NOT EXISTS main.%s (
      delivery_id    VARCHAR,
      match_id       VARCHAR,
      match_date     DATE,
      innings_number INTEGER,
      over_number    INTEGER,
      ball_number    INTEGER,
      format         VARCHAR,
      gender         VARCHAR,
      batter_id      VARCHAR,
      bowler_id      VARCHAR,
      exp_runs       DOUBLE,
      exp_wicket     DOUBLE,
      actual_runs    INTEGER,
      is_wicket      INTEGER,
      raa_run        DOUBLE,
      waa            DOUBLE,
      raa_wicket     DOUBLE,
      raa            DOUBLE
    )", table_name))

  DBI::dbExecute(conn, sprintf("DELETE FROM main.%s WHERE format = '%s'",
                               table_name, db_format))
  duckdb::duckdb_register(conn, "cs_raa_staging", data[, ..wanted])
  on.exit(duckdb::duckdb_unregister(conn, "cs_raa_staging"), add = TRUE)
  n <- DBI::dbExecute(conn, sprintf(
    "INSERT INTO main.%s (%s) SELECT %s FROM cs_raa_staging",
    table_name, paste(wanted, collapse = ", "), paste(wanted, collapse = ", ")))
  cli::cli_alert_success("Stored {n} {db_format} rows in {.field main.{table_name}}.")
  invisible(n)
}
