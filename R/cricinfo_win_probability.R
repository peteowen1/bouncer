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


#' Build Bouncer Win Probability for Every Cricinfo Delivery
#'
#' Scores every delivery of a limited-overs format from `cricinfo.balls` with
#' bouncer's own in-match models and writes the result to
#' `main.cricinfo_ball_win_probability`.
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
                                           table_name = "cricinfo_ball_win_probability") {

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
           COALESCE(b.is_wicket, FALSE) AS is_wicket
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

  states <- data.frame(
    current_score = balls$score[scoreable],
    wickets       = balls$wickets[scoreable],
    overs         = balls$overs_actual[scoreable],
    innings       = balls$innings[scoreable],
    target        = balls$target[scoreable]
  )
  states <- cbind(states, as.data.frame(balls[scoreable, ..mom_cols]))

  cli::cli_alert_info("Scoring...")
  t0 <- Sys.time()
  wp <- predict_win_probability_batch(states, format = format, models = models)
  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))

  balls[, win_probability := NA_real_]
  balls[scoreable, win_probability := wp]

  n_scored <- sum(!is.na(balls$win_probability))
  cli::cli_alert_success(
    "Scored {n_scored}/{nrow(balls)} deliveries in {round(elapsed, 1)}s ({round(1000 * elapsed / max(n_scored, 1), 3)} ms/ball)."
  )

  out <- balls[, .(
    id,
    match_id,
    innings_number = innings,
    over_number    = over,
    ball_number    = ball,
    format         = db_format,
    win_probability
  )]

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
                                           table_name = "cricinfo_ball_win_probability") {

  db_format <- toupper(format)

  DBI::dbExecute(conn, sprintf("
    CREATE TABLE IF NOT EXISTS main.%s (
      id              VARCHAR,
      match_id        VARCHAR,
      innings_number  INTEGER,
      over_number     DOUBLE,
      ball_number     INTEGER,
      format          VARCHAR,
      win_probability DOUBLE
    )", table_name))

  duckdb::duckdb_register(conn, "cwp_staging", as.data.frame(data))
  on.exit(duckdb::duckdb_unregister(conn, "cwp_staging"), add = TRUE)

  removed <- DBI::dbExecute(conn, sprintf(
    "DELETE FROM main.%s WHERE format = '%s'", table_name, db_format
  ))

  cols <- c("id", "match_id", "innings_number", "over_number",
            "ball_number", "format", "win_probability")
  col_list <- paste(cols, collapse = ", ")

  n <- DBI::dbExecute(conn, sprintf(
    "INSERT INTO main.%s (%s) SELECT %s FROM cwp_staging",
    table_name, col_list, col_list
  ))

  cli::cli_alert_success(
    "Stored {n} {db_format} rows in {.field main.{table_name}}{if (removed > 0) paste0(' (replaced ', removed, ')') else ''}."
  )
  invisible(n)
}
