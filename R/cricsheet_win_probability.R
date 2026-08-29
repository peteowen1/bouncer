# Win Probability for Every Cricsheet Delivery
#
# The companion to build_cricinfo_win_probability(), over the deeper source
# (bouncerverse#33). calculate_rolling_features() is already written against
# cricsheet's column names (runs_total / is_four / is_six), so the 14 momentum
# features are native here rather than reconstructed.
#
# KNOWN FIDELITY LIMIT, measured not assumed. predict_win_probability_batch()
# takes `overs` in cricket notation and converts with overs_to_balls(), which
# CLAMPS partial balls at 6. Training used `balls_bowled = over * 6 + ball`
# unclamped, and cricsheet's `ball` counts extras and reaches 19 -- 93,325 T20
# deliveries (3.1%) sit at ball 7 or beyond. For those the served state is a
# few balls earlier than training's. The batch API cannot express them, so this
# is a bounded, documented divergence rather than a silent one; it is the same
# root cause as D-P5.

#' Build Win Probability for Every Cricsheet Delivery
#'
#' Scores every Cricsheet limited-overs delivery with the in-match models and
#' writes `main.bouncer_wp_from_cricsheet`.
#'
#' @param format Character. "t20" or "odi". Test is decomposed differently --
#'   see [build_cricinfo_test_win_probability()].
#' @param conn DBI connection. If NULL, opens one and closes it on exit.
#' @param models In-match models; NULL resolves via [load_in_match_models()].
#' @param write Logical. Write the table, or return the scored frame.
#' @param table_name Character. Target table in the `main` schema.
#' @param exclude_short_overs Logical. Drop `balls_per_over != 6` matches (The
#'   Hundred is filed as T20), matching [build_cricsheet_raa()].
#'
#' @return data.table with `win_prob_before`/`after`, `delta_wp` and the
#'   projected-score triple. Invisibly when `write = TRUE`.
#'
#' @export
build_cricsheet_win_probability <- function(format = c("t20", "odi"),
                                            conn = NULL,
                                            models = NULL,
                                            write = TRUE,
                                            table_name = "bouncer_wp_from_cricsheet",
                                            exclude_short_overs = TRUE) {

  format <- match.arg(format)
  db_format <- toupper(format)

  own_conn <- is.null(conn)
  if (own_conn) {
    conn <- get_db_connection(read_only = !write)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }
  if (is.null(models)) models <- load_in_match_models(format)

  types <- if (format == "t20") c("t20", "it20") else c("odi", "odm")
  type_list <- paste0("'", types, "'", collapse = ", ")
  short_over_filter <- if (exclude_short_overs) "AND COALESCE(m.balls_per_over, 6) = 6" else ""

  cli::cli_alert_info("Loading {db_format} deliveries from cricsheet.deliveries...")
  balls <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT d.delivery_id, d.match_id, d.match_date, d.innings, d.over, d.ball,
           d.batter_id, d.bowler_id, d.gender, d.venue,
           d.runs_total, d.runs_batter,
           CAST(d.is_wicket AS INT) AS is_wicket,
           CAST(d.is_four AS INT)   AS is_four,
           CAST(d.is_six AS INT)    AS is_six,
           d.total_runs, d.wickets_fallen
    FROM cricsheet.deliveries d
    JOIN cricsheet.matches m ON m.match_id = d.match_id
    WHERE LOWER(d.match_type) IN (%1$s)
      %2$s
      AND d.innings IN (1, 2)
  ", type_list, short_over_filter)))
  if (!nrow(balls)) cli::cli_abort("No {db_format} deliveries found.")
  cli::cli_alert_info("{nrow(balls)} deliveries across {data.table::uniqueN(balls$match_id)} matches.")

  balls <- data.table::as.data.table(calculate_rolling_features(balls))
  mom_cols <- grep("_last_", names(balls), value = TRUE)
  for (nm in mom_cols) balls[is.na(get(nm)), (nm) := 0]

  # Target: the first innings' final total plus one.
  inn1 <- balls[innings == 1L, .(target = max(total_runs, na.rm = TRUE) + 1L,
                                 innings1_wickets = max(wickets_fallen, na.rm = TRUE)),
                by = match_id]
  balls <- merge(balls, inn1, by = "match_id", all.x = TRUE)
  balls[is.na(innings1_wickets), innings1_wickets := 10L]
  unscoreable <- balls[innings == 2L & is.na(target), .N]
  if (unscoreable > 0) {
    cli::cli_warn(c(
      "{unscoreable} second-innings deliveries have no first-innings total.",
      "i" = "They stay NA rather than being scored against an assumed target."))
  }

  # Per-venue statistics from cricsheet's own history, the same three the batch
  # predictor expects. Venues below the match threshold fall back to the format
  # default rather than to a one-match average.
  v1 <- balls[innings == 1L, .(i1 = max(total_runs, na.rm = TRUE)), by = .(venue, match_id)]
  vavg <- v1[, .(venue_avg_score = mean(i1), n = .N), by = venue]
  v2 <- balls[innings == 2L, .(i2 = max(total_runs, na.rm = TRUE)), by = .(venue, match_id)]
  vv <- merge(v1, v2, by = c("venue", "match_id"))
  vchase <- vv[, .(venue_chase_success_rate = mean(i2 >= i1),
                   venue_avg_second_innings = mean(i2)), by = venue]
  dflt <- get_default_venue_stats(format)
  vstats <- merge(vavg, vchase, by = "venue", all = TRUE)
  MIN_V <- 5L
  # Field names are avg_first_innings / avg_second_innings / chase_win_rate --
  # checked against get_default_venue_stats(), not guessed. A `%||%` fallback on
  # a wrong name would silently substitute a different statistic.
  stopifnot(all(c("avg_first_innings", "avg_second_innings", "chase_win_rate")
                %in% names(dflt)))
  vstats[is.na(n) | n < MIN_V, `:=`(
    venue_avg_score = dflt$avg_first_innings,
    venue_chase_success_rate = dflt$chase_win_rate,
    venue_avg_second_innings = dflt$avg_second_innings)]
  balls <- merge(balls, vstats[, .(venue, venue_avg_score, venue_chase_success_rate,
                                   venue_avg_second_innings)],
                 by = "venue", all.x = TRUE)
  balls[, gender_male := as.integer(tolower(gender) == "male")]
  data.table::setorder(balls, match_id, innings, over, ball)
  scoreable <- balls[innings == 1L | !is.na(target), which = TRUE]

  mk_states <- function(idx, score, wkts, overs) {
    s <- data.frame(
      current_score = score, wickets = wkts, overs = overs,
      innings = balls$innings[idx],
      target = balls$target[idx],
      gender_male = balls$gender_male[idx],
      venue_avg_score = balls$venue_avg_score[idx],
      venue_chase_success_rate = balls$venue_chase_success_rate[idx],
      venue_avg_second_innings = balls$venue_avg_second_innings[idx],
      innings1_wickets = balls$innings1_wickets[idx])
    cbind(s, as.data.frame(balls[idx, ..mom_cols]))
  }

  # AFTER state: this ball's own row. `ball` is clamped to 6 for the cricket
  # notation the batch API takes -- see the fidelity note at the top.
  cli::cli_alert_info("Scoring...")
  t0 <- Sys.time()
  after <- predict_win_probability_batch(
    mk_states(scoreable, balls$total_runs[scoreable], balls$wickets_fallen[scoreable],
              balls$over[scoreable] + pmin(balls$ball[scoreable], 6L) / 10),
    format = format, models = models, detail = TRUE)

  # BEFORE state: the same ball rolled back one delivery (epv_delta), never the
  # previous row's after -- a LAG across a data gap charges the gap's drift to
  # whoever bowls next (D-P14, bouncer 0e802dc).
  pre_mom <- paste0(mom_cols, "_pre")
  for (nm in mom_cols) {
    balls[, (paste0(nm, "_pre")) := data.table::shift(get(nm), 1L, type = "lag", fill = 0),
          by = .(match_id, innings)]
  }
  pre_ball <- pmin(balls$ball[scoreable], 6L) - 1L
  pre_over <- balls$over[scoreable]
  roll <- pre_ball < 0L
  pre_ball[roll] <- 0L
  s_pre <- mk_states(scoreable,
                     pmax(balls$total_runs[scoreable] - balls$runs_total[scoreable], 0L),
                     pmax(balls$wickets_fallen[scoreable] - balls$is_wicket[scoreable], 0L),
                     pre_over + pre_ball / 10)
  s_pre[, mom_cols] <- as.data.frame(balls[scoreable, ..pre_mom])
  before <- predict_win_probability_batch(s_pre, format = format, models = models,
                                          detail = TRUE)
  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  cli::cli_alert_success("Scored {length(scoreable)} deliveries twice in {round(elapsed, 1)}s.")

  balls[, `:=`(win_prob_after = NA_real_, proj_score_after = NA_real_,
               win_prob_before = NA_real_, proj_score_before = NA_real_)]
  balls[scoreable, `:=`(win_prob_after = after$win_prob,
                        proj_score_after = after$projected_score,
                        win_prob_before = before$win_prob,
                        proj_score_before = before$projected_score)]
  balls[, delta_wp := win_prob_after - win_prob_before]
  balls[, delta_ps := proj_score_after - proj_score_before]

  out <- balls[, .(delivery_id, match_id, match_date,
                   innings_number = innings, over_number = over, ball_number = ball,
                   format = db_format, gender, batter_id, bowler_id,
                   win_prob_before, win_prob_after, delta_wp,
                   proj_score_before, proj_score_after, delta_ps)]
  if (!write) return(out[])
  store_cricsheet_wp(conn, out, format = format, table_name = table_name)
  invisible(out[])
}


#' Store Per-Ball Win Probability for Cricsheet Deliveries
#' @param conn DBI connection with write access.
#' @param data data.table from [build_cricsheet_win_probability()].
#' @param format Character. Format whose rows are replaced.
#' @param table_name Character. Target table in the `main` schema.
#' @return Rows inserted, invisibly.
#' @keywords internal
.cricsheet_wp_schema <- c(
  delivery_id = "VARCHAR", match_id = "VARCHAR", match_date = "DATE",
  innings_number = "INTEGER", over_number = "INTEGER", ball_number = "INTEGER",
  format = "VARCHAR", gender = "VARCHAR", batter_id = "VARCHAR",
  bowler_id = "VARCHAR", win_prob_before = "DOUBLE", win_prob_after = "DOUBLE",
  delta_wp = "DOUBLE", proj_score_before = "DOUBLE",
  proj_score_after = "DOUBLE", delta_ps = "DOUBLE")

store_cricsheet_wp <- function(conn, data, format,
                               table_name = "bouncer_wp_from_cricsheet") {
  db_format <- toupper(format)
  wanted <- names(data)
  extra <- setdiff(wanted, names(.cricsheet_wp_schema))
  if (length(extra)) {
    cli::cli_abort(c(
      "{.arg data} carries {length(extra)} column{?s} the table has no home for: {.field {extra}}.",
      "i" = "Add them to {.code .cricsheet_wp_schema} deliberately, so the
             migration can create them, rather than letting the shape drift."))
  }

  # Replacement is PER FORMAT, and the table holds every format. This used to
  # answer any shape mismatch by dropping the whole table, so a schema change
  # would silently destroy the other formats' rows -- the same defect as
  # bouncerverse#45, in a different file, on a 5.5M-row table that feeds TSA
  # and the kappa fit. It now migrates instead.
  #
  # DELETE and INSERT also share ONE transaction. DuckDB auto-commits each
  # dbExecute, so without it a DELETE that succeeds followed by an INSERT that
  # fails leaves that format PERMANENTLY EMPTY -- "replacement" that destroys
  # what it was replacing, on a table nothing else can regenerate quickly.
  duckdb::duckdb_register(conn, "cs_wp_staging", data)
  on.exit(duckdb::duckdb_unregister(conn, "cs_wp_staging"), add = TRUE)
  cols <- paste(wanted, collapse = ", ")

  n <- .in_transaction(conn, function() {
    DBI::dbExecute(conn, sprintf(
      "CREATE TABLE IF NOT EXISTS main.%s (\n%s\n    )",
      table_name, .schema_ddl(.cricsheet_wp_schema)))
    .migrate_schema(conn, table_name, .cricsheet_wp_schema)
    DBI::dbExecute(conn, sprintf("DELETE FROM main.%s WHERE format = '%s'",
                                 table_name, db_format))
    DBI::dbExecute(conn, sprintf(
      "INSERT INTO main.%s (%s) SELECT %s FROM cs_wp_staging",
      table_name, cols, cols))
  })
  cli::cli_alert_success("Stored {n} {db_format} rows in {.field main.{table_name}}.")
  invisible(n)
}
