# Player Game Data: Per-Player Per-Match Value Metrics
#
# Central function that produces one row per player per match from Cricinfo
# ball-by-ball data, including box-score stats, WPA, ERA, and Hawkeye features.
#
# This is the foundation for all downstream value metrics:
#   - Stat ratings (Phase 2)
#   - PSV/BatV/BowlV (Phase 3)
#   - EPR career ratings (Phase 4)
#   - BOUNCER composite (Phase 5)
#
# ============================================================================
# THE WPA IN THIS FILE IS NOW BOUNCER'S OWN MODEL, BY DEFAULT (D-P6)
# ============================================================================
# batting_wpa / bowling_wpa are built from delta_wp, a LEAD() window difference
# over a win probability. WHICH win probability is the `wp_source` argument:
#
#   "bouncer"  (default) main.cricinfo_ball_win_probability -- our in-match
#              models, written by build_cricinfo_win_probability().
#   "cricinfo"           cricinfo.balls.win_probability -- ESPNcricinfo's own
#              forecaster, scraped by bouncerdata/scripts/cricinfo_scraper.py.
#              Kept so the two can be compared, not because it is preferred.
#
# The switch happened on evidence, not preference. Benchmarked over 20,326 ODI
# deliveries where both numbers exist (docs/NEXT-STEPS.md, 2026-08-13):
#
#     ours     Brier 0.1354   skill +45.8% vs base rate
#     scraped  Brier 0.2208   skill +11.5%
#
# and coverage improves in the same direction rather than trading against it:
#
#     ball-level   ODI 259,894 vs 20,592   T20 261,677 vs 120,007
#     match-level  ODI 898/977 vs 96/977   T20 1,685/1,977 vs 895/1,977
#
# TWO THINGS THAT ARE STILL TRUE AND STILL BITE:
#
# 1. TEST IS NOT COVERED BY EITHER. The scraped column is 0.0% populated for
#    Test, and build_cricinfo_win_probability() handles limited-overs only --
#    Test win probability runs through the decomposed
#    predict_test_win_probability(), which is not batched yet. 355,962 Test
#    deliveries therefore still produce no WPA at all.
#
# 2. SUM() OVER AN ALL-NULL GROUP RETURNS NULL. Matches with no win
#    probability still reach calculate_epr() as NA, so its coverage warning is
#    still load-bearing -- do not silence it. 371 T20/ODI matches have a second
#    innings but no first in cricinfo.balls, so no chase target can be derived
#    and their second innings is deliberately left unscored.
# ============================================================================


#' Create Player Game Data from Cricinfo Ball-by-Ball
#'
#' Aggregates Cricinfo delivery data into one row per player per match,
#' with box-score stats, WPA, ERA, and Hawkeye features for both batting
#' and bowling roles.
#'
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param conn DBI connection to DuckDB. If NULL, opens a read-only connection.
#' @param match_ids Character vector. Specific match IDs to process (NULL = all).
#' @param gender Character. Filter by gender: "male", "female", or NULL for all.
#' @param wp_source Character. Which win probability `batting_wpa` /
#'   `bowling_wpa` are differenced from. `"bouncer"` (default) uses our own
#'   models via `main.cricinfo_ball_win_probability`; `"cricinfo"` uses the
#'   scraped `cricinfo.balls.win_probability`. See the file header for the
#'   benchmark that settled the default, and D-P6 in `docs/DECISIONS.md`.
#'
#' @return data.table with one row per player per match. Players who both
#'   bat and bowl get a single row with both batting and bowling columns.
#'   Key columns: match_id, player_id, player_name, team, match_date, role,
#'   batting_runs, batting_wpa, batting_era, batting_raa, bowling_wickets,
#'   bowling_wpa, bowling_era, total_wpa, total_era, plus Hawkeye features.
#'
#' @export
create_player_game_data <- function(format = c("t20", "odi", "test"),
                                    conn = NULL,
                                    match_ids = NULL,
                                    gender = NULL,
                                    wp_source = c("bouncer", "cricinfo")) {

  format <- match.arg(format)
  wp_source <- match.arg(wp_source)
  own_conn <- is.null(conn)
  if (own_conn) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))
  }

  # Our WP is limited-overs only. Falling back silently would hand Test a
  # column of NA that looks like thin coverage rather than an unsupported path.
  if (wp_source == "bouncer" && format == "test") {
    cli::cli_warn(c(
      "Test win probability is not produced by {.fn build_cricinfo_win_probability}.",
      "!" = "{.field batting_wpa}/{.field bowling_wpa} will be NA for every Test match.",
      "i" = "The scraped column is 0.0% populated for Test too, so neither source has it."
    ))
  }

  cli::cli_alert_info(
    "Building player game data for {toupper(format)} (WPA source: {.val {wp_source}})..."
  )


  # --- Batting aggregation ---
  batting <- .aggregate_batting_game_data(conn, format, match_ids, gender, wp_source)
  cli::cli_alert_success("Batting: {nrow(batting)} player-match rows")

  # --- Bowling aggregation ---
  bowling <- .aggregate_bowling_game_data(conn, format, match_ids, gender, wp_source)
  cli::cli_alert_success("Bowling: {nrow(bowling)} player-match rows")

  # --- Merge batting and bowling ---
  pgd <- .merge_batting_bowling(batting, bowling)
  cli::cli_alert_success("Combined: {nrow(pgd)} player-match rows ({sum(pgd$role == 'all_rounder')} all-rounders)")

  # --- Join player names and team from cricinfo.innings ---
  pgd <- .join_player_names(pgd, conn, format)

  pgd
}


# ============================================================================
# WIN PROBABILITY SOURCE
# ============================================================================

#' SQL Fragments Selecting the Win Probability Source
#'
#' Both aggregations difference a win probability across consecutive
#' deliveries. Which number gets differenced is the whole of D-P6, so it is
#' chosen in exactly one place rather than duplicated into two near-identical
#' queries.
#'
#' The join is on `b.id`, not `(match_id, innings_number, over_number,
#' ball_number)`. That composite is **not** unique in `cricinfo.balls` -- six
#' T20/ODI rows share one, all in match `1099000` innings 1 over 30 -- and
#' joining on it would duplicate those deliveries inside the `SUM()`s below.
#'
#' @param wp_source Character. `"bouncer"` for our model's number from
#'   `main.cricinfo_ball_win_probability` (built by
#'   [build_cricinfo_win_probability()]), `"cricinfo"` for the scraped
#'   `cricinfo.balls.win_probability`.
#'
#' @section The delta is flipped to the batting team's perspective:
#' Both stored win probabilities are single-perspective numbers -- ours is
#' P(the side batting FIRST wins), the scraped column is P(the CHASING side
#' wins) -- so summing raw deltas credits half of all batters with their
#' opponents' fortunes. Measured before the 2026-08-13 fix (bouncerverse#25):
#' corr(batting_wpa, runs) was +0.45 in innings 1 and **-0.43** in innings 2
#' for T20 male -- a chasing batter was docked for scoring, and under the
#' scraped source it was innings-1 batters instead. The flip resolves the
#' batting side per innings from `cricinfo.innings.team_id` against the
#' innings-1 team (so a Test follow-on flips innings 3, not innings 4),
#' falling back to innings parity where the scorecard is missing. `delta_ps`
#' needs no flip: the projected score is always the current batting innings'
#' own, so its delta is already batting-perspective.
#'
#' @return List with `col` (the win probability expression), `delta` (the
#'   batting-perspective delta expression) and `join` (extra FROM-clause
#'   joins, including the innings-team lookup the flip needs).
#'
#' @keywords internal
.wp_source_sql <- function(wp_source = c("bouncer", "cricinfo")) {

  wp_source <- match.arg(wp_source)

  col <- switch(wp_source,
    bouncer  = "w.win_prob_after",
    cricinfo = "b.win_probability"
  )

  # +1 when the striker's team is the side batting first, -1 otherwise.
  team_sign <- "CASE
      WHEN ti.team_id IS NOT NULL AND t1.team_id IS NOT NULL THEN
        CASE WHEN ti.team_id = t1.team_id THEN 1 ELSE -1 END
      WHEN b.innings_number IN (1, 3) THEN 1
      ELSE -1
    END"

  team_join <- "LEFT JOIN (
        SELECT match_id, innings_number, MAX(team_id) AS team_id
        FROM cricinfo.innings GROUP BY match_id, innings_number
      ) ti ON ti.match_id = b.match_id AND ti.innings_number = b.innings_number
      LEFT JOIN (
        SELECT match_id, MAX(team_id) AS team_id
        FROM cricinfo.innings WHERE innings_number = 1 GROUP BY match_id
      ) t1 ON t1.match_id = b.match_id"

  join <- switch(wp_source,
    bouncer  = paste("LEFT JOIN main.cricinfo_ball_win_probability w ON w.id = b.id",
                     team_join, sep = "\n      "),
    cricinfo = team_join
  )

  # The delta a delivery CAUSED is wp_after(i) - wp_after(i-1). Both columns
  # are post-delivery states, so the LEAD form this code used until 2026-08-13
  # computed the NEXT delivery's swing and credited it to this delivery's
  # batter and bowler -- an off-by-one across every WPA-derived rating. See
  # build_cricinfo_win_probability() for the measurement that established it.
  #
  # For our source the delta is precomputed in the table, including the
  # innings-start "before" state that a LAG cannot supply for ball 1. For the
  # scraped column there is no such table, so the LAG is taken here and ball 1
  # of each innings is necessarily NA.
  # Our stored delta is on P(batting first); the scraped column is on
  # P(chasing), so its batting-perspective sign is the OPPOSITE of team_sign.
  delta <- switch(wp_source,
    bouncer  = sprintf("(%s) * w.delta_wp", team_sign),
    cricinfo = sprintf(
      "-(%s) * (%s - LAG(%s) OVER (
          PARTITION BY b.match_id, b.innings_number
          ORDER BY b.over_number, b.ball_number
        ))", team_sign, col, col)
  )

  # ERA differences a projected score and must follow the SAME source. It is
  # not cosmetic: calculate_epr() computes bat_value = batting_wpa +
  # batting_era, so an NA in either kills the pair. cricinfo.predicted_score
  # has exactly the sparse coverage the scraped win probability had -- 7.7% of
  # ODI deliveries, 42.8% of T20 -- so leaving ERA on it would drag EPR back
  # to that level however complete the WPA became.
  ps_col <- switch(wp_source,
    bouncer  = "w.proj_score_after",
    cricinfo = "b.predicted_score"
  )
  ps_delta <- switch(wp_source,
    bouncer  = "w.delta_ps",
    cricinfo = sprintf(
      "%s - LAG(%s) OVER (
          PARTITION BY b.match_id, b.innings_number
          ORDER BY b.over_number, b.ball_number
        )", ps_col, ps_col)
  )

  list(col = col, delta = delta, join = join,
       ps_col = ps_col, ps_delta = ps_delta)
}


#' SQL Fragments Selecting Per-Ball RAA
#'
#' Runs Above Average is produced per delivery by [build_cricinfo_raa()] into
#' `main.cricinfo_ball_raa`. Formats whose lambda is not fitted yet (and any
#' database where the builder has not run) have no rows there -- and possibly
#' no table -- so the fragment degrades to a NULL column rather than a broken
#' join, and `batting_raa` arrives as NA exactly like an unscored WPA.
#'
#' @param conn DBI connection, used to check the table exists.
#'
#' @return List with `col` (per-ball RAA expression) and `join` (FROM-clause
#'   join, possibly empty).
#'
#' @keywords internal
.raa_sql <- function(conn) {
  has_table <- nrow(DBI::dbGetQuery(conn, "
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'main' AND table_name = 'cricinfo_ball_raa'
  ")) > 0

  if (has_table) {
    list(col = "r.raa", join = "LEFT JOIN main.cricinfo_ball_raa r ON r.id = b.id")
  } else {
    list(col = "CAST(NULL AS DOUBLE)", join = "")
  }
}


# ============================================================================
# BATTING AGGREGATION
# ============================================================================

#' Aggregate Batting Stats Per Player Per Match
#'
#' @param conn DBI connection
#' @param format Character. Match format.
#' @param match_ids Character vector or NULL.
#' @param gender Character or NULL.
#'
#' @return data.table with batting stats per batter per match.
#' @keywords internal
.aggregate_batting_game_data <- function(conn, format, match_ids = NULL,
                                         gender = NULL, wp_source = "bouncer") {

  format_filter <- cricinfo_format_sql("m.format", format)
  match_filter <- .build_match_filter(match_ids)
  gender_filter <- .build_gender_filter(gender)
  wp <- .wp_source_sql(wp_source)
  raa <- .raa_sql(conn)

  query <- sprintf("
    WITH deliveries_with_delta AS (
      SELECT
        b.match_id,
        b.batsman_player_id AS player_id,
        b.innings_number,
        b.over_number,
        b.ball_number,
        b.batsman_runs,
        b.total_runs,
        b.is_four,
        b.is_six,
        b.is_wicket,
        b.dismissal_type,
        b.wides,
        b.noballs,
        b.wagon_zone,
        b.wagon_x,
        b.pitch_line,
        b.pitch_length,
        b.shot_type,
        b.shot_control,
        %s AS win_probability,
        %s AS predicted_score,
        b.total_innings_runs,

        -- WPA: change in win probability caused by this delivery
        %s AS delta_wp,

        -- ERA: change in projected score caused by this delivery
        %s AS delta_ps,

        -- RAA: per-ball runs above the agnostic (state-only) expectation
        %s AS raa,

        -- Match metadata
        m.start_date AS match_date,
        COALESCE(
          CASE WHEN m.team1_id = (
            -- Determine batting team from innings context
            SELECT b2.bowler_player_id FROM cricinfo.balls b2
            WHERE b2.match_id = b.match_id LIMIT 1
          ) THEN m.team2_name ELSE m.team1_name END,
          'Unknown'
        ) AS team_name_fallback

      FROM cricinfo.balls b
      JOIN cricinfo.matches m ON b.match_id = m.match_id
      %s
      %s
      WHERE %s
        AND b.batsman_player_id IS NOT NULL
        AND (b.wides IS NULL OR b.wides = 0)
        %s
        %s
    )
    SELECT
      match_id,
      player_id,
      MIN(match_date) AS match_date,

      -- Box-score stats
      COUNT(*) AS batting_balls_faced,
      SUM(batsman_runs) AS batting_runs,
      SUM(CASE WHEN is_four THEN 1 ELSE 0 END) AS batting_fours,
      SUM(CASE WHEN is_six THEN 1 ELSE 0 END) AS batting_sixes,
      SUM(CASE WHEN is_four OR is_six THEN 1 ELSE 0 END) AS batting_boundaries,
      SUM(CASE WHEN batsman_runs = 0 AND NOT is_wicket THEN 1 ELSE 0 END) AS batting_dot_balls,
      SUM(CASE WHEN is_wicket AND dismissal_type NOT IN ('retired hurt', 'retired not out', 'retired out')
          THEN 1 ELSE 0 END) AS batting_dismissed,
      ROUND(SUM(batsman_runs) * 100.0 / NULLIF(COUNT(*), 0), 2) AS batting_strike_rate,

      -- WPA (from Cricinfo win_probability)
      SUM(delta_wp) AS batting_wpa,
      MAX(ABS(COALESCE(delta_wp, 0))) AS batting_max_wpa,
      AVG(CASE WHEN delta_wp > 0 THEN 1.0 WHEN delta_wp < 0 THEN 0.0 ELSE NULL END) AS batting_positive_wpa_pct,

      -- ERA (from Cricinfo predicted_score for 1st innings)
      SUM(delta_ps) AS batting_era,

      -- RAA (agnostic-baseline runs above average; NA when the match is
      -- unscored, partial-coverage visible via batting_raa_balls)
      SUM(raa) AS batting_raa,
      SUM(CASE WHEN raa IS NOT NULL THEN 1 ELSE 0 END) AS batting_raa_balls,

      -- Hawkeye batting features (per-match)
      AVG(CASE WHEN shot_control = 'controlled' THEN 1.0
          WHEN shot_control IS NOT NULL THEN 0.0
          ELSE NULL END) AS batting_pct_controlled,
      AVG(CASE WHEN shot_type IN ('drive', 'pull', 'sweep', 'hook', 'cut', 'slog',
                                   'reverse sweep', 'scoop') THEN 1.0
          WHEN shot_type IS NOT NULL THEN 0.0
          ELSE NULL END) AS batting_pct_attacking,
      AVG(CASE WHEN wagon_x > 0 THEN 1.0
          WHEN wagon_x IS NOT NULL THEN 0.0
          ELSE NULL END) AS batting_pct_leg_side,
      SUM(CASE WHEN pitch_length IS NOT NULL THEN 1 ELSE 0 END) AS batting_hawkeye_balls

    FROM deliveries_with_delta
    GROUP BY match_id, player_id
    ORDER BY match_id, batting_runs DESC
  ", wp$col, wp$ps_col, wp$delta, wp$ps_delta, raa$col, wp$join, raa$join,
     format_filter, match_filter, gender_filter)

  result <- DBI::dbGetQuery(conn, query)
  data.table::as.data.table(result)
}


# ============================================================================
# BOWLING AGGREGATION
# ============================================================================

#' Aggregate Bowling Stats Per Player Per Match
#'
#' @inheritParams .aggregate_batting_game_data
#' @return data.table with bowling stats per bowler per match.
#' @keywords internal
.aggregate_bowling_game_data <- function(conn, format, match_ids = NULL,
                                          gender = NULL, wp_source = "bouncer") {

  format_filter <- cricinfo_format_sql("m.format", format)
  match_filter <- .build_match_filter(match_ids)
  gender_filter <- .build_gender_filter(gender)
  wp <- .wp_source_sql(wp_source)

  query <- sprintf("
    WITH deliveries_with_delta AS (
      SELECT
        b.match_id,
        b.bowler_player_id AS player_id,
        b.innings_number,
        b.over_number,
        b.ball_number,
        b.batsman_runs,
        b.total_runs,
        b.is_four,
        b.is_six,
        b.is_wicket,
        b.dismissal_type,
        b.wides,
        b.noballs,
        b.byes,
        b.legbyes,
        b.pitch_line,
        b.pitch_length,
        b.shot_control,
        %s AS win_probability,
        %s AS predicted_score,

        -- WPA delta (bowler perspective = negated batting delta)
        %s AS delta_wp,

        -- ERA delta (bowler perspective = negated)
        %s AS delta_ps,

        m.start_date AS match_date

      FROM cricinfo.balls b
      JOIN cricinfo.matches m ON b.match_id = m.match_id
      %s
      WHERE %s
        AND b.bowler_player_id IS NOT NULL
        %s
        %s
    )
    SELECT
      match_id,
      player_id,
      MIN(match_date) AS match_date,

      -- Box-score stats
      -- Legal deliveries only (exclude wides for ball count)
      SUM(CASE WHEN wides IS NULL OR wides = 0 THEN 1 ELSE 0 END) AS bowling_balls_bowled,
      COUNT(*) AS bowling_total_deliveries,
      SUM(total_runs) AS bowling_runs_conceded,
      SUM(CASE WHEN is_wicket AND dismissal_type NOT IN ('run out', 'retired hurt',
          'retired not out', 'retired out', 'obstructing the field')
          THEN 1 ELSE 0 END) AS bowling_wickets,
      SUM(CASE WHEN is_four THEN 1 ELSE 0 END) AS bowling_fours_conceded,
      SUM(CASE WHEN is_six THEN 1 ELSE 0 END) AS bowling_sixes_conceded,
      SUM(CASE WHEN is_four OR is_six THEN 1 ELSE 0 END) AS bowling_boundaries_conceded,
      SUM(CASE WHEN total_runs = 0 AND (wides IS NULL OR wides = 0) THEN 1 ELSE 0 END) AS bowling_dot_balls,
      SUM(COALESCE(wides, 0)) AS bowling_wides,
      SUM(COALESCE(noballs, 0)) AS bowling_noballs,
      ROUND(SUM(total_runs) * 6.0 / NULLIF(
        SUM(CASE WHEN wides IS NULL OR wides = 0 THEN 1 ELSE 0 END), 0), 2) AS bowling_economy,

      -- WPA (negated: bowling team benefits when batting WP drops)
      -SUM(delta_wp) AS bowling_wpa,
      MAX(ABS(COALESCE(delta_wp, 0))) AS bowling_max_wpa,

      -- ERA (negated: conceding fewer than expected = positive bowler ERA)
      -SUM(delta_ps) AS bowling_era,

      -- Hawkeye bowling features (per-match)
      AVG(CASE WHEN LOWER(pitch_length) IN ('good', 'good length') THEN 1.0
          WHEN pitch_length IS NOT NULL THEN 0.0
          ELSE NULL END) AS bowling_pct_good_length,
      AVG(CASE WHEN LOWER(pitch_line) IN ('stumps', 'off stump', 'middle', 'leg stump') THEN 1.0
          WHEN pitch_line IS NOT NULL THEN 0.0
          ELSE NULL END) AS bowling_pct_on_stump,
      AVG(CASE WHEN shot_control = 'controlled' THEN 0.0
          WHEN shot_control IS NOT NULL THEN 1.0
          ELSE NULL END) AS bowling_pct_beat_bat,
      SUM(CASE WHEN pitch_length IS NOT NULL THEN 1 ELSE 0 END) AS bowling_hawkeye_balls

    FROM deliveries_with_delta
    GROUP BY match_id, player_id
    ORDER BY match_id, bowling_wickets DESC
  ", wp$col, wp$ps_col, wp$delta, wp$ps_delta, wp$join,
     format_filter, match_filter, gender_filter)

  result <- DBI::dbGetQuery(conn, query)
  data.table::as.data.table(result)
}


# ============================================================================
# MERGE & ROLE ASSIGNMENT
# ============================================================================

#' Merge Batting and Bowling Game Data
#'
#' Full outer join of batting and bowling data per player per match.
#' Players who bat AND bowl get role = "all_rounder".
#'
#' @param batting data.table from .aggregate_batting_game_data()
#' @param bowling data.table from .aggregate_bowling_game_data()
#'
#' @return data.table with one row per player per match.
#' @keywords internal
.merge_batting_bowling <- function(batting, bowling) {

  # Full outer join on match_id + player_id
  pgd <- merge(batting, bowling,
               by = c("match_id", "player_id"),
               all = TRUE,
               suffixes = c("_bat", "_bowl"))

  # Resolve match_date from whichever side has it
  pgd[, match_date := data.table::fcoalesce(match_date_bat, match_date_bowl)]
  pgd[, c("match_date_bat", "match_date_bowl") := NULL]

  # Assign role based on which columns are populated
  pgd[, role := data.table::fcase(
    !is.na(batting_balls_faced) & !is.na(bowling_balls_bowled), "all_rounder",
    !is.na(batting_balls_faced), "batter",
    !is.na(bowling_balls_bowled), "bowler",
    default = "unknown"
  )]

  # Fill NA value columns with 0 for the missing role.
  #
  # NA here has TWO causes and they do not deserve the same treatment:
  #
  #   (a) the player did not bat (or did not bowl) in this match. Zero is the
  #       right answer -- they contributed no runs, no wickets, no WPA.
  #   (b) the player DID bat, but the match has no win probability, so
  #       SUM(delta_wp) came back NULL over an all-NULL group.
  #
  # Filling (b) with zero fabricates a neutral performance. Measured on the
  # scraped source, that was 13,668 of 15,012 ODI player-match rows -- 91% of
  # the format carrying an invented 0 WPA that calculate_epr() then consumed as
  # real. It also silently disarms that function's coverage warning, which can
  # only fire on NA and by this point never sees one.
  #
  # So the role mask is captured BEFORE any filling, and value columns are
  # zeroed only for the role the player did not perform.
  did_bat  <- !is.na(pgd$batting_balls_faced)
  did_bowl <- !is.na(pgd$bowling_balls_bowled)

  # Columns whose NA means "no win probability / no projected score for this
  # match", not "no contribution". These stay NA for a player who did perform.
  value_cols <- c(
    "batting_wpa", "batting_max_wpa", "batting_positive_wpa_pct", "batting_era",
    "batting_raa",
    "bowling_wpa", "bowling_max_wpa", "bowling_era"
  )

  batting_cols <- grep("^batting_", names(pgd), value = TRUE)
  bowling_cols <- grep("^bowling_", names(pgd), value = TRUE)

  for (col in batting_cols) {
    na_rows <- which(is.na(pgd[[col]]))
    if (col %in% value_cols) na_rows <- na_rows[!did_bat[na_rows]]
    data.table::set(pgd, na_rows, col, 0)
  }
  for (col in bowling_cols) {
    na_rows <- which(is.na(pgd[[col]]))
    if (col %in% value_cols) na_rows <- na_rows[!did_bowl[na_rows]]
    data.table::set(pgd, na_rows, col, 0)
  }

  # Combined value metrics. NA propagates deliberately: a total_wpa built by
  # treating an unmeasured innings as zero is not a total.
  pgd[, total_wpa := batting_wpa + bowling_wpa]
  pgd[, total_era := batting_era + bowling_era]

  # Reorder columns: identifiers first, then batting, bowling, combined
  id_cols <- c("match_id", "player_id", "match_date", "role")
  bat_cols <- sort(grep("^batting_", names(pgd), value = TRUE))
  bowl_cols <- sort(grep("^bowling_", names(pgd), value = TRUE))
  value_cols <- c("total_wpa", "total_era")
  other_cols <- setdiff(names(pgd), c(id_cols, bat_cols, bowl_cols, value_cols))
  data.table::setcolorder(pgd, c(id_cols, bat_cols, bowl_cols, value_cols, other_cols))

  # Sort by match_date, match_id, total_wpa descending
  data.table::setorder(pgd, match_date, match_id, -total_wpa)

  pgd
}


# ============================================================================
# SQL FILTER HELPERS
# ============================================================================

#' Build match ID filter clause for SQL
#' @param match_ids Character vector or NULL.
#' @return Character string for SQL WHERE clause (empty string if NULL).
#' @keywords internal
.build_match_filter <- function(match_ids) {
  if (is.null(match_ids)) return("")
  validate_match_ids(match_ids, context = ".build_match_filter")
  ids_sql <- paste(sprintf("'%s'", escape_sql_quotes(match_ids)), collapse = ", ")
  sprintf("AND b.match_id IN (%s)", ids_sql)
}

#' Build gender filter clause for SQL
#' @param gender Character or NULL.
#' @return Character string for SQL WHERE clause (empty string if NULL).
#' @keywords internal
.build_gender_filter <- function(gender) {
  if (is.null(gender)) return("")
  gender <- tolower(gender)
  if (!gender %in% c("male", "female")) {
    cli::cli_abort("gender must be 'male' or 'female', not {.val {gender}}")
  }
  sprintf("AND LOWER(m.gender) = '%s'", escape_sql_quotes(gender))
}


#' Join Player Names and Team from cricinfo.balls title field
#'
#' Parses the "Bowler to Batter" title field to extract player names,
#' then joins team names from cricinfo.matches.
#'
#' @param pgd data.table with player_id and match_id columns.
#' @param conn DBI connection.
#' @param format Character. Match format.
#'
#' @return pgd with added player_name and team columns.
#' @keywords internal
.join_player_names <- function(pgd, conn, format) {
  format_sql <- cricinfo_format_sql("m.format", format)

  # Initialize columns
  if (!"player_name" %in% names(pgd)) pgd[, player_name := NA_character_]
  if (!"team" %in% names(pgd)) pgd[, team := NA_character_]

  # Build name lookup from title field: "Bowler to Batter"
  # Use most common name per player_id (handles minor variations)
  name_lookup <- tryCatch({
    data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
      WITH batter_names AS (
        SELECT batsman_player_id AS player_id,
               SPLIT_PART(b.title, ' to ', 2) AS name,
               COUNT(*) AS n
        FROM cricinfo.balls b
        JOIN cricinfo.matches m ON b.match_id = m.match_id
        WHERE %s AND b.title IS NOT NULL AND b.title LIKE '%%to%%'
        GROUP BY batsman_player_id, SPLIT_PART(b.title, ' to ', 2)
      ),
      bowler_names AS (
        SELECT bowler_player_id AS player_id,
               SPLIT_PART(b.title, ' to ', 1) AS name,
               COUNT(*) AS n
        FROM cricinfo.balls b
        JOIN cricinfo.matches m ON b.match_id = m.match_id
        WHERE %s AND b.title IS NOT NULL AND b.title LIKE '%%to%%'
        GROUP BY bowler_player_id, SPLIT_PART(b.title, ' to ', 1)
      ),
      all_names AS (
        SELECT * FROM batter_names
        UNION ALL
        SELECT * FROM bowler_names
      )
      SELECT player_id, name AS player_name
      FROM (
        SELECT player_id, name,
               ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY n DESC) AS rn
        FROM all_names
      ) ranked
      WHERE rn = 1
    ", format_sql, format_sql)))
  }, error = function(e) {
    cli::cli_alert_warning("Could not parse player names from title: {e$message}")
    return(NULL)
  })

  if (!is.null(name_lookup) && nrow(name_lookup) > 0) {
    pgd[name_lookup, on = "player_id", player_name := i.player_name]
  }

  # Team assignment: determine which team each player belongs to per match
  # Batters in innings 1 play for team1 (batting first), bowlers in innings 1 for team2
  team_info <- tryCatch({
    data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
      SELECT DISTINCT
        b.match_id,
        b.batsman_player_id AS player_id,
        CASE WHEN b.innings_number IN (1, 3) THEN m.team1_name
             ELSE m.team2_name END AS team
      FROM cricinfo.balls b
      JOIN cricinfo.matches m ON b.match_id = m.match_id
      WHERE %s AND b.batsman_player_id IS NOT NULL
      UNION
      SELECT DISTINCT
        b.match_id,
        b.bowler_player_id AS player_id,
        CASE WHEN b.innings_number IN (1, 3) THEN m.team2_name
             ELSE m.team1_name END AS team
      FROM cricinfo.balls b
      JOIN cricinfo.matches m ON b.match_id = m.match_id
      WHERE %s AND b.bowler_player_id IS NOT NULL
    ", format_sql, format_sql)))
  }, error = function(e) {
    cli::cli_warn("Could not assign teams: {e$message}")
    NULL
  })

  if (!is.null(team_info) && nrow(team_info) > 0) {
    # Deduplicate: take first team assignment per player-match
    team_info <- team_info[!duplicated(team_info, by = c("match_id", "player_id"))]
    pgd[team_info, on = c("match_id", "player_id"), team := i.team]
  }

  n_named <- sum(!is.na(pgd$player_name))
  n_teamed <- sum(!is.na(pgd$team))
  cli::cli_alert_info("Names: {n_named}/{nrow(pgd)} ({round(n_named/nrow(pgd)*100, 1)}%), Teams: {n_teamed}/{nrow(pgd)}")

  pgd
}
