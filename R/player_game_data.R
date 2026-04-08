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
#'
#' @return data.table with one row per player per match. Players who both
#'   bat and bowl get a single row with both batting and bowling columns.
#'   Key columns: match_id, player_id, player_name, team, match_date, role,
#'   batting_runs, batting_wpa, batting_era, bowling_wickets, bowling_wpa,
#'   bowling_era, total_wpa, total_era, plus Hawkeye features.
#'
#' @export
create_player_game_data <- function(format = c("t20", "odi", "test"),
                                    conn = NULL,
                                    match_ids = NULL,
                                    gender = NULL) {

  format <- match.arg(format)
  own_conn <- is.null(conn)
  if (own_conn) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))
  }

  cli::cli_alert_info("Building player game data for {toupper(format)}...")


  # --- Batting aggregation ---
  batting <- .aggregate_batting_game_data(conn, format, match_ids, gender)
  cli::cli_alert_success("Batting: {nrow(batting)} player-match rows")

  # --- Bowling aggregation ---
  bowling <- .aggregate_bowling_game_data(conn, format, match_ids, gender)
  cli::cli_alert_success("Bowling: {nrow(bowling)} player-match rows")

  # --- Merge batting and bowling ---
  pgd <- .merge_batting_bowling(batting, bowling)
  cli::cli_alert_success("Combined: {nrow(pgd)} player-match rows ({sum(pgd$role == 'all_rounder')} all-rounders)")

  # --- Join player names and team from cricinfo.innings ---
  pgd <- .join_player_names(pgd, conn, format)

  pgd
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
                                         gender = NULL) {

  format_filter <- cricinfo_format_sql("m.format", format)
  match_filter <- .build_match_filter(match_ids)
  gender_filter <- .build_gender_filter(gender)

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
        b.win_probability,
        b.predicted_score,
        b.total_innings_runs,

        -- WPA: change in win probability caused by this delivery
        LEAD(b.win_probability) OVER (
          PARTITION BY b.match_id, b.innings_number
          ORDER BY b.over_number, b.ball_number
        ) - b.win_probability AS delta_wp,

        -- ERA: change in projected score caused by this delivery
        LEAD(b.predicted_score) OVER (
          PARTITION BY b.match_id, b.innings_number
          ORDER BY b.over_number, b.ball_number
        ) - b.predicted_score AS delta_ps,

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
  ", format_filter, match_filter, gender_filter)

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
                                          gender = NULL) {

  format_filter <- cricinfo_format_sql("m.format", format)
  match_filter <- .build_match_filter(match_ids)
  gender_filter <- .build_gender_filter(gender)

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
        b.win_probability,
        b.predicted_score,

        -- WPA delta (bowler perspective = negated batting delta)
        LEAD(b.win_probability) OVER (
          PARTITION BY b.match_id, b.innings_number
          ORDER BY b.over_number, b.ball_number
        ) - b.win_probability AS delta_wp,

        -- ERA delta (bowler perspective = negated)
        LEAD(b.predicted_score) OVER (
          PARTITION BY b.match_id, b.innings_number
          ORDER BY b.over_number, b.ball_number
        ) - b.predicted_score AS delta_ps,

        m.start_date AS match_date

      FROM cricinfo.balls b
      JOIN cricinfo.matches m ON b.match_id = m.match_id
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
  ", format_filter, match_filter, gender_filter)

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

  # Fill NA value columns with 0 for the missing role
  batting_cols <- grep("^batting_", names(pgd), value = TRUE)
  bowling_cols <- grep("^bowling_", names(pgd), value = TRUE)
  for (col in batting_cols) {
    data.table::set(pgd, which(is.na(pgd[[col]])), col, 0)
  }
  for (col in bowling_cols) {
    data.table::set(pgd, which(is.na(pgd[[col]])), col, 0)
  }

  # Combined value metrics
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
