# Hawkeye-Based Player Skill Features
#
# Extracts advanced player features from Cricinfo Hawkeye data
# (wagon wheel, pitch line/length, shot type, shot control).
# These features enrich the existing EMA-based skill indices.


#' Calculate Hawkeye Batter Features
#'
#' Computes advanced batting features from Cricinfo Hawkeye data:
#' wagon zone distributions, shot type tendencies, and shot control rates.
#'
#' @param conn DBI connection
#' @param player_ids Character vector of batter IDs (optional, NULL = all)
#' @param min_balls Integer. Minimum balls with Hawkeye data for reliable features
#'
#' @return data.table with one row per batter:
#'   - pct_leg_side: % of runs scored on leg side
#'   - pct_v_zone: % of shots in the V (wagon zones 1-3)
#'   - pct_boundary_leg: % of boundaries to leg side
#'   - shot_diversity: Number of distinct shot types used (normalized)
#'   - pct_controlled: % of shots rated as controlled
#'   - pct_attacking: % of attacking shot types (drive, pull, sweep, etc.)
#'   - avg_wagon_x: Mean wagon X (higher = more leg-side)
#'   - hawkeye_balls: Number of balls with Hawkeye data
#'
#' @export
calculate_hawkeye_batter_features <- function(conn, player_ids = NULL,
                                               min_balls = 30) {
  player_filter <- if (!is.null(player_ids)) {
    placeholders <- paste(rep("?", length(player_ids)), collapse = ", ")
    sprintf("AND batsman_id IN (%s)", placeholders)
  } else {
    ""
  }

  query <- sprintf("
    SELECT
      batsman_id AS player_id,
      COUNT(*) AS hawkeye_balls,

      -- Wagon wheel zone distributions
      AVG(wagon_x) AS avg_wagon_x,
      AVG(wagon_y) AS avg_wagon_y,
      -- Leg side: wagon_x > 0 (right-hand convention)
      AVG(CASE WHEN wagon_x > 0 THEN 1.0 ELSE 0.0 END) AS pct_leg_side,
      -- V zone: straight-ish shots (wagon_zone in front sectors)
      AVG(CASE WHEN wagon_zone IN ('1', '2', '3', 'straight', 'mid-on', 'mid-off', 'cover')
          THEN 1.0 ELSE 0.0 END) AS pct_v_zone,
      -- Boundary distribution
      AVG(CASE WHEN (is_four = 1 OR is_six = 1) AND wagon_x > 0 THEN 1.0
          WHEN (is_four = 1 OR is_six = 1) THEN 0.0
          ELSE NULL END) AS pct_boundary_leg,

      -- Shot type diversity and tendencies
      COUNT(DISTINCT shot_type) AS distinct_shot_types,
      AVG(CASE WHEN shot_type IN ('drive', 'pull', 'sweep', 'hook', 'cut', 'slog', 'reverse sweep', 'scoop')
          THEN 1.0 ELSE 0.0 END) AS pct_attacking,
      AVG(CASE WHEN shot_type IN ('defence', 'leave', 'block', 'forward defence', 'back defence')
          THEN 1.0 ELSE 0.0 END) AS pct_defensive,

      -- Shot control
      AVG(CASE WHEN shot_control = 'controlled' THEN 1.0
          WHEN shot_control IS NOT NULL THEN 0.0
          ELSE NULL END) AS pct_controlled

    FROM cricinfo.balls
    WHERE batsman_id IS NOT NULL
      %s
    GROUP BY batsman_id
    HAVING COUNT(*) >= %d
  ", player_filter, min_balls)

  params <- if (!is.null(player_ids)) as.list(player_ids) else list()
  result <- DBI::dbGetQuery(conn, query, params = params)
  dt <- data.table::as.data.table(result)

  # Normalize shot diversity (0-1 scale, based on max ~15 distinct types)
  if (nrow(dt) > 0) {
    dt[, shot_diversity := pmin(1, distinct_shot_types / 12)]
    dt[, distinct_shot_types := NULL]
  }

  dt
}


#' Calculate Hawkeye Bowler Features
#'
#' Computes advanced bowling features from Cricinfo Hawkeye data:
#' pitch line/length consistency, dot ball pitch maps, and wicket-taking zones.
#'
#' @param conn DBI connection
#' @param player_ids Character vector of bowler IDs (optional, NULL = all)
#' @param min_balls Integer. Minimum balls with Hawkeye data
#'
#' @return data.table with one row per bowler:
#'   - pct_good_length: % of balls on good length
#'   - pct_full: % of full-length balls
#'   - pct_short: % of short balls
#'   - pct_on_stump: % of balls on or around the stumps (line)
#'   - pct_dot_controlled: % of dot balls from controlled lengths
#'   - wicket_pct_good_length: % of wickets from good length balls
#'   - line_consistency: Entropy-based measure of line variety (lower = more consistent)
#'   - hawkeye_balls: Number of balls with Hawkeye data
#'
#' @export
calculate_hawkeye_bowler_features <- function(conn, player_ids = NULL,
                                               min_balls = 60) {
  player_filter <- if (!is.null(player_ids)) {
    placeholders <- paste(rep("?", length(player_ids)), collapse = ", ")
    sprintf("AND bowler_id IN (%s)", placeholders)
  } else {
    ""
  }

  query <- sprintf("
    SELECT
      bowler_id AS player_id,
      COUNT(*) AS hawkeye_balls,

      -- Pitch length distributions
      AVG(CASE WHEN LOWER(pitch_length) IN ('good', 'good length') THEN 1.0 ELSE 0.0 END) AS pct_good_length,
      AVG(CASE WHEN LOWER(pitch_length) IN ('full', 'full toss', 'yorker', 'overpitched') THEN 1.0 ELSE 0.0 END) AS pct_full,
      AVG(CASE WHEN LOWER(pitch_length) IN ('short', 'short of length', 'bouncer') THEN 1.0 ELSE 0.0 END) AS pct_short,

      -- Pitch line distributions
      AVG(CASE WHEN LOWER(pitch_line) IN ('stumps', 'off stump', 'middle', 'leg stump')
          THEN 1.0 ELSE 0.0 END) AS pct_on_stump,
      AVG(CASE WHEN LOWER(pitch_line) IN ('outside off', 'wide outside off')
          THEN 1.0 ELSE 0.0 END) AS pct_outside_off,

      -- Dot ball quality (dots from good areas)
      AVG(CASE WHEN total_runs = 0 AND LOWER(pitch_length) IN ('good', 'good length')
          THEN 1.0
          WHEN total_runs = 0 THEN 0.0
          ELSE NULL END) AS pct_dot_good_length,

      -- Wicket-taking length
      AVG(CASE WHEN is_wicket = 1 AND LOWER(pitch_length) IN ('good', 'good length')
          THEN 1.0
          WHEN is_wicket = 1 THEN 0.0
          ELSE NULL END) AS wicket_pct_good_length,

      -- Control metrics
      AVG(CASE WHEN shot_control = 'controlled' THEN 0.0
          WHEN shot_control IS NOT NULL THEN 1.0
          ELSE NULL END) AS pct_beat_bat

    FROM cricinfo.balls
    WHERE bowler_id IS NOT NULL
      AND pitch_length IS NOT NULL
      %s
    GROUP BY bowler_id
    HAVING COUNT(*) >= %d
  ", player_filter, min_balls)

  params <- if (!is.null(player_ids)) as.list(player_ids) else list()
  result <- DBI::dbGetQuery(conn, query, params = params)
  data.table::as.data.table(result)
}


#' Get Hawkeye Feature Summary
#'
#' Returns coverage statistics for Hawkeye features.
#'
#' @param conn DBI connection
#'
#' @return List with coverage stats by format and field
#' @export
get_hawkeye_coverage <- function(conn) {
  DBI::dbGetQuery(conn, "
    SELECT
      CASE
        WHEN LOWER(title) LIKE '%t20%' OR LOWER(title) LIKE '%twenty20%' THEN 'T20'
        WHEN LOWER(title) LIKE '%odi%' OR LOWER(title) LIKE '%one day%' THEN 'ODI'
        WHEN LOWER(title) LIKE '%test%' THEN 'Test'
        ELSE 'Other'
      END AS format_guess,
      COUNT(*) AS total_balls,
      SUM(CASE WHEN wagon_x IS NOT NULL THEN 1 ELSE 0 END) AS wagon_balls,
      SUM(CASE WHEN pitch_line IS NOT NULL THEN 1 ELSE 0 END) AS pitch_line_balls,
      SUM(CASE WHEN pitch_length IS NOT NULL THEN 1 ELSE 0 END) AS pitch_length_balls,
      SUM(CASE WHEN shot_type IS NOT NULL THEN 1 ELSE 0 END) AS shot_type_balls,
      SUM(CASE WHEN shot_control IS NOT NULL THEN 1 ELSE 0 END) AS shot_control_balls,
      COUNT(DISTINCT match_id) AS matches
    FROM cricinfo.balls
    GROUP BY 1
    ORDER BY total_balls DESC
  ")
}
