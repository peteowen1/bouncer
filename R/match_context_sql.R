# Shared SQL fragments for the ball-outcome context features.
#
# The knockout / event-tier definitions and the pre-ball innings score CTEs
# were pasted into every query that builds agnostic-model inputs, training and
# serving alike, and the pre-ball batting_score leak fix had to be applied to
# each copy by hand. These builders are the single definition; callers splice
# the result in through a sprintf() "%s" slot, so the LIKE patterns below use
# a single "%".


#' SQL CASE expressions for is_knockout and event_tier
#'
#' @param alias Character. Alias of the cricsheet.matches table in the query.
#' @return Character. Two comma-separated CASE expressions, aliased
#'   `is_knockout` and `event_tier`.
#' @noRd
.match_context_cases_sql <- function(alias = "m") {
  sprintf("CASE
          WHEN LOWER(CAST(%1$s.event_match_number AS VARCHAR)) LIKE '%%final%%' THEN 1
          WHEN LOWER(CAST(%1$s.event_match_number AS VARCHAR)) LIKE '%%qualifier%%' THEN 1
          WHEN LOWER(CAST(%1$s.event_match_number AS VARCHAR)) LIKE '%%eliminator%%' THEN 1
          WHEN LOWER(CAST(%1$s.event_match_number AS VARCHAR)) LIKE '%%playoff%%' THEN 1
          WHEN LOWER(CAST(%1$s.event_match_number AS VARCHAR)) LIKE '%%semi%%' THEN 1
          ELSE 0
        END AS is_knockout,
        CASE
          WHEN LOWER(%1$s.event_name) LIKE '%%world cup%%' THEN 1
          WHEN LOWER(%1$s.event_name) LIKE '%%ipl%%' OR LOWER(%1$s.event_name) LIKE '%%indian premier%%' THEN 1
          WHEN LOWER(%1$s.event_name) LIKE '%%big bash%%' OR LOWER(%1$s.event_name) LIKE '%%bbl%%' THEN 2
          WHEN LOWER(%1$s.event_name) LIKE '%%psl%%' OR LOWER(%1$s.event_name) LIKE '%%super league%%' THEN 2
          WHEN LOWER(%1$s.event_name) LIKE '%%cpl%%' OR LOWER(%1$s.event_name) LIKE '%%caribbean%%' THEN 2
          WHEN LOWER(%1$s.match_type) IN ('test', 'odi', 't20i', 'it20') THEN 1
          ELSE 3
        END AS event_tier", alias)
}


#' SQL CTEs for pre-ball batting and bowling scores
#'
#' Returns the `innings_totals` and `cumulative_scores` CTE definitions
#' (without the leading WITH), for use as `WITH %s, ...`.
#'
#' @param totals_filter Character. WHERE condition on the bare
#'   cricsheet.deliveries table, for innings_totals.
#' @param deliveries_filter Character. WHERE condition on cricsheet.deliveries
#'   aliased `d`, for cumulative_scores.
#' @return Character. Two comma-separated CTE definitions.
#' @noRd
.innings_score_ctes_sql <- function(totals_filter, deliveries_filter) {
  paste0("innings_totals AS (
      SELECT
        match_id,
        innings,
        batting_team,
        MAX(total_runs) AS innings_total
      FROM cricsheet.deliveries
      WHERE ", totals_filter, "
      GROUP BY match_id, innings, batting_team
    ),
    cumulative_scores AS (
      SELECT
        d.*,
        -- FIX: total_runs is the innings score AFTER this delivery (the parser writes
        -- the running total post-ball). Subtract the ball's own runs to get the score
        -- BEFORE it, or runs_difference leaks the target it is used to predict.
        (d.total_runs - (d.runs_batter + d.runs_extras)) AS batting_score,
        COALESCE(
          (SELECT SUM(it.innings_total)
           FROM innings_totals it
           WHERE it.match_id = d.match_id
             AND it.batting_team = d.bowling_team
             AND it.innings < d.innings),
          0
        ) AS bowling_score
      FROM cricsheet.deliveries d
      WHERE ", deliveries_filter, "
    )")
}
