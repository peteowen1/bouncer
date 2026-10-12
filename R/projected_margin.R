# Projected run margin (D-P72): every decided match is judged on runs.
#
# - Won by runs: the official runs margin. It equals the winner's total minus
#   the loser's total across all innings (innings wins included), and the
#   revised target minus one minus the chaser's score under D/L.
# - Won by wickets: the chaser's score plus the runs it would still have made
#   from where it stopped, minus what it had to beat. "Would still have made"
#   is MEASURED, not modelled: the average runs real innings added from the
#   same balls left and wickets in hand (limited overs), or the average runs
#   the last N wickets added in all-out 4th innings (Tests). The resource
#   formula in calculate_projected_score() undercounted this by 20-25% at every
#   stage of the chase (2026-10-13), which is why it is not used here.
#
# Sign: positive = the side batting in innings 1 won, as the stored column has
# always been. Ties, draws and no results are 0.

.margin_format <- function(match_type) {
  unname(c(T20 = "t20", IT20 = "t20", ODI = "odi", ODM = "odi",
           Test = "test", MDM = "test")[match_type])
}


#' Runs Still To Come, Measured From Real Innings
#'
#' Builds the lookup tables that [compute_projected_margins()] uses to value a
#' chasing side's unused balls and wickets.
#'
#' @param conn DuckDB connection.
#' @return List with `lo` (format, gender, balls_rem, in_hand, n, runs) from
#'   completed first innings of full-length T20/ODI matches, and `test`
#'   (gender, fourth, in_hand, n, runs) from all-out, undeclared Test and
#'   multi-day innings; in_hand 10 is the whole innings from 0.
#' @keywords internal
build_runs_to_come_lookup <- function(conn) {
  lo <- data.table::as.data.table(DBI::dbGetQuery(conn, "
    WITH d AS (
      SELECT d.match_id, d.gender, d.total_runs, d.wickets_fallen, m.overs_per_innings,
             CASE WHEN m.overs_per_innings = 20 THEN 't20' ELSE 'odi' END AS format,
             SUM(CASE WHEN COALESCE(d.wides, 0) = 0 AND COALESCE(d.noballs, 0) = 0 THEN 1 ELSE 0 END)
               OVER (PARTITION BY d.match_id ORDER BY d.over, d.ball, d.delivery_id) AS legal
      FROM cricsheet.deliveries d JOIN cricsheet.matches m USING (match_id)
      WHERE m.match_type IN ('T20', 'IT20', 'ODI', 'ODM') AND m.outcome_method IS NULL
        AND d.innings = 1 AND m.overs_per_innings IN (20, 50)),
    fin AS (SELECT match_id, MAX(total_runs) AS final, MAX(legal) AS legal_n,
                   MAX(wickets_fallen) AS wk FROM d GROUP BY 1)
    SELECT d.format, d.gender, d.overs_per_innings * 6 - d.legal AS balls_rem,
           10 - d.wickets_fallen AS in_hand, COUNT(*) AS n, AVG(f.final - d.total_runs) AS runs
    FROM d JOIN fin f USING (match_id)
    WHERE (f.legal_n >= d.overs_per_innings * 6 OR f.wk = 10)
      AND d.overs_per_innings * 6 - d.legal >= 1 AND d.wickets_fallen < 10
    GROUP BY 1, 2, 3, 4"))
  test <- data.table::as.data.table(DBI::dbGetQuery(conn, "
    WITH d AS (
      SELECT d.match_id, d.gender, d.innings, d.total_runs, d.wickets_fallen, d.is_wicket,
             ROW_NUMBER() OVER (PARTITION BY d.match_id, d.innings ORDER BY d.over, d.ball, d.delivery_id) AS rn
      FROM cricsheet.deliveries d WHERE d.match_type IN ('Test', 'MDM')),
    inn AS (SELECT match_id, innings, total_runs AS final FROM cricsheet.match_innings
            WHERE total_wickets = 10 AND NOT declared AND NOT COALESCE(is_super_over, FALSE)),
    fall AS (SELECT match_id, innings, wickets_fallen AS w, MIN(rn) AS rn
             FROM d WHERE is_wicket AND wickets_fallen BETWEEN 1 AND 9 GROUP BY 1, 2, 3)
    SELECT d.gender, f.innings = 4 AS fourth, 10 - f.w AS in_hand,
           COUNT(*) AS n, AVG(i.final - d.total_runs) AS runs
    FROM fall f JOIN d USING (match_id, innings, rn) JOIN inn i USING (match_id, innings)
    GROUP BY 1, 2, 3
    UNION ALL
    -- 10 in hand (a 10-wicket win): the whole all-out innings from 0
    SELECT m.gender, i.innings = 4, 10, COUNT(*), AVG(i.final)
    FROM inn i JOIN cricsheet.matches m USING (match_id)
    WHERE m.match_type IN ('Test', 'MDM')
    GROUP BY 1, 2"))
  if (!nrow(lo) || !nrow(test)) cli::cli_abort("Runs-to-come lookup is empty: no completed innings found.")
  # integer keys on both sides of the joins below; DuckDB returns doubles for arithmetic
  lo[, `:=`(balls_rem = as.integer(balls_rem), in_hand = as.integer(in_hand), n = as.integer(n))]
  test[, `:=`(in_hand = as.integer(in_hand), n = as.integer(n))]
  list(lo = lo, test = test)
}


# Limited overs: exact cell for the gender if it has min_n innings behind it,
# else both genders pooled, else the nearest balls_rem with min_n (pooled),
# else the nearest balls_rem however thin.
.lo_runs_to_come <- function(lookup, format, gender, balls_rem, in_hand, min_n) {
  lo <- lookup$lo
  pooled <- lo[, .(n = sum(n), runs = sum(runs * n) / sum(n)), by = .(format, balls_rem, in_hand)]
  q <- data.table::data.table(format, gender, balls_rem = as.integer(balls_rem), in_hand = as.integer(in_hand))
  exact <- lo[n >= min_n][q, on = .(format, gender, balls_rem, in_hand), .(runs = x.runs)]
  pool <- pooled[n >= min_n][q, on = .(format, balls_rem, in_hand), .(runs = x.runs)]
  near <- pooled[n >= min_n][q, on = .(format, in_hand, balls_rem), roll = "nearest", .(runs = x.runs)]
  any_n <- pooled[q, on = .(format, in_hand, balls_rem), roll = "nearest", .(runs = x.runs)]
  out <- data.table::fcoalesce(exact$runs, pool$runs, near$runs, any_n$runs)
  src <- ifelse(!is.na(exact$runs), "exact", ifelse(!is.na(pool$runs), "pooled",
                ifelse(!is.na(near$runs), "nearest", "nearest_thin")))
  done <- !is.na(balls_rem) & balls_rem <= 0
  out[done] <- 0
  src[done] <- "no_balls_left"
  out[is.na(balls_rem)] <- NA_real_   # the nearest-roll would otherwise invent a value
  src[is.na(balls_rem)] <- "no_balls_info"
  list(runs = out, source = src)
}

# Tests: 4th-innings value for the gender, else 4th innings pooled, else all innings.
.test_runs_to_come <- function(lookup, gender, in_hand, min_n) {
  t <- lookup$test
  g4 <- t[fourth == TRUE & n >= min_n]
  p4 <- t[fourth == TRUE, .(n = sum(n), runs = sum(runs * n) / sum(n)), by = in_hand][n >= min_n]
  pa <- t[, .(n = sum(n), runs = sum(runs * n) / sum(n)), by = in_hand]
  q <- data.table::data.table(gender, in_hand = as.integer(in_hand))
  data.table::fcoalesce(g4[q, on = .(gender, in_hand), x.runs],
                        p4[q, on = .(in_hand), x.runs],
                        pa[q, on = .(in_hand), x.runs])
}


#' Projected Run Margin for Decided Matches
#'
#' Judges every decided match on runs (D-P72). Runs wins use the official runs
#' margin. Wickets wins add the runs the chasing side would still have made,
#' measured from real innings by [build_runs_to_come_lookup()], counting balls
#' left against the revised overs when rain cut the chase.
#'
#' @param conn DuckDB connection (read-only is fine).
#' @param match_ids Optional character vector; default every match.
#' @param lookup Optional output of [build_runs_to_come_lookup()], to reuse.
#' @param min_examples Minimum innings behind a lookup cell before falling back
#'   to a coarser one.
#' @return data.table with match_id, unified_margin (positive = the side
#'   batting first won) and margin_method saying how it was reached; NA margin
#'   where it could not be computed, with the reason in margin_method.
#' @keywords internal
compute_projected_margins <- function(conn, match_ids = NULL, lookup = NULL, min_examples = 30L) {
  if (is.null(lookup)) lookup <- build_runs_to_come_lookup(conn)
  sel <- ""
  if (!is.null(match_ids)) {
    duckdb::duckdb_register(conn, "pm_ids", data.frame(match_id = unique(as.character(match_ids))))
    on.exit(try(duckdb::duckdb_unregister(conn, "pm_ids"), silent = TRUE), add = TRUE)
    sel <- "AND match_id IN (SELECT match_id FROM pm_ids)"
  }
  x <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    WITH m AS (SELECT * FROM cricsheet.matches WHERE TRUE %s),
    inn AS (SELECT * FROM cricsheet.match_innings WHERE NOT COALESCE(is_super_over, FALSE) %s),
    li AS (SELECT match_id, MAX(innings) AS innings FROM inn GROUP BY 1),
    last AS (SELECT i.match_id, i.innings AS last_innings, i.batting_team AS last_bat,
                    i.total_runs AS last_runs, i.target_runs, i.target_overs
             FROM inn i JOIN li USING (match_id, innings)),
    agg AS (SELECT i.match_id,
                   SUM(CASE WHEN i.batting_team = l.last_bat THEN i.total_runs ELSE 0 END) AS chaser_agg,
                   SUM(CASE WHEN i.batting_team <> l.last_bat THEN i.total_runs ELSE 0 END) AS other_agg
            FROM inn i JOIN last l USING (match_id) GROUP BY 1),
    lb AS (SELECT d.match_id,
                  SUM(CASE WHEN COALESCE(d.wides, 0) = 0 AND COALESCE(d.noballs, 0) = 0 THEN 1 ELSE 0 END) AS legal
           FROM cricsheet.deliveries d JOIN last l ON d.match_id = l.match_id AND d.innings = l.last_innings
           GROUP BY 1)
    SELECT m.match_id, m.match_type, m.gender, m.overs_per_innings, m.outcome_type, m.outcome_winner,
           m.outcome_by_runs, m.outcome_by_wickets,
           f.batting_team AS first_bat, l.last_bat, l.last_runs, l.target_runs, l.target_overs,
           a.chaser_agg, a.other_agg, lb.legal
    FROM m
    LEFT JOIN inn f ON f.match_id = m.match_id AND f.innings = 1
    LEFT JOIN last l ON l.match_id = m.match_id
    LEFT JOIN agg a ON a.match_id = m.match_id
    LEFT JOIN lb ON lb.match_id = m.match_id", sel, sel)))

  x[, format := .margin_format(match_type)]
  x[, `:=`(unified_margin = NA_real_, margin_method = NA_character_)]
  runs_win <- !is.na(x$outcome_by_runs) & x$outcome_by_runs > 0
  wkts_win <- !runs_win & !is.na(x$outcome_by_wickets) & x$outcome_by_wickets > 0
  no_dec <- !runs_win & !wkts_win & x$outcome_type %in% c("tie", "draw", "no result", "no_result")
  x[no_dec, `:=`(unified_margin = 0, margin_method = "no_decision")]
  x[runs_win, `:=`(unified_margin = as.numeric(outcome_by_runs), margin_method = "runs_official")]

  w <- which(wkts_win & !is.na(x$format) & !is.na(x$last_bat) & !is.na(x$first_bat))
  if (length(w)) {
    ok <- !is.na(x$outcome_winner[w]) & x$outcome_winner[w] == x$last_bat[w] & !is.na(x$chaser_agg[w])
    x[w[!ok], margin_method := "winner_not_last_to_bat"]
    w <- w[ok]
    test <- w[x$format[w] == "test"]
    lo <- w[x$format[w] != "test"]
    if (length(test)) {
      to_come <- .test_runs_to_come(lookup, x$gender[test], as.integer(x$outcome_by_wickets[test]), min_examples)
      x[test, `:=`(unified_margin = chaser_agg - other_agg + to_come, margin_method = "chase_test_wickets")]
    }
    lo <- lo[!is.na(x$legal[lo])]
    if (length(lo)) {
      max_b <- ifelse(!is.na(x$target_overs[lo]), x$target_overs[lo], x$overs_per_innings[lo]) * 6
      par <- ifelse(!is.na(x$target_runs[lo]), x$target_runs[lo] - 1, x$other_agg[lo])
      tc <- .lo_runs_to_come(lookup, x$format[lo], x$gender[lo], as.integer(max_b - x$legal[lo]),
                             as.integer(x$outcome_by_wickets[lo]), min_examples)
      x[lo, `:=`(unified_margin = last_runs - par + tc$runs, margin_method = paste0("chase_lo_", tc$source))]
    }
  }
  x[!is.na(margin_method) & margin_method != "no_decision" & is.na(unified_margin),
    margin_method := paste0(margin_method, "_no_value")]
  x[wkts_win & is.na(margin_method), margin_method := "no_deliveries"]
  x[is.na(margin_method), margin_method := "no_result_recorded"]
  # Sign: positive when the side batting first won.
  won_first <- !is.na(x$outcome_winner) & !is.na(x$first_bat) & x$outcome_winner == x$first_bat
  x[!is.na(unified_margin) & unified_margin != 0 & !won_first, unified_margin := -abs(unified_margin)]
  x[!is.na(unified_margin) & won_first, unified_margin := abs(unified_margin)]
  x[, .(match_id, unified_margin, margin_method)]
}

utils::globalVariables(c(
  "balls_rem", "in_hand", "runs", "fourth", "x.runs", "margin_method",
  "chaser_agg", "other_agg", "last_runs", "outcome_by_runs"
))
