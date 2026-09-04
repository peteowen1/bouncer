# Leverage and Leverage-Weighted WPA
#
# Prompted by comparing bouncer's rating stack against Himanish Ganjoo's T20
# Metrics primer (hganjoo.github.io/t20basics), which defines leverage as "the
# capacity of a delivery to change win probability". We already have per-ball
# WPA (win_probability_added.R / build_cricinfo_win_probability()) but nothing
# weights it by how much a delivery COULD have swung the match -- so a
# boundary in a dead rubber and a boundary in a last-over thriller currently
# count the same in every clutch/timeliness read.
#
# Two candidate formulas were sized empirically before building
# (debug/leverage_formula_comparison.R, gitignored) against 24,000 real T20
# deliveries: the article's six-vs-wicket spread (|WP(six) - WP(wicket)|) vs
# the full multinomial spread (variance of WP across the outcome distribution,
# weighted by each outcome's own predicted probability). Rank correlation
# between them is high (Spearman 0.944) but they disagree on 32% of the
# top-1% "most leveraged" balls -- exactly the tier a clutch leaderboard is
# built from -- because six-vs-wicket overstates dramatic-but-implausible
# states and misses realistic-but-balanced ones. Full multinomial wins on
# signal at negligible extra cost (1.8x of a sub-millisecond operation), so
# that is what this file implements. Not a replacement for WPA: this is
# deliberately a separate, joinable output, not wired into calculate_impact()
# or player_game_data.R's live composite -- MODELLING-IDEAS.md's own caution
# on this idea, kept.
#
# KNOWN SIMPLIFICATION, carried over from the sizing exercise: the
# counterfactual states hold momentum features constant across outcomes
# (isolates the score/wicket effect on WP; doesn't model how six different
# outcomes would themselves alter the rolling-window momentum features). This
# is the same shape as accepting a bounded, documented divergence rather than
# a silent one -- revisit if a real momentum-sensitivity gap shows up in the
# calibration check below.


#' Outcome deltas for the full-multinomial leverage spread
#'
#' `(runs, wickets)` pairs for each state a delivery could produce, in the
#' same order as `OUTCOME_CATEGORIES`'s scoreable subset (wide excluded -- it
#' does not advance the ball, so a delivery can't turn "into" a wide the way
#' it can turn into a wicket or a six).
#'
#' @keywords internal
.LEVERAGE_OUTCOMES <- list(
  wicket = c(0L, 1L), r0 = c(0L, 0L), r1 = c(1L, 0L), r2 = c(2L, 0L),
  r3 = c(3L, 0L), r4 = c(4L, 0L), r6 = c(6L, 0L)
)


#' Leverage from an outcome-probability matrix and a WP-per-outcome matrix
#'
#' `leverage(state) = Var[WP(state + outcome)]` over outcome ~ P(outcome |
#' state) -- the probability-weighted variance of win probability across the
#' outcome distribution. Pure function, no model calls, so it is directly
#' unit-testable against small synthetic matrices.
#'
#' @param p_mat Numeric matrix, one row per ball, one column per outcome.
#'   Rows are renormalized to sum to 1 (safe to pass un-normalized, e.g. after
#'   dropping the "wide" column from a full outcome-model output).
#' @param wp_mat Numeric matrix, same dimensions as `p_mat`: WP(state +
#'   outcome) for the matching outcome column.
#'
#' @return Numeric vector, one leverage value per row.
#' @keywords internal
.leverage_from_probs <- function(p_mat, wp_mat) {
  if (!identical(dim(p_mat), dim(wp_mat))) {
    cli::cli_abort("{.arg p_mat} and {.arg wp_mat} must have the same dimensions.")
  }
  p_mat <- p_mat / rowSums(p_mat)
  wp_mean <- rowSums(p_mat * wp_mat)
  rowSums(p_mat * (wp_mat - wp_mean)^2)
}


#' Batting-team sign for a per-ball win-probability delta
#'
#' +1 when the striker's team is the side that batted first, -1 otherwise --
#' the same construction as `.wp_source_sql()`'s `team_sign` CASE expression
#' in `player_game_data.R`. Kept as a separate R implementation (that one
#' lives in SQL) rather than a shared call, so if the two ever need to be
#' reconciled, this docstring is the pointer.
#'
#' @param striker_team_id,team1_id Integer/character vectors. `team1_id` is
#'   the team that batted in innings 1; NA in either falls back to
#'   `innings_number`'s parity.
#' @param innings_number Integer vector, used only where a team id is missing.
#'
#' @return Numeric vector of 1/-1, same length as the inputs.
#' @keywords internal
.wpa_team_sign <- function(striker_team_id, team1_id, innings_number) {
  data.table::fifelse(
    !is.na(striker_team_id) & !is.na(team1_id),
    data.table::fifelse(striker_team_id == team1_id, 1, -1),
    data.table::fifelse(innings_number %in% c(1L, 3L), 1, -1)
  )
}


#' Build Leverage for Every Cricinfo Delivery
#'
#' Scores every T20/ODI delivery in `cricinfo.balls` with leverage(state) =
#' Var[WP(state + outcome)], weighted by P(outcome | state) from the agnostic
#' ball-outcome model, over the seven scoreable outcomes in
#' [.LEVERAGE_OUTCOMES]. Writes `main.bouncer_leverage_from_cricinfo`, joinable to
#' `main.bouncer_wp_from_cricinfo` on `id`.
#'
#' Reuses [build_cricinfo_win_probability()]'s pre-delivery state construction
#' (momentum, venue stats, target derivation, gap handling) via
#' `return_pre_states = TRUE` rather than a second, drifting copy of it.
#'
#' @section Inherits the agnostic model's unstamped-feature gap (#79):
#' [predict_agnostic_outcome()]'s column-alignment check can only genuinely
#' verify order when the model carries a stamped feature-name attribute, and
#' no agnostic model currently does -- so a real drift between this
#' function's `feat` frame and what the model trained on would degrade to a
#' once-per-session warning, not an abort, and P(outcome | state) -- and
#' therefore leverage -- would be silently wrong rather than missing. Not
#' introduced by this function; #79 tracks stamping the agnostic models,
#' which would close this for every caller including this one.
#'
#' @param format Character. "t20" or "odi". Test is not supported -- see
#'   [build_cricinfo_test_win_probability()]'s own decomposed pipeline; a
#'   single win-probability delta doesn't apply the same way across four
#'   innings and three outcomes, and leverage over that surface is a
#'   separate, harder question left for a later pass.
#' @param conn DBI connection. If NULL, opens one (write access when
#'   `write = TRUE`) and closes it on exit.
#' @param models_path Character. Directory holding the in-match models; NULL
#'   resolves via [load_in_match_models()].
#' @param write Logical. Write the table, or just return the scored frame.
#' @param table_name Character. Target table in the `main` schema.
#'
#' @return data.table with `id`, `match_id`, `leverage`, `p_wicket`, `p_six`
#'   (the two most-referenced individual outcome probabilities, kept for
#'   diagnostics). Invisibly when `write = TRUE`.
#'
#' @export
build_ball_leverage <- function(format = c("t20", "odi"),
                                conn = NULL,
                                models_path = NULL,
                                write = TRUE,
                                table_name = "bouncer_leverage_from_cricinfo") {

  format <- match.arg(format)
  db_format <- toupper(format)

  own_conn <- is.null(conn)
  if (own_conn) {
    conn <- get_db_connection(read_only = !write)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }

  cli::cli_alert_info("Building pre-delivery states via build_cricinfo_win_probability()...")
  wp <- build_cricinfo_win_probability(format = format, conn = conn,
                                       models_path = models_path,
                                       write = FALSE, return_pre_states = TRUE)
  pre_states <- attr(wp, "pre_states")
  models <- attr(wp, "models")
  scoreable <- attr(wp, "scoreable")

  if (is.null(pre_states) || nrow(pre_states) == 0) {
    cli::cli_abort("No scoreable pre-delivery states returned for {.val {format}}.")
  }

  agnostic_model <- load_agnostic_model(format)

  # Agnostic-model feature frame, derived from the same pre-delivery state WP
  # was scored from. league_avg_runs/league_avg_wicket/is_free_hit are left
  # absent -- prepare_agnostic_features() DOES default those three for any
  # caller (agnostic_model.R:815-824), the same accepted gap
  # build_cricinfo_win_probability() itself carries for rain features.
  #
  # is_knockout/event_tier are NOT defaulted by prepare_agnostic_features()
  # (confirmed by review: omitting them errors, "object 'is_knockout' not
  # found" -- it references them as bare symbols inside a dplyr::mutate()).
  # cricinfo.balls/cricinfo.matches carry no per-match knockout/tier
  # classification the way cricsheet's event_tiers.R does, so real values
  # aren't available here without building that mapping for cricinfo event
  # names too -- out of scope for this feature. Supplied as explicit,
  # documented placeholders instead of omitted-and-crashing: is_knockout = 0
  # (the common case), event_tier = 3, matching event_tiers.R's own
  # "default to Tier 3 (conservative)" convention for an unclassifiable
  # competition.
  bowling_score <- ifelse(pre_states$innings == 2L, pre_states$target - 1L, 0L)
  balls_pre <- overs_to_balls(pre_states$overs)
  feat <- data.frame(
    match_type = db_format,
    innings = pre_states$innings,
    over = balls_pre %/% 6L,
    ball = balls_pre %% 6L + 1L,
    wickets_fallen = pre_states$wickets,
    runs_difference = as.numeric(pre_states$current_score - bowling_score),
    gender = ifelse(pre_states$gender_male == 1, "male", "female"),
    is_knockout = 0L,
    event_tier = 3L
  )

  cli::cli_alert_info("Scoring P(outcome | state) with the agnostic model...")
  probs <- predict_agnostic_outcome(agnostic_model, feat, format)
  # OUTCOME_CATEGORIES <- c("wicket","0","1","2","3","4","6","wide") -- positional
  # (R/constants.R). Wide excluded from the leverage spread (see
  # .LEVERAGE_OUTCOMES); its mass is dropped and the remaining seven renormalized.
  p_mat <- probs[, 1:7, drop = FALSE]
  p_mat <- p_mat / rowSums(p_mat)
  colnames(p_mat) <- names(.LEVERAGE_OUTCOMES)

  cli::cli_alert_info("Scoring WP at each of the {length(.LEVERAGE_OUTCOMES)} counterfactual outcomes...")
  wp_mat <- matrix(NA_real_, nrow = nrow(pre_states), ncol = length(.LEVERAGE_OUTCOMES),
                   dimnames = list(NULL, names(.LEVERAGE_OUTCOMES)))
  for (nm in names(.LEVERAGE_OUTCOMES)) {
    d <- .LEVERAGE_OUTCOMES[[nm]]
    after <- pre_states
    balls_after <- balls_pre + 1L
    after$current_score <- pre_states$current_score + d[1]
    after$wickets <- pmin(pre_states$wickets + d[2], 10)
    after$overs <- balls_after %/% 6L + (balls_after %% 6L) / 10
    wp_mat[, nm] <- predict_win_probability_batch(after, format = format, models = models)
  }

  leverage <- .leverage_from_probs(p_mat, wp_mat)

  out <- data.table::data.table(
    id = wp$id[scoreable],
    match_id = wp$match_id[scoreable],
    leverage = leverage,
    p_wicket = p_mat[, "wicket"],
    p_six = p_mat[, "r6"]
  )

  if (!write) return(out[])

  store_ball_leverage(conn, out, format = format, table_name = table_name)
  invisible(out[])
}


# One source of truth for the table shape (player_rating_v2_storage.R's
# pattern) -- the column list and the CREATE TABLE body used to be written out
# separately elsewhere in this package, which is how they drifted and how
# bouncerverse#45 happened (a shape mismatch answered by dropping the WHOLE
# table, destroying every other format's rows, not just the one being
# replaced). Not repeating that here.
.leverage_schema <- c(
  id = "VARCHAR", match_id = "VARCHAR", format = "VARCHAR",
  leverage = "DOUBLE", p_wicket = "DOUBLE", p_six = "DOUBLE"
)


#' Store Ball Leverage
#'
#' Replaces this format's rows in `main.<table_name>`, creating the table on
#' first use and migrating (never dropping) a stale shape -- see
#' [.migrate_schema()]'s docstring for why. DELETE and INSERT share one
#' transaction ([.in_transaction()]), so a failed insert cannot leave a format
#' permanently empty.
#'
#' @param conn DBI connection with write access.
#' @param data data.table as returned by [build_ball_leverage()].
#' @param format Character. Format whose rows are being replaced.
#' @param table_name Character. Target table in the `main` schema.
#'
#' @return Number of rows inserted, invisibly.
#'
#' @keywords internal
store_ball_leverage <- function(conn, data, format,
                                table_name = "bouncer_leverage_from_cricinfo") {

  db_format <- toupper(format)
  wanted <- names(.leverage_schema)

  data <- as.data.frame(data)
  data$format <- db_format
  extra <- setdiff(names(data), wanted)
  if (length(extra)) {
    cli::cli_abort(c(
      "{.arg data} carries {length(extra)} column{?s} the table has no home for: {.field {extra}}.",
      "i" = "Add them to {.code .leverage_schema} deliberately, so the migration can create them."
    ))
  }
  data <- data[, wanted]

  duckdb::duckdb_register(conn, "cbl_staging", data)
  on.exit(duckdb::duckdb_unregister(conn, "cbl_staging"), add = TRUE)
  col_list <- paste(wanted, collapse = ", ")

  n <- .in_transaction(conn, function() {
    DBI::dbExecute(conn, sprintf(
      "CREATE TABLE IF NOT EXISTS main.%s (\n%s\n    )",
      table_name, .schema_ddl(.leverage_schema)))
    .migrate_schema(conn, table_name, .leverage_schema)
    DBI::dbExecute(conn, sprintf("DELETE FROM main.%s WHERE format = '%s'",
                                 table_name, db_format))
    DBI::dbExecute(conn, sprintf(
      "INSERT INTO main.%s (%s) SELECT %s FROM cbl_staging",
      table_name, col_list, col_list))
  })

  cli::cli_alert_success("Stored {n} {db_format} rows in {.field main.{table_name}}.")
  invisible(n)
}


#' Calculate Leverage-Weighted WPA per Player-Match
#'
#' Joins `main.bouncer_wp_from_cricinfo` and `main.bouncer_leverage_from_cricinfo`
#' to `cricinfo.balls`, computes the batting-team-signed per-ball WPA (same
#' team-sign construction as `.wp_source_sql()` in `player_game_data.R` --
#' replicated here rather than shared because one lives in SQL and this in R;
#' see that function if the two ever need to be reconciled), multiplies by
#' leverage, credits batter/bowler via the existing [assign_delivery_credit()]
#' (unchanged from plain WPA's credit logic -- wides/no-balls to the bowler
#' only, byes/leg-byes neutral), and aggregates to one row per player per
#' match per role.
#'
#' This is a standalone diagnostic, deliberately NOT wired into
#' `calculate_impact()` or `player_game_data.R`'s live rating composite --
#' MODELLING-IDEAS.md's "plug in as a variant, not a replacement".
#'
#' @param format Character. "t20" or "odi".
#' @param conn DBI connection. If NULL, opens a read-only one and closes it on
#'   exit.
#'
#' @return data.table with `match_id`, `player_id`, `role`, `deliveries`,
#'   `total_wpa`, `total_leverage_weighted_wpa`, `mean_leverage`.
#'
#' @export
calculate_leverage_weighted_wpa <- function(format = c("t20", "odi"), conn = NULL) {

  format <- match.arg(format)
  db_format <- toupper(format)

  own_conn <- is.null(conn)
  if (own_conn) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }

  df <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT b.id, b.match_id, b.innings_number,
           b.batsman_player_id AS batter_id,
           b.bowler_player_id  AS bowler_id,
           b.wides, b.noballs, b.byes, b.legbyes,
           w.delta_wp, lv.leverage,
           ti.team_id AS striker_team_id, t1.team_id AS team1_id
    FROM cricinfo.balls b
    JOIN cricinfo.matches m ON m.match_id = b.match_id
    JOIN main.bouncer_wp_from_cricinfo w ON w.id = b.id
    JOIN main.bouncer_leverage_from_cricinfo lv ON lv.id = b.id
    LEFT JOIN (
      SELECT match_id, innings_number, MAX(team_id) AS team_id
      FROM cricinfo.innings GROUP BY match_id, innings_number
    ) ti ON ti.match_id = b.match_id AND ti.innings_number = b.innings_number
    LEFT JOIN (
      SELECT match_id, MAX(team_id) AS team_id
      FROM cricinfo.innings WHERE innings_number = 1 GROUP BY match_id
    ) t1 ON t1.match_id = b.match_id
    WHERE m.format = '%s' AND w.delta_wp IS NOT NULL AND lv.leverage IS NOT NULL
  ", db_format)))

  if (nrow(df) == 0) {
    cli::cli_abort(c(
      "No joined rows for {.val {format}}.",
      "i" = "Run build_cricinfo_win_probability() and build_ball_leverage() first."
    ))
  }

  # Coverage EVERY run, same pattern as calculate_impact()'s ok_pct check
  # (player_career_ratings.R): the inner joins above plus the delta_wp/leverage
  # IS NOT NULL filter silently drop any ball missing WP or leverage coverage
  # -- a partial/interrupted build_ball_leverage() run, or a data vintage
  # bouncer_wp_from_cricinfo hasn't been rebuilt for, would otherwise produce
  # a leaderboard that looks complete while covering an arbitrary, unreported
  # subset of deliveries.
  total_deliveries <- DBI::dbGetQuery(conn, sprintf("
    SELECT COUNT(*) AS n FROM cricinfo.balls b
    JOIN cricinfo.matches m ON m.match_id = b.match_id
    WHERE m.format = '%s'
  ", db_format))$n
  coverage_pct <- 100 * nrow(df) / max(1L, total_deliveries)
  if (coverage_pct < 99) {
    lvl <- if (coverage_pct < 50) cli::cli_warn else cli::cli_alert_info
    lvl(c(
      "Leverage-weighted WPA: {round(coverage_pct, 1)}% of {total_deliveries} {toupper(format)} deliveries have both WP and leverage ({nrow(df)} joined rows).",
      "i" = "Rebuild build_cricinfo_win_probability() / build_ball_leverage() if this is unexpected."
    ))
  }

  df[, team_sign := .wpa_team_sign(striker_team_id, team1_id, innings_number)]
  df[, wpa := team_sign * delta_wp]
  df[, leverage_weighted_wpa := wpa * leverage]

  credit_wpa <- assign_delivery_credit(df, df$wpa, "wpa")
  credit_lwpa <- assign_delivery_credit(df, df$leverage_weighted_wpa, "lwpa")
  df[, batter_wpa := credit_wpa$batter_wpa]
  df[, bowler_wpa := credit_wpa$bowler_wpa]
  df[, batter_lwpa := credit_lwpa$batter_lwpa]
  df[, bowler_lwpa := credit_lwpa$bowler_lwpa]

  batting <- df[, .(
    deliveries = .N,
    total_wpa = sum(batter_wpa, na.rm = TRUE),
    total_leverage_weighted_wpa = sum(batter_lwpa, na.rm = TRUE),
    mean_leverage = mean(leverage, na.rm = TRUE)
  ), by = .(match_id, player_id = batter_id)][, role := "batting"]

  bowling <- df[, .(
    deliveries = .N,
    total_wpa = sum(bowler_wpa, na.rm = TRUE),
    total_leverage_weighted_wpa = sum(bowler_lwpa, na.rm = TRUE),
    mean_leverage = mean(leverage, na.rm = TRUE)
  ), by = .(match_id, player_id = bowler_id)][, role := "bowling"]

  result <- data.table::rbindlist(list(batting, bowling))
  result[, .(match_id, player_id, role, deliveries, total_wpa,
            total_leverage_weighted_wpa, mean_leverage)]
}
