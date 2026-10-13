# Pre-Match Model Inputs: One Loader for Training and Live Predictions
#
# The pre-match models are trained on features from calc_match_features()
# (data-raw/models/pre-match/02_calculate_pre_match_features.R). Live fixtures
# used to go through a separate single-match builder,
# calculate_pre_match_features(), which drifted: on 2026-10-13 every live
# fixture had team Elo 1500, skills 0, form NA and h2h 0, and the published
# predictions were near-constant. These two functions let the live job build
# fixture features with the training builder itself.

utils::globalVariables(c("..keep", "team_id", "team_type", "skill_date", "player_id"))


#' Load Pre-Match Model Inputs for One Format
#'
#' Loads every table [calc_match_features()] reads, for one format, from a
#' DuckDB connection. Works on the full database and on the GHA temp database
#' built from the prediction caches. Tables that are missing are reported by
#' message and returned empty, so the affected features fall back to the same
#' neutral defaults as training uses for teams with no history.
#'
#' @param conn DBI connection.
#' @param format Character. "t20", "odi" or "test".
#' @param strict Logical. TRUE aborts when any input is missing or empty
#'   instead of falling back to defaults. The live prediction job uses TRUE:
#'   defaults hidden behind a green run are how its features went flat.
#'
#' @return List with `matches_dt` (format matches with composite team ids),
#'   `team_elo_dt`, `participation_dt`, `venue_stats_dt`, `team_skill_dt`,
#'   `venue_skill_dt`, `batter_skills`, `bowler_skills`, and `missing`
#'   (character vector of inputs that were not available).
#' @keywords internal
load_pre_match_inputs <- function(conn, format, strict = FALSE) {
  format_types <- switch(format,
    t20 = c("T20", "IT20"), odi = c("ODI", "ODM"), test = c("Test", "MDM"),
    cli::cli_abort("Unknown format {.val {format}}"))
  types_sql <- paste(sprintf("'%s'", tolower(format_types)), collapse = ", ")
  tables <- DBI::dbListTables(conn)
  missing <- character(0)

  # Matches (training: 02 script section 4/5)
  matches_dt <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT match_id, match_date, match_type, gender, team_type, event_name, venue,
           team1, team2, outcome_winner, toss_winner, toss_decision,
           event_match_number, event_group
    FROM cricsheet.matches
    WHERE LOWER(match_type) IN (%s) AND gender IS NOT NULL
    ORDER BY match_date, match_id", types_sql)))
  matches_dt[, match_date := as.Date(match_date)]
  n_dup <- sum(duplicated(matches_dt$match_id))
  if (n_dup > 0) {
    # Repeated rows inflate form, h2h and venue stats (the cricsheet release
    # held matches repeated up to 86 times, 2026-10-13)
    if (strict) cli::cli_abort("{n_dup} duplicated match rows in cricsheet.matches ({format})")
    cli::cli_alert_warning("Dropping {n_dup} duplicated match rows in cricsheet.matches ({format})")
    matches_dt <- unique(matches_dt, by = "match_id")
  }
  # season is not read by calc_match_features(); the prediction cache's
  # fallback matches file does not have it.
  matches_dt[, season := NA_character_]
  matches_dt[, `:=`(
    format = format,
    team1_id = make_team_id_vec(team1, gender, format, team_type),
    team2_id = make_team_id_vec(team2, gender, format, team_type),
    outcome_winner_id = ifelse(is.na(outcome_winner) | outcome_winner == "", NA_character_,
                               make_team_id_vec(outcome_winner, gender, format, team_type))
  )]

  # Team Elo: the database stores elo_after; the prediction cache renames it elo_result
  elo_col <- if ("elo_after" %in% DBI::dbListFields(conn, "team_elo")) "elo_after" else "elo_result"
  team_elo_dt <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT team_id, match_id, match_date, %s AS elo_result, %s AS elo_roster_combined
    FROM team_elo", elo_col, elo_col)))
  team_elo_dt[, match_date := as.Date(match_date)]
  team_elo_dt <- team_elo_dt[filter_team_ids_by_format(team_id, format)]
  data.table::setkey(team_elo_dt, team_id, match_date)

  # Player participation (roster inference). The GHA deliveries table is built
  # from recent_lineups and may lack match_type; then it is not filtered.
  # Gender comes with it: men's and women's sides share names ("Australia").
  del_cols <- DBI::dbListFields(conn, DBI::Id(schema = "cricsheet", table = "deliveries"))
  participation_dt <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT DISTINCT d.match_id, d.match_date, d.batting_team, d.bowling_team, d.batter_id, d.bowler_id,
           %s AS gender
    FROM cricsheet.deliveries d %s %s",
    if ("gender" %in% del_cols) "d.gender" else "m.gender",
    if ("gender" %in% del_cols) "" else "LEFT JOIN cricsheet.matches m ON m.match_id = d.match_id",
    if ("match_type" %in% del_cols) sprintf("WHERE LOWER(d.match_type) IN (%s)", types_sql) else "")))
  participation_dt[, match_date := as.Date(match_date)]
  if (!"match_type" %in% del_cols) missing <- c(missing, "deliveries.match_type (rosters not format-filtered)")
  # Per-team row index: get_team_skills_fast() would otherwise scan every row
  # for every match (79% of feature-building time, measured 2026-10-13)
  n_part <- nrow(participation_dt)
  team_rows <- split(c(seq_len(n_part), seq_len(n_part)),
                     c(participation_dt$batting_team, participation_dt$bowling_team))
  data.table::setattr(participation_dt, "team_rows", lapply(team_rows, unique))

  # Venue stats
  venue_stats_dt <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT m.venue, m.match_date, m.outcome_winner, mi.innings, mi.total_runs, mi.batting_team
    FROM (SELECT DISTINCT ON (match_id) * FROM cricsheet.matches ORDER BY match_id) m
    JOIN cricsheet.match_innings mi ON m.match_id = mi.match_id
    WHERE m.outcome_winner IS NOT NULL AND m.outcome_winner != ''
      AND LOWER(m.match_type) IN (%s)", types_sql)))
  venue_stats_dt[, match_date := as.Date(match_date)]

  # Team and venue per-delivery skills: one row per team / venue per match
  # date, so calc_match_features() takes them AS OF each match. (Until
  # 2026-10-13 only the latest row was kept: past matches mostly got defaults
  # while live fixtures got real values.) The prediction cache holds only the
  # latest row, which is the same thing for an upcoming fixture.
  team_skill_table <- paste0(format, "_team_skill")
  has_rows <- function(tbl) tbl %in% tables &&
    DBI::dbGetQuery(conn, sprintf("SELECT COUNT(*) AS n FROM %s", tbl))$n > 0
  if (has_rows(team_skill_table)) {
    team_skill_dt <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
      WITH latest AS (
        SELECT batting_team_id, bowling_team_id, match_date,
               batting_team_runs_skill, batting_team_wicket_skill,
               bowling_team_runs_skill, bowling_team_wicket_skill,
               ROW_NUMBER() OVER (PARTITION BY batting_team_id, match_date ORDER BY delivery_id DESC) AS rn
        FROM %s)
      SELECT * FROM latest WHERE rn = 1", team_skill_table)))
    team_skill_dt[, match_date := as.Date(match_date)]
    # Since 2026-10-13 features look teams up by composite id. A table (or
    # prediction cache) still keyed by name would match nothing and give every
    # team a neutral 0 with no error, so refuse it.
    legacy <- !is_composite_team_id(team_skill_dt$batting_team_id)
    if (any(legacy)) {
      cli::cli_abort(c(
        "{team_skill_table} keys {sum(legacy)} row(s) by team name (e.g. {.val {team_skill_dt$batting_team_id[legacy][1]}}), not composite id.",
        "i" = "Rebuild it with FORCE_FULL in 02_calculate_team_skill_indices.R, then refresh the prediction cache."
      ))
    }
    data.table::setkey(team_skill_dt, batting_team_id, match_date)
  } else {
    team_skill_dt <- data.table::data.table(batting_team_id = character())
    missing <- c(missing, team_skill_table)
  }
  venue_skill_table <- paste0(format, "_venue_skill")
  if (has_rows(venue_skill_table)) {
    venue_skill_dt <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
      WITH latest AS (
        SELECT venue, match_date, venue_run_rate, venue_wicket_rate, venue_boundary_rate, venue_dot_rate,
               ROW_NUMBER() OVER (PARTITION BY venue, match_date ORDER BY delivery_id DESC) AS rn
        FROM %s)
      SELECT * FROM latest WHERE rn = 1", venue_skill_table)))
    venue_skill_dt[, match_date := as.Date(match_date)]
    data.table::setkey(venue_skill_dt, venue, match_date)
  } else {
    venue_skill_dt <- data.table::data.table(venue = character())
    missing <- c(missing, venue_skill_table)
  }

  # Player skills: one row per player per match date (the skill after their
  # last delivery that day), so get_team_skills_fast() can take each player's
  # skill AS OF a match. The prediction cache holds only the latest row per
  # player, which is the same thing for an upcoming fixture.
  skill_table <- paste0(format, "_player_skill")
  if (has_rows(skill_table)) {
    batter_skills <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
      WITH r AS (
        SELECT batter_id, match_date, batter_scoring_index, batter_survival_rate, batter_balls_faced,
               ROW_NUMBER() OVER (PARTITION BY batter_id, match_date ORDER BY delivery_id DESC) AS rn
        FROM %s WHERE batter_id IS NOT NULL)
      SELECT batter_id AS player_id, match_date AS skill_date, batter_scoring_index AS scoring_index,
             batter_survival_rate AS survival_rate, batter_balls_faced AS balls_faced
      FROM r WHERE rn = 1", skill_table)))
    bowler_skills <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
      WITH r AS (
        SELECT bowler_id, match_date, bowler_economy_index, bowler_strike_rate, bowler_balls_bowled,
               ROW_NUMBER() OVER (PARTITION BY bowler_id, match_date ORDER BY delivery_id DESC) AS rn
        FROM %s WHERE bowler_id IS NOT NULL)
      SELECT bowler_id AS player_id, match_date AS skill_date, bowler_economy_index AS economy_index,
             bowler_strike_rate AS strike_rate, bowler_balls_bowled AS balls_bowled
      FROM r WHERE rn = 1", skill_table)))
    batter_skills[, skill_date := as.Date(skill_date)]
    bowler_skills[, skill_date := as.Date(skill_date)]
    data.table::setkey(batter_skills, player_id, skill_date)
    data.table::setkey(bowler_skills, player_id, skill_date)
  } else {
    batter_skills <- data.table::data.table(player_id = character(), scoring_index = numeric())
    bowler_skills <- data.table::data.table(player_id = character(), economy_index = numeric())
    missing <- c(missing, skill_table)
  }

  for (nm in c("matches_dt", "team_elo_dt", "participation_dt", "venue_stats_dt")) {
    if (nrow(get(nm)) == 0) missing <- c(missing, paste0(nm, " (0 rows)"))
  }
  if (length(missing) > 0 && strict) {
    cli::cli_abort(paste0(
      "{toupper(format)} pre-match inputs missing or empty: {paste(missing, collapse = ', ')}. ",
      "Refusing to predict on default features."))
  }
  if (length(missing) > 0) {
    cli::cli_alert_warning(paste0(
      "{toupper(format)} pre-match inputs not available, features fall back to neutral defaults: ",
      "{paste(missing, collapse = ', ')}"))
  }
  cli::cli_alert_info(paste0(
    "{toupper(format)} inputs: {nrow(matches_dt)} matches, {nrow(team_elo_dt)} team Elo rows, ",
    "{nrow(participation_dt)} roster rows, {nrow(batter_skills)} batters, {nrow(bowler_skills)} bowlers"))

  list(matches_dt = matches_dt, team_elo_dt = team_elo_dt, participation_dt = participation_dt,
       venue_stats_dt = venue_stats_dt, team_skill_dt = team_skill_dt,
       venue_skill_dt = venue_skill_dt, batter_skills = batter_skills,
       bowler_skills = bowler_skills, missing = missing)
}


#' Build Pre-Match Features for Upcoming Fixtures
#'
#' Runs each fixture through [calc_match_features()], the builder the
#' pre-match models were trained on, using inputs from
#' [load_pre_match_inputs()].
#'
#' Fixtures often arrive without a team type (Cricinfo's fixture list has
#' none). A team's composite id depends on it, so it is resolved from the
#' team Elo history: "international" when both teams have an international id
#' with history in this format and gender, otherwise "club" when either has a
#' club id, otherwise the fixture's own value or "international". A trailing
#' " Women" is dropped from women's team names to match Cricsheet's naming.
#'
#' @param fixtures data.frame with `match_id`, `match_date`, `team1`, `team2`,
#'   `gender` and optionally `venue`, `match_type`, `team_type`, `event_name`,
#'   `event_match_number`, `event_group`, `toss_winner`, `toss_decision`.
#' @param inputs List from [load_pre_match_inputs()] for the fixtures' format.
#' @param home_lookups List from [build_home_lookups()], or NULL.
#'
#' @return data.frame, one row per fixture, with the columns
#'   [calc_match_features()] returns plus `team_type_resolved`,
#'   `team1_elo_found` / `team2_elo_found` (the team's id has rating history;
#'   FALSE means its features are defaults) and `team1_in_history` /
#'   `team2_in_history` (the team NAME appears in this format's matches).
#'   In history but no rating found means a lookup is broken.
#' @keywords internal
build_fixture_features <- function(fixtures, inputs, home_lookups = NULL) {
  fx <- data.table::as.data.table(fixtures)
  fmt <- inputs$matches_dt$format[1] %||% NA_character_
  if (is.na(fmt)) cli::cli_abort("inputs$matches_dt is empty; cannot tell the format")
  col_or <- function(x, default) if (x %in% names(fx)) fx[[x]] else rep(default, nrow(fx))

  fx[, `:=`(match_id = as.character(match_id), match_date = as.Date(match_date),
            gender = tolower(col_or("gender", NA_character_)))]
  if (anyNA(fx$gender) || anyNA(fx$match_date)) {
    cli::cli_abort("{sum(is.na(fx$gender) | is.na(fx$match_date))} fixture{?s} with no gender or date; team ids cannot be built")
  }
  # Cricinfo names women's sides "Australia Women"; Cricsheet (and so every
  # history table) calls them "Australia" with gender "female".
  fx[gender == "female", `:=`(team1 = sub(" Women$", "", team1), team2 = sub(" Women$", "", team2))]
  known_ids <- unique(inputs$team_elo_dt$team_id)
  known_names <- unique(c(paste(inputs$matches_dt$team1, inputs$matches_dt$gender),
                          paste(inputs$matches_dt$team2, inputs$matches_dt$gender)))
  given_type <- col_or("team_type", NA_character_)
  fx[, team_type := mapply(function(t1, t2, g, given) {
    intl <- make_team_id_vec(c(t1, t2), g, fmt, "international")
    club <- make_team_id_vec(c(t1, t2), g, fmt, "club")
    if (all(intl %in% known_ids)) "international"
    else if (any(club %in% known_ids)) "club"
    else if (!is.na(given)) given
    else "international"
  }, team1, team2, gender, given_type)]
  fx[, `:=`(
    match_type = col_or("match_type", NA_character_), venue = col_or("venue", NA_character_),
    event_name = col_or("event_name", NA_character_),
    event_match_number = col_or("event_match_number", NA_integer_),
    event_group = col_or("event_group", NA_character_),
    toss_winner = col_or("toss_winner", NA_character_),
    toss_decision = col_or("toss_decision", NA_character_),
    season = NA_character_, outcome_winner = NA_character_, outcome_winner_id = NA_character_,
    format = fmt,
    team1_id = make_team_id_vec(team1, gender, fmt, team_type),
    team2_id = make_team_id_vec(team2, gender, fmt, team_type))]

  # calc_match_features() looks rows up by index in one table; history rows
  # (training uses matches with a result) plus the fixtures appended at the end.
  # (Test keeps draws, as training does.)
  hist <- if (fmt == "test") inputs$matches_dt[!is.na(match_date)] else
    inputs$matches_dt[!is.na(outcome_winner) & outcome_winner != ""]
  keep <- intersect(names(hist), names(fx))
  all_rows <- data.table::rbindlist(list(hist[, ..keep], fx[, ..keep]), use.names = TRUE)
  offset <- nrow(hist)

  rows <- lapply(seq_len(nrow(fx)), function(k) {
    out <- calc_match_features(offset + k, all_rows, inputs$team_elo_dt, inputs$batter_skills,
                               inputs$bowler_skills, inputs$participation_dt,
                               inputs$venue_stats_dt, inputs$team_skill_dt,
                               inputs$venue_skill_dt, home_lookups = home_lookups)
    out$team_type_resolved <- fx$team_type[k]
    out$team1_elo_found <- fx$team1_id[k] %in% known_ids
    out$team2_elo_found <- fx$team2_id[k] %in% known_ids
    out$team1_in_history <- paste(fx$team1[k], fx$gender[k]) %in% known_names
    out$team2_in_history <- paste(fx$team2[k], fx$gender[k]) %in% known_names
    out
  })
  data.table::rbindlist(rows, fill = TRUE)
}


#' Predict Fixture Win Probabilities from Pre-Match Features
#'
#' Applies a two-stage pre-match model to rows from [build_fixture_features()].
#' Binary models (T20, ODI) give P(team1 wins). The Test model is 3-class
#' (team1 win, draw, team2 win); xgboost 3.x returns that as an n x 3 matrix,
#' which is used as is (reshaping it again with `byrow = TRUE` scrambles the
#' classes).
#'
#' @param features data.frame from [build_fixture_features()].
#' @param model xgb.Booster win-probability model.
#' @param margin_model xgb.Booster margin model, or NULL (then the model gets
#'   no `predicted_margin` and a warning is given).
#'
#' @return data.frame with `match_id`, `team1_win_prob`, `draw_prob`,
#'   `team2_win_prob` (draw_prob is 0 for binary models).
#' @keywords internal
predict_fixture_probs <- function(features, model, margin_model = NULL) {
  prepared <- prepare_prediction_features(as.data.frame(features))
  if (!is.null(margin_model)) {
    prepared$predicted_margin <- get_margin_prediction(prepared, margin_model)
    cols <- get_prediction_feature_cols_full()
  } else {
    cli::cli_alert_warning("No margin model: predicting without predicted_margin, which the model was trained with")
    cols <- get_prediction_feature_cols()
  }
  X <- as.matrix(prepared[, cols, drop = FALSE])
  X[is.na(X)] <- 0
  raw <- stats::predict(model, xgboost::xgb.DMatrix(X))
  if (is.matrix(raw) || length(raw) == 3L * nrow(X)) {
    P <- if (is.matrix(raw)) raw else matrix(raw, ncol = 3, byrow = TRUE)
    data.frame(match_id = features$match_id, team1_win_prob = P[, 1], draw_prob = P[, 2],
               team2_win_prob = P[, 3])
  } else {
    data.frame(match_id = features$match_id, team1_win_prob = raw, draw_prob = 0,
               team2_win_prob = 1 - raw)
  }
}


#' Check Live Fixture Features Before Publishing
#'
#' Reports, per format, how many fixtures have rating history for both teams,
#' and aborts when teams that DO appear in the match history were not found in
#' the ratings. Teams absent from the history entirely (the Ranji Trophy,
#' under-19 sides) legitimately get default features; a known team defaulting
#' means a lookup broke, which is how the live predictions went flat unnoticed
#' until 2026-10-13.
#'
#' @param features data.frame from [build_fixture_features()], with a `format`
#'   column added.
#' @param max_broken_share Numeric. Largest allowed share, per format, of
#'   in-history teams with no rating found.
#'
#' @return Invisibly, a data.frame with one row per format: `fixtures`,
#'   `both_rated`, `teams_in_history`, `in_history_unrated`.
#' @keywords internal
check_fixture_coverage <- function(features, max_broken_share = 0.1) {
  need <- c("format", "team1_elo_found", "team2_elo_found", "team1_in_history", "team2_in_history")
  stopifnot(nrow(features) > 0, all(need %in% names(features)))
  rows <- lapply(split(features, features$format), function(f) {
    in_hist <- c(f$team1_in_history, f$team2_in_history)
    found <- c(f$team1_elo_found, f$team2_elo_found)
    data.frame(format = f$format[1], fixtures = nrow(f),
               both_rated = sum(f$team1_elo_found & f$team2_elo_found),
               teams_in_history = sum(in_hist), in_history_unrated = sum(in_hist & !found))
  })
  cov <- do.call(rbind, rows)
  for (i in seq_len(nrow(cov))) {
    cli::cli_alert_info(paste0(
      "{toupper(cov$format[i])}: {cov$both_rated[i]}/{cov$fixtures[i]} fixtures rated for both teams; ",
      "{cov$in_history_unrated[i]} of {cov$teams_in_history[i]} known teams unrated"))
  }
  bad <- cov[cov$in_history_unrated > max_broken_share * cov$teams_in_history, , drop = FALSE]
  if (nrow(bad) > 0) {
    cli::cli_abort(paste0(
      "Teams in the match history but with no rating found: ",
      paste(sprintf("%s %d of %d", toupper(bad$format), bad$in_history_unrated, bad$teams_in_history), collapse = ", "),
      ". A team lookup is probably broken; not publishing."))
  }
  invisible(cov)
}
