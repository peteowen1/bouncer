# Building the full model's training frame.
#
# EXTRACTED 2026-08-20 from data-raw/models/ball-outcome/02_train_full_model.R,
# where it was 218 lines inline inside the per-format loop (bouncerverse#65).
#
# It had to come out because nothing else could reproduce it. The trainer
# compared its fresh logloss against a number read out of a STORED
# agnostic_model_results.rds -- a different run, a different split, an older
# corpus, differing by 186,074 rows in T20 -- and reported a 2.7% gain against
# an established ceiling of ~0.4%. An honest comparison needs both models
# scored on ONE common held-out set, and that needs this frame available to
# something other than the trainer.
#
# Copying the query into a checking script instead would have been the same
# two-declarations-of-one-truth defect that produced #63, where a table name
# was rebuilt at each call site and drifted.

#' Build the Full Outcome Model's Training Frame
#'
#' Context features from cricsheet, then player/team/venue skills, then the
#' 3-way ELO features. This is the frame the full model trains on and the
#' frame any comparison against it must use.
#'
#' @param conn A DBI connection.
#' @param format Character. `"t20"`, `"odi"` or `"test"`.
#' @param match_limit Integer or NULL. Cap the number of matches, for testing.
#' @param include_elo Logical. Join the 3-way ELO features. When FALSE the
#'   three ELO columns are present and zeroed, matching the trainer's
#'   INCLUDE_ELO_FEATURES = FALSE path.
#'
#' @return A data frame of one row per delivery.
#
# NOTE ON dplyr:: QUALIFICATION. This block ran inside a script that had done
# library(dplyr). Inside the package namespace it has not, so bare left_join()
# and coalesce() are not found -- and the tryCatch around the ELO join turned
# that into "ELO features unavailable" and ZEROED the three ELO columns. That
# is the identical silent zero-fill this whole ticket exists to remove, so the
# extraction reintroduced it for exactly one test run. Every dplyr verb here is
# qualified.
#' @keywords internal
build_full_model_frame <- function(conn, format, match_limit = NULL,
                                   include_elo = TRUE) {

  format <- match.arg(tolower(format), c("t20", "odi", "test"))
  MATCH_LIMIT <- match_limit
  INCLUDE_ELO_FEATURES <- include_elo

  # Format filter and over cap, previously set by the trainer's loop.
  if (format == "t20") {
    format_filter <- "LOWER(match_type) IN ('t20', 'it20')"
    max_overs <- 20
  } else if (format == "odi") {
    format_filter <- "LOWER(match_type) IN ('odi', 'odm')"
    max_overs <- 50
  } else {
    format_filter <- "LOWER(match_type) IN ('test', 'mdm')"
    max_overs <- NULL
  }

  # Build SQL query with context features
  query <- sprintf("
    WITH innings_totals AS (
      SELECT
        match_id,
        innings,
        batting_team,
        MAX(total_runs) AS innings_total
      FROM cricsheet.deliveries
      WHERE %s
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
      WHERE %s
    ),
    match_context AS (
      SELECT DISTINCT
        m.match_id,
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
    )
    SELECT
      cs.delivery_id,
      cs.match_id,
      cs.match_type,
      cs.innings,
      cs.over,
      cs.ball,
      cs.over_ball,
      cs.venue,
      cs.gender,
      cs.batter_id,
      cs.bowler_id,
      cs.batting_team,
      cs.bowling_team,
      cs.runs_batter,
      cs.is_wicket,
      -- FIX: wickets_fallen in Cricsheet is AFTER the delivery, so subtract is_wicket
      -- to get the count BEFORE this delivery (prevents data leakage)
      (cs.wickets_fallen - CAST(cs.is_wicket AS INT)) AS wickets_fallen,
      (cs.batting_score - cs.bowling_score) AS runs_difference,
      COALESCE(mc.is_knockout, 0) AS is_knockout,
      COALESCE(mc.event_tier, 3) AS event_tier
    FROM cumulative_scores cs
    LEFT JOIN match_context mc ON cs.match_id = mc.match_id
    WHERE cs.runs_batter NOT IN (5)
      AND cs.runs_batter <= 6
    %s
  ", format_filter, format_filter,
     if (!is.null(MATCH_LIMIT)) sprintf("LIMIT %d", MATCH_LIMIT * 1000) else "")

  # Execute query
  cli::cli_h3("Loading data")
  cli::cli_alert_info("Executing query...")
  model_data <- DBI::dbGetQuery(conn, query)

  if (nrow(model_data) == 0) {
    cli::cli_alert_warning("No data found for {format} format, skipping")
    next
  }

  cli::cli_alert_success("Loaded {.val {nrow(model_data)}} deliveries")

  # Join Skill Indices ----
  cli::cli_h3("Joining skill indices")

  # Player skills
  cli::cli_alert_info("Adding player skills...")
  model_data <- add_skill_features(model_data, format = format, conn = conn, fill_missing = TRUE)
  n_player <- sum(!is.na(model_data$batter_scoring_index))
  cli::cli_alert_success("{n_player}/{nrow(model_data)} have player skills")

  # Team skills
  cli::cli_alert_info("Adding team skills...")
  tryCatch({
    model_data <- join_team_skill_indices(model_data, format = format, conn = conn)
    # Fill missing with 0 (neutral for residual-based)
    model_data <- model_data %>%
      dplyr::mutate(
        batting_team_runs_skill = dplyr::coalesce(batting_team_runs_skill, 0),
        batting_team_wicket_skill = dplyr::coalesce(batting_team_wicket_skill, 0),
        bowling_team_runs_skill = dplyr::coalesce(bowling_team_runs_skill, 0),
        bowling_team_wicket_skill = dplyr::coalesce(bowling_team_wicket_skill, 0)
      )
    n_team <- sum(!is.na(model_data$batting_team_runs_skill) & model_data$batting_team_runs_skill != 0)
    cli::cli_alert_success("{n_team}/{nrow(model_data)} have team skills")
  }, error = function(e) {
    cli::cli_alert_warning("Team skills not available: {e$message}")
    cli::cli_alert_info("Using neutral values (0) for team skills")
    model_data <<- model_data %>%
      dplyr::mutate(
        batting_team_runs_skill = 0,
        batting_team_wicket_skill = 0,
        bowling_team_runs_skill = 0,
        bowling_team_wicket_skill = 0
      )
  })

  # Venue skills
  cli::cli_alert_info("Adding venue skills...")
  tryCatch({
    model_data <- join_venue_skill_indices(model_data, format = format, conn = conn)
    # Fill missing with neutral values
    # For residual-based (run_rate, wicket_rate): 0
    # For raw EMA (boundary_rate, dot_rate): use format defaults
    start_vals <- get_venue_start_values(format)
    model_data <- model_data %>%
      dplyr::mutate(
        venue_run_rate = dplyr::coalesce(venue_run_rate, 0),
        venue_wicket_rate = dplyr::coalesce(venue_wicket_rate, 0),
        venue_boundary_rate = dplyr::coalesce(venue_boundary_rate, start_vals$boundary_rate),
        venue_dot_rate = dplyr::coalesce(venue_dot_rate, start_vals$dot_rate)
      )
    n_venue <- sum(!is.na(model_data$venue_run_rate) & model_data$venue_run_rate != 0)
    cli::cli_alert_success("{n_venue}/{nrow(model_data)} have venue skills")
  }, error = function(e) {
    cli::cli_alert_warning("Venue skills not available: {e$message}")
    cli::cli_alert_info("Using neutral values for venue skills")
    start_vals <- get_venue_start_values(format)
    model_data <<- model_data %>%
      dplyr::mutate(
        venue_run_rate = 0,
        venue_wicket_rate = 0,
        venue_boundary_rate = start_vals$boundary_rate,
        venue_dot_rate = start_vals$dot_rate
      )
  })

  # 3-Way ELO features (optional, default to neutral if unavailable)
  has_elo_features <- FALSE
  if (!INCLUDE_ELO_FEATURES) {
    model_data <- model_data %>%
      dplyr::mutate(elo_run_diff = 0, elo_wicket_diff = 0, elo_venue_run = 0)
  } else if (INCLUDE_ELO_FEATURES) {
    cli::cli_alert_info("Adding 3-way ELO features...")
    # The frame is mixed-gender (gender_male is itself a feature), and the ELO
    # tables are per gender AND format. Building the name as
    # paste0(format, "_3way_elo") hit an empty legacy table in T20 and a stale
    # women's-only one in ODI/Test, so every ELO feature coalesced to neutral
    # for every row while the step reported success (bouncerverse#63).
    elo_query <- three_way_elo_query(format, c(
      "delivery_id",
      "batter_run_elo_before AS batter_run_elo",
      "bowler_run_elo_before AS bowler_run_elo",
      "batter_wicket_elo_before AS batter_wicket_elo",
      "bowler_wicket_elo_before AS bowler_wicket_elo",
      "venue_session_run_elo_before AS venue_session_run_elo",
      "venue_perm_run_elo_before AS venue_perm_run_elo"), conn)
    tryCatch({
      if (!is.null(elo_query)) {
        elo_data <- DBI::dbGetQuery(conn, elo_query)

        model_data <- model_data %>%
          dplyr::left_join(elo_data, by = "delivery_id") %>%
          dplyr::mutate(
            # ELO differences (more useful as features than raw values)
            elo_run_diff = dplyr::coalesce(batter_run_elo, 1400) - dplyr::coalesce(bowler_run_elo, 1400),
            elo_wicket_diff = dplyr::coalesce(batter_wicket_elo, 1400) - dplyr::coalesce(bowler_wicket_elo, 1400),
            elo_venue_run = dplyr::coalesce(venue_session_run_elo, 1400) + dplyr::coalesce(venue_perm_run_elo, 1400) - 2800
          )

        n_elo <- sum(!is.na(model_data$batter_run_elo))
        cov <- n_elo / nrow(model_data)
        # A zero-coverage join used to print as a success line with "0/N" in it.
        # Neutral features for every row is a missing join, not a trained model.
        if (cov < 0.5) {
          cli::cli_abort(c(
            "Only {n_elo}/{nrow(model_data)} rows ({round(100*cov, 1)}%) matched a 3-way ELO.",
            "x" = "Below 50% the ELO features are mostly neutral and the model is not using them.",
            "i" = "Read from: {.val {three_way_elo_tables(format, conn)}}.",
            "i" = "Set INCLUDE_ELO_FEATURES <- FALSE to train without them deliberately."))
        }
        cli::cli_alert_success("{n_elo}/{nrow(model_data)} ({round(100*cov, 1)}%) have ELO features")
        has_elo_features <- TRUE
      } else {
        cli::cli_alert_warning(
          "No 3-way ELO table for {.val {format}}, skipping ELO features")
        model_data <- model_data %>%
          dplyr::mutate(elo_run_diff = 0, elo_wicket_diff = 0, elo_venue_run = 0)
      }
    }, error = function(e) {
      cli::cli_alert_warning("ELO features unavailable: {e$message}")
      model_data <<- model_data %>%
        dplyr::mutate(elo_run_diff = 0, elo_wicket_diff = 0, elo_venue_run = 0)
    })
  }

  model_data
}


#' The 7-Class Ball Outcome Used by the Outcome Models
#'
#' Wicket, dot, 1, 2, 3, 4, 6 as classes 0-6. Anything else (5s, 7s, and any
#' row missing its inputs) is `NA` and is dropped by the trainer.
#'
#' Declared once because it was inline in `02_train_full_model.R`'s mutate,
#' and any comparison against those models has to reproduce the label exactly.
#' A checker that rebuilds the mapping by hand is one edit away from scoring
#' against a different target and reporting the difference as a model result
#' (bouncerverse#65).
#'
#' @param runs_batter Integer vector. Runs off the bat.
#' @param is_wicket Logical or integer vector.
#' @return Integer vector of class labels, `NA` where undefined.
#' @keywords internal
ball_outcome_class <- function(runs_batter, is_wicket) {
  data.table::fcase(
    as.logical(is_wicket), 0L,
    runs_batter == 0, 1L,
    runs_batter == 1, 2L,
    runs_batter == 2, 3L,
    runs_batter == 3, 4L,
    runs_batter == 4, 5L,
    runs_batter == 6, 6L,
    default = NA_integer_
  )
}
