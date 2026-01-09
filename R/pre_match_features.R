# Pre-Match Feature Engineering
#
# This module provides functions to calculate features for pre-game match prediction.
# All features use only data available BEFORE the match starts.


#' Calculate Pre-Match Features
#'
#' Calculates all pre-match features for a given match. Uses only data
#' available before the match date.
#'
#' @param match_id Character. Match identifier
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with one row containing all features for the match
#' @export
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection()
#' features <- calculate_pre_match_features("1234567", conn)
#' }
calculate_pre_match_features <- function(match_id, conn) {

  # Get match info
match_info <- DBI::dbGetQuery(conn, "
    SELECT match_id, match_date, match_type, event_name, venue,
           team1, team2, toss_winner, toss_decision,
           event_match_number, event_group
    FROM matches
    WHERE match_id = ?
  ", params = list(match_id))

  if (nrow(match_info) == 0) {
    cli::cli_alert_warning("Match {match_id} not found")
    return(NULL)
  }

  match_date <- match_info$match_date
  match_type <- match_info$match_type
  event_name <- match_info$event_name
  venue <- match_info$venue
  team1 <- match_info$team1
  team2 <- match_info$team2

  # Get Team 1 ELOs
  team1_elo_result <- get_team_elo(team1, match_date, "result", conn)
  team1_roster <- calculate_team_roster_elo(team1, match_date, conn,
                                             match_type = match_type,
                                             event_name = event_name)

  # Get Team 2 ELOs
  team2_elo_result <- get_team_elo(team2, match_date, "result", conn)
  team2_roster <- calculate_team_roster_elo(team2, match_date, conn,
                                             match_type = match_type,
                                             event_name = event_name)

  # Get Team 1 Skill Indices (from format-specific skill table)
  team1_skills <- calculate_team_roster_skill(team1, match_date, conn,
                                               match_type = match_type,
                                               event_name = event_name)

  # Get Team 2 Skill Indices
  team2_skills <- calculate_team_roster_skill(team2, match_date, conn,
                                               match_type = match_type,
                                               event_name = event_name)

  # Get form (last 5 matches)
  team1_form <- calculate_team_form(team1, match_date, n_matches = 5, conn,
                                     match_type = match_type,
                                     event_name = event_name)
  team2_form <- calculate_team_form(team2, match_date, n_matches = 5, conn,
                                     match_type = match_type,
                                     event_name = event_name)

  # Get head-to-head record
  h2h <- calculate_h2h_record(team1, team2, match_date, conn,
                               match_type = match_type,
                               event_name = event_name)

  # Get venue features
  venue_features <- calculate_venue_features(venue, match_type, conn, as_of_date = match_date)

  # Determine if knockout match
  is_knockout <- detect_knockout_match(match_info$event_match_number,
                                        match_info$event_group)


  # Toss features
  toss_winner <- match_info$toss_winner
  toss_decision <- match_info$toss_decision
  team1_won_toss <- as.integer(!is.na(toss_winner) && toss_winner == team1)
  toss_elect_bat <- as.integer(!is.na(toss_decision) && tolower(toss_decision) == "bat")

  # Build feature row
  features <- data.frame(
    match_id = match_id,
    match_date = match_date,
    match_type = match_type,
    event_name = event_name %||% NA_character_,
    team1 = team1,
    team2 = team2,

    # Team 1 ELO features
    team1_elo_result = team1_elo_result,
    team1_elo_roster = team1_roster$elo_combined,
    team1_form_last5 = team1_form %||% NA_real_,
    team1_h2h_wins = h2h$team1_wins,
    team1_h2h_total = h2h$total_matches,

    # Team 1 Skill Index features
    team1_bat_scoring_avg = team1_skills$batter_scoring_avg,
    team1_bat_scoring_top5 = team1_skills$batter_scoring_top5,
    team1_bat_survival_avg = team1_skills$batter_survival_avg,
    team1_bowl_economy_avg = team1_skills$bowler_economy_avg,
    team1_bowl_economy_top5 = team1_skills$bowler_economy_top5,
    team1_bowl_strike_avg = team1_skills$bowler_strike_avg,

    # Team 2 ELO features
    team2_elo_result = team2_elo_result,
    team2_elo_roster = team2_roster$elo_combined,
    team2_form_last5 = team2_form %||% NA_real_,
    team2_h2h_wins = h2h$team2_wins,
    team2_h2h_total = h2h$total_matches,

    # Team 2 Skill Index features
    team2_bat_scoring_avg = team2_skills$batter_scoring_avg,
    team2_bat_scoring_top5 = team2_skills$batter_scoring_top5,
    team2_bat_survival_avg = team2_skills$batter_survival_avg,
    team2_bowl_economy_avg = team2_skills$bowler_economy_avg,
    team2_bowl_economy_top5 = team2_skills$bowler_economy_top5,
    team2_bowl_strike_avg = team2_skills$bowler_strike_avg,

    # Venue features
    venue = venue,
    venue_avg_score = venue_features$avg_score,
    venue_chase_success_rate = venue_features$chase_success_rate,
    venue_matches = venue_features$n_matches,

    # Match context
    is_knockout = is_knockout,
    is_neutral_venue = FALSE,  # TODO: detect neutral venues

    # Toss features
    team1_won_toss = team1_won_toss,
    toss_elect_bat = toss_elect_bat,

    created_at = Sys.time(),

    stringsAsFactors = FALSE
  )

  return(features)
}


#' Calculate Venue Features
#'
#' Calculates venue-specific features for prediction.
#'
#' @param venue Character. Venue name
#' @param match_type Character. Match type for filtering
#' @param conn DBI connection. Database connection
#' @param as_of_date Date. Only consider matches before this date (optional)
#' @param min_matches Integer. Minimum matches required (default 5)
#'
#' @return List with avg_score, chase_success_rate, n_matches
#' @export
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection()
#' venue_stats <- calculate_venue_features("Wankhede Stadium", "t20", conn)
#' }
calculate_venue_features <- function(venue, match_type, conn,
                                      as_of_date = NULL,
                                      min_matches = 5) {

  # Default return for venues with insufficient data
  default_result <- list(
    avg_score = NA_real_,
    chase_success_rate = NA_real_,
    n_matches = 0L
  )

  if (is.null(venue) || is.na(venue) || venue == "") {
    return(default_result)
  }

  # Build query
  query <- "
    SELECT
      m.match_id,
      m.outcome_winner,
      m.team1,
      m.team2,
      mi.innings,
      mi.total_runs,
      mi.batting_team
    FROM matches m
    JOIN match_innings mi ON m.match_id = mi.match_id
    WHERE m.venue = ?
      AND LOWER(m.match_type) = LOWER(?)
      AND m.outcome_winner IS NOT NULL
      AND m.outcome_winner != ''
  "
  params <- list(venue, match_type)

  if (!is.null(as_of_date)) {
    # Ensure as_of_date is properly formatted as a date string
    as_of_date <- as.character(as.Date(as_of_date))
    query <- paste0(query, " AND m.match_date < CAST(? AS DATE)")
    params <- c(params, as_of_date)
  }

  result <- DBI::dbGetQuery(conn, query, params = params)

  if (nrow(result) == 0) {
    return(default_result)
  }

  # Get unique matches
  matches <- unique(result[, c("match_id", "outcome_winner", "team1", "team2")])
  n_matches <- nrow(matches)

  if (n_matches < min_matches) {
    return(default_result)
  }

  # Calculate average first innings score
  first_innings <- result[result$innings == 1, ]
  avg_score <- if (nrow(first_innings) > 0) {
    mean(first_innings$total_runs, na.rm = TRUE)
  } else {
    NA_real_
  }

  # Calculate chase success rate
  # For each match, determine who batted second and if they won
  second_innings <- result[result$innings == 2, ]

  if (nrow(second_innings) > 0) {
    second_innings$chaser_won <- second_innings$batting_team == second_innings$outcome_winner
    chase_success_rate <- mean(second_innings$chaser_won, na.rm = TRUE)
  } else {
    chase_success_rate <- NA_real_
  }

  return(list(
    avg_score = avg_score,
    chase_success_rate = chase_success_rate,
    n_matches = as.integer(n_matches)
  ))
}


#' Detect Knockout Match
#'
#' Determines if a match is a knockout/playoff match based on event info.
#'
#' @param event_match_number Character or Integer. Match number in event
#' @param event_group Character. Group/stage information
#'
#' @return Logical. TRUE if knockout match
#' @export
detect_knockout_match <- function(event_match_number, event_group) {

  # Convert to character for pattern matching
  match_num <- tolower(as.character(event_match_number %||% ""))
  group <- tolower(as.character(event_group %||% ""))

  # Keywords that indicate knockout
  knockout_keywords <- c("final", "qualifier", "eliminator", "playoff",
                         "semi", "knockout", "super")

  # Check both fields
  is_knockout <- any(sapply(knockout_keywords, function(kw) {
    grepl(kw, match_num, fixed = TRUE) || grepl(kw, group, fixed = TRUE)
  }))

  return(is_knockout)
}


#' Batch Calculate Pre-Match Features
#'
#' Calculates pre-match features for multiple matches efficiently.
#'
#' @param match_ids Character vector. Match identifiers
#' @param conn DBI connection. Database connection
#' @param progress Logical. Show progress bar (default TRUE)
#'
#' @return Data frame with features for all matches
#' @export
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection()
#' match_ids <- c("1234567", "1234568", "1234569")
#' features <- batch_calculate_features(match_ids, conn)
#' }
batch_calculate_features <- function(match_ids, conn, progress = TRUE) {

  n_matches <- length(match_ids)

  if (n_matches == 0) {
    return(data.frame())
  }

  if (progress) {
    cli::cli_progress_bar("Calculating features", total = n_matches)
  }

  results <- vector("list", n_matches)

  for (i in seq_along(match_ids)) {
    results[[i]] <- tryCatch({
      calculate_pre_match_features(match_ids[i], conn)
    }, error = function(e) {
      cli::cli_alert_warning("Error for match {match_ids[i]}: {e$message}")
      NULL
    })

    if (progress) {
      cli::cli_progress_update()
    }
  }

  if (progress) {
    cli::cli_progress_done()
  }

  # Combine results
  results <- results[!sapply(results, is.null)]

  if (length(results) == 0) {
    return(data.frame())
  }

  features_df <- do.call(rbind, results)

  return(features_df)
}


#' Store Pre-Match Features
#'
#' Stores calculated features in the database.
#'
#' @param features Data frame. Features to store (from calculate_pre_match_features)
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns number of rows inserted
#' @export
store_pre_match_features <- function(features, conn) {

  if (nrow(features) == 0) {
    return(invisible(0L))
  }

  # Use INSERT OR REPLACE to handle updates
  DBI::dbWriteTable(conn, "pre_match_features", features,
                    append = TRUE, overwrite = FALSE)

  invisible(nrow(features))
}


#' Get Pre-Match Features
#'
#' Retrieves pre-match features from the database.
#'
#' @param match_id Character. Match identifier (optional - if NULL, returns all)
#' @param event_name Character. Filter by event name (optional)
#' @param match_type Character. Filter by match type (optional)
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with features
#' @export
get_pre_match_features <- function(match_id = NULL, event_name = NULL,
                                    match_type = NULL, conn) {

  query <- "SELECT * FROM pre_match_features WHERE 1=1"
  params <- list()

  if (!is.null(match_id)) {
    query <- paste0(query, " AND match_id = ?")
    params <- c(params, match_id)
  }

  if (!is.null(event_name)) {
    query <- paste0(query, " AND event_name LIKE ?")
    params <- c(params, paste0("%", event_name, "%"))
  }

  if (!is.null(match_type)) {
    query <- paste0(query, " AND LOWER(match_type) = LOWER(?)")
    params <- c(params, match_type)
  }

  query <- paste0(query, " ORDER BY match_date")

  if (length(params) > 0) {
    DBI::dbGetQuery(conn, query, params = params)
  } else {
    DBI::dbGetQuery(conn, query)
  }
}


#' Prepare Prediction Features
#'
#' Creates derived features from raw pre-match features for model training
#' and prediction. This includes ELO differences, skill index differences,
#' form features, and interaction terms.
#'
#' @param df Data frame with raw pre-match features (from calculate_pre_match_features
#'   or the format-specific prediction_features.rds file)
#'
#' @return Data frame with additional derived feature columns
#' @export
#'
#' @examples
#' \dontrun{
#' features <- readRDS("t20_prediction_features.rds")
#' prepared <- prepare_prediction_features(features$train)
#' }
prepare_prediction_features <- function(df) {
  df %>%
    dplyr::mutate(
      # ELO differences (most important features)
      elo_diff_result = team1_elo_result - team2_elo_result,
      elo_diff_roster = team1_elo_roster - team2_elo_roster,

      # Raw ELO values (scaled to be centered around 0)
      team1_elo_scaled = (team1_elo_result - 1500) / 100,
      team2_elo_scaled = (team2_elo_result - 1500) / 100,
      team1_roster_scaled = (team1_elo_roster - 1500) / 100,
      team2_roster_scaled = (team2_elo_roster - 1500) / 100,

      # Average match quality (higher = stronger teams)
      match_quality = (team1_elo_result + team2_elo_result) / 2 - 1500,

      # Form features
      form_diff = dplyr::coalesce(team1_form_last5, 0.5) - dplyr::coalesce(team2_form_last5, 0.5),
      team1_form = dplyr::coalesce(team1_form_last5, 0.5),
      team2_form = dplyr::coalesce(team2_form_last5, 0.5),

      # H2H advantage (centered at 0)
      h2h_advantage = ifelse(
        team1_h2h_total > 0,
        team1_h2h_wins / team1_h2h_total - 0.5,
        0
      ),
      h2h_matches = pmin(team1_h2h_total, 20) / 20,  # Normalized, capped at 20

      # Skill index differences (team1 - team2)
      # Batting: higher is better for team1
      bat_scoring_diff = team1_bat_scoring_avg - team2_bat_scoring_avg,
      bat_scoring_top5_diff = team1_bat_scoring_top5 - team2_bat_scoring_top5,
      bat_survival_diff = team1_bat_survival_avg - team2_bat_survival_avg,

      # Bowling: lower economy is better, so team2 - team1 (positive = team1 better)
      bowl_economy_diff = team2_bowl_economy_avg - team1_bowl_economy_avg,
      bowl_economy_top5_diff = team2_bowl_economy_top5 - team1_bowl_economy_top5,
      # Strike rate: higher is better for bowler
      bowl_strike_diff = team1_bowl_strike_avg - team2_bowl_strike_avg,

      # Combined skill advantage
      overall_bat_advantage = (bat_scoring_diff + bat_scoring_top5_diff) / 2,
      overall_bowl_advantage = (bowl_economy_diff + bowl_economy_top5_diff) / 2,

      # Team per-delivery skill differences (residual-based: higher = better for team1)
      team_runs_skill_diff = dplyr::coalesce(team1_team_runs_skill, 0) -
                             dplyr::coalesce(team2_team_runs_skill, 0),
      team_wicket_skill_diff = dplyr::coalesce(team1_team_wicket_skill, 0) -
                               dplyr::coalesce(team2_team_wicket_skill, 0),

      # Venue skill features
      venue_run_skill = dplyr::coalesce(venue_run_rate_skill, 0),
      venue_wicket_skill = dplyr::coalesce(venue_wicket_rate_skill, 0),
      venue_boundary = dplyr::coalesce(venue_boundary_rate, 0.15),
      venue_dot = dplyr::coalesce(venue_dot_rate, 0.35),

      # Toss features
      team1_won_toss = dplyr::coalesce(as.integer(team1_won_toss), 0L),
      toss_elect_bat = dplyr::coalesce(as.integer(toss_elect_bat), 0L),
      # Toss interaction: did toss winner choose to bat?
      toss_chose_bat_first = team1_won_toss * toss_elect_bat,

      # Venue features (fill NAs)
      venue_avg_score = dplyr::coalesce(venue_avg_score, 160),
      venue_chase_success_rate = dplyr::coalesce(venue_chase_success_rate, 0.5),
      venue_high_scoring = as.integer(dplyr::coalesce(venue_avg_score, 160) > 170),

      # Match context
      is_knockout = as.integer(dplyr::coalesce(is_knockout, FALSE)),

      # Interaction features
      # Strong team with good form = momentum
      elo_form_interaction = elo_diff_result * form_diff / 100,
      # Toss advantage varies by venue chase rate
      toss_venue_interaction = team1_won_toss * (venue_chase_success_rate - 0.5),
      # Skill advantage in high-scoring venues
      bat_advantage_high_scoring = bat_scoring_diff * (venue_avg_score - 160) / 20,
      # Combined strength indicator
      total_advantage = (elo_diff_result / 100 + bat_scoring_diff + bowl_economy_diff) / 3
    )
}


#' Get Prediction Feature Columns
#'
#' Returns the list of feature column names used for XGBoost prediction models.
#'
#' @return Character vector of feature column names
#' @export
#'
#' @examples
#' feature_cols <- get_prediction_feature_cols()
get_prediction_feature_cols <- function() {
  c(
    # ELO differences (2)
    "elo_diff_result", "elo_diff_roster",

    # Raw ELO values (5)
    "team1_elo_scaled", "team2_elo_scaled",
    "team1_roster_scaled", "team2_roster_scaled",
    "match_quality",

    # Form & H2H (5)
    "form_diff", "team1_form", "team2_form",
    "h2h_advantage", "h2h_matches",

    # Player skill index differences (8)
    "bat_scoring_diff", "bat_scoring_top5_diff", "bat_survival_diff",
    "bowl_economy_diff", "bowl_economy_top5_diff", "bowl_strike_diff",
    "overall_bat_advantage", "overall_bowl_advantage",

    # Team per-delivery skill differences (2)
    "team_runs_skill_diff", "team_wicket_skill_diff",

    # Toss (3)
    "team1_won_toss", "toss_elect_bat", "toss_chose_bat_first",

    # Venue historical (3)
    "venue_avg_score", "venue_chase_success_rate", "venue_high_scoring",

    # Venue skill indices (4)
    "venue_run_skill", "venue_wicket_skill",
    "venue_boundary", "venue_dot",

    # Interactions (4)
    "elo_form_interaction", "toss_venue_interaction",
    "bat_advantage_high_scoring", "total_advantage",

    # Match context (1)
    "is_knockout"
  )
}


# =============================================================================
# Margin Model Helpers (Two-Stage Prediction)
# =============================================================================

#' Get Default Models Path
#'
#' Returns the default path for model files.
#'
#' @return Character string with models directory path
#' @keywords internal
get_default_models_path <- function() {
  data_dir <- find_bouncerdata_dir(create = FALSE)
  file.path(data_dir, "models")
}


#' Load Margin Model
#'
#' Loads a trained XGBoost margin regression model for a given format.
#'
#' @param format Character. Format to load ("t20", "odi", "test")
#' @param model_dir Character. Directory containing models. Default: "../bouncerdata/models"
#'
#' @return XGBoost model object or NULL if not found
#' @export
#'
#' @examples
#' \dontrun{
#' margin_model <- load_margin_model("t20")
#' }
load_margin_model <- function(format, model_dir = NULL) {
  if (!requireNamespace("xgboost", quietly = TRUE)) {
    cli::cli_abort("xgboost package required. Install with: install.packages('xgboost')")
  }

  # Default model directory
  if (is.null(model_dir)) {
    model_dir <- get_default_models_path()
  }

  model_path <- file.path(model_dir, paste0(format, "_margin_model.ubj"))

  if (!file.exists(model_path)) {
    cli::cli_alert_warning("Margin model not found at {model_path}")
    return(NULL)
  }

  model <- xgboost::xgb.load(model_path)
  cli::cli_alert_success("Loaded margin model for {toupper(format)}")
  model
}


#' Get Margin Prediction
#'
#' Generates margin predictions for prepared feature data using the margin model.
#'
#' @param features Data frame with prepared features (from prepare_prediction_features)
#' @param margin_model XGBoost margin model (from load_margin_model)
#'
#' @return Numeric vector of predicted margins (positive = team1 favored)
#' @export
#'
#' @examples
#' \dontrun{
#' margin_model <- load_margin_model("t20")
#' features <- prepare_prediction_features(raw_features)
#' pred_margin <- get_margin_prediction(features, margin_model)
#' }
get_margin_prediction <- function(features, margin_model) {
  if (!requireNamespace("xgboost", quietly = TRUE)) {
    cli::cli_abort("xgboost package required. Install with: install.packages('xgboost')")
  }

  # Get base feature columns (without predicted_margin)
  feature_cols <- get_prediction_feature_cols()

  # Check all required features exist
  missing_cols <- setdiff(feature_cols, names(features))
  if (length(missing_cols) > 0) {
    cli::cli_abort("Missing features: {paste(missing_cols, collapse = ', ')}")
  }

  # Create feature matrix
  X <- as.matrix(features[, feature_cols, drop = FALSE])
  X[is.na(X)] <- 0

  # Generate predictions
  dmat <- xgboost::xgb.DMatrix(data = X)
  predicted_margin <- predict(margin_model, dmat)

  predicted_margin
}


#' Get Prediction Feature Columns (with Margin)
#'
#' Returns feature columns for the two-stage model, including predicted_margin.
#'
#' @param include_margin Logical. Include predicted_margin feature? Default: TRUE
#'
#' @return Character vector of feature column names
#' @export
get_prediction_feature_cols_full <- function(include_margin = TRUE) {
  base_cols <- get_prediction_feature_cols()


  if (include_margin) {
    c(base_cols, "predicted_margin")
  } else {
    base_cols
  }
}


# =============================================================================
# Fast Feature Calculation Helpers (for bulk processing)
# =============================================================================

#' Get Team ELO (Fast Version)
#'
#' Fast lookup of team ELO from pre-loaded data.table.
#'
#' @param team Character. Team ID (composite format: team_gender_format)
#' @param as_of_date Date. Get ELO as of this date
#' @param team_elo_dt data.table. Pre-loaded team ELO data
#'
#' @return List with elo_result and elo_roster
#' @export
get_team_elo_fast <- function(team, as_of_date, team_elo_dt) {
  idx <- which(team_elo_dt$team_id == team & team_elo_dt$match_date < as_of_date)
  if (length(idx) == 0) {
    return(list(elo_result = 1500, elo_roster = 1500))
  }
  subset_dt <- team_elo_dt[idx, , drop = FALSE]
  best_idx <- which.max(subset_dt$match_date)
  list(
    elo_result = subset_dt$elo_result[best_idx],
    elo_roster = subset_dt$elo_roster_combined[best_idx] %||% 1500
  )
}


#' Get Team Form (Fast Version)
#'
#' Fast calculation of team form from pre-loaded data.table.
#' Uses composite team_id for proper gender/format separation.
#'
#' @param team_id Character. Composite team ID (team_gender_format)
#' @param as_of_date Date. Calculate form as of this date
#' @param matches_dt data.table. Pre-loaded matches data with team1_id, team2_id columns
#' @param n Integer. Number of recent matches to consider (default 5)
#'
#' @return Numeric. Win rate in last n matches (0-1), or NA if no matches
#' @export
get_team_form_fast <- function(team_id, as_of_date, matches_dt, n = 5) {
  idx <- which(
    (matches_dt$team1_id == team_id | matches_dt$team2_id == team_id) &
      matches_dt$match_date < as_of_date &
      matches_dt$outcome_winner != "" &
      !is.na(matches_dt$outcome_winner)
  )

  if (length(idx) == 0) return(NA_real_)

  subset_dt <- matches_dt[idx, , drop = FALSE]
  ord <- order(subset_dt$match_date, decreasing = TRUE)
  top_n <- ord[1:min(n, length(ord))]

  wins <- sum(subset_dt$outcome_winner_id[top_n] == team_id, na.rm = TRUE)
  wins / length(top_n)
}


#' Get Head-to-Head Record (Fast Version)
#'
#' Fast calculation of H2H record from pre-loaded data.table.
#' Uses composite team_ids for proper gender/format separation.
#'
#' @param team1_id Character. Composite team ID for team 1
#' @param team2_id Character. Composite team ID for team 2
#' @param as_of_date Date. Calculate H2H as of this date
#' @param matches_dt data.table. Pre-loaded matches data
#'
#' @return List with team1_wins and total
#' @export
get_h2h_fast <- function(team1_id, team2_id, as_of_date, matches_dt) {
  idx <- which(
    ((matches_dt$team1_id == team1_id & matches_dt$team2_id == team2_id) |
       (matches_dt$team1_id == team2_id & matches_dt$team2_id == team1_id)) &
      matches_dt$match_date < as_of_date &
      matches_dt$outcome_winner != "" &
      !is.na(matches_dt$outcome_winner)
  )

  if (length(idx) == 0) {
    return(list(team1_wins = 0L, total = 0L))
  }
  list(
    team1_wins = sum(matches_dt$outcome_winner_id[idx] == team1_id),
    total = length(idx)
  )
}


#' Get Venue Features (Fast Version)
#'
#' Fast calculation of venue features from pre-loaded data.table.
#'
#' @param venue_name Character. Venue name
#' @param as_of_date Date. Calculate features as of this date
#' @param venue_stats_dt data.table. Pre-loaded venue statistics
#' @param min_matches Integer. Minimum matches required (default 5)
#'
#' @return List with avg_score, chase_rate, n_matches
#' @export
get_venue_features_fast <- function(venue_name, as_of_date, venue_stats_dt, min_matches = 5) {

  idx <- which(venue_stats_dt$venue == venue_name & venue_stats_dt$match_date < as_of_date)

  if (length(idx) == 0) {
    return(list(avg_score = NA_real_, chase_rate = NA_real_, n_matches = 0L))
  }

  n_matches <- length(unique(venue_stats_dt$match_date[idx]))

  if (n_matches < min_matches) {
    return(list(avg_score = NA_real_, chase_rate = NA_real_, n_matches = as.integer(n_matches)))
  }

  # First innings average
  first_idx <- idx[venue_stats_dt$innings[idx] == 1]
  avg_score <- if (length(first_idx) > 0) mean(venue_stats_dt$total_runs[first_idx], na.rm = TRUE) else NA_real_

  # Chase success rate
  second_idx <- idx[venue_stats_dt$innings[idx] == 2]
  if (length(second_idx) > 0) {
    chaser_won <- venue_stats_dt$batting_team[second_idx] == venue_stats_dt$outcome_winner[second_idx]
    chase_rate <- mean(chaser_won, na.rm = TRUE)
  } else {
    chase_rate <- NA_real_
  }

  list(avg_score = avg_score, chase_rate = chase_rate, n_matches = as.integer(n_matches))
}


#' Get Team Skills (Fast Version)
#'
#' Fast calculation of team skill aggregates from pre-loaded data.
#'
#' @param team Character. Team name (not composite ID)
#' @param as_of_date Date. Calculate skills as of this date
#' @param player_participation_dt data.table. Pre-loaded player participation data
#' @param batter_skills data.table. Pre-loaded batter skill indices
#' @param bowler_skills data.table. Pre-loaded bowler skill indices
#' @param n_recent Integer. Number of recent matches to infer roster (default 3)
#'
#' @return List with batting and bowling skill aggregates
#' @export
get_team_skills_fast <- function(team, as_of_date, player_participation_dt,
                                  batter_skills, bowler_skills, n_recent = 3) {
  idx <- which(
    (player_participation_dt$batting_team == team | player_participation_dt$bowling_team == team) &
      player_participation_dt$match_date < as_of_date
  )

  if (length(idx) == 0) {
    return(list(
      bat_scoring_avg = 1.25, bat_scoring_top5 = 1.25, bat_survival_avg = 0.975,
      bowl_economy_avg = 1.25, bowl_economy_top5 = 1.25, bowl_strike_avg = 0.025
    ))
  }

  # Get recent matches
  match_ids_subset <- player_participation_dt$match_id[idx]
  match_dates_subset <- player_participation_dt$match_date[idx]
  match_dates <- tapply(match_dates_subset, match_ids_subset, function(x) x[1])
  ord <- order(match_dates, decreasing = TRUE)
  recent_match_ids <- names(match_dates)[ord][1:min(n_recent, length(ord))]

  # Get players from recent matches
  batter_idx <- which(
    player_participation_dt$match_id %in% recent_match_ids &
      player_participation_dt$batting_team == team
  )
  team_batters <- unique(player_participation_dt$batter_id[batter_idx])

  bowler_idx <- which(
    player_participation_dt$match_id %in% recent_match_ids &
      player_participation_dt$bowling_team == team
  )
  team_bowlers <- unique(player_participation_dt$bowler_id[bowler_idx])

  # Look up skills
  bat_idx <- which(batter_skills$player_id %in% team_batters & batter_skills$balls_faced >= 30)
  bowl_idx <- which(bowler_skills$player_id %in% team_bowlers & bowler_skills$balls_bowled >= 30)

  # Aggregate batting
  if (length(bat_idx) > 0) {
    bat_scoring_avg <- mean(batter_skills$scoring_index[bat_idx], na.rm = TRUE)
    bat_survival_avg <- mean(batter_skills$survival_rate[bat_idx], na.rm = TRUE)
    top5_ord <- order(batter_skills$scoring_index[bat_idx], decreasing = TRUE)[1:min(5, length(bat_idx))]
    bat_scoring_top5 <- mean(batter_skills$scoring_index[bat_idx][top5_ord], na.rm = TRUE)
  } else {
    bat_scoring_avg <- 1.25
    bat_scoring_top5 <- 1.25
    bat_survival_avg <- 0.975
  }

  # Aggregate bowling
  if (length(bowl_idx) > 0) {
    bowl_economy_avg <- mean(bowler_skills$economy_index[bowl_idx], na.rm = TRUE)
    bowl_strike_avg <- mean(bowler_skills$strike_rate[bowl_idx], na.rm = TRUE)
    top5_ord <- order(bowler_skills$economy_index[bowl_idx])[1:min(5, length(bowl_idx))]
    bowl_economy_top5 <- mean(bowler_skills$economy_index[bowl_idx][top5_ord], na.rm = TRUE)
  } else {
    bowl_economy_avg <- 1.25
    bowl_economy_top5 <- 1.25
    bowl_strike_avg <- 0.025
  }

  list(
    bat_scoring_avg = bat_scoring_avg,
    bat_scoring_top5 = bat_scoring_top5,
    bat_survival_avg = bat_survival_avg,
    bowl_economy_avg = bowl_economy_avg,
    bowl_economy_top5 = bowl_economy_top5,
    bowl_strike_avg = bowl_strike_avg
  )
}


#' Get Team Per-Delivery Skill (Fast Version)
#'
#' Fast lookup of team per-delivery skill indices from pre-loaded data.
#' These are the residual-based team skills calculated from the agnostic model.
#'
#' @param team Character. Team name
#' @param as_of_date Date. Get skills as of this date
#' @param team_skill_dt data.table. Pre-loaded team skill data (e.g., t20_team_skill table)
#'
#' @return List with runs_skill and wicket_skill
#' @export
get_team_delivery_skill_fast <- function(team, as_of_date, team_skill_dt) {
  # Look for batting team or bowling team skills
  idx_batting <- which(team_skill_dt$batting_team_id == team &
                         team_skill_dt$match_date < as_of_date)
  idx_bowling <- which(team_skill_dt$bowling_team_id == team &
                         team_skill_dt$match_date < as_of_date)

  # Default values (neutral for residual-based)
  runs_skill <- 0
  wicket_skill <- 0

  # Get most recent batting skills
  if (length(idx_batting) > 0) {
    subset_dt <- team_skill_dt[idx_batting, , drop = FALSE]
    best_idx <- which.max(subset_dt$match_date)
    # Use the skill where team was batting
    runs_skill <- subset_dt$batting_team_runs_skill[best_idx] %||% 0
    wicket_skill <- subset_dt$batting_team_wicket_skill[best_idx] %||% 0
  }

  list(runs_skill = runs_skill, wicket_skill = wicket_skill)
}


#' Get Venue Skill (Fast Version)
#'
#' Fast lookup of venue skill indices from pre-loaded data.
#' These are the residual-based venue skills calculated from the agnostic model.
#'
#' @param venue_name Character. Venue name (normalized)
#' @param as_of_date Date. Get skills as of this date
#' @param venue_skill_dt data.table. Pre-loaded venue skill data (e.g., t20_venue_skill table)
#'
#' @return List with run_rate_skill, wicket_rate_skill, boundary_rate, dot_rate
#' @export
get_venue_skill_fast <- function(venue_name, as_of_date, venue_skill_dt) {
  idx <- which(venue_skill_dt$venue == venue_name &
                 venue_skill_dt$match_date < as_of_date)

  # Default values (neutral for residual-based, format averages for raw EMA)
  if (length(idx) == 0) {
    return(list(
      run_rate_skill = 0,
      wicket_rate_skill = 0,
      boundary_rate = 0.15,  # Reasonable T20 average
      dot_rate = 0.35
    ))
  }

  subset_dt <- venue_skill_dt[idx, , drop = FALSE]
  best_idx <- which.max(subset_dt$match_date)

  list(
    run_rate_skill = subset_dt$venue_run_rate[best_idx] %||% 0,
    wicket_rate_skill = subset_dt$venue_wicket_rate[best_idx] %||% 0,
    boundary_rate = subset_dt$venue_boundary_rate[best_idx] %||% 0.15,
    dot_rate = subset_dt$venue_dot_rate[best_idx] %||% 0.35
  )
}


#' Calculate Match Features (Fast Version)
#'
#' Calculates all pre-match features for a single match from pre-loaded data.
#' Used for bulk feature calculation in the prediction pipeline.
#'
#' @param i Integer. Row index in matches_with_outcome
#' @param matches_with_outcome data.table/data.frame. Matches to process
#' @param team_elo_dt data.table. Pre-loaded team ELO data
#' @param batter_skills data.table. Pre-loaded batter skill indices
#' @param bowler_skills data.table. Pre-loaded bowler skill indices
#' @param player_participation_dt data.table. Pre-loaded player participation data
#' @param venue_stats_dt data.table. Pre-loaded venue statistics
#' @param team_skill_dt data.table. Pre-loaded team per-delivery skills (optional)
#' @param venue_skill_dt data.table. Pre-loaded venue skills (optional)
#'
#' @return data.frame with one row of features
#' @export
calc_match_features <- function(i, matches_with_outcome, team_elo_dt, batter_skills,
                                 bowler_skills, player_participation_dt, venue_stats_dt,
                                 team_skill_dt = NULL, venue_skill_dt = NULL) {
  # Extract match data

  match_id <- matches_with_outcome$match_id[i]
  team1 <- matches_with_outcome$team1[i]
  team2 <- matches_with_outcome$team2[i]
  team1_id <- matches_with_outcome$team1_id[i]
  team2_id <- matches_with_outcome$team2_id[i]
  match_date <- matches_with_outcome$match_date[i]
  match_type <- matches_with_outcome$match_type[i]
  event_name <- matches_with_outcome$event_name[i]
  venue <- matches_with_outcome$venue[i]
  toss_winner <- matches_with_outcome$toss_winner[i]
  toss_decision <- matches_with_outcome$toss_decision[i]
  event_match_number <- matches_with_outcome$event_match_number[i]
  event_group <- matches_with_outcome$event_group[i]

  # Team ELOs
  t1_elo <- get_team_elo_fast(team1_id, match_date, team_elo_dt)
  t2_elo <- get_team_elo_fast(team2_id, match_date, team_elo_dt)

  # Form
  t1_form <- get_team_form_fast(team1_id, match_date, matches_with_outcome)
  t2_form <- get_team_form_fast(team2_id, match_date, matches_with_outcome)

  # H2H
  h2h <- get_h2h_fast(team1_id, team2_id, match_date, matches_with_outcome)

  # Skills
  t1_skills <- get_team_skills_fast(team1, match_date, player_participation_dt, batter_skills, bowler_skills)
  t2_skills <- get_team_skills_fast(team2, match_date, player_participation_dt, batter_skills, bowler_skills)

  # Venue
  venue_feat <- get_venue_features_fast(venue, match_date, venue_stats_dt)

  # Team per-delivery skills (if available)
  if (!is.null(team_skill_dt) && nrow(team_skill_dt) > 0) {
    t1_team_skill <- get_team_delivery_skill_fast(team1, match_date, team_skill_dt)
    t2_team_skill <- get_team_delivery_skill_fast(team2, match_date, team_skill_dt)
  } else {
    t1_team_skill <- list(runs_skill = 0, wicket_skill = 0)
    t2_team_skill <- list(runs_skill = 0, wicket_skill = 0)
  }

  # Venue skills (if available)
  if (!is.null(venue_skill_dt) && nrow(venue_skill_dt) > 0) {
    venue_skill <- get_venue_skill_fast(venue, match_date, venue_skill_dt)
  } else {
    venue_skill <- list(run_rate_skill = 0, wicket_rate_skill = 0,
                        boundary_rate = 0.15, dot_rate = 0.35)
  }

  # Toss
  team1_won_toss <- as.integer(!is.na(toss_winner) && toss_winner == team1)
  toss_elect_bat <- as.integer(!is.na(toss_decision) && tolower(toss_decision) == "bat")

  # Knockout
  is_knockout <- detect_knockout_match(event_match_number, event_group)

  data.frame(
    match_id = match_id,
    match_date = match_date,
    match_type = match_type,
    event_name = event_name,
    team1 = team1,
    team2 = team2,
    team1_elo_result = t1_elo$elo_result,
    team1_elo_roster = t1_elo$elo_roster,
    team1_form_last5 = t1_form,
    team1_h2h_wins = h2h$team1_wins,
    team1_h2h_total = h2h$total,
    team1_bat_scoring_avg = t1_skills$bat_scoring_avg,
    team1_bat_scoring_top5 = t1_skills$bat_scoring_top5,
    team1_bat_survival_avg = t1_skills$bat_survival_avg,
    team1_bowl_economy_avg = t1_skills$bowl_economy_avg,
    team1_bowl_economy_top5 = t1_skills$bowl_economy_top5,
    team1_bowl_strike_avg = t1_skills$bowl_strike_avg,
    team2_elo_result = t2_elo$elo_result,
    team2_elo_roster = t2_elo$elo_roster,
    team2_form_last5 = t2_form,
    team2_h2h_wins = h2h$total - h2h$team1_wins,
    team2_h2h_total = h2h$total,
    team2_bat_scoring_avg = t2_skills$bat_scoring_avg,
    team2_bat_scoring_top5 = t2_skills$bat_scoring_top5,
    team2_bat_survival_avg = t2_skills$bat_survival_avg,
    team2_bowl_economy_avg = t2_skills$bowl_economy_avg,
    team2_bowl_economy_top5 = t2_skills$bowl_economy_top5,
    team2_bowl_strike_avg = t2_skills$bowl_strike_avg,
    venue = venue,
    venue_avg_score = venue_feat$avg_score,
    venue_chase_success_rate = venue_feat$chase_rate,
    venue_matches = venue_feat$n_matches,

    # Team per-delivery skills (residual-based)
    team1_team_runs_skill = t1_team_skill$runs_skill,
    team1_team_wicket_skill = t1_team_skill$wicket_skill,
    team2_team_runs_skill = t2_team_skill$runs_skill,
    team2_team_wicket_skill = t2_team_skill$wicket_skill,

    # Venue skills (residual-based + raw EMA)
    venue_run_rate_skill = venue_skill$run_rate_skill,
    venue_wicket_rate_skill = venue_skill$wicket_rate_skill,
    venue_boundary_rate = venue_skill$boundary_rate,
    venue_dot_rate = venue_skill$dot_rate,

    is_knockout = is_knockout,
    is_neutral_venue = FALSE,
    team1_won_toss = team1_won_toss,
    toss_elect_bat = toss_elect_bat,
    created_at = Sys.time(),
    stringsAsFactors = FALSE
  )
}
