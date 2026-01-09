# Feature Engineering Functions for Win Probability Models
#
# Functions for calculating features used in projected score and win probability models.

#' Calculate Rolling Features for Deliveries
#'
#' Calculates rolling momentum features over specified ball/over windows.
#' Designed for efficient processing of large delivery datasets.
#'
#' @param dt A data.table or data.frame of deliveries, sorted by match_id, innings, over, ball.
#'   Must contain columns: match_id, innings, over, ball, runs_total, is_wicket, is_four, is_six.
#' @param ball_windows Integer vector of ball window sizes (default: c(12, 24))
#' @param over_windows Integer vector of over window sizes (default: c(3, 6))
#'
#' @return Data with additional rolling feature columns:
#'   \itemize{
#'     \item runs_last_N_balls: Total runs in last N balls
#'     \item dots_last_N_balls: Dot balls in last N balls
#'     \item boundaries_last_N_balls: Boundaries (4s and 6s) in last N balls
#'     \item wickets_last_N_balls: Wickets in last N balls
#'     \item runs_last_N_overs: Total runs in last N overs
#'     \item wickets_last_N_overs: Wickets in last N overs
#'     \item rr_last_N_overs: Run rate in last N overs
#'   }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Add rolling features to delivery data
#' deliveries_with_features <- calculate_rolling_features(deliveries)
#'
#' # Custom windows
#' deliveries_with_features <- calculate_rolling_features(
#'   deliveries,
#'   ball_windows = c(6, 12, 24),
#'   over_windows = c(2, 4, 6)
#' )
#' }
calculate_rolling_features <- function(dt, ball_windows = c(12, 24), over_windows = c(3, 6)) {

  df <- as.data.frame(dt)
  df <- dplyr::arrange(df, match_id, innings, over, ball)

  # Ball-level rolling features
  for (window in ball_windows) {
    col_runs <- paste0("runs_last_", window, "_balls")
    col_dots <- paste0("dots_last_", window, "_balls")
    col_boundaries <- paste0("boundaries_last_", window, "_balls")
    col_wickets <- paste0("wickets_last_", window, "_balls")

    df <- df %>%
      dplyr::group_by(match_id, innings) %>%
      dplyr::mutate(
        !!col_runs := slider::slide_dbl(runs_total, sum, .before = window - 1, .complete = FALSE),
        !!col_dots := slider::slide_dbl(as.integer(runs_total == 0 & !is_wicket), sum, .before = window - 1, .complete = FALSE),
        !!col_boundaries := slider::slide_dbl(as.integer(is_four | is_six), sum, .before = window - 1, .complete = FALSE),
        !!col_wickets := slider::slide_dbl(as.integer(is_wicket), sum, .before = window - 1, .complete = FALSE)
      ) %>%
      dplyr::ungroup()
  }

  # Over-level aggregation for rolling over features
  over_agg <- df %>%
    dplyr::group_by(match_id, innings, over) %>%
    dplyr::summarise(
      over_runs = sum(runs_total, na.rm = TRUE),
      over_wickets = sum(as.integer(is_wicket), na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(match_id, innings, over)

  # Calculate rolling over features
  for (window in over_windows) {
    col_runs <- paste0("runs_last_", window, "_overs")
    col_wickets <- paste0("wickets_last_", window, "_overs")
    col_rr <- paste0("rr_last_", window, "_overs")

    over_agg <- over_agg %>%
      dplyr::group_by(match_id, innings) %>%
      dplyr::mutate(
        !!col_runs := slider::slide_dbl(over_runs, sum, .before = window - 1, .complete = FALSE),
        !!col_wickets := slider::slide_dbl(over_wickets, sum, .before = window - 1, .complete = FALSE),
        !!col_rr := .data[[col_runs]] / window
      ) %>%
      dplyr::ungroup()
  }

  # Merge back to ball-level data
  merge_cols <- grep("^(runs_last|wickets_last|rr_last).*overs$", names(over_agg), value = TRUE)
  over_agg_merge <- over_agg %>%
    dplyr::select(match_id, innings, over, dplyr::all_of(merge_cols))

  df <- dplyr::left_join(df, over_agg_merge, by = c("match_id", "innings", "over"))

  # Fill NA values at start of innings with 0
  rolling_cols <- grep("^(runs_last|dots_last|boundaries_last|wickets_last|rr_last)", names(df), value = TRUE)
  for (col in rolling_cols) {
    df[[col]] <- dplyr::coalesce(df[[col]], 0)
  }

  return(df)
}


#' Calculate Phase Features for T20 Cricket
#'
#' Identifies game phase (powerplay, middle, death) and related timing features.
#'
#' @param over Numeric vector of over numbers (0-indexed)
#' @param ball Numeric vector of ball numbers within over
#' @param match_type Character. Match format: "t20", "odi", or "test"
#'
#' @return Data frame with columns:
#'   \itemize{
#'     \item phase: Factor with levels "powerplay", "middle", "death", "test"
#'     \item overs_into_phase: How many overs into current phase
#'     \item balls_bowled: Total balls bowled in innings
#'     \item overs_completed: Decimal overs completed
#'     \item overs_remaining: Overs left in innings (NA for Test)
#'     \item balls_remaining: Balls left in innings (NA for Test)
#'   }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' phase_data <- calculate_phase_features(over = 12, ball = 3, match_type = "t20")
#' # Returns: phase = "middle", overs_into_phase = 6, etc.
#' }
calculate_phase_features <- function(over, ball, match_type = "t20") {

  match_type <- tolower(match_type)

  if (match_type == "t20") {
    phase <- dplyr::case_when(
      over < 6  ~ "powerplay",
      over < 16 ~ "middle",
      TRUE      ~ "death"
    )

    overs_into_phase <- dplyr::case_when(
      over < 6  ~ over,
      over < 16 ~ over - 6,
      TRUE      ~ over - 16
    )

    total_overs <- 20

  } else if (match_type == "odi") {
    phase <- dplyr::case_when(
      over < 10 ~ "powerplay",
      over < 40 ~ "middle",
      TRUE      ~ "death"
    )

    overs_into_phase <- dplyr::case_when(
      over < 10 ~ over,
      over < 40 ~ over - 10,
      TRUE      ~ over - 40
    )

    total_overs <- 50

  } else {
    # Test match - no phases
    phase <- "test"
    overs_into_phase <- over
    total_overs <- NA_real_
  }

  # Calculate balls bowled and overs remaining
  balls_bowled <- over * 6 + ball
  overs_completed <- over + ball / 6

  overs_remaining <- if (!is.na(total_overs)) {
    pmax(0, total_overs - overs_completed)
  } else {
    NA_real_
  }

  balls_remaining <- if (!is.na(total_overs)) {
    pmax(0, (total_overs * 6) - balls_bowled)
  } else {
    NA_real_
  }

  data.frame(
    phase = factor(phase, levels = c("powerplay", "middle", "death", "test")),
    overs_into_phase = overs_into_phase,
    balls_bowled = balls_bowled,
    overs_completed = overs_completed,
    overs_remaining = overs_remaining,
    balls_remaining = balls_remaining
  )
}


#' Calculate Venue Statistics
#'
#' Calculates historical venue performance metrics from match data.
#'
#' @param conn DuckDB connection
#' @param venue_filter Character vector of venues to calculate stats for (NULL = all)
#' @param match_type Character. Filter by match type ("t20", "odi", etc.)
#' @param min_matches Minimum matches at venue to include (default: 5)
#'
#' @return Data frame with columns:
#'   \itemize{
#'     \item venue: Venue name
#'     \item total_matches: Number of matches at venue
#'     \item avg_first_innings_score: Mean 1st innings total
#'     \item sd_first_innings_score: Std dev of 1st innings totals
#'     \item avg_second_innings_score: Mean 2nd innings total
#'     \item avg_first_innings_wickets: Mean wickets lost in 1st innings
#'     \item chase_success_rate: Proportion of successful chases
#'   }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection()
#' venue_stats <- calculate_venue_statistics(conn, match_type = "t20")
#' DBI::dbDisconnect(conn, shutdown = TRUE)
#' }
calculate_venue_statistics <- function(conn, venue_filter = NULL, match_type = "t20", min_matches = 5) {

  # Build venue filter clause
  venue_clause <- ""
  if (!is.null(venue_filter) && length(venue_filter) > 0) {
    escaped_venues <- gsub("'", "''", venue_filter)
    venue_list <- paste0("'", escaped_venues, "'", collapse = ",")
    venue_clause <- paste0(" AND m.venue IN (", venue_list, ")")
  }

  query <- sprintf("
    SELECT
      m.venue,
      COUNT(DISTINCT mi.match_id) as total_matches,
      AVG(CASE WHEN mi.innings = 1 THEN mi.total_runs END) as avg_first_innings_score,
      STDDEV(CASE WHEN mi.innings = 1 THEN mi.total_runs END) as sd_first_innings_score,
      AVG(CASE WHEN mi.innings = 2 THEN mi.total_runs END) as avg_second_innings_score,
      AVG(CASE WHEN mi.innings = 1 THEN mi.total_wickets END) as avg_first_innings_wickets,
      SUM(CASE
        WHEN mi.innings = 2 AND m.outcome_winner = mi.batting_team
        THEN 1 ELSE 0 END) * 1.0 /
      NULLIF(COUNT(DISTINCT CASE WHEN mi.innings = 2 THEN mi.match_id END), 0) as chase_success_rate
    FROM match_innings mi
    JOIN matches m ON mi.match_id = m.match_id
    WHERE LOWER(m.match_type) = ?
    %s
    GROUP BY m.venue
    HAVING COUNT(DISTINCT mi.match_id) >= ?
  ", venue_clause)

  venue_stats <- DBI::dbGetQuery(conn, query, params = list(match_type, min_matches))

  if (nrow(venue_stats) == 0) {
    cli::cli_alert_warning("No venue statistics found for match_type = {match_type}")
    return(data.frame(
      venue = character(),
      total_matches = integer(),
      avg_first_innings_score = numeric(),
      sd_first_innings_score = numeric(),
      avg_second_innings_score = numeric(),
      avg_first_innings_wickets = numeric(),
      chase_success_rate = numeric()
    ))
  }

  return(venue_stats)
}


#' Calculate Pressure Metrics for 2nd Innings Chase
#'
#' Calculates required run rate and chase pressure indicators.
#'
#' @param target Integer. Target score to chase
#' @param current_runs Integer. Runs scored so far
#' @param current_wickets Integer. Wickets fallen
#' @param balls_remaining Integer. Balls left in innings
#' @param current_run_rate Numeric. Current run rate
#'
#' @return Data frame with columns:
#'   \itemize{
#'     \item runs_needed: Runs still required
#'     \item wickets_in_hand: Wickets remaining (10 - current_wickets)
#'     \item required_run_rate: Runs per over needed to win
#'     \item rr_differential: Required RR minus current RR (positive = behind)
#'     \item balls_per_run_needed: Balls available per run required
#'     \item balls_per_wicket_available: Balls per remaining wicket
#'     \item is_death_chase: TRUE if in last 4 overs of chase
#'   }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Chasing 180, scored 100/3 with 30 balls left at 6.5 RPO
#' pressure <- calculate_pressure_metrics(
#'   target = 180,
#'   current_runs = 100,
#'   current_wickets = 3,
#'   balls_remaining = 30,
#'   current_run_rate = 6.5
#' )
#' }
calculate_pressure_metrics <- function(target, current_runs, current_wickets,
                                       balls_remaining, current_run_rate) {

  runs_needed <- pmax(0, target - current_runs)
  wickets_in_hand <- 10 - current_wickets
  overs_remaining <- balls_remaining / 6

  required_run_rate <- dplyr::case_when(
    runs_needed <= 0 ~ 0,
    overs_remaining <= 0 ~ Inf,
    TRUE ~ runs_needed / overs_remaining
  )

  rr_differential <- required_run_rate - current_run_rate

  balls_per_run_needed <- dplyr::case_when(
    runs_needed <= 0 ~ Inf,
    TRUE ~ balls_remaining / runs_needed
  )

  balls_per_wicket_available <- dplyr::case_when(
    wickets_in_hand <= 0 ~ 0,
    TRUE ~ balls_remaining / wickets_in_hand
  )

  is_death_chase <- balls_remaining <= 24 & balls_remaining > 0

  data.frame(
    runs_needed = runs_needed,
    wickets_in_hand = wickets_in_hand,
    required_run_rate = required_run_rate,
    rr_differential = rr_differential,
    balls_per_run_needed = balls_per_run_needed,
    balls_per_wicket_available = balls_per_wicket_available,
    is_death_chase = is_death_chase
  )
}


#' Calculate Current Run Rate
#'
#' Simple utility to calculate run rate from runs and balls.
#'
#' @param runs Numeric. Total runs scored
#' @param balls Numeric. Balls faced
#'
#' @return Numeric. Run rate (runs per over)
#'
#' @export
#'
#' @examples
#' calculate_run_rate(runs = 85, balls = 60)
#' # Returns 8.5 (runs per over)
calculate_run_rate <- function(runs, balls) {
  overs <- balls / 6
  dplyr::case_when(
    overs <= 0 ~ 0,
    TRUE ~ runs / overs
  )
}


#' Calculate Expected Runs per Ball
#'
#' Calculates the expected runs for each ball based on the projected final score
#' and current state. This is used for ERA (Expected Runs Added) calculations.
#'
#' Works for both innings:
#' - Innings 1: How many runs per ball needed to reach projected score
#' - Innings 2: How many runs per ball needed to reach projected score (and hopefully target)
#'
#' @param projected_score Numeric. Projected final innings score
#' @param current_runs Numeric. Runs scored so far
#' @param balls_remaining Numeric. Balls left in innings
#'
#' @return Numeric. Expected runs per ball (capped between 0 and 6)
#'
#' @export
#'
#' @examples
#' # Early in innings: projected 170, scored 30, 90 balls left
#' calculate_expected_runs_per_ball(170, 30, 90)
#' # Returns: (170 - 30) / 90 = 1.56 expected runs per ball
#'
#' # Late in innings: projected 165, scored 140, 12 balls left
#' calculate_expected_runs_per_ball(165, 140, 12)
#' # Returns: (165 - 140) / 12 = 2.08 expected runs per ball
calculate_expected_runs_per_ball <- function(projected_score, current_runs, balls_remaining) {

  expected_runs <- dplyr::case_when(
    balls_remaining <= 0 ~ 0,
    projected_score <= current_runs ~ 0,  # Already exceeded projection
    TRUE ~ (projected_score - current_runs) / balls_remaining
  )

  # Cap between 0 and 6 (max possible per ball)
  expected_runs <- pmax(0, pmin(expected_runs, 6))

  return(expected_runs)
}


#' Calculate Expected Runs Added (ERA)
#'
#' Calculates how many runs were scored above or below expectation.
#' Positive ERA = scored more than expected = good for batter
#' Negative ERA = scored less than expected = good for bowler
#'
#' @param actual_runs Numeric. Actual runs scored on the ball
#' @param expected_runs Numeric. Expected runs for this ball
#'
#' @return Numeric. ERA value (actual - expected)
#'
#' @export
#'
#' @examples
#' # Hit a 4 when expected 1.5 runs
#' calculate_era(4, 1.5)
#' # Returns: 2.5 (scored 2.5 above expectation)
#'
#' # Dot ball when expected 1.5 runs
#' calculate_era(0, 1.5)
#' # Returns: -1.5 (scored 1.5 below expectation)
calculate_era <- function(actual_runs, expected_runs) {
  actual_runs - expected_runs
}


#' Calculate Tail Calibration Features for Win Probability Model
#'
#' Calculates features that help the model produce well-calibrated predictions
#' at the extremes (near 0% and 100% win probability). These features capture
#' "game essentially decided" situations that the model might otherwise miss.
#'
#' @param runs_needed Integer. Runs still required to win
#' @param balls_remaining Integer. Balls left in innings
#' @param wickets_in_hand Integer. Wickets remaining (10 - wickets_fallen)
#'
#' @return Data frame with columns:
#'   \itemize{
#'     \item chase_completed: Binary. 1 if runs_needed <= 0 (already won)
#'     \item chase_impossible: Binary. 1 if balls/wickets exhausted with runs still needed
#'     \item runs_per_ball_needed: runs_needed / balls_remaining (Inf capped at 6)
#'     \item balls_per_run_available: balls_remaining / runs_needed (capped at 20)
#'     \item resources_per_run: (balls + wickets*6) / runs_needed (capped at 30)
#'     \item chase_buffer: balls_remaining - runs_needed (how many "spare" balls)
#'     \item chase_buffer_ratio: chase_buffer / balls_remaining
#'     \item is_easy_chase: Binary. < 1 run per ball needed with 5+ wickets
#'     \item is_difficult_chase: Binary. > 2 runs per ball needed or < 3 wickets
#'     \item theoretical_min_balls: runs_needed / 6 (if all sixes)
#'     \item balls_surplus: balls_remaining - theoretical_min_balls
#'   }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Easy chase: need 20 runs from 30 balls with 8 wickets
#' tail_features <- calculate_tail_calibration_features(
#'   runs_needed = 20,
#'   balls_remaining = 30,
#'   wickets_in_hand = 8
#' )
#'
#' # Difficult chase: need 60 runs from 18 balls with 2 wickets
#' tail_features <- calculate_tail_calibration_features(
#'   runs_needed = 60,
#'   balls_remaining = 18,
#'   wickets_in_hand = 2
#' )
#' }
calculate_tail_calibration_features <- function(runs_needed, balls_remaining, wickets_in_hand) {

  # Binary indicators for game decided
  chase_completed <- as.integer(runs_needed <= 0)
  chase_impossible <- as.integer(
    (balls_remaining <= 0 | wickets_in_hand <= 0) & runs_needed > 0
  )

  # Runs per ball needed (key metric for chase difficulty)
  # Cap at 6 (max possible per ball) for Inf cases
  runs_per_ball_needed <- dplyr::case_when(
    runs_needed <= 0 ~ 0,
    balls_remaining <= 0 ~ 6,  # Cap at max possible
    TRUE ~ pmin(runs_needed / balls_remaining, 6)
  )

  # Balls per run available (inverse - higher = easier)
  # Cap at 20 for very easy chases
  balls_per_run_available <- dplyr::case_when(
    runs_needed <= 0 ~ 20,  # Cap for completed chase
    balls_remaining <= 0 ~ 0,
    TRUE ~ pmin(balls_remaining / runs_needed, 20)
  )

  # Combined resources: balls + wickets * 6 (each wicket worth ~6 balls)
  # This captures that having wickets in hand is like having extra balls
  total_resources <- balls_remaining + (wickets_in_hand * 6)
  resources_per_run <- dplyr::case_when(
    runs_needed <= 0 ~ 30,  # Cap for completed chase
    TRUE ~ pmin(total_resources / pmax(runs_needed, 1), 30)
  )

  # Chase buffer: how many "spare" balls beyond what's strictly needed
  chase_buffer <- balls_remaining - runs_needed

  # Chase buffer ratio (normalized)
  chase_buffer_ratio <- dplyr::case_when(
    balls_remaining <= 0 ~ -1,
    TRUE ~ chase_buffer / balls_remaining
  )

  # Binary indicators for easy/difficult situations
  is_easy_chase <- as.integer(
    runs_per_ball_needed < 1 & wickets_in_hand >= 5 & runs_needed > 0
  )

  is_difficult_chase <- as.integer(
    (runs_per_ball_needed > 2 | wickets_in_hand < 3) & runs_needed > 0 & balls_remaining > 0
  )

  # Theoretical minimum balls needed (if hitting all sixes)
  theoretical_min_balls <- ceiling(runs_needed / 6)

  # Balls surplus over theoretical minimum
  balls_surplus <- balls_remaining - theoretical_min_balls

  data.frame(
    chase_completed = chase_completed,
    chase_impossible = chase_impossible,
    runs_per_ball_needed = runs_per_ball_needed,
    balls_per_run_available = balls_per_run_available,
    resources_per_run = resources_per_run,
    chase_buffer = chase_buffer,
    chase_buffer_ratio = chase_buffer_ratio,
    is_easy_chase = is_easy_chase,
    is_difficult_chase = is_difficult_chase,
    theoretical_min_balls = theoretical_min_balls,
    balls_surplus = balls_surplus
  )
}


#' Create Grouped CV Folds by Match ID
#'
#' Creates cross-validation fold indices grouped by match_id to prevent data leakage.
#' All deliveries from the same match are kept in the same fold.
#'
#' @param data Data frame with match_id column
#' @param n_folds Integer. Number of CV folds (default: 5)
#' @param seed Integer. Random seed for reproducibility
#'
#' @return List of integer vectors, each containing row indices for a fold
#'
#' @export
#'
#' @examples
#' \dontrun{
#' folds <- create_grouped_folds(deliveries, n_folds = 5)
#' # folds[[1]] contains row indices for fold 1
#' # All deliveries from any given match are in the same fold
#' }
create_grouped_folds <- function(data, n_folds = 5, seed = 42) {

  set.seed(seed)

  unique_matches <- unique(data$match_id)
  shuffled_matches <- sample(unique_matches)

  fold_assignments <- cut(
    seq_along(shuffled_matches),
    breaks = n_folds,
    labels = FALSE
  )

  folds <- list()
  for (i in 1:n_folds) {
    fold_matches <- shuffled_matches[fold_assignments == i]
    folds[[i]] <- which(data$match_id %in% fold_matches)
  }

  return(folds)
}
