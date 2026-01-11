# Player Attribution via Zero-Ablation
#
# Quantifies individual player contributions to predicted outcomes using
# zero-ablation comparison. Since the model uses skill VALUES (not player identity),
# traditional SHAP doesn't directly give player attribution.
#
# Approach:
#   1. Full prediction: predict with all skills included
#   2. Ablated prediction: set one entity's skills to neutral (0 for residual-based)
#   3. Contribution = Full - Ablated
#
# This can be applied to:
#   - Batter: Set batter skills to neutral, difference = batter's contribution
#   - Bowler: Set bowler skills to neutral, difference = bowler's contribution
#   - Team: Set team skills to neutral, difference = team's contribution
#   - Venue: Set venue skills to neutral, difference = venue's contribution


#' Calculate Player Attribution
#'
#' Calculates individual player contributions to predicted runs/wickets
#' using zero-ablation.
#'
#' @param model XGBoost model from load_full_model()
#' @param delivery_data Data frame with delivery data including all skill features
#' @param format Character. Format: "t20", "odi", or "test"
#'
#' @return Data frame with original data plus contribution columns:
#'   - batter_contribution: runs contribution from batter skills
#'   - bowler_contribution: runs contribution from bowler skills
#'   - team_contribution: runs contribution from team skills
#'   - venue_contribution: runs contribution from venue skills
#'   - context_baseline: expected runs from context alone
#'
#' @keywords internal
calculate_player_attribution <- function(model, delivery_data, format = c("t20", "odi", "test")) {

  format <- match.arg(format)

  # Ensure we have all required columns
  required_cols <- c("batter_scoring_index", "batter_survival_rate",
                     "bowler_economy_index", "bowler_strike_rate")
  missing <- setdiff(required_cols, names(delivery_data))
  if (length(missing) > 0) {
    stop("Missing required columns: ", paste(missing, collapse = ", "))
  }

  n <- nrow(delivery_data)
  cli::cli_alert_info("Calculating attribution for {n} deliveries...")

  # Get starting values for neutral baseline
  skill_start <- get_skill_start_values(format)
  venue_start <- get_venue_start_values(format)

  # Full prediction (with all skills)
  probs_full <- predict_full_outcome(model, delivery_data, format)
  exp_runs_full <- get_full_expected_runs(probs_full)

  # 1. Batter ablation: Set batter skills to neutral
  data_no_batter <- delivery_data
  data_no_batter$batter_scoring_index <- skill_start$runs_per_ball
  data_no_batter$batter_survival_rate <- skill_start$survival_rate
  data_no_batter$batter_balls_faced <- 0

  probs_no_batter <- predict_full_outcome(model, data_no_batter, format)
  exp_runs_no_batter <- get_full_expected_runs(probs_no_batter)
  batter_contribution <- exp_runs_full - exp_runs_no_batter

  # 2. Bowler ablation: Set bowler skills to neutral
  data_no_bowler <- delivery_data
  data_no_bowler$bowler_economy_index <- skill_start$runs_per_ball
  data_no_bowler$bowler_strike_rate <- 1 - skill_start$survival_rate
  data_no_bowler$bowler_balls_bowled <- 0

  probs_no_bowler <- predict_full_outcome(model, data_no_bowler, format)
  exp_runs_no_bowler <- get_full_expected_runs(probs_no_bowler)
  bowler_contribution <- exp_runs_full - exp_runs_no_bowler

  # 3. Team ablation: Set team skills to 0 (neutral for residual-based)
  data_no_team <- delivery_data
  data_no_team$batting_team_runs_skill <- 0
  data_no_team$batting_team_wicket_skill <- 0
  data_no_team$bowling_team_runs_skill <- 0
  data_no_team$bowling_team_wicket_skill <- 0

  probs_no_team <- predict_full_outcome(model, data_no_team, format)
  exp_runs_no_team <- get_full_expected_runs(probs_no_team)
  team_contribution <- exp_runs_full - exp_runs_no_team

  # 4. Venue ablation: Set venue skills to neutral
  data_no_venue <- delivery_data
  data_no_venue$venue_run_rate <- 0
  data_no_venue$venue_wicket_rate <- 0
  data_no_venue$venue_boundary_rate <- venue_start$boundary_rate
  data_no_venue$venue_dot_rate <- venue_start$dot_rate

  probs_no_venue <- predict_full_outcome(model, data_no_venue, format)
  exp_runs_no_venue <- get_full_expected_runs(probs_no_venue)
  venue_contribution <- exp_runs_full - exp_runs_no_venue

  # 5. Context baseline: All skills neutral (equivalent to agnostic model)
  data_context_only <- delivery_data
  # Player skills
  data_context_only$batter_scoring_index <- skill_start$runs_per_ball
  data_context_only$batter_survival_rate <- skill_start$survival_rate
  data_context_only$batter_balls_faced <- 0
  data_context_only$bowler_economy_index <- skill_start$runs_per_ball
  data_context_only$bowler_strike_rate <- 1 - skill_start$survival_rate
  data_context_only$bowler_balls_bowled <- 0
  # Team skills
  data_context_only$batting_team_runs_skill <- 0
  data_context_only$batting_team_wicket_skill <- 0
  data_context_only$bowling_team_runs_skill <- 0
  data_context_only$bowling_team_wicket_skill <- 0
  # Venue skills
  data_context_only$venue_run_rate <- 0
  data_context_only$venue_wicket_rate <- 0
  data_context_only$venue_boundary_rate <- venue_start$boundary_rate
  data_context_only$venue_dot_rate <- venue_start$dot_rate

  probs_context <- predict_full_outcome(model, data_context_only, format)
  context_baseline <- get_full_expected_runs(probs_context)

  # Combine results
  result <- delivery_data
  result$exp_runs_full <- exp_runs_full
  result$batter_contribution <- batter_contribution
  result$bowler_contribution <- bowler_contribution
  result$team_contribution <- team_contribution
  result$venue_contribution <- venue_contribution
  result$context_baseline <- context_baseline

  cli::cli_alert_success("Attribution calculated")

  return(result)
}


#' Calculate Wicket Attribution
#'
#' Calculates individual contributions to wicket probability using zero-ablation.
#'
#' @param model XGBoost model from load_full_model()
#' @param delivery_data Data frame with delivery data including all skill features
#' @param format Character. Format: "t20", "odi", or "test"
#'
#' @return Data frame with wicket probability contributions
#'
#' @keywords internal
calculate_wicket_attribution <- function(model, delivery_data, format = c("t20", "odi", "test")) {

  format <- match.arg(format)

  # Get starting values for neutral baseline
  skill_start <- get_skill_start_values(format)
  venue_start <- get_venue_start_values(format)

  # Full prediction
  probs_full <- predict_full_outcome(model, delivery_data, format)
  exp_wicket_full <- get_full_expected_wicket(probs_full)

  # Batter ablation
  data_no_batter <- delivery_data
  data_no_batter$batter_scoring_index <- skill_start$runs_per_ball
  data_no_batter$batter_survival_rate <- skill_start$survival_rate
  data_no_batter$batter_balls_faced <- 0

  probs_no_batter <- predict_full_outcome(model, data_no_batter, format)
  exp_wicket_no_batter <- get_full_expected_wicket(probs_no_batter)
  batter_wicket_contribution <- exp_wicket_full - exp_wicket_no_batter

  # Bowler ablation
  data_no_bowler <- delivery_data
  data_no_bowler$bowler_economy_index <- skill_start$runs_per_ball
  data_no_bowler$bowler_strike_rate <- 1 - skill_start$survival_rate
  data_no_bowler$bowler_balls_bowled <- 0

  probs_no_bowler <- predict_full_outcome(model, data_no_bowler, format)
  exp_wicket_no_bowler <- get_full_expected_wicket(probs_no_bowler)
  bowler_wicket_contribution <- exp_wicket_full - exp_wicket_no_bowler

  # Combine results
  result <- delivery_data
  result$exp_wicket_full <- exp_wicket_full
  result$batter_wicket_contribution <- batter_wicket_contribution
  result$bowler_wicket_contribution <- bowler_wicket_contribution

  return(result)
}


#' Summarize Player Contributions
#'
#' Aggregates per-delivery contributions to player-level summaries.
#'
#' @param attribution_df Data frame from calculate_player_attribution()
#'
#' @return List with batter_summary and bowler_summary data frames
#'
#' @keywords internal
summarize_player_contributions <- function(attribution_df) {

  if (!"batter" %in% names(attribution_df)) {
    cli::cli_alert_warning("No 'batter' column found")
    return(NULL)
  }

  # Batter summary
  batter_summary <- attribution_df %>%
    dplyr::group_by(.data$batter) %>%
    dplyr::summarise(
      deliveries = dplyr::n(),
      total_runs_contribution = sum(.data$batter_contribution, na.rm = TRUE),
      avg_runs_contribution = mean(.data$batter_contribution, na.rm = TRUE),
      total_actual_runs = sum(.data$runs_batter, na.rm = TRUE),
      actual_runs_vs_context = sum(.data$runs_batter, na.rm = TRUE) -
                               sum(.data$context_baseline, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::arrange(dplyr::desc(.data$total_runs_contribution))

  # Bowler summary
  if ("bowler" %in% names(attribution_df)) {
    bowler_summary <- attribution_df %>%
      dplyr::group_by(.data$bowler) %>%
      dplyr::summarise(
        deliveries = dplyr::n(),
        total_runs_contribution = sum(.data$bowler_contribution, na.rm = TRUE),
        avg_runs_contribution = mean(.data$bowler_contribution, na.rm = TRUE),
        runs_conceded = sum(.data$runs_batter, na.rm = TRUE),
        wickets = sum(.data$is_wicket, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::arrange(.data$total_runs_contribution)  # Lower is better for bowlers
  } else {
    bowler_summary <- NULL
  }

  list(
    batter = batter_summary,
    bowler = bowler_summary
  )
}


#' Compare Expected vs Actual with Attribution
#'
#' Creates a summary comparing expected runs (from model) with actual runs,
#' broken down by attribution component.
#'
#' @param attribution_df Data frame from calculate_player_attribution()
#'
#' @return Data frame with summary statistics
#'
#' @keywords internal
compare_expected_vs_actual <- function(attribution_df) {

  if (!"runs_batter" %in% names(attribution_df)) {
    stop("Need 'runs_batter' column for comparison")
  }

  # Overall summary
  summary_stats <- data.frame(
    metric = c("Total Actual Runs", "Total Expected Runs (Full Model)",
               "Context Baseline Total", "Batter Contribution Total",
               "Bowler Contribution Total", "Team Contribution Total",
               "Venue Contribution Total"),
    value = c(
      sum(attribution_df$runs_batter, na.rm = TRUE),
      sum(attribution_df$exp_runs_full, na.rm = TRUE),
      sum(attribution_df$context_baseline, na.rm = TRUE),
      sum(attribution_df$batter_contribution, na.rm = TRUE),
      sum(attribution_df$bowler_contribution, na.rm = TRUE),
      sum(attribution_df$team_contribution, na.rm = TRUE),
      sum(attribution_df$venue_contribution, na.rm = TRUE)
    ),
    stringsAsFactors = FALSE
  )

  # Add percentage breakdown
  total_exp <- sum(attribution_df$exp_runs_full, na.rm = TRUE)
  if (total_exp > 0) {
    summary_stats$pct_of_expected <- c(
      NA, 100,
      sum(attribution_df$context_baseline, na.rm = TRUE) / total_exp * 100,
      sum(attribution_df$batter_contribution, na.rm = TRUE) / total_exp * 100,
      sum(attribution_df$bowler_contribution, na.rm = TRUE) / total_exp * 100,
      sum(attribution_df$team_contribution, na.rm = TRUE) / total_exp * 100,
      sum(attribution_df$venue_contribution, na.rm = TRUE) / total_exp * 100
    )
  }

  return(summary_stats)
}


#' Get Match Attribution Summary
#'
#' Calculates attribution summary for a specific match.
#'
#' @param model XGBoost model from load_full_model()
#' @param match_id Character. Match ID
#' @param format Character. Format: "t20", "odi", or "test"
#' @param conn DBI connection
#'
#' @return List with match-level and player-level attribution summaries
#'
#' @keywords internal
get_match_attribution <- function(model, match_id, format = "t20", conn) {

  # Load delivery data for match
  query <- sprintf("
    SELECT d.*
    FROM deliveries d
    WHERE d.match_id = ?
    ORDER BY d.innings, d.over, d.ball
  ")
  deliveries <- DBI::dbGetQuery(conn, query, params = list(match_id))

  if (nrow(deliveries) == 0) {
    stop("No deliveries found for match: ", match_id)
  }

  # Add skill indices
  deliveries <- add_skill_features(deliveries, format, conn, fill_missing = TRUE)

  # Add team skills (if available)
  tryCatch({
    deliveries <- join_team_skill_indices(deliveries, format, conn)
  }, error = function(e) {
    deliveries$batting_team_runs_skill <- 0
    deliveries$batting_team_wicket_skill <- 0
    deliveries$bowling_team_runs_skill <- 0
    deliveries$bowling_team_wicket_skill <- 0
  })

  # Add venue skills (if available)
  tryCatch({
    deliveries <- join_venue_skill_indices(deliveries, format, conn)
  }, error = function(e) {
    deliveries$venue_run_rate <- 0
    deliveries$venue_wicket_rate <- 0
    deliveries$venue_boundary_rate <- 0.15
    deliveries$venue_dot_rate <- 0.35
  })

  # Calculate attribution
  attribution <- calculate_player_attribution(model, deliveries, format)

  # Summarize
  player_summary <- summarize_player_contributions(attribution)
  overall_summary <- compare_expected_vs_actual(attribution)

  list(
    match_id = match_id,
    n_deliveries = nrow(deliveries),
    overall = overall_summary,
    batters = player_summary$batter,
    bowlers = player_summary$bowler,
    attribution_df = attribution
  )
}
