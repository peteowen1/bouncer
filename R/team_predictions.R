# Team-Level Prediction Functions
#
# Functions for predicting match outcomes, innings scores, and
# comparing team rosters based on player ELO ratings.

#' Calculate Roster ELO
#'
#' Aggregates individual player ELO ratings into a team strength metric.
#'
#' @param player_ids Character vector. Player identifiers in the roster
#' @param match_type Character. Match type ("test", "odi", "t20"). Default "t20".
#' @param weights Named list with batting/bowling weights. Default gives equal weight.
#' @param db_path Character. Database path
#'
#' @return List with:
#'   \itemize{
#'     \item team_batting_elo: Average batting ELO of roster
#'     \item team_bowling_elo: Average bowling ELO of roster
#'     \item combined_elo: Weighted combination
#'     \item player_details: Data frame with individual player ELOs
#'   }
#' @export
#'
#' @examples
#' \dontrun{
#' roster <- c("V Kohli", "R Sharma", "KL Rahul", "S Gill", "J Bumrah")
#' team_elo <- calculate_roster_elo(roster, match_type = "t20")
#' print(team_elo$combined_elo)
#' }
calculate_roster_elo <- function(player_ids,
                                  match_type = "t20",
                                  weights = list(batting = 0.5, bowling = 0.5),
                                  db_path = NULL) {

  if (length(player_ids) == 0) {
    stop("player_ids cannot be empty")
  }

  # Get ELO for each player
  player_elos <- data.frame(
    player_id = player_ids,
    batting_elo = numeric(length(player_ids)),
    bowling_elo = numeric(length(player_ids)),
    stringsAsFactors = FALSE
  )

  for (i in seq_along(player_ids)) {
    player_elos$batting_elo[i] <- get_batting_elo(player_ids[i], match_type, db_path = db_path)
    player_elos$bowling_elo[i] <- get_bowling_elo(player_ids[i], match_type, db_path = db_path)
  }

  # Calculate team aggregates
  team_batting_elo <- mean(player_elos$batting_elo, na.rm = TRUE)
  team_bowling_elo <- mean(player_elos$bowling_elo, na.rm = TRUE)

  # Combined ELO with weights
  batting_weight <- weights$batting %||% 0.5
  bowling_weight <- weights$bowling %||% 0.5
  combined_elo <- (team_batting_elo * batting_weight) + (team_bowling_elo * bowling_weight)

  list(
    team_batting_elo = team_batting_elo,
    team_bowling_elo = team_bowling_elo,
    combined_elo = combined_elo,
    player_details = player_elos,
    match_type = match_type
  )
}


#' Compare Team Rosters
#'
#' Compares two team rosters based on player ELO ratings.
#'
#' @param team1_players Character vector. Player IDs for team 1
#' @param team2_players Character vector. Player IDs for team 2
#' @param team1_name Character. Name for team 1. Default "Team 1".
#' @param team2_name Character. Name for team 2. Default "Team 2".
#' @param match_type Character. Match type. Default "t20".
#' @param db_path Character. Database path
#'
#' @return List with:
#'   \itemize{
#'     \item team1_elo: Roster ELO for team 1
#'     \item team2_elo: Roster ELO for team 2
#'     \item batting_advantage: Team with better batting (and by how much)
#'     \item bowling_advantage: Team with better bowling (and by how much)
#'     \item overall_advantage: Team with higher combined ELO
#'     \item expected_win_prob: Win probability for team 1
#'   }
#' @export
#'
#' @examples
#' \dontrun{
#' india <- c("V Kohli", "R Sharma", "J Bumrah")
#' australia <- c("S Smith", "D Warner", "P Cummins")
#' comparison <- compare_team_rosters(india, australia, "India", "Australia")
#' print(comparison$expected_win_prob)
#' }
compare_team_rosters <- function(team1_players,
                                  team2_players,
                                  team1_name = "Team 1",
                                  team2_name = "Team 2",
                                  match_type = "t20",
                                  db_path = NULL) {

  # Get roster ELOs for both teams
  team1_elo <- calculate_roster_elo(team1_players, match_type, db_path = db_path)
  team2_elo <- calculate_roster_elo(team2_players, match_type, db_path = db_path)

  # Calculate advantages
  batting_diff <- team1_elo$team_batting_elo - team2_elo$team_batting_elo
  bowling_diff <- team1_elo$team_bowling_elo - team2_elo$team_bowling_elo
  combined_diff <- team1_elo$combined_elo - team2_elo$combined_elo

  # Determine who has advantage
  batting_advantage <- if (batting_diff > 0) {
    list(team = team1_name, margin = batting_diff)
  } else if (batting_diff < 0) {
    list(team = team2_name, margin = abs(batting_diff))
  } else {
    list(team = "Even", margin = 0)
  }

  bowling_advantage <- if (bowling_diff > 0) {
    list(team = team1_name, margin = bowling_diff)
  } else if (bowling_diff < 0) {
    list(team = team2_name, margin = abs(bowling_diff))
  } else {
    list(team = "Even", margin = 0)
  }

  overall_advantage <- if (combined_diff > 0) {
    list(team = team1_name, margin = combined_diff)
  } else if (combined_diff < 0) {
    list(team = team2_name, margin = abs(combined_diff))
  } else {
    list(team = "Even", margin = 0)
  }

  # Win probability using ELO formula
  expected_win_prob <- calculate_expected_outcome(team1_elo$combined_elo, team2_elo$combined_elo)

  list(
    team1_name = team1_name,
    team2_name = team2_name,
    team1_elo = team1_elo,
    team2_elo = team2_elo,
    batting_advantage = batting_advantage,
    bowling_advantage = bowling_advantage,
    overall_advantage = overall_advantage,
    expected_win_prob = expected_win_prob
  )
}


#' Predict Roster Matchup Outcome
#'
#' Predicts the outcome of a match between two teams based on roster ELO ratings.
#'
#' @param team1_players Character vector. Player IDs for team 1
#' @param team2_players Character vector. Player IDs for team 2
#' @param team1_name Character. Name for team 1. Default "Team 1".
#' @param team2_name Character. Name for team 2. Default "Team 2".
#' @param match_type Character. Match type. Default "t20".
#' @param venue Character. Venue name for context (optional)
#' @param db_path Character. Database path
#'
#' @return List with:
#'   \itemize{
#'     \item team1_win_prob: Win probability for team 1
#'     \item team2_win_prob: Win probability for team 2
#'     \item predicted_winner: Team most likely to win
#'     \item confidence: Confidence level (difference from 50%)
#'     \item team_comparison: Full roster comparison
#'   }
#' @export
#'
#' @examples
#' \dontrun{
#' india <- c("V Kohli", "R Sharma", "J Bumrah")
#' australia <- c("S Smith", "D Warner", "P Cummins")
#' prediction <- predict_roster_matchup(india, australia, "India", "Australia")
#' print(paste(prediction$predicted_winner, "win probability:", prediction$team1_win_prob))
#' }
predict_roster_matchup <- function(team1_players,
                                   team2_players,
                                   team1_name = "Team 1",
                                   team2_name = "Team 2",
                                   match_type = "t20",
                                   venue = NULL,
                                   db_path = NULL) {

  # Get roster comparison
  comparison <- compare_team_rosters(
    team1_players, team2_players,
    team1_name, team2_name,
    match_type, db_path
  )

  team1_win_prob <- comparison$expected_win_prob
  team2_win_prob <- 1 - team1_win_prob

  # Determine predicted winner
  predicted_winner <- if (team1_win_prob > team2_win_prob) {
    team1_name
  } else if (team2_win_prob > team1_win_prob) {
    team2_name
  } else {
    "Toss-up"
  }

  # Confidence is how far from 50-50
  confidence <- abs(team1_win_prob - 0.5) * 2  # Scale to 0-1

  list(
    team1_name = team1_name,
    team2_name = team2_name,
    team1_win_prob = team1_win_prob,
    team2_win_prob = team2_win_prob,
    predicted_winner = predicted_winner,
    confidence = confidence,
    match_type = match_type,
    venue = venue,
    team_comparison = comparison
  )
}


#' Predict Innings Score
#'
#' Projects the final innings score based on current state and ELO ratings.
#'
#' @param batting_team_players Character vector. Batting team player IDs
#' @param bowling_team_players Character vector. Bowling team player IDs
#' @param current_score Integer. Current score. Default 0.
#' @param current_wickets Integer. Wickets fallen. Default 0.
#' @param current_overs Numeric. Overs completed. Default 0.
#' @param total_overs Integer. Total overs in innings. Default 20 for T20.
#' @param match_type Character. Match type. Default "t20".
#' @param db_path Character. Database path
#'
#' @return List with:
#'   \itemize{
#'     \item projected_score: Expected final score
#'     \item projected_range: Score range (low, high) based on variance
#'     \item run_rate_required: Required run rate (if chasing)
#'     \item current_run_rate: Current run rate
#'   }
#' @export
#'
#' @examples
#' \dontrun{
#' batters <- c("V Kohli", "R Sharma", "KL Rahul")
#' bowlers <- c("P Cummins", "M Starc", "A Zampa")
#' projection <- predict_innings_score(
#'   batters, bowlers,
#'   current_score = 80, current_wickets = 2, current_overs = 10
#' )
#' print(projection$projected_score)
#' }
predict_innings_score <- function(batting_team_players,
                                   bowling_team_players,
                                   current_score = 0,
                                   current_wickets = 0,
                                   current_overs = 0,
                                   total_overs = 20,
                                   match_type = "t20",
                                   db_path = NULL) {

  # Get team ELOs
  batting_elo <- calculate_roster_elo(batting_team_players, match_type, db_path = db_path)
  bowling_elo <- calculate_roster_elo(bowling_team_players, match_type, db_path = db_path)

  # Calculate expected runs per ball based on ELO difference
  p_batter_success <- calculate_expected_outcome(
    batting_elo$team_batting_elo,
    bowling_elo$team_bowling_elo
  )

  # Base run rate expectations by format
  base_rpo <- switch(tolower(match_type),
    "t20" = 8.0,
    "odi" = 5.5,
    "test" = 3.5,
    7.0  # default
  )

  # Adjust run rate based on ELO
  # p_batter_success of 0.5 = neutral, adjust +/- 20% based on ELO advantage
  elo_adjustment <- (p_batter_success - 0.5) * 0.4
  adjusted_rpo <- base_rpo * (1 + elo_adjustment)

  # Calculate current run rate
  current_rr <- if (current_overs > 0) current_score / current_overs else 0

  # Overs remaining
  overs_remaining <- total_overs - current_overs
  balls_remaining <- overs_remaining * 6

  # Project remaining runs
  remaining_balls_factor <- (10 - current_wickets) / 10  # Wicket impact
  expected_remaining_rr <- adjusted_rpo * remaining_balls_factor
  expected_remaining_runs <- expected_remaining_rr * overs_remaining

  # Projected final score
  projected_score <- round(current_score + expected_remaining_runs)

  # Calculate range (variance based on wickets and overs remaining)
  variance_factor <- sqrt(overs_remaining / total_overs) * (1 + current_wickets * 0.1)
  score_std <- expected_remaining_runs * 0.2 * variance_factor
  projected_low <- round(projected_score - 1.5 * score_std)
  projected_high <- round(projected_score + 1.5 * score_std)

  list(
    projected_score = projected_score,
    projected_range = c(low = max(current_score, projected_low), high = projected_high),
    current_score = current_score,
    current_wickets = current_wickets,
    current_overs = current_overs,
    current_run_rate = round(current_rr, 2),
    expected_run_rate = round(expected_remaining_rr, 2),
    overs_remaining = overs_remaining,
    batting_elo = batting_elo$team_batting_elo,
    bowling_elo = bowling_elo$team_bowling_elo
  )
}


#' Simulate Match
#'
#' Runs a Monte Carlo simulation of a match to estimate win probabilities
#' with confidence intervals.
#'
#' @param team1_players Character vector. Player IDs for team 1
#' @param team2_players Character vector. Player IDs for team 2
#' @param team1_name Character. Name for team 1. Default "Team 1".
#' @param team2_name Character. Name for team 2. Default "Team 2".
#' @param match_type Character. Match type. Default "t20".
#' @param n_simulations Integer. Number of simulations. Default 1000.
#' @param db_path Character. Database path
#'
#' @return List with:
#'   \itemize{
#'     \item team1_win_pct: Win percentage for team 1
#'     \item team2_win_pct: Win percentage for team 2
#'     \item tie_pct: Tie percentage
#'     \item team1_scores: Distribution of team 1 scores
#'     \item team2_scores: Distribution of team 2 scores
#'     \item margin_distribution: Distribution of victory margins
#'     \item confidence_interval: 95% CI for team1 win probability
#'   }
#' @export
#'
#' @examples
#' \dontrun{
#' india <- c("V Kohli", "R Sharma", "J Bumrah")
#' australia <- c("S Smith", "D Warner", "P Cummins")
#' sim <- simulate_match(india, australia, "India", "Australia", n_simulations = 1000)
#' print(paste("India wins:", sim$team1_win_pct, "%"))
#' }
simulate_match <- function(team1_players,
                            team2_players,
                            team1_name = "Team 1",
                            team2_name = "Team 2",
                            match_type = "t20",
                            n_simulations = 1000,
                            db_path = NULL) {

  # Get team ELOs
  team1_batting <- calculate_roster_elo(team1_players, match_type,
                                        weights = list(batting = 1, bowling = 0),
                                        db_path = db_path)
  team1_bowling <- calculate_roster_elo(team1_players, match_type,
                                        weights = list(batting = 0, bowling = 1),
                                        db_path = db_path)
  team2_batting <- calculate_roster_elo(team2_players, match_type,
                                        weights = list(batting = 1, bowling = 0),
                                        db_path = db_path)
  team2_bowling <- calculate_roster_elo(team2_players, match_type,
                                        weights = list(batting = 0, bowling = 1),
                                        db_path = db_path)

  # Base parameters by format
  total_overs <- switch(tolower(match_type),
    "t20" = 20,
    "odi" = 50,
    "test" = 90,  # Per innings approximation
    20
  )

  base_rpo <- switch(tolower(match_type),
    "t20" = 8.0,
    "odi" = 5.5,
    "test" = 3.5,
    7.0
  )

  base_sd <- switch(tolower(match_type),
    "t20" = 20,
    "odi" = 35,
    "test" = 50,
    20
  )

  # Run simulations
  team1_scores <- numeric(n_simulations)
  team2_scores <- numeric(n_simulations)

  for (i in seq_len(n_simulations)) {
    # Team 1 batting vs Team 2 bowling
    p1_bat <- calculate_expected_outcome(
      team1_batting$team_batting_elo,
      team2_bowling$team_bowling_elo
    )
    adj1 <- (p1_bat - 0.5) * 0.4
    mean1 <- base_rpo * total_overs * (1 + adj1)
    team1_scores[i] <- round(stats::rnorm(1, mean = mean1, sd = base_sd))

    # Team 2 batting vs Team 1 bowling
    p2_bat <- calculate_expected_outcome(
      team2_batting$team_batting_elo,
      team1_bowling$team_bowling_elo
    )
    adj2 <- (p2_bat - 0.5) * 0.4
    mean2 <- base_rpo * total_overs * (1 + adj2)
    team2_scores[i] <- round(stats::rnorm(1, mean = mean2, sd = base_sd))
  }

  # Ensure non-negative scores

  team1_scores <- pmax(team1_scores, 0)
  team2_scores <- pmax(team2_scores, 0)

  # Calculate outcomes
  team1_wins <- sum(team1_scores > team2_scores)
  team2_wins <- sum(team2_scores > team1_scores)
  ties <- sum(team1_scores == team2_scores)

  team1_win_pct <- round(100 * team1_wins / n_simulations, 1)
  team2_win_pct <- round(100 * team2_wins / n_simulations, 1)
  tie_pct <- round(100 * ties / n_simulations, 1)

  # Calculate margins
  margins <- team1_scores - team2_scores

  # 95% CI for win probability
  p_hat <- team1_wins / n_simulations
  se <- sqrt(p_hat * (1 - p_hat) / n_simulations)
  ci_low <- max(0, p_hat - 1.96 * se)
  ci_high <- min(1, p_hat + 1.96 * se)

  list(
    team1_name = team1_name,
    team2_name = team2_name,
    team1_win_pct = team1_win_pct,
    team2_win_pct = team2_win_pct,
    tie_pct = tie_pct,
    team1_avg_score = round(mean(team1_scores)),
    team2_avg_score = round(mean(team2_scores)),
    team1_score_range = c(min = min(team1_scores), max = max(team1_scores)),
    team2_score_range = c(min = min(team2_scores), max = max(team2_scores)),
    avg_margin = round(mean(abs(margins))),
    confidence_interval = c(low = round(ci_low * 100, 1), high = round(ci_high * 100, 1)),
    n_simulations = n_simulations,
    match_type = match_type
  )
}
