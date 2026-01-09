# Team Correlation Functions for ELO Propagation
#
# Calculates correlations between teams based on:
# - Shared opponents (who they play against)
# - Same events (tournaments they participate in)
# - Direct matches (head-to-head encounters)
#
# Used to propagate ELO adjustments to correlated teams when
# cross-league matches reveal information about team strength.

# ============================================================================
# CORRELATION CONSTANTS
# ============================================================================

#' @keywords internal
CORRELATION_W_OPPONENTS <- 0.5  # Weight for shared opponents
CORRELATION_W_EVENTS <- 0.3     # Weight for same events
CORRELATION_W_DIRECT <- 0.2     # Weight for direct matches
CORRELATION_THRESHOLD <- 0.1    # Min correlation to propagate
PROPAGATION_FACTOR <- 0.2       # How much ELO change propagates

# ============================================================================
# JACCARD SIMILARITY
# ============================================================================

#' Calculate Jaccard Similarity
#'
#' Computes Jaccard index between two sets: |A ∩ B| / |A ∪ B|
#'
#' @param set_a First set (vector)
#' @param set_b Second set (vector)
#' @return Numeric between 0 and 1
#' @keywords internal
jaccard_similarity <- function(set_a, set_b) {
  if (length(set_a) == 0 && length(set_b) == 0) {
    return(0)
  }

  intersection <- length(intersect(set_a, set_b))
  union_size <- length(union(set_a, set_b))

  if (union_size == 0) return(0)
  intersection / union_size
}

# ============================================================================
# TEAM CORRELATION CALCULATION
# ============================================================================

#' Calculate Correlation Between Two Teams
#'
#' Computes a hybrid correlation score based on:
#' - Shared opponents (Jaccard similarity of opponent sets)
#' - Same events (Jaccard similarity of event sets)
#' - Direct matches (proportion of matches against each other)
#'
#' @param team_a First team ID
#' @param team_b Second team ID
#' @param team_opponents List mapping team_id -> vector of opponent_ids
#' @param team_events List mapping team_id -> vector of event_names
#' @param team_matchups List mapping team_id -> list(opponent_id -> count)
#' @param w_opponents Weight for shared opponents (default 0.5)
#' @param w_events Weight for same events (default 0.3)
#' @param w_direct Weight for direct matches (default 0.2)
#' @return Numeric correlation score between 0 and 1
#' @keywords internal
calculate_team_correlation <- function(team_a, team_b,
                                        team_opponents,
                                        team_events,
                                        team_matchups,
                                        w_opponents = CORRELATION_W_OPPONENTS,
                                        w_events = CORRELATION_W_EVENTS,
                                        w_direct = CORRELATION_W_DIRECT) {

  # Edge case: same team

if (team_a == team_b) return(1)

  # Get opponent sets (use unique to avoid counting repeated opponents multiple times)
  opp_a <- unique(team_opponents[[team_a]] %||% character(0))
  opp_b <- unique(team_opponents[[team_b]] %||% character(0))

  # Shared opponents (Jaccard similarity)
  jaccard_opponents <- jaccard_similarity(opp_a, opp_b)

  # Get event sets (use unique)
  evt_a <- unique(team_events[[team_a]] %||% character(0))
  evt_b <- unique(team_events[[team_b]] %||% character(0))

  # Same events (Jaccard similarity)
  jaccard_events <- jaccard_similarity(evt_a, evt_b)

  # Direct matches proportion
  matchups_a <- team_matchups[[team_a]] %||% list()
  matchups_b <- team_matchups[[team_b]] %||% list()

  direct_ab <- matchups_a[[team_b]] %||% 0
  direct_ba <- matchups_b[[team_a]] %||% 0
  total_direct <- direct_ab + direct_ba

  # Total matches for each team
  total_a <- sum(unlist(matchups_a), na.rm = TRUE)
  total_b <- sum(unlist(matchups_b), na.rm = TRUE)
  max_matches <- max(total_a, total_b, 1)

  direct_prop <- total_direct / max_matches

  # Weighted sum
  correlation <- w_opponents * jaccard_opponents +
                 w_events * jaccard_events +
                 w_direct * direct_prop

  # Ensure result is in [0, 1]
  min(max(correlation, 0), 1)
}

# ============================================================================
# BATCH CORRELATION CALCULATION
# ============================================================================

#' Calculate Correlations for a Team Against All Others
#'
#' Efficiently computes correlations between one team and all other teams.
#'
#' @param target_team Team ID to calculate correlations for
#' @param all_team_ids Vector of all team IDs
#' @param team_opponents List of opponent history
#' @param team_events List of event history
#' @param team_matchups List of matchup counts
#' @param threshold Minimum correlation to include (default 0.1)
#' @param w_opponents Weight for shared opponents
#' @param w_events Weight for same events
#' @param w_direct Weight for direct matches
#' @return Named numeric vector of correlations above threshold
#' @keywords internal
get_correlated_teams <- function(target_team,
                                  all_team_ids,
                                  team_opponents,
                                  team_events,
                                  team_matchups,
                                  threshold = CORRELATION_THRESHOLD,
                                  w_opponents = CORRELATION_W_OPPONENTS,
                                  w_events = CORRELATION_W_EVENTS,
                                  w_direct = CORRELATION_W_DIRECT) {

  correlations <- vapply(all_team_ids, function(other_team) {
    if (other_team == target_team) return(0)

    calculate_team_correlation(
      target_team, other_team,
      team_opponents, team_events, team_matchups,
      w_opponents, w_events, w_direct
    )
  }, numeric(1))

  names(correlations) <- all_team_ids

  # Filter to only those above threshold
  correlations[correlations > threshold]
}

# ============================================================================
# ELO PROPAGATION
# ============================================================================

#' Propagate ELO Adjustment to Correlated Teams
#'
#' When a team wins/loses, propagate a portion of the ELO change to
#' all teams that are correlated with them.
#'
#' @param team_elos Named list of current team ELOs
#' @param winner Winner team ID
#' @param loser Loser team ID
#' @param delta ELO change amount (positive)
#' @param team_opponents List of opponent history
#' @param team_events List of event history
#' @param team_matchups List of matchup counts
#' @param propagation_factor How much of delta to propagate (default 0.2)
#' @param correlation_threshold Min correlation to propagate (default 0.1)
#' @param w_opponents Weight for shared opponents
#' @param w_events Weight for same events
#' @param w_direct Weight for direct matches
#' @return Updated team_elos list with propagated adjustments
#' @keywords internal
propagate_elo_adjustment <- function(team_elos,
                                      winner,
                                      loser,
                                      delta,
                                      team_opponents,
                                      team_events,
                                      team_matchups,
                                      propagation_factor = PROPAGATION_FACTOR,
                                      correlation_threshold = CORRELATION_THRESHOLD,
                                      w_opponents = CORRELATION_W_OPPONENTS,
                                      w_events = CORRELATION_W_EVENTS,
                                      w_direct = CORRELATION_W_DIRECT) {

  all_team_ids <- names(team_elos)

  # Get correlations for winner and loser
  winner_correlations <- get_correlated_teams(
    winner, all_team_ids,
    team_opponents, team_events, team_matchups,
    correlation_threshold, w_opponents, w_events, w_direct
  )

  loser_correlations <- get_correlated_teams(
    loser, all_team_ids,
    team_opponents, team_events, team_matchups,
    correlation_threshold, w_opponents, w_events, w_direct
  )

  # Propagate to loser's correlates (they go DOWN)
  for (team in names(loser_correlations)) {
    if (team %in% c(winner, loser)) next
    corr <- loser_correlations[[team]]
    adjustment <- delta * corr * propagation_factor
    team_elos[[team]] <- team_elos[[team]] - adjustment
  }

  # Propagate to winner's correlates (they go UP)
  for (team in names(winner_correlations)) {
    if (team %in% c(winner, loser)) next
    corr <- winner_correlations[[team]]
    adjustment <- delta * corr * propagation_factor
    team_elos[[team]] <- team_elos[[team]] + adjustment
  }

  team_elos
}

# ============================================================================
# HISTORY TRACKING HELPERS
# ============================================================================

#' Update Team Match History
#'
#' Updates the history tracking lists after a match.
#'
#' @param team_id Team ID to update
#' @param opponent_id Opponent team ID
#' @param event_name Event/tournament name
#' @param team_opponents Current opponent history list
#' @param team_events Current event history list
#' @param team_matchups Current matchup counts list
#' @return List with updated team_opponents, team_events, team_matchups
#' @keywords internal
update_team_history <- function(team_id,
                                 opponent_id,
                                 event_name,
                                 team_opponents,
                                 team_events,
                                 team_matchups) {

  # Add opponent to history
  team_opponents[[team_id]] <- c(team_opponents[[team_id]], opponent_id)

  # Add event to history
  team_events[[team_id]] <- c(team_events[[team_id]], event_name)

  # Update matchup count
  if (is.null(team_matchups[[team_id]])) {
    team_matchups[[team_id]] <- list()
  }
  current_count <- team_matchups[[team_id]][[opponent_id]] %||% 0
  team_matchups[[team_id]][[opponent_id]] <- current_count + 1

  list(
    team_opponents = team_opponents,
    team_events = team_events,
    team_matchups = team_matchups
  )
}

# ============================================================================
# VECTORIZED CORRELATION MATRIX (for post-processing)
# ============================================================================

#' Build Full Correlation Matrix
#'
#' Efficiently builds a sparse correlation matrix for all teams at once.
#' Uses vectorized operations where possible.
#'
#' @param all_team_ids Vector of all team IDs
#' @param team_opponents List of opponent history
#' @param team_events List of event history
#' @param team_matchups List of matchup counts
#' @param threshold Minimum correlation to include (default 0.1)
#' @param w_opponents Weight for shared opponents
#' @param w_events Weight for same events
#' @param w_direct Weight for direct matches
#' @return Sparse matrix (list of lists) with correlations > threshold
#' @keywords internal
build_correlation_matrix <- function(all_team_ids,
                                      team_opponents,
                                      team_events,
                                      team_matchups,
                                      threshold = CORRELATION_THRESHOLD,
                                      w_opponents = CORRELATION_W_OPPONENTS,
                                      w_events = CORRELATION_W_EVENTS,
                                      w_direct = CORRELATION_W_DIRECT) {

  n_teams <- length(all_team_ids)
  cli::cli_alert_info("Building correlation matrix for {n_teams} teams...")

  # Pre-compute unique sets for each team (once)
  team_opp_sets <- lapply(all_team_ids, function(t) {
    unique(team_opponents[[t]] %||% character(0))
  })
  names(team_opp_sets) <- all_team_ids

  team_evt_sets <- lapply(all_team_ids, function(t) {
    unique(team_events[[t]] %||% character(0))
  })
  names(team_evt_sets) <- all_team_ids

  # Pre-compute total matches per team
  team_total_matches <- vapply(all_team_ids, function(t) {
    sum(unlist(team_matchups[[t]] %||% list()), na.rm = TRUE)
  }, numeric(1))
  names(team_total_matches) <- all_team_ids

  # Build sparse correlation matrix (only store > threshold)
  correlation_matrix <- list()

  cli::cli_progress_bar("Calculating correlations", total = n_teams)

  for (i in seq_len(n_teams)) {
    team_a <- all_team_ids[i]
    opp_a <- team_opp_sets[[team_a]]
    evt_a <- team_evt_sets[[team_a]]
    matchups_a <- team_matchups[[team_a]] %||% list()
    total_a <- team_total_matches[team_a]

    correlations <- list()

    for (j in seq_len(n_teams)) {
      if (i == j) next

      team_b <- all_team_ids[j]
      opp_b <- team_opp_sets[[team_b]]
      evt_b <- team_evt_sets[[team_b]]

      # Shared opponents (Jaccard)
      opp_intersect <- length(intersect(opp_a, opp_b))
      opp_union <- length(union(opp_a, opp_b))
      jaccard_opp <- if (opp_union > 0) opp_intersect / opp_union else 0

      # Same events (Jaccard)
      evt_intersect <- length(intersect(evt_a, evt_b))
      evt_union <- length(union(evt_a, evt_b))
      jaccard_evt <- if (evt_union > 0) evt_intersect / evt_union else 0

      # Direct matches
      matchups_b <- team_matchups[[team_b]] %||% list()
      direct_ab <- matchups_a[[team_b]] %||% 0
      direct_ba <- matchups_b[[team_a]] %||% 0
      total_b <- team_total_matches[team_b]
      max_matches <- max(total_a, total_b, 1)
      direct_prop <- (direct_ab + direct_ba) / max_matches

      # Weighted sum
      corr <- w_opponents * jaccard_opp +
              w_events * jaccard_evt +
              w_direct * direct_prop

      if (corr > threshold) {
        correlations[[team_b]] <- corr
      }
    }

    if (length(correlations) > 0) {
      correlation_matrix[[team_a]] <- correlations
    }

    cli::cli_progress_update(id = NULL)
  }

  cli::cli_progress_done()
  cli::cli_alert_success("Built correlation matrix with {sum(sapply(correlation_matrix, length))} non-zero entries")

  correlation_matrix
}

#' Apply Post-Processing Propagation
#'
#' Applies ELO propagation as a post-processing step using pre-computed
#' correlation matrix. This is much faster than per-match propagation.
#'
#' @param team_elos Named vector of current team ELOs
#' @param match_results Data frame with winner_id, loser_id, delta columns
#' @param correlation_matrix Pre-computed sparse correlation matrix
#' @param propagation_factor How much of delta to propagate (default 0.15)
#' @return Updated team_elos named vector
#' @keywords internal
apply_propagation_post <- function(team_elos,
                                    match_results,
                                    correlation_matrix,
                                    propagation_factor = PROPAGATION_FACTOR) {

  if (nrow(match_results) == 0 || propagation_factor == 0) {
    return(team_elos)
  }

  cli::cli_alert_info("Applying propagation to {nrow(match_results)} match results...")

  # Aggregate deltas per loser and winner
  loser_deltas <- list()
  winner_deltas <- list()

  for (i in seq_len(nrow(match_results))) {
    winner <- match_results$winner_id[i]
    loser <- match_results$loser_id[i]
    delta <- match_results$delta[i]

    loser_deltas[[loser]] <- (loser_deltas[[loser]] %||% 0) + delta
    winner_deltas[[winner]] <- (winner_deltas[[winner]] %||% 0) + delta
  }

  # Apply propagation from correlation matrix
  adjustment_count <- 0

  for (team in names(team_elos)) {
    if (team %in% names(correlation_matrix)) {
      correlates <- correlation_matrix[[team]]

      for (corr_team in names(correlates)) {
        corr <- correlates[[corr_team]]

        # If team lost, correlated teams go down
        if (team %in% names(loser_deltas)) {
          adj <- loser_deltas[[team]] * corr * propagation_factor
          if (corr_team %in% names(team_elos)) {
            team_elos[corr_team] <- team_elos[corr_team] - adj
            adjustment_count <- adjustment_count + 1
          }
        }

        # If team won, correlated teams go up
        if (team %in% names(winner_deltas)) {
          adj <- winner_deltas[[team]] * corr * propagation_factor
          if (corr_team %in% names(team_elos)) {
            team_elos[corr_team] <- team_elos[corr_team] + adj
            adjustment_count <- adjustment_count + 1
          }
        }
      }
    }
  }

  cli::cli_alert_success("Applied {adjustment_count} ELO adjustments via propagation")
  team_elos
}

#' Apply Yearly Propagation Normalization
#'
#' Groups matches by year and applies propagation at year boundaries.
#' This balances responsiveness (yearly updates) with efficiency.
#'
#' @param matches Data frame of all matches with match_date, winner_id, loser_id, delta
#' @param team_elos Named vector of current team ELOs
#' @param team_opponents List of opponent history
#' @param team_events List of event history
#' @param team_matchups List of matchup counts
#' @param propagation_factor How much of delta to propagate
#' @param correlation_threshold Min correlation to propagate
#' @param w_opponents Weight for shared opponents
#' @param w_events Weight for same events
#' @param w_direct Weight for direct matches
#' @return Updated team_elos named vector
#' @keywords internal
apply_yearly_propagation <- function(matches,
                                      team_elos,
                                      team_opponents,
                                      team_events,
                                      team_matchups,
                                      propagation_factor = PROPAGATION_FACTOR,
                                      correlation_threshold = CORRELATION_THRESHOLD,
                                      w_opponents = CORRELATION_W_OPPONENTS,
                                      w_events = CORRELATION_W_EVENTS,
                                      w_direct = CORRELATION_W_DIRECT) {

  # Extract year from match_date
  years <- unique(format(as.Date(matches$match_date), "%Y"))
  years <- sort(years)

  cli::cli_alert_info("Applying yearly propagation for {length(years)} years...")

  for (year in years) {
    year_matches <- matches[format(as.Date(matches$match_date), "%Y") == year, ]

    if (nrow(year_matches) == 0) next

    # Build correlation matrix for this year's teams
    year_teams <- unique(c(year_matches$winner_id, year_matches$loser_id))

    corr_matrix <- build_correlation_matrix(
      year_teams,
      team_opponents,
      team_events,
      team_matchups,
      correlation_threshold,
      w_opponents,
      w_events,
      w_direct
    )

    # Apply propagation for this year
    team_elos <- apply_propagation_post(
      team_elos,
      year_matches,
      corr_matrix,
      propagation_factor
    )

    cli::cli_alert_success("Year {year}: propagation applied")
  }

  team_elos
}

NULL
