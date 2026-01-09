# Player Correlation Functions for ELO Propagation
#
# Calculates correlations between players based on:
# - Shared opponents (who they play against)
# - Same events (tournaments they participate in)
# - Direct matches (head-to-head encounters)
#
# Used to propagate ELO adjustments to correlated players when
# cross-league matches reveal information about player strength.
#
# Adapted from team_correlation.R for player-level ELO.

# ============================================================================
# PLAYER CORRELATION CONSTANTS
# ============================================================================
# Constants are defined in constants.R:
#   PLAYER_CORR_W_OPPONENTS, PLAYER_CORR_W_EVENTS, PLAYER_CORR_W_DIRECT
#   PLAYER_CORR_THRESHOLD, PLAYER_PROPAGATION_FACTOR

# ============================================================================
# PLAYER CORRELATION CALCULATION
# ============================================================================

#' Calculate Correlation Between Two Players
#'
#' Computes a hybrid correlation score based on:
#' - Shared opponents (Jaccard similarity of opponent sets)
#' - Same events (Jaccard similarity of event sets)
#' - Direct matches (proportion of matches against each other)
#'
#' For batters, "opponents" means bowlers faced.
#' For bowlers, "opponents" means batters bowled to.
#'
#' @param player_a First player ID
#' @param player_b Second player ID
#' @param player_opponents List mapping player_id -> vector of opponent_ids
#' @param player_events List mapping player_id -> vector of event_names
#' @param player_matchups List mapping player_id -> list(opponent_id -> count)
#' @param w_opponents Weight for shared opponents (default 0.5)
#' @param w_events Weight for same events (default 0.3)
#' @param w_direct Weight for direct matches (default 0.2)
#' @return Numeric correlation score between 0 and 1
#' @keywords internal
calculate_player_correlation <- function(player_a, player_b,
                                          player_opponents,
                                          player_events,
                                          player_matchups,
                                          w_opponents = PLAYER_CORR_W_OPPONENTS,
                                          w_events = PLAYER_CORR_W_EVENTS,
                                          w_direct = PLAYER_CORR_W_DIRECT) {

  # Edge case: same player
  if (player_a == player_b) return(1)

  # Get opponent sets (use unique to avoid counting repeated opponents multiple times)
  opp_a <- unique(player_opponents[[player_a]] %||% character(0))
  opp_b <- unique(player_opponents[[player_b]] %||% character(0))

  # Shared opponents (Jaccard similarity)
  jaccard_opponents <- jaccard_similarity(opp_a, opp_b)

  # Get event sets (use unique)
  evt_a <- unique(player_events[[player_a]] %||% character(0))
  evt_b <- unique(player_events[[player_b]] %||% character(0))

  # Same events (Jaccard similarity)
  jaccard_events <- jaccard_similarity(evt_a, evt_b)

  # Direct matches proportion
  matchups_a <- player_matchups[[player_a]] %||% list()
  matchups_b <- player_matchups[[player_b]] %||% list()

  direct_ab <- matchups_a[[player_b]] %||% 0
  direct_ba <- matchups_b[[player_a]] %||% 0

  total_direct <- direct_ab + direct_ba

  # Total matchups for each player
  total_a <- sum(unlist(matchups_a), na.rm = TRUE)
  total_b <- sum(unlist(matchups_b), na.rm = TRUE)
  max_matchups <- max(total_a, total_b, 1)

  direct_prop <- total_direct / max_matchups

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

#' Get Correlated Players for a Target Player
#'
#' Efficiently computes correlations between one player and all other players.
#'
#' @param target_player Player ID to calculate correlations for
#' @param all_player_ids Vector of all player IDs
#' @param player_opponents List of opponent history
#' @param player_events List of event history
#' @param player_matchups List of matchup counts
#' @param threshold Minimum correlation to include (default 0.1)
#' @param w_opponents Weight for shared opponents
#' @param w_events Weight for same events
#' @param w_direct Weight for direct matches
#' @return Named numeric vector of correlations above threshold
#' @keywords internal
get_correlated_players <- function(target_player,
                                    all_player_ids,
                                    player_opponents,
                                    player_events,
                                    player_matchups,
                                    threshold = PLAYER_CORR_THRESHOLD,
                                    w_opponents = PLAYER_CORR_W_OPPONENTS,
                                    w_events = PLAYER_CORR_W_EVENTS,
                                    w_direct = PLAYER_CORR_W_DIRECT) {

  correlations <- vapply(all_player_ids, function(other_player) {
    if (other_player == target_player) return(0)

    calculate_player_correlation(
      target_player, other_player,
      player_opponents, player_events, player_matchups,
      w_opponents, w_events, w_direct
    )
  }, numeric(1))

  names(correlations) <- all_player_ids

  # Filter to only those above threshold
  correlations[correlations > threshold]
}

# ============================================================================
# VECTORIZED CORRELATION MATRIX (for post-processing)
# ============================================================================

#' Build Player Correlation Matrix
#'
#' Efficiently builds a sparse correlation matrix for all players at once.
#' Uses vectorized operations where possible.
#'
#' @param all_player_ids Vector of all player IDs
#' @param player_opponents List of opponent history
#' @param player_events List of event history
#' @param player_matchups List of matchup counts
#' @param threshold Minimum correlation to include (default 0.1)
#' @param w_opponents Weight for shared opponents
#' @param w_events Weight for same events
#' @param w_direct Weight for direct matches
#' @return Sparse matrix (list of lists) with correlations > threshold
#' @keywords internal
build_player_correlation_matrix <- function(all_player_ids,
                                             player_opponents,
                                             player_events,
                                             player_matchups,
                                             threshold = PLAYER_CORR_THRESHOLD,
                                             w_opponents = PLAYER_CORR_W_OPPONENTS,
                                             w_events = PLAYER_CORR_W_EVENTS,
                                             w_direct = PLAYER_CORR_W_DIRECT) {

  n_players <- length(all_player_ids)
  cli::cli_alert_info("Building player correlation matrix for {n_players} players...")

  # Pre-compute unique sets for each player (once)
  player_opp_sets <- lapply(all_player_ids, function(p) {
    unique(player_opponents[[p]] %||% character(0))
  })
  names(player_opp_sets) <- all_player_ids

  player_evt_sets <- lapply(all_player_ids, function(p) {
    unique(player_events[[p]] %||% character(0))
  })
  names(player_evt_sets) <- all_player_ids

  # Pre-compute total matchups per player
  player_total_matchups <- vapply(all_player_ids, function(p) {
    sum(unlist(player_matchups[[p]] %||% list()), na.rm = TRUE)
  }, numeric(1))
  names(player_total_matchups) <- all_player_ids

  # Build sparse correlation matrix (only store > threshold)
  correlation_matrix <- list()

  cli::cli_progress_bar("Calculating player correlations", total = n_players)

  for (i in seq_len(n_players)) {
    player_a <- all_player_ids[i]
    opp_a <- player_opp_sets[[player_a]]
    evt_a <- player_evt_sets[[player_a]]
    matchups_a <- player_matchups[[player_a]] %||% list()
    total_a <- player_total_matchups[player_a]

    correlations <- list()

    for (j in seq_len(n_players)) {
      if (i == j) next

      player_b <- all_player_ids[j]
      opp_b <- player_opp_sets[[player_b]]
      evt_b <- player_evt_sets[[player_b]]

      # Shared opponents (Jaccard)
      opp_intersect <- length(intersect(opp_a, opp_b))
      opp_union <- length(union(opp_a, opp_b))
      jaccard_opp <- if (opp_union > 0) opp_intersect / opp_union else 0

      # Same events (Jaccard)
      evt_intersect <- length(intersect(evt_a, evt_b))
      evt_union <- length(union(evt_a, evt_b))
      jaccard_evt <- if (evt_union > 0) evt_intersect / evt_union else 0

      # Direct matches
      matchups_b <- player_matchups[[player_b]] %||% list()
      direct_ab <- matchups_a[[player_b]] %||% 0
      direct_ba <- matchups_b[[player_a]] %||% 0
      total_b <- player_total_matchups[player_b]
      max_matchups <- max(total_a, total_b, 1)
      direct_prop <- (direct_ab + direct_ba) / max_matchups

      # Weighted sum
      corr <- w_opponents * jaccard_opp +
              w_events * jaccard_evt +
              w_direct * direct_prop

      if (corr > threshold) {
        correlations[[player_b]] <- corr
      }
    }

    if (length(correlations) > 0) {
      correlation_matrix[[player_a]] <- correlations
    }

    cli::cli_progress_update(id = NULL)
  }

  cli::cli_progress_done()
  cli::cli_alert_success("Built player correlation matrix with {sum(sapply(correlation_matrix, length))} non-zero entries")

  correlation_matrix
}

# ============================================================================
# PLAYER HISTORY TRACKING HELPERS
# ============================================================================

#' Update Player Match History
#'
#' Updates the history tracking lists after a delivery.
#'
#' @param player_id Player ID to update (batter or bowler)
#' @param opponent_id Opponent player ID (bowler if player is batter, vice versa)
#' @param event_name Event/tournament name
#' @param player_opponents Current opponent history list
#' @param player_events Current event history list
#' @param player_matchups Current matchup counts list
#' @return List with updated player_opponents, player_events, player_matchups
#' @keywords internal
update_player_history <- function(player_id,
                                   opponent_id,
                                   event_name,
                                   player_opponents,
                                   player_events,
                                   player_matchups) {

  # Add opponent to history
  player_opponents[[player_id]] <- c(player_opponents[[player_id]], opponent_id)

  # Add event to history
  player_events[[player_id]] <- c(player_events[[player_id]], event_name)

  # Update matchup count
  if (is.null(player_matchups[[player_id]])) {
    player_matchups[[player_id]] <- list()
  }
  current_count <- player_matchups[[player_id]][[opponent_id]] %||% 0
  player_matchups[[player_id]][[opponent_id]] <- current_count + 1

  list(
    player_opponents = player_opponents,
    player_events = player_events,
    player_matchups = player_matchups
  )
}

# ============================================================================
# ELO PROPAGATION FOR PLAYERS
# ============================================================================

#' Apply Player ELO Propagation Post-Processing
#'
#' Applies ELO propagation as a post-processing step using pre-computed
#' correlation matrix. This anchors isolated player pools to the main pool.
#'
#' For run ELO:
#' - When a batter scores (wins the delivery), correlated batters adjust up
#' - When a bowler concedes runs (loses), correlated bowlers adjust down
#'
#' For wicket ELO:
#' - When a batter survives (wins), correlated batters adjust up
#' - When a bowler gets a wicket (wins), correlated bowlers adjust up
#'
#' @param player_elos Named list with run_elo and wicket_elo for each player
#' @param delivery_results Data frame with batter_id, bowler_id, run_update, wicket_update
#' @param correlation_matrix Pre-computed sparse correlation matrix
#' @param propagation_factor How much of update to propagate (default 0.15)
#' @param elo_type "run" or "wicket" to select which ELO to propagate
#' @return Updated player_elos list
#' @keywords internal
apply_player_propagation <- function(player_elos,
                                      delivery_results,
                                      correlation_matrix,
                                      propagation_factor = PLAYER_PROPAGATION_FACTOR,
                                      elo_type = "run") {

  if (nrow(delivery_results) == 0 || propagation_factor == 0) {
    return(player_elos)
  }

  cli::cli_alert_info("Applying {elo_type} ELO propagation to {nrow(delivery_results)} deliveries...")

  # Aggregate updates per player
  # For run ELO: batter gains when scoring, bowler loses
  # For wicket ELO: batter loses when out, bowler gains
  batter_updates <- list()
  bowler_updates <- list()

  update_col <- if (elo_type == "run") "run_update_batter" else "wicket_update_batter"
  update_col_bowler <- if (elo_type == "run") "run_update_bowler" else "wicket_update_bowler"

  for (i in seq_len(nrow(delivery_results))) {
    batter <- delivery_results$batter_id[i]
    bowler <- delivery_results$bowler_id[i]
    batter_upd <- delivery_results[[update_col]][i]
    bowler_upd <- delivery_results[[update_col_bowler]][i]

    batter_updates[[batter]] <- (batter_updates[[batter]] %||% 0) + batter_upd
    bowler_updates[[bowler]] <- (bowler_updates[[bowler]] %||% 0) + bowler_upd
  }

  # Apply propagation from correlation matrix
  adjustment_count <- 0
  elo_field <- if (elo_type == "run") "run_elo" else "wicket_elo"

  for (player in names(player_elos)) {
    if (player %in% names(correlation_matrix)) {
      correlates <- correlation_matrix[[player]]

      for (corr_player in names(correlates)) {
        corr <- correlates[[corr_player]]

        # If player had net positive update, correlated players go up
        total_update <- (batter_updates[[player]] %||% 0) + (bowler_updates[[player]] %||% 0)

        if (abs(total_update) > 0.01 && corr_player %in% names(player_elos)) {
          adj <- total_update * corr * propagation_factor
          player_elos[[corr_player]][[elo_field]] <- player_elos[[corr_player]][[elo_field]] + adj
          adjustment_count <- adjustment_count + 1
        }
      }
    }
  }

  cli::cli_alert_success("Applied {adjustment_count} {elo_type} ELO adjustments via propagation")
  player_elos
}


#' Apply Batch Propagation to Player ELOs
#'
#' High-level function that applies propagation for both run and wicket ELO.
#' Typically called after initial ELO calculation is complete.
#'
#' @param player_elos Named list with run_elo and wicket_elo for each player
#' @param delivery_results Data frame with delivery-level ELO updates
#' @param player_opponents List of opponent history
#' @param player_events List of event history
#' @param player_matchups List of matchup counts
#' @param propagation_factor How much of update to propagate (default 0.15)
#' @param correlation_threshold Min correlation to propagate (default 0.1)
#' @return Updated player_elos list
#' @export
apply_player_elo_propagation <- function(player_elos,
                                          delivery_results,
                                          player_opponents,
                                          player_events,
                                          player_matchups,
                                          propagation_factor = PLAYER_PROPAGATION_FACTOR,
                                          correlation_threshold = PLAYER_CORR_THRESHOLD) {

  all_player_ids <- names(player_elos)

  cli::cli_h3("Building player correlation matrix")
  corr_matrix <- build_player_correlation_matrix(
    all_player_ids,
    player_opponents,
    player_events,
    player_matchups,
    threshold = correlation_threshold
  )

  cli::cli_h3("Applying Run ELO propagation")
  player_elos <- apply_player_propagation(
    player_elos,
    delivery_results,
    corr_matrix,
    propagation_factor,
    elo_type = "run"
  )

  cli::cli_h3("Applying Wicket ELO propagation")
  player_elos <- apply_player_propagation(
    player_elos,
    delivery_results,
    corr_matrix,
    propagation_factor,
    elo_type = "wicket"
  )

  player_elos
}


# ============================================================================
# TEAM-ANCHORED PROPAGATION (Uses Team ELO to Anchor Player ELO)
# ============================================================================

#' Apply Team-Anchored Propagation to Player ELOs
#'
#' Uses team ELO as an anchor for player ELOs. When a team's average player ELO
#' diverges too far from the team's actual ELO (from team_elo table), this function
#' pulls players back toward the team-implied level.
#'
#' This addresses the isolated pool problem: if NZ domestic teams have team ELO
#' of 1350 but their players average 1600, the players are likely inflated and
#' should be pulled down.
#'
#' @param player_elos Named list with run_elo and wicket_elo for each player.
#' @param team_elos Named numeric vector. Team ELOs keyed by team name.
#' @param player_teams Named character vector. Maps player_id to their most recent team.
#' @param propagation_factor Numeric. How much to adjust (default 0.2).
#'   An adjustment of gap * 0.2 means 20% of the gap is corrected.
#' @param min_players Integer. Minimum players per team to apply (default 3).
#' @param gap_threshold Numeric. Minimum gap to trigger adjustment (default 50).
#' @param elo_type Character. "run" or "wicket" to select which ELO to adjust.
#' @return Updated player_elos list.
#' @export
#'
#' @examples
#' # If NZ domestic team has team_elo = 1350 and avg player ELO = 1600
#' # Gap = 250, adjustment = -250 * 0.2 = -50 points per player
apply_team_anchored_propagation <- function(player_elos,
                                             team_elos,
                                             player_teams,
                                             propagation_factor = 0.2,
                                             min_players = 3,
                                             gap_threshold = 50,
                                             elo_type = "run") {

  if (length(team_elos) == 0 || length(player_teams) == 0) {
    cli::cli_alert_warning("No team ELOs or player-team mappings - skipping team-anchored propagation")
    return(player_elos)
  }

  elo_field <- if (elo_type == "run") "run_elo" else "wicket_elo"
  adjustment_count <- 0
  total_adjustment <- 0

  cli::cli_alert_info("Applying team-anchored {elo_type} ELO propagation...")

  # For each team with a known ELO
  for (team in names(team_elos)) {
    team_elo <- team_elos[[team]]

    if (is.na(team_elo)) next

    # Get players on this team
    team_player_ids <- names(player_teams)[player_teams == team]

    # Filter to players that exist in player_elos
    team_player_ids <- team_player_ids[team_player_ids %in% names(player_elos)]

    if (length(team_player_ids) < min_players) next

    # Calculate average player ELO for this team
    player_elos_for_team <- sapply(team_player_ids, function(p) {
      player_elos[[p]][[elo_field]]
    })
    avg_player_elo <- mean(player_elos_for_team, na.rm = TRUE)

    # Calculate gap from team ELO
    gap <- avg_player_elo - team_elo

    # Only adjust if gap exceeds threshold
    if (abs(gap) > gap_threshold) {
      # Adjustment pulls players toward team ELO level
      adjustment <- -gap * propagation_factor

      for (p in team_player_ids) {
        player_elos[[p]][[elo_field]] <- player_elos[[p]][[elo_field]] + adjustment
        adjustment_count <- adjustment_count + 1
        total_adjustment <- total_adjustment + abs(adjustment)
      }
    }
  }

  if (adjustment_count > 0) {
    avg_adj <- total_adjustment / adjustment_count
    cli::cli_alert_success("Applied {adjustment_count} {elo_type} ELO adjustments (avg: {round(avg_adj, 1)} pts)")
  } else {
    cli::cli_alert_info("No team-anchored adjustments needed for {elo_type} ELO")
  }

  player_elos
}


#' Apply Full Team-Anchored Propagation (Both ELO Types)
#'
#' Applies team-anchored propagation to both run ELO and wicket ELO.
#'
#' @param player_elos Named list with run_elo and wicket_elo for each player.
#' @param team_elos Named numeric vector. Team ELOs keyed by team name.
#' @param player_teams Named character vector. Maps player_id to team.
#' @param propagation_factor Numeric. How much to adjust (default 0.2).
#' @return Updated player_elos list.
#' @export
apply_full_team_anchored_propagation <- function(player_elos,
                                                  team_elos,
                                                  player_teams,
                                                  propagation_factor = 0.2) {

  cli::cli_h3("Applying team-anchored propagation")

  # Run ELO
  player_elos <- apply_team_anchored_propagation(
    player_elos, team_elos, player_teams,
    propagation_factor = propagation_factor,
    elo_type = "run"
  )

  # Wicket ELO
  player_elos <- apply_team_anchored_propagation(
    player_elos, team_elos, player_teams,
    propagation_factor = propagation_factor,
    elo_type = "wicket"
  )

  player_elos
}

NULL
