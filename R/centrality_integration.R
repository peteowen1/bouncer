# Player Network Centrality - ELO Integration
#
# Functions for integrating network centrality with ELO rating systems.
# Includes K-factor multipliers, league-based starting ELO, and regression.
#
# Split from player_centrality.R for better maintainability.

# ============================================================================
# COLD START HANDLING
# ============================================================================

#' Get Cold Start Percentile
#'
#' Returns a default PageRank percentile for players with no snapshot history.
#' The default is based on the event tier where the player debuts - higher tiers
#' suggest the player is likely better quality.
#'
#' @param event_tier Integer. Event tier (1-4). Use get_event_tier() to determine.
#'
#' @return Numeric. Default percentile (30-60 based on tier).
#' @export
get_cold_start_percentile <- function(event_tier) {
  switch(as.character(event_tier),
    "1" = CENTRALITY_COLD_START_TIER_1,  # 60 - IPL/BBL debut
    "2" = CENTRALITY_COLD_START_TIER_2,  # 50 - Strong domestic
    "3" = CENTRALITY_COLD_START_TIER_3,  # 40 - Regional
    "4" = CENTRALITY_COLD_START_TIER_4,  # 30 - Development
    50  # Default fallback
  )
}


# ============================================================================
# K-FACTOR MULTIPLIERS
# ============================================================================

#' Get Centrality K-Factor Multiplier
#'
#' Calculates the K-factor multiplier based on opponent's centrality percentile.
#' Uses a sigmoid function to smoothly transition between floor and ceiling.
#'
#' This implements Option A (Preventive): learn more from elite opponents,
#' less from weak ones.
#'
#' @param opponent_percentile Numeric. Opponent's centrality percentile (0-100).
#'   Use get_centrality_as_of() or get_cold_start_percentile() to obtain.
#'
#' @return Numeric. K-factor multiplier (CENTRALITY_K_FLOOR to CENTRALITY_K_CEILING).
#' @export
#'
#' @examples
#' get_centrality_k_multiplier(99)  # ~1.5 for elite opponent
#' get_centrality_k_multiplier(50)  # ~1.0 for average opponent
#' get_centrality_k_multiplier(10)  # ~0.5 for weak opponent
get_centrality_k_multiplier <- function(opponent_percentile) {
  # Handle NA/NULL
  if (is.null(opponent_percentile) || is.na(opponent_percentile)) {
    return(1.0)  # Neutral multiplier
  }

  # Sigmoid scaling between floor and ceiling
  # Formula: floor + (ceiling - floor) / (1 + exp(-steepness * (percentile - midpoint)))
  range_size <- CENTRALITY_K_CEILING - CENTRALITY_K_FLOOR
  sigmoid_arg <- -CENTRALITY_K_STEEPNESS * (opponent_percentile - CENTRALITY_K_MIDPOINT)

  CENTRALITY_K_FLOOR + range_size / (1 + exp(sigmoid_arg))
}


# ============================================================================
# LEAGUE-BASED STARTING ELO
# ============================================================================

#' Calculate League-Based Starting ELO
#'
#' Calculates a player's starting ELO based on the average centrality of players
#' in the league/event where they debut. Players debuting in isolated leagues
#' (low centrality) start with lower ELO, preventing inflation.
#'
#' Formula: starting_elo = ELO_START + (league_avg_centrality - 50) * elo_per_percentile
#'
#' @param league_avg_centrality Numeric. Average centrality percentile of players
#'   in the debut league (0-100). If NULL or NA, uses default starting ELO.
#' @param elo_start Numeric. Base starting ELO. Default uses THREE_WAY_ELO_START (1400).
#' @param elo_per_percentile Numeric. ELO points per percentile point.
#'   Default uses CENTRALITY_ELO_PER_PERCENTILE (4).
#'
#' @return Numeric. The league-adjusted starting ELO.
#'
#' @examples
#' # IPL debut (high centrality league ~78%)
#' calculate_league_starting_elo(78)  # ~1512
#'
#' # Central American league debut (low centrality ~4%)
#' calculate_league_starting_elo(4)   # ~1216
#'
#' # International debut (very high centrality ~95%)
#' calculate_league_starting_elo(95)  # ~1580
#'
#' @export
calculate_league_starting_elo <- function(league_avg_centrality,
                                           elo_start = THREE_WAY_ELO_START,
                                           elo_per_percentile = CENTRALITY_ELO_PER_PERCENTILE) {
  # Handle NULL/NA - use default starting ELO
  if (is.null(league_avg_centrality) || is.na(league_avg_centrality)) {
    return(elo_start)
  }

  # Clamp to valid range
  league_avg_centrality <- max(0, min(100, league_avg_centrality))

  # Calculate adjusted starting ELO
  # 50th percentile league → start at elo_start (neutral)
  # Higher centrality → start higher
  # Lower centrality → start lower
  elo_start + (league_avg_centrality - 50) * elo_per_percentile
}


# ============================================================================
# CENTRALITY-BASED ELO REGRESSION
# ============================================================================

#' Calculate Centrality-Based ELO Regression
#'
#' Applies a continuous "gravity" pull toward the player's centrality-implied ELO.
#' This creates a stronger Bayesian prior that prevents low-centrality players from
#' accumulating inflated ratings by dominating weak opponents in isolated ecosystems.
#'
#' The regression is proportional to the gap between current ELO and implied ELO:
#'   correction = regression_strength × (implied_elo - current_elo)
#'
#' This means:
#' - Players far above their implied ELO are pulled down more strongly
#' - Players near their implied ELO experience minimal correction
#' - Elite players (high centrality) have higher implied ELOs, so their high ratings
#'   are "justified" by their network position
#'
#' @param current_elo Numeric. The player's current ELO rating after normal update.
#' @param player_centrality_percentile Numeric. The player's centrality percentile (0-100).
#'   If NULL or NA, no regression is applied.
#' @param elo_start Numeric. Base ELO (default THREE_WAY_ELO_START = 1400).
#' @param elo_per_percentile Numeric. ELO points per percentile point (default 4).
#' @param regression_strength Numeric. Pull strength per delivery (default 0.002).
#'
#' @return Numeric. The ELO correction to ADD to current_elo (can be negative).
#'
#' @examples
#' # Low centrality player with inflated ELO
#' calculate_centrality_regression(2400, 5)   # ~ -2.36 (strong pull down)
#'
#' # Elite player with high ELO
#' calculate_centrality_regression(2000, 95)  # ~ -0.72 (small pull down, justified)
#'
#' # Average player at average ELO
#' calculate_centrality_regression(1400, 50)  # 0 (no correction needed)
#'
#' @export
calculate_centrality_regression <- function(current_elo,
                                             player_centrality_percentile,
                                             elo_start = THREE_WAY_ELO_START,
                                             elo_per_percentile = CENTRALITY_ELO_PER_PERCENTILE,
                                             regression_strength = CENTRALITY_REGRESSION_STRENGTH) {
  # No correction if centrality unknown
  if (is.null(player_centrality_percentile) || is.na(player_centrality_percentile)) {
    return(0)
  }

  # Clamp to valid range
  player_centrality_percentile <- max(0, min(100, player_centrality_percentile))

  # Calculate centrality-implied ELO
  # Same formula as league_starting_elo: higher centrality = higher implied ELO
  implied_elo <- elo_start + (player_centrality_percentile - 50) * elo_per_percentile

  # Calculate correction (positive if current < implied, negative if current > implied)
  correction <- regression_strength * (implied_elo - current_elo)

  correction
}


# ============================================================================
# EVENT/LEAGUE CENTRALITY LOOKUP
# ============================================================================

#' Build Event Centrality Lookup Table
#'
#' Pre-computes average centrality for each event/league based on the players
#' who have participated in it. This is used to determine league-based
#' starting ELOs for new players.
#'
#' @param conn DBI connection to the database.
#' @param format Character. Match format: "t20", "odi", or "test".
#' @param gender Character. Gender: "mens" or "womens".
#' @param min_players Integer. Minimum players in an event to include it.
#'   Default 10.
#'
#' @return Named list (event_name -> avg_centrality) for quick lookup.
#'
#' @examples
#' \dontrun{
#' conn <- get_db_connection(read_only = TRUE)
#' event_centrality <- build_event_centrality_lookup(conn, "t20", "mens")
#' event_centrality[["Indian Premier League"]]  # ~78
#' }
#'
#' @export
build_event_centrality_lookup <- function(conn, format, gender, min_players = 10) {
  format <- tolower(format)
  gender <- tolower(gender)

  # Build centrality table name
  centrality_table <- paste0(gender, "_", format, "_player_centrality_history")

  # Check if centrality table exists
  if (!centrality_table %in% DBI::dbListTables(conn)) {
    cli::cli_alert_warning("Centrality table {centrality_table} not found, using default starting ELOs")
    return(list())
  }

  # Determine match types for this format
  match_types <- get_match_types_for_format(format)
  match_types_sql <- paste0("'", match_types, "'", collapse = ", ")

  # Get average centrality by event
  query <- sprintf("
    WITH event_players AS (
      SELECT DISTINCT m.event_name, d.batter_id as player_id
      FROM deliveries d
      JOIN matches m ON d.match_id = m.match_id
      WHERE m.match_type IN (%s)
        AND m.gender = '%s'
        AND m.event_name IS NOT NULL
      UNION
      SELECT DISTINCT m.event_name, d.bowler_id as player_id
      FROM deliveries d
      JOIN matches m ON d.match_id = m.match_id
      WHERE m.match_type IN (%s)
        AND m.gender = '%s'
        AND m.event_name IS NOT NULL
    ),
    latest_centrality AS (
      SELECT player_id, AVG(percentile) as percentile
      FROM %s
      WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM %s)
      GROUP BY player_id
    )
    SELECT
      ep.event_name,
      AVG(lc.percentile) as avg_centrality
    FROM event_players ep
    LEFT JOIN latest_centrality lc ON ep.player_id = lc.player_id
    GROUP BY ep.event_name
    HAVING COUNT(*) >= %d AND AVG(lc.percentile) IS NOT NULL
  ", match_types_sql, ifelse(gender == "mens", "male", "female"),
     match_types_sql, ifelse(gender == "mens", "male", "female"),
     centrality_table, centrality_table, min_players)

  result <- tryCatch(
    DBI::dbGetQuery(conn, query),
    error = function(e) {
      cli::cli_alert_warning("Failed to build event centrality lookup: {e$message}")
      return(data.frame(event_name = character(0), avg_centrality = numeric(0)))
    }
  )

  if (nrow(result) == 0) {
    return(list())
  }

  # Convert to named list for fast lookup
  event_lookup <- as.list(result$avg_centrality)
  names(event_lookup) <- result$event_name

  cli::cli_alert_success("Built centrality lookup for {length(event_lookup)} events")
  event_lookup
}


# ============================================================================
# PLAYER DEBUT EVENT TRACKING
# ============================================================================

#' Get Player Debut Event
#'
#' Finds the first event/league where a player appeared, used to determine
#' their league-based starting ELO.
#'
#' @param player_id Character. The player's ID.
#' @param deliveries_dt Data.table. The deliveries data with event_name column.
#' @param role Character. "batter" or "bowler" to determine which column to check.
#'
#' @return Character. The event name, or NA if not found.
#' @keywords internal
get_player_debut_event <- function(player_id, deliveries_dt, role = "batter") {
  if (role == "batter") {
    idx <- which(deliveries_dt$batter_id == player_id)[1]
  } else {
    idx <- which(deliveries_dt$bowler_id == player_id)[1]
  }

  if (is.na(idx)) return(NA_character_)

  deliveries_dt$event_name[idx]
}


#' Batch Get Player Debut Events
#'
#' Efficiently finds the debut event for multiple players at once.
#' Much faster than calling get_player_debut_event in a loop.
#'
#' @param player_ids Character vector. Player IDs to look up.
#' @param deliveries_dt Data.table. The deliveries data (must have event_name).
#' @param role Character. "batter" or "bowler".
#'
#' @return Named character vector (player_id -> event_name).
#' @export
batch_get_player_debut_events <- function(player_ids, deliveries_dt, role = "batter") {
  if (!requireNamespace("data.table", quietly = TRUE)) {
    stop("data.table package required for batch_get_player_debut_events")
  }

  # Ensure data.table
  if (!data.table::is.data.table(deliveries_dt)) {
    deliveries_dt <- data.table::as.data.table(deliveries_dt)
  }

  id_col <- if (role == "batter") "batter_id" else "bowler_id"

  # Get first occurrence of each player (data is chronologically sorted)
  first_appearances <- deliveries_dt[, .SD[1], by = id_col, .SDcols = "event_name"]

  # Create lookup
  result <- first_appearances$event_name
  names(result) <- first_appearances[[id_col]]

  # Return in requested order, with NA for missing
  result[player_ids]
}

