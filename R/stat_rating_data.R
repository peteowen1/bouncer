# Player Stat Rating Data Preparation
# ====================================
# Role assignment, derived column computation, and data shaping
# for the stat rating estimation pipeline.
#
# Rate stats use Gamma-Poisson (counts per ball faced/bowled).
# Efficiency stats use Beta-Binomial (proportions).
#
# Output columns use `_rating` suffix (e.g. `batting_runs_rating`).


# ============================================================================
# Role assignment
# ============================================================================

#' Resolve Cricket Player Roles
#'
#' Assigns role groups based on career batting/bowling balance within the data.
#' Uses the proportion of matches where a player bowled vs batted to classify.
#'
#' @param dt data.table with player_id, batting_balls_faced, bowling_balls_bowled columns.
#'   Must contain multiple matches per player for reliable classification.
#'
#' @return The data.table with added \code{role_group} column:
#'   "BATTER", "BOWLER", "ALL_ROUNDER", or "WICKETKEEPER".
#' @keywords internal
.resolve_cricket_roles <- function(dt) {
  # Compute career batting/bowling balance per player
  career <- dt[, .(
    total_batting_balls = sum(batting_balls_faced, na.rm = TRUE),
    total_bowling_balls = sum(bowling_balls_bowled, na.rm = TRUE),
    matches_batted = sum(batting_balls_faced > 0, na.rm = TRUE),
    matches_bowled = sum(bowling_balls_bowled > 0, na.rm = TRUE),
    total_matches = .N
  ), by = player_id]

  # Classify based on balance

  career[, role_group := data.table::fcase(
    # ALL_ROUNDER: bowled in >= 30% AND batted in >= 30% of matches
    matches_bowled / total_matches >= 0.3 & matches_batted / total_matches >= 0.3,
    "ALL_ROUNDER",

    # BOWLER: bowled in >= 30% of matches but batted rarely or low order
    matches_bowled / total_matches >= 0.3,
    "BOWLER",

    # BATTER: primarily batted
    default = "BATTER"
  )]

  # Join back to main data
  dt[career, on = "player_id", role_group := i.role_group]

  # Fill any remaining NAs

  dt[is.na(role_group), role_group := "BATTER"]

  dt
}


# ============================================================================
# Data preparation
# ============================================================================

#' Prepare Data for Stat Rating Estimation
#'
#' Takes player game data and computes all derived columns needed by the
#' stat rating estimation engine: efficiency stat successes/failures,
#' role groups, and match dates.
#'
#' @param player_game_data data.table from \code{\link{load_player_game_data}}
#'   or \code{\link{create_player_game_data}}.
#'
#' @return data.table with one row per player-match, augmented with:
#'   role_group, match_date_rating (Date), and all derived columns
#'   referenced by \code{\link{stat_rating_definitions}}.
#'
#' @export
prepare_stat_rating_data <- function(player_game_data) {
  dt <- data.table::as.data.table(player_game_data)

  if (nrow(dt) == 0) {
    cli::cli_abort("player_game_data is empty")
  }

  # Ensure match_date is Date type
  if (!inherits(dt$match_date, "Date")) {
    dt[, match_date_rating := as.Date(match_date)]
  } else {
    dt[, match_date_rating := match_date]
  }

  # --- Assign role groups ---
  .resolve_cricket_roles(dt)

  # --- Compute derived columns for efficiency stats ---

  # Batting survival: balls faced without being dismissed
  dt[, batting_balls_survived := batting_balls_faced - batting_dismissed]

  # Hawkeye-derived counts (convert proportions back to counts for Beta-Binomial)
  dt[, batting_controlled_balls := round(batting_pct_controlled * batting_hawkeye_balls)]
  dt[, batting_attacking_balls := round(batting_pct_attacking * batting_hawkeye_balls)]
  dt[batting_hawkeye_balls == 0, c("batting_controlled_balls", "batting_attacking_balls") := 0L]

  dt[, bowling_good_length_balls := round(bowling_pct_good_length * bowling_hawkeye_balls)]
  dt[, bowling_on_stump_balls := round(bowling_pct_on_stump * bowling_hawkeye_balls)]
  dt[, bowling_beat_bat_balls := round(bowling_pct_beat_bat * bowling_hawkeye_balls)]
  dt[bowling_hawkeye_balls == 0, c("bowling_good_length_balls", "bowling_on_stump_balls", "bowling_beat_bat_balls") := 0L]

  # Handle NAs in Hawkeye columns (some matches have no Hawkeye data)
  hawkeye_cols <- c("batting_controlled_balls", "batting_attacking_balls",
                    "bowling_good_length_balls", "bowling_on_stump_balls",
                    "bowling_beat_bat_balls")
  for (col in hawkeye_cols) {
    data.table::set(dt, which(is.na(dt[[col]])), col, 0L)
  }

  # Sort chronologically (critical for Bayesian updating)
  data.table::setorder(dt, match_date_rating, match_id)

  n_players <- data.table::uniqueN(dt$player_id)
  n_matches <- data.table::uniqueN(dt$match_id)
  cli::cli_alert_success(
    "Prepared stat rating data: {n_players} players, {n_matches} matches, roles: {paste(names(table(dt$role_group)), collapse='/')}"
  )

  dt
}


#' Compute Role-Specific Prior Means
#'
#' Computes the weighted average of each stat within each role group,
#' used as the prior mean for Bayesian shrinkage.
#'
#' @param stat_data data.table from \code{\link{prepare_stat_rating_data}}.
#' @param stat_defs data.frame from \code{\link{stat_rating_definitions}}.
#'
#' @return Named list of data.tables, one per role group, with columns
#'   stat_name and mu (prior mean).
#'
#' @keywords internal
.compute_role_priors <- function(stat_data, stat_defs) {
  role_groups <- unique(stat_data$role_group)
  result <- list()

  for (rg in role_groups) {
    rg_data <- stat_data[role_group == rg]
    priors <- data.table::data.table(stat_name = stat_defs$stat_name, mu = NA_real_)

    for (i in seq_len(nrow(stat_defs))) {
      sname <- stat_defs$stat_name[i]
      stype <- stat_defs$type[i]

      if (stype == "rate") {
        src <- stat_defs$source_col[i]
        exp <- stat_defs$exposure_col[i]
        if (src %in% names(rg_data) && exp %in% names(rg_data)) {
          total_stat <- sum(rg_data[[src]], na.rm = TRUE)
          total_exp <- sum(rg_data[[exp]], na.rm = TRUE)
          priors[stat_name == sname, mu := total_stat / max(total_exp, 1)]
        }
      } else if (stype == "efficiency") {
        succ <- stat_defs$success_col[i]
        att <- stat_defs$attempts_col[i]
        if (succ %in% names(rg_data) && att %in% names(rg_data)) {
          total_succ <- sum(rg_data[[succ]], na.rm = TRUE)
          total_att <- sum(rg_data[[att]], na.rm = TRUE)
          priors[stat_name == sname, mu := total_succ / max(total_att, 1)]
        }
      }
    }

    result[[rg]] <- priors
  }

  result
}
