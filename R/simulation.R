# Simulation Framework
#
# Core infrastructure for cricket match and season simulations.
# Provides configuration, random seed management, result aggregation,
# and database storage utilities.

# Configuration ============================================================

#' Create Simulation Configuration
#'
#' Creates a configuration object for simulations.
#'
#' @param simulation_type Character. Type: "match", "season", "playoffs"
#' @param event_name Character. Event to simulate (e.g., "Indian Premier League")
#' @param season Character. Season to simulate (e.g., "2024")
#' @param n_simulations Integer. Number of Monte Carlo iterations (default 10000)
#' @param random_seed Integer. Random seed for reproducibility (default 42)
#' @param use_model Logical. Use trained model vs simple ELO (default TRUE)
#' @param model_version Character. Model version to use (default "v1.0")
#'
#' @return List with simulation configuration
#' @export
create_simulation_config <- function(
    simulation_type = "season",
    event_name = "Indian Premier League",
    season = "2024",
    n_simulations = 10000,
    random_seed = 42,
    use_model = TRUE,
    model_version = "v1.0"
) {
  config <- list(
    simulation_id = paste0(
      simulation_type, "_",
      gsub(" ", "_", tolower(event_name)), "_",
      season, "_",
      format(Sys.time(), "%Y%m%d%H%M%S")
    ),
    simulation_type = simulation_type,
    event_name = event_name,
    season = season,
    n_simulations = n_simulations,
    random_seed = random_seed,
    use_model = use_model,
    model_version = model_version,
    created_at = Sys.time()
  )

  return(config)
}


# Random Seed Management ===================================================

#' Set Simulation Seed
#'
#' Sets the random seed for reproducible simulations.
#'
#' @param seed Integer. Random seed
#'
#' @return Invisibly returns the seed
#' @keywords internal
set_simulation_seed <- function(seed) {
  set.seed(seed)
  invisible(seed)
}


#' Get Simulation Seeds
#'
#' Generates a vector of seeds for parallel simulations.
#'
#' @param n Integer. Number of seeds to generate
#' @param base_seed Integer. Base seed (default 42)
#'
#' @return Integer vector of seeds
#' @keywords internal
get_simulation_seeds <- function(n, base_seed = 42) {
  set.seed(base_seed)
  sample.int(.Machine$integer.max, n)
}


# Match Outcome Simulation =================================================

#' Simulate Match Outcome
#'
#' Simulates a single match outcome based on win probabilities.
#'
#' @param team1_win_prob Numeric. Probability that team1 wins (0-1)
#' @param team1 Character. Team 1 name
#' @param team2 Character. Team 2 name
#'
#' @return List with winner, loser, margin, and team1_won flag
#' @keywords internal
simulate_match_outcome <- function(team1_win_prob, team1, team2) {
  # Simulate outcome
  team1_wins <- stats::runif(1) < team1_win_prob

  winner <- if (team1_wins) team1 else team2
  loser <- if (team1_wins) team2 else team1

  # Simulate margin (simplified - just for display)
  margin_type <- sample(c("runs", "wickets"), 1, prob = c(0.5, 0.5))

  if (margin_type == "runs") {
    margin <- sample(1:50, 1, prob = stats::dnorm(1:50, mean = 20, sd = 15))
    margin_str <- paste(margin, "runs")
  } else {
    margin <- sample(1:10, 1, prob = stats::dnorm(1:10, mean = 5, sd = 2))
    margin_str <- paste(margin, "wickets")
  }

  list(
    winner = winner,
    loser = loser,
    margin = margin_str,
    team1_won = team1_wins
  )
}


# Result Aggregation =======================================================

#' Aggregate Match Simulation Results
#'
#' Aggregates results from multiple match simulations.
#'
#' @param results List. List of simulation results from simulate_match_outcome
#' @param team1 Character. Team 1 name
#' @param team2 Character. Team 2 name
#'
#' @return List with aggregated statistics
#' @keywords internal
aggregate_match_results <- function(results, team1, team2) {
  n <- length(results)
  team1_wins <- sum(vapply(results, function(r) r$team1_won, logical(1)))
  team2_wins <- n - team1_wins

  list(
    n_simulations = n,
    team1 = team1,
    team2 = team2,
    team1_wins = team1_wins,
    team2_wins = team2_wins,
    team1_win_pct = team1_wins / n,
    team2_win_pct = team2_wins / n
  )
}


#' Aggregate Season Simulation Results
#'
#' Aggregates results from multiple season simulations.
#'
#' @param standings_list List. List of final standings from each simulation.
#'   Each element should be a data frame with columns: team, wins, losses,
#'   points, makes_playoffs, position.
#'
#' @return Data frame with team probabilities
#' @importFrom dplyr group_by summarise mutate arrange desc
#' @keywords internal
aggregate_season_results <- function(standings_list) {
  n <- length(standings_list)

  # Combine all standings
  all_standings <- do.call(rbind, lapply(seq_along(standings_list), function(i) {
    df <- standings_list[[i]]
    df$simulation <- i
    df
  }))

  # Calculate probabilities
  results <- all_standings |>
    dplyr::group_by(.data$team) |>
    dplyr::summarise(
      avg_wins = mean(.data$wins),
      avg_losses = mean(.data$losses),
      avg_points = mean(.data$points),
      playoff_appearances = sum(.data$makes_playoffs),
      playoff_pct = mean(.data$makes_playoffs) * 100,
      top2_pct = mean(.data$position <= 2) * 100,
      championship_pct = mean(.data$position == 1) * 100,
      .groups = "drop"
    ) |>
    dplyr::arrange(dplyr::desc(.data$playoff_pct), dplyr::desc(.data$avg_points))

  return(results)
}


# Database Storage =========================================================

#' Store Simulation Results
#'
#' Stores simulation results in the database.
#'
#' @param config List. Simulation configuration from create_simulation_config
#' @param team_results Data frame or list. Per-team results
#' @param match_results Data frame or list. Per-match results (optional)
#' @param conn DBI connection. Database connection
#'
#' @return Invisibly returns TRUE on success
#' @importFrom DBI dbExecute
#' @keywords internal
store_simulation_results <- function(config, team_results, match_results = NULL, conn) {
  # Convert results to JSON
  team_results_json <- jsonlite::toJSON(team_results, auto_unbox = TRUE)
  match_results_json <- if (!is.null(match_results)) {
    jsonlite::toJSON(match_results, auto_unbox = TRUE)
  } else {
    NA_character_
  }
  params_json <- jsonlite::toJSON(config, auto_unbox = TRUE)

  DBI::dbExecute(conn, "
    INSERT INTO simulation_results (
      simulation_id, simulation_type, event_name, season,
      simulation_date, n_simulations, parameters,
      team_results, match_results, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (simulation_id) DO UPDATE SET
      team_results = EXCLUDED.team_results,
      match_results = EXCLUDED.match_results,
      created_at = EXCLUDED.created_at
  ", params = list(
    config$simulation_id,
    config$simulation_type,
    config$event_name,
    config$season,
    config$created_at,
    config$n_simulations,
    params_json,
    team_results_json,
    match_results_json,
    Sys.time()
  ))

  cli::cli_alert_success("Stored simulation results: {config$simulation_id}")
  invisible(TRUE)
}


#' Get Simulation Results
#'
#' Retrieves simulation results from the database.
#'
#' @param simulation_id Character. Simulation ID (optional)
#' @param simulation_type Character. Filter by type (optional)
#' @param event_name Character. Filter by event (optional)
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with simulation results
#' @importFrom DBI dbGetQuery
#' @keywords internal
get_simulation_results <- function(simulation_id = NULL, simulation_type = NULL,
                                   event_name = NULL, conn) {
  query <- "SELECT * FROM simulation_results WHERE 1=1"
  params <- list()

  if (!is.null(simulation_id)) {
    query <- paste0(query, " AND simulation_id = ?")
    params <- c(params, simulation_id)
  }

  if (!is.null(simulation_type)) {
    query <- paste0(query, " AND simulation_type = ?")
    params <- c(params, simulation_type)
  }

  if (!is.null(event_name)) {
    query <- paste0(query, " AND event_name LIKE ?")
    params <- c(params, paste0("%", event_name, "%"))
  }

  query <- paste0(query, " ORDER BY created_at DESC")

  if (length(params) > 0) {
    DBI::dbGetQuery(conn, query, params = params)
  } else {
    DBI::dbGetQuery(conn, query)
  }
}


# Progress Utilities =======================================================

#' Run Simulations with Progress
#'
#' Runs a simulation function multiple times with progress bar.
#'
#' @param n Integer. Number of simulations
#' @param sim_fn Function. Simulation function to call
#' @param ... Additional arguments passed to sim_fn
#' @param progress Logical. Show progress bar (default TRUE)
#'
#' @return List of simulation results
#' @keywords internal
run_simulations <- function(n, sim_fn, ..., progress = TRUE) {
  if (progress) {
    cli::cli_progress_bar("Simulating", total = n)
  }

  results <- vector("list", n)

  for (i in seq_len(n)) {
    results[[i]] <- sim_fn(...)

    if (progress) {
      cli::cli_progress_update()
    }
  }

  if (progress) {
    cli::cli_progress_done()
  }

  return(results)
}


# Data Loading =============================================================

#' Get Season Fixtures
#'
#' Retrieves all matches for a specific event and season from the database.
#'
#' @param event_name Character. Event name (e.g., "Indian Premier League")
#' @param season Character. Season (e.g., "2024", "2023/24")
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with match fixtures including team ELOs and actual outcomes
#' @importFrom DBI dbGetQuery
#' @keywords internal
get_season_fixtures <- function(event_name, season, conn) {
  query <- "
    SELECT
      m.match_id,
      m.match_date,
      m.team1,
      m.team2,
      m.venue,
      m.outcome_winner,
      m.outcome_by_runs,
      m.outcome_by_wickets,
      m.event_match_number,
      t1.elo_result AS team1_elo,
      t2.elo_result AS team2_elo
    FROM matches m
    LEFT JOIN team_elo t1 ON m.match_id = t1.match_id AND m.team1 = t1.team_id
    LEFT JOIN team_elo t2 ON m.match_id = t2.match_id AND m.team2 = t2.team_id
    WHERE m.event_name LIKE ?
      AND m.season = ?
      AND m.outcome_winner IS NOT NULL
    ORDER BY m.match_date, m.event_match_number
  "

  fixtures <- DBI::dbGetQuery(conn, query, params = list(
    paste0("%", event_name, "%"),
    season
  ))

  # Fill missing ELOs with starting value
  fixtures$team1_elo[is.na(fixtures$team1_elo)] <- 1500
  fixtures$team2_elo[is.na(fixtures$team2_elo)] <- 1500

  # Calculate win probabilities from ELO
  fixtures$team1_win_prob <- elo_win_probability(
    fixtures$team1_elo,
    fixtures$team2_elo
  )

  # Add actual outcome flag
  fixtures$team1_won <- fixtures$outcome_winner == fixtures$team1

  return(fixtures)
}


#' Get Available Seasons
#'
#' Lists available event/season combinations in the database.
#'
#' @param event_name Character. Filter by event name (optional, uses LIKE)
#' @param conn DBI connection. Database connection
#'
#' @return Data frame with event_name, season, and match count
#' @importFrom DBI dbGetQuery
#' @keywords internal
get_available_seasons <- function(event_name = NULL, conn) {
  query <- "
    SELECT
      event_name,
      season,
      COUNT(*) as n_matches,
      MIN(match_date) as start_date,
      MAX(match_date) as end_date
    FROM matches
    WHERE outcome_winner IS NOT NULL
  "

  if (!is.null(event_name)) {
    query <- paste0(query, " AND event_name LIKE ?")
    query <- paste0(query, " GROUP BY event_name, season ORDER BY event_name, season DESC")
    DBI::dbGetQuery(conn, query, params = list(paste0("%", event_name, "%")))
  } else {
    query <- paste0(query, " GROUP BY event_name, season ORDER BY n_matches DESC")
    DBI::dbGetQuery(conn, query)
  }
}


# ELO Utilities ============================================================

#' Calculate Win Probability from ELO
#'
#' Converts ELO ratings to win probability using the standard formula.
#'
#' @param team1_elo Numeric. Team 1 ELO rating
#' @param team2_elo Numeric. Team 2 ELO rating
#' @param divisor Numeric. ELO divisor (default 400)
#'
#' @return Numeric. Probability that team1 wins (0-1)
#' @keywords internal
elo_win_probability <- function(team1_elo, team2_elo, divisor = 400) {
  1 / (1 + 10^((team2_elo - team1_elo) / divisor))
}


# Season Simulation ========================================================

#' Simulate Season
#'
#' Simulates a complete season using pre-match win probabilities.
#' For each match, simulates the outcome based on the ELO-derived win probability.
#'
#' @param fixtures Data frame. Season fixtures from get_season_fixtures()
#' @param points_for_win Integer. Points awarded for a win (default 2)
#' @param points_for_loss Integer. Points awarded for a loss (default 0)
#'
#' @return Data frame with simulated standings
#' @export
simulate_season <- function(fixtures, points_for_win = 2, points_for_loss = 0) {
  # Get all teams
  all_teams <- unique(c(fixtures$team1, fixtures$team2))

  # Initialize standings
  standings <- data.frame(
    team = all_teams,
    wins = 0L,
    losses = 0L,
    points = 0L,
    stringsAsFactors = FALSE
  )
  rownames(standings) <- standings$team

  # Simulate each match
  for (i in seq_len(nrow(fixtures))) {
    team1 <- fixtures$team1[i]
    team2 <- fixtures$team2[i]
    team1_win_prob <- fixtures$team1_win_prob[i]

    # Simulate outcome
    team1_wins <- stats::runif(1) < team1_win_prob

    if (team1_wins) {
      standings[team1, "wins"] <- standings[team1, "wins"] + 1L
      standings[team1, "points"] <- standings[team1, "points"] + points_for_win
      standings[team2, "losses"] <- standings[team2, "losses"] + 1L
      standings[team2, "points"] <- standings[team2, "points"] + points_for_loss
    } else {
      standings[team2, "wins"] <- standings[team2, "wins"] + 1L
      standings[team2, "points"] <- standings[team2, "points"] + points_for_win
      standings[team1, "losses"] <- standings[team1, "losses"] + 1L
      standings[team1, "points"] <- standings[team1, "points"] + points_for_loss
    }
  }

  # Sort by points, then wins
  standings <- standings[order(-standings$points, -standings$wins), ]
  standings$position <- seq_len(nrow(standings))
  standings$makes_playoffs <- standings$position <= 4

  rownames(standings) <- NULL
  return(standings)
}


#' Simulate Season Multiple Times
#'
#' Runs multiple season simulations and aggregates the results.
#'
#' @param fixtures Data frame. Season fixtures from get_season_fixtures()
#' @param n_simulations Integer. Number of simulations to run
#' @param seed Integer. Random seed for reproducibility (optional)
#' @param progress Logical. Show progress bar (default TRUE)
#'
#' @return Data frame with aggregated probabilities per team
#' @export
simulate_season_n <- function(fixtures, n_simulations = 1000, seed = NULL,
                               progress = TRUE) {
  if (!is.null(seed)) {
    set.seed(seed)
  }

  if (progress) {
    cli::cli_progress_bar("Simulating seasons", total = n_simulations)
  }

  standings_list <- vector("list", n_simulations)

  for (i in seq_len(n_simulations)) {
    standings_list[[i]] <- simulate_season(fixtures)

    if (progress) {
      cli::cli_progress_update()
    }
  }

  if (progress) {
    cli::cli_progress_done()
  }

  aggregate_season_results(standings_list)
}


#' Get Actual Season Standings
#'
#' Calculates the actual final standings from completed fixtures.
#'
#' @param fixtures Data frame. Season fixtures from get_season_fixtures()
#' @param points_for_win Integer. Points awarded for a win (default 2)
#'
#' @return Data frame with actual standings
#' @keywords internal
get_actual_standings <- function(fixtures, points_for_win = 2) {
  # Get all teams
  all_teams <- unique(c(fixtures$team1, fixtures$team2))

  # Initialize standings
  standings <- data.frame(
    team = all_teams,
    wins = 0L,
    losses = 0L,
    points = 0L,
    stringsAsFactors = FALSE
  )
  rownames(standings) <- standings$team

  # Count actual results
  for (i in seq_len(nrow(fixtures))) {
    winner <- fixtures$outcome_winner[i]
    loser <- if (fixtures$team1[i] == winner) fixtures$team2[i] else fixtures$team1[i]

    if (!is.na(winner) && winner != "") {
      standings[winner, "wins"] <- standings[winner, "wins"] + 1L
      standings[winner, "points"] <- standings[winner, "points"] + points_for_win
      standings[loser, "losses"] <- standings[loser, "losses"] + 1L
    }
  }

  # Sort by points, then wins
  standings <- standings[order(-standings$points, -standings$wins), ]
  standings$position <- seq_len(nrow(standings))
  standings$makes_playoffs <- standings$position <= 4

  rownames(standings) <- NULL
  return(standings)
}


# Playoff Simulation =======================================================

#' Get Playoff Teams
#'
#' Extracts the top 4 teams from standings for playoff simulation.
#'
#' @param standings Data frame. Season standings
#' @param fixtures Data frame. Season fixtures (for ELO lookup)
#'
#' @return Data frame with top 4 teams and their average ELOs
#' @keywords internal
get_playoff_teams <- function(standings, fixtures) {
  top4 <- standings[1:4, ]

  # Calculate average ELO for each team across the season
  team_elos <- sapply(top4$team, function(team) {
    team1_matches <- fixtures$team1 == team
    team2_matches <- fixtures$team2 == team
    elos <- c(fixtures$team1_elo[team1_matches], fixtures$team2_elo[team2_matches])
    if (length(elos) > 0) mean(elos, na.rm = TRUE) else 1500
  })

  top4$elo <- team_elos
  top4
}


#' Simulate IPL Playoffs
#'
#' Simulates the IPL playoff bracket (Qualifier 1, Eliminator, Qualifier 2, Final).
#'
#' @param teams Data frame. Top 4 teams with columns: team, elo, position
#'
#' @return List with winner and path
#' @export
simulate_ipl_playoffs <- function(teams) {
  # Ensure teams are ordered by position
  teams <- teams[order(teams$position), ]

  team1 <- teams$team[1]
  team2 <- teams$team[2]
  team3 <- teams$team[3]
  team4 <- teams$team[4]

  elo1 <- teams$elo[1]
  elo2 <- teams$elo[2]
  elo3 <- teams$elo[3]
  elo4 <- teams$elo[4]

  # Qualifier 1: Team 1 vs Team 2
  q1_prob <- elo_win_probability(elo1, elo2)
  q1_winner <- if (stats::runif(1) < q1_prob) team1 else team2
  q1_loser <- if (q1_winner == team1) team2 else team1
  q1_loser_elo <- if (q1_winner == team1) elo2 else elo1

  # Eliminator: Team 3 vs Team 4
  elim_prob <- elo_win_probability(elo3, elo4)
  elim_winner <- if (stats::runif(1) < elim_prob) team3 else team4
  elim_winner_elo <- if (elim_winner == team3) elo3 else elo4

  # Qualifier 2: Loser Q1 vs Winner Eliminator
  q2_prob <- elo_win_probability(q1_loser_elo, elim_winner_elo)
  q2_winner <- if (stats::runif(1) < q2_prob) q1_loser else elim_winner
  q2_winner_elo <- if (q2_winner == q1_loser) q1_loser_elo else elim_winner_elo

  # Final: Winner Q1 vs Winner Q2
  q1_winner_elo <- if (q1_winner == team1) elo1 else elo2
  final_prob <- elo_win_probability(q1_winner_elo, q2_winner_elo)
  champion <- if (stats::runif(1) < final_prob) q1_winner else q2_winner

  list(
    champion = champion,
    finalist_q1 = q1_winner,
    finalist_q2 = q2_winner,
    q1_winner = q1_winner,
    elim_winner = elim_winner,
    q2_winner = q2_winner
  )
}


#' Simulate Playoffs Multiple Times
#'
#' Runs multiple playoff simulations and aggregates championship probabilities.
#'
#' @param teams Data frame. Top 4 teams from get_playoff_teams()
#' @param n_simulations Integer. Number of simulations
#' @param seed Integer. Random seed (optional)
#' @param progress Logical. Show progress bar (default TRUE)
#'
#' @return Data frame with championship and final appearance probabilities
#' @keywords internal
simulate_playoffs_n <- function(teams, n_simulations = 10000, seed = NULL,
                                 progress = TRUE) {
  if (!is.null(seed)) {
    set.seed(seed)
  }

  if (progress) {
    cli::cli_progress_bar("Simulating playoffs", total = n_simulations)
  }

  # Track results
  championships <- setNames(rep(0L, 4), teams$team[1:4])
  finals <- setNames(rep(0L, 4), teams$team[1:4])

  for (i in seq_len(n_simulations)) {
    result <- simulate_ipl_playoffs(teams)
    championships[result$champion] <- championships[result$champion] + 1L
    finals[result$finalist_q1] <- finals[result$finalist_q1] + 1L
    finals[result$finalist_q2] <- finals[result$finalist_q2] + 1L

    if (progress) {
      cli::cli_progress_update()
    }
  }

  if (progress) {
    cli::cli_progress_done()
  }

  data.frame(
    team = names(championships),
    elo = teams$elo[match(names(championships), teams$team)],
    final_appearances = as.integer(finals),
    final_pct = finals / n_simulations * 100,
    championships = as.integer(championships),
    championship_pct = championships / n_simulations * 100,
    stringsAsFactors = FALSE
  )
}
