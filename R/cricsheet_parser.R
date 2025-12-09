# Cricsheet JSON Parser Functions

#' Parse Cricsheet JSON File
#'
#' Parses a Cricsheet JSON file and extracts all relevant cricket data.
#'
#' @param file_path Character string path to JSON file
#'
#' @return A list with parsed data including match info, innings, deliveries, and players
#' @export
#'
#' @examples
#' \dontrun{
#' # Parse a single match file
#' match_data <- parse_cricsheet_json("path/to/match.json")
#'
#' # Access different components
#' match_info <- match_data$match_info
#' deliveries <- match_data$deliveries
#' }
parse_cricsheet_json <- function(file_path) {
  if (!file.exists(file_path)) {
    cli::cli_abort("File not found: {.file {file_path}}")
  }

  # Read JSON
  tryCatch({
    json_data <- jsonlite::fromJSON(file_path, simplifyVector = FALSE)
  }, error = function(e) {
    cli::cli_abort("Failed to parse JSON file {.file {file_path}}: {e$message}")
  })

  # Parse different components
  match_info <- parse_match_info(json_data, file_path)
  innings_data <- parse_innings(json_data, match_info)
  deliveries_data <- parse_deliveries(json_data, match_info)
  players_data <- parse_players(json_data, match_info)

  result <- list(
    match_info = match_info,
    innings = innings_data,
    deliveries = deliveries_data,
    players = players_data
  )

  return(result)
}


#' Parse Match Info
#'
#' Extracts match metadata from Cricsheet JSON.
#'
#' @param json_data Parsed JSON data
#' @param file_path Original file path for generating match_id
#'
#' @return A data frame with match information
#' @keywords internal
parse_match_info <- function(json_data, file_path) {
  info <- json_data$info

  # Safety check
  if (is.null(info) || !is.list(info)) {
    cli::cli_abort("Invalid match info structure")
  }

  # Generate match_id from filename if not present
  match_id <- if (!is.null(info$match_id) && length(info$match_id) > 0) {
    as.character(info$match_id)
  } else {
    tools::file_path_sans_ext(basename(file_path))
  }

  # Extract dates - handle both list and atomic vector
  match_dates <- if (!is.null(info$dates)) {
    if (is.list(info$dates)) {
      unlist(info$dates)
    } else {
      info$dates
    }
  } else {
    character(0)
  }

  match_date <- if (length(match_dates) > 0) {
    tryCatch(as.Date(match_dates[1]), error = function(e) NA)
  } else {
    NA
  }

  # Extract season
  season <- if (!is.null(info$season)) {
    as.character(info$season)
  } else {
    # Try to extract from date
    if (!is.na(match_date)) {
      format(match_date, "%Y")
    } else {
      NA_character_
    }
  }

  # Extract teams - handle both list and atomic vector
  teams <- if (!is.null(info$teams)) {
    if (is.list(info$teams)) {
      unlist(info$teams)
    } else {
      info$teams
    }
  } else {
    character(0)
  }
  team1 <- if (length(teams) >= 1) teams[1] else NA_character_
  team2 <- if (length(teams) >= 2) teams[2] else NA_character_

  # Extract match type
  match_type <- if (!is.null(info$match_type)) {
    as.character(info$match_type)
  } else {
    NA_character_
  }

  # Extract venue info
  venue <- if (!is.null(info$venue)) {
    as.character(info$venue)
  } else {
    NA_character_
  }

  city <- if (!is.null(info$city)) {
    as.character(info$city)
  } else {
    NA_character_
  }

  # Extract gender
  gender <- if (!is.null(info$gender)) {
    as.character(info$gender)
  } else {
    "male"  # Default to male if not specified
  }

  # Extract balls per over (usually 6)
  balls_per_over <- if (!is.null(info$balls_per_over)) {
    as.integer(info$balls_per_over)
  } else {
    6L
  }

  # Extract overs (for limited overs matches)
  overs_per_innings <- if (!is.null(info$overs)) {
    as.integer(info$overs)
  } else {
    NA_integer_
  }

  # Extract toss information
  toss_winner <- if (!is.null(info$toss$winner)) {
    as.character(info$toss$winner)
  } else {
    NA_character_
  }

  toss_decision <- if (!is.null(info$toss$decision)) {
    as.character(info$toss$decision)
  } else {
    NA_character_
  }

  # Extract outcome
  outcome <- info$outcome
  outcome_type <- "normal"
  outcome_winner <- NA_character_
  outcome_by_runs <- NA_integer_
  outcome_by_wickets <- NA_integer_
  outcome_method <- NA_character_

  if (!is.null(outcome)) {
    if (!is.null(outcome$winner)) {
      outcome_winner <- as.character(outcome$winner)
    }

    if (!is.null(outcome$by)) {
      if (!is.null(outcome$by$runs)) {
        outcome_by_runs <- as.integer(outcome$by$runs)
      }
      if (!is.null(outcome$by$wickets)) {
        outcome_by_wickets <- as.integer(outcome$by$wickets)
      }
    }

    if (!is.null(outcome$method)) {
      outcome_method <- as.character(outcome$method)
    }

    if (!is.null(outcome$result)) {
      outcome_type <- as.character(outcome$result)
    }
  }

  # Extract officials
  umpires <- info$officials$umpires
  umpire1 <- if (length(umpires) >= 1) as.character(umpires[[1]]) else NA_character_
  umpire2 <- if (length(umpires) >= 2) as.character(umpires[[2]]) else NA_character_
  tv_umpire <- if (!is.null(info$officials$tv_umpires) && length(info$officials$tv_umpires) > 0) {
    as.character(info$officials$tv_umpires[[1]])
  } else {
    NA_character_
  }
  referee <- if (!is.null(info$officials$referee) && length(info$officials$referee) > 0) {
    as.character(info$officials$referee[[1]])
  } else {
    NA_character_
  }

  # Extract player of the match
  player_of_match <- info$player_of_match
  player_of_match_id <- if (length(player_of_match) > 0) {
    as.character(player_of_match[[1]])
  } else {
    NA_character_
  }

  # Extract event info
  event_name <- if (!is.null(info$event$name)) {
    as.character(info$event$name)
  } else {
    NA_character_
  }

  event_match_number <- if (!is.null(info$event$match_number)) {
    as.integer(info$event$match_number)
  } else {
    NA_integer_
  }

  # Create data frame
  match_df <- data.frame(
    match_id = match_id,
    season = season,
    match_type = match_type,
    match_date = match_date,
    venue = venue,
    city = city,
    gender = gender,
    team1 = team1,
    team2 = team2,
    balls_per_over = balls_per_over,
    overs_per_innings = overs_per_innings,
    toss_winner = toss_winner,
    toss_decision = toss_decision,
    outcome_type = outcome_type,
    outcome_winner = outcome_winner,
    outcome_by_runs = outcome_by_runs,
    outcome_by_wickets = outcome_by_wickets,
    outcome_method = outcome_method,
    umpire1 = umpire1,
    umpire2 = umpire2,
    tv_umpire = tv_umpire,
    referee = referee,
    player_of_match_id = player_of_match_id,
    event_name = event_name,
    event_match_number = event_match_number,
    stringsAsFactors = FALSE
  )

  return(match_df)
}


#' Parse Innings Data
#'
#' Extracts innings summary information.
#'
#' @param json_data Parsed JSON data
#' @param match_info Match information data frame
#'
#' @return A data frame with innings information
#' @keywords internal
parse_innings <- function(json_data, match_info) {
  if (is.null(json_data$innings)) {
    return(data.frame())
  }

  innings_list <- list()

  for (i in seq_along(json_data$innings)) {
    inning_data <- json_data$innings[[i]]

    # Get team name (key of the list)
    batting_team <- names(inning_data)[1]
    inning_details <- inning_data[[1]]

    # Calculate innings number
    innings_num <- i

    # Determine bowling team
    bowling_team <- if (batting_team == match_info$team1) {
      match_info$team2
    } else {
      match_info$team1
    }

    # Calculate totals from deliveries if available
    overs <- inning_details$overs
    total_runs <- 0
    total_balls <- 0
    wickets <- 0

    if (!is.null(overs)) {
      for (over in overs) {
        deliveries <- over$deliveries
        if (!is.null(deliveries)) {
          for (delivery in deliveries) {
            total_balls <- total_balls + 1
            runs <- delivery$runs
            if (!is.null(runs$total)) {
              total_runs <- total_runs + runs$total
            }
            if (!is.null(delivery$wickets)) {
              wickets <- wickets + length(delivery$wickets)
            }
          }
        }
      }
    }

    total_overs <- floor(total_balls / 6) + (total_balls %% 6) / 10

    # Check for declared/forfeited
    declared <- !is.null(inning_details$declared) && inning_details$declared
    forfeited <- !is.null(inning_details$forfeited) && inning_details$forfeited

    innings_list[[i]] <- data.frame(
      match_id = match_info$match_id,
      innings = innings_num,
      batting_team = batting_team,
      bowling_team = bowling_team,
      total_runs = total_runs,
      total_wickets = wickets,
      total_overs = total_overs,
      declared = declared,
      forfeited = forfeited,
      stringsAsFactors = FALSE
    )
  }

  if (length(innings_list) > 0) {
    do.call(rbind, innings_list)
  } else {
    data.frame()
  }
}


#' Parse Deliveries Data
#'
#' Extracts ball-by-ball delivery data.
#'
#' @param json_data Parsed JSON data
#' @param match_info Match information data frame
#'
#' @return A data frame with delivery-level data
#' @keywords internal
parse_deliveries <- function(json_data, match_info) {
  if (is.null(json_data$innings)) {
    return(data.frame())
  }

  delivery_list <- list()
  delivery_counter <- 0

  for (innings_idx in seq_along(json_data$innings)) {
    inning_data <- json_data$innings[[innings_idx]]

    batting_team <- names(inning_data)[1]
    bowling_team <- if (batting_team == match_info$team1) {
      match_info$team2
    } else {
      match_info$team1
    }

    inning_details <- inning_data[[1]]
    overs_data <- inning_details$overs

    if (is.null(overs_data)) next

    # Track running totals
    total_runs_running <- 0
    wickets_running <- 0

    for (over_data in overs_data) {
      over_num <- over_data$over
      deliveries <- over_data$deliveries

      if (is.null(deliveries)) next

      for (ball_idx in seq_along(deliveries)) {
        delivery <- deliveries[[ball_idx]]
        delivery_counter <- delivery_counter + 1

        # Extract batter and bowler
        batter_id <- as.character(delivery$batter)
        bowler_id <- as.character(delivery$bowler)
        non_striker_id <- as.character(delivery$non_striker)

        # Extract runs
        runs <- delivery$runs
        runs_batter <- if (!is.null(runs$batter)) as.integer(runs$batter) else 0L
        runs_extras <- if (!is.null(runs$extras)) as.integer(runs$extras) else 0L
        runs_total <- if (!is.null(runs$total)) as.integer(runs$total) else 0L

        # Update running total
        total_runs_running <- total_runs_running + runs_total

        # Extract extras
        extras <- delivery$extras
        wides <- if (!is.null(extras$wides)) as.integer(extras$wides) else 0L
        noballs <- if (!is.null(extras$noballs)) as.integer(extras$noballs) else 0L
        byes <- if (!is.null(extras$byes)) as.integer(extras$byes) else 0L
        legbyes <- if (!is.null(extras$legbyes)) as.integer(extras$legbyes) else 0L
        penalty <- if (!is.null(extras$penalty)) as.integer(extras$penalty) else 0L

        # Check for boundary
        is_four <- runs_batter == 4 && runs_extras == 0
        is_six <- runs_batter == 6 && runs_extras == 0
        is_boundary <- is_four || is_six

        # Extract wicket information
        wickets_data <- delivery$wickets
        is_wicket <- !is.null(wickets_data) && length(wickets_data) > 0

        wicket_kind <- NA_character_
        player_out_id <- NA_character_
        fielder1_id <- NA_character_
        fielder2_id <- NA_character_

        if (is_wicket) {
          wickets_running <- wickets_running + 1
          wicket <- wickets_data[[1]]  # Take first wicket if multiple

          wicket_kind <- if (!is.null(wicket$kind)) as.character(wicket$kind) else NA_character_
          player_out_id <- if (!is.null(wicket$player_out)) as.character(wicket$player_out) else NA_character_

          fielders <- wicket$fielders
          if (!is.null(fielders) && length(fielders) > 0) {
            fielder1_id <- if (length(fielders) >= 1) as.character(fielders[[1]]$name) else NA_character_
            fielder2_id <- if (length(fielders) >= 2) as.character(fielders[[2]]$name) else NA_character_
          }
        }

        # Create delivery record
        delivery_list[[delivery_counter]] <- data.frame(
          delivery_id = delivery_counter,
          match_id = match_info$match_id,
          season = match_info$season,
          match_type = match_info$match_type,
          match_date = match_info$match_date,
          venue = match_info$venue,
          city = match_info$city,
          gender = match_info$gender,
          batting_team = batting_team,
          bowling_team = bowling_team,
          innings = innings_idx,
          over = over_num,
          ball = ball_idx,
          over_ball = over_num + ball_idx / 10,
          batter_id = batter_id,
          bowler_id = bowler_id,
          non_striker_id = non_striker_id,
          runs_batter = runs_batter,
          runs_extras = runs_extras,
          runs_total = runs_total,
          is_boundary = is_boundary,
          is_four = is_four,
          is_six = is_six,
          wides = wides,
          noballs = noballs,
          byes = byes,
          legbyes = legbyes,
          penalty = penalty,
          is_wicket = is_wicket,
          wicket_kind = wicket_kind,
          player_out_id = player_out_id,
          fielder1_id = fielder1_id,
          fielder2_id = fielder2_id,
          total_runs = total_runs_running,
          wickets_fallen = wickets_running,
          stringsAsFactors = FALSE
        )
      }
    }
  }

  if (length(delivery_list) > 0) {
    do.call(rbind, delivery_list)
  } else {
    data.frame()
  }
}


#' Parse Players Data
#'
#' Extracts player registry information.
#'
#' @param json_data Parsed JSON data
#' @param match_info Match information data frame
#'
#' @return A data frame with player information
#' @keywords internal
parse_players <- function(json_data, match_info) {
  if (is.null(json_data$info$players)) {
    return(data.frame())
  }

  players_data <- json_data$info$players
  player_list <- list()
  player_idx <- 0

  for (team_name in names(players_data)) {
    team_players <- players_data[[team_name]]

    for (player_name in team_players) {
      player_idx <- player_idx + 1

      player_list[[player_idx]] <- data.frame(
        player_id = as.character(player_name),
        player_name = as.character(player_name),
        country = team_name,
        dob = NA,
        batting_style = NA_character_,
        bowling_style = NA_character_,
        stringsAsFactors = FALSE
      )
    }
  }

  if (length(player_list) > 0) {
    players_df <- do.call(rbind, player_list)
    # Remove duplicates
    players_df <- players_df[!duplicated(players_df$player_id), ]
    return(players_df)
  } else {
    return(data.frame())
  }
}
