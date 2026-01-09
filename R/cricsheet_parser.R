# Cricsheet JSON Parser Functions (Optimized)
#
# This module provides high-performance parsing of Cricsheet JSON files.
# Key optimizations:
# - Pre-allocated vectors (no rbind loops)
# - Single-pass parsing (innings, deliveries, players in one traversal)
# - Minimal object creation in hot loops

#' Parse Cricsheet JSON File
#'
#' Parses a Cricsheet JSON file with optimized vectorized operations.
#' Uses pre-allocation and single-pass parsing for high performance.
#'
#' @param file_path Character string path to JSON file
#'
#' @return A list with parsed data:
#'   - match_info: data frame with match metadata
#'   - innings: data frame with innings summaries
#'   - deliveries: data frame with ball-by-ball data
#'   - players: data frame with player information
#' @export
#'
#' @examples
#' \dontrun{
#' # Parse a single match file
#' match_data <- parse_cricsheet_json("64012.json")
#'
#' # Access different components
#' match_info <- match_data$match_info
#' deliveries <- match_data$deliveries
#' }
parse_cricsheet_json <- function(file_path) {
  if (!file.exists(file_path)) {
    cli::cli_abort("File not found: {.file {file_path}}")
  }

  # Extract match_id from filename
  match_id <- tools::file_path_sans_ext(basename(file_path))

  # Read JSON
  tryCatch({
    json_data <- jsonlite::fromJSON(file_path, simplifyVector = FALSE)
  }, error = function(e) {
    cli::cli_abort("Failed to parse JSON file {.file {file_path}}: {e$message}")
  })

  # Parse match info (lightweight)
  match_info <- parse_match_info(json_data, match_id)

  # Single-pass parsing: deliveries + innings + players in one traversal
  parsed <- parse_all_data(json_data, match_info)

  list(
    match_info = match_info,
    innings = parsed$innings,
    deliveries = parsed$deliveries,
    players = parsed$players
  )
}


#' Parse Match Info
#'
#' Extracts match metadata from Cricsheet JSON.
#'
#' @param json_data Parsed JSON data
#' @param match_id Match ID (from filename)
#'
#' @return A data frame with match information
#' @keywords internal
parse_match_info <- function(json_data, match_id) {
  info <- json_data$info
  meta <- json_data$meta

  if (is.null(info) || !is.list(info)) {
    cli::cli_abort("Invalid match info structure")
  }

  # Extract dates
  match_dates <- if (is.list(info$dates)) unlist(info$dates) else info$dates
  match_date <- if (length(match_dates) > 0) {
    tryCatch(as.Date(match_dates[1]), error = function(e) NA)
  } else NA

  # Extract season (fallback to year from date)
  season <- info$season %||% {
    if (!is.na(match_date)) format(match_date, "%Y") else NA_character_
  }

  # Extract teams
  teams <- if (is.list(info$teams)) unlist(info$teams) else info$teams
  team1 <- if (length(teams) >= 1) teams[1] else NA_character_
  team2 <- if (length(teams) >= 2) teams[2] else NA_character_

  # Extract toss info
  toss_winner <- NA_character_
  toss_decision <- NA_character_
  if (!is.null(info$toss) && is.list(info$toss)) {
    toss_winner <- info$toss$winner %||% NA_character_
    toss_decision <- info$toss$decision %||% NA_character_
  }

  # Extract outcome
  outcome_type <- "normal"
  outcome_winner <- NA_character_
  outcome_by_runs <- NA_integer_
  outcome_by_wickets <- NA_integer_
  outcome_method <- NA_character_

  if (!is.null(info$outcome) && is.list(info$outcome)) {
    outcome <- info$outcome
    outcome_winner <- outcome$winner %||% NA_character_
    if (!is.null(outcome$by) && is.list(outcome$by)) {
      outcome_by_runs <- as.integer(outcome$by$runs %||% NA)
      outcome_by_wickets <- as.integer(outcome$by$wickets %||% NA)
    }
    outcome_method <- outcome$method %||% NA_character_
    outcome_type <- outcome$result %||% "normal"
  }

  # Extract officials
  umpire1 <- NA_character_
  umpire2 <- NA_character_
  tv_umpire <- NA_character_
  referee <- NA_character_

  if (!is.null(info$officials) && is.list(info$officials)) {
    umpires <- info$officials$umpires
    if (length(umpires) >= 1) umpire1 <- umpires[[1]]
    if (length(umpires) >= 2) umpire2 <- umpires[[2]]
    tv_umpires <- info$officials$tv_umpires
    if (length(tv_umpires) >= 1) tv_umpire <- tv_umpires[[1]]
    refs <- info$officials$match_referees %||% info$officials$referee
    if (length(refs) >= 1) referee <- refs[[1]]
  }

  # Extract player of match
  pom <- info$player_of_match
  player_of_match_id <- if (length(pom) > 0) pom[[1]] else NA_character_

  # Extract event info
  event_name <- NA_character_
  event_match_number <- NA_integer_
  event_group <- NA_character_

  if (!is.null(info$event) && is.list(info$event)) {
    event_name <- info$event$name %||% NA_character_
    event_match_number <- as.integer(info$event$match_number %||% NA)
    event_group <- info$event$group %||% NA_character_
  }

  # Extract meta info
  data_version <- meta$data_version %||% NA_character_
  data_created <- tryCatch(as.Date(meta$created %||% NA), error = function(e) NA)
  data_revision <- as.integer(meta$revision %||% NA)

  # Create data frame
  data.frame(
    match_id = match_id,
    season = as.character(season),
    match_type = info$match_type %||% NA_character_,
    match_type_number = as.integer(info$match_type_number %||% NA),
    match_date = match_date,
    venue = info$venue %||% NA_character_,
    city = info$city %||% NA_character_,
    gender = info$gender %||% "male",
    team_type = info$team_type %||% NA_character_,
    team1 = team1,
    team2 = team2,
    balls_per_over = as.integer(info$balls_per_over %||% 6L),
    overs_per_innings = as.integer(info$overs %||% NA),
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
    event_group = event_group,
    data_version = data_version,
    data_created = data_created,
    data_revision = data_revision,
    stringsAsFactors = FALSE
  )
}


#' Parse All Data in One Pass (Optimized)
#'
#' Extracts innings, deliveries, and players in a single JSON traversal.
#' Uses pre-allocated vectors for O(n) performance instead of O(n²) rbind.
#'
#' @param json_data Parsed JSON data
#' @param match_info Match information data frame
#'
#' @return List with innings, deliveries, and players data frames
#' @keywords internal
parse_all_data <- function(json_data, match_info) {
  innings_data <- json_data$innings

  if (is.null(innings_data) || length(innings_data) == 0) {
    return(list(
      innings = data.frame(),
      deliveries = data.frame(),
      players = extract_players(json_data$info$players)
    ))
  }

  # Count total deliveries for pre-allocation
  total_deliveries <- count_deliveries(innings_data)

  if (total_deliveries == 0) {
    return(list(
      innings = data.frame(),
      deliveries = data.frame(),
      players = extract_players(json_data$info$players)
    ))
  }

  # PRE-ALLOCATE all delivery vectors (THE KEY OPTIMIZATION!)
  del_delivery_id <- character(total_deliveries)
  del_match_id <- character(total_deliveries)
  del_season <- character(total_deliveries)
  del_match_type <- character(total_deliveries)
  del_match_date <- rep(match_info$match_date, total_deliveries)
  del_venue <- character(total_deliveries)
  del_city <- character(total_deliveries)
  del_gender <- character(total_deliveries)
  del_batting_team <- character(total_deliveries)
  del_bowling_team <- character(total_deliveries)
  del_innings <- integer(total_deliveries)
  del_over <- integer(total_deliveries)
  del_ball <- integer(total_deliveries)
  del_over_ball <- numeric(total_deliveries)
  del_batter_id <- character(total_deliveries)
  del_bowler_id <- character(total_deliveries)
  del_non_striker_id <- character(total_deliveries)
  del_runs_batter <- integer(total_deliveries)
  del_runs_extras <- integer(total_deliveries)
  del_runs_total <- integer(total_deliveries)
  del_is_boundary <- logical(total_deliveries)
  del_is_four <- logical(total_deliveries)
  del_is_six <- logical(total_deliveries)
  del_wides <- integer(total_deliveries)
  del_noballs <- integer(total_deliveries)
  del_byes <- integer(total_deliveries)
  del_legbyes <- integer(total_deliveries)
  del_penalty <- integer(total_deliveries)
  del_is_wicket <- logical(total_deliveries)
  del_wicket_kind <- character(total_deliveries)
  del_player_out_id <- character(total_deliveries)
  del_fielder1_id <- character(total_deliveries)
  del_fielder2_id <- character(total_deliveries)
  del_total_runs <- integer(total_deliveries)
  del_wickets_fallen <- integer(total_deliveries)

  # Pre-allocate innings vectors (max 4 for Test)
  n_innings <- length(innings_data)
  inn_match_id <- character(n_innings)
  inn_innings <- integer(n_innings)
  inn_batting_team <- character(n_innings)
  inn_bowling_team <- character(n_innings)
  inn_total_runs <- integer(n_innings)
  inn_total_wickets <- integer(n_innings)
  inn_total_overs <- numeric(n_innings)
  inn_declared <- logical(n_innings)
  inn_forfeited <- logical(n_innings)

  # Process all data in single pass
  delivery_idx <- 0

  for (innings_num in seq_along(innings_data)) {
    inning <- innings_data[[innings_num]]
    batting_team <- inning$team %||% NA_character_

    bowling_team <- if (!is.na(batting_team) && batting_team == match_info$team1) {
      match_info$team2
    } else {
      match_info$team1
    }

    innings_runs <- 0L
    innings_wickets <- 0L
    innings_balls <- 0L

    overs <- inning$overs
    if (!is.null(overs)) {
      for (over_data in overs) {
        over_num <- over_data$over %||% 0L
        deliveries <- over_data$deliveries

        if (!is.null(deliveries)) {
          for (ball_num in seq_along(deliveries)) {
            delivery <- deliveries[[ball_num]]
            delivery_idx <- delivery_idx + 1
            innings_balls <- innings_balls + 1

            # Extract runs
            runs <- delivery$runs
            runs_batter <- as.integer(runs$batter %||% 0L)
            runs_extras <- as.integer(runs$extras %||% 0L)
            runs_total <- as.integer(runs$total %||% 0L)
            innings_runs <- innings_runs + runs_total

            # Extract extras (if present)
            extras <- delivery$extras
            wides <- as.integer(extras$wides %||% 0L)
            noballs <- as.integer(extras$noballs %||% 0L)
            byes <- as.integer(extras$byes %||% 0L)
            legbyes <- as.integer(extras$legbyes %||% 0L)
            penalty <- as.integer(extras$penalty %||% 0L)

            # Extract wicket
            wickets <- delivery$wickets
            is_wicket <- !is.null(wickets) && length(wickets) > 0
            wicket_kind <- NA_character_
            player_out_id <- NA_character_
            fielder1_id <- NA_character_
            fielder2_id <- NA_character_

            if (is_wicket) {
              innings_wickets <- innings_wickets + 1L
              w <- wickets[[1]]
              wicket_kind <- w$kind %||% NA_character_
              player_out_id <- w$player_out %||% NA_character_
              fielders <- w$fielders
              if (!is.null(fielders) && length(fielders) > 0) {
                fielder1_id <- fielders[[1]]$name %||% NA_character_
                if (length(fielders) >= 2) {
                  fielder2_id <- fielders[[2]]$name %||% NA_character_
                }
              }
            }

            # Fill pre-allocated vectors (FAST!)
            # delivery_id format: {match_id}_{batting_team}_{innings}_{over}_{ball}
            # Over is zero-padded to 3 digits (e.g., 003, 015, 100) for correct sorting
            # Ball is zero-padded to 2 digits (e.g., 04, 12) for correct sorting
            # Replace spaces in team name with underscores for valid ID
            batting_team_clean <- gsub(" ", "_", batting_team)
            del_delivery_id[delivery_idx] <- sprintf("%s_%s_%d_%03d_%02d",
                                                      match_info$match_id,
                                                      batting_team_clean,
                                                      innings_num, over_num, ball_num)
            del_match_id[delivery_idx] <- match_info$match_id
            del_season[delivery_idx] <- match_info$season %||% NA_character_
            del_match_type[delivery_idx] <- match_info$match_type %||% NA_character_
            del_venue[delivery_idx] <- match_info$venue %||% NA_character_
            del_city[delivery_idx] <- match_info$city %||% NA_character_
            del_gender[delivery_idx] <- match_info$gender %||% NA_character_
            del_batting_team[delivery_idx] <- batting_team
            del_bowling_team[delivery_idx] <- bowling_team
            del_innings[delivery_idx] <- innings_num
            del_over[delivery_idx] <- over_num
            del_ball[delivery_idx] <- ball_num
            del_over_ball[delivery_idx] <- over_num + ball_num / 10
            del_batter_id[delivery_idx] <- delivery$batter %||% NA_character_
            del_bowler_id[delivery_idx] <- delivery$bowler %||% NA_character_
            del_non_striker_id[delivery_idx] <- delivery$non_striker %||% NA_character_
            del_runs_batter[delivery_idx] <- runs_batter
            del_runs_extras[delivery_idx] <- runs_extras
            del_runs_total[delivery_idx] <- runs_total
            del_is_four[delivery_idx] <- runs_batter == 4L && runs_extras == 0L
            del_is_six[delivery_idx] <- runs_batter == 6L && runs_extras == 0L
            del_is_boundary[delivery_idx] <- del_is_four[delivery_idx] || del_is_six[delivery_idx]
            del_wides[delivery_idx] <- wides
            del_noballs[delivery_idx] <- noballs
            del_byes[delivery_idx] <- byes
            del_legbyes[delivery_idx] <- legbyes
            del_penalty[delivery_idx] <- penalty
            del_is_wicket[delivery_idx] <- is_wicket
            del_wicket_kind[delivery_idx] <- wicket_kind
            del_player_out_id[delivery_idx] <- player_out_id
            del_fielder1_id[delivery_idx] <- fielder1_id
            del_fielder2_id[delivery_idx] <- fielder2_id
            del_total_runs[delivery_idx] <- innings_runs
            del_wickets_fallen[delivery_idx] <- innings_wickets
          }
        }
      }
    }

    # Fill innings summary
    inn_match_id[innings_num] <- match_info$match_id
    inn_innings[innings_num] <- innings_num
    inn_batting_team[innings_num] <- batting_team
    inn_bowling_team[innings_num] <- bowling_team
    inn_total_runs[innings_num] <- innings_runs
    inn_total_wickets[innings_num] <- innings_wickets
    inn_total_overs[innings_num] <- floor(innings_balls / 6) + (innings_balls %% 6) / 10
    inn_declared[innings_num] <- isTRUE(inning$declared)
    inn_forfeited[innings_num] <- isTRUE(inning$forfeited)
  }

  # Create data frames in SINGLE call (no rbind!)
  deliveries_df <- data.frame(
    delivery_id = del_delivery_id,
    match_id = del_match_id,
    season = del_season,
    match_type = del_match_type,
    match_date = del_match_date,
    venue = del_venue,
    city = del_city,
    gender = del_gender,
    batting_team = del_batting_team,
    bowling_team = del_bowling_team,
    innings = del_innings,
    over = del_over,
    ball = del_ball,
    over_ball = del_over_ball,
    batter_id = del_batter_id,
    bowler_id = del_bowler_id,
    non_striker_id = del_non_striker_id,
    runs_batter = del_runs_batter,
    runs_extras = del_runs_extras,
    runs_total = del_runs_total,
    is_boundary = del_is_boundary,
    is_four = del_is_four,
    is_six = del_is_six,
    wides = del_wides,
    noballs = del_noballs,
    byes = del_byes,
    legbyes = del_legbyes,
    penalty = del_penalty,
    is_wicket = del_is_wicket,
    wicket_kind = del_wicket_kind,
    player_out_id = del_player_out_id,
    fielder1_id = del_fielder1_id,
    fielder2_id = del_fielder2_id,
    total_runs = del_total_runs,
    wickets_fallen = del_wickets_fallen,
    stringsAsFactors = FALSE
  )

  innings_df <- data.frame(
    match_id = inn_match_id,
    innings = inn_innings,
    batting_team = inn_batting_team,
    bowling_team = inn_bowling_team,
    total_runs = inn_total_runs,
    total_wickets = inn_total_wickets,
    total_overs = inn_total_overs,
    declared = inn_declared,
    forfeited = inn_forfeited,
    stringsAsFactors = FALSE
  )

  list(
    innings = innings_df,
    deliveries = deliveries_df,
    players = extract_players(json_data$info$players)
  )
}


#' Count Total Deliveries
#'
#' Counts total deliveries for pre-allocation.
#'
#' @param innings_data List of innings from JSON
#'
#' @return Integer count of total deliveries
#' @keywords internal
count_deliveries <- function(innings_data) {
  total <- 0L
  for (inning in innings_data) {
    overs <- inning$overs
    if (!is.null(overs)) {
      for (over in overs) {
        if (!is.null(over$deliveries)) {
          total <- total + length(over$deliveries)
        }
      }
    }
  }
  total
}


#' Extract Players
#'
#' Extracts player information from JSON with deduplication.
#'
#' @param players_data Players section from JSON info
#'
#' @return Data frame with player information
#' @keywords internal
extract_players <- function(players_data) {
  if (is.null(players_data)) {
    return(data.frame(
      player_id = character(),
      player_name = character(),
      country = character(),
      dob = as.Date(character()),
      batting_style = character(),
      bowling_style = character(),
      stringsAsFactors = FALSE
    ))
  }

  # Pre-count for allocation
  n_players <- sum(vapply(players_data, length, integer(1)))

  if (n_players == 0) {
    return(data.frame(
      player_id = character(),
      player_name = character(),
      country = character(),
      dob = as.Date(character()),
      batting_style = character(),
      bowling_style = character(),
      stringsAsFactors = FALSE
    ))
  }

  player_id <- character(n_players)
  player_name <- character(n_players)
  country <- character(n_players)

  idx <- 0
  for (team_name in names(players_data)) {
    team_players <- players_data[[team_name]]
    for (p in team_players) {
      idx <- idx + 1
      player_id[idx] <- p
      player_name[idx] <- p
      country[idx] <- team_name
    }
  }

  df <- data.frame(
    player_id = player_id,
    player_name = player_name,
    country = country,
    dob = as.Date(NA),
    batting_style = NA_character_,
    bowling_style = NA_character_,
    stringsAsFactors = FALSE
  )

  # Remove duplicates
  df[!duplicated(df$player_id), ]
}
