# Fox Sports Cricket Scraper Functions
# Fetches ball-by-ball data from Fox Sports API

# Package-level environment for storing browser session
.fox_env <- new.env(parent = emptyenv())

# FOX Sports format configurations
# Each format has:
#   - prefix: Match ID prefix (e.g., "TEST", "T20I")
#   - max_innings: Maximum innings for the format (4 for Tests, 2 for limited overs)
#   - max_series: Typical max series number per year for discovery
#   - max_matches: Typical max matches per series for discovery
FOX_FORMATS <- list(
  TEST = list(prefix = "TEST", max_innings = 4, max_series = 15, max_matches = 7),
  T20I = list(prefix = "T20I", max_innings = 2, max_series = 30, max_matches = 10),
  WT20I = list(prefix = "WT20I", max_innings = 2, max_series = 30, max_matches = 10),
  ODI = list(prefix = "ODI", max_innings = 2, max_series = 20, max_matches = 7),
  WODI = list(prefix = "WODI", max_innings = 2, max_series = 20, max_matches = 7),
  BBL = list(prefix = "BBL", max_innings = 2, max_series = 10, max_matches = 70),
  WBBL = list(prefix = "WBBL", max_innings = 2, max_series = 10, max_matches = 70),
  WNCL = list(prefix = "WNCL", max_innings = 2, max_series = 10, max_matches = 30),
  WPL = list(prefix = "WPL", max_innings = 2, max_series = 5, max_matches = 30)
)

#' Get Stored Fox Browser Session
#'
#' Returns the browser session stored after a reconnection during fox_fetch_matches.
#'
#' @return ChromoteSession object, or NULL if no session is stored
#' @keywords internal
fox_get_browser <- function() {
  .fox_env$browser
}

#' Parse Fox Sports aggregatestats response to data.frame
#'
#' @param json_data List from Fox Sports API response
#' @param match_id Character string of the match ID
#' @return A data.frame of ball-by-ball data, or NULL if no data
#' @keywords internal
parse_fox_aggregatestats <- function(json_data, match_id) {

  if (is.null(json_data$innings_list) || length(json_data$innings_list) == 0) {
    return(NULL)
  }

  all_balls <- list()

  for (innings_data in json_data$innings_list) {
    innings_num <- innings_data$innings
    batting_team <- innings_data$batting_team$name
    batting_team_id <- innings_data$batting_team$id
    batting_team_code <- innings_data$batting_team$code
    bowling_team <- innings_data$bowling_team$name

    if (is.null(innings_data$overs)) next

    for (over_data in innings_data$overs) {
      over_num <- over_data$over
      bowler_id <- over_data$bowler$id
      bowler_name <- over_data$bowler$full_name
      bowler_short_name <- over_data$bowler$short_name
      bowling_arm <- over_data$bowler$bowling_arm

      if (is.null(over_data$balls)) next

      for (ball_data in over_data$balls) {
        ball_row <- list(
          match_id = match_id,
          innings = innings_num,
          over = over_num,
          ball = ball_data$ball,
          legal_ball = ball_data$legal_ball,
          running_over = ball_data$running_over,
          team_id = batting_team_id,
          team_name = batting_team,
          team_code = batting_team_code,
          bowling_team = bowling_team,
          bowler_id = bowler_id,
          bowler_name = bowler_name,
          bowler_short_name = bowler_short_name,
          bowling_arm = bowling_arm,
          striker_id = ball_data$striker$id,
          striker_name = ball_data$striker$full_name,
          striker_short_name = ball_data$striker$short_name,
          batting_arm = ball_data$striker$batting_arm,
          dismissed_id = ball_data$dismissed_batsman$id,
          dismissed_name = ball_data$dismissed_batsman$full_name,
          byes = ball_data$byes,
          runs = ball_data$runs,
          wides = ball_data$wides,
          commentary = ball_data$commentary,
          leg_byes = ball_data$leg_byes,
          is_boundary = ball_data$is_boundary,
          is_wicket = ball_data$is_wicket,
          no_ball_runs = ball_data$no_ball_runs,
          no_balls = ball_data$no_balls,
          shot_x = ball_data$shot_x,
          shot_y = ball_data$shot_y,
          total_runs = ball_data$total_runs_per_ball,
          wide_runs = ball_data$wide_runs
        )
        ball_row <- lapply(ball_row, function(x) if (is.null(x)) NA else x)
        all_balls[[length(all_balls) + 1]] <- as.data.frame(ball_row, stringsAsFactors = FALSE)
      }
    }
  }

  if (length(all_balls) == 0) return(NULL)
  dplyr::bind_rows(all_balls)
}

#' Parse Fox Sports players.json response to data.frame
#'
#' @param json_data List from Fox Sports API players response
#' @param match_id Character string of the match ID
#' @return A data.frame of player data for both teams, or NULL if no data
#' @keywords internal
parse_fox_players <- function(json_data, match_id) {
  if (is.null(json_data$team_A) && is.null(json_data$team_B)) {
    return(NULL)
  }

  parse_team_players <- function(team_data, match_id) {
    if (is.null(team_data$players) || length(team_data$players) == 0) {
      return(NULL)
    }

    team_id <- team_data$id
    team_name <- team_data$name
    team_code <- team_data$code

    players <- lapply(team_data$players, function(p) {
      data.frame(
        match_id = match_id,
        team_id = team_id,
        team_name = team_name,
        team_code = team_code,
        player_id = p$id %||% NA,
        full_name = p$full_name %||% NA,
        short_name = p$short_name %||% NA,
        surname = p$surname %||% NA,
        other_names = p$other_names %||% NA,
        country = p$country %||% NA,
        country_code = p$country_code %||% NA,
        batting_arm = p$batting_arm %||% NA,
        bowling_arm = p$bowling_arm %||% NA,
        position_code = p$position_code %||% NA,
        position_description = p$position_description %||% NA,
        position_order = p$position_order %||% NA,
        jumper_number = p$jumper_number %||% NA,
        is_captain = p$is_captain %||% FALSE,
        is_interchange = p$is_interchange %||% FALSE,
        stringsAsFactors = FALSE
      )
    })

    dplyr::bind_rows(players)
  }

  team_a <- parse_team_players(json_data$team_A, match_id)
  team_b <- parse_team_players(json_data$team_B, match_id)

  result <- dplyr::bind_rows(team_a, team_b)
  if (nrow(result) == 0) return(NULL)
  result
}

#' Parse Fox Sports details.json response to data.frame
#'
#' @param json_data List from Fox Sports API details response
#' @param match_id Character string of the match ID
#' @return A single-row data.frame of match details, or NULL if no data
#' @keywords internal
parse_fox_details <- function(json_data, match_id) {
  md <- json_data$match_details
  if (is.null(md)) return(NULL)

  # Extract venue info
 venue <- md$venue
  venue_id <- venue$id %||% NA
  venue_name <- venue$name %||% NA
  venue_city <- venue$city %||% NA
  venue_country <- venue$country %||% NA
  venue_state <- venue$state %||% NA

  # Extract team info
  team_a <- md$team_A
  team_b <- md$team_B

  # Extract player of match
  pom <- json_data$performers$player_of_the_match
  pom_id <- if (!is.null(pom) && length(pom) > 0) pom[[1]]$id else NA
  pom_name <- if (!is.null(pom) && length(pom) > 0) pom[[1]]$full_name else NA

  # Extract umpires
  refs <- json_data$referees
  umpires <- if (!is.null(refs)) {
    ump_names <- sapply(refs[sapply(refs, function(r) r$type == "Umpire")], function(r) r$full_name)
    if (length(ump_names) > 0) paste(ump_names, collapse = ", ") else NA
  } else NA

  third_umpire <- if (!is.null(refs)) {
    tu <- refs[sapply(refs, function(r) r$type == "3rd Umpire")]
    if (length(tu) > 0) tu[[1]]$full_name else NA
  } else NA

  match_referee <- if (!is.null(refs)) {
    mr <- refs[sapply(refs, function(r) r$type == "Match Referee")]
    if (length(mr) > 0) mr[[1]]$full_name else NA
  } else NA

  data.frame(
    match_id = match_id,
    internal_match_id = md$internal_match_id %||% NA,
    match_type = md$match_type %||% NA,
    match_day = md$match_day %||% NA,
    match_result = md$match_result %||% NA,
    match_note = md$match_note %||% NA,
    venue_id = venue_id,
    venue_name = venue_name,
    venue_city = venue_city,
    venue_country = venue_country,
    venue_state = venue_state,
    weather = md$weather %||% NA,
    pitch_state = md$pitch_state %||% NA,
    surface_state = md$surface_state %||% NA,
    crowd = md$crowd %||% NA,
    team_a_id = team_a$id %||% NA,
    team_a_name = team_a$name %||% NA,
    team_a_code = team_a$code %||% NA,
    team_b_id = team_b$id %||% NA,
    team_b_name = team_b$name %||% NA,
    team_b_code = team_b$code %||% NA,
    toss_winner_id = md$won_toss_team_id %||% NA,
    toss_elected_bat = md$won_toss_team_elected_to_bat %||% NA,
    player_of_match_id = pom_id,
    player_of_match_name = pom_name,
    umpires = umpires,
    third_umpire = third_umpire,
    match_referee = match_referee,
    stringsAsFactors = FALSE
  )
}

#' Get userkey from Fox Sports by visiting a match page
#'
#' @param browser A ChromoteSession object
#' @param sample_match_id A known valid match ID to visit
#' @param timeout_sec Timeout in seconds (default 60)
#' @return Character string userkey, or NULL if not found
#' @keywords internal
fox_get_userkey <- function(browser, sample_match_id, timeout_sec = 60) {
  url <- paste0("https://www.foxsports.com.au/cricket/match-centre/", sample_match_id)

  trap_env <- new.env()
  trap_env$found <- FALSE
  trap_env$userkey <- NULL

  sniper_callback <- function(params) {
    if (trap_env$found) return()
    req_url <- params$response$url
    if (grepl("statsapi.foxsports.com.au", req_url) && grepl("userkey=", req_url)) {
      userkey <- sub(".*userkey=([^&]+).*", "\\1", req_url)
      trap_env$userkey <- userkey
      trap_env$found <- TRUE
    }
  }

  browser$Network$responseReceived(callback = sniper_callback)
  browser$Page$navigate(url)

  start_time <- Sys.time()
  scroll_counter <- 0

  while (!trap_env$found) {
    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) > timeout_sec) {
      break
    }
    scroll_counter <- scroll_counter + 1
    if (scroll_counter %% 5 == 0) {
      browser$Runtime$evaluate("window.scrollTo(0, document.body.scrollHeight)")
    }
    Sys.sleep(1)
  }

  browser$Network$responseReceived(callback = NULL)
  return(trap_env$userkey)
}

#' Fetch player squad data for a single match
#'
#' Fetches the players.json endpoint which contains full squad information
#' including batting/bowling arms, positions, captain status, and country.
#'
#' @param browser A ChromoteSession object
#' @param match_id Fox Sports match ID (e.g., "BBL2025-260301")
#' @param userkey Valid userkey from fox_get_userkey()
#' @return A data.frame of player data for both teams, or NULL if no data
#' @keywords internal
fox_fetch_match_players <- function(browser, match_id, userkey) {
  players_url <- sprintf(
    "https://statsapi.foxsports.com.au/3.0/api/sports/cricket/matches/%s/players.json?userkey=%s",
    match_id, userkey
  )

  js_fetch <- sprintf("
    (async () => {
      try {
        const response = await fetch('%s');
        return await response.json();
      } catch(e) {
        return {error: e.message};
      }
    })();
  ", players_url)

  tryCatch({
    result <- browser$Runtime$evaluate(js_fetch, awaitPromise = TRUE, returnByValue = TRUE, timeout_ = 30)
    json_data <- result$result$value

    if (!is.null(json_data$error)) {
      return(NULL)
    }

    parse_fox_players(json_data, match_id)
  }, error = function(e) {
    NULL
  })
}

#' Fetch match details/metadata for a single match
#'
#' Fetches the details.json endpoint which contains match metadata including
#' venue, weather, pitch conditions, toss, result, umpires, and player of match.
#'
#' @param browser A ChromoteSession object
#' @param match_id Fox Sports match ID (e.g., "BBL2025-260301")
#' @param userkey Valid userkey from fox_get_userkey()
#' @return A single-row data.frame of match details, or NULL if no data
#' @keywords internal
fox_fetch_match_details <- function(browser, match_id, userkey) {
  details_url <- sprintf(
    "https://statsapi.foxsports.com.au/3.0/api/sports/cricket/matches/%s/details.json?userkey=%s",
    match_id, userkey
  )

  js_fetch <- sprintf("
    (async () => {
      try {
        const response = await fetch('%s');
        return await response.json();
      } catch(e) {
        return {error: e.message};
      }
    })();
  ", details_url)

  tryCatch({
    result <- browser$Runtime$evaluate(js_fetch, awaitPromise = TRUE, returnByValue = TRUE, timeout_ = 30)
    json_data <- result$result$value

    if (!is.null(json_data$error)) {
      return(NULL)
    }

    parse_fox_details(json_data, match_id)
  }, error = function(e) {
    NULL
  })
}

#' Fetch ball-by-ball data for a single match
#'
#' @param browser A ChromoteSession object
#' @param match_id Fox Sports match ID (e.g., "TEST2025-260604")
#' @param userkey Valid userkey from fox_get_userkey()
#' @param format Cricket format code (default "TEST"). Used to determine max_innings if not specified.
#' @param max_innings Maximum innings to fetch (default: from format config)
#' @param delay_seconds Delay between innings requests
#' @param include_players Also fetch player squad data (default FALSE)
#' @param include_details Also fetch match details/metadata (default FALSE)
#' @return A data.frame of ball-by-ball data, or a list with balls/players/details if extras requested
#' @keywords internal
fox_fetch_match <- function(browser, match_id, userkey, format = "TEST", max_innings = NULL,
                            delay_seconds = 1, include_players = FALSE, include_details = FALSE) {
  # Use format-specific max_innings if not provided
  if (is.null(max_innings)) {
    if (format %in% names(FOX_FORMATS)) {
      max_innings <- FOX_FORMATS[[format]]$max_innings
    } else {
      max_innings <- 4  # Default fallback for Tests
    }
  }
  all_innings_data <- list()

  for (inning_num in 1:max_innings) {
    innings_url <- sprintf(
      "https://statsapi.foxsports.com.au/3.0/api/sports/cricket/matches/%s/aggregatestats.json;fromInnings=%d;fromOver=0;fromBall=0;toInnings=%d;toOver=999;toBall=6;limit=500?userkey=%s",
      match_id, inning_num, inning_num, userkey
    )

    js_fetch <- sprintf("
      (async () => {
        const response = await fetch('%s');
        return await response.json();
      })();
    ", innings_url)

    tryCatch({
      result <- browser$Runtime$evaluate(js_fetch, awaitPromise = TRUE, returnByValue = TRUE, timeout_ = 60)
      json_data <- result$result$value

      if (!is.null(json_data$innings_list) && length(json_data$innings_list) > 0) {
        innings_df <- parse_fox_aggregatestats(json_data, match_id)
        if (!is.null(innings_df) && nrow(innings_df) > 0) {
          all_innings_data[[inning_num]] <- innings_df
        }
      }
    }, error = function(e) {
      # Silently skip errors for individual innings
    })

    # Delay between innings requests
    Sys.sleep(delay_seconds + runif(1, 0.5, 2))
  }

  # Process ball-by-ball data
  balls_data <- NULL
  if (length(all_innings_data) > 0) {
    balls_data <- dplyr::bind_rows(all_innings_data)
    balls_data <- balls_data %>% dplyr::arrange(innings, running_over, ball)
  }

  # If no extras requested, return just the balls data (backward compatible)
  if (!include_players && !include_details) {
    return(balls_data)
  }

  # Fetch additional data if requested
  players_data <- NULL
  details_data <- NULL

  if (include_players) {
    Sys.sleep(0.5)  # Brief delay
    players_data <- fox_fetch_match_players(browser, match_id, userkey)
  }

  if (include_details) {
    Sys.sleep(0.5)  # Brief delay
    details_data <- fox_fetch_match_details(browser, match_id, userkey)
  }

  # Return as a list when extras are included
  list(
    balls = balls_data,
    players = players_data,
    details = details_data
  )
}

#' Check if a Fox Sports match ID exists
#'
#' @param browser A ChromoteSession object
#' @param match_id Fox Sports match ID to check
#' @param userkey Valid userkey
#' @return Logical TRUE if match has data, FALSE otherwise
#' @keywords internal
fox_match_exists <- function(browser, match_id, userkey) {
  innings_url <- sprintf(
    "https://statsapi.foxsports.com.au/3.0/api/sports/cricket/matches/%s/aggregatestats.json;fromInnings=1;fromOver=0;fromBall=0;toInnings=1;toOver=10;toBall=6;limit=5?userkey=%s",
    match_id, userkey
  )

  js_fetch <- sprintf("
    (async () => {
      try {
        const response = await fetch('%s');
        const data = await response.json();
        return data;
      } catch(e) {
        return {error: e.message};
      }
    })();
  ", innings_url)

  tryCatch({
    result <- browser$Runtime$evaluate(js_fetch, awaitPromise = TRUE, returnByValue = TRUE, timeout_ = 10)
    json_data <- result$result$value

    if (!is.null(json_data$innings_list) && length(json_data$innings_list) > 0) {
      return(TRUE)
    }
    return(FALSE)
  }, error = function(e) {
    return(FALSE)
  })
}

#' Generate possible match IDs for a given year/series/match
#'
#' Fox Sports uses two URL patterns:
#' - Pattern A: \{PREFIX\}2025-260604 (hyphen + season suffix like "26" for 2025-26)
#' - Pattern B: \{PREFIX\}20250501 (no hyphen, just year)
#'
#' @param year The year
#' @param series The series number
#' @param match The match number within the series
#' @param format Cricket format code (default "TEST"). See FOX_FORMATS for options.
#' @return Character vector of possible match IDs to try
#' @keywords internal
generate_match_id_variants <- function(year, series, match, format = "TEST") {
  # Validate format
 if (!format %in% names(FOX_FORMATS)) {
    stop("Unknown format: ", format, ". Valid formats: ", paste(names(FOX_FORMATS), collapse = ", "))
  }

  prefix <- FOX_FORMATS[[format]]$prefix
  series_str <- sprintf("%02d", series)
  match_str <- sprintf("%02d", match)

  # Pattern A: with hyphen and season suffix (e.g., TEST2025-260604)
  season_suffix <- sprintf("%02d", (year %% 100) + 1)
  pattern_a <- sprintf("%s%d-%s%s%s", prefix, year, season_suffix, series_str, match_str)

  # Pattern B: no hyphen, just year (e.g., TEST20250501)
  pattern_b <- sprintf("%s%d%s%s", prefix, year, series_str, match_str)

  return(c(pattern_a, pattern_b))
}

#' Discover valid match IDs by enumeration
#'
#' Tries both Fox Sports URL patterns:
#' - Pattern A: \{PREFIX\}2025-260604 (hyphen + season suffix)
#' - Pattern B: \{PREFIX\}20250501 (no hyphen)
#'
#' Caches discovered matches to avoid re-checking. Skips API calls for:
#' - Matches already in cache file
#' - Matches with downloaded .rds or .parquet files
#'
#' @param browser A ChromoteSession object
#' @param userkey Valid userkey
#' @param format Cricket format code (default "TEST"). See fox_list_formats().
#' @param years Vector of years to scan (default 2024:2025)
#' @param max_series Maximum series number to check per year (default: from format config)
#' @param max_matches Maximum matches per series (default: from format config)
#' @param output_dir Base directory for match files (default: bouncerdata/fox_cricket)
#' @param cache_file File to cache discovered match IDs (default: format-specific)
#' @param verbose Print progress messages
#' @return Character vector of valid match IDs
#' @keywords internal
fox_discover_matches <- function(browser, userkey, format = "TEST", years = 2024:2025,
                                  max_series = NULL, max_matches = NULL,
                                  output_dir = "../bouncerdata/fox_cricket",
                                  cache_file = NULL, verbose = TRUE) {

  # Validate format
  if (!format %in% names(FOX_FORMATS)) {
    stop("Unknown format: ", format, ". Valid formats: ", paste(names(FOX_FORMATS), collapse = ", "))
  }

  format_config <- FOX_FORMATS[[format]]
  prefix <- format_config$prefix

  # Use format-specific defaults if not provided
  if (is.null(max_series)) max_series <- format_config$max_series
  if (is.null(max_matches)) max_matches <- format_config$max_matches

  # Create format subdirectory
  format_dir <- file.path(output_dir, tolower(format))
  if (!dir.exists(format_dir)) dir.create(format_dir, recursive = TRUE)

  # Format-specific cache file
  if (is.null(cache_file)) {
    cache_file <- file.path(dirname(output_dir), paste0("fox_discovered_", tolower(format), "_matches.rds"))
  }
  cache_dir <- dirname(cache_file)
  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)

  # Load cached discoveries
  cached_matches <- character(0)
  if (file.exists(cache_file)) {
    cached_matches <- readRDS(cache_file)
    if (verbose) cli::cli_alert_info("Loaded {length(cached_matches)} previously discovered {format} matches from cache")
  }

  # Get list of already-downloaded matches (support both .rds and .parquet)
  existing_files <- character(0)
  if (dir.exists(format_dir)) {
    file_pattern <- paste0("^", prefix, ".*\\.(rds|parquet)$")
    existing_files <- list.files(format_dir, pattern = file_pattern)
    existing_files <- sub("\\.(rds|parquet)$", "", existing_files)
  }

  # Combine cached + downloaded as "known" matches
  known_matches <- unique(c(cached_matches, existing_files))
  if (verbose && length(known_matches) > 0) {
    cli::cli_alert_info("Found {length(known_matches)} known {format} matches (will skip API checks)")
  }

  if (verbose) cli::cli_alert_info("Discovering {format} match IDs by enumeration...")

  valid_matches <- character(0)
  skipped_known <- 0
  new_discoveries <- 0

  for (year in years) {
    if (verbose) cli::cli_alert("Scanning year {year}...")

    for (series in 1:max_series) {
      series_found_any <- FALSE

      for (match in 1:max_matches) {
        # Try both URL patterns
        variants <- generate_match_id_variants(year, series, match, format = format)
        match_found <- FALSE

        for (match_id in variants) {
          # Check if already known - skip API call (no delay!)
          if (match_id %in% known_matches) {
            valid_matches <- c(valid_matches, match_id)
            series_found_any <- TRUE
            match_found <- TRUE
            skipped_known <- skipped_known + 1
            if (verbose) cli::cli_alert_success("  Found (cached): {match_id}")
            break
          }

          # Not known - check API (with delay)
          exists <- fox_match_exists(browser, match_id, userkey)
          Sys.sleep(0.3 + runif(1, 0, 0.5))  # Only delay after API calls

          if (exists) {
            valid_matches <- c(valid_matches, match_id)
            series_found_any <- TRUE
            match_found <- TRUE
            new_discoveries <- new_discoveries + 1
            if (verbose) cli::cli_alert_success("  Found: {match_id}")
            break
          }
        }

        if (!match_found && match == 1) {
          break
        } else if (!match_found) {
          break
        }
        # No delay after cached matches - only API calls need delays
      }
    }
  }

  # Save updated cache
  all_discovered <- unique(c(cached_matches, valid_matches))
  saveRDS(all_discovered, cache_file)

  if (verbose) {
    cli::cli_alert_success("Discovered {length(valid_matches)} valid {format} matches")
    if (skipped_known > 0) {
      cli::cli_alert_info("  ({skipped_known} from cache, {new_discoveries} new)")
    }
    cli::cli_alert_info("Cache saved to {cache_file}")
  }

  return(valid_matches)
}

#' Fetch multiple Fox Sports matches with rate limiting
#'
#' Includes automatic recovery if browser connection fails.
#'
#' @param browser A ChromoteSession object
#' @param match_ids Vector of match IDs to fetch
#' @param userkey Valid userkey
#' @param format Cricket format code (default "TEST"). See fox_list_formats().
#' @param output_dir Base directory to save match files (default: bouncerdata/fox_cricket)
#' @param use_parquet Save as parquet instead of RDS (default TRUE). Requires arrow package.
#' @param include_players Also fetch and save player squad data (default TRUE)
#' @param include_details Also fetch and save match details/metadata (default TRUE)
#' @param skip_existing Skip matches that already have files
#' @param refresh_key_every Refresh userkey after this many matches
#' @param delay_between Seconds to wait between matches
#' @param max_consecutive_failures Stop and reconnect after this many failures in a row
#' @return Combined data.frame of all ball-by-ball data, or NULL
#' @keywords internal
fox_fetch_matches <- function(browser, match_ids, userkey, format = "TEST",
                               output_dir = "../bouncerdata/fox_cricket",
                               use_parquet = TRUE, include_players = TRUE,
                               include_details = TRUE, skip_existing = TRUE,
                               refresh_key_every = 10, delay_between = 8,
                               max_consecutive_failures = 3) {

  # Validate format
  if (!format %in% names(FOX_FORMATS)) {
    stop("Unknown format: ", format, ". Valid formats: ", paste(names(FOX_FORMATS), collapse = ", "))
  }

  # Check arrow package if using parquet
  if (use_parquet && !requireNamespace("arrow", quietly = TRUE)) {
    cli::cli_alert_warning("arrow package not available, falling back to RDS format")
    use_parquet <- FALSE
  }

  # Create format subdirectory
  format_dir <- file.path(output_dir, tolower(format))
  if (!dir.exists(format_dir)) dir.create(format_dir, recursive = TRUE)

  file_ext <- if (use_parquet) ".parquet" else ".rds"

  results <- list()
  skipped <- 0

  fetched_count <- 0
  consecutive_failures <- 0
  current_userkey <- userkey
  current_browser <- browser

  for (i in seq_along(match_ids)) {
    match_id <- match_ids[i]

    # Check which files already exist
    output_file <- file.path(format_dir, paste0(match_id, file_ext))
    balls_exists <- file.exists(file.path(format_dir, paste0(match_id, ".rds"))) ||
                    file.exists(file.path(format_dir, paste0(match_id, ".parquet")))
    players_exists <- file.exists(file.path(format_dir, paste0(match_id, "_players.rds"))) ||
                      file.exists(file.path(format_dir, paste0(match_id, "_players.parquet")))
    details_exists <- file.exists(file.path(format_dir, paste0(match_id, "_details.rds"))) ||
                      file.exists(file.path(format_dir, paste0(match_id, "_details.parquet")))

    # Determine what we need to fetch
    need_balls <- !skip_existing || !balls_exists
    need_players <- include_players && (!skip_existing || !players_exists)
    need_details <- include_details && (!skip_existing || !details_exists)

    # Skip entirely if we have everything we need
    if (!need_balls && !need_players && !need_details) {
      skipped <- skipped + 1
      next
    }

    # Check if we need to reconnect due to consecutive failures
    if (consecutive_failures >= max_consecutive_failures) {
      cli::cli_alert_warning("Too many consecutive failures - reconnecting browser...")

      # Try to close old browser
      tryCatch(current_browser$close(), error = function(e) NULL)
      Sys.sleep(2)

      # Create new browser session
      reconnect_failed <- FALSE
      tryCatch({
        current_browser <- chromote::ChromoteSession$new()
        current_browser$Network$enable()
        Sys.sleep(2)

        # Get new userkey
        cli::cli_alert_info("Getting new userkey...")
        new_key <- fox_get_userkey(current_browser, match_id, timeout_sec = 60)
        if (!is.null(new_key)) {
          current_userkey <- new_key
          cli::cli_alert_success("Reconnected successfully!")
          consecutive_failures <- 0
        } else {
          cli::cli_alert_danger("Failed to get userkey after reconnect. Stopping.")
          reconnect_failed <- TRUE
        }
      }, error = function(e) {
        cli::cli_alert_danger("Failed to reconnect: {e$message}")
        reconnect_failed <<- TRUE
      })

      if (reconnect_failed) break
      Sys.sleep(3)
    }

    # Refresh userkey periodically (even without failures)
    if (consecutive_failures == 0 && fetched_count > 0 && fetched_count %% refresh_key_every == 0) {
      cli::cli_alert_info("Refreshing userkey...")
      Sys.sleep(3 + runif(1, 1, 3))
      tryCatch({
        new_key <- fox_get_userkey(current_browser, match_id, timeout_sec = 45)
        if (!is.null(new_key)) {
          current_userkey <- new_key
          cli::cli_alert_success("Got new userkey")
        }
      }, error = function(e) {
        cli::cli_alert_warning("Failed to refresh userkey: {e$message}")
      })
      Sys.sleep(2 + runif(1, 1, 2))
    }

    # Build status message
    fetch_parts <- c()
    if (need_balls) fetch_parts <- c(fetch_parts, "balls")
    if (need_players) fetch_parts <- c(fetch_parts, "players")
    if (need_details) fetch_parts <- c(fetch_parts, "details")
    cli::cli_alert("[{i}/{length(match_ids)}] Fetching {match_id} ({paste(fetch_parts, collapse = ', ')})...")

    # Initialize data holders
    balls_data <- NULL
    players_data <- NULL
    details_data <- NULL
    fetch_success <- FALSE

    # Fetch balls if needed (this also fetches players/details if requested)
    if (need_balls) {
      match_result <- tryCatch({
        fox_fetch_match(current_browser, match_id, current_userkey, format = format,
                        include_players = need_players, include_details = need_details)
      }, error = function(e) {
        cli::cli_alert_warning("  Error: {e$message}")
        NULL
      })

      # Handle result based on whether extras were requested
      if (need_players || need_details) {
        balls_data <- if (!is.null(match_result)) match_result$balls else NULL
        players_data <- if (!is.null(match_result)) match_result$players else NULL
        details_data <- if (!is.null(match_result)) match_result$details else NULL
      } else {
        balls_data <- match_result
      }
    } else {
      # Balls exist, only fetch missing extras
      if (need_players) {
        Sys.sleep(0.5)
        players_data <- tryCatch({
          fox_fetch_match_players(current_browser, match_id, current_userkey)
        }, error = function(e) NULL)
      }
      if (need_details) {
        Sys.sleep(0.5)
        details_data <- tryCatch({
          fox_fetch_match_details(current_browser, match_id, current_userkey)
        }, error = function(e) NULL)
      }
    }

    saved_files <- c()

    # Save ball-by-ball data if we fetched it
    if (!is.null(balls_data) && nrow(balls_data) > 0) {
      results[[match_id]] <- balls_data

      if (use_parquet) {
        arrow::write_parquet(balls_data, output_file)
      } else {
        saveRDS(balls_data, output_file)
      }
      saved_files <- c(saved_files, "balls")
      fetch_success <- TRUE
    }

    # Save players data if available
    if (!is.null(players_data) && nrow(players_data) > 0) {
      players_file <- file.path(format_dir, paste0(match_id, "_players", file_ext))
      if (use_parquet) {
        arrow::write_parquet(players_data, players_file)
      } else {
        saveRDS(players_data, players_file)
      }
      saved_files <- c(saved_files, "players")
      fetch_success <- TRUE
    }

    # Save details data if available
    if (!is.null(details_data) && nrow(details_data) > 0) {
      details_file <- file.path(format_dir, paste0(match_id, "_details", file_ext))
      if (use_parquet) {
        arrow::write_parquet(details_data, details_file)
      } else {
        saveRDS(details_data, details_file)
      }
      saved_files <- c(saved_files, "details")
      fetch_success <- TRUE
    }

    if (fetch_success) {
      cli::cli_alert_success("  Saved: {paste(saved_files, collapse = ', ')}")
      fetched_count <- fetched_count + 1
      consecutive_failures <- 0  # Reset on success
    } else {
      cli::cli_alert_warning("  No data found")
      consecutive_failures <- consecutive_failures + 1
    }

    # Delay between matches
    if (i < length(match_ids)) {
      Sys.sleep(delay_between + runif(1, 1, 4))
    }
  }

  if (skipped > 0) cli::cli_alert_info("Skipped {skipped} already-downloaded matches")

  # Update browser reference in package environment if it changed
  if (!identical(current_browser, browser)) {
    cli::cli_alert_info("Note: Browser was reconnected. Access with bouncer::fox_get_browser()")
    .fox_env$browser <- current_browser
  }

  if (length(results) > 0) {
    all_data <- dplyr::bind_rows(results)
    cli::cli_alert_success("Fetched {nrow(all_data)} balls from {length(results)} {format} matches")
    return(all_data)
  }

  return(NULL)
}

#' Combine all downloaded Fox Sports match files
#'
#' Reads both .rds and .parquet files, outputs as parquet by default.
#' Combines balls, players, and details files separately.
#'
#' @param format Cricket format code (default "TEST"). See fox_list_formats().
#' @param output_dir Base directory containing match files (default: bouncerdata/fox_cricket)
#' @param use_parquet Save combined file as parquet (default TRUE). Requires arrow package.
#' @param include_players Also combine player files (default TRUE)
#' @param include_details Also combine details files (default TRUE)
#' @return Combined data.frame of all ball-by-ball data (players/details saved separately)
#' @keywords internal
fox_combine_matches <- function(format = "TEST", output_dir = "../bouncerdata/fox_cricket",
                                 use_parquet = TRUE, include_players = TRUE,
                                 include_details = TRUE) {

  # Validate format
  if (!format %in% names(FOX_FORMATS)) {
    stop("Unknown format: ", format, ". Valid formats: ", paste(names(FOX_FORMATS), collapse = ", "))
  }

  prefix <- FOX_FORMATS[[format]]$prefix
  format_dir <- file.path(output_dir, tolower(format))

  # Check arrow package if using parquet
  if (use_parquet && !requireNamespace("arrow", quietly = TRUE)) {
    cli::cli_alert_warning("arrow package not available, falling back to RDS format")
    use_parquet <- FALSE
  }

  file_ext <- if (use_parquet) ".parquet" else ".rds"

  # Helper function to read and combine files
  combine_files <- function(pattern, output_name) {
    rds_files <- list.files(format_dir, pattern = paste0(pattern, "\\.rds$"), full.names = TRUE)
    parquet_files <- list.files(format_dir, pattern = paste0(pattern, "\\.parquet$"), full.names = TRUE)

    # Exclude combined files
    rds_files <- rds_files[!grepl("^all_", basename(rds_files))]
    parquet_files <- parquet_files[!grepl("^all_", basename(parquet_files))]

    data_list <- list()
    if (length(rds_files) > 0) {
      data_list <- c(data_list, lapply(rds_files, readRDS))
    }
    if (length(parquet_files) > 0 && requireNamespace("arrow", quietly = TRUE)) {
      data_list <- c(data_list, lapply(parquet_files, arrow::read_parquet))
    }

    if (length(data_list) == 0) return(NULL)

    combined <- dplyr::bind_rows(data_list)
    output_file <- file.path(format_dir, paste0(output_name, file_ext))

    if (use_parquet) {
      arrow::write_parquet(combined, output_file)
    } else {
      saveRDS(combined, output_file)
    }

    list(data = combined, file = output_file, count = length(data_list))
  }

  # Combine ball-by-ball data (exclude _players and _details files)
  balls_pattern <- paste0("^", prefix, "[^_]*")  # Match PREFIX... but not PREFIX..._players
  balls_result <- combine_files(balls_pattern, paste0("all_", tolower(format), "_matches"))

  if (is.null(balls_result)) {
    cli::cli_alert_warning("No {format} ball-by-ball files found in {format_dir}")
    return(NULL)
  }

  cli::cli_alert_success("Combined {balls_result$count} {format} matches, {nrow(balls_result$data)} total balls")
  cli::cli_alert_info("Saved to: {balls_result$file}")

  # Combine players data
  if (include_players) {
    players_pattern <- paste0("^", prefix, ".*_players")
    players_result <- combine_files(players_pattern, paste0("all_", tolower(format), "_players"))
    if (!is.null(players_result)) {
      cli::cli_alert_success("Combined {players_result$count} player files, {nrow(players_result$data)} total players")
    }
  }

  # Combine details data
  if (include_details) {
    details_pattern <- paste0("^", prefix, ".*_details")
    details_result <- combine_files(details_pattern, paste0("all_", tolower(format), "_details"))
    if (!is.null(details_result)) {
      cli::cli_alert_success("Combined {details_result$count} details files")
    }
  }

  return(balls_result$data)
}

#' List available FOX Sports cricket formats
#'
#' Returns the format codes that can be used with fox_discover_matches(),
#' fox_fetch_matches(), and fox_combine_matches().
#'
#' @param details If TRUE, return a data.frame with format details. If FALSE (default),
#'   return just the format codes as a character vector.
#' @return Character vector of format codes, or data.frame with details
#' @export
#' @examples
#' fox_list_formats()
#' fox_list_formats(details = TRUE)
fox_list_formats <- function(details = FALSE) {
  if (details) {
    data.frame(
      format = names(FOX_FORMATS),
      prefix = sapply(FOX_FORMATS, function(x) x$prefix),
      max_innings = sapply(FOX_FORMATS, function(x) x$max_innings),
      max_series = sapply(FOX_FORMATS, function(x) x$max_series),
      max_matches = sapply(FOX_FORMATS, function(x) x$max_matches),
      row.names = NULL
    )
  } else {
    names(FOX_FORMATS)
  }
}
