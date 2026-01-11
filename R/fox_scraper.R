# Fox Sports Cricket Scraper Functions
# Fetches ball-by-ball data from Fox Sports API

# Package-level environment for storing browser session
.fox_env <- new.env(parent = emptyenv())

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

#' Fetch ball-by-ball data for a single match
#'
#' @param browser A ChromoteSession object
#' @param match_id Fox Sports match ID (e.g., "TEST2025-260604")
#' @param userkey Valid userkey from fox_get_userkey()
#' @param max_innings Maximum innings to fetch (default 4 for Tests)
#' @param delay_seconds Delay between innings requests
#' @return A data.frame of ball-by-ball data, or NULL if no data
#' @keywords internal
fox_fetch_match <- function(browser, match_id, userkey, max_innings = 4, delay_seconds = 1) {
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

  if (length(all_innings_data) > 0) {
    final_data <- dplyr::bind_rows(all_innings_data)
    final_data <- final_data %>% dplyr::arrange(innings, running_over, ball)
    return(final_data)
  }
  return(NULL)
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
#' - Pattern A: TEST2025-260604 (hyphen + season suffix like "26" for 2025-26)
#' - Pattern B: TEST20250501 (no hyphen, just year)
#'
#' @param year The year
#' @param series The series number
#' @param match The match number within the series
#' @return Character vector of possible match IDs to try
#' @keywords internal
generate_match_id_variants <- function(year, series, match) {

  series_str <- sprintf("%02d", series)
  match_str <- sprintf("%02d", match)

  # Pattern A: with hyphen and season suffix (e.g., TEST2025-260604)
  season_suffix <- sprintf("%02d", (year %% 100) + 1)
  pattern_a <- sprintf("TEST%d-%s%s%s", year, season_suffix, series_str, match_str)

  # Pattern B: no hyphen, just year (e.g., TEST20250501)
  pattern_b <- sprintf("TEST%d%s%s", year, series_str, match_str)

  return(c(pattern_a, pattern_b))
}

#' Discover valid Test match IDs by enumeration
#'
#' Tries both Fox Sports URL patterns:
#' - Pattern A: TEST2025-260604 (hyphen + season suffix)
#' - Pattern B: TEST20250501 (no hyphen)
#'
#' Caches discovered matches to avoid re-checking. Skips API calls for:
#' - Matches already in cache file
#' - Matches with downloaded .rds files
#'
#' @param browser A ChromoteSession object
#' @param userkey Valid userkey
#' @param years Vector of years to scan (default 2024:2025)
#' @param max_series Maximum series number to check per year
#' @param max_matches Maximum matches per series
#' @param output_dir Directory to check for existing .rds files (default: bouncerdata/fox_cricket)
#' @param cache_file File to cache discovered match IDs (default: output_dir/discovered_matches.rds)
#' @param verbose Print progress messages
#' @return Character vector of valid match IDs
#' @keywords internal
fox_discover_matches <- function(browser, userkey, years = 2024:2025,
                                  max_series = 15, max_matches = 7,
                                  output_dir = "../bouncerdata/fox_cricket",
                                  cache_file = NULL, verbose = TRUE) {

  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

  # Cache file in bouncerdata repo (sibling to bouncer package)
  if (is.null(cache_file)) {
    cache_file <- "../bouncerdata/fox_discovered_matches.rds"
  }
  # Ensure parent dir exists
  cache_dir <- dirname(cache_file)
  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)

  # Load cached discoveries
  cached_matches <- character(0)
  if (file.exists(cache_file)) {
    cached_matches <- readRDS(cache_file)
    if (verbose) cli::cli_alert_info("Loaded {length(cached_matches)} previously discovered matches from cache")
  }

  # Get list of already-downloaded matches
  existing_files <- character(0)
  if (dir.exists(output_dir)) {
    existing_files <- list.files(output_dir, pattern = "^TEST.*\\.rds$")
    existing_files <- sub("\\.rds$", "", existing_files)
  }

  # Combine cached + downloaded as "known" matches

  known_matches <- unique(c(cached_matches, existing_files))
  if (verbose && length(known_matches) > 0) {
    cli::cli_alert_info("Found {length(known_matches)} known matches (will skip API checks)")
  }

  if (verbose) cli::cli_alert_info("Discovering Test match IDs by enumeration...")

  valid_matches <- character(0)
  skipped_known <- 0
  new_discoveries <- 0


  for (year in years) {
    if (verbose) cli::cli_alert("Scanning year {year}...")

    for (series in 1:max_series) {
      series_found_any <- FALSE

      for (match in 1:max_matches) {
        # Try both URL patterns
        variants <- generate_match_id_variants(year, series, match)
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
    cli::cli_alert_success("Discovered {length(valid_matches)} valid matches")
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
#' @param output_dir Directory to save individual match files (default: bouncerdata/fox_cricket)
#' @param skip_existing Skip matches that already have files
#' @param refresh_key_every Refresh userkey after this many matches
#' @param delay_between Seconds to wait between matches
#' @param max_consecutive_failures Stop and reconnect after this many failures in a row
#' @return Combined data.frame of all matches, or NULL
#' @keywords internal
fox_fetch_matches <- function(browser, match_ids, userkey, output_dir = "../bouncerdata/fox_cricket",
                               skip_existing = TRUE, refresh_key_every = 10,
                               delay_between = 8, max_consecutive_failures = 3) {
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

  results <- list()
  skipped <- 0

  fetched_count <- 0
  consecutive_failures <- 0
  current_userkey <- userkey
  current_browser <- browser

  for (i in seq_along(match_ids)) {
    match_id <- match_ids[i]

    # Skip if already downloaded
    output_file <- file.path(output_dir, paste0(match_id, ".rds"))
    if (skip_existing && file.exists(output_file)) {
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

    cli::cli_alert("[{i}/{length(match_ids)}] Fetching {match_id}...")

    match_data <- tryCatch({
      fox_fetch_match(current_browser, match_id, current_userkey)
    }, error = function(e) {
      cli::cli_alert_warning("  Error: {e$message}")
      NULL
    })

    if (!is.null(match_data) && nrow(match_data) > 0) {
      results[[match_id]] <- match_data
      saveRDS(match_data, output_file)
      cli::cli_alert_success("  Saved {nrow(match_data)} balls")
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
    cli::cli_alert_success("Fetched {nrow(all_data)} balls from {length(results)} matches")
    return(all_data)
  }

  return(NULL)
}

#' Combine all downloaded Fox Sports match files
#'
#' @param output_dir Directory containing .rds match files (default: bouncerdata/fox_cricket)
#' @return Combined data.frame of all matches
#' @keywords internal
fox_combine_matches <- function(output_dir = "../bouncerdata/fox_cricket") {
  files <- list.files(output_dir, pattern = "^TEST.*\\.rds$", full.names = TRUE)
  if (length(files) == 0) {
    cli::cli_alert_warning("No match files found in {output_dir}")
    return(NULL)
  }

  all_data <- lapply(files, readRDS) %>% dplyr::bind_rows()
  combined_file <- file.path(output_dir, "all_test_matches.rds")
  saveRDS(all_data, combined_file)
  cli::cli_alert_success("Combined {length(files)} matches, {nrow(all_data)} total balls")
  return(all_data)
}
