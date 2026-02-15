# ESPN Cricinfo Scraper Functions
# Fetches ball-by-ball commentary data from ESPN Cricinfo's consumer API
#
# Architecture:
#   ESPN Cricinfo uses Next.js SSR. Data is available via two methods:
#   1. __NEXT_DATA__: Page props embedded in HTML (scorecard, details, schedule)
#   2. CDP Network interception: Capture API responses triggered by scrolling (commentary)
#
#   Direct API calls (fetch/XHR) are blocked by Akamai CDN for match endpoints.
#   The /current endpoint works but match-specific endpoints return 403.

# Package-level environment for storing browser session
.cricinfo_env <- new.env(parent = emptyenv())

# ESPN Cricinfo API base URL
CRICINFO_API_BASE <- "https://hs-consumer-api.espncricinfo.com"

# Format mapping from Cricinfo names to our format codes
CRICINFO_FORMATS <- list(
  T20  = list(name = "T20", max_innings = 2L),
  ODI  = list(name = "ODI", max_innings = 2L),
  Test = list(name = "Test", max_innings = 4L)
)

# ============================================================================
# SESSION MANAGEMENT
# ============================================================================

#' Get Stored Cricinfo Browser Session
#'
#' Returns the browser session stored after a reconnection during cricinfo_fetch_matches.
#'
#' @return ChromoteSession object, or NULL if no session is stored
#' @keywords internal
cricinfo_get_browser <- function() {
  .cricinfo_env$browser
}

#' Apply Stealth Patches to Browser Session
#'
#' ESPN Cricinfo uses Cloudflare bot detection that blocks headless Chrome.
#' This function injects stealth overrides via CDP to bypass detection:
#' removes navigator.webdriver flag, sets a realistic user-agent, and
#' adds fake plugin/language properties.
#'
#' Must be called BEFORE navigating to any espncricinfo.com page.
#'
#' @param browser A ChromoteSession object
#' @return Invisible NULL
#' @keywords internal
cricinfo_apply_stealth <- function(browser) {
  # Inject script that runs before any page JS
  tryCatch({
    browser$Page$addScriptToEvaluateOnNewDocument(source = "
      Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
      window.navigator.chrome = { runtime: {} };
      Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
      Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
    ")
  }, error = function(e) NULL)

  # Set realistic user-agent
  tryCatch({
    browser$Network$setUserAgentOverride(
      userAgent = paste0(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ",
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
      )
    )
  }, error = function(e) NULL)

  invisible(NULL)
}

# ============================================================================
# URL BUILDERS
# ============================================================================

#' Build ESPN Cricinfo Match Page URL
#'
#' Constructs a URL using dummy slugs. ESPN's router extracts numeric IDs
#' from the path and redirects to the canonical URL.
#'
#' @param series_id Integer series objectId
#' @param match_id Integer match objectId
#' @param page Page tab: "ball-by-ball-commentary", "full-scorecard", "scorecard"
#' @return Character URL
#' @keywords internal
cricinfo_match_url <- function(series_id, match_id, page = "full-scorecard") {
  sprintf("https://www.espncricinfo.com/series/s-%d/m-%d/%s",
          series_id, match_id, page)
}

#' Build ESPN Cricinfo Series Page URL
#'
#' @param series_id Integer series objectId
#' @param page Page type: "match-schedule-fixtures-and-results"
#' @return Character URL
#' @keywords internal
cricinfo_series_url <- function(series_id,
                                 page = "match-schedule-fixtures-and-results") {
  sprintf("https://www.espncricinfo.com/series/s-%d/%s", series_id, page)
}

# ============================================================================
# CORE EXTRACTION
# ============================================================================

#' Extract __NEXT_DATA__ from Current Page
#'
#' ESPN Cricinfo uses Next.js SSR. Page data is embedded in a script tag
#' with id __NEXT_DATA__. This function extracts and parses that data.
#'
#' @param browser A ChromoteSession object (must already be on the target page)
#' @return Parsed list of page data, or NULL on error
#' @keywords internal
cricinfo_extract_nextdata <- function(browser) {
  tryCatch({
    result <- browser$Runtime$evaluate(
      'JSON.stringify(JSON.parse(document.getElementById("__NEXT_DATA__").textContent).props.appPageProps.data)',
      returnByValue = TRUE, timeout_ = 30
    )
    jsonlite::fromJSON(result$result$value, simplifyVector = FALSE)
  }, error = function(e) NULL)
}

#' Navigate to Page and Extract __NEXT_DATA__
#'
#' Navigates the browser to the specified URL, waits for the page to load,
#' then extracts the embedded Next.js page data. Uses the browser's load
#' event rather than a fixed sleep, since __NEXT_DATA__ is server-rendered
#' and available as soon as the HTML arrives.
#'
#' @param browser A ChromoteSession object
#' @param url URL to navigate to
#' @param timeout Seconds to wait for page load event (default 15)
#' @return Parsed list of page data, or NULL on error
#' @keywords internal
cricinfo_navigate_and_extract <- function(browser, url, timeout = 15) {
  browser$Page$navigate(url)
  # Wait for load event instead of fixed sleep — __NEXT_DATA__ is in the

  # initial HTML so it's ready as soon as the page finishes loading
  tryCatch(
    browser$Page$loadEventFired(wait_ = TRUE, timeout_ = timeout),
    error = function(e) Sys.sleep(5)  # fallback if event times out
  )
  Sys.sleep(0.5)  # brief buffer for DOM to settle
  cricinfo_extract_nextdata(browser)
}

# ============================================================================
# AUTHENTICATION (for /current endpoint only)
# ============================================================================

#' Extract Auth Token from ESPN Cricinfo
#'
#' Visits the live scores page and intercepts the x-hsci-auth-token header
#' from outgoing API requests. Only needed for the /current matches endpoint.
#'
#' @param browser A ChromoteSession object with Network enabled
#' @param sample_url A known cricinfo URL to visit, or NULL for default
#' @param timeout_sec Timeout in seconds (default 60)
#' @return Character auth token, or NULL if not found
#' @keywords internal
cricinfo_get_auth_token <- function(browser, sample_url = NULL, timeout_sec = 60) {
  if (is.null(sample_url)) {
    sample_url <- "https://www.espncricinfo.com/live-cricket-score"
  }

  cricinfo_apply_stealth(browser)

  trap_env <- new.env()
  trap_env$found <- FALSE
  trap_env$token <- NULL

  sniper_callback <- function(params) {
    if (trap_env$found) return()
    req_url <- params$request$url
    if (grepl("hs-consumer-api\\.espncricinfo\\.com", req_url)) {
      headers <- params$request$headers
      token <- headers[["x-hsci-auth-token"]]
      if (!is.null(token) && nchar(token) > 0) {
        trap_env$token <- token
        trap_env$found <- TRUE
      }
    }
  }

  browser$Network$requestWillBeSent(callback = sniper_callback)
  browser$Page$navigate(sample_url)

  start_time <- Sys.time()
  scroll_counter <- 0

  while (!trap_env$found) {
    if (as.numeric(difftime(Sys.time(), start_time, units = "secs")) > timeout_sec) {
      break
    }
    scroll_counter <- scroll_counter + 1
    if (scroll_counter %% 5 == 0) {
      tryCatch(
        browser$Runtime$evaluate("window.scrollTo(0, document.body.scrollHeight)"),
        error = function(e) NULL
      )
    }
    Sys.sleep(1)
  }

  browser$Network$requestWillBeSent(callback = NULL)
  return(trap_env$token)
}

#' Parse Auth Token Expiry Time
#'
#' Extracts the expiry timestamp from a cricinfo auth token.
#' Token format: exp=(unix_timestamp)~hmac=(hash)
#'
#' @param token Character auth token
#' @return POSIXct expiry time, or NA if parsing fails
#' @keywords internal
cricinfo_token_expiry <- function(token) {
  exp_str <- sub(".*exp=(\\d+).*", "\\1", token)
  exp_num <- suppressWarnings(as.numeric(exp_str))
  if (is.na(exp_num)) return(NA)
  as.POSIXct(exp_num, origin = "1970-01-01")
}

#' Check if Auth Token is Still Valid
#'
#' @param token Character auth token
#' @param buffer_minutes Minutes of buffer before actual expiry (default 5)
#' @return Logical TRUE if token is still valid
#' @keywords internal
cricinfo_token_valid <- function(token, buffer_minutes = 5) {
  if (is.null(token) || !nzchar(token)) return(FALSE)
  expiry <- cricinfo_token_expiry(token)
  if (is.na(expiry)) return(TRUE)
  Sys.time() < (expiry - buffer_minutes * 60)
}

# ============================================================================
# COMMENTARY CAPTURE (CDP Network Interception + Scroll)
# ============================================================================

#' Capture Ball-by-Ball Commentary via Page Scroll
#'
#' Navigates to the ball-by-ball commentary page, then scrolls gradually
#' to trigger the page's own API calls for loading earlier overs. Captures
#' all API responses via CDP Network interception.
#'
#' Returns commentary for the latest innings. The page always loads the
#' most recent innings first; innings switching is not currently supported
#' via the page UI.
#'
#' @param browser A ChromoteSession object with Network enabled
#' @param series_id Integer series objectId
#' @param match_id Integer match objectId
#' @param scroll_pause Seconds between scroll actions (default 1.5)
#' @param max_scrolls Maximum number of scroll actions (default 30)
#' @param stale_threshold Stop after this many scrolls with no new data (default 6)
#' @param all_innings Logical. Whether to capture all innings (default TRUE)
#' @return List with $initial_data (from __NEXT_DATA__) and $api_responses (from scroll)
#' @keywords internal
cricinfo_capture_commentary <- function(browser, series_id, match_id,
                                         scroll_pause = 1.5,
                                         max_scrolls = 30,
                                         stale_threshold = 6,
                                         all_innings = TRUE) {
  url <- cricinfo_match_url(series_id, match_id, "ball-by-ball-commentary")

  # Set up CDP response capture
  capture_env <- new.env()
  capture_env$responses <- list()
  capture_env$count <- 0

  cdp_callback <- function(params) {
    resp_url <- params$response$url
    if (grepl("hs-consumer-api.*comments", resp_url) &&
        params$response$status == 200) {
      req_id <- params$requestId
      tryCatch({
        body <- browser$Network$getResponseBody(requestId = req_id)
        if (!is.null(body$body) && nchar(body$body) > 100) {
          capture_env$count <- capture_env$count + 1
          capture_env$responses[[capture_env$count]] <- body$body
        }
      }, error = function(e) NULL)
    }
  }

  browser$Network$responseReceived(callback = cdp_callback)

  # Navigate and wait for load event (not fixed sleep)
  browser$Page$navigate(url)
  tryCatch(
    browser$Page$loadEventFired(wait_ = TRUE, timeout_ = 15),
    error = function(e) Sys.sleep(5)
  )
  Sys.sleep(1)

  # Extract __NEXT_DATA__ for initial commentary + match metadata
  initial_data <- cricinfo_extract_nextdata(browser)

  # Determine how many innings to capture
  num_innings <- 1L
  current_inn <- tryCatch(
    initial_data$content$currentInningNumber %||% 1L,
    error = function(e) 1L
  )
  if (all_innings && !is.null(initial_data$content$innings)) {
    num_innings <- length(initial_data$content$innings)
  }

  # Helper: scroll one innings worth of commentary
  scroll_innings <- function() {
    prev <- capture_env$count
    stale <- 0
    for (i in seq_len(max_scrolls)) {
      tryCatch(
        browser$Runtime$evaluate("window.scrollBy(0, 1500)"),
        error = function(e) NULL
      )
      Sys.sleep(scroll_pause)
      if (capture_env$count > prev) {
        prev <- capture_env$count
        stale <- 0
      } else {
        stale <- stale + 1
      }
      if (stale >= stale_threshold) break
    }
  }

  # Capture current innings
  scroll_innings()

  # Switch to other innings and capture those too
  if (all_innings && num_innings > 1) {
    for (inn in seq_len(num_innings)) {
      if (inn == current_inn) next

      # Click the innings tab via JavaScript
      switched <- tryCatch({
        js <- sprintf('
          (function() {
            var tabs = document.querySelectorAll("[data-testid*=\\"innings\\"], button, [role=\\"tab\\"]");
            for (var t of tabs) {
              var text = t.textContent.trim();
              if (text.match(/^(1st|2nd|3rd|4th)\\s*inn/i) && text.match(/%d/)) {
                t.click();
                return true;
              }
              if (text === "%d" || text === "Inn %d" || text === "Innings %d") {
                t.click();
                return true;
              }
            }
            // Try ordinal matching
            var ordinals = ["1st", "2nd", "3rd", "4th"];
            var target = ordinals[%d - 1];
            for (var t of tabs) {
              if (t.textContent.trim().toLowerCase().startsWith(target)) {
                t.click();
                return true;
              }
            }
            return false;
          })()
        ', inn, inn, inn, inn, inn)
        r <- browser$Runtime$evaluate(js, returnByValue = TRUE, timeout_ = 5)
        isTRUE(r$result$value)
      }, error = function(e) FALSE)

      if (switched) {
        Sys.sleep(2)  # Wait for innings data to start loading
        # Scroll back to top and then down to trigger lazy loading
        tryCatch(browser$Runtime$evaluate("window.scrollTo(0, 0)"), error = function(e) NULL)
        Sys.sleep(1)
        scroll_innings()
      }
    }
  }

  # Clean up callback
  tryCatch(browser$Network$responseReceived(callback = NULL), error = function(e) NULL)

  list(
    initial_data = initial_data,
    api_responses = capture_env$responses
  )
}

# ============================================================================
# PARSERS
# ============================================================================

#' Helper: Convert Empty List to NA
#'
#' The Cricinfo API returns empty objects (list()) instead of null for
#' missing scalar values. This converts them to NA.
#'
#' @param x Value to check
#' @return The value, or NA if NULL or zero-length
#' @keywords internal
to_scalar <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA)
  if (is.list(x)) return(NA)
  x
}

#' Parse Ball-by-Ball Data from Scorecard inningOvers
#'
#' Extracts ball-level data from the scorecard page's __NEXT_DATA__
#' inningOvers field. Only overs with embedded ball data are included
#' (typically only over 1 per innings in the scorecard page).
#'
#' @param innings_data List of innings from scorecard content
#' @param match_id Integer match objectId
#' @param series_id Integer series objectId
#' @param player_map Named list mapping player IDs to names
#' @return data.frame of ball-by-ball data, or NULL
#' @keywords internal
parse_cricinfo_balls_from_overs <- function(innings_data, match_id,
                                              series_id = NA, player_map = NULL) {
  all_balls <- list()

  for (inn in innings_data) {
    if (is.null(inn$inningOvers)) next

    for (ov in inn$inningOvers) {
      if (is.null(ov$balls) || length(ov$balls) == 0) next

      for (ball in ov$balls) {
        bat_id <- ball$batsmanPlayerId
        bowl_id <- ball$bowlerPlayerId

        predictions <- ball$predictions
        pred_score <- to_scalar(predictions$score)
        pred_win_prob <- to_scalar(predictions$winProbability)

        row <- list(
          match_id             = match_id,
          series_id            = series_id,
          innings              = ball$inningNumber,
          over                 = ball$oversActual,
          over_number          = ball$overNumber,
          ball_number          = ball$ballNumber,
          batsman_id           = bat_id,
          batsman_name         = if (!is.null(player_map) && !is.null(bat_id))
            player_map[[as.character(bat_id)]] %||% NA_character_ else NA_character_,
          non_striker_id       = ball$nonStrikerPlayerId,
          bowler_id            = bowl_id,
          bowler_name          = if (!is.null(player_map) && !is.null(bowl_id))
            player_map[[as.character(bowl_id)]] %||% NA_character_ else NA_character_,
          total_runs           = ball$totalRuns,
          batsman_runs         = ball$batsmanRuns,
          is_four              = ball$isFour %||% FALSE,
          is_six               = ball$isSix %||% FALSE,
          is_wicket            = ball$isWicket %||% FALSE,
          dismissal_type       = to_scalar(ball$dismissalType),
          out_player_id        = to_scalar(ball$outPlayerId),
          wides                = ball$wides %||% 0L,
          noballs              = ball$noballs %||% 0L,
          byes                 = ball$byes %||% 0L,
          legbyes              = ball$legbyes %||% 0L,
          pitch_line           = to_scalar(ball$pitchLine),
          pitch_length         = to_scalar(ball$pitchLength),
          shot_type            = to_scalar(ball$shotType),
          shot_control         = to_scalar(ball$shotControl),
          wagon_x              = to_scalar(ball$wagonX),
          wagon_y              = to_scalar(ball$wagonY),
          wagon_zone           = to_scalar(ball$wagonZone),
          predicted_score      = pred_score,
          win_probability      = pred_win_prob,
          total_innings_runs   = ball$totalInningRuns,
          total_innings_wickets = ball$totalInningWickets,
          timestamp            = to_scalar(ball$timestamp)
        )

        row <- lapply(row, function(x) {
          if (is.null(x) || length(x) == 0 || is.list(x)) NA else x
        })
        all_balls[[length(all_balls) + 1]] <- as.data.frame(row, stringsAsFactors = FALSE)
      }
    }
  }

  if (length(all_balls) == 0) return(NULL)
  dplyr::bind_rows(all_balls)
}

#' Parse Commentary API Response to Ball-by-Ball Data
#'
#' Extracts ball-by-ball data from a commentary endpoint response
#' (captured via CDP Network interception during scrolling).
#'
#' @param json_data List from commentary API response (parsed JSON)
#' @param match_id Integer match objectId
#' @param series_id Integer series objectId
#' @param player_map Named list mapping player IDs to names
#' @return data.frame of ball-by-ball data, or NULL
#' @keywords internal
parse_cricinfo_commentary <- function(json_data, match_id, series_id = NA,
                                       player_map = NULL) {
  comments <- json_data$comments
  if (is.null(comments) || length(comments) == 0) return(NULL)

  all_balls <- vector("list", length(comments))
  idx <- 0

  for (item in comments) {
    idx <- idx + 1

    predictions <- item$predictions
    pred_score <- to_scalar(predictions$score)
    pred_win_prob <- to_scalar(predictions$winProbability)

    # Extract batsman/bowler names from title ("Bowler to Batter" format)
    title <- to_scalar(item$title)
    batsman_name <- NA_character_
    bowler_name <- NA_character_

    if (!is.na(title) && grepl(" to ", title)) {
      parts <- strsplit(title, " to ", fixed = TRUE)[[1]]
      if (length(parts) >= 2) {
        bowler_name <- trimws(parts[1])
        batter_part <- parts[2]
        if (grepl(",", batter_part)) {
          batsman_name <- trimws(sub(",.*", "", batter_part))
        } else {
          batsman_name <- trimws(batter_part)
        }
      }
    }

    # Override with player_map if available
    bat_id <- item$batsmanPlayerId
    bowl_id <- item$bowlerPlayerId
    if (!is.null(player_map)) {
      if (!is.null(bat_id)) batsman_name <- player_map[[as.character(bat_id)]] %||% batsman_name
      if (!is.null(bowl_id)) bowler_name <- player_map[[as.character(bowl_id)]] %||% bowler_name
    }

    ball_row <- list(
      match_id             = match_id,
      series_id            = series_id,
      innings              = item$inningNumber,
      over                 = item$oversActual,
      over_number          = item$overNumber,
      ball_number          = item$ballNumber,
      batsman_id           = bat_id,
      batsman_name         = batsman_name,
      non_striker_id       = item$nonStrikerPlayerId,
      bowler_id            = bowl_id,
      bowler_name          = bowler_name,
      total_runs           = item$totalRuns,
      batsman_runs         = item$batsmanRuns,
      is_four              = item$isFour %||% FALSE,
      is_six               = item$isSix %||% FALSE,
      is_wicket            = item$isWicket %||% FALSE,
      dismissal_type       = to_scalar(item$dismissalType),
      dismissal_text       = to_scalar(if (is.list(item$dismissalText)) item$dismissalText$long else item$dismissalText),
      out_player_id        = to_scalar(item$outPlayerId),
      wides                = item$wides %||% 0L,
      noballs              = item$noballs %||% 0L,
      byes                 = item$byes %||% 0L,
      legbyes              = item$legbyes %||% 0L,
      pitch_line           = to_scalar(item$pitchLine),
      pitch_length         = to_scalar(item$pitchLength),
      shot_type            = to_scalar(item$shotType),
      shot_control         = to_scalar(item$shotControl),
      wagon_x              = to_scalar(item$wagonX),
      wagon_y              = to_scalar(item$wagonY),
      wagon_zone           = to_scalar(item$wagonZone),
      predicted_score      = pred_score,
      win_probability      = pred_win_prob,
      total_innings_runs   = item$totalInningRuns,
      total_innings_wickets = item$totalInningWickets,
      title                = title,
      timestamp            = to_scalar(item$timestamp)
    )

    ball_row <- lapply(ball_row, function(x) if (is.null(x) || length(x) == 0 || is.list(x)) NA else x)
    all_balls[[idx]] <- as.data.frame(ball_row, stringsAsFactors = FALSE)
  }

  if (idx == 0) return(NULL)
  dplyr::bind_rows(all_balls[seq_len(idx)])
}

#' Parse Cricinfo Scorecard from __NEXT_DATA__
#'
#' Extracts batting and bowling scorecard data from the scorecard page's
#' embedded Next.js data.
#'
#' @param data List from __NEXT_DATA__ extraction (scorecard page)
#' @param match_id Integer match objectId
#' @return List with $batting and $bowling data.frames, or NULL
#' @keywords internal
parse_cricinfo_scorecard <- function(data, match_id) {
  innings_list <- data$content$innings
  if (is.null(innings_list) || length(innings_list) == 0) return(NULL)

  all_batting <- list()
  all_bowling <- list()

  for (inn in innings_list) {
    inn_num <- inn$inningNumber
    team_name <- inn$team$name %||% inn$team$longName

    if (!is.null(inn$inningBatsmen)) {
      for (bat in inn$inningBatsmen) {
        # dismissalText is a list with keys: short, long, commentary, fielderText, bowlerText
        dt <- bat$dismissalText
        row <- list(
          match_id      = match_id,
          innings       = inn_num,
          team          = team_name,
          player_id     = bat$player$id %||% bat$player$objectId,
          player_name   = bat$player$longName %||% bat$player$name,
          runs          = bat$runs,
          balls_faced   = bat$ballsFaced %||% bat$balls,
          fours         = bat$fours,
          sixes         = bat$sixes,
          strike_rate   = bat$strikerate %||% bat$strikeRate,
          dismissal     = to_scalar(if (is.list(dt)) dt$long else dt),
          dismissal_short = to_scalar(if (is.list(dt)) dt$short else NA),
          position      = bat$battingPosition %||% bat$position,
          is_out        = bat$isOut %||% NA,
          minutes       = bat$minutes
        )
        row <- lapply(row, function(x) if (is.null(x) || length(x) == 0 || is.list(x)) NA else x)
        all_batting[[length(all_batting) + 1]] <- as.data.frame(row, stringsAsFactors = FALSE)
      }
    }

    if (!is.null(inn$inningBowlers)) {
      for (bowl in inn$inningBowlers) {
        row <- list(
          match_id      = match_id,
          innings       = inn_num,
          team          = team_name,
          player_id     = bowl$player$id %||% bowl$player$objectId,
          player_name   = bowl$player$longName %||% bowl$player$name,
          overs         = bowl$overs,
          maidens       = bowl$maidens,
          runs_conceded = bowl$conceded %||% bowl$runs,
          wickets       = bowl$wickets,
          economy       = bowl$economy,
          dots          = bowl$dots,
          fours_conceded = bowl$fours,
          sixes_conceded = bowl$sixes,
          wides         = bowl$wides,
          noballs       = bowl$noballs
        )
        row <- lapply(row, function(x) if (is.null(x) || length(x) == 0 || is.list(x)) NA else x)
        all_bowling[[length(all_bowling) + 1]] <- as.data.frame(row, stringsAsFactors = FALSE)
      }
    }
  }

  batting_df <- if (length(all_batting) > 0) dplyr::bind_rows(all_batting) else NULL
  bowling_df <- if (length(all_bowling) > 0) dplyr::bind_rows(all_bowling) else NULL

  if (is.null(batting_df) && is.null(bowling_df)) return(NULL)
  list(batting = batting_df, bowling = bowling_df)
}

#' Parse Cricinfo Match Details from __NEXT_DATA__
#'
#' Extracts match metadata from any match page's embedded data.
#'
#' @param data List from __NEXT_DATA__ extraction
#' @param match_id Integer match objectId
#' @return Single-row data.frame of match details, or NULL
#' @keywords internal
parse_cricinfo_details <- function(data, match_id) {
  match <- data$match
  if (is.null(match)) return(NULL)

  ground <- match$ground %||% list()
  teams <- match$teams %||% list()
  team1 <- if (length(teams) >= 1) teams[[1]] else list()
  team2 <- if (length(teams) >= 2) teams[[2]] else list()

  # Toss info may be nested under tpiToss/toss or at match level
  toss <- match$tpiToss %||% match$toss %||% list()

  row <- list(
    match_id        = match_id,
    object_id       = to_scalar(match$objectId),
    slug            = to_scalar(match$slug),
    title           = to_scalar(match$title),
    format          = to_scalar(match$format),
    status          = to_scalar(match$status),
    status_text     = to_scalar(match$statusText),
    stage           = to_scalar(match$stage),
    start_date      = to_scalar(match$startDate),
    end_date        = to_scalar(match$endDate),
    start_time      = to_scalar(match$startTime),
    ground_id       = to_scalar(ground$id),
    ground_name     = to_scalar(ground$name %||% ground$longName),
    ground_city     = to_scalar(ground$town$name %||% ground$city),
    ground_country  = to_scalar(if (is.list(ground$country)) ground$country$name else ground$country),
    team1_id        = to_scalar(team1$team$id),
    team1_name      = to_scalar(team1$team$name %||% team1$team$longName),
    team2_id        = to_scalar(team2$team$id),
    team2_name      = to_scalar(team2$team$name %||% team2$team$longName),
    toss_winner_id  = to_scalar(toss$tossWinnerId %||% match$tossWinnerTeamId),
    toss_decision   = to_scalar(toss$tossDecision %||% toss$decision %||% match$tossWinnerChoice),
    result          = to_scalar(match$result),
    result_text     = to_scalar(match$resultText %||% match$statusText)
  )
  row <- lapply(row, function(x) if (is.null(x) || length(x) == 0 || is.list(x)) NA else x)
  as.data.frame(row, stringsAsFactors = FALSE)
}

#' Parse Over-Level Summaries from Scorecard
#'
#' Extracts per-over run/wicket totals from the scorecard page.
#' This data is available for ALL overs (unlike ball-level data).
#'
#' @param data List from __NEXT_DATA__ extraction (scorecard page)
#' @param match_id Integer match objectId
#' @return data.frame of over summaries, or NULL
#' @keywords internal
parse_cricinfo_over_summaries <- function(data, match_id) {
  innings_list <- data$content$innings
  if (is.null(innings_list) || length(innings_list) == 0) return(NULL)

  all_overs <- list()
  for (inn in innings_list) {
    if (is.null(inn$inningOvers)) next
    for (ov in inn$inningOvers) {
      row <- list(
        match_id       = match_id,
        innings        = inn$inningNumber,
        team           = inn$team$name %||% NA,
        over_number    = ov$overNumber,
        over_runs      = ov$overRuns %||% 0L,
        over_wickets   = ov$overWickets %||% 0L,
        total_runs     = ov$totalRuns %||% NA,
        total_wickets  = ov$totalWickets %||% NA,
        is_complete    = ov$isComplete %||% NA
      )
      row <- lapply(row, function(x) if (is.null(x) || length(x) == 0 || is.list(x)) NA else x)
      all_overs[[length(all_overs) + 1]] <- as.data.frame(row, stringsAsFactors = FALSE)
    }
  }

  if (length(all_overs) == 0) return(NULL)
  dplyr::bind_rows(all_overs)
}

#' Parse Cricinfo Series Schedule from __NEXT_DATA__
#'
#' Extracts match listing from a series schedule page.
#'
#' @param data List from __NEXT_DATA__ extraction (schedule page)
#' @param series_id Integer series objectId
#' @return data.frame of matches in the series, or NULL
#' @keywords internal
parse_cricinfo_schedule <- function(data, series_id) {
  matches <- data$content$matches
  if (is.null(matches) || length(matches) == 0) return(NULL)

  all_matches <- list()

  for (m in matches) {
    ground <- m$ground %||% list()
    teams <- m$teams %||% list()
    team1 <- if (length(teams) >= 1) teams[[1]] else list()
    team2 <- if (length(teams) >= 2) teams[[2]] else list()

    row <- list(
      match_id    = m$objectId %||% m$id,
      series_id   = series_id,
      title       = m$title %||% NA,
      slug        = m$slug %||% NA,
      format      = m$format %||% NA,
      status      = m$status %||% NA,
      stage       = m$stage %||% NA,
      start_date  = m$startDate %||% NA,
      end_date    = m$endDate %||% NA,
      ground_name = ground$name %||% ground$longName %||% NA,
      ground_city = ground$town$name %||% ground$city %||% NA,
      team1_id    = team1$team$id %||% NA,
      team1_name  = team1$team$name %||% team1$team$longName %||% NA,
      team2_id    = team2$team$id %||% NA,
      team2_name  = team2$team$name %||% team2$team$longName %||% NA
    )
    row <- lapply(row, function(x) if (is.null(x) || length(x) == 0 || is.list(x)) NA else x)
    all_matches[[length(all_matches) + 1]] <- as.data.frame(row, stringsAsFactors = FALSE)
  }

  if (length(all_matches) == 0) return(NULL)
  dplyr::bind_rows(all_matches)
}

#' Build Player ID to Name Map from Scorecard Data
#'
#' Creates a lookup table from player IDs to long names using the
#' batting and bowling entries in the scorecard innings.
#'
#' @param data List from __NEXT_DATA__ extraction (scorecard page)
#' @return Named list (character ID -> character name)
#' @keywords internal
cricinfo_player_map <- function(data) {
  player_map <- list()
  innings <- data$content$innings
  if (is.null(innings)) return(player_map)

  for (inn in innings) {
    for (bat in inn$inningBatsmen %||% list()) {
      pid <- bat$player$id %||% bat$player$objectId
      pname <- bat$player$longName %||% bat$player$name
      if (!is.null(pid) && !is.null(pname)) {
        player_map[[as.character(pid)]] <- pname
      }
    }
    for (bowl in inn$inningBowlers %||% list()) {
      pid <- bowl$player$id %||% bowl$player$objectId
      pname <- bowl$player$longName %||% bowl$player$name
      if (!is.null(pid) && !is.null(pname)) {
        player_map[[as.character(pid)]] <- pname
      }
    }
  }

  player_map
}

# ============================================================================
# DISCOVERY FUNCTIONS
# ============================================================================

#' Discover Series for a Season from ESPN Cricinfo
#'
#' Navigates to the old-style season index page and extracts series IDs
#' and names from the HTML links. Returns a deduplicated data.frame of
#' all series listed for the given season.
#'
#' @param browser A ChromoteSession object
#' @param season Character season string (e.g., "2025", "2025%2F26" for 2025/26)
#' @return data.frame with columns: series_id, name, url. Or NULL on failure.
#'
#' @examples
#' \dontrun{
#' browser <- chromote::ChromoteSession$new()
#' series <- cricinfo_discover_series(browser, season = "2025")
#' series <- cricinfo_discover_series(browser, season = "2024%2F25")
#' }
#'
#' @export
cricinfo_discover_series <- function(browser, season = "2025") {
  url <- sprintf(
    "https://www.espncricinfo.com/ci/engine/series/index.html?season=%s;view=season",
    season
  )

  browser$Page$navigate(url)
  tryCatch(
    browser$Page$loadEventFired(wait_ = TRUE, timeout_ = 15),
    error = function(e) Sys.sleep(5)
  )
  Sys.sleep(1)  # brief buffer for old-style page

  series_data <- tryCatch({
    r <- browser$Runtime$evaluate('
      (function() {
        var links = document.querySelectorAll("a[href*=\\"/series/\\"]");
        var seen = {};
        var result = [];
        links.forEach(function(a) {
          var href = a.href;
          var match = href.match(/\\/series\\/[^/]+-([0-9]+)/);
          if (!match) return;
          var id = match[1];
          if (seen[id]) return;
          var text = a.textContent.trim();
          if (!text || text.length < 3) return;
          // Skip non-series sub-page links (stats, squads, photos, etc.)
          var skip = /\\/(squads|points-table|stats|most-valuable|fan-ratings|videos|teams|photo)/.test(href);
          if (skip) return;
          seen[id] = true;
          // Strip /match-schedule-fixtures suffix from URL
          var cleanUrl = href.replace(/\\/match-schedule-fixtures.*$/, "");
          result.push({
            series_id: parseInt(id),
            name: text,
            url: cleanUrl
          });
        });
        return result;
      })()
    ', returnByValue = TRUE, timeout_ = 15)
    r$result$value
  }, error = function(e) NULL)

  if (is.null(series_data) || length(series_data) == 0) return(NULL)

  rows <- lapply(series_data, function(s) {
    data.frame(
      series_id = s$series_id,
      name = s$name %||% NA_character_,
      url = s$url %||% NA_character_,
      stringsAsFactors = FALSE
    )
  })
  dplyr::bind_rows(rows)
}

#' Discover Matches in a Cricinfo Series
#'
#' Navigates to the series schedule page and extracts match listing
#' from the embedded Next.js data.
#'
#' @param browser A ChromoteSession object
#' @param series_id Integer series objectId
#' @return data.frame of matches, or NULL
#'
#' @examples
#' \dontrun{
#' browser <- chromote::ChromoteSession$new()
#' matches <- cricinfo_discover_matches(browser, series_id = 1455609)
#' }
#'
#' @export
cricinfo_discover_matches <- function(browser, series_id) {
  url <- cricinfo_series_url(series_id)
  data <- cricinfo_navigate_and_extract(browser, url)
  if (is.null(data)) return(NULL)
  parse_cricinfo_schedule(data, series_id)
}

#' List Currently Live Matches on Cricinfo
#'
#' Uses XHR to fetch the current matches endpoint (the only endpoint
#' that works via direct API call).
#'
#' @param browser A ChromoteSession object
#' @param auth_token Auth token from cricinfo_get_auth_token()
#' @return List with current match data, or NULL
#'
#' @examples
#' \dontrun{
#' browser <- chromote::ChromoteSession$new()
#' browser$Network$enable()
#' token <- cricinfo_get_auth_token(browser)
#' current <- cricinfo_list_current(browser, token)
#' }
#'
#' @export
cricinfo_list_current <- function(browser, auth_token) {
  url <- paste0(CRICINFO_API_BASE, "/v1/pages/matches/current?lang=en&latest=true")

  js_xhr <- sprintf('
    (function() {
      try {
        var xhr = new XMLHttpRequest();
        xhr.open("GET", "%s", false);
        xhr.setRequestHeader("x-hsci-auth-token", "%s");
        xhr.send();
        if (xhr.status === 200) {
          return JSON.parse(xhr.responseText);
        }
        return null;
      } catch(e) { return null; }
    })()
  ', url, auth_token)

  tryCatch({
    result <- browser$Runtime$evaluate(js_xhr, returnByValue = TRUE, timeout_ = 30)
    result$result$value
  }, error = function(e) NULL)
}

# ============================================================================
# DATA FETCHING
# ============================================================================

#' Fetch All Data for a Single Cricinfo Match
#'
#' Downloads scorecard, match details, over summaries, and ball-by-ball
#' commentary for a single match. Uses two page loads:
#' 1. Scorecard page: extracts scorecard, details, over summaries, player map
#' 2. Commentary page: captures ball-by-ball via scroll + CDP interception
#'
#' @param browser A ChromoteSession object with Network enabled
#' @param series_id Integer series objectId
#' @param match_id Integer match objectId
#' @param include_commentary Fetch ball-by-ball commentary via scroll capture
#'   (default FALSE). Set to TRUE for matches where you want pitch/shot/wagon data.
#'   Commentary scraping takes ~2 minutes per match due to scrolling.
#' @param include_scorecard Fetch scorecard data (default TRUE)
#' @param max_scrolls Maximum scroll actions for commentary (default 50)
#' @return List with $commentary, $scorecard, $details, $over_summaries
#'
#' @examples
#' \dontrun{
#' browser <- chromote::ChromoteSession$new()
#' browser$Network$enable()
#' cricinfo_apply_stealth(browser)
#'
#' # Fetch scorecard only (fast)
#' match <- cricinfo_fetch_match(browser, series_id = 1455609, match_id = 1473561)
#'
#' # Fetch with ball-by-ball commentary (slower, requires scrolling)
#' match <- cricinfo_fetch_match(browser, series_id = 1455609, match_id = 1473561,
#'                               include_commentary = TRUE)
#' }
#'
#' @export
cricinfo_fetch_match <- function(browser, series_id, match_id,
                                  include_commentary = FALSE,
                                  include_scorecard = TRUE,
                                  max_scrolls = 50) {
  result <- list(
    commentary = NULL, scorecard = NULL, details = NULL,
    over_summaries = NULL, player_map = NULL
  )

  # Step 1: Scorecard page (metadata + scorecard + over summaries + player map)
  if (include_scorecard) {
    sc_url <- cricinfo_match_url(series_id, match_id, "full-scorecard")
    sc_data <- cricinfo_navigate_and_extract(browser, sc_url)

    if (!is.null(sc_data)) {
      result$details <- parse_cricinfo_details(sc_data, match_id)
      result$scorecard <- parse_cricinfo_scorecard(sc_data, match_id)
      result$over_summaries <- parse_cricinfo_over_summaries(sc_data, match_id)
      result$player_map <- cricinfo_player_map(sc_data)
    }
  }

  # Step 2: Commentary page (ball-by-ball via scroll capture)
  if (include_commentary) {
    Sys.sleep(1)
    capture <- cricinfo_capture_commentary(
      browser, series_id, match_id, max_scrolls = max_scrolls
    )

    all_commentary <- list()
    player_map <- result$player_map

    # Parse __NEXT_DATA__ initial commentary
    if (!is.null(capture$initial_data) && !is.null(capture$initial_data$content$comments)) {
      initial_df <- parse_cricinfo_commentary(
        capture$initial_data$content, match_id, series_id, player_map
      )
      if (!is.null(initial_df)) all_commentary[[length(all_commentary) + 1]] <- initial_df

      # Also extract details if we skipped scorecard
      if (is.null(result$details)) {
        result$details <- parse_cricinfo_details(capture$initial_data, match_id)
      }
    }

    # Parse API responses from scroll capture
    for (body_text in capture$api_responses) {
      parsed <- tryCatch(
        jsonlite::fromJSON(body_text, simplifyVector = FALSE),
        error = function(e) NULL
      )
      if (!is.null(parsed)) {
        api_df <- parse_cricinfo_commentary(parsed, match_id, series_id, player_map)
        if (!is.null(api_df)) all_commentary[[length(all_commentary) + 1]] <- api_df
      }
    }

    if (length(all_commentary) > 0) {
      combined <- dplyr::bind_rows(all_commentary)
      # Remove duplicates (initial data may overlap with first API response)
      if (nrow(combined) > 0 && "over" %in% names(combined)) {
        combined <- combined[!duplicated(combined[, c("innings", "over", "ball_number")]), ]
      }
      result$commentary <- combined
    }
  }

  result
}

# ============================================================================
# BATCH FETCHING
# ============================================================================

#' Fetch Multiple Cricinfo Matches with Rate Limiting
#'
#' Downloads data for multiple matches from a series with automatic
#' rate limiting and browser reconnection on failure.
#'
#' @param browser A ChromoteSession object with Network enabled
#' @param series_id Integer series objectId
#' @param match_ids Integer vector of match objectIds
#' @param output_dir Base directory to save match files (default: bouncerdata/cricinfo)
#' @param format_subdir Subdirectory name for format (e.g., "t20i", "test")
#' @param include_commentary Fetch ball-by-ball commentary via scroll capture
#'   (default FALSE). Set to TRUE for matches where you want pitch/shot/wagon data.
#' @param include_scorecard Fetch scorecard data (default TRUE)
#' @param skip_existing Skip completed matches that already have saved details
#' @param delay_between Seconds to wait between matches (default 8)
#' @param max_consecutive_failures Stop and reconnect after this many failures (default 5)
#' @return data.frame of all combined commentary data, or NULL
#'
#' @examples
#' \dontrun{
#' browser <- chromote::ChromoteSession$new()
#' browser$Network$enable()
#' cricinfo_apply_stealth(browser)
#'
#' match_ids <- c(1473561, 1473562, 1473563)
#' data <- cricinfo_fetch_matches(browser, series_id = 1455609,
#'                                match_ids = match_ids,
#'                                format_subdir = "t20i")
#' }
#'
#' @export
cricinfo_fetch_matches <- function(browser, series_id, match_ids,
                                    output_dir = "../bouncerdata/cricinfo",
                                    format_subdir = NULL,
                                    include_commentary = FALSE,
                                    include_scorecard = TRUE,
                                    skip_existing = TRUE,
                                    delay_between = 8,
                                    max_consecutive_failures = 5) {
  use_parquet <- requireNamespace("arrow", quietly = TRUE)
  file_ext <- if (use_parquet) ".parquet" else ".rds"

  format_dir <- if (!is.null(format_subdir)) {
    file.path(output_dir, tolower(format_subdir))
  } else {
    output_dir
  }
  if (!dir.exists(format_dir)) dir.create(format_dir, recursive = TRUE)

  results <- list()
  skipped <- 0
  fetched_count <- 0
  consecutive_failures <- 0
  current_browser <- browser

  for (i in seq_along(match_ids)) {
    mid <- match_ids[i]

    # Check for existing files - only skip if match was completed (RESULT)
    # This ensures matches scraped while live get re-scraped once finished
    if (skip_existing) {
      details_parquet <- file.path(format_dir, paste0(mid, "_details.parquet"))
      details_rds <- file.path(format_dir, paste0(mid, "_details.rds"))
      match_complete <- FALSE

      if (file.exists(details_parquet)) {
        tryCatch({
          det <- arrow::read_parquet(details_parquet)
          match_complete <- !is.na(det$status[1]) &&
            toupper(det$status[1]) %in% c("RESULT", "NO RESULT")
        }, error = function(e) NULL)
      } else if (file.exists(details_rds)) {
        tryCatch({
          det <- readRDS(details_rds)
          match_complete <- !is.na(det$status[1]) &&
            toupper(det$status[1]) %in% c("RESULT", "NO RESULT")
        }, error = function(e) NULL)
      }

      if (match_complete) {
        skipped <- skipped + 1
        next
      }
    }

    # Reconnect on too many failures
    if (consecutive_failures >= max_consecutive_failures) {
      cli::cli_alert_warning("Too many consecutive failures - reconnecting browser...")
      tryCatch(current_browser$close(), error = function(e) NULL)
      Sys.sleep(2)

      reconnect_failed <- FALSE
      tryCatch({
        current_browser <- chromote::ChromoteSession$new()
        current_browser$Network$enable()
        Sys.sleep(2)
        cricinfo_apply_stealth(current_browser)
        consecutive_failures <- 0
        cli::cli_alert_success("Reconnected successfully!")
      }, error = function(e) {
        cli::cli_alert_danger("Failed to reconnect: {e$message}")
        reconnect_failed <<- TRUE
      })

      if (reconnect_failed) break
      Sys.sleep(3)
    }

    cli::cli_alert("[{i}/{length(match_ids)}] Fetching match {mid}...")

    match_result <- tryCatch({
      cricinfo_fetch_match(
        current_browser, series_id, mid,
        include_commentary = include_commentary,
        include_scorecard = include_scorecard
      )
    }, error = function(e) {
      cli::cli_alert_warning("  Error: {e$message}")
      NULL
    })

    # Save results
    save_data <- function(data, suffix) {
      if (is.null(data)) return(FALSE)
      if (is.list(data) && !is.data.frame(data)) {
        saved_any <- FALSE
        for (nm in names(data)) {
          if (!is.null(data[[nm]]) && is.data.frame(data[[nm]]) && nrow(data[[nm]]) > 0) {
            out_path <- file.path(format_dir, paste0(mid, "_", suffix, "_", nm, file_ext))
            if (use_parquet) arrow::write_parquet(data[[nm]], out_path)
            else saveRDS(data[[nm]], out_path)
            saved_any <- TRUE
          }
        }
        return(saved_any)
      }
      if (!is.data.frame(data) || nrow(data) == 0) return(FALSE)
      out_path <- file.path(format_dir, paste0(mid, "_", suffix, file_ext))
      if (use_parquet) arrow::write_parquet(data, out_path) else saveRDS(data, out_path)
      TRUE
    }

    saved_parts <- c()
    if (!is.null(match_result)) {
      if (save_data(match_result$commentary, "commentary")) {
        results[[as.character(mid)]] <- match_result$commentary
        saved_parts <- c(saved_parts, "commentary")
      }
      if (save_data(match_result$scorecard, "scorecard")) {
        saved_parts <- c(saved_parts, "scorecard")
      }
      if (save_data(match_result$details, "details")) {
        saved_parts <- c(saved_parts, "details")
      }
      if (save_data(match_result$over_summaries, "over_summaries")) {
        saved_parts <- c(saved_parts, "over_summaries")
      }
    }

    if (length(saved_parts) > 0) {
      cli::cli_alert_success("  Saved: {paste(saved_parts, collapse = ', ')}")
      fetched_count <- fetched_count + 1
      consecutive_failures <- 0
    } else {
      cli::cli_alert_warning("  No data found")
      consecutive_failures <- consecutive_failures + 1
    }

    if (i < length(match_ids)) {
      Sys.sleep(delay_between + runif(1, 1, 3))
    }
  }

  if (skipped > 0) cli::cli_alert_info("Skipped {skipped} already-downloaded matches")

  if (!identical(current_browser, browser)) {
    cli::cli_alert_info("Note: Browser was reconnected. Access with bouncer:::cricinfo_get_browser()")
    .cricinfo_env$browser <- current_browser
  }

  if (length(results) > 0) {
    all_data <- dplyr::bind_rows(results)
    cli::cli_alert_success("Fetched {nrow(all_data)} balls from {length(results)} matches")
    return(all_data)
  }

  return(NULL)
}

# ============================================================================
# COMBINATION & STORAGE
# ============================================================================

#' Combine Downloaded Cricinfo Match Files
#'
#' Reads individual match files and combines them into single files per data type.
#'
#' @param format_subdir Subdirectory name (e.g., "t20i", "test", "odi")
#' @param output_dir Base directory containing match files (default: bouncerdata/cricinfo)
#' @return Combined commentary data.frame, or NULL
#'
#' @examples
#' \dontrun{
#' # Combine all downloaded T20I match files into single files
#' combined <- cricinfo_combine_matches("t20i")
#'
#' # Combine Test match files from a custom directory
#' combined <- cricinfo_combine_matches("test",
#'                                      output_dir = "../bouncerdata/cricinfo")
#' }
#'
#' @export
cricinfo_combine_matches <- function(format_subdir, output_dir = "../bouncerdata/cricinfo") {
  use_parquet <- requireNamespace("arrow", quietly = TRUE)
  file_ext <- if (use_parquet) ".parquet" else ".rds"
  format_dir <- file.path(output_dir, tolower(format_subdir))

  combine_files <- function(pattern_suffix, output_name) {
    rds_files <- list.files(format_dir, pattern = paste0("_", pattern_suffix, "\\.rds$"),
                            full.names = TRUE)
    parquet_files <- list.files(format_dir, pattern = paste0("_", pattern_suffix, "\\.parquet$"),
                                full.names = TRUE)

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

    if (use_parquet) arrow::write_parquet(combined, output_file)
    else saveRDS(combined, output_file)

    list(data = combined, file = output_file, count = length(data_list))
  }

  commentary_result <- combine_files("commentary",
                                     paste0("all_", tolower(format_subdir), "_commentary"))
  if (is.null(commentary_result)) {
    cli::cli_alert_warning("No {format_subdir} commentary files found in {format_dir}")
    return(NULL)
  }

  cli::cli_alert_success("Combined {commentary_result$count} matches, {nrow(commentary_result$data)} total balls")
  cli::cli_alert_info("Saved to: {commentary_result$file}")

  for (suffix in c("scorecard_batting", "scorecard_bowling", "details", "over_summaries")) {
    result <- combine_files(suffix, paste0("all_", tolower(format_subdir), "_", suffix))
    if (!is.null(result)) {
      cli::cli_alert_success("Combined {result$count} {suffix} files")
    }
  }

  return(commentary_result$data)
}

#' List Available Cricinfo Formats
#'
#' Returns the format codes supported by the Cricinfo scraper.
#'
#' @param details If TRUE, return a data.frame with format details.
#'   If FALSE (default), return just the format codes.
#' @return Character vector or data.frame
#'
#' @examples
#' cricinfo_list_formats()
#' cricinfo_list_formats(details = TRUE)
#'
#' @export
cricinfo_list_formats <- function(details = FALSE) {
  if (details) {
    data.frame(
      format = names(CRICINFO_FORMATS),
      name = vapply(CRICINFO_FORMATS, function(x) x$name, character(1)),
      max_innings = vapply(CRICINFO_FORMATS, function(x) x$max_innings, integer(1)),
      row.names = NULL
    )
  } else {
    names(CRICINFO_FORMATS)
  }
}
