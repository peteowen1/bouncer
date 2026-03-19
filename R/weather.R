# Weather Data Functions ----
#
# Fetch and manage weather data for cricket matches using Open-Meteo API.
# Adapted from torpverse/torp weather workflow.
#
# Architecture:
#   - Venue coordinates: geocoded via Nominatim (OpenStreetMap), stored in DuckDB
#   - Match weather: fetched via Open-Meteo daily API, stored in DuckDB
#   - No API keys required (both APIs are free)


# ============================================================================
# Geocoding
# ============================================================================

#' Geocode a Venue Using Nominatim (OpenStreetMap)
#'
#' @param venue_name Character. Venue name (e.g., "Adelaide Oval").
#' @param city Character. City name (optional, improves accuracy).
#' @param max_retries Integer. Maximum retry attempts.
#'
#' @return A list with `lat` and `lon`, or `list(lat = NA, lon = NA)` on failure.
#'
#' @keywords internal
geocode_venue <- function(venue_name, city = NULL, max_retries = 3) {
  # Build search query — venue + city gives best results for cricket grounds

  query <- venue_name
  if (!is.null(city) && !is.na(city) && nchar(city) > 0) {
    query <- paste(venue_name, city, sep = ", ")
  }

  for (attempt in seq_len(max_retries)) {
    result <- tryCatch({
      resp <- httr2::request("https://nominatim.openstreetmap.org/search") |>
        httr2::req_url_query(
          q = query,
          format = "json",
          limit = 1
        ) |>
        httr2::req_headers(`User-Agent` = "bouncer-cricket-analytics/0.3.0") |>
        httr2::req_retry(max_tries = 2, backoff = ~ 2) |>
        httr2::req_perform()

      json <- httr2::resp_body_json(resp)

      if (length(json) > 0) {
        return(list(
          lat = as.numeric(json[[1]]$lat),
          lon = as.numeric(json[[1]]$lon)
        ))
      }

      # If full query fails, try just the city
      if (!is.null(city) && !is.na(city) && nchar(city) > 0 && attempt == 1) {
        Sys.sleep(1.1)  # Nominatim rate limit: 1 req/sec
        resp2 <- httr2::request("https://nominatim.openstreetmap.org/search") |>
          httr2::req_url_query(q = city, format = "json", limit = 1) |>
          httr2::req_headers(`User-Agent` = "bouncer-cricket-analytics/0.3.0") |>
          httr2::req_perform()

        json2 <- httr2::resp_body_json(resp2)
        if (length(json2) > 0) {
          return(list(
            lat = as.numeric(json2[[1]]$lat),
            lon = as.numeric(json2[[1]]$lon)
          ))
        }
      }

      list(lat = NA_real_, lon = NA_real_)
    }, error = function(e) {
      if (attempt < max_retries) {
        Sys.sleep(2)
        NULL
      } else {
        list(lat = NA_real_, lon = NA_real_)
      }
    })

    if (!is.null(result)) return(result)
  }

  list(lat = NA_real_, lon = NA_real_)
}


#' Geocode Multiple Venues
#'
#' Geocodes a data frame of venues with rate limiting for Nominatim API.
#'
#' @param venues data.frame with columns `venue` and optionally `city`.
#' @param delay Numeric. Seconds between API calls (Nominatim requires >= 1).
#'
#' @return The input data frame with `latitude` and `longitude` columns added.
#'
#' @export
geocode_venues <- function(venues, delay = 1.1) {
  if (!"venue" %in% names(venues)) {
    cli::cli_abort("venues must have a 'venue' column")
  }

  city_col <- if ("city" %in% names(venues)) venues$city else rep(NA_character_, nrow(venues))

  lat <- numeric(nrow(venues))
  lon <- numeric(nrow(venues))

  for (i in seq_len(nrow(venues))) {
    if (i > 1) Sys.sleep(delay)

    coords <- geocode_venue(venues$venue[i], city_col[i])
    lat[i] <- coords$lat
    lon[i] <- coords$lon

    if (i %% 25 == 0 || i == nrow(venues)) {
      n_ok <- sum(!is.na(lat[1:i]))
      cli::cli_alert_info("[{i}/{nrow(venues)}] Geocoded {n_ok}/{i} venues")
    }
  }

  venues$latitude <- lat
  venues$longitude <- lon
  venues
}


#' Load Venue Coordinates from Database
#'
#' @param conn DBI connection. If NULL, opens one.
#' @return data.table with venue, latitude, longitude.
#' @export
load_venue_coordinates <- function(conn = NULL) {
  own_conn <- is.null(conn)
  if (own_conn) conn <- get_db_connection(read_only = TRUE)
  on.exit(if (own_conn) DBI::dbDisconnect(conn, shutdown = TRUE))

  if (!table_exists(conn, "main.venue_coordinates")) {
    cli::cli_alert_warning("No venue_coordinates table. Run geocoding first.")
    return(NULL)
  }

  dt <- data.table::setDT(DBI::dbGetQuery(conn,
    "SELECT venue, latitude, longitude FROM main.venue_coordinates
     WHERE latitude IS NOT NULL"))
  dt
}


#' Save Venue Coordinates to Database
#'
#' @param coords data.frame with venue, latitude, longitude columns.
#' @param conn DBI connection. If NULL, opens one.
#' @keywords internal
save_venue_coordinates <- function(coords, conn = NULL) {
  own_conn <- is.null(conn)
  if (own_conn) conn <- get_db_connection(read_only = FALSE)
  on.exit(if (own_conn) DBI::dbDisconnect(conn, shutdown = TRUE))

  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS main")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS main.venue_coordinates (
      venue VARCHAR PRIMARY KEY,
      city VARCHAR,
      latitude DOUBLE,
      longitude DOUBLE,
      geocode_source VARCHAR,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )")

  # Write to temp table, then upsert
  temp_name <- paste0("coords_temp_", format(Sys.time(), "%H%M%S"))
  DBI::dbWriteTable(conn, temp_name, coords, overwrite = TRUE)

  DBI::dbExecute(conn, sprintf("
    DELETE FROM main.venue_coordinates
    WHERE venue IN (SELECT venue FROM %s)", temp_name))

  DBI::dbExecute(conn, sprintf("
    INSERT INTO main.venue_coordinates (venue, city, latitude, longitude, geocode_source)
    SELECT venue, city, latitude, longitude, geocode_source FROM %s", temp_name))

  DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", temp_name))

  cli::cli_alert_success("Saved {nrow(coords)} venue coordinates")
}


# ============================================================================
# Weather Fetching
# ============================================================================

#' Fetch Daily Weather from Open-Meteo Archive API
#'
#' Fetches historical daily weather data for a location and date range.
#' Uses the free Open-Meteo archive API (no API key required).
#'
#' @param lat Numeric. Latitude.
#' @param lon Numeric. Longitude.
#' @param start_date Date or character. Start date (YYYY-MM-DD).
#' @param end_date Date or character. End date (YYYY-MM-DD).
#'
#' @return data.frame with columns: date, temperature_max, temperature_min,
#'   precipitation_sum, wind_speed_max, rain_sum.
#'   Returns NULL on failure.
#'
#' @export
fetch_weather_openmeteo <- function(lat, lon, start_date, end_date) {
  resp <- tryCatch({
    httr2::request("https://archive-api.open-meteo.com/v1/archive") |>
      httr2::req_url_query(
        latitude = lat,
        longitude = lon,
        start_date = format(as.Date(start_date), "%Y-%m-%d"),
        end_date = format(as.Date(end_date), "%Y-%m-%d"),
        daily = paste(
          "temperature_2m_max", "temperature_2m_min",
          "precipitation_sum", "wind_speed_10m_max", "rain_sum",
          sep = ","
        ),
        timezone = "auto"
      ) |>
      httr2::req_retry(max_tries = 2, backoff = ~ 5) |>
      httr2::req_perform()
  }, error = function(e) {
    cli::cli_alert_warning("Weather fetch failed: {conditionMessage(e)}")
    return(NULL)
  })

  if (is.null(resp)) return(NULL)

  json <- httr2::resp_body_json(resp)

  if (is.null(json$daily) || length(json$daily$time) == 0) return(NULL)

  data.frame(
    date = as.Date(unlist(json$daily$time)),
    temperature_max = as.numeric(unlist(lapply(json$daily$temperature_2m_max, function(x) x %||% NA_real_))),
    temperature_min = as.numeric(unlist(lapply(json$daily$temperature_2m_min, function(x) x %||% NA_real_))),
    precipitation_sum = as.numeric(unlist(lapply(json$daily$precipitation_sum, function(x) x %||% 0))),
    wind_speed_max = as.numeric(unlist(lapply(json$daily$wind_speed_10m_max, function(x) x %||% NA_real_))),
    rain_sum = as.numeric(unlist(lapply(json$daily$rain_sum, function(x) x %||% 0))),
    stringsAsFactors = FALSE
  )
}


#' Fetch Weather for a Batch of Matches at One Venue
#'
#' Fetches daily weather for a venue's full date range and returns
#' match-level aggregated weather. Handles multi-day Test matches.
#'
#' @param lat Numeric. Venue latitude.
#' @param lon Numeric. Venue longitude.
#' @param match_dates data.frame with columns: match_id, match_date, match_days
#'   (number of days the match spans; 1 for T20/ODI, up to 5 for Test).
#'
#' @return data.frame with one row per match and weather features.
#'
#' @keywords internal
fetch_venue_weather_batch <- function(lat, lon, match_dates) {
  if (nrow(match_dates) == 0 || is.na(lat) || is.na(lon)) return(NULL)

  # Fetch the full date range for this venue in one API call
  min_date <- min(match_dates$match_date)
  max_date <- max(match_dates$match_date) + max(match_dates$match_days, na.rm = TRUE)

  daily <- fetch_weather_openmeteo(lat, lon, min_date, max_date)
  if (is.null(daily) || nrow(daily) == 0) return(NULL)

  # Aggregate per match
  results <- lapply(seq_len(nrow(match_dates)), function(i) {
    md <- match_dates[i, ]
    start <- as.Date(md$match_date)
    end <- start + md$match_days - 1

    window <- daily[daily$date >= start & daily$date <= end, ]
    if (nrow(window) == 0) return(NULL)

    data.frame(
      match_id = md$match_id,
      temp_avg = mean(c(window$temperature_max, window$temperature_min), na.rm = TRUE),
      temp_max = max(window$temperature_max, na.rm = TRUE),
      temp_min = min(window$temperature_min, na.rm = TRUE),
      precipitation_total = sum(window$precipitation_sum, na.rm = TRUE),
      rain_total = sum(window$rain_sum, na.rm = TRUE),
      wind_max = max(window$wind_speed_max, na.rm = TRUE),
      wind_avg = mean(window$wind_speed_max, na.rm = TRUE),
      rain_days = sum(window$rain_sum > 1, na.rm = TRUE),
      match_days_weather = nrow(window),
      stringsAsFactors = FALSE
    )
  })

  do.call(rbind, Filter(Negate(is.null), results))
}


# ============================================================================
# Match Weather Loading & Features
# ============================================================================

#' Load Match Weather Data
#'
#' Loads weather data from the database. If not available, returns NULL.
#'
#' @param conn DBI connection. If NULL, opens one.
#' @param format Character. Filter by match format (optional).
#'
#' @return data.table with match-level weather features, or NULL.
#'
#' @export
load_match_weather <- function(conn = NULL, format = NULL) {
  own_conn <- is.null(conn)
  if (own_conn) conn <- get_db_connection(read_only = TRUE)
  on.exit(if (own_conn) DBI::dbDisconnect(conn, shutdown = TRUE))

  if (!table_exists(conn, "main.match_weather")) {
    cli::cli_alert_warning("No match_weather table found")
    return(NULL)
  }

  query <- "SELECT * FROM main.match_weather"
  if (!is.null(format)) {
    query <- paste0(query, " WHERE match_type = '", tolower(format), "'")
  }

  dt <- data.table::setDT(DBI::dbGetQuery(conn, query))
  dt
}


#' Save Match Weather Data to Database
#'
#' @param weather data.frame with match weather data.
#' @param conn DBI connection. If NULL, opens one.
#' @keywords internal
save_match_weather <- function(weather, conn = NULL) {
  own_conn <- is.null(conn)
  if (own_conn) conn <- get_db_connection(read_only = FALSE)
  on.exit(if (own_conn) DBI::dbDisconnect(conn, shutdown = TRUE))

  DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS main")
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS main.match_weather (
      match_id VARCHAR PRIMARY KEY,
      venue VARCHAR,
      match_type VARCHAR,
      match_date DATE,
      temp_avg DOUBLE,
      temp_max DOUBLE,
      temp_min DOUBLE,
      precipitation_total DOUBLE,
      rain_total DOUBLE,
      wind_max DOUBLE,
      wind_avg DOUBLE,
      rain_days INTEGER,
      match_days_weather INTEGER,
      is_rain BOOLEAN,
      log_precip DOUBLE,
      log_wind DOUBLE
    )")

  # Delete existing rows for these matches, then insert
  temp_name <- paste0("weather_temp_", format(Sys.time(), "%H%M%S"))
  DBI::dbWriteTable(conn, temp_name, weather, overwrite = TRUE)

  DBI::dbExecute(conn, sprintf("
    DELETE FROM main.match_weather
    WHERE match_id IN (SELECT match_id FROM %s)", temp_name))

  DBI::dbExecute(conn, sprintf("
    INSERT INTO main.match_weather
    SELECT * FROM %s", temp_name))

  DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", temp_name))

  cli::cli_alert_success("Saved weather for {nrow(weather)} matches")
}


#' Prepare Weather Features for Modeling
#'
#' Joins weather data to a match data frame and creates derived features.
#' Uses median imputation for missing weather values (neutral contribution).
#'
#' @param match_data data.frame with at least a `match_id` column.
#' @param weather_data data.frame from load_match_weather(). If NULL,
#'   attempts to load from database.
#'
#' @return The input data frame with weather feature columns added:
#'   temp_avg, wind_avg, precipitation_total, rain_days, is_rain,
#'   log_precip, log_wind.
#'
#' @export
add_weather_features <- function(match_data, weather_data = NULL) {
  if (is.null(weather_data)) {
    weather_data <- load_match_weather()
  }

  if (is.null(weather_data) || nrow(weather_data) == 0) {
    cli::cli_alert_warning("No weather data available, adding NA columns")
    match_data$temp_avg <- NA_real_
    match_data$wind_avg <- NA_real_
    match_data$precipitation_total <- NA_real_
    match_data$rain_days <- NA_integer_
    match_data$is_rain <- NA
    match_data$log_precip <- NA_real_
    match_data$log_wind <- NA_real_
    return(match_data)
  }

  # Select weather columns for join
  weather_cols <- c("match_id", "temp_avg", "wind_avg", "precipitation_total",
                    "rain_days", "is_rain", "log_precip", "log_wind")
  weather_subset <- weather_data[, intersect(weather_cols, names(weather_data)),
                                  drop = FALSE]

  # Join
  result <- merge(match_data, weather_subset, by = "match_id", all.x = TRUE)

  # Median imputation for missing values (neutral contribution)
  for (col in c("temp_avg", "wind_avg")) {
    med <- stats::median(result[[col]], na.rm = TRUE)
    if (!is.na(med)) {
      result[[col]][is.na(result[[col]])] <- med
    }
  }
  result$precipitation_total[is.na(result$precipitation_total)] <- 0
  result$rain_days[is.na(result$rain_days)] <- 0L
  result$is_rain[is.na(result$is_rain)] <- FALSE
  result$log_precip[is.na(result$log_precip)] <- 0
  if ("log_wind" %in% names(result)) {
    med_wind <- stats::median(result$log_wind, na.rm = TRUE)
    if (!is.na(med_wind)) {
      result$log_wind[is.na(result$log_wind)] <- med_wind
    }
  }

  result
}


#' Fetch Forecast Weather for Upcoming Match
#'
#' Uses Open-Meteo forecast API for upcoming matches (within 16 days).
#'
#' @param lat Numeric. Venue latitude.
#' @param lon Numeric. Venue longitude.
#' @param match_date Date. Match start date.
#' @param match_days Integer. Number of match days (1 for T20/ODI, 5 for Test).
#'
#' @return data.frame with weather features, or NULL if unavailable.
#'
#' @export
fetch_forecast_weather <- function(lat, lon, match_date, match_days = 1) {
  end_date <- as.Date(match_date) + match_days - 1

  resp <- tryCatch({
    httr2::request("https://api.open-meteo.com/v1/forecast") |>
      httr2::req_url_query(
        latitude = lat,
        longitude = lon,
        daily = paste(
          "temperature_2m_max", "temperature_2m_min",
          "precipitation_sum", "wind_speed_10m_max", "rain_sum",
          sep = ","
        ),
        start_date = format(as.Date(match_date), "%Y-%m-%d"),
        end_date = format(end_date, "%Y-%m-%d"),
        timezone = "auto"
      ) |>
      httr2::req_retry(max_tries = 3, backoff = ~ 2) |>
      httr2::req_perform()
  }, error = function(e) {
    cli::cli_alert_warning("Forecast fetch failed: {conditionMessage(e)}")
    return(NULL)
  })

  if (is.null(resp)) return(NULL)

  json <- httr2::resp_body_json(resp)
  if (is.null(json$daily) || length(json$daily$time) == 0) return(NULL)

  daily <- data.frame(
    date = as.Date(unlist(json$daily$time)),
    temperature_max = as.numeric(unlist(lapply(json$daily$temperature_2m_max, function(x) x %||% NA_real_))),
    temperature_min = as.numeric(unlist(lapply(json$daily$temperature_2m_min, function(x) x %||% NA_real_))),
    precipitation_sum = as.numeric(unlist(lapply(json$daily$precipitation_sum, function(x) x %||% 0))),
    wind_speed_max = as.numeric(unlist(lapply(json$daily$wind_speed_10m_max, function(x) x %||% NA_real_))),
    rain_sum = as.numeric(unlist(lapply(json$daily$rain_sum, function(x) x %||% 0))),
    stringsAsFactors = FALSE
  )

  data.frame(
    temp_avg = mean(c(daily$temperature_max, daily$temperature_min), na.rm = TRUE),
    temp_max = max(daily$temperature_max, na.rm = TRUE),
    temp_min = min(daily$temperature_min, na.rm = TRUE),
    precipitation_total = sum(daily$precipitation_sum, na.rm = TRUE),
    rain_total = sum(daily$rain_sum, na.rm = TRUE),
    wind_max = max(daily$wind_speed_max, na.rm = TRUE),
    wind_avg = mean(daily$wind_speed_max, na.rm = TRUE),
    rain_days = sum(daily$rain_sum > 1, na.rm = TRUE),
    match_days_weather = nrow(daily),
    is_rain = sum(daily$rain_sum > 0.5, na.rm = TRUE) > 0,
    log_precip = log1p(sum(daily$precipitation_sum, na.rm = TRUE)),
    log_wind = log1p(mean(daily$wind_speed_max, na.rm = TRUE)),
    stringsAsFactors = FALSE
  )
}
