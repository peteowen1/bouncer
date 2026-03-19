# Backfill Historical Weather ----
#
# Fetches historical weather for all cricket matches using Open-Meteo archive API.
# Groups matches by venue to minimize API calls (one call per venue covers all dates).
# Free API, no key needed.
#
# Prerequisites: Run 01_geocode_venues.R first.
# Results stored in DuckDB: main.match_weather

library(DBI)
library(data.table)
devtools::load_all()

cli::cli_h1("Backfill Historical Match Weather")

conn <- get_db_connection(read_only = TRUE)

# Load venue coordinates
coords <- load_venue_coordinates(conn)
if (is.null(coords) || nrow(coords) == 0) {
  stop("No venue coordinates found. Run 01_geocode_venues.R first.")
}
cli::cli_alert_success("Loaded coordinates for {nrow(coords)} venues")

# Load all matches with their format (to determine match duration)
matches <- DBI::dbGetQuery(conn, "
  SELECT match_id, venue, match_date, LOWER(match_type) AS match_type
  FROM cricsheet.matches
  WHERE outcome_type IS NOT NULL
    AND match_date IS NOT NULL
    AND match_date < CURRENT_DATE
  ORDER BY match_date
")
setDT(matches)
DBI::dbDisconnect(conn, shutdown = TRUE)

# Match duration by format
matches[, match_days := fcase(
  match_type %in% c("test", "mdm"), 5L,
  match_type %in% c("odi", "odm"), 1L,
  match_type %in% c("t20", "it20"), 1L,
  default = 1L
)]

# Join coordinates
matches <- merge(matches, coords, by = "venue", all.x = TRUE)
has_coords <- sum(!is.na(matches$latitude))
cli::cli_alert_info("Matches with coordinates: {has_coords}/{nrow(matches)} ({round(has_coords/nrow(matches)*100, 1)}%)")

# Filter to matches with coordinates only
matches <- matches[!is.na(latitude)]

# Check what's already fetched
existing_weather <- load_match_weather()
if (!is.null(existing_weather) && nrow(existing_weather) > 0) {
  already_done <- matches$match_id %in% existing_weather$match_id
  cli::cli_alert_info("{sum(already_done)} matches already have weather, {sum(!already_done)} remaining")
  matches <- matches[!already_done]
}

if (nrow(matches) == 0) {
  cli::cli_alert_success("All matches already have weather data!")
  quit(save = "no")
}

# Group by venue for efficient API calls
venue_groups <- matches[, .(
  n_matches = .N,
  min_date = min(match_date),
  max_date = max(match_date),
  lat = latitude[1],
  lon = longitude[1]
), by = venue]

cli::cli_alert_info("Fetching weather for {nrow(venue_groups)} venues, {nrow(matches)} matches")
cli::cli_alert_info("Open-Meteo archive API (free, no key needed)")

# Fetch weather venue by venue
all_weather <- list()
venues_done <- 0
matches_done <- 0

for (i in seq_len(nrow(venue_groups))) {
  vg <- venue_groups[i]

  venue_matches <- matches[venue == vg$venue, .(match_id, match_date, match_days)]

  cat(sprintf("[%d/%d] %s (%d matches, %s to %s)\n",
              i, nrow(venue_groups), vg$venue, vg$n_matches,
              vg$min_date, vg$max_date))

  weather <- tryCatch(
    fetch_venue_weather_batch(vg$lat, vg$lon, venue_matches),
    error = function(e) {
      cli::cli_alert_warning("  Failed: {conditionMessage(e)}")
      NULL
    }
  )

  if (!is.null(weather) && nrow(weather) > 0) {
    # Add venue and match metadata
    weather$venue <- vg$venue
    weather_dt <- data.table::setDT(weather)
    weather_dt <- merge(weather_dt,
                        matches[venue == vg$venue, .(match_id, match_type, match_date)],
                        by = "match_id", all.x = TRUE)

    # Derived features
    weather_dt[, `:=`(
      is_rain = precipitation_total > 0.5,
      log_precip = log1p(precipitation_total),
      log_wind = log1p(wind_avg)
    )]

    all_weather[[i]] <- weather_dt
    matches_done <- matches_done + nrow(weather_dt)
  }

  venues_done <- venues_done + 1

  # Save progress every 100 venues
  if (venues_done %% 100 == 0 && length(all_weather) > 0) {
    batch_weather <- data.table::rbindlist(Filter(Negate(is.null), all_weather), fill = TRUE)
    save_match_weather(batch_weather)
    cli::cli_alert_success("  Checkpoint: saved {nrow(batch_weather)} matches")
    all_weather <- list()  # Reset to avoid double-saving
  }

  # Rate limit: Open-Meteo allows ~600 req/min for free tier
  Sys.sleep(2)  # Open-Meteo free tier: ~600 req/min, be conservative
}

# Save remaining
if (length(all_weather) > 0) {
  remaining <- data.table::rbindlist(Filter(Negate(is.null), all_weather), fill = TRUE)
  if (nrow(remaining) > 0) {
    save_match_weather(remaining)
  }
}

# Summary
cli::cli_h2("Summary")
all_weather_data <- load_match_weather()
if (!is.null(all_weather_data)) {
  cli::cli_alert_success("Total matches with weather: {nrow(all_weather_data)}")

  # Weather stats
  cat(sprintf("  Temperature range: %.1f to %.1f C\n",
              min(all_weather_data$temp_min, na.rm = TRUE),
              max(all_weather_data$temp_max, na.rm = TRUE)))
  cat(sprintf("  Rain matches: %d (%.1f%%)\n",
              sum(all_weather_data$is_rain, na.rm = TRUE),
              mean(all_weather_data$is_rain, na.rm = TRUE) * 100))

  # By format
  by_format <- all_weather_data[, .(
    n = .N,
    avg_precip = round(mean(precipitation_total, na.rm = TRUE), 1),
    pct_rain = round(mean(is_rain, na.rm = TRUE) * 100, 1)
  ), by = match_type]
  cat("\nBy format:\n")
  print(by_format)
}
