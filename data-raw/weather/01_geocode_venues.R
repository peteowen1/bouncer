# Geocode Cricket Venues ----
#
# Fetches lat/lon for all cricket venues using Nominatim (OpenStreetMap).
# Free API, no key needed. Rate-limited to 1 request/second.
#
# Run once, then incrementally for new venues.
# Results stored in DuckDB: main.venue_coordinates

library(DBI)
library(data.table)
devtools::load_all()

cli::cli_h1("Geocoding Cricket Venues")

conn <- get_db_connection(read_only = TRUE)

# Get all unique venues with their most common city
venues <- DBI::dbGetQuery(conn, "
  SELECT
    venue,
    MODE(city) AS city,
    COUNT(DISTINCT match_id) AS n_matches
  FROM cricsheet.matches
  WHERE venue IS NOT NULL AND venue != ''
  GROUP BY venue
  ORDER BY n_matches DESC
")
setDT(venues)
DBI::dbDisconnect(conn, shutdown = TRUE)

cli::cli_alert_info("Found {nrow(venues)} unique venues")

# Check what's already geocoded
existing <- load_venue_coordinates()
if (!is.null(existing) && nrow(existing) > 0) {
  already_done <- venues$venue %in% existing$venue
  cli::cli_alert_info("{sum(already_done)} already geocoded, {sum(!already_done)} remaining")
  venues <- venues[!already_done]
}

if (nrow(venues) == 0) {
  cli::cli_alert_success("All venues already geocoded!")
  quit(save = "no")
}

# Geocode in batches (save progress every 50 venues)
BATCH_SIZE <- 50

n_batches <- ceiling(nrow(venues) / BATCH_SIZE)
cli::cli_alert_info("Geocoding {nrow(venues)} venues in {n_batches} batches of {BATCH_SIZE}")
cli::cli_alert_info("Estimated time: ~{round(nrow(venues) * 1.2 / 60, 1)} minutes (1.1s per venue)")

for (batch_i in seq_len(n_batches)) {
  start_idx <- (batch_i - 1) * BATCH_SIZE + 1
  end_idx <- min(batch_i * BATCH_SIZE, nrow(venues))
  batch <- venues[start_idx:end_idx]

  cli::cli_h2("Batch {batch_i}/{n_batches} (venues {start_idx}-{end_idx})")

  geocoded <- geocode_venues(batch)

  # Add metadata
  geocoded$geocode_source <- "nominatim"

  n_ok <- sum(!is.na(geocoded$latitude))
  cli::cli_alert_info("Geocoded {n_ok}/{nrow(geocoded)} in this batch")

  # Save to DB
  save_coords <- geocoded[, .(venue, city, latitude, longitude, geocode_source)]
  save_venue_coordinates(save_coords)
}

# Summary
cli::cli_h2("Summary")
all_coords <- load_venue_coordinates()
if (!is.null(all_coords)) {
  n_total <- nrow(all_coords)
  n_ok <- sum(!is.na(all_coords$latitude))
  cli::cli_alert_success("Total: {n_ok}/{n_total} venues with coordinates")

  # Show coverage by match count
  conn <- get_db_connection(read_only = TRUE)
  match_coverage <- DBI::dbGetQuery(conn, "
    SELECT COUNT(DISTINCT m.match_id) AS n_matches
    FROM cricsheet.matches m
    JOIN main.venue_coordinates vc ON m.venue = vc.venue
    WHERE vc.latitude IS NOT NULL
  ")
  total_matches <- DBI::dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricsheet.matches")
  DBI::dbDisconnect(conn, shutdown = TRUE)

  pct <- round(match_coverage$n_matches / total_matches$n * 100, 1)
  cli::cli_alert_success("Match coverage: {match_coverage$n_matches}/{total_matches$n} ({pct}%)")
}
