# Fetch per-day weather for every Test venue (bouncerverse#72).
#
# WHY PER DAY. `main.match_weather` holds one row per match, so the only rain
# feature it can express is a match TOTAL -- future information at any ball
# before the match ends. Prorating that total by progress does not fix it
# (#24). "Rain that has already fallen" needs rows keyed by (venue, date).
#
# THE FETCH LAYER IS `wheather`, NOT BOUNCER. Pete's C:/dev/wheather is a
# dedicated Open-Meteo package and is strictly better than the client that had
# grown inside R/weather.R:
#
#   * 16 daily variables against 5, including `precip_hours` -- hours of rain
#     in a day, a far better proxy for lost playing time than millimetres, and
#     which bouncer's fetcher never asked for.
#   * a parquet cache per location, so a rerun costs nothing and the corpus is
#     fetched exactly once, ever.
#   * split_contiguous(), which fetches only the dates missing from cache.
#   * five retries with exponential backoff on 429, already written.
#
# Both hit the same endpoint (archive-api.open-meteo.com/v1/archive), so this is
# not a data-source change -- it is deleting a worse copy of a client that
# already existed.
#
# CACHE LOCATION. Pointed at bouncerdata rather than wheather's own user cache,
# so the result is self-contained and publishable to a release the way torpdata
# publishes its weather. wheather's existing 996-city cache overlaps only 3 of
# 289 Test venues and covers 2025 plus part of 2024, so there is nothing to
# reuse there anyway.
#
# ONE SHARP EDGE. wheather keys its cache on lat/lon rounded to 2dp, about
# 1.1km. Two Test grounds within 1.1km would share a cache entry. Checked and
# reported below rather than left to be discovered.
#
# SIZING FIRST, per #71's lesson: on the 227 matches that already had weather,
# 0 rain days gave a 13.2% draw rate and 2+ gave 32.4% -- a +19.2 point swing,
# cor(precipitation_total, is_draw) = +0.291. That justified the calls.
# See data-raw/validation/rain_effect_sizing.R.
#
# Usage:
#   Rscript data-raw/models/in-match/fetch_test_venue_weather.R

suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
if (!requireNamespace("wheather", quietly = TRUE)) {
  cli::cli_abort(c("The {.pkg wheather} package is required.",
                   "i" = "devtools::install('C:/dev/wheather')"))
}

PAD_DAYS      <- 7L    # cover a Test that runs past its scheduled span
GAP_TOLERANCE <- 30L   # merge ranges nearer than this into one
DELAY_SECONDS <- 1.0

CACHE_DIR <- file.path(find_bouncerdata_dir(), "weather-cache")
dir.create(CACHE_DIR, showWarnings = FALSE, recursive = TRUE)
options(wheather.cache_dir = CACHE_DIR)
cli::cli_alert_info("Cache: {.file {CACHE_DIR}}")

conn <- get_db_connection(read_only = TRUE)
match_days_raw <- as.data.table(dbGetQuery(conn, "
  SELECT m.venue, CAST(m.match_date AS DATE) AS match_date
  FROM cricsheet.matches m
  WHERE LOWER(m.match_type) IN ('test', 'mdm') AND m.venue IS NOT NULL"))
coords <- load_venue_coordinates(conn)
dbDisconnect(conn, shutdown = TRUE)

# Only the dates actually needed, merged into contiguous ranges. Asking for a
# venue's whole 25-year span was ~9,000 days per call and earned HTTP 429 after
# 69 venues; the union of match windows is ~24,000 days across ALL venues.
md <- match_days_raw[, .(day = seq(match_date, match_date + PAD_DAYS, by = "day")),
                     by = .(venue, match_date)]
md <- unique(md[, .(venue, day)])
setorder(md, venue, day)
md[, grp := cumsum(c(1L, as.integer(diff(day)) > GAP_TOLERANCE)), by = venue]
ranges <- md[, .(range_start = min(day), range_end = max(day), days = .N),
             by = .(venue, grp)]

work <- merge(ranges, coords, by = "venue")
n_no_coord <- uniqueN(ranges$venue) - uniqueN(work$venue)
if (n_no_coord > 0) {
  cli::cli_alert_warning("{n_no_coord} Test venue{?s} have no coordinates and are skipped.")
}

# The 2dp cache collision, reported rather than discovered later.
work[, cache_key := sprintf("%.2f_%.2f", round(latitude, 2), round(longitude, 2))]
collide <- unique(work[, .(venue, cache_key)])[, .N, by = cache_key][N > 1]
if (nrow(collide)) {
  shared <- unique(work[cache_key %in% collide$cache_key, .(venue, cache_key)])
  cli::cli_alert_warning(
    "{nrow(collide)} cache key{?s} shared by more than one venue (2dp is ~1.1km):")
  for (k in collide$cache_key) {
    cli::cli_bullets(c("*" = paste(shared[cache_key == k, venue], collapse = " | ")))
  }
  cli::cli_alert_info("They share weather, which is almost certainly right at 1km.")
}

setorder(work, venue, range_start)
cli::cli_h1("Per-day weather for Test venues")
cli::cli_alert_info(
  "{nrow(work)} range{?s} across {uniqueN(work$venue)} venue{?s}, {format(sum(work$days), big.mark = ',')} days.")

FLUSH_EVERY <- 25L
ok <- 0L; failed <- character(0); rows <- 0L; buf <- list()
t0 <- Sys.time()

flush_buf <- function() {
  if (!length(buf)) return(invisible(0L))
  conn_w <- get_db_connection()
  on.exit(dbDisconnect(conn_w, shutdown = TRUE), add = TRUE)
  n <- save_venue_weather_daily(rbindlist(buf, fill = TRUE), conn = conn_w)
  buf <<- list()
  invisible(n)
}

for (i in seq_len(nrow(work))) {
  v <- work[i]
  daily <- tryCatch(
    suppressMessages(wheather::fetch_weather(v$latitude, v$longitude,
                                             v$range_start, v$range_end)),
    error = function(e) NULL)
  if (is.null(daily) || !nrow(daily)) {
    failed <- c(failed, v$venue)
  } else {
    d <- as.data.table(daily)
    d[, venue := v$venue]
    buf[[length(buf) + 1L]] <- d
    ok <- ok + 1L
  }
  if (length(buf) >= FLUSH_EVERY || i == nrow(work)) rows <- rows + flush_buf()
  if (i %% 25 == 0 || i == nrow(work)) {
    cat(sprintf("%d/%d ranges | %d ok | %d failed | %d rows | %.1f mins\n",
        i, nrow(work), ok, length(failed), rows,
        as.numeric(difftime(Sys.time(), t0, units = "mins"))))
    flush.console()
  }
  Sys.sleep(DELAY_SECONDS)
}

cli::cli_h2("Done")
cli::cli_alert_success("{ok} range{?s}, {rows} daily row{?s}, {round(difftime(Sys.time(), t0, units = 'mins'), 1)} mins.")
if (length(failed)) {
  # Named, not counted -- a silently missing venue becomes a silently missing
  # feature for every match played there.
  fv <- unique(failed)
  cli::cli_alert_warning("{length(fv)} venue{?s} failed and are NOT in the table:")
  for (f in fv) cli::cli_bullets(c("*" = f))
}
