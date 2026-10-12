# Cricsheet's player register and the date-of-birth table built on it (D-P69).
#
# cricsheet.players.dob is empty for every player, so ages come from cricinfo's
# scorecards, joined through cricsheet's own register of ids on other sites.
# Where both exist, cricinfo matched the player_details file exactly for 2,685
# of 2,728 players (2026-10-11); the 39 that differ by over a year look like
# same-name mix-ups in the details file, so cricinfo is trusted first.

#' Store cricsheet's player register
#'
#' Downloads cricsheet's `people.csv` (every player's id on cricinfo, cricbuzz,
#' BCCI and other sites) and writes it, every column as text, to
#' `cricsheet.people_register`, replacing the previous copy. Nothing is dropped
#' at load: columns nobody reads yet are kept, because re-fetching later is not
#' free.
#'
#' @param conn A read-write DuckDB connection.
#' @param path Local copy of `people.csv`, or NULL to download `url`.
#' @param url Where to download it from.
#' @return The number of rows written, invisibly.
#' @export
sync_people_register <- function(conn, path = NULL,
                                 url = "https://cricsheet.org/register/people.csv") {
  if (is.null(path)) {
    path <- tempfile(fileext = ".csv")
    on.exit(unlink(path), add = TRUE)
    utils::download.file(url, path, mode = "wb", quiet = TRUE)
  }
  reg <- data.table::fread(path, colClasses = "character", na.strings = "")
  need <- c("identifier", "name", "key_cricinfo")
  missing <- setdiff(need, names(reg))
  if (length(missing)) {
    cli::cli_abort("people register is missing column{?s} {.val {missing}}; cricsheet changed the file?")
  }
  # A partial download still parses; refuse a register far smaller than the
  # ~17,000 people it held in 2026, or one with duplicated identifiers.
  if (nrow(reg) < 10000L) {
    cli::cli_abort("people register has only {nrow(reg)} rows; refusing to replace the stored copy.")
  }
  if (anyDuplicated(reg$identifier)) {
    cli::cli_abort("people register has duplicated identifiers; refusing to write it.")
  }
  DBI::dbWithTransaction(conn, {
    DBI::dbExecute(conn, "CREATE SCHEMA IF NOT EXISTS cricsheet")
    DBI::dbWriteTable(conn, DBI::Id(schema = "cricsheet", table = "people_register"),
                      as.data.frame(reg), overwrite = TRUE)
  })
  known <- DBI::dbGetQuery(conn, "
    SELECT COUNT(*) AS n, COUNT(r.identifier) AS in_register
    FROM cricsheet.players p LEFT JOIN cricsheet.people_register r ON r.identifier = p.player_id")
  cli::cli_alert_success(
    "Stored {nrow(reg)} people ({ncol(reg)} columns) in cricsheet.people_register; {known$in_register} of {known$n} cricsheet.players ids are in it.")
  invisible(nrow(reg))
}

# cricinfo stores a birth date as a Python dict string,
# "{'date': 27, 'month': 3, 'year': 2011}", sometimes with None/NULL parts.
# A date missing its day, month or year is treated as unknown, not guessed.
.parse_cricinfo_dob <- function(x) {
  # One string at a time: a single malformed entry (a stray True or quote)
  # must cost that player his date, not abort the whole table.
  one <- function(s) {
    if (is.na(s) || !nzchar(s)) return(NA_character_)
    z <- tryCatch(jsonlite::parse_json(gsub("None|NULL", "null", gsub("'", '"', s, fixed = TRUE))),
                  error = function(e) NULL)
    if (!is.list(z) || is.null(z$year) || is.null(z$month) || is.null(z$date)) return(NA_character_)
    sprintf("%04d-%02d-%02d", as.integer(z$year), as.integer(z$month), as.integer(z$date))
  }
  # An explicit format: bare as.Date() picks ONE format from the first string
  # and applies it to all, so one impossible date (30 February) first in the
  # vector turned every date NA. With a format, each element stands alone.
  as.Date(vapply(x, one, "", USE.NAMES = FALSE), format = "%Y-%m-%d")
}

#' Build the player date-of-birth table
#'
#' Writes `main.player_dob` (`player_id`, `dob`, `dob_source`) from cricinfo
#' scorecards joined through `cricsheet.people_register` (any of
#' `key_cricinfo`, `key_cricinfo_2`, `key_cricinfo_3`), then Wikidata, then
#' the player_details file, each filling only players the earlier sources
#' lack. Players with no date here are not aged by
#' [calculate_player_rating_v2()] (`dob_source` "none").
#'
#' @param conn A read-write DuckDB connection.
#' @param details_path Path to `bouncer_player_details.parquet`, or NULL to
#'   skip it.
#' @param wikidata_path Path to the CSV written by [fetch_wikidata_dob()], or
#'   NULL to skip it.
#' @return The table, invisibly.
#' @export
build_player_dob <- function(conn, details_path = NULL, wikidata_path = NULL) {
  if (!DBI::dbExistsTable(conn, DBI::Id(schema = "cricsheet", table = "people_register"))) {
    cli::cli_abort("cricsheet.people_register does not exist; run {.fn sync_people_register} first.")
  }
  keys <- data.table::as.data.table(DBI::dbGetQuery(conn, "
    SELECT identifier AS player_id, key FROM (
      SELECT identifier, key_cricinfo AS key FROM cricsheet.people_register
      UNION ALL SELECT identifier, key_cricinfo_2 FROM cricsheet.people_register
      UNION ALL SELECT identifier, key_cricinfo_3 FROM cricsheet.people_register
    ) WHERE key IS NOT NULL AND key <> ''"))
  # Every distinct string per cricinfo id, not ANY_VALUE: a player's innings
  # can carry an incomplete date in one row and the full one in another.
  ci <- data.table::as.data.table(DBI::dbGetQuery(conn, "
    SELECT DISTINCT CAST(player_id AS VARCHAR) AS key, player_dob
    FROM cricinfo.innings WHERE player_dob IS NOT NULL"))
  ci[, dob := .parse_cricinfo_dob(player_dob)]
  ci <- unique(ci[!is.na(dob), .(key, dob)])
  from_ci <- merge(keys, ci, by = "key")
  # Two cricinfo ids for one cricsheet player can disagree; keep it only if they
  # agree, rather than picking one silently.
  from_ci <- from_ci[, .(dob = dob[1], n_dates = data.table::uniqueN(dob)), by = player_id]
  conflicts <- from_ci[n_dates > 1L, .N]
  from_ci <- from_ci[n_dates == 1L, .(player_id, dob, dob_source = "cricinfo")]

  # Coverage floor: 6,864 cricinfo dates on 2026-10-12. A parse or key
  # regression would otherwise fall back to the details file with a green run.
  if (nrow(from_ci) < 3000L) {
    cli::cli_abort(c("Only {nrow(from_ci)} dates of birth came from cricinfo (6,864 on 2026-10-12).",
                     "i" = "Check the player_dob format in cricinfo.innings and the key_cricinfo join before rebuilding."))
  }
  out <- from_ci
  # Wikidata fills only the gaps. It is the noisier source: on the 3,866
  # players both sources date, 94% agree exactly but 150 differ by more than
  # a month, and the big gaps are mostly Wikidata's (a 1923 birth for a
  # current player, 1970-01-01 placeholders). The rating applies a tighter
  # debut-age check to these rows for the same reason.
  if (!is.null(wikidata_path)) {
    wd <- data.table::fread(wikidata_path, colClasses = "character")
    if (!all(c("key", "dob") %in% names(wd))) {
      cli::cli_abort("{.path {wikidata_path}} needs columns key and dob; rebuild it with {.fn fetch_wikidata_dob}.")
    }
    wd[, dob := as.Date(dob, format = "%Y-%m-%d")]
    wd <- unique(wd[!is.na(dob) & dob != as.Date("1970-01-01"), .(key, dob)])
    from_wd <- merge(keys, wd, by = "key")
    from_wd <- from_wd[, .(dob = dob[1], n_dates = data.table::uniqueN(dob)), by = player_id]
    from_wd <- from_wd[n_dates == 1L & !player_id %in% out$player_id,
                       .(player_id, dob, dob_source = "wikidata")]
    out <- rbind(out, from_wd)
  }
  if (is.null(details_path)) {
    cli::cli_alert_warning("No player_details file; dates of birth come from cricinfo only.")
  } else {
    if (!requireNamespace("arrow", quietly = TRUE)) cli::cli_abort("Reading {.arg details_path} needs the arrow package.")
    det <- data.table::as.data.table(arrow::read_parquet(details_path, col_select = c("player_id", "dob")))
    det <- unique(det[, dob := as.Date(dob)][!is.na(dob)], by = "player_id")
    out <- rbind(out, det[!player_id %in% out$player_id, .(player_id, dob, dob_source = "details")])
  }
  # Ages outside 10-60 at a first-class debut are typos, but the check that
  # needs a match date belongs in the rating; here only impossible dates go.
  bad <- out[dob < as.Date("1850-01-01") | dob > Sys.Date(), .N]
  out <- out[dob >= as.Date("1850-01-01") & dob <= Sys.Date()]
  if (!nrow(out)) cli::cli_abort("No dates of birth found; check cricinfo.innings and the register.")
  DBI::dbWithTransaction(conn, {
    DBI::dbWriteTable(conn, DBI::Id(schema = "main", table = "player_dob"), as.data.frame(out), overwrite = TRUE)
  })
  cov <- if (DBI::dbExistsTable(conn, DBI::Id(schema = "main", table = "player_rating_v2"))) {
    DBI::dbGetQuery(conn, "
      SELECT COUNT(*) AS n, COUNT(d.player_id) AS dated
      FROM (SELECT DISTINCT player_id FROM main.player_rating_v2) r
      LEFT JOIN main.player_dob d USING (player_id)")
  } else data.frame(n = NA, dated = NA)
  cli::cli_alert_success(paste0(
    "Stored {nrow(out)} dates of birth in main.player_dob ",
    "({sum(out$dob_source == 'cricinfo')} cricinfo, {sum(out$dob_source == 'wikidata')} wikidata, ",
    "{sum(out$dob_source == 'details')} details); ",
    "{cov$dated} of {cov$n} rated players dated; dropped {conflicts} with conflicting cricinfo dates and {bad} impossible dates."))
  invisible(out)
}

#' Fetch dates of birth from Wikidata by ESPNcricinfo player id
#'
#' One SPARQL query for every Wikidata item with an ESPNcricinfo player id
#' (property P2697) and a day-precision date of birth (P569). Cricinfo
#' scorecards in the database only cover the Hawkeye era, so older players
#' (Muralitharan, Swann, Graeme Smith) have no date without this. Ids with
#' more than one recorded date are dropped rather than guessed.
#'
#' Written to a file so the rating build does not depend on Wikidata being up.
#'
#' @param path Where to write the CSV (`key`, `dob`).
#' @param min_rows Refuse a result smaller than this; 29,295 ids on 2026-10-12.
#' @return The table, invisibly.
#' @export
fetch_wikidata_dob <- function(path, min_rows = 20000L) {
  query <- paste(
    "SELECT ?ci ?dob WHERE { ?p wdt:P2697 ?ci . ?p p:P569/psv:P569 ?v .",
    "?v wikibase:timeValue ?dob ; wikibase:timePrecision 11 . }")
  resp <- httr2::request("https://query.wikidata.org/sparql") |>
    httr2::req_url_query(query = query) |>
    httr2::req_headers(Accept = "text/csv") |>
    httr2::req_user_agent("bouncer R package (https://github.com/peteowen1/bouncer)") |>
    httr2::req_timeout(120) |>
    httr2::req_retry(max_tries = 3) |>
    httr2::req_perform()
  wd <- data.table::fread(text = httr2::resp_body_string(resp), colClasses = "character")
  if (!all(c("ci", "dob") %in% names(wd))) cli::cli_abort("Unexpected Wikidata response: columns {names(wd)}.")
  wd <- unique(wd[, .(key = ci, dob = substr(dob, 1, 10))])
  wd <- wd[, if (.N == 1L) .SD, by = key]
  if (nrow(wd) < min_rows) {
    cli::cli_abort("Only {nrow(wd)} dated cricinfo ids from Wikidata (29,295 on 2026-10-12); not overwriting {.path {path}}.")
  }
  # write then rename, so a crash mid-write cannot leave a truncated file
  tmp <- paste0(path, ".tmp")
  data.table::fwrite(wd, tmp)
  if (!file.rename(tmp, path)) {
    unlink(tmp)
    cli::cli_abort("Could not replace {.path {path}}.")
  }
  cli::cli_alert_success("Wrote {nrow(wd)} Wikidata dates of birth to {.path {path}}.")
  invisible(wd)
}
