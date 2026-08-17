# Resolving a player by name, safely.
#
# Looking a player up by name went wrong three separate times in one session:
#   - `player_name == "V Kohli"` matched TWO player_ids, and which one came
#     back depended on table order, so a diagnostic reported 16 matches for a
#     man with 371.
#   - `grepl("Ecclestone")` against "SF Ecclestone" found nothing, because the
#     registry spells her "S Ecclestone" -- and a miss returns an empty table,
#     which reads as "not rated" rather than "you asked wrong".
#   - `grepl("Sharma")` returned Abhishek Sharma when the question was about
#     Rohit, because the first match won silently.
#
# Every one of those failed QUIETLY and produced a confident wrong answer.
# This resolves against the registry and makes ambiguity loud.

#' Find a Player by Name
#'
#' Resolves a name or fragment to registry entries, loudest-first on ambiguity.
#'
#' Matching is tried in order and stops at the first tier that hits: exact
#' name, then case-insensitive exact, then surname, then substring. That way
#' `"V Kohli"` does not silently fall through to a substring match on some
#' other Kohli, while `"Ecclestone"` still finds `"S Ecclestone"`.
#'
#' Results carry `balls` so the well-established player sorts first, and a
#' warning fires whenever more than one player matches — the case that has
#' produced wrong answers before.
#'
#' @param query Character. A name, surname, or fragment.
#' @param format,gender Optional bucket for the career-ball counts used to rank
#'   candidates. NULL counts across everything.
#' @param conn DBI connection; opened read-only and closed on exit if NULL.
#' @param quiet Logical. Suppress the ambiguity warning (it still returns all
#'   matches).
#' @return data.table of `player_id`, `player_name`, `balls`, `matches`,
#'   `match_type` (which tier matched), best-evidenced first. Zero rows if
#'   nothing matched — check `nrow()`, never assume.
#' @export
find_player <- function(query, format = NULL, gender = NULL, conn = NULL,
                        quiet = FALSE) {
  own <- is.null(conn)
  if (own) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }
  stopifnot(is.character(query), length(query) == 1L, nzchar(query))

  nm <- data.table::as.data.table(DBI::dbGetQuery(conn,
    "SELECT player_id, ANY_VALUE(player_name) AS player_name
     FROM cricsheet.players GROUP BY player_id"))

  where <- c(if (!is.null(format)) sprintf("LOWER(d.match_type) IN (%s)",
                                           if (tolower(format) == "t20") "'t20','it20'" else "'odi','odm'"),
             if (!is.null(gender)) sprintf("m.gender = '%s'", gender))
  vol <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT pid AS player_id, COUNT(*) AS balls, COUNT(DISTINCT match_id) AS matches
    FROM (SELECT d.batter_id AS pid, d.match_id FROM cricsheet.deliveries d
          JOIN cricsheet.matches m ON m.match_id = d.match_id %s
          UNION ALL
          SELECT d.bowler_id, d.match_id FROM cricsheet.deliveries d
          JOIN cricsheet.matches m ON m.match_id = d.match_id %s)
    WHERE pid IS NOT NULL GROUP BY pid",
    if (length(where)) paste("WHERE", paste(where, collapse = " AND ")) else "",
    if (length(where)) paste("WHERE", paste(where, collapse = " AND ")) else "")))

  nm <- merge(nm, vol, by = "player_id", all.x = TRUE)
  nm[is.na(balls), `:=`(balls = 0L, matches = 0L)]

  surname <- function(x) vapply(strsplit(x, " ", fixed = TRUE),
                                function(p) p[length(p)], character(1))
  tiers <- list(
    exact      = nm$player_name == query,
    exact_ci   = tolower(nm$player_name) == tolower(query),
    surname    = tolower(surname(nm$player_name)) == tolower(surname(query)),
    substring  = grepl(query, nm$player_name, fixed = TRUE, useBytes = FALSE)
  )
  hit <- NULL; tier <- NA_character_
  for (nmt in names(tiers)) {
    # `%in% TRUE` collapses NA to FALSE. A registry name that is NA makes every
    # tier's vector carry an NA, and then `any()` returns NA rather than FALSE
    # for a genuine no-match -- `if (NA)` aborts, and an NA index would select
    # a phantom all-NA row. Either failure would land on exactly the case this
    # function exists to make loud and safe.
    sel <- tiers[[nmt]] %in% TRUE
    if (any(sel)) { hit <- nm[sel]; tier <- nmt; break }
  }
  if (is.null(hit)) {
    if (!quiet) cli::cli_warn("No player matches {.val {query}}.")
    return(nm[0][, match_type := character(0)][])
  }
  hit[, match_type := tier]
  data.table::setorder(hit, -balls)
  if (nrow(hit) > 1L && !quiet) {
    cli::cli_warn(c(
      "{.val {query}} matches {nrow(hit)} players; returning all, best-evidenced first.",
      "i" = "{paste(sprintf('%s (%s balls)', hit$player_name, hit$balls), collapse = '; ')}",
      "!" = "Pick deliberately -- taking the first row silently is how three wrong answers happened."))
  }
  hit[, .(player_id, player_name, balls, matches, match_type)][]
}
