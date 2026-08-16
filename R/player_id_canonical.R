# Canonical player ids (#43).
#
# `cricsheet_parser.R` resolves a player to an id using that MATCH's
# `info$registry$people` block, and falls back to the player's NAME when he is
# missing from it (lines 236 and 712). The same player is registered properly in
# other matches, so his career splits across two ids: 3,775 of 16,690 registry
# ids are a bare name, 2,903 display names carry both a name id and a hash id,
# and 4.47% of all delivery appearances sit on the wrong side.
#
# The fix is to resolve against a GLOBAL registry instead of a per-match one,
# which is what these functions build. Merging is done conservatively, because
# a wrong merge fuses two real people permanently and is not detectable
# afterwards.

#' Build a Canonical Player ID Map
#'
#' Folds bare-name player ids onto the registry hash id for the same person.
#'
#' Three exclusions, all deliberately conservative:
#' \itemize{
#'   \item the two ids appear in the SAME match — proof of two different
#'     people, since one person cannot bat twice in an innings as two ids. One
#'     name is excluded this way (`E Jones`).
#'   \item two or more hash ids share the display name, so there is no way to
#'     pick a target (57 names, e.g. three different `A Sharma`s).
#'   \item no hash id shares the name at all — a genuinely unregistered player,
#'     with nothing to merge into (930 ids, 0.45% of appearances).
#' }
#'
#' @param conn DBI connection; opened read-only and closed on exit if NULL.
#' @return data.table of `player_id` (the bare-name id to retire),
#'   `canonical_id`, `player_name` and `apps`.
#' @export
build_player_id_map <- function(conn = NULL) {
  own <- is.null(conn)
  if (own) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }

  p <- data.table::as.data.table(DBI::dbGetQuery(conn,
    "SELECT player_id, ANY_VALUE(player_name) AS pn FROM cricsheet.players GROUP BY player_id"))
  p[, is_name := player_id == pn]

  app <- data.table::as.data.table(DBI::dbGetQuery(conn, "
    SELECT match_id, batter_id AS pid FROM cricsheet.deliveries WHERE batter_id IS NOT NULL
    UNION SELECT match_id, bowler_id FROM cricsheet.deliveries WHERE bowler_id IS NOT NULL"))
  app <- merge(app, p, by.x = "pid", by.y = "player_id")

  byname <- app[, .(n_name = data.table::uniqueN(pid[is_name]),
                    n_hash = data.table::uniqueN(pid[!is_name])), by = pn]
  cand <- byname[n_name >= 1 & n_hash >= 1]

  co <- app[pn %in% cand$pn, .(k = data.table::uniqueN(pid)), by = .(pn, match_id)][k > 1]
  excl <- c(unique(co$pn), cand[n_hash > 1, pn])
  ok <- cand[!pn %in% excl]

  a <- app[pn %in% ok$pn, .(apps = .N), by = .(pn, pid, is_name)]
  tgt <- a[is_name == FALSE, .(canonical_id = pid[1]), by = pn]
  idmap <- merge(a[is_name == TRUE, .(player_id = pid, pn, apps)], tgt, by = "pn")

  # A source must never also be a target, or applying the map twice would move
  # ids again; and a target must never be a bare name.
  stopifnot(!any(idmap$canonical_id %in% idmap$player_id),
            !any(idmap$canonical_id == idmap$pn),
            !anyDuplicated(idmap$player_id))

  cli::cli_alert_success(
    "Mapped {nrow(idmap)} bare-name id{?s} onto {data.table::uniqueN(idmap$canonical_id)} canonical id{?s} ({sum(idmap$apps)} appearance{?s}); excluded {length(unique(excl))} ambiguous name{?s}.")
  idmap[, .(player_id, canonical_id, player_name = pn, apps)][]
}

#' Apply a Canonical Player ID Map
#'
#' Rewrites id columns in place. Safe to call twice: no canonical id is ever a
#' source, so a second pass is a no-op.
#'
#' @param dt data.table to rewrite.
#' @param map Output of [build_player_id_map()].
#' @param cols Id columns to rewrite.
#' @return `dt`, modified by reference.
#' @export
canonicalise_player_ids <- function(dt, map,
                                    cols = c("batter_id", "bowler_id", "player_id")) {
  stopifnot(data.table::is.data.table(dt))
  m <- stats::setNames(map$canonical_id, map$player_id)
  for (cc in intersect(cols, names(dt))) {
    hit <- m[dt[[cc]]]
    data.table::set(dt, which(!is.na(hit)), cc, hit[!is.na(hit)])
  }
  dt
}
