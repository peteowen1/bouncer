# The same ground appears under several venue names (bouncerverse#73).
#
# Geocoding 289 Test venues produced 67 coordinate keys shared by more than one
# name, and across all formats 16,141 of 21,776 matches (74.1%) sit on a venue
# whose coordinates are shared. Four distinct causes, and only two of them are
# aliases:
#
#   suffix creep  Brisbane Cricket Ground | ..., Woolloongabba |
#                 ..., Woolloongabba, Brisbane      -- the Gabba, three times
#   renames       Feroz Shah Kotla -> Arun Jaitley Stadium
#   spellings     Daren vs Darren Sammy National Cricket Stadium
#   PROXIMITY     the Colombo cluster -- SSC, NCC, Colts, P Sara and others are
#                 genuinely different grounds within a kilometre
#
# Why this is not cosmetic: every venue-keyed feature is computed on a fraction
# of the ground's real history. venue_result_rate is 13.9% of model A's gain in
# Test win probability, and time_causal_venue_mean() reports n_prior -- matches
# at the ground BEFORE this one -- so an aliased ground looks like it has less
# history than it does and is shrunk further toward the prior than it should be.
#
# COORDINATES ARE NOT A SAFE MERGE KEY ON THEIR OWN. Checked against cricsheet's
# own `city` column, which the geocoder never saw:
#
#   County Ground             261 matches | 6 cities: Bristol, Chelmsford, Derby, Hove, ...
#   Lord's                    102 matches | 1 city:   London
#
# `County Ground` is a generic English name covering at least six grounds, and
# it geocoded to one arbitrary point. Seventeen venue names span more than one
# city (884 matches). So a coordinate collision means "same ground", "adjacent
# grounds", or "the geocoder had to guess", and only names can tell them apart.
#
# The rule below therefore merges only where BOTH agree: one name is a prefix of
# another modulo punctuation, they share coordinates, AND every name involved
# maps to exactly one city. Everything else is reported for a human, the way
# build_player_id_map() excludes 58 ambiguous player names rather than guessing.

#' Normalise a venue name for comparison
#' @keywords internal
.venue_norm <- function(x) tolower(gsub("[^a-z0-9]", "", tolower(x)))

#' The merge rules, separated from the database so they can be tested
#'
#' @param d data.table with `venue`, `city`, `matches`, `latitude`, `longitude`
#'   and optionally `n_cities` (how many distinct cities the name is used in).
#' @param has_ncities Logical. TRUE if `d` already carries `n_cities`.
#' @param min_matches Integer. Alias names below this are left alone.
#' @return As [build_venue_id_map()].
#' @keywords internal
.venue_map_from <- function(d, has_ncities = FALSE, min_matches = 1L) {
  d <- data.table::as.data.table(data.table::copy(d))
  if (!has_ncities && !"n_cities" %in% names(d)) d[, n_cities := 1L]
  d[, coord_key := sprintf("%.2f_%.2f", round(latitude, 2), round(longitude, 2))]
  d[, nname := .venue_norm(venue)]

  # A name used in more than one city cannot be resolved by coordinates. It is
  # not an alias of anything -- it is one label over several grounds.
  ambiguous <- d[n_cities > 1]
  d <- d[n_cities <= 1]

  groups <- d[, .N, by = coord_key][N > 1, coord_key]
  keep <- list(); review <- list()

  for (k in groups) {
    g <- d[coord_key == k]
    longest <- g$nname[which.max(nchar(g$nname))]
    is_creep <- all(vapply(g$nname, function(x) startsWith(longest, x), logical(1)))
    cities <- unique(g$city[!is.na(g$city)])
    if (!is_creep) {
      review[[length(review) + 1L]] <- data.table::data.table(
        coord_key = k, reason = "names are not one prefix family",
        venues = paste(g$venue, collapse = " | "))
      next
    }
    if (length(cities) > 1) {
      review[[length(review) + 1L]] <- data.table::data.table(
        coord_key = k, reason = "prefix family spans several cities",
        venues = paste(g$venue, collapse = " | "))
      next
    }
    # Canonical = the most-used name, so the merge disturbs the fewest rows.
    data.table::setorder(g, -matches)
    canon <- g$venue[1]
    src <- g[venue != canon & matches >= min_matches, venue]
    if (length(src)) {
      keep[[length(keep) + 1L]] <- data.table::data.table(
        venue = src, canonical_venue = canon)
    }
  }

  out <- if (length(keep)) data.table::rbindlist(keep) else
    data.table::data.table(venue = character(), canonical_venue = character())

  # A source must never also be a target, or applying the map twice would move
  # rows again -- the same invariant build_player_id_map() asserts.
  stopifnot("a venue cannot be both an alias and a canonical target" =
              !any(out$canonical_venue %in% out$venue))

  data.table::setattr(out, "ambiguous",
    if (nrow(ambiguous)) ambiguous[, .(venue, matches, n_cities)] else
      data.table::data.table(venue = character(), matches = numeric(), n_cities = integer()))
  data.table::setattr(out, "review",
    if (length(review)) data.table::rbindlist(review) else
      data.table::data.table(coord_key = character(), reason = character(), venues = character()))
  out[]
}

#' Build a Venue Name Crosswalk
#'
#' Maps alias venue names to a canonical one. Mirrors
#' [build_player_id_map()]: conservative, auditable, and it declines rather
#' than guesses.
#'
#' @param conn DBI connection; opened read-only and closed on exit if NULL.
#' @param min_matches Integer. Ignore names with fewer than this many matches
#'   when deciding a canonical target. Default 1.
#'
#' @return data.table with `venue` (the alias) and `canonical_venue`. Carries
#'   two attributes for inspection: `ambiguous`, the names spanning several
#'   cities, and `review`, coordinate groups that were NOT merged and why.
#' @export
build_venue_id_map <- function(conn = NULL, min_matches = 1L) {
  own <- is.null(conn)
  if (own) {
    conn <- get_db_connection(read_only = TRUE)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }

  v <- data.table::as.data.table(DBI::dbGetQuery(conn, "
    SELECT m.venue,
           COUNT(*) AS matches,
           COUNT(DISTINCT m.city) FILTER (WHERE m.city IS NOT NULL) AS n_cities,
           ANY_VALUE(m.city) AS city
    FROM cricsheet.matches m WHERE m.venue IS NOT NULL GROUP BY 1"))
  co <- load_venue_coordinates(conn)
  if (is.null(co) || !nrow(co)) {
    cli::cli_warn("No venue coordinates available; returning an empty crosswalk.")
    return(data.table::data.table(venue = character(), canonical_venue = character()))
  }

  d <- merge(v, co[, .(venue, latitude, longitude)], by = "venue")
  .venue_map_from(d, has_ncities = TRUE, min_matches = min_matches)
}

#' Apply a Venue Crosswalk in Place
#'
#' Modifies `dt` by reference when it is already a data.table, matching
#' [canonicalise_player_ids()].
#'
#' @section Why not as.data.table():
#' `as.data.table()` on an object that is ALREADY a data.table performs a full
#' deep copy -- it is not the no-op it looks like. Writing
#' `d <- as.data.table(dt)` and then updating `d` therefore updates a throwaway
#' and leaves the caller's table untouched, silently. That is exactly what this
#' function did until the effect measurement came back reporting no change at
#' all, which is the only reason it was caught.
#'
#' @param dt data.table with a `venue` column, modified by reference.
#' @param map Output of [build_venue_id_map()].
#' @return `dt`, invisibly, with `venue` canonicalised.
#' @export
canonicalise_venues <- function(dt, map) {
  stopifnot(is.data.frame(dt), "venue" %in% names(dt))
  if (!nrow(map)) return(invisible(dt))
  if (!data.table::is.data.table(dt)) {
    dt <- data.table::as.data.table(dt)
  }
  data.table::setDT(dt)
  dt[data.table::as.data.table(map), on = "venue", venue := i.canonical_venue]
  invisible(dt[])
}


#' Flatten Multi-Hop Chains and Cycles in venue_aliases
#'
#' `venue_aliases` must maintain the invariant that no `canonical_venue` is
#' ever also an `alias` key -- a single-hop lookup (this table's own normal
#' read pattern, `match(venue, alias)`) silently stops at an intermediate
#' name otherwise. This bit twice (2026-08-29 commit d6c80c1, and again
#' 2026-09-01 reconciling a large batch of new rows): adding a row whose
#' canonical target is itself an existing alias key creates a chain or, if
#' the existing row points back the other way, a real cycle.
#'
#' Ground truth, not an arbitrary tie-break: every node visited while
#' resolving a chain is checked against how many rows it actually has in
#' `cricsheet.matches` right now, and the one with the most real rows wins.
#' A phantom string (0 rows) never wins against a populated one, and a
#' 2-cycle is resolved the same way rather than by insertion order.
#'
#' @param conn DBI connection with write access; opened and closed if NULL.
#' @param dry_run Logical. TRUE (the default) reports what would change and
#'   writes nothing.
#' @return data.table of the rows that would be, or were, repointed, invisibly.
#' @export
flatten_venue_alias_table <- function(conn = NULL, dry_run = TRUE) {
  own <- is.null(conn)
  if (own) {
    conn <- get_db_connection(read_only = dry_run)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }
  if (!table_exists(conn, "venue_aliases")) return(invisible(data.table::data.table()))

  va <- data.table::as.data.table(DBI::dbGetQuery(conn, "SELECT alias, canonical_venue FROM venue_aliases"))
  if (!nrow(va)) return(invisible(va))

  all_strings <- unique(c(va$alias, va$canonical_venue))
  counts_df <- data.table::as.data.table(DBI::dbGetQuery(conn, sprintf(
    "SELECT venue, COUNT(*) AS n FROM cricsheet.matches WHERE venue IN (%s) GROUP BY 1",
    paste(sprintf("'%s'", gsub("'", "''", all_strings)), collapse = ","))))
  row_count <- stats::setNames(rep(0L, length(all_strings)), all_strings)
  if (nrow(counts_df)) row_count[counts_df$venue] <- counts_df$n

  # Only re-litigate a row's target if it is ACTUALLY part of a chain --
  # i.e. its current canonical_venue is itself a known alias key. A plain,
  # non-chained mapping is left exactly as-is regardless of relative row
  # counts: re-deriving "the right direction" from corpus volume for every
  # row (not just chained ones) can flip a perfectly correct, deliberately-
  # chosen mapping when both sides happen to have similar volume. Caught by
  # a test before this shipped: a 1-hop alias/canonical pair with equal
  # counts on both sides was getting reversed for no reason.
  resolve_by_ground_truth <- function(v, map, counts, max_hops = 20L) {
    visited <- v
    cur <- v
    for (i in seq_len(max_hops)) {
      hit <- map[alias == cur, canonical_venue]
      if (!length(hit) || hit[1] %in% visited) break
      cur <- hit[1]
      visited <- c(visited, cur)
    }
    n <- counts[visited]
    n[is.na(n)] <- 0L
    visited[which.max(n)]
  }
  is_chained <- va$canonical_venue %in% va$alias
  va[, final := canonical_venue]
  if (any(is_chained)) {
    va[is_chained, final := vapply(alias, resolve_by_ground_truth, character(1),
                                   map = va, counts = row_count)]
  }
  changed <- va[final != canonical_venue]

  final_map <- stats::setNames(va$final, va$alias)
  still_bad <- va[final %in% alias & final != alias & final_map[final] != final]
  if (nrow(still_bad)) {
    cli::cli_abort("{nrow(still_bad)} row{?s} still chain after ground-truth resolution -- investigate before writing.")
  }

  if (!dry_run && nrow(changed)) {
    for (i in seq_len(nrow(changed))) {
      DBI::dbExecute(conn, "UPDATE venue_aliases SET canonical_venue = ? WHERE alias = ?",
                     params = list(changed$final[i], changed$alias[i]))
    }
    self_final <- va[final == alias, alias]
    if (length(self_final)) {
      for (s in self_final) DBI::dbExecute(conn, "DELETE FROM venue_aliases WHERE alias = ?", params = list(s))
    }
    va2 <- data.table::as.data.table(DBI::dbGetQuery(conn, "SELECT alias, canonical_venue FROM venue_aliases"))
    still_bad2 <- va2[canonical_venue %in% alias & canonical_venue != alias]
    if (nrow(still_bad2)) {
      cli::cli_abort("{nrow(still_bad2)} chain{?s} remain after flattening -- did not apply cleanly.")
    }
    cli::cli_alert_success("Flattened {nrow(changed)} row{?s} by ground truth; 0 chains remain ({nrow(va2)} rows).")
  } else if (nrow(changed)) {
    cli::cli_alert_info("{nrow(changed)} row{?s} would be repointed to their ground-truth target (dry run).")
  }
  invisible(changed[, .(alias, canonical_venue, final)])
}


#' Store Derived Aliases in the Existing venue_aliases Table
#'
#' There is already a `venue_aliases` table and a resolver
#' ([normalize_venue()], [add_venue_alias()]), so derived aliases belong THERE
#' rather than in a second mechanism. [build_venue_id_map()] supplies content;
#' this writes it.
#'
#' @section What the existing table contains:
#' 98 hand-curated rows, and only **27 of their aliases appear as a cricsheet
#' venue name at all** -- they are abbreviations like `MCG`, `SCG`, `Gabba` and
#' `WACA` written against a different naming convention. So the machinery is
#' real and its content largely does not match the corpus it is applied to. The
#' derivation adds 116 aliases that DO occur in cricsheet.
#'
#' @param map Output of [build_venue_id_map()].
#' @param conn DBI connection with write access; opened and closed if NULL.
#' @param dry_run Logical. TRUE (the default) reports what would change and
#'   writes nothing. Populating this table moves every venue-keyed feature, so
#'   the write is opt-in.
#' @return data.table of the rows that would be, or were, inserted.
#' @export
store_venue_aliases <- function(map, conn = NULL, dry_run = TRUE) {
  stopifnot(is.data.frame(map), all(c("venue", "canonical_venue") %in% names(map)))
  own <- is.null(conn)
  if (own) {
    conn <- get_db_connection(read_only = dry_run)
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  }
  m <- data.table::as.data.table(map)[, .(alias = venue, canonical_venue)]
  if (!nrow(m)) return(m[])

  existing <- if (table_exists(conn, "venue_aliases")) {
    data.table::as.data.table(DBI::dbGetQuery(conn,
      "SELECT alias, canonical_venue AS existing_canonical FROM venue_aliases"))
  } else data.table::data.table(alias = character(), existing_canonical = character())

  m <- merge(m, existing, by = "alias", all.x = TRUE)

  # A row already present with a DIFFERENT target is a disagreement between a
  # hand-curated decision and a derived one.
  #
  # Deference is the right default -- but ONLY when the curated target actually
  # exists in the corpus. It usually does not: 12 of the 13 disagreements point
  # at names cricsheet never uses ("Trent Bridge Nottingham" against the real
  # "Trent Bridge, Nottingham"). Keeping those splits the ground rather than
  # merging it, because the alias resolves to a phantom while its sibling keeps
  # the real name. Curation earns deference when its answer is reachable.
  real <- DBI::dbGetQuery(conn,
    "SELECT DISTINCT venue FROM cricsheet.matches WHERE venue IS NOT NULL")$venue
  clash <- m[!is.na(existing_canonical) & existing_canonical != canonical_venue]
  clash[, `:=`(curated_real = existing_canonical %in% real,
               derived_real = canonical_venue %in% real)]
  keep_curated <- clash[curated_real == TRUE]
  fix_curated  <- clash[curated_real == FALSE & derived_real == TRUE]

  if (nrow(keep_curated)) {
    cli::cli_alert_info(
      "{nrow(keep_curated)} derived alias{?es} disagree with a curated mapping whose target IS real; curation kept.")
  }
  if (nrow(fix_curated)) {
    cli::cli_warn(c(
      "{nrow(fix_curated)} curated mapping{?s} point at a venue name cricsheet never uses, so they SPLIT the ground:",
      stats::setNames(sprintf("%s: %s (absent) -> %s",
                              fix_curated$alias, fix_curated$existing_canonical,
                              fix_curated$canonical_venue),
                      rep("*", nrow(fix_curated)))))
    if (!dry_run) {
      for (i in seq_len(nrow(fix_curated))) {
        DBI::dbExecute(conn,
          "UPDATE venue_aliases SET canonical_venue = ? WHERE alias = ?",
          params = list(fix_curated$canonical_venue[i], fix_curated$alias[i]))
      }
      cli::cli_alert_success("Repointed {nrow(fix_curated)} curated alias{?es} at names that exist.")
    }
  }
  new <- m[is.na(existing_canonical), .(alias, canonical_venue)]

  cli::cli_alert_info(
    "{nrow(new)} new alias{?es} to add; {nrow(m) - nrow(new)} already present.")
  if (dry_run) {
    cli::cli_alert_info("Dry run -- nothing written. Pass {.code dry_run = FALSE} to apply.")
    return(new[])
  }
  if (nrow(new)) {
    duckdb::duckdb_register(conn, "va_staging", as.data.frame(new))
    on.exit(duckdb::duckdb_unregister(conn, "va_staging"), add = TRUE)
    DBI::dbExecute(conn,
      "INSERT INTO venue_aliases (alias, canonical_venue) SELECT alias, canonical_venue FROM va_staging")
    cli::cli_alert_success("Inserted {nrow(new)} alias{?es}.")
  }

  # The gap that let a real chain/cycle bug happen twice (2026-08-29,
  # 2026-09-01): this function checks whether an ALIAS it's inserting
  # already exists with a different target, but never checked whether the
  # new CANONICAL is itself an existing alias key. Flatten unconditionally
  # after every write, not just when new rows were added -- fix_curated
  # above can also repoint an existing row onto a target that chains.
  if (nrow(new) || nrow(fix_curated)) {
    flatten_venue_alias_table(conn = conn, dry_run = FALSE)
  }
  new[]
}
