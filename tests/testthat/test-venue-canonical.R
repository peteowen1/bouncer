# The same ground under several names, and the rules for merging them safely.
#
# 74.1% of matches sit on a venue whose coordinates are shared with another
# name, but a coordinate collision means one of three things: the same ground,
# two adjacent grounds (the Colombo cluster), or a name so generic the geocoder
# had to guess. "County Ground" alone spans six cities and 261 matches
# (bouncerverse#73).

vrow <- function(venue, city, matches, lat, lon) {
  data.table::data.table(venue = venue, city = city, matches = matches,
                         latitude = lat, longitude = lon)
}

test_that("normalisation ignores punctuation and case", {
  expect_equal(.venue_norm("Lord's, London"), "lordslondon")
  expect_equal(.venue_norm("LORDS LONDON"), "lordslondon")
  expect_equal(.venue_norm("M.Chinnaswamy"), "mchinnaswamy")
})

test_that("suffix creep merges to the most-used name", {
  # Canonical is the most-used name so the merge disturbs the fewest rows.
  d <- rbind(vrow("Basin Reserve", "Wellington", 20, -41.30, 174.78),
             vrow("Basin Reserve, Wellington", "Wellington", 80, -41.30, 174.78))
  m <- .venue_map_from(d)
  expect_equal(nrow(m), 1L)
  expect_equal(m$venue, "Basin Reserve")
  expect_equal(m$canonical_venue, "Basin Reserve, Wellington")
})

test_that("a three-name family collapses to one", {
  d <- rbind(vrow("Brisbane Cricket Ground", "Brisbane", 30, -27.49, 153.04),
             vrow("Brisbane Cricket Ground, Woolloongabba", "Brisbane", 60, -27.49, 153.04),
             vrow("Brisbane Cricket Ground, Woolloongabba, Brisbane", "Brisbane", 10, -27.49, 153.04))
  m <- .venue_map_from(d)
  expect_equal(nrow(m), 2L)
  expect_equal(unique(m$canonical_venue), "Brisbane Cricket Ground, Woolloongabba")
})

test_that("adjacent but DIFFERENT grounds are not merged", {
  # The Colombo case: same coordinates to 2dp, genuinely separate grounds.
  d <- rbind(vrow("Sinhalese Sports Club Ground", "Colombo", 40, 6.90, 79.87),
             vrow("Nondescripts Cricket Club Ground", "Colombo", 30, 6.90, 79.87))
  m <- .venue_map_from(d)
  expect_equal(nrow(m), 0L)
  expect_match(attr(m, "review")$reason, "prefix family", fixed = FALSE)
})

test_that("a name spanning several cities is held out, not merged", {
  # "County Ground" is six English grounds wearing one label.
  d <- rbind(vrow("County Ground", NA, 261, 51.46, -2.60),
             vrow("County Ground, Bristol", "Bristol", 40, 51.46, -2.60))
  d$n_cities <- c(6L, 1L)
  m <- .venue_map_from(d, has_ncities = TRUE)
  expect_equal(nrow(m), 0L)
  expect_true("County Ground" %in% attr(m, "ambiguous")$venue)
})

test_that("a prefix family split across cities is refused", {
  d <- rbind(vrow("National Stadium", "Karachi", 30, 24.89, 67.06),
             vrow("National Stadium, Karachi", "Lahore", 20, 24.89, 67.06))
  m <- .venue_map_from(d)
  expect_equal(nrow(m), 0L)
})

test_that("a venue is never both an alias and a target", {
  # Applying the map twice must not move rows again -- the invariant
  # build_player_id_map() asserts for players.
  d <- rbind(vrow("Lord's", "London", 20, 51.53, -0.17),
             vrow("Lord's, London", "London", 80, 51.53, -0.17))
  m <- .venue_map_from(d)
  expect_false(any(m$canonical_venue %in% m$venue))
})

test_that("canonicalise_venues modifies a data.table BY REFERENCE", {
  # as.data.table() on an existing data.table deep-copies, so an implementation
  # that assigns through it updates a throwaway and silently changes nothing.
  # That happened, and only the effect measurement caught it.
  dt <- data.table::data.table(venue = c("Lord's", "Lord's, London", "Other"),
                               x = 1:3)
  map <- data.table::data.table(venue = "Lord's", canonical_venue = "Lord's, London")
  canonicalise_venues(dt, map)
  expect_equal(dt$venue, c("Lord's, London", "Lord's, London", "Other"))
})

test_that("an empty map leaves the table alone", {
  dt <- data.table::data.table(venue = c("A", "B"))
  canonicalise_venues(dt, data.table::data.table(venue = character(),
                                                 canonical_venue = character()))
  expect_equal(dt$venue, c("A", "B"))
})

test_that("a table without a venue column is refused", {
  expect_error(canonicalise_venues(data.table::data.table(x = 1),
                                   data.table::data.table(venue = "a", canonical_venue = "b")),
               "venue")
})

# ---- flatten_venue_alias_table(): the chain/cycle bug that bit twice ------
#
# 2026-08-29 (d6c80c1) and again 2026-09-01: a row whose canonical_venue is
# itself an existing alias key breaks any single-hop lookup. Minimal in-
# memory fixtures -- cricsheet.matches for the ground-truth row counts,
# venue_aliases for the table under test.

.venue_fixture_conn <- function(matches, aliases) {
  conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  DBI::dbExecute(conn, "CREATE SCHEMA cricsheet")
  DBI::dbWriteTable(conn, DBI::Id(schema = "cricsheet", table = "matches"), matches)
  DBI::dbWriteTable(conn, "venue_aliases", aliases)
  conn
}

test_that("a plain chain (A -> B -> C, no cycle) resolves to the real venue", {
  # B is a phantom (0 rows) sitting between a real alias and a real target --
  # the exact shape that split "Seddon Park, Hamilton" the first time this
  # function was written.
  m <- data.frame(venue = c("C", "C", "C"))
  va <- data.frame(alias = c("A", "B"), canonical_venue = c("B", "C"))
  conn <- .venue_fixture_conn(m, va)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  changed <- flatten_venue_alias_table(conn = conn, dry_run = FALSE)
  expect_equal(changed$final[changed$alias == "A"], "C")

  out <- DBI::dbGetQuery(conn, "SELECT * FROM venue_aliases ORDER BY alias")
  expect_false(any(out$canonical_venue %in% out$alias))
})

test_that("a real 2-cycle (A -> B, B -> A) resolves by which side has real corpus rows", {
  m <- data.frame(venue = c("B", "B"))  # only B has real rows; A has none
  va <- data.frame(alias = c("A", "B"), canonical_venue = c("B", "A"))
  conn <- .venue_fixture_conn(m, va)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  flatten_venue_alias_table(conn = conn, dry_run = FALSE)

  out <- DBI::dbGetQuery(conn, "SELECT * FROM venue_aliases")
  # A must now point at B (the real one); B must not survive as an alias of A.
  expect_equal(out$canonical_venue[out$alias == "A"], "B")
  expect_false("B" %in% out$alias)
})

test_that("store_venue_aliases() auto-flattens after writing -- the actual regression", {
  # This IS the 2026-09-01 bug reproduced directly: an existing row says
  # "OldName -> Canonical"; the new map being stored says
  # "Canonical -> LongerName" (a longer, more-qualified string winning the
  # tie-break) -- store_venue_aliases() must not leave that as a live chain.
  m <- data.frame(venue = c("LongerName", "LongerName", "LongerName"))
  va <- data.frame(alias = "OldName", canonical_venue = "Canonical")
  conn <- .venue_fixture_conn(m, va)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  new_map <- data.table::data.table(venue = "Canonical", canonical_venue = "LongerName")
  suppressWarnings(store_venue_aliases(new_map, conn = conn, dry_run = FALSE))

  out <- DBI::dbGetQuery(conn, "SELECT * FROM venue_aliases")
  expect_false(any(out$canonical_venue %in% out$alias))
  # OldName must end up at the real, populated final target, not stuck at
  # the now-dead intermediate "Canonical".
  expect_equal(out$canonical_venue[out$alias == "OldName"], "LongerName")
})

test_that("a chain with real evidence on BOTH sides leaves the table untouched", {
  # The common case: an alias and its target are both real, populated venue
  # strings and there is no chain at all -- nothing should move.
  m <- data.frame(venue = c("A", "B"))
  va <- data.frame(alias = "A", canonical_venue = "B")
  conn <- .venue_fixture_conn(m, va)
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))

  changed <- flatten_venue_alias_table(conn = conn, dry_run = FALSE)
  expect_equal(nrow(changed), 0L)
  out <- DBI::dbGetQuery(conn, "SELECT * FROM venue_aliases")
  expect_equal(out$canonical_venue, "B")
})
