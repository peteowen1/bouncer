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
