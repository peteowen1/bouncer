# A name substituted for a registry id does not fail -- it SPLITS a player.
# Every rating keyed on player_id then treats the same man as a new,
# low-exposure person. bouncerverse#74: 995 of 1,318 matches in 2026 stored
# names, no current player had 2026 form in their rating, and 3,139 phantom
# identities entered every leaderboard. Nothing errored, for months.
#
# So the fallback stays -- a registryless match is still worth parsing -- but
# it must be counted and it must be loud.

fake_match <- function(with_registry = TRUE) {
  people <- list("A Batter" = "aaaa1111", "B Bowler" = "bbbb2222")
  j <- list(
    info = list(
      match_type = "T20", gender = "male", dates = list("2026-05-01"),
      teams = list("Team A", "Team B"),
      players = list("Team A" = list("A Batter"), "Team B" = list("B Bowler")),
      registry = if (with_registry) list(people = people) else NULL
    ),
    innings = list(list(team = "Team A", overs = list(list(over = 0, deliveries = list(
      list(batter = "A Batter", bowler = "B Bowler", non_striker = "A Batter",
           runs = list(batter = 4, extras = 0, total = 4)))))))
  )
  j
}

test_that("a match WITH a registry resolves every reference and warns about none", {
  res <- expect_no_warning(parse_all_data(fake_match(TRUE), parse_match_info(fake_match(TRUE), "m1")))
  expect_gt(res$registry_resolved, 0)
  expect_equal(res$registry_fallback, 0)
  expect_true(all(grepl("^[0-9a-f]{8}$", res$deliveries$batter_id)))
})

test_that("a match WITHOUT a registry warns and reports the fallback count", {
  expect_warning(res <- parse_all_data(fake_match(FALSE), parse_match_info(fake_match(FALSE), "m2")),
                 "no registry entry")  # cli wraps "stored as NAMES" across a line
  res <- suppressWarnings(parse_all_data(fake_match(FALSE), parse_match_info(fake_match(FALSE), "m2")))
  expect_gt(res$registry_fallback, 0)
  expect_equal(res$registry_resolved, 0)
  # and the id really is the name, which is the harm being reported
  expect_true(any(res$deliveries$batter_id == "A Batter"))
})

test_that("the counters are returned so a loader can act on them", {
  res <- suppressWarnings(parse_all_data(fake_match(FALSE), parse_match_info(fake_match(FALSE), "m3")))
  expect_true(all(c("registry_resolved", "registry_fallback") %in% names(res)))
  expect_true(is.integer(res$registry_fallback))
})

test_that("a real cached 2026 match resolves entirely to registry ids", {
  # 1512764 is one of the 323 matches that ingested cleanly; it is the
  # control for the 995 that did not.
  f <- file.path("C:/dev/bouncerverse/bouncerdata/json_files", "1512764.json")
  skip_if_not(file.exists(f), "cricsheet json cache not available")
  j <- jsonlite::fromJSON(f, simplifyVector = FALSE)
  res <- expect_no_warning(parse_all_data(j, parse_match_info(j, "1512764")))
  expect_equal(res$registry_fallback, 0)
  expect_true(all(grepl("^[0-9a-f]{8}$", unique(res$deliveries$batter_id))))
})

test_that("the PUBLIC entry point forwards the registry counters", {
  # parse_cricsheet_json() rebuilds its return list by hand, and it dropped
  # registry_resolved/registry_fallback -- so the #74 instrumentation, whose
  # whole purpose was letting a batch loader quarantine a match that resolved
  # nothing, reached no caller. Found by a documentation audit, not a failure.
  f <- file.path("C:/dev/bouncerverse/bouncerdata/json_files", "1512764.json")
  skip_if_not(file.exists(f), "cricsheet json cache not available")
  res <- parse_cricsheet_json(f)
  expect_true(all(c("registry_resolved", "registry_fallback") %in% names(res)))
  expect_equal(res$registry_fallback, 0)
  expect_gt(res$registry_resolved, 0)
})
