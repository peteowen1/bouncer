# competition_units.R had no test coverage (flagged as housekeeping owed in
# bouncerverse docs/NEXT-STEPS.md). It became load-bearing when the Test bucket
# started using it as its competition key, so it gets covered here.

test_that("every Test row is the Test unit whatever the series is called", {
  # ~187 bilateral series names, several of them for the SAME contest:
  # India v Australia is filed as both "Border-Gavaskar Trophy" and "India tour
  # of Australia"; England v India as "England in India Test Series",
  # "England tour of India" AND "Pataudi Trophy".
  series <- c("The Ashes", "Border-Gavaskar Trophy", "India tour of Australia",
              "Pataudi Trophy", "England in India Test Series",
              "Basil D'Oliveira Trophy", "The Wisden Trophy",
              "a series name nobody has ever seen")
  out <- normalise_competition(series, rep("Test", length(series)))
  expect_true(all(out == "Test"))
  expect_false(anyNA(out))
})

test_that("sponsor names collapse to one continuous domestic competition", {
  county <- c("County Championship", "Specsavers County Championship",
              "LV= County Championship", "Bob Willis Trophy")
  expect_true(all(normalise_competition(county, rep("MDM", 4)) == "County Championship"))

  shield <- c("Sheffield Shield", "Marsh Sheffield Shield")
  expect_true(all(normalise_competition(shield, rep("MDM", 2)) == "Sheffield Shield"))

  icc <- c("ICC Intercontinental Cup", "ICC Intercontinental Shield")
  expect_true(all(normalise_competition(icc, rep("MDM", 2)) == "ICC Intercontinental"))
})

test_that("an unrecognised first-class competition returns NA rather than a guess", {
  # The whole point: a new competition must surface loudly as unrated, not be
  # folded into whichever unit happens to be nearby.
  expect_true(is.na(normalise_competition("Ranji Trophy", "MDM")))
  expect_true(is.na(normalise_competition(NA_character_, "MDM")))
})

test_that("normalise_competition refuses mismatched input lengths", {
  expect_error(normalise_competition(c("a", "b"), "Test"))
})

test_that(".competition_sql keeps event_name for T20/ODI and normalises for Test", {
  expect_match(.competition_sql("t20"), "event_name", fixed = TRUE)
  expect_match(.competition_sql("odi"), "event_name", fixed = TRUE)

  s <- .competition_sql("test")
  # Test rows short-circuit to the single unit before any name is consulted.
  expect_match(s, "LOWER\\(m\\.match_type\\) = 'test' THEN 'Test'")
  # The CASE is GENERATED from the map, so every mapping must appear in it --
  # this is what stops the SQL drifting from COMPETITION_UNIT_MAP.
  for (nm in names(COMPETITION_UNIT_MAP)) {
    expect_true(grepl(nm, s, fixed = TRUE),
                info = paste("missing from generated SQL:", nm))
  }
  # Unrecognised events must be NULL, not bucketed.
  expect_match(s, "ELSE NULL END", fixed = TRUE)
})

test_that("no query builds a competition key without going through .competition_sql", {
  # Regression guard for a real bug: fit_competition_factors() was moved onto
  # normalised units while the two rating queries still keyed on raw
  # event_name, so the factors joined onto nothing for 60.5% of Test deliveries
  # -- every Test series plus the sponsor-named county seasons -- which then
  # defaulted to reference difficulty and undid the entire adjustment. The
  # symptom was only a coverage WARNING, so nothing failed.
  #
  # Source-level check, in the spirit of test-versebus-sync.R. Skipped when the
  # sources are not present (R CMD check runs against the installed package).
  src <- NULL
  for (p in c("../../R/player_rating_v2.R", "R/player_rating_v2.R",
              testthat::test_path("../../R/player_rating_v2.R"))) {
    if (file.exists(p)) { src <- readLines(p, warn = FALSE); break }
  }
  skip_if(is.null(src), "package sources not available in this context")

  # The only legitimate mention of raw event_name as a competition key is inside
  # .competition_sql() itself (its T20/ODI branch and its generated CASE).
  hits <- grep("event_name", src, value = TRUE)
  hits <- hits[!grepl("^\\s*#", hits)]                 # drop comments
  offenders <- grep("AS comp|GROUP BY .*event_name", hits, value = TRUE)
  expect_equal(offenders, character(0),
               info = paste("raw event_name used as a competition key:",
                            paste(offenders, collapse = " | ")))
})

test_that("the Test reference set is the single elite unit", {
  expect_equal(default_competition_reference("test", "male"), "Test")
  # Every reference entry must be a unit normalise_competition can actually
  # produce, or the anchor would match nothing and the scale would be arbitrary.
  expect_true(all(COMPETITION_REFERENCE_TEST %in%
                    c("Test", unname(COMPETITION_UNIT_MAP))))
})

test_that("women's Test is refused rather than silently rated on 24 matches", {
  expect_error(default_competition_reference("test", "female"),
               "No reference set defined")
})

test_that("sponsor variants of one competition map to a single canonical name", {
  # England domestic T20 -- 1,554 matches across three names, more than the IPL.
  expect_equal(alias_competition("NatWest T20 Blast"),  "Vitality Blast")
  expect_equal(alias_competition("Vitality Blast Men"), "Vitality Blast")
  # South Africa domestic T20, three sponsors.
  expect_equal(alias_competition("Ram Slam T20 Challenge"), "CSA T20 Challenge")
  expect_equal(alias_competition("MiWAY T20 Challenge"),    "CSA T20 Challenge")
  # England domestic 50-over, and the women's equivalent.
  expect_equal(alias_competition("Royal London One-Day Cup"), "One-Day Cup")
  expect_equal(alias_competition("Rachael Heyhoe Flint Trophy"),
               "ECB Women's One-Day Cup")
  # Global events renamed over time.
  expect_equal(alias_competition("ICC World Twenty20"), "ICC Men's T20 World Cup")
  expect_equal(alias_competition("ICC World Cup"),      "ICC Cricket World Cup")
})

test_that("alias_competition is a rename, not a partition", {
  # Unlike COMPETITION_UNIT_MAP, an unlisted competition passes through
  # unchanged -- limited-overs cricket has hundreds of genuinely distinct
  # competitions and they cannot be enumerated.
  expect_equal(alias_competition("Indian Premier League"), "Indian Premier League")
  expect_equal(alias_competition("a competition invented for this test"),
               "a competition invented for this test")
  expect_true(is.na(alias_competition(NA_character_)))
  expect_equal(length(alias_competition(character(0))), 0L)
})

test_that("every reference set names a CANONICAL competition, not a retired alias", {
  # This is the defect the aliases exposed: COMPETITION_REFERENCE_ODI_FEMALE
  # anchored on "Rachael Heyhoe Flint Trophy", which ended in 2024, so from
  # 2025 the competition carrying that cricket was unanchored. A reference set
  # that names an alias silently anchors on a competition that no longer
  # appears under that name.
  for (nm in c("COMPETITION_REFERENCE_T20", "COMPETITION_REFERENCE_ODI",
               "COMPETITION_REFERENCE_T20_FEMALE", "COMPETITION_REFERENCE_ODI_FEMALE")) {
    ref <- get(nm)
    bad <- intersect(ref, names(COMPETITION_ALIASES))
    expect_equal(bad, character(0),
                 info = paste(nm, "names a retired alias:", paste(bad, collapse = ", ")))
  }
})

test_that(".competition_sql applies aliases for T20/ODI and units for Test", {
  s20 <- .competition_sql("t20")
  expect_match(s20, "NatWest T20 Blast", fixed = TRUE)
  expect_match(s20, "ELSE m.event_name END", fixed = TRUE)  # rename, not partition
  # Test still partitions to units and returns NULL for the unrecognised.
  expect_match(.competition_sql("test"), "ELSE NULL END", fixed = TRUE)
})

test_that("fit_competition_factors stamps the basis it was fitted on", {
  # Without this stamp a caller reusing one factors object across metrics has no
  # way to detect that a runs-basis (batting average) difficulty is being
  # applied to a wickets (survival) rating -- which produces a correctly
  # ordered, plausible, wrongly calibrated leaderboard.
  skip_if_not(exists("fit_competition_factors"))
  f <- data.table::data.table(comp = "x", factor = 1, n_bridges = NA_integer_, step = 0L)
  data.table::setattr(f, "basis", "runs")
  expect_equal(attr(f, "basis"), "runs")
})

test_that("a factors object on the wrong basis is refused, before any query runs", {
  # Regression guard, added 2026-08-19 after a code review caught it.
  #
  # When the rating switched from dividing the raw value by a factor to
  # compressing the DEVIATION by it, the `basis = want_basis` argument was
  # dropped from the internal fit and this test's predecessor was deleted along
  # with the guard it covered. The factor is still a ratio of per-dismissal
  # rates and `metric = "wickets"` still reads r.waa, so a runs-basis
  # (runs-per-dismissal) factor was silently compressing a survival quantity.
  # Not live in the pipeline, which runs metric = "composite", but reachable by
  # any caller of an exported function.
  #
  # The connection is deliberately dead: if the check ran AFTER the query this
  # would abort with a DBI error instead, so the test proves the validation
  # happens up front rather than merely that it happens.
  runs_basis <- data.table::data.table(comp = "Test", factor = 1,
                                       n_bridges = NA_integer_, step = 0L)
  data.table::setattr(runs_basis, "basis", "runs")
  dead <- structure(list(), class = "DBIConnection")

  expect_error(
    calculate_player_rating_v2("test", "male", role = "batter", conn = dead,
                               factors = runs_basis, metric = "wickets"),
    "fitted on the .*runs.* basis")

  # A MATCHING basis must not trip the guard. It still errors, on the dead
  # connection, so assert on the message rather than on success: the point is
  # that the failure is no longer about the basis.
  msg <- tryCatch({
    calculate_player_rating_v2("test", "male", role = "batter", conn = dead,
                               factors = runs_basis, metric = "composite")
    ""
  }, error = function(e) conditionMessage(e))
  expect_false(grepl("basis", msg, fixed = TRUE))
})

test_that("an offsets object fitted on the wrong side is refused, before any query runs", {
  # The runs/survival BASIS guard this replaces is gone: the rating no longer
  # divides by a factor, so there is no basis to mismatch. The analogous hazard
  # survives -- a BATTING offset reaching a BOWLING rating is correctly ordered,
  # plausible, and wrongly calibrated, which is this codebase's recurring shape.
  #
  # The connection is deliberately a dead one. If the check ran AFTER the query
  # this would abort with a DBI error instead, so the test proves the validation
  # happens up front rather than merely that it happens.
  bat <- data.table::data.table(comp = "Test", offset = 0, m_here = 0, m_ref = 0,
                                n_bridges = NA_integer_, evidence = NA_real_,
                                step = 0L)
  data.table::setattr(bat, "id_col", "batter_id")
  dead <- structure(list(), class = "DBIConnection")

  expect_error(
    calculate_player_rating_v2("test", "male", role = "bowler", conn = dead,
                               offsets = bat),
    "not fitted on the .*bowler.* side")

  # An offsets object with no stamp at all is refused too: silently trusting an
  # unlabelled one is how the mismatch would get through.
  bare <- data.table::copy(bat)
  data.table::setattr(bare, "id_col", NULL)
  expect_error(
    calculate_player_rating_v2("test", "male", role = "batter", conn = dead,
                               offsets = bare),
    "not fitted on the .*batter.* side")
})

# Bilateral tour bucketing ----------------------------------------------------

test_that("the top-nations list is by playing standard, not ICC status", {
  expect_length(COMPETITION_TOP_NATIONS, 10L)
  expect_true(all(c("Afghanistan", "Australia", "Bangladesh", "England", "India",
                    "New Zealand", "Pakistan", "South Africa", "Sri Lanka",
                    "West Indies") %in% COMPETITION_TOP_NATIONS))
  # Ireland and Zimbabwe are ICC Full Members and are deliberately excluded on
  # playing standard. If someone "corrects" this back to the Full Member list,
  # this test should stop them and send them to the comment in competition_units.R
  expect_false("Ireland" %in% COMPETITION_TOP_NATIONS)
  expect_false("Zimbabwe" %in% COMPETITION_TOP_NATIONS)
  expect_false(any(c("Malta", "Gibraltar", "Nepal", "Namibia", "Bulgaria") %in%
                   COMPETITION_TOP_NATIONS))
})

test_that("the tour pattern does not use SIMILAR TO", {
  # DuckDB's SIMILAR TO is RE2 regex: '%' is a LITERAL, not a wildcard, and the
  # pattern must match the whole string. An earlier version used it here and
  # matched NOTHING, silently leaving every bilateral series as its own
  # competition -- the failure looked exactly like success.
  expect_false(grepl("SIMILAR TO", COMPETITION_TOUR_PATTERN_SQL, fixed = TRUE))
  expect_true(grepl("regexp_matches", COMPETITION_TOUR_PATTERN_SQL, fixed = TRUE))
})

test_that("limited-overs competition SQL emits the four bilateral buckets", {
  for (fmt in c("t20", "odi")) {
    sql <- bouncer:::.competition_sql(fmt)
    expect_true(grepl("International (Top Nations)", sql, fixed = TRUE))
    expect_true(grepl("International (Associate)", sql, fixed = TRUE))
    expect_true(grepl("International (Developing)", sql, fixed = TRUE))
    expect_true(grepl("International (Mixed)", sql, fixed = TRUE))
    # the bucket only applies to international cricket -- franchise leagues and
    # named tournaments must keep their own identity
    expect_true(grepl("team_type = 'international'", sql, fixed = TRUE))
  }
})

test_that("Test competition SQL is unaffected by the bilateral buckets", {
  sql <- bouncer:::.competition_sql("test")
  expect_false(grepl("International (", sql, fixed = TRUE))
})

test_that("the World Cup associate tier is data-derived and excludes top nations", {
  expect_true(all(c("Ireland", "Netherlands", "Zimbabwe", "Scotland", "Namibia",
                    "Nepal", "Oman") %in% COMPETITION_WC_ASSOCIATES))
  # no overlap with the top tier, or a match would match two buckets
  expect_length(intersect(COMPETITION_WC_ASSOCIATES, COMPETITION_TOP_NATIONS), 0L)
  # sides that have never reached a T20 World Cup must NOT be here -- that is
  # the whole point of the Associate/Developing split (Karanbir Singh ranked 3rd
  # among T20 men off European associate cricket when they shared a bucket)
  expect_false(any(c("Malta", "Gibraltar", "Bulgaria", "Hungary", "Serbia") %in%
                   COMPETITION_WC_ASSOCIATES))
})
