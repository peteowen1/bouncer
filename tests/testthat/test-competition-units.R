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
