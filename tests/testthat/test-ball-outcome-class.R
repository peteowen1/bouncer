# The label the outcome models are trained against. It was inline in the
# trainer's mutate, so any comparison script had to rebuild it by hand -- one
# edit away from scoring against a different target and reporting the
# difference as a model result (bouncerverse#65).

test_that("the seven classes map as the trainer defines them", {
  expect_equal(ball_outcome_class(0, TRUE), 0L)   # wicket wins over runs
  expect_equal(ball_outcome_class(4, TRUE), 0L)
  expect_equal(ball_outcome_class(0, FALSE), 1L)
  expect_equal(ball_outcome_class(1, FALSE), 2L)
  expect_equal(ball_outcome_class(2, FALSE), 3L)
  expect_equal(ball_outcome_class(3, FALSE), 4L)
  expect_equal(ball_outcome_class(4, FALSE), 5L)
  expect_equal(ball_outcome_class(6, FALSE), 6L)
})

test_that("undefined run values are NA, not silently bucketed", {
  # A 5 is real (overthrows) and rare. Bucketing it into 4 or 6 would move
  # mass into a class the model was never trained to expect.
  expect_true(is.na(ball_outcome_class(5, FALSE)))
  expect_true(is.na(ball_outcome_class(7, FALSE)))
})

test_that("a wide is its own class, not silently a dot ball (#81/D-P50 stage 5)", {
  # runs_batter is always 0 on a genuine wide (the batter cannot play a shot
  # at a ball called wide) -- before `wides` was wired in, this fell through
  # to the runs_batter == 0 branch and trained as a dot ball. Class 7 matches
  # OUTCOME_CATEGORIES's order: wicket, 0, 1, 2, 3, 4, 6, wide.
  expect_equal(ball_outcome_class(0, FALSE, wides = 1L), 7L)
  expect_equal(ball_outcome_class(0, FALSE, wides = 4L), 7L)
  # A wicket on a wide (stumped/run-out/hit-wicket) still wins.
  expect_equal(ball_outcome_class(0, TRUE, wides = 1L), 0L)
  # A missing/zero wides value is "not wide", matching the old 7-class calls.
  expect_equal(ball_outcome_class(0, FALSE, wides = 0L), 1L)
  expect_equal(ball_outcome_class(0, FALSE, wides = NA_integer_), 1L)
})

test_that("the wides default keeps existing 3-argument-less callers unchanged", {
  expect_equal(ball_outcome_class(0, TRUE), 0L)
  expect_equal(ball_outcome_class(0, FALSE), 1L)
  expect_equal(ball_outcome_class(6, FALSE), 6L)
})

test_that("wides is vectorised and recycles a scalar against a longer vector", {
  r <- c(0L, 0L, 1L)
  w <- c(FALSE, FALSE, FALSE)
  out <- ball_outcome_class(r, w, wides = 0L)
  expect_length(out, 3)
  expect_equal(out, c(1L, 1L, 2L))

  wides_vec <- c(0L, 3L, 0L)
  out2 <- ball_outcome_class(r, w, wides = wides_vec)
  expect_equal(out2, c(1L, 7L, 2L))
})

test_that("it is vectorised and preserves length", {
  r <- c(0L, 1L, 4L, 6L, 5L)
  w <- c(FALSE, FALSE, FALSE, FALSE, FALSE)
  out <- ball_outcome_class(r, w)
  expect_length(out, 5)
  expect_equal(out, c(1L, 2L, 5L, 6L, NA_integer_))
})

test_that("integer and logical wicket flags agree", {
  expect_equal(ball_outcome_class(1, 1L), ball_outcome_class(1, TRUE))
  expect_equal(ball_outcome_class(1, 0L), ball_outcome_class(1, FALSE))
})

test_that("the trainer uses ball_outcome_class() rather than its own copy", {
  # The mapping was extracted in #65 so a checker could not rebuild it by hand
  # and drift. The trainer then kept its own case_when() anyway, leaving two
  # declarations of one truth -- the exact defect the extraction addressed.
  # A grep test is crude but it is the only thing that notices a SECOND
  # declaration reappearing.
  f <- testthat::test_path("..", "..", "data-raw", "models", "ball-outcome",
                           "02_train_full_model.R")
  skip_if_not(file.exists(f), "trainer not available")
  txt <- readLines(f, warn = FALSE)
  expect_true(any(grepl("ball_outcome_class(", txt, fixed = TRUE)),
              info = "trainer no longer calls the shared label function")
  # and no local re-declaration of the same 7-class mapping
  expect_false(any(grepl("runs_batter == 6 ~ 6L", txt, fixed = TRUE)),
               info = "trainer has re-grown its own copy of the outcome mapping")
})
