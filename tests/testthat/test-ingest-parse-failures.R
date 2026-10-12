test_that("batch_load_matches stops when a new file fails to parse", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("arrow")
  db <- tempfile(fileext = ".duckdb")
  suppressMessages(initialize_bouncer_database(path = db, verbose = FALSE))
  on.exit(unlink(db), add = TRUE)
  bad <- file.path(tempdir(), "9999991.json")
  writeLines('{"meta": {"data_version": "1.1.0"}, "info": ', bad)
  on.exit(unlink(bad), add = TRUE)
  # before 2026-10-12 this warned "1 files failed to parse" and returned 0
  expect_error(suppressMessages(batch_load_matches(bad, path = db, progress = FALSE, parallel = FALSE)),
               "failed to parse")
})
