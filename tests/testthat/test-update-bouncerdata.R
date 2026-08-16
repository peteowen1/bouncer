# Regression tests for update_bouncerdata() tag parsing (bouncer H6,
# ECOSYSTEM-FIX-PLAN.md B8). Daily release tags look like "v2026.02.26"
# (leading "v", dot-separated %Y.%m.%d); as.Date() can't infer that format
# unassisted, so parse_release_tag_date() strips the "v" and parses
# explicitly. Before the fix, every tag failed to parse, fell back to the
# 1970-01-01 epoch on both sides, and update_bouncerdata() always reported
# "already up to date" regardless of what was actually on the remote.

local_manifest_dir <- function(local_tag, envir = parent.frame()) {
  # .local_envir = envir (the caller's frame, i.e. the enclosing test_that()
  # block) so the tempdir survives for the whole test instead of being
  # unlinked when this helper returns.
  dir <- withr::local_tempdir(.local_envir = envir)
  json_dir <- file.path(dir, "json_files")
  dir.create(json_dir, recursive = TRUE)
  jsonlite::write_json(
    list(release_date = local_tag),
    file.path(json_dir, "manifest.json"),
    auto_unbox = TRUE
  )
  dir
}

test_that("update_bouncerdata parses 'v2026.02.26'-style tags and detects a newer remote", {
  dir <- local_manifest_dir("v2026.01.01")

  installed <- FALSE
  testthat::local_mocked_bindings(
    get_latest_release = function(repo, type = "any") list(tag_name = "v2026.02.26"),
    install_bouncerdata_from_release = function(...) {
      installed <<- TRUE
      invisible(NULL)
    }
  )

  result <- update_bouncerdata(data_dir = dir)

  expect_true(result)
  expect_true(installed)
})

test_that("update_bouncerdata reports up to date when local and remote tags parse equal", {
  dir <- local_manifest_dir("v2026.02.26")

  installed <- FALSE
  testthat::local_mocked_bindings(
    get_latest_release = function(repo, type = "any") list(tag_name = "v2026.02.26"),
    install_bouncerdata_from_release = function(...) {
      installed <<- TRUE
      invisible(NULL)
    }
  )

  result <- update_bouncerdata(data_dir = dir)

  expect_false(result)
  expect_false(installed)
})

test_that("update_bouncerdata does not treat an older remote tag as an update", {
  dir <- local_manifest_dir("v2026.02.26")

  installed <- FALSE
  testthat::local_mocked_bindings(
    get_latest_release = function(repo, type = "any") list(tag_name = "v2026.01.01"),
    install_bouncerdata_from_release = function(...) {
      installed <<- TRUE
      invisible(NULL)
    }
  )

  result <- update_bouncerdata(data_dir = dir)

  expect_false(result)
  expect_false(installed)
})
