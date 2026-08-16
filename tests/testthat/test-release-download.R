# Regression tests for download_release_asset()'s manifest sha256 verification
# (ECOSYSTEM-FIX-PLAN.md B8 - install_parquets_from_release() and other
# download sites verify sha256 vs bus_manifest.json WHEN PRESENT; NULL
# manifest -> unchanged legacy behaviour).

test_that("download_release_asset errors on sha256 mismatch and leaves dest untouched", {
  dir <- withr::local_tempdir()
  dest <- file.path(dir, "table.parquet")

  bad_entry <- list(name = "table.parquet", sha256 = strrep("0", 64),
                     bytes = 4, rows = NA_integer_)
  manifest <- list(schema_version = 1L, tag = "cricsheet", assets = list(bad_entry))

  testthat::local_mocked_bindings(
    req_perform = function(req, path = NULL, ...) {
      writeLines("bogus content", path)
      invisible(NULL)
    },
    .package = "httr2"
  )

  expect_error(
    download_release_asset("https://example.com/table.parquet", dest,
                            show_progress = FALSE, manifest = manifest),
    class = "vb_error_integrity"
  )
  expect_false(file.exists(dest))
})

test_that("download_release_asset passes through and installs when sha256 matches manifest", {
  dir <- withr::local_tempdir()
  dest <- file.path(dir, "table.parquet")
  content <- "matching content"

  tmpf <- tempfile()
  writeLines(content, tmpf)
  good_sha <- digest::digest(tmpf, algo = "sha256", file = TRUE)
  unlink(tmpf)

  entry <- list(name = "table.parquet", sha256 = good_sha, bytes = 4, rows = NA_integer_)
  manifest <- list(schema_version = 1L, tag = "cricsheet", assets = list(entry))

  testthat::local_mocked_bindings(
    req_perform = function(req, path = NULL, ...) {
      writeLines(content, path)
      invisible(NULL)
    },
    .package = "httr2"
  )

  download_release_asset("https://example.com/table.parquet", dest,
                          show_progress = FALSE, manifest = manifest)

  expect_true(file.exists(dest))
  expect_identical(readLines(dest), content)
})

test_that("download_release_asset warns but proceeds for an uncommitted asset (lenient mode)", {
  dir <- withr::local_tempdir()
  dest <- file.path(dir, "unlisted.parquet")

  manifest <- list(schema_version = 1L, tag = "cricsheet", assets = list())

  testthat::local_mocked_bindings(
    req_perform = function(req, path = NULL, ...) {
      writeLines("content", path)
      invisible(NULL)
    },
    .package = "httr2"
  )

  expect_warning(
    download_release_asset("https://example.com/unlisted.parquet", dest,
                            show_progress = FALSE, manifest = manifest),
    "uncommitted"
  )
  expect_true(file.exists(dest))
})

test_that("download_release_asset with no manifest is unchanged legacy behaviour", {
  dir <- withr::local_tempdir()
  dest <- file.path(dir, "table.parquet")

  testthat::local_mocked_bindings(
    req_perform = function(req, path = NULL, ...) {
      writeLines("anything", path)
      invisible(NULL)
    },
    .package = "httr2"
  )

  download_release_asset("https://example.com/table.parquet", dest, show_progress = FALSE)
  expect_true(file.exists(dest))
})
