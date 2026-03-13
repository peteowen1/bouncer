# Tests for Visualization Functions (visualization.R)
#
# These test that functions return ggplot objects without error.
# Full rendering requires DB data, so we test structure only.

# ============================================================================
# theme_bouncer() Tests
# ============================================================================

test_that("theme_bouncer returns a ggplot2 theme", {
  skip_if_not_installed("ggplot2")

  theme <- theme_bouncer()
  expect_s3_class(theme, "theme")
})

test_that("theme_bouncer accepts custom base_size", {
  skip_if_not_installed("ggplot2")

  theme_small <- theme_bouncer(base_size = 8)
  theme_large <- theme_bouncer(base_size = 16)

  expect_s3_class(theme_small, "theme")
  expect_s3_class(theme_large, "theme")
})

test_that("theme_bouncer can be added to a ggplot", {
  skip_if_not_installed("ggplot2")

  p <- ggplot2::ggplot(data.frame(x = 1:5, y = 1:5), ggplot2::aes(x, y)) +
    ggplot2::geom_point() +
    theme_bouncer()

  expect_s3_class(p, "ggplot")
})
