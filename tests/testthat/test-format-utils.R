# Tests for Format Utility Functions (format_utils.R)
#
# Covers: get_format_groups, get_gender_categories, get_max_balls

# ============================================================================
# get_format_groups() Tests
# ============================================================================

test_that("get_format_groups returns all three formats", {
  groups <- get_format_groups()

  expect_type(groups, "list")
  expect_true("t20" %in% names(groups))
  expect_true("odi" %in% names(groups))
  expect_true("test" %in% names(groups))
})

test_that("get_format_groups contains correct match types", {
  groups <- get_format_groups()

  expect_equal(groups$t20, c("T20", "IT20"))
  expect_equal(groups$odi, c("ODI", "ODM"))
  expect_equal(groups$test, c("Test", "MDM"))
})

test_that("get_format_groups match types are unique across formats", {
  groups <- get_format_groups()
  all_types <- unlist(groups)

  expect_equal(length(all_types), length(unique(all_types)))
})

# ============================================================================
# get_gender_categories() Tests
# ============================================================================

test_that("get_gender_categories returns both genders", {
  cats <- get_gender_categories()

  expect_type(cats, "list")
  expect_equal(cats$mens, "male")
  expect_equal(cats$womens, "female")
  expect_equal(length(cats), 2)
})

# ============================================================================
# get_max_balls() Tests
# ============================================================================

test_that("get_max_balls returns correct values per format", {
  expect_equal(get_max_balls("t20"), 120)
  expect_equal(get_max_balls("odi"), 300)
  expect_equal(get_max_balls("test"), 540)
})

test_that("get_max_balls handles format aliases", {
  expect_equal(get_max_balls("T20"), 120)
  expect_equal(get_max_balls("IT20"), 120)
  expect_equal(get_max_balls("ODI"), 300)
  expect_equal(get_max_balls("ODM"), 300)
  expect_equal(get_max_balls("Test"), 540)
  expect_equal(get_max_balls("MDM"), 540)
})

test_that("get_max_balls is consistent with get_max_overs", {
  # T20: 20 overs * 6 = 120 balls

  expect_equal(get_max_balls("t20"), get_max_overs("t20") * 6)
  # ODI: 50 overs * 6 = 300 balls
  expect_equal(get_max_balls("odi"), get_max_overs("odi") * 6)
  # Test: max_overs is NULL (unlimited), but max_balls = 540 (90 overs per day)
  expect_null(get_max_overs("test"))
  expect_equal(get_max_balls("test"), 540)
})

test_that("get_max_balls errors on unknown format", {
  expect_error(get_max_balls("unknown"), "Unknown cricket format")
})
