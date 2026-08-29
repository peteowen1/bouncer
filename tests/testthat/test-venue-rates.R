# venue_result_rate used to include the match being predicted.
#
# The property under test is strict causality: the value for a match must be a
# function of matches at that ground STRICTLY BEFORE it, and of nothing else.
#
# Measured on the real corpus (3,071 Test/MDM matches, 289 venues, median 3
# matches per venue), correlation of the feature with the match's OWN outcome:
#
#   venue history      old      new
#   < 5 matches      0.684    0.061
#   < 10 matches     0.549    0.104
#   >= 30 matches    0.203    0.108
#
# The old value concentrated where the venue history is thin — the signature of
# a leak, since that is where the match's own outcome carries the most weight.
# What survives is flat across thickness, which is what a real venue effect
# looks like (bouncerverse#29).

rate_of  <- function(r, id) r$venue_result_rate[match(id, r$match_id)]
rate2    <- function(r, id) r$venue_mean[match(id, r$match_id)]
nprior2  <- function(r, id) r$n_prior[match(id, r$match_id)]
nprior_of <- function(r, id) r$n_prior[match(id, r$match_id)]

mk <- function(...) {
  d <- data.frame(..., stringsAsFactors = FALSE)
  d$match_date <- as.Date(d$match_date)
  d
}

test_that("the first match at a ground gets the prior and nothing else", {
  m <- mk(match_id = "m1", venue = "Lords", match_date = "2020-01-01",
          decided = 1L, is_result = 1L)
  r <- time_causal_venue_result_rate(m, prior_weight = 10, prior_rate = 0.6)
  expect_equal(r$venue_result_rate, 0.6)
  expect_true(r$at_prior)
  expect_equal(r$n_prior, 0L)
})

test_that("a match cannot see its own outcome", {
  # Two identical grounds differing ONLY in the second match's result. If the
  # feature for match 2 differs between them, it is reading its own label.
  base <- function(res) mk(match_id = c("m1", "m2"), venue = c("A", "A"),
                           match_date = c("2020-01-01", "2021-01-01"),
                           decided = c(1L, 1L), is_result = res)
  won  <- time_causal_venue_result_rate(base(c(1L, 1L)), prior_weight = 10, prior_rate = 0.6)
  drew <- time_causal_venue_result_rate(base(c(1L, 0L)), prior_weight = 10, prior_rate = 0.6)
  m2_won  <- rate_of(won, "m2")
  m2_drew <- rate_of(drew, "m2")
  expect_equal(m2_won, m2_drew)
  # And it did see match 1: (1 + 10*0.6) / (1 + 10)
  expect_equal(m2_won, (1 + 10 * 0.6) / 11)
})

test_that("earlier matches accumulate in date order", {
  m <- mk(match_id = c("a", "b", "c"), venue = "A",
          match_date = c("2020-01-01", "2021-01-01", "2022-01-01"),
          decided = 1L, is_result = c(1L, 1L, 0L))
  r <- time_causal_venue_result_rate(m, prior_weight = 10, prior_rate = 0.5)
  expect_equal(nprior_of(r, "a"), 0L)
  expect_equal(nprior_of(r, "b"), 1L)
  expect_equal(nprior_of(r, "c"), 2L)
  expect_equal(rate_of(r, "c"), (2 + 10 * 0.5) / 12)
})

test_that("same-day matches at one ground cannot see each other", {
  # A prediction for either has no access to the other's result.
  m <- mk(match_id = c("x", "y"), venue = "A",
          match_date = c("2021-06-01", "2021-06-01"),
          decided = 1L, is_result = c(1L, 0L))
  r <- time_causal_venue_result_rate(m, prior_weight = 10, prior_rate = 0.5)
  expect_equal(length(unique(r$venue_result_rate)), 1L)
  expect_true(all(r$n_prior == 0L))
})

test_that("venues are independent of each other", {
  m <- mk(match_id = c("a1", "b1", "a2"), venue = c("A", "B", "A"),
          match_date = c("2020-01-01", "2020-06-01", "2021-01-01"),
          decided = 1L, is_result = c(1L, 1L, 1L))
  r <- time_causal_venue_result_rate(m, prior_weight = 10, prior_rate = 0.5)
  expect_equal(nprior_of(r, "b1"), 0L)   # B has no history of its own
  expect_equal(nprior_of(r, "a2"), 1L)   # A has exactly one
})

test_that("undecided matches are excluded from the denominator but still scored", {
  m <- mk(match_id = c("a", "b", "c"), venue = "A",
          match_date = c("2020-01-01", "2021-01-01", "2022-01-01"),
          decided = c(1L, 0L, 1L), is_result = c(1L, 1L, 0L))
  r <- time_causal_venue_result_rate(m, prior_weight = 10, prior_rate = 0.5)
  # b is abandoned: it gets a value, and does not count toward c's history.
  expect_false(is.na(rate_of(r, "b")))
  expect_equal(nprior_of(r, "c"), 1L)
})

test_that("the default prior is the global decided result rate", {
  m <- mk(match_id = c("a", "b", "c", "d"), venue = c("A", "A", "B", "B"),
          match_date = c("2020-01-01", "2021-01-01", "2020-01-01", "2021-01-01"),
          decided = 1L, is_result = c(1L, 1L, 1L, 0L))
  r <- time_causal_venue_result_rate(m, prior_weight = 10)
  expect_equal(attr(r, "prior_rate"), 0.75)
})

test_that("a missing column is named rather than silently defaulted", {
  m <- mk(match_id = "a", venue = "A", match_date = "2020-01-01", decided = 1L)
  expect_error(time_causal_venue_result_rate(m), "is_result")
})

test_that("leave-one-out is NOT what this does", {
  # Recorded because LOO was tried and looked like the best result of the
  # session: it moved holdout mlogloss 0.8171 -> 0.5000. Subtracting the row's
  # own label turns the feature into an encoding of the target.
  m <- mk(match_id = c("a", "b"), venue = "A",
          match_date = c("2020-01-01", "2021-01-01"),
          decided = 1L, is_result = c(1L, 1L))
  r <- time_causal_venue_result_rate(m, prior_weight = 10, prior_rate = 0.5)
  # LOO for match a would be (1 - 1 + 5) / (2 - 1 + 10) = 0.4545.
  # Time-causal for match a is the bare prior, because nothing precedes it.
  expect_equal(rate_of(r, "a"), 0.5)
})

# ---- the numeric sibling: venue averages had the same defect ----------------
#
# Measured on the same 3,071 Test/MDM matches. Correlation of the feature with
# the match's OWN innings-1 total:
#
#   venue history      old      new
#   1 match          1.000    constant (no history -> the prior)
#   < 5 matches      0.764    0.114
#   >= 30 matches    0.172    0.077
#
# At the 79 one-match venues the old feature WAS the label, exactly, for both
# the result rate and the average. Unsmoothed, self-inclusive, and used in the
# Test WPA batch (bouncerverse#69).

test_that("a venue's first match gets the prior, not its own total", {
  m <- mk(match_id = "m1", venue = "A", match_date = "2020-01-01", inn1 = 400)
  r <- time_causal_venue_mean(m, "inn1", prior_weight = 5, prior_value = 300)
  expect_equal(r$venue_mean, 300)
  expect_true(r$at_prior)
})

test_that("a match cannot see its own total", {
  base <- function(v2) mk(match_id = c("a", "b"), venue = "A",
                          match_date = c("2020-01-01", "2021-01-01"),
                          inn1 = c(400, v2))
  hi <- time_causal_venue_mean(base(600), "inn1", prior_weight = 5, prior_value = 300)
  lo <- time_causal_venue_mean(base(100), "inn1", prior_weight = 5, prior_value = 300)
  expect_equal(rate2(hi, "b"), rate2(lo, "b"))
  expect_equal(rate2(hi, "b"), (400 + 5 * 300) / 6)
})

test_that("earlier totals accumulate, and NA contributes nothing", {
  m <- mk(match_id = c("a", "b", "c"), venue = "A",
          match_date = c("2020-01-01", "2021-01-01", "2022-01-01"),
          inn1 = c(400, NA, 200))
  r <- time_causal_venue_mean(m, "inn1", prior_weight = 5, prior_value = 300)
  expect_equal(nprior2(r, "c"), 1L)          # b had no total to contribute
  expect_equal(rate2(r, "c"), (400 + 5 * 300) / 6)
})

test_that("same-day matches at one ground cannot see each other", {
  m <- mk(match_id = c("x", "y"), venue = "A",
          match_date = c("2021-06-01", "2021-06-01"), inn1 = c(500, 100))
  r <- time_causal_venue_mean(m, "inn1", prior_weight = 5, prior_value = 300)
  expect_equal(length(unique(r$venue_mean)), 1L)
})

test_that("a missing value column is named", {
  m <- mk(match_id = "a", venue = "A", match_date = "2020-01-01", inn1 = 300)
  expect_error(time_causal_venue_mean(m, "not_a_column"), "not_a_column")
})

# ---- time_causal_hierarchical_mean: nested venue -> competition -> root -----
#
# bouncerverse#83: a single flat scalar prior serves 82-89% of a cross-
# competition T20 corpus, because most competitions have too little history at
# a shared venue to move away from the prior at all. This shrinks a chain of
# levels instead, so a sparse competition still borrows strength from the
# global root rather than being indistinguishable from every other sparse one.

hier_of <- function(r, id) r$hier_mean[match(id, r$match_id)]

test_that("the very first match anywhere gets the whole-sample mean", {
  m <- mk(match_id = "m1", venue = "A", competition = "X",
          match_date = "2020-01-01", inn1 = 400)
  r <- time_causal_hierarchical_mean(m, "inn1", levels = c("venue", "competition"),
                                     weights = c(venue = 5, competition = 20))
  expect_equal(r$hier_mean, attr(r, "overall_mean"))
  expect_equal(attr(r, "overall_mean"), 400)
})

test_that("a match cannot see its own total, at any level", {
  # root_prior_value pinned so the fixed root scalar itself (which, like
  # time_causal_venue_mean()'s default prior, is one constant shared by every
  # row rather than computed row-by-row) doesn't confound this toy 2-row case.
  base <- function(v2) mk(match_id = c("a", "b"), venue = "A", competition = "X",
                          match_date = c("2020-01-01", "2021-01-01"),
                          inn1 = c(400, v2))
  hi <- time_causal_hierarchical_mean(base(600), "inn1", levels = c("venue", "competition"),
                                      weights = c(venue = 5, competition = 20),
                                      root_prior_value = 300)
  lo <- time_causal_hierarchical_mean(base(100), "inn1", levels = c("venue", "competition"),
                                      weights = c(venue = 5, competition = 20),
                                      root_prior_value = 300)
  expect_equal(hier_of(hi, "b"), hier_of(lo, "b"))
})

test_that("a huge root prior weight degenerates to a single-level shrink", {
  # With root_prior_weight enormous, the root causal mean is pinned to the
  # whole-sample mean for every row (the causal component is negligible), so a
  # one-level hierarchy should agree with time_causal_venue_mean() using that
  # same fixed prior.
  m <- mk(match_id = c("a", "b", "c"), venue = c("A", "A", "B"), competition = "X",
          match_date = c("2020-01-01", "2021-01-01", "2020-06-01"),
          inn1 = c(400, 500, 200))
  overall <- mean(m$inn1)
  single <- time_causal_venue_mean(m, "inn1", prior_weight = 5, prior_value = overall)
  hier <- time_causal_hierarchical_mean(m, "inn1", levels = "venue",
                                        weights = c(venue = 5),
                                        root_prior_weight = 1e9)
  expect_equal(hier$hier_mean, single$venue_mean, tolerance = 1e-6)
})

test_that("a sparse competition borrows strength from the root, not just its own venue prior", {
  # Competition Y has one prior match (700) at a DIFFERENT venue than the one
  # being predicted, so the venue level alone has zero history and would fall
  # straight to the (unknown) competition mean. The hierarchy should show
  # competition Y's causal mean pulling AWAY from the flat root as its own
  # evidence accumulates.
  m <- mk(match_id = c("y1", "y2"), venue = c("V1", "V2"), competition = "Y",
          match_date = c("2020-01-01", "2020-06-01"), inn1 = c(700, 700))
  r <- time_causal_hierarchical_mean(m, "inn1", levels = c("venue", "competition"),
                                     weights = c(venue = 5, competition = 20),
                                     root_prior_weight = 30)
  # y1: no venue or competition history -> pure root (== overall mean, its only value).
  expect_equal(hier_of(r, "y1"), 700)
  # y2: venue V2 has no history either, but competition Y now has one prior
  # match at 700 -- its estimate should move toward 700, away from the root.
  expect_gt(hier_of(r, "y2"), hier_of(r, "y1") - 1e-9)
  expect_equal(hier_of(r, "y2"), 700)  # everything upstream is 700, so it stays 700
})

test_that("weights must be named exactly by levels", {
  m <- mk(match_id = "a", venue = "A", competition = "X",
          match_date = "2020-01-01", inn1 = 300)
  expect_error(
    time_causal_hierarchical_mean(m, "inn1", levels = c("venue", "competition"),
                                  weights = c(venue = 5)),
    "weights"
  )
})

test_that("a missing level column is named", {
  m <- mk(match_id = "a", venue = "A", match_date = "2020-01-01", inn1 = 300)
  expect_error(
    time_causal_hierarchical_mean(m, "inn1", levels = c("venue", "competition"),
                                  weights = c(venue = 5, competition = 20)),
    "competition"
  )
})
