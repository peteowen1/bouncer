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
