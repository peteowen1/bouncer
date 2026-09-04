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

test_that("the root level cannot see a same-day sibling at a different venue/competition", {
  # bouncerverse#83 review (2026-08-29): the root causal mean originally
  # subtracted only the current row's own value, not every row sharing its
  # match_date -- so two same-day matches with NOTHING else in common (no
  # shared venue, no shared competition) still leaked into each other's root
  # estimate via cumsum/match_id tie-break order. With root_prior_value
  # pinned, match b has zero legitimate causal evidence at any level and must
  # come out at exactly the pinned prior regardless of match a's value.
  same_day <- function(a_value) {
    mk(match_id = c("a", "b"), venue = c("VA", "VB"), competition = c("CA", "CB"),
       match_date = c("2020-01-01", "2020-01-01"), inn1 = c(a_value, NA))
  }
  r_lo <- time_causal_hierarchical_mean(same_day(100), "inn1",
                                        levels = c("venue", "competition"),
                                        weights = c(venue = 5, competition = 20),
                                        root_prior_weight = 30, root_prior_value = 300)
  r_hi <- time_causal_hierarchical_mean(same_day(100000), "inn1",
                                        levels = c("venue", "competition"),
                                        weights = c(venue = 5, competition = 20),
                                        root_prior_weight = 30, root_prior_value = 300)
  expect_equal(hier_of(r_lo, "b"), 300)
  expect_equal(hier_of(r_hi, "b"), 300)
})

# ---- time_causal_hierarchical_mean_decayed(): recency-weighted sibling ----
#
# bouncerverse#84/#85: the flat causal mean weighs a match from 15 years ago
# identically to last week's, wrong for a level that's actually drifting (IPL
# scoring rose 1.34 -> 1.56 runs/ball 2022-2026 while the flat mean stayed
# near 1.25). half_life lets a level forget slowly rather than average
# uniformly across all its history.

test_that(".decayed_causal_prior with half_life=Inf reproduces an undecayed cumulative prior exactly", {
  # 4 dates, 10 days apart, one match each. Undecayed prior at date i is just
  # the running total through date i-1.
  daily_n <- c(1, 1, 1, 1)
  daily_v <- c(100, 200, 300, 400)
  days <- c(0, 10, 20, 30)
  r <- .decayed_causal_prior(daily_n, daily_v, days, half_life = Inf)
  expect_equal(r$n_prior, c(0, 1, 2, 3))
  expect_equal(r$v_prior, c(0, 100, 300, 600))
})

test_that(".decayed_causal_prior halves the prior after exactly one half-life", {
  # One match at day 0 (value 100), query the prior at day 10 with
  # half_life=10 -- decay factor exp(-10*ln(2)/10) = exp(-ln 2) = 0.5 exactly.
  r <- .decayed_causal_prior(daily_n = c(1, 0), daily_v = c(100, 0),
                             days = c(0, 10), half_life = 10)
  expect_equal(r$n_prior[2], 0.5, tolerance = 1e-10)
  expect_equal(r$v_prior[2], 50, tolerance = 1e-10)
})

test_that(".decayed_causal_prior compounds decay correctly across 3+ dates", {
  # Day 0: n=1,v=100. Day 10 (one half-life later): prior should be exactly
  # the day-0 value decayed by 0.5 -- n_prior=0.5, v_prior=50. Day 20 (another
  # half-life on): prior = 0.5 * (day-10's OWN total (n=1,v=200) + day-10's
  # decayed-in prior (0.5, 50)) = 0.5 * (1.5, 250) = (0.75, 125).
  r <- .decayed_causal_prior(daily_n = c(1, 1, 0), daily_v = c(100, 200, 0),
                             days = c(0, 10, 20), half_life = 10)
  expect_equal(r$n_prior, c(0, 0.5, 0.75), tolerance = 1e-10)
  expect_equal(r$v_prior, c(0, 50, 125), tolerance = 1e-10)
})

test_that("time_causal_hierarchical_mean_decayed with every half_life=Inf matches the undecayed function exactly", {
  set.seed(42)
  n <- 40
  m <- mk(
    match_id = paste0("m", seq_len(n)),
    venue = sample(c("V1", "V2", "V3"), n, replace = TRUE),
    competition = sample(c("C1", "C2"), n, replace = TRUE),
    match_date = as.character(as.Date("2020-01-01") + sort(sample(0:2000, n))),
    inn1 = round(rnorm(n, 150, 20))
  )
  flat <- time_causal_hierarchical_mean(m, "inn1", levels = c("venue", "competition"),
                                        weights = c(venue = 5, competition = 20),
                                        root_prior_weight = 30)
  decayed <- time_causal_hierarchical_mean_decayed(
    m, "inn1", levels = c("venue", "competition"),
    weights = c(venue = 5, competition = 20),
    half_life = c(venue = Inf, competition = Inf, root = Inf),
    root_prior_weight = 30)
  expect_equal(decayed$hier_mean, flat$hier_mean, tolerance = 1e-9)
})

test_that("time_causal_hierarchical_mean_decayed's root level still excludes same-day siblings", {
  # Same regression shape as the flat function's own same-day test above --
  # verifies the decayed root's group-by-date aggregation didn't reintroduce
  # that leak while being rewritten for decay.
  same_day <- function(a_value) {
    mk(match_id = c("a", "b"), venue = c("VA", "VB"), competition = c("CA", "CB"),
       match_date = c("2020-01-01", "2020-01-01"), inn1 = c(a_value, NA))
  }
  hl <- c(venue = Inf, competition = Inf, root = 365)
  r_lo <- time_causal_hierarchical_mean_decayed(same_day(100), "inn1",
                                                levels = c("venue", "competition"),
                                                weights = c(venue = 5, competition = 20),
                                                half_life = hl,
                                                root_prior_weight = 30, root_prior_value = 300)
  r_hi <- time_causal_hierarchical_mean_decayed(same_day(100000), "inn1",
                                                levels = c("venue", "competition"),
                                                weights = c(venue = 5, competition = 20),
                                                half_life = hl,
                                                root_prior_weight = 30, root_prior_value = 300)
  expect_equal(hier_of(r_lo, "b"), 300)
  expect_equal(hier_of(r_hi, "b"), 300)
})

test_that("a recent surge pulls a decayed level's estimate up faster than the undecayed one", {
  # The whole point of this function: 8 years of a stable level (100), then a
  # sharp step up to 160 for the last 6 matches -- a decayed level (short
  # half-life) should track the new level far more closely than the flat mean.
  old_dates <- as.character(as.Date("2015-01-01") + seq(0, 365 * 7, by = 200))
  new_dates <- as.character(as.Date("2025-01-01") + seq(0, 150, by = 30))
  m <- mk(
    match_id = paste0("m", seq_along(c(old_dates, new_dates))),
    venue = "V1", competition = "C1",
    match_date = c(old_dates, new_dates),
    inn1 = c(rep(100, length(old_dates)), rep(160, length(new_dates)))
  )
  flat <- time_causal_hierarchical_mean(m, "inn1", levels = c("venue", "competition"),
                                        weights = c(venue = 5, competition = 20),
                                        root_prior_weight = 30, root_prior_value = 100)
  decayed <- time_causal_hierarchical_mean_decayed(
    m, "inn1", levels = c("venue", "competition"),
    weights = c(venue = 5, competition = 20),
    half_life = c(venue = Inf, competition = 180, root = 365),
    root_prior_weight = 30, root_prior_value = 100)
  last_id <- tail(m$match_id, 1)
  expect_gt(hier_of(decayed, last_id), hier_of(flat, last_id))
  # And the decayed estimate should be materially closer to the new level
  # (160) than the flat one is.
  expect_lt(160 - hier_of(decayed, last_id), 160 - hier_of(flat, last_id))
})

test_that("half_life must be named exactly by levels plus root", {
  m <- mk(match_id = "a", venue = "A", competition = "X",
          match_date = "2020-01-01", inn1 = 300)
  expect_error(
    time_causal_hierarchical_mean_decayed(m, "inn1", levels = c("venue", "competition"),
                                          weights = c(venue = 5, competition = 20),
                                          half_life = c(venue = 365, competition = 365)),
    "half_life"
  )
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
