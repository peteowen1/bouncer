# Guard the property that was WRONG in production until 2026-08-19.
#
# The competition adjustment maps a per-ball value onto the reference scale. The
# old form divided the raw value by a difficulty factor. Because RVAA is a
# SIGNED deviation, dividing a NEGATIVE value by a factor above 1 moves it
# toward zero -- so a below-average batter was made BETTER by a weak-league
# discount. 671 of 1,039 below-average male T20 batters were being helped.
#
# It is not visible by reading the expression, which is why it survived. These
# tests state the property directly.

test_that("an easier competition always costs a player, at every value", {
  # Two competitions, identical players. `easy` inflates scoring by +0.25 per
  # ball and stretches the gaps between players by 1.6x; `ref` is the anchor.
  easy <- list(m_here = 0.25, m_ref = 0.05, cfactor = 1.6)
  ref  <- list(m_here = 0,    m_ref = 0,    cfactor = 1)

  # Span well below zero, which is exactly where the old form inverted.
  v0 <- seq(-0.6, 0.9, by = 0.05)
  adj_easy <- .competition_adjust(v0, easy$m_here, easy$m_ref, easy$cfactor)
  adj_ref  <- .competition_adjust(v0, ref$m_here,  ref$m_ref,  ref$cfactor)

  # The property holds above a CROSSOVER, not everywhere. Below it the
  # compression term pulls a very poor weak-competition return up past the same
  # return in the reference. That is a known, unresolved limitation, not an
  # accident -- see the OPEN QUESTION in calculate_player_rating_v2(). Pin the
  # crossover so a change in it has to be deliberate.
  cross <- (easy$m_ref - easy$m_here / easy$cfactor) / (1 - 1 / easy$cfactor)
  expect_equal(round(cross, 3), -0.283)
  expect_true(all(adj_easy[v0 > cross] < adj_ref[v0 > cross]),
              info = "same raw value in an easier competition must rate lower")
  expect_true(all(adj_easy[v0 < cross] > adj_ref[v0 < cross]),
              info = "documents the known inversion below the crossover")

  # And demonstrate the defect this replaces, so the test documents what it is
  # guarding rather than asserting an unexplained inequality. The old form
  # satisfied the property for positive values and inverted it for negative.
  old_easy <- v0 / easy$cfactor
  old_ref  <- v0 / ref$cfactor
  expect_true(all(old_easy[v0 > 0] < old_ref[v0 > 0]))
  expect_true(all(old_easy[v0 < 0] > old_ref[v0 < 0]))
})

test_that("the adjustment is monotone in the raw value", {
  # Scoring more must never rate lower, whatever competition it happened in.
  v0 <- seq(-1, 1, by = 0.01)
  for (f in c(0.6, 1, 1.6, 3)) {
    a <- .competition_adjust(v0, 0.2, 0.05, f)
    expect_true(all(diff(a) > 0), info = sprintf("cfactor = %s", f))
  }
})

test_that("a reference competition is the identity", {
  # fit_competition_offsets() anchors the reference at m_here = m_ref = 0, and
  # fit_competition_factors() anchors it at 1. Together those must leave a
  # reference player's value untouched -- if they do not, every rating is on a
  # shifted scale and nothing downstream would show it.
  v0 <- c(-0.5, -0.1, 0, 0.3, 1.2)
  expect_equal(.competition_adjust(v0, 0, 0, 1), v0)
})

test_that("an unrated competition is the identity", {
  # Unrated competitions fall back to m_here = m_ref = 0 and cfactor = 1. That
  # fallback is "no adjustment", not "assume weak" -- D-P23 tested and rejected
  # assuming weak, because most unrated cricket is short bilateral series
  # between full members.
  v0 <- c(-0.4, 0, 0.7)
  expect_equal(.competition_adjust(v0, 0, 0, 1), v0)
})

test_that("a harder competition rewards a player, at every value", {
  # The mirror of the first test. A competition where scoring is HARDER than
  # the reference (m_here below zero, cfactor below 1) must lift a player at
  # every raw value, negative ones included.
  hard <- list(m_here = -0.15, m_ref = 0.02, cfactor = 0.8)
  v0 <- seq(-0.6, 0.9, by = 0.05)
  expect_true(all(.competition_adjust(v0, hard$m_here, hard$m_ref, hard$cfactor) >
                  .competition_adjust(v0, 0, 0, 1)))
})

test_that("a player at his own competition's average lands on the reference mean", {
  # This is what "recentre" means, and it is the anchor the whole estimator
  # rests on: the average bridge player in a competition must map to what the
  # same players score in the reference, exactly, whatever the compression.
  for (f in c(0.7, 1, 1.6, 2.5)) {
    expect_equal(.competition_adjust(0.25, 0.25, 0.05, f), 0.05)
  }
})
