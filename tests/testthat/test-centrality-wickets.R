# Run outs were credited to whoever was bowling.
#
# Measured on the real corpus: 10,113 of 132,814 T20 male dismissals (7.6%) are
# not the bowler's — 9,845 run outs, plus 179 retired hurt, which is not a
# dismissal at all. Same bug class as bouncerverse#31, which inflated T20
# wickets 9.7%, understated bowling averages by 1.94 runs, and REORDERED
# bowlers rather than merely rescaling them (bouncerverse#44).
#
# Scope worth recording: this feeds `wicket_matrix`, which reaches
# `compute_cricket_pagerank()` and nothing else. The shipped rating runs
# `calculate_network_centrality()`, which takes only delivery counts — so the
# defect was inert for published output. Fixed because it is wrong and because
# an inert landmine is still a landmine.

deliv <- function(kinds, batters = NULL, bowlers = NULL) {
  n <- length(kinds)
  data.table::data.table(
    batter_id = batters %||% rep("bat1", n),
    bowler_id = bowlers %||% rep("bowl1", n),
    runs_batter = 0,
    wicket_kind = kinds,
    match_type = "T20")
}

wickets_for <- function(d, min_deliveries = 1) {
  m <- suppressMessages(build_matchup_matrices(d, format = "all",
                                               min_deliveries = min_deliveries))
  # wicket_matrix holds the PROPORTION; recover the count.
  as.numeric(m$wicket_matrix["bat1", "bowl1"] * m$matchup_matrix["bat1", "bowl1"])
}

test_that("a run out is not the bowler's wicket", {
  expect_equal(wickets_for(deliv(c("caught", "run out", "bowled", NA, NA))), 2)
})

test_that("every kind the bowler earns is counted", {
  expect_equal(wickets_for(deliv(BOWLER_WICKET_KINDS)), length(BOWLER_WICKET_KINDS))
})

test_that("retirements and obstruction are not the bowler's either", {
  # retired hurt is not a dismissal at all, and was being counted as one.
  not_his <- c("run out", "retired hurt", "retired out", "obstructing the field",
               "handled the ball", "hit the ball twice")
  expect_equal(wickets_for(deliv(not_his)), 0)
})

test_that("kind matching survives case and stray whitespace", {
  expect_equal(wickets_for(deliv(c("Caught", " bowled ", "LBW"))), 3)
})

test_that("the SQL clause and the R vector are the same list", {
  # They were written out separately in five files, which is the shape that let
  # the rating tables' schema drift in #45.
  from_sql <- strsplit(gsub("'", "", bowler_wicket_kinds_sql()), ",")[[1]]
  expect_identical(from_sql, BOWLER_WICKET_KINDS)
})

test_that("falling back to player_out_id warns rather than silently overcounting", {
  d <- data.table::data.table(
    batter_id = "bat1", bowler_id = "bowl1", runs_batter = 0,
    player_out_id = c("x", "", "y"), match_type = "T20")
  expect_warning(
    suppressMessages(build_matchup_matrices(d, format = "all", min_deliveries = 1)),
    "run outs")
})

test_that("an explicit is_wicket_delivery is trusted, since the caller filtered it", {
  d <- data.table::data.table(
    batter_id = "bat1", bowler_id = "bowl1", runs_batter = 0,
    is_wicket_delivery = c(1, 0, 1), match_type = "T20")
  m <- suppressMessages(build_matchup_matrices(d, format = "all", min_deliveries = 1))
  expect_equal(as.numeric(m$wicket_matrix["bat1", "bowl1"] *
                            m$matchup_matrix["bat1", "bowl1"]), 2)
})
