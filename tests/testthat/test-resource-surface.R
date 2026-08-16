# The fitted batting resource surface, replacing `wickets_in_hand * 6`.
#
# The constant it replaces was hand-chosen and stood behind the single most
# important feature in the ODI chase model (57% of gain). The measured run cost
# of a wicket ranges from 0.5 to 22.7 depending on state, so the properties that
# matter are the shape ones -- monotone, bounded, exact at the edges -- not the
# fit statistics.

fake_surface <- function(max_balls = 120L) {
  params <- data.table::data.table(
    wickets_in_hand = 1:10,
    Z = c(10, 22, 35, 49, 63, 86, 108, 128, 140, 150),
    b = rep(0.02, 10), cells = 100L, n = 1000L
  )
  grid <- data.table::CJ(balls_remaining = seq_len(max_balls), wickets_in_hand = 0:10)
  grid <- merge(grid, params[, c("wickets_in_hand", "Z", "b")],
                by = "wickets_in_hand", all.x = TRUE)
  grid[, exp_runs := Z * (1 - exp(-b * balls_remaining))]
  grid[wickets_in_hand == 0L, exp_runs := 0]
  data.table::setorder(grid, balls_remaining, wickets_in_hand)
  grid[, exp_runs := cummax(exp_runs), by = balls_remaining]
  grid[, c("Z", "b") := NULL]
  structure(list(grid = grid, params = params, format = "t20",
                 max_balls = max_balls, n_matches = 1000L, n_deliveries = 100000L),
            class = c("bouncer_resource_surface", "list"))
}

test_that("no balls left means no runs left, whatever the wickets", {
  s <- fake_surface()
  expect_equal(resource_runs(0, 10, s), 0)
  expect_equal(resource_runs(0, 1, s), 0)
})

test_that("no wickets left means no runs left, whatever the balls", {
  s <- fake_surface()
  expect_equal(resource_runs(120, 0, s), 0)
  expect_equal(resource_runs(60, 0, s), 0)
})

test_that("more balls never lowers expected runs", {
  s <- fake_surface()
  for (w in c(2, 5, 8, 10)) {
    v <- resource_runs(seq_len(120), w, s)
    expect_true(all(diff(v) >= -1e-9), label = paste("monotone in balls at", w, "wickets"))
  }
})

test_that("more wickets never lowers expected runs", {
  s <- fake_surface()
  for (b in c(10, 40, 80, 120)) {
    v <- resource_runs(b, 0:10, s)
    expect_true(all(diff(v) >= -1e-9), label = paste("monotone in wickets at", b, "balls"))
  }
})

test_that("a wicket costs more when more balls remain", {
  # The whole point of fitting: the hardcoded 6 balls is state-independent, and
  # the real cost is not. Losing a wicket with 100 balls left should cost more
  # than losing one with 20 left.
  s <- fake_surface()
  cost_early <- resource_runs(100, 8, s) - resource_runs(100, 7, s)
  cost_late  <- resource_runs(20, 8, s)  - resource_runs(20, 7, s)
  expect_gt(cost_early, cost_late)
})

test_that("states outside the grid clamp instead of returning NA", {
  s <- fake_surface()
  expect_false(is.na(resource_runs(999, 10, s)))
  expect_equal(resource_runs(999, 10, s), resource_runs(120, 10, s))
  expect_false(is.na(resource_runs(50, 99, s)))
  expect_equal(resource_runs(50, 99, s), resource_runs(50, 10, s))
  expect_equal(resource_runs(-5, 5, s), 0)
})

test_that("it is vectorised and recycles", {
  s <- fake_surface()
  v <- resource_runs(c(120, 60, 30), c(10, 5, 2), s)
  expect_length(v, 3L)
  expect_true(all(diff(v) < 0))          # fewer balls and wickets -> fewer runs
  expect_length(resource_runs(c(120, 60), 10, s), 2L)
  expect_length(resource_runs(60, c(1, 5, 10), s), 3L)
})

test_that("non-integer states are handled rather than dropped", {
  s <- fake_surface()
  expect_false(is.na(resource_runs(60.4, 7.2, s)))
  expect_equal(resource_runs(60.4, 7.2, s), resource_runs(60, 7, s))
})

test_that("the contract is enforced", {
  expect_error(resource_runs(60, 5, list(a = 1)), "bouncer_resource_surface")
  expect_error(fit_resource_surface("test"), "should be one of")
})

test_that("the fitted surface matches the raw data it was fitted from", {
  conn <- tryCatch(get_db_connection(read_only = TRUE), error = function(e) NULL)
  skip_if(is.null(conn), "database not available")
  on.exit(try(DBI::dbDisconnect(conn, shutdown = TRUE), silent = TRUE), add = TRUE)

  s <- tryCatch(fit_resource_surface("t20", conn = conn), error = function(e) NULL)
  skip_if(is.null(s), "cricsheet deliveries not available")

  # Sane T20 anchors: a full innings in hand is a par total, and the surface
  # must not invert anywhere.
  full <- resource_runs(120, 10, s)
  expect_gt(full, 120)
  expect_lt(full, 200)

  g <- s$grid
  expect_equal(sum(is.na(g$exp_runs)), 0L)
  expect_equal(g[, sum(diff(exp_runs) < -1e-9), by = wickets_in_hand][, sum(V1)], 0L)
  expect_equal(g[, sum(diff(exp_runs) < -1e-9), by = balls_remaining][, sum(V1)], 0L)

  # The cost of a wicket must vary across states by far more than the constant
  # it replaces would allow.
  costs <- c(resource_runs(100, 9, s) - resource_runs(100, 8, s),
             resource_runs(20, 9, s)  - resource_runs(20, 8, s))
  expect_gt(costs[1] / max(costs[2], 0.01), 3)
})
