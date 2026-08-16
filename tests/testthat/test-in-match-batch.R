# Parity between predict_win_probability() and predict_win_probability_batch().
#
# The batch path exists purely for speed -- 940,985 cricinfo deliveries at the
# scalar path's ~32 ms/ball is 8.3 hours. Speed is only worth having if the
# answer is identical, and the original serving bug WAS a silent feature
# mismatch, so "identical" is asserted rather than assumed. If someone adds a
# feature to one path and not the other, this file is what catches it.

skip_if_no_models <- function(format) {
  m <- tryCatch(load_in_match_models(format), error = function(e) NULL)
  skip_if(is.null(m) || is.null(m$stage1_model), "in-match models unavailable")
  m
}

# A spread of states rather than a couple of convenient ones: early and late,
# wickets in hand and none, chases that are won, lost, and impossible.
batch_test_states <- function() {
  data.frame(
    current_score = c(  5,  45,  85, 140,  10,  60, 120, 175, 199,   0),
    wickets       = c(  0,   2,   3,   6,   1,   4,   7,   9,  10,   0),
    overs         = c(1.0, 6.3, 10.0, 18.4, 2.2, 9.5, 15.1, 19.3, 19.5, 0.0),
    innings       = c(  1,   1,   1,   1,   2,   2,    2,    2,    2,   2),
    target        = c( NA,  NA,   NA,   NA, 180, 180,  180,  180,  180, 150)
  )
}

test_that("batch scoring matches the scalar path exactly", {
  models <- skip_if_no_models("t20")
  withr::local_options(bouncer.warn_momentum_impute = FALSE)

  st <- batch_test_states()
  batched <- predict_win_probability_batch(st, format = "t20", models = models)

  scalar <- vapply(seq_len(nrow(st)), function(i) {
    predict_win_probability(
      current_score = st$current_score[i],
      wickets       = st$wickets[i],
      overs         = st$overs[i],
      innings       = st$innings[i],
      target        = if (st$innings[i] == 2) st$target[i] else NULL,
      format        = "t20",
      models        = models
    )$win_prob
  }, numeric(1))

  expect_equal(batched, scalar, tolerance = 1e-8)
})

test_that("batch scoring matches the scalar path when real momentum is supplied", {
  models <- skip_if_no_models("t20")
  withr::local_options(bouncer.warn_momentum_impute = FALSE)

  st <- batch_test_states()
  # Values that are deliberately NOT what the run-rate imputation would produce,
  # so a path that ignored the supplied columns would disagree.
  mom <- data.frame(
    runs_last_12_balls = 14, runs_last_24_balls = 26,
    dots_last_12_balls = 3,  dots_last_24_balls = 7,
    boundaries_last_12_balls = 2, boundaries_last_24_balls = 3,
    wickets_last_12_balls = 1, wickets_last_24_balls = 1,
    runs_last_3_overs = 21, runs_last_6_overs = 44,
    wickets_last_3_overs = 1, wickets_last_6_overs = 2,
    rr_last_3_overs = 7.0, rr_last_6_overs = 7.3
  )
  st <- cbind(st, mom[rep(1L, nrow(st)), ])

  batched <- predict_win_probability_batch(st, format = "t20", models = models)

  mom_cols <- names(mom)
  scalar <- vapply(seq_len(nrow(st)), function(i) {
    predict_win_probability(
      current_score = st$current_score[i],
      wickets       = st$wickets[i],
      overs         = st$overs[i],
      innings       = st$innings[i],
      target        = if (st$innings[i] == 2) st$target[i] else NULL,
      format        = "t20",
      models        = models,
      recent_balls  = as.list(st[i, mom_cols])
    )$win_prob
  }, numeric(1))

  expect_equal(batched, scalar, tolerance = 1e-8)
})

test_that("supplied momentum actually changes the answer", {
  models <- skip_if_no_models("t20")
  withr::local_options(bouncer.warn_momentum_impute = FALSE)

  st <- batch_test_states()
  imputed <- predict_win_probability_batch(st, format = "t20", models = models)

  st2 <- st
  st2$runs_last_12_balls <- 0
  st2$runs_last_24_balls <- 0
  st2$wickets_last_12_balls <- 3
  st2$wickets_last_24_balls <- 4
  st2$rr_last_3_overs <- 0
  st2$rr_last_6_overs <- 0
  collapsed <- predict_win_probability_batch(st2, format = "t20", models = models)

  # A collapse (no runs, three wickets) must move the number somewhere. If these
  # are equal, the momentum columns are being dropped on the floor -- the exact
  # failure mode the zero-fill bug had.
  expect_false(isTRUE(all.equal(imputed, collapsed)))
})

test_that("batch scoring is materially faster than the scalar path", {
  models <- skip_if_no_models("t20")
  withr::local_options(bouncer.warn_momentum_impute = FALSE)
  skip_on_cran()

  st <- batch_test_states()[rep(1:10, 20), ]  # 200 states

  t_batch <- system.time(
    predict_win_probability_batch(st, format = "t20", models = models)
  )[["elapsed"]]

  t_scalar <- system.time(
    for (i in seq_len(50)) {  # only 50 rows; the scalar path is the slow one
      predict_win_probability(
        current_score = st$current_score[i], wickets = st$wickets[i],
        overs = st$overs[i], innings = st$innings[i],
        target = if (st$innings[i] == 2) st$target[i] else NULL,
        format = "t20", models = models
      )
    }
  )[["elapsed"]]

  # 200 batched rows should beat 50 scalar rows outright. Deliberately loose --
  # this is a guard against the batch path silently degenerating into a loop,
  # not a benchmark.
  expect_lt(t_batch, t_scalar)
})

test_that("input contract is enforced rather than silently filled", {
  withr::local_options(bouncer.warn_momentum_impute = FALSE)

  expect_error(
    predict_win_probability_batch(
      data.frame(current_score = 100, wickets = 3, overs = 10, innings = 3),
      format = "t20", models = list(stage1_model = TRUE)
    ),
    "innings"
  )

  expect_error(
    predict_win_probability_batch(
      data.frame(current_score = 100, wickets = 3, innings = 1),
      format = "t20", models = list(stage1_model = TRUE)
    ),
    "missing required column"
  )

  expect_error(
    predict_win_probability_batch(
      data.frame(current_score = 100, wickets = 3, overs = 10, innings = 2),
      format = "t20", models = list(stage1_model = TRUE)
    ),
    "target"
  )

  expect_error(
    predict_win_probability_batch(data.frame(), format = "test"),
    "does not handle"
  )
})

test_that("empty input returns an empty vector, not an error", {
  expect_identical(
    predict_win_probability_batch(
      data.frame(current_score = numeric(0), wickets = numeric(0),
                 overs = numeric(0), innings = integer(0)),
      format = "t20"
    ),
    numeric(0)
  )
})
