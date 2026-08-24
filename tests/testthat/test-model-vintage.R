# The guard that stops a pre-leak-fix outcome model loading silently.
#
# Why this test exists: for five months the `bouncermodels` release served a
# 2026-03-27 vintage, and both `load_agnostic_model()` and `load_full_model()`
# prefer the release over local disk. Any machine with `bouncermodels` installed
# therefore got the LEAKED baseline in preference to the corrected model sitting
# on disk, and nothing said so. It never bit only because `bouncermodels` was
# not installed on the one machine doing the work (bouncerverse#50).
#
# The property under test is deliberately blunt: unstamped is refused, because
# every artefact built before the stamp existed also predates the fix.

tiny_booster <- function() {
  x <- matrix(stats::runif(40), ncol = 2)
  y <- as.numeric(x[, 1] > 0.5)
  xgboost::xgb.train(
    params = list(objective = "binary:logistic", nthread = 1),
    data = xgboost::xgb.DMatrix(x, label = y),
    nrounds = 1L,
    verbose = 0
  )
}

test_that("an unstamped model is refused", {
  skip_if_not_installed("xgboost")
  m <- tiny_booster()
  expect_error(
    .check_model_vintage(m, "agnostic_outcome_t20", "bouncermodels"),
    "carries no build date"
  )
})

test_that("a model built before the leak fix is refused, and the date is reported", {
  skip_if_not_installed("xgboost")
  m <- tiny_booster()
  xgboost::xgb.attr(m, "bouncer_build_date") <- "2026-03-27"
  expect_error(
    .check_model_vintage(m, "agnostic_outcome_t20", "bouncermodels"),
    "2026-03-27"
  )
})

test_that("a model built on the leak-fix date passes", {
  skip_if_not_installed("xgboost")
  m <- tiny_booster()
  xgboost::xgb.attr(m, "bouncer_build_date") <- MODEL_LEAK_FIX_DATE
  expect_silent(.check_model_vintage(m, "agnostic_outcome_t20", "local disk"))
})

test_that("the boundary is inclusive: the day before fails, the day itself does not", {
  skip_if_not_installed("xgboost")
  day_before <- as.character(as.Date(MODEL_LEAK_FIX_DATE) - 1L)
  m <- tiny_booster()
  xgboost::xgb.attr(m, "bouncer_build_date") <- day_before
  expect_error(.check_model_vintage(m, "m", "local disk"), day_before)

  xgboost::xgb.attr(m, "bouncer_build_date") <- MODEL_LEAK_FIX_DATE
  expect_silent(.check_model_vintage(m, "m", "local disk"))
})

test_that("the shipped agnostic models are stamped and pass the guard", {
  skip_if_not_installed("xgboost")
  md <- tryCatch(get_models_dir(create = FALSE), error = function(e) NULL)
  skip_if(is.null(md) || !dir.exists(md), "no local models directory")

  for (fmt in c("t20", "odi", "test")) {
    f <- file.path(md, get_model_filename("agnostic", fmt))
    skip_if(!file.exists(f), paste("no local agnostic model for", fmt))
    m <- xgboost::xgb.load(f)
    stamp <- xgboost::xgb.attr(m, "bouncer_build_date")
    expect_true(!is.null(stamp) && nzchar(stamp),
                info = paste(fmt, "agnostic model is unstamped"))
    expect_gte(as.Date(stamp), as.Date(MODEL_LEAK_FIX_DATE))
  }
})
