# Tests for the dated prediction history (issue #88): R/prediction_history.R

fake_predictions <- function() {
  data.frame(
    match_id = c("64012", "64013"),
    format = c("T20", "ODI"),
    team1 = c("India", "Australia"),
    team2 = c("England", "New Zealand"),
    team1_win_prob = c(0.55, 0.40),
    draw_prob = c(0, 0.10),
    team2_win_prob = c(0.45, 0.50),
    model_version = c("v1.0", "v1.0"),
    stringsAsFactors = FALSE
  )
}

test_that("prediction_history_filename uses the UTC time to the second", {
  t <- as.POSIXct("2026-11-05 07:08:09", tz = "UTC")
  expect_equal(prediction_history_filename(t), "predictions_20261105T070809Z.parquet")
})

test_that("prediction_history_filename converts a non-UTC time to UTC", {
  # 19:08:09 on 5 Nov in Auckland (NZDT, UTC+13) is 06:08:09 UTC
  t <- as.POSIXct("2026-11-05 19:08:09", tz = "Pacific/Auckland")
  expect_equal(prediction_history_filename(t), "predictions_20261105T060809Z.parquet")
})

test_that("prediction_history_filename rejects a non-time argument", {
  expect_error(prediction_history_filename("2026-11-05"), "POSIXct")
})

test_that("build_prediction_history writes three outcome rows per fixture", {
  t <- as.POSIXct("2026-11-05 07:08:09", tz = "UTC")
  h <- build_prediction_history(fake_predictions(), t)

  expect_equal(nrow(h), 6)
  required <- c("verse", "event", "market", "selection", "probability",
                "published_utc", "model_version")
  expect_true(all(required %in% names(h)))

  expect_equal(unique(h$verse), "bouncer")
  expect_equal(unique(h$market), "match_winner")
  expect_equal(unique(h$model_version), "v1.0")
  expect_equal(h$event, c("64012", "64013", "64012", "64013", "64012", "64013"))
  expect_equal(h$selection,
               c("India", "Australia", "Draw", "Draw", "England", "New Zealand"))
  expect_equal(h$probability, c(0.55, 0.40, 0, 0.10, 0.45, 0.50))
  expect_equal(as.numeric(unique(h$published_utc)), as.numeric(t))
})

test_that("each fixture's outcome probabilities sum to one", {
  t <- as.POSIXct("2026-11-05 07:08:09", tz = "UTC")
  h <- build_prediction_history(fake_predictions(), t)
  per_fixture <- tapply(h$probability, h$event, sum)
  expect_equal(as.numeric(per_fixture), c(1, 1))
})

test_that("build_prediction_history refuses a table missing a required column", {
  pred <- fake_predictions()
  pred$draw_prob <- NULL
  expect_error(
    build_prediction_history(pred, as.POSIXct("2026-11-05 07:08:09", tz = "UTC")),
    "draw_prob"
  )
})

test_that("build_prediction_history refuses probabilities outside [0, 1] or missing", {
  t <- as.POSIXct("2026-11-05 07:08:09", tz = "UTC")
  pred <- fake_predictions()
  pred$team1_win_prob[1] <- NA
  expect_error(build_prediction_history(pred, t), "must be finite")

  pred <- fake_predictions()
  pred$team2_win_prob[2] <- 1.5
  expect_error(build_prediction_history(pred, t), "must be finite")
})

test_that("write_prediction_history writes a new file and never overwrites it", {
  skip_if_not_installed("arrow")
  out <- tempfile("prediction_history_")
  t <- as.POSIXct("2026-11-05 07:08:09", tz = "UTC")
  h <- build_prediction_history(fake_predictions(), t)

  p <- write_prediction_history(h, out, t)
  expect_equal(basename(p), "predictions_20261105T070809Z.parquet")
  expect_true(file.exists(p))

  back <- as.data.frame(arrow::read_parquet(p))
  expect_equal(nrow(back), 6)
  expect_true(all(c("verse", "event", "market", "selection", "probability",
                    "published_utc", "model_version") %in% names(back)))
  expect_equal(back$selection, h$selection)
  expect_equal(back$probability, h$probability)

  # Same timestamp again: refused, and the original bytes are unchanged
  before <- unname(tools::md5sum(p))
  expect_error(write_prediction_history(h, out, t), "never overwritten")
  expect_equal(unname(tools::md5sum(p)), before)
})

test_that("a later publish adds a new file and keeps the earlier one", {
  skip_if_not_installed("arrow")
  out <- tempfile("prediction_history_")
  t1 <- as.POSIXct("2026-11-05 07:08:09", tz = "UTC")
  t2 <- t1 + 60
  write_prediction_history(build_prediction_history(fake_predictions(), t1), out, t1)
  write_prediction_history(build_prediction_history(fake_predictions(), t2), out, t2)

  expect_equal(sort(list.files(out)),
               c("predictions_20261105T070809Z.parquet",
                 "predictions_20261105T070909Z.parquet"))
})
