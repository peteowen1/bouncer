# The "daily" release family was retired (decision D-P1, 2026-10-15): bouncerdata
# publishes no v<date> releases, so every daily path aborted "No daily releases
# found". The two daily-only entry points are defunct and get_latest_release()
# no longer accepts type = "daily". None of these tests touch the network.

test_that("install_bouncerdata_from_release() is defunct", {
  expect_error(install_bouncerdata_from_release(), class = "defunctError")
  expect_error(install_bouncerdata_from_release(), "install_parquets_from_release")
  # Old arguments still reach the defunct message rather than 'unused argument'
  expect_error(
    install_bouncerdata_from_release(formats = "short_form", genders = "male",
                                     tag = "latest", build_db = FALSE),
    class = "defunctError"
  )
})

test_that("update_bouncerdata() is defunct", {
  expect_error(update_bouncerdata(), class = "defunctError")
  expect_error(update_bouncerdata(), "install_all_bouncer_data")
  expect_error(update_bouncerdata(repo = "peteowen1/bouncerdata", data_dir = tempdir()),
               class = "defunctError")
})

test_that("get_latest_release() rejects type = 'daily' before any network call", {
  expect_error(get_latest_release(type = "daily"), "should be one of")
})

test_that("get_latest_release() rejects type = 'weekly' before any network call", {
  expect_error(get_latest_release(type = "weekly"), "should be one of")
})
