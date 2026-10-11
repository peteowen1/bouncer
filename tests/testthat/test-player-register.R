test_that(".parse_cricinfo_dob reads cricinfo's dict strings", {
  x <- c("{'date': 27, 'month': 3, 'year': 2011}",
         "{'date': 5, 'month': 9, 'year': 2000}")
  expect_equal(.parse_cricinfo_dob(x), as.Date(c("2011-03-27", "2000-09-05")))
})

test_that(".parse_cricinfo_dob treats a partial date as unknown, never a guess", {
  x <- c("{'date': None, 'month': 3, 'year': 1990}",
         "{'date': NULL, 'month': 3, 'year': NULL}",
         "{'date': 1, 'month': 2, 'year': 1985}")
  expect_equal(.parse_cricinfo_dob(x), as.Date(c(NA, NA, "1985-02-01")))
})

test_that(".parse_cricinfo_dob passes NA and empty strings through as NA", {
  expect_equal(.parse_cricinfo_dob(c(NA, "", "{'date': 2, 'month': 1, 'year': 1970}")),
               as.Date(c(NA, NA, "1970-01-02")))
  expect_equal(.parse_cricinfo_dob(c(NA_character_, "")), as.Date(c(NA, NA)))
})
