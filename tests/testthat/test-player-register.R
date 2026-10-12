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

test_that(".parse_cricinfo_dob loses only the malformed entry, not the batch", {
  x <- c("{'date': 1, 'month': 2, 'year': 1985}", "{'date': True, 'month': 'x", "{'date': 3, 'month': 4, 'year': 1990}")
  expect_equal(.parse_cricinfo_dob(x), as.Date(c("1985-02-01", NA, "1990-04-03")))
})

test_that(".parse_cricinfo_dob: an impossible date first in the vector loses only itself", {
  x <- c("{'date': 30, 'month': 2, 'year': 1985}", "{'date': 3, 'month': 4, 'year': 1990}")
  expect_equal(.parse_cricinfo_dob(x), as.Date(c(NA, "1990-04-03")))
  expect_equal(.parse_cricinfo_dob(rev(x)), as.Date(c("1990-04-03", NA)))
  expect_equal(.parse_cricinfo_dob(c("{'date': 3, 'month': 13, 'year': 1990}",
                                     "{'date': 3, 'month': 4, 'year': 1990}")),
               as.Date(c(NA, "1990-04-03")))
})

test_that("build_player_dob: Wikidata fills only gaps, cricinfo wins, placeholders and conflicts drop", {
  skip_if_not_installed("duckdb")
  conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))
  DBI::dbExecute(conn, "CREATE SCHEMA cricsheet; CREATE SCHEMA cricinfo")
  n <- 3000L
  # p1..p3000 dated by cricinfo; w1..w4 only reachable through Wikidata
  reg <- data.frame(identifier = c(paste0("p", 1:n), paste0("w", 1:4)),
                    key_cricinfo = c(as.character(1:n), as.character(90001:90004)),
                    key_cricinfo_2 = NA_character_, key_cricinfo_3 = NA_character_)
  DBI::dbWriteTable(conn, DBI::Id(schema = "cricsheet", table = "people_register"), reg)
  DBI::dbWriteTable(conn, DBI::Id(schema = "cricinfo", table = "innings"),
                    data.frame(player_id = 1:n,
                               player_dob = "{'date': 1, 'month': 2, 'year': 1990}"))
  wd <- data.frame(key = c("1", "90001", "90002", "90003", "90003"),
                   dob = c("1955-05-05", "1980-03-04", "1970-01-01", "1981-01-01", "1982-01-01"))
  wd_path <- tempfile(fileext = ".csv")
  data.table::fwrite(wd, wd_path)

  out <- suppressMessages(build_player_dob(conn, wikidata_path = wd_path))
  data.table::setDT(out)
  # cricinfo's date stands for p1 even though Wikidata disagrees
  expect_equal(out[player_id == "p1", dob], as.Date("1990-02-01"))
  expect_equal(out[player_id == "p1", dob_source], "cricinfo")
  expect_equal(out[player_id == "w1", .(dob, dob_source)],
               data.table::data.table(dob = as.Date("1980-03-04"), dob_source = "wikidata"))
  # the 1970-01-01 placeholder and the two-date id are dropped, w4 never had one
  expect_false(any(c("w2", "w3", "w4") %in% out$player_id))
  expect_equal(nrow(out), n + 1L)
})
