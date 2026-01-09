# Tests for Skill Utility Functions

test_that("normalize_format handles T20 variants", {
  expect_equal(normalize_format("t20"), "t20")
  expect_equal(normalize_format("T20"), "t20")
  expect_equal(normalize_format("it20"), "t20")
  expect_equal(normalize_format("IT20"), "t20")
})

test_that("normalize_format handles ODI variants", {
  expect_equal(normalize_format("odi"), "odi")
  expect_equal(normalize_format("ODI"), "odi")
  expect_equal(normalize_format("odm"), "odi")
  expect_equal(normalize_format("ODM"), "odi")
})

test_that("normalize_format handles Test variants", {
  expect_equal(normalize_format("test"), "test")
  expect_equal(normalize_format("Test"), "test")
  expect_equal(normalize_format("TEST"), "test")
  expect_equal(normalize_format("mdm"), "test")
  expect_equal(normalize_format("MDM"), "test")
})

test_that("normalize_format defaults to t20 for unknown formats", {
  expect_equal(normalize_format("unknown"), "t20")
  expect_equal(normalize_format(""), "t20")
})

test_that("get_skill_table_name generates correct table names", {
  expect_equal(get_skill_table_name("t20", "player_skill"), "t20_player_skill")
  expect_equal(get_skill_table_name("odi", "team_skill"), "odi_team_skill")
  expect_equal(get_skill_table_name("test", "venue_skill"), "test_venue_skill")

  # Should normalize format

  expect_equal(get_skill_table_name("IT20", "player_skill"), "t20_player_skill")
  expect_equal(get_skill_table_name("ODM", "team_skill"), "odi_team_skill")
  expect_equal(get_skill_table_name("MDM", "venue_skill"), "test_venue_skill")
})

test_that("escape_sql_strings escapes single quotes", {
  expect_equal(escape_sql_strings("test"), "test")
  expect_equal(escape_sql_strings("it's"), "it''s")
  expect_equal(escape_sql_strings("Durban's Super Giants"), "Durban''s Super Giants")

  # Multiple quotes
  expect_equal(escape_sql_strings("'quoted'"), "''quoted''")
})

test_that("escape_sql_strings handles vectors", {
  input <- c("plain", "it's", "no'quote")
  expected <- c("plain", "it''s", "no''quote")
  expect_equal(escape_sql_strings(input), expected)
})

test_that("batch_skill_query processes small batches directly", {
  # Mock query function that returns a data frame
  mock_query <- function(ids) {
    data.frame(id = ids, value = seq_along(ids))
  }

  # Small batch should call query function once
  result <- batch_skill_query(c("a", "b", "c"), mock_query, batch_size = 10, verbose = FALSE)
  expect_equal(nrow(result), 3)
  expect_equal(result$id, c("a", "b", "c"))
})

test_that("batch_skill_query handles large batches", {
  # Mock query function
  mock_query <- function(ids) {
    data.frame(id = ids, value = seq_along(ids))
  }

  # Large batch with small batch_size should process in chunks
  ids <- paste0("id_", 1:25)
  result <- batch_skill_query(ids, mock_query, batch_size = 10, verbose = FALSE)

  expect_equal(nrow(result), 25)
  expect_equal(result$id, ids)
})
