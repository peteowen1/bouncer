test_that(".match_type_filter_sql is a no-op for NULL -- every existing bucket's default", {
  # This is the invariant bouncerverse#40 item 1's new test_intl bucket
  # depends on: t20/odi/blended-test must be byte-identical to before the
  # match_type_filter parameter existed.
  expect_equal(.match_type_filter_sql(NULL), "")
})

test_that(".match_type_filter_sql builds a single-value clause, lowercased", {
  expect_equal(
    .match_type_filter_sql("test"),
    " AND LOWER(m.match_type) IN ('test')")
  # Case-insensitive input, since callers (BUCKETS entries) are free-typed.
  expect_equal(
    .match_type_filter_sql("Test"),
    " AND LOWER(m.match_type) IN ('test')")
})

test_that(".match_type_filter_sql builds a multi-value clause", {
  expect_equal(
    .match_type_filter_sql(c("test", "mdm")),
    " AND LOWER(m.match_type) IN ('test', 'mdm')")
})
