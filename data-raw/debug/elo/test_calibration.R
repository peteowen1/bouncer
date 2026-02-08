library(DBI)
library(duckdb)

conn <- dbConnect(duckdb(), '../../../bouncerdata/bouncer.duckdb', read_only = TRUE)

# Test direct query
cat('Direct query for t20:\n')
result <- dbGetQuery(conn, "
  SELECT metric_type, metric_key, metric_value, sample_size
  FROM elo_calibration_metrics
  WHERE format = 't20'
")
print(result)

# Test parameterized query
cat('\nParameterized query for t20:\n')
result2 <- dbGetQuery(conn, "
  SELECT metric_type, metric_key, metric_value, sample_size
  FROM elo_calibration_metrics
  WHERE format = ?
", params = list("t20"))
print(result2)

dbDisconnect(conn, shutdown = TRUE)
