library(DBI)
library(duckdb)

conn <- dbConnect(duckdb(), '../bouncerdata/bouncer.duckdb', read_only = TRUE)
cat('Rows in mens_t20_3way_elo:', dbGetQuery(conn, 'SELECT COUNT(*) FROM mens_t20_3way_elo')[[1]], '\n')
dbDisconnect(conn, shutdown = TRUE)
