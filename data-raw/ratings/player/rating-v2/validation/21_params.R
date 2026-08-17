# Measure, per bucket, the things I asserted in a table without checking:
# competition units rated, unrated delivery share, and the derived shrinkage prior.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
id_map <- build_player_id_map(conn)

buckets <- list(c("t20","male"), c("odi","male"), c("test","male"),
                c("t20","female"), c("odi","female"))

res <- data.table()
for (bk in buckets) {
  f <- bk[1]; g <- bk[2]; tag <- paste(f, g)
  cat("\n===", toupper(tag), "===\n")

  fac <- tryCatch(suppressMessages(fit_competition_factors(conn, f, g, id_map = id_map)),
                  error = function(e) NULL)
  if (is.null(fac)) { cat("  competition fit FAILED\n"); next }

  b <- as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT COALESCE(%s,'unknown') AS comp, COUNT(*) AS balls
    FROM main.cricsheet_ball_raa r
    JOIN cricsheet.matches m ON m.match_id = r.match_id
    WHERE r.format='%s' AND r.gender='%s' GROUP BY 1",
    .competition_sql(f), toupper(f), g)))
  fmap <- setNames(fac$factor, fac$comp)
  b[, rated := comp %in% names(fmap)]
  unrated_pct <- 100 * b[rated == FALSE, sum(balls)] / b[, sum(balls)]

  cat(sprintf("  competitions in data: %d | rated: %d (direct %d, chained %d)\n",
              nrow(b), nrow(fac), sum(fac$step == 0, na.rm = TRUE),
              sum(fac$step > 0, na.rm = TRUE)))
  cat(sprintf("  unrated deliveries: %.2f%%\n", unrated_pct))
  cat(sprintf("  factor range %.2f - %.2f, median %.2f\n",
              min(fac$factor), max(fac$factor), median(fac$factor)))

  res <- rbind(res, data.table(bucket = tag, comps_in_data = nrow(b),
                               comps_rated = nrow(fac),
                               direct = sum(fac$step == 0, na.rm = TRUE),
                               chained = sum(fac$step > 0, na.rm = TRUE),
                               unrated_pct = round(unrated_pct, 2),
                               factor_median = round(median(fac$factor), 3),
                               factor_max = round(max(fac$factor), 2)))
}

cat("\n\n=== SUMMARY ===\n")
print(res)
