# Score every Test+MDM batter-faced delivery with lambda = 33 and write
# main.cricsheet_ball_raa. Per-format replacement, so T20/ODI rows must be
# untouched -- asserted before and after rather than assumed.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})

DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)   # never the empty stub (#46)

conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

cat("lambda for test:", get_raa_lambda("test"), "\n\n")

has_tbl <- nrow(DBI::dbGetQuery(conn, "
  SELECT 1 FROM information_schema.tables
  WHERE table_schema='main' AND table_name='cricsheet_ball_raa'")) > 0

before <- if (has_tbl) DBI::dbGetQuery(conn, "
  SELECT format, gender, COUNT(*) AS n, ROUND(AVG(raa),5) AS mean_raa
  FROM main.cricsheet_ball_raa GROUP BY 1,2 ORDER BY 1,2") else data.frame()
cat("=== BEFORE ===\n"); print(before)

t0 <- Sys.time()
cat("\nscoring Test... (5.4M deliveries; this is the long step)\n")
invisible(build_cricsheet_raa("test", conn = conn))
cat(sprintf("\nelapsed: %.1f min\n", as.numeric(difftime(Sys.time(), t0, units = "mins"))))

after <- DBI::dbGetQuery(conn, "
  SELECT format, gender, COUNT(*) AS n, ROUND(AVG(raa),5) AS mean_raa,
         ROUND(STDDEV(raa),3) AS sd_raa,
         MIN(match_date) AS from_date, MAX(match_date) AS to_date
  FROM main.cricsheet_ball_raa GROUP BY 1,2 ORDER BY 1,2")
cat("\n=== AFTER ===\n"); print(after)

cat("\n=== per-format replacement check: did T20/ODI move? ===\n")
if (nrow(before)) {
  for (i in seq_len(nrow(before))) {
    b <- before[i, ]
    a <- after[after$format == b$format & after$gender == b$gender, ]
    if (b$format == "TEST") next
    same <- nrow(a) == 1 && a$n == b$n && isTRUE(all.equal(a$mean_raa, b$mean_raa))
    cat(sprintf("  %-5s %-7s %s  (%s -> %s rows)\n", b$format, b$gender,
                if (same) "UNCHANGED" else "*** MOVED ***",
                format(b$n, big.mark = ","),
                if (nrow(a)) format(a$n, big.mark = ",") else "absent"))
  }
} else cat("  (table did not exist before; nothing to preserve)\n")

cat("\n=== sanity on the new TEST rows ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT format, COUNT(*) AS balls,
         COUNT(DISTINCT match_id) AS matches,
         COUNT(DISTINCT batter_id) AS batters,
         COUNT(DISTINCT bowler_id) AS bowlers,
         ROUND(AVG(raa),5) AS mean_raa,
         ROUND(AVG(actual_runs),4) AS mean_runs,
         SUM(CASE WHEN raa IS NULL THEN 1 ELSE 0 END) AS null_raa
  FROM main.cricsheet_ball_raa WHERE format='TEST' GROUP BY 1"))

cat("\n  mean_raa should be ~0 by construction (it is runs above the model's own\n")
cat("  expectation over the same population it was trained on).\n")
