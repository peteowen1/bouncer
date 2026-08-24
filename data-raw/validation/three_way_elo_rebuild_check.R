# Verify a rebuilt 3-way ELO table before believing it (bouncerverse#63).
#
# Usage: Rscript data-raw/validation/three_way_elo_rebuild_check.R mens t20
#
# Three checks, cheapest first. Each is a thing that has actually gone wrong
# here, not a generic sanity pass.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})

a <- commandArgs(trailingOnly = TRUE)
gender <- if (length(a) >= 1) a[1] else "mens"
fmt    <- if (length(a) >= 2) a[2] else "t20"
tbl    <- paste0(gender, "_", fmt, "_3way_elo")
gdb    <- if (gender == "mens") "male" else "female"
types  <- switch(fmt, t20 = "'t20','it20'", odi = "'odi','odm'", test = "'test','mdm'")

conn <- get_db_connection(read_only = TRUE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)
stopifnot(table_exists(conn, tbl))

cli::cli_h1("{tbl}")

# 1. Coverage and recency. The whole point of the rebuild was that the table
#    was frozen at 2026-01-19 while the corpus ran to August.
q <- dbGetQuery(conn, sprintf("
  SELECT (SELECT COUNT(*) FROM main.%s) AS rated,
         (SELECT MAX(match_date) FROM main.%s) AS rated_last,
         (SELECT COUNT(*) FROM cricsheet.deliveries
           WHERE gender='%s' AND LOWER(match_type) IN (%s)) AS corpus,
         (SELECT MAX(match_date) FROM cricsheet.deliveries
           WHERE gender='%s' AND LOWER(match_type) IN (%s)) AS corpus_last",
  tbl, tbl, gdb, types, gdb, types))
cli::cli_alert_info("rated {format(q$rated, big.mark=',')} of {format(q$corpus, big.mark=',')} ({round(100*q$rated/q$corpus,1)}%)")
cli::cli_alert_info("rated to {q$rated_last}; corpus to {q$corpus_last}")
# The final line claims the table "passes coverage". It has to actually check.
cov <- q$rated / q$corpus
if (cov < 0.98) {
  cli::cli_abort(c("Coverage is {round(100*cov, 1)}%, not the ~100% a completed rebuild gives.",
                   "x" = "Refusing to report a pass on a partial table."))
}

# 2. The leak anchor. exp_runs IS the baseline at that ball. At the first ball
#    of an innings every match opens 0/0, so a baseline that has not been told
#    the answer must predict nearly the same value for all of them and must
#    NOT correlate with the runs actually scored off that ball.
d <- as.data.table(dbGetQuery(conn, sprintf(
  "SELECT exp_runs, actual_runs FROM main.%s WHERE delivery_id LIKE '%%_000_01'", tbl)))
r <- cor(d$exp_runs, d$actual_runs)
cli::cli_alert_info("first balls {format(nrow(d), big.mark=',')}: exp_runs {round(min(d$exp_runs),3)}-{round(max(d$exp_runs),3)} (sd {round(sd(d$exp_runs),3)}), cor with that ball's runs {sprintf('%+.4f', r)}")
if (abs(r) > 0.30) {
  cli::cli_abort(c("First-ball exp_runs correlates {round(r,3)} with the ball's own runs.",
                   "x" = "Under the post-delivery leak this was 1.000. Do not publish this table."))
}

# 3. Ratings must actually separate players. A table full of the start rating
#    reads as a working model -- that is exactly what the gender-free table
#    name produced downstream.
sep <- dbGetQuery(conn, sprintf("
  SELECT COUNT(*) AS n_players, ROUND(STDDEV(elo), 1) AS sd_elo,
         ROUND(MIN(elo), 1) AS lo, ROUND(MAX(elo), 1) AS hi
  FROM (SELECT batter_id, MAX(batter_run_elo_after) AS elo
        FROM main.%s GROUP BY batter_id HAVING COUNT(*) >= 200)", tbl))
cli::cli_alert_info("batters with 200+ balls: {sep$n_players}, final run ELO sd {sep$sd_elo}, range {sep$lo}-{sep$hi}")
stopifnot(sep$sd_elo > 1)

cli::cli_alert_success("{tbl} passes coverage, the leak anchor and separation.")
