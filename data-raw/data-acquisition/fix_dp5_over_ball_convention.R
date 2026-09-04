# D-P5: correct cricsheet.deliveries.over_ball to use the LEGAL ball count
# within each over, not the raw delivery-within-over position (extras
# included). See R/format_utils.R::calculate_over_ball() for the full defect
# writeup, and R/cricsheet_parser.R for the matching parser fix that keeps
# future ingests correct going forward.
#
# `ball` counts every delivery in an over including wides/no-balls, so it
# reaches 19 in the stored data (233,975 deliveries have ball > 6). With the
# stored `/10` convention, an over needing 10+ deliveries spilled into the
# next over's numeric range: over 5 ball 12 and over 6 ball 2 both gave 6.2
# (2,941 stored deliveries measured colliding this way as of 2026-09-04).
#
# This is a derived-column correction, not a re-parse: over_ball is fully
# recomputable from already-stored over/ball/wides/noballs via a per-over
# running count of legal deliveries, so no JSON re-download or re-parse is
# needed.
#
# Verified before running (2026-09-04): 11,308,598 rows total; 2,941 rows
# with ball>=10 all resolve to a non-colliding value; row count parity
# holds; zero NULL/negative/out-of-range results. 8.96% of all rows change
# (1,012,877 of 11,308,598) -- far more than the 2,941 headline collision
# figure, because a wide/no-ball shifts every LATER legal ball in the same
# over too, not just its own row. This is why every ball-outcome model
# needs retraining after this runs, not just a documentation update.
#
# Usage: Rscript data-raw/data-acquisition/fix_dp5_over_ball_convention.R
# Under PowerShell on Windows, since arrow/duckdb segfault under Git Bash R.

suppressPackageStartupMessages({
  library(DBI)
  devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE)
})
conn <- get_db_connection(read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

before_n <- dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricsheet.deliveries")$n
cat(sprintf("before: %d rows\n", before_n))

DBI::dbBegin(conn)
n_updated <- tryCatch({
  dbExecute(conn, "
    UPDATE cricsheet.deliveries
    SET over_ball = corrected.corrected_over_ball
    FROM (
      SELECT delivery_id,
             over + (SUM(CASE WHEN COALESCE(wides,0)=0 AND COALESCE(noballs,0)=0 THEN 1 ELSE 0 END)
                       OVER (PARTITION BY match_id, innings, over ORDER BY ball
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) / 10.0
               AS corrected_over_ball
      FROM cricsheet.deliveries
    ) AS corrected
    WHERE cricsheet.deliveries.delivery_id = corrected.delivery_id
  ")
}, error = function(e) {
  DBI::dbRollback(conn)
  cli::cli_abort("UPDATE failed, rolled back: {conditionMessage(e)}")
})
cat(sprintf("rows updated: %d\n", n_updated))

after_n <- dbGetQuery(conn, "SELECT COUNT(*) AS n FROM cricsheet.deliveries")$n

# Post-update checks, INSIDE the transaction -- any failure rolls back rather
# than leaving a half-corrected table.
n_still_colliding <- dbGetQuery(conn,
  "SELECT COUNT(*) AS n FROM cricsheet.deliveries WHERE over_ball >= over + 1")$n
n_null <- dbGetQuery(conn,
  "SELECT COUNT(*) AS n FROM cricsheet.deliveries WHERE over_ball IS NULL")$n
n_lt_over <- dbGetQuery(conn,
  "SELECT COUNT(*) AS n FROM cricsheet.deliveries WHERE over_ball < over")$n

cat(sprintf("still colliding with next over (want 0): %d\n", n_still_colliding))
cat(sprintf("NULL over_ball (want 0): %d\n", n_null))
cat(sprintf("over_ball < over (want 0): %d\n", n_lt_over))

if (n_still_colliding > 0 || n_null > 0 || n_lt_over > 0 || after_n != before_n) {
  DBI::dbRollback(conn)
  cli::cli_abort("Post-update checks failed -- rolled back. before={before_n} after={after_n}")
}

DBI::dbCommit(conn)
cli::cli_alert_success("Committed. {n_updated} rows in cricsheet.deliveries corrected.")
cli::cli_alert_warning("Every ball-outcome model must be retrained after this -- overs_left changed for ~9% of rows.")
