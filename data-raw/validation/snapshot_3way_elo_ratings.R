# Snapshot per-player FINAL 3-way ELOs before a rebuild replaces the table.
#
# promote_3way_elo_staging() drops the live table, so once a rebuild lands
# there is nothing left to compare against. Snapshotting the whole table would
# cost millions of rows; one row per player is a few thousand and is enough for
# a rank correlation and a top-N diff, which is the comparison anyone actually
# wants (bouncerverse#63).
#
# Writes main.{table}_prev_summary, replacing any previous snapshot.
#
# Usage: Rscript data-raw/validation/snapshot_3way_elo_ratings.R mens odi
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages(library(DBI))

a <- commandArgs(trailingOnly = TRUE)
gender <- if (length(a) >= 1) a[1] else "mens"
fmt    <- if (length(a) >= 2) a[2] else "t20"
tbl    <- paste0(gender, "_", fmt, "_3way_elo")
snap   <- paste0(tbl, "_prev_summary")

conn <- get_db_connection(read_only = FALSE)
on.exit(dbDisconnect(conn, shutdown = TRUE), add = TRUE)

if (!table_exists(conn, tbl)) {
  cli::cli_alert_warning("{tbl} does not exist; nothing to snapshot.")
  quit(status = 0)
}

n <- dbGetQuery(conn, sprintf("SELECT COUNT(*) AS n FROM main.%s", tbl))$n
if (n == 0) {
  cli::cli_alert_warning("{tbl} is empty; nothing worth snapshotting.")
  quit(status = 0)
}

invisible(.in_transaction(conn, function() {
  DBI::dbExecute(conn, sprintf("DROP TABLE IF EXISTS %s", snap))
  # Last rating per player, taken by date then delivery id -- the same ordering
  # the ratings themselves are built in, so "final" means the same thing.
  DBI::dbExecute(conn, sprintf("
    CREATE TABLE %s AS
    WITH bat AS (
      SELECT batter_id AS player_id, batter_run_elo_after AS run_elo,
             batter_wicket_elo_after AS wicket_elo, COUNT(*) OVER (PARTITION BY batter_id) AS balls,
             ROW_NUMBER() OVER (PARTITION BY batter_id ORDER BY match_date DESC, delivery_id DESC) AS rn
      FROM main.%s)
    SELECT player_id, 'batter' AS role, run_elo, wicket_elo, balls
    FROM bat WHERE rn = 1", snap, tbl))
  DBI::dbExecute(conn, sprintf("
    INSERT INTO %s
    WITH bowl AS (
      SELECT bowler_id AS player_id, bowler_run_elo_after AS run_elo,
             bowler_wicket_elo_after AS wicket_elo, COUNT(*) OVER (PARTITION BY bowler_id) AS balls,
             ROW_NUMBER() OVER (PARTITION BY bowler_id ORDER BY match_date DESC, delivery_id DESC) AS rn
      FROM main.%s)
    SELECT player_id, 'bowler' AS role, run_elo, wicket_elo, balls
    FROM bowl WHERE rn = 1", snap, tbl))
}))

s <- dbGetQuery(conn, sprintf("SELECT role, COUNT(*) n FROM %s GROUP BY 1 ORDER BY 1", snap))
cli::cli_alert_success("Snapshotted {tbl} ({format(n, big.mark=',')} rows) into {snap}:")
for (i in seq_len(nrow(s))) cli::cli_bullets(c("*" = "{s$role[i]}: {s$n[i]} players"))
