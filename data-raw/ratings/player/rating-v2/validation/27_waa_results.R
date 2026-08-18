suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

nm <- as.data.table(DBI::dbGetQuery(conn, "
  SELECT player_id, ANY_VALUE(player_name) AS player
  FROM cricsheet.players GROUP BY player_id"))

show <- function(fmt, minballs, lam) {
  cat("\n", strrep("=", 74), "\n", fmt, " male batters (min ", minballs,
      " balls faced), lambda = ", lam, "\n", strrep("=", 74), "\n", sep = "")
  d <- as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT batter_id AS player_id, COUNT(*) AS balls,
           SUM(raa_run) AS raa_run, SUM(waa) AS waa,
           SUM(actual_runs) AS runs,
           SUM(CAST(is_wicket AS INT)) AS outs
    FROM main.cricsheet_ball_raa
    WHERE format='%s' AND gender='male'
    GROUP BY batter_id HAVING COUNT(*) >= %d", fmt, minballs)))
  d <- merge(d, nm, by = "player_id", all.x = TRUE)
  d[, `:=`(raa100 = 100 * raa_run / balls,
           waa100 = 100 * waa / balls,
           sr     = 100 * runs / balls,
           avg    = runs / pmax(outs, 1))]
  d[, comp100 := raa100 + lam * waa100]

  cat("\n--- TOP 10 by RAA per 100 balls (scoring above expectation) ---\n")
  print(d[order(-raa100)][1:10, .(player, raa100 = round(raa100,1),
        waa100 = round(waa100,2), sr = round(sr,1), avg = round(avg,1), balls)])

  cat("\n--- TOP 10 by WAA per 100 balls (surviving above expectation) ---\n")
  print(d[order(-waa100)][1:10, .(player, waa100 = round(waa100,2),
        raa100 = round(raa100,1), sr = round(sr,1), avg = round(avg,1), balls)])

  cat("\n--- TOP 10 by COMPOSITE  raa + lambda*waa  per 100 balls ---\n")
  print(d[order(-comp100)][1:10, .(player, comp100 = round(comp100,1),
        raa100 = round(raa100,1), waa100 = round(waa100,2),
        sr = round(sr,1), avg = round(avg,1))])

  cat(sprintf("\n  spearman(RAA rate, WAA rate) = %.3f over %d players\n",
              cor(d$raa100, d$waa100, method = "spearman"), nrow(d)))
  invisible(d)
}

t <- show("T20", 2000, 9.0)
show("TEST", 4000, 33.0)

cat("\n\n=== THE POINT: same player, two very different answers (T20) ===\n")
t[, `:=`(r_raa = frank(-raa100), r_waa = frank(-waa100))]
t[, gap := r_waa - r_raa]
cat("\n-- biggest SLOGGERS: rank far better on runs than on survival --\n")
print(t[order(-gap)][1:6, .(player, raa_rank = r_raa, waa_rank = r_waa,
      raa100 = round(raa100,1), waa100 = round(waa100,2), sr = round(sr,1))])
cat("\n-- biggest BLOCKERS: rank far better on survival than on runs --\n")
print(t[order(gap)][1:6, .(player, raa_rank = r_raa, waa_rank = r_waa,
      raa100 = round(raa100,1), waa100 = round(waa100,2), sr = round(sr,1))])
