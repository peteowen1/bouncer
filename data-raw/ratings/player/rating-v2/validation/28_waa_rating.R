# L3: the WICKETS rating -- waa through the same decay + prior + competition
# machinery as runs, with competition strength on a SURVIVAL basis.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
id_map <- build_player_id_map(conn)

cat("=== competition factors: runs basis vs survival basis (T20 male) ===\n")
fr <- fit_competition_factors(conn, "t20", "male", id_map = id_map, basis = "runs")
fs <- fit_competition_factors(conn, "t20", "male", id_map = id_map, basis = "survival")
m <- merge(as.data.table(fr)[, .(comp, runs_basis = round(factor, 3))],
           as.data.table(fs)[, .(comp, survival_basis = round(factor, 3))], by = "comp")
cat(sprintf("  %d competitions rated on both\n", nrow(m)))
cat(sprintf("  spearman(runs factor, survival factor) = %.3f\n",
            cor(m$runs_basis, m$survival_basis, method = "spearman")))
cat("  a high correlation would mean the survival basis adds nothing\n")
print(m[order(-runs_basis)][1:6])
print(m[order(runs_basis)][1:6])

for (fmt in c("t20", "test")) {
  cat("\n", strrep("=", 72), "\n", toupper(fmt), " MALE -- three L3 ratings\n",
      strrep("=", 72), "\n", sep = "")
  res <- list()
  for (mt in c("composite", "runs", "wickets")) {
    r <- tryCatch(suppressMessages(calculate_player_rating_v2(
           fmt, "male", role = "batter", conn = conn, id_map = id_map, metric = mt)),
         error = function(e) { cat("  ", mt, "ERROR:", conditionMessage(e), "\n"); NULL })
    if (is.null(r)) next
    res[[mt]] <- as.data.table(r)[, .(player_id, player_name, rating, average,
                                      main_comp, effective_matches)]
    cat(sprintf("  %-10s rated %d players\n", mt, nrow(r)))
  }
  if (length(res) < 3) next

  cat("\n--- TOP 10, RUNS rating (runs above average per match) ---\n")
  print(res$runs[1:10, .(player_name, rating = round(rating,2), average = round(average,1), main_comp)])

  cat("\n--- TOP 10, WICKETS rating (wickets saved above average per match) ---\n")
  print(res$wickets[1:10, .(player_name, rating = round(rating,3), average = round(average,1), main_comp)])

  cat("\n--- TOP 10, COMPOSITE (as shipped) ---\n")
  print(res$composite[1:10, .(player_name, rating = round(rating,2), average = round(average,1), main_comp)])

  a <- merge(res$runs[, .(player_id, r_runs = rating)],
             res$wickets[, .(player_id, r_wkt = rating)], by = "player_id")
  cat(sprintf("\n  spearman(runs rating, wickets rating) = %.3f over %d players\n",
              cor(a$r_runs, a$r_wkt, method = "spearman"), nrow(a)))
}
