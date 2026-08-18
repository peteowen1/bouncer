# All three L3 ratings side by side for T20 male, plus the Narine test.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = TRUE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
id_map <- build_player_id_map(conn)

res <- list()
for (mt in c("runs", "wickets", "team_score")) {
  r <- tryCatch(suppressMessages(calculate_player_rating_v2(
         "t20", "male", role = "batter", conn = conn, id_map = id_map, metric = mt)),
       error = function(e) { cat(mt, "ERROR:", conditionMessage(e), "\n"); NULL })
  if (is.null(r)) next
  res[[mt]] <- as.data.table(r)[, .(player_id, player_name, rating, average,
                                    matches, effective_matches)]
  cat(sprintf("%-11s rated %d players\n", mt, nrow(r)))
}

cat("\n=== TOP 12 by TEAM SCORE ADDED rating (projected runs per match) ===\n")
print(res$team_score[1:12, .(player_name, rating = round(rating,2),
                             average = round(average,1), matches)])

a <- Reduce(function(x, y) merge(x, y, by = "player_id"), list(
  res$runs[,       .(player_id, player_name, r_runs = rating)],
  res$wickets[,    .(player_id, r_wkt  = rating)],
  res$team_score[, .(player_id, r_tsa  = rating)]))
a[, `:=`(rank_runs = frank(-r_runs), rank_wkt = frank(-r_wkt), rank_tsa = frank(-r_tsa))]

cat(sprintf("\n=== how independent are the three? (n = %d) ===\n", nrow(a)))
cat(sprintf("  runs    vs wickets     %+.3f\n", cor(a$r_runs, a$r_wkt, method="spearman")))
cat(sprintf("  runs    vs team score  %+.3f\n", cor(a$r_runs, a$r_tsa, method="spearman")))
cat(sprintf("  wickets vs team score  %+.3f\n", cor(a$r_wkt,  a$r_tsa, method="spearman")))

cat("\n=== THE ADJUDICATION TEST: players the two components disagree about ===\n")
a[, disagree := abs(rank_runs - rank_wkt)]
setorder(a, -disagree)
print(a[1:10, .(player_name, rank_runs, rank_wkt, rank_tsa,
                runs = round(r_runs,2), wkt = round(r_wkt,3), tsa = round(r_tsa,2))])
cat("\n  rank_tsa is where the team-score model lands them once tempo and wicket\n")
cat("  cost are both priced -- the number that settles the argument.\n")
