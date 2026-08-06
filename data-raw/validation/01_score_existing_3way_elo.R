# Score the EXISTING 3-Way ELO with the forward-looking validation harness.
#
# This fixes the bar every new estimator has to clear. Spec:
# .scratch/player-delivery-elo/issues/03-validation-harness.md (bouncerverse repo).
#
# Run via PowerShell -- DuckDB segfaults under Git Bash R:
#   powershell.exe -Command 'Rscript "bouncer/data-raw/validation/01_score_existing_3way_elo.R"'

suppressPackageStartupMessages({library(DBI); library(data.table)})

# Same root-resolution pattern as data-raw/ratings/player/3way-elo/00_compute_centrality_snapshots.R
bouncer_root <- tryCatch({
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- args[grep("^--file=", args)]
  if (length(file_arg) > 0) {
    script_path <- normalizePath(sub("^--file=", "", file_arg[1]))
    normalizePath(file.path(dirname(script_path), "..", ".."))
  } else {
    getwd()
  }
}, error = function(e) getwd())
devtools::load_all(bouncer_root)

DB <- file.path(find_bouncerdata_dir(), "bouncer.duckdb")

# Ratings as of each origin: last stored ELO strictly before the origin date.
load_3way_ratings <- function(con, role = c("batter", "bowler")) {
  role <- match.arg(role)
  idc <- if (role == "batter") "batter_id" else "bowler_id"
  r <- DBI::dbGetQuery(con, sprintf("
    SELECT %s AS player_id, match_date,
           MAX_BY(%s_run_elo_after, delivery_id)    AS run_elo,
           MAX_BY(%s_wicket_elo_after, delivery_id) AS wkt_elo
    FROM mens_test_3way_elo GROUP BY 1,2", idc, role, role))
  r <- as.data.table(r)
  r[, match_date := as.Date(match_date)]
  r[order(player_id, match_date)]
}

con <- dbConnect(duckdb::duckdb(), DB, read_only = TRUE)
on.exit(dbDisconnect(con, shutdown = TRUE))

results <- list()
for (role in c("batter", "bowler")) {
  pool <- load_rating_pool(con, role)
  rts  <- load_3way_ratings(con, role)
  frame <- build_rating_frame(pool, rts, c("run_elo", "wkt_elo"))
  cat(sprintf("\n%s: %d evaluation rows across %d origins, %d rated\n",
              role, nrow(frame), uniqueN(frame$origin), sum(!is.na(frame$run_elo))))
  runs_label <- if (role == "batter") "batting: runs per ball" else "bowling: runs conceded per ball"
  evt_label  <- if (role == "batter") "batting: dismissals per ball" else "bowling: wickets per ball"
  results[[length(results) + 1]] <-
    summarise_rating_score(score_rating(frame, "runs",   "run_elo"), runs_label)
  results[[length(results) + 1]] <-
    summarise_rating_score(score_rating(frame, "events", "wkt_elo"), evt_label)
}

cat("\n--- pooled ---\n")
print(as.data.frame(rbindlist(results)))
