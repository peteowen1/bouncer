# Persist TSA into main.cricsheet_ball_raa for every limited-overs bucket.
#
# Innings 1 only, deliberately: a chase is truncated the moment the target is
# passed, so "projected final score" stops being the quantity the model
# predicts. D-P22 measured that truncation at -26 runs in minor cricket against
# -13 in major, i.e. it biases unevenly by competition, which is worse than
# losing the rows. Test has no fixed ball allocation and gets no TSA at all.
suppressMessages(devtools::load_all("C:/dev/bouncerverse/bouncer", quiet = TRUE))
suppressMessages({library(DBI); library(data.table)})
DB <- "C:/dev/bouncerverse/bouncerdata/bouncer.duckdb"
stopifnot(file.exists(DB), file.info(DB)$size > 1e10)
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB, read_only = FALSE)
on.exit(DBI::dbDisconnect(conn, shutdown = TRUE), add = TRUE)

have <- DBI::dbGetQuery(conn, "
  SELECT column_name FROM information_schema.columns
  WHERE table_schema='main' AND table_name='cricsheet_ball_raa'")$column_name
if (!"tsa" %in% have) {
  DBI::dbExecute(conn, "ALTER TABLE main.cricsheet_ball_raa ADD COLUMN tsa DOUBLE")
  cat("added column tsa\n")
}

buckets <- list(list(f="t20", g="male", mb=120L), list(f="odi", g="male", mb=300L),
                list(f="t20", g="female", mb=120L), list(f="odi", g="female", mb=300L))

total <- 0L
for (bk in buckets) {
  FMT <- bk$f; GEN <- bk$g; MAXB <- bk$mb
  p <- tryCatch(load_projection_params(FMT, GEN, "international", conn = conn),
                error = function(e) NULL)
  if (is.null(p)) { cat(sprintf("  %s %s: no projection params, skipped\n", FMT, GEN)); next }

  d <- as.data.table(DBI::dbGetQuery(conn, sprintf("
    SELECT r.delivery_id, r.over_number, r.ball_number, r.actual_runs,
           CAST(r.is_wicket AS INT) AS is_wicket, r.exp_runs, r.exp_wicket,
           d.total_runs - d.runs_total AS runs_pre,
           d.wickets_fallen - CAST(d.is_wicket AS INT) AS wkts_pre
    FROM main.cricsheet_ball_raa r
    JOIN cricsheet.deliveries d ON d.delivery_id = r.delivery_id
    WHERE r.format='%s' AND r.gender='%s' AND r.innings_number = 1",
    toupper(FMT), GEN)))
  if (!nrow(d)) { cat(sprintf("  %s %s: no rows\n", FMT, GEN)); next }

  d[, bb := over_number * 6L + ball_number]
  pr <- function(sc, wr, br) calculate_projected_scores_vectorized(
    current_score = sc, wickets_remaining = wr, balls_remaining = br,
    expected_initial_score = p$eis_agnostic, a = p$a, b = p$b, z = p$z, y = p$y,
    max_balls = MAXB)
  d[, tsa := pr(runs_pre + actual_runs, 10L - wkts_pre - is_wicket, pmax(0L, MAXB - bb)) -
             pr(runs_pre + exp_runs, 10 - wkts_pre - exp_wicket, pmax(0L, MAXB - bb))]

  DBI::dbWriteTable(conn, "tsa_stage", d[, .(delivery_id, tsa)], overwrite = TRUE, temporary = TRUE)
  n <- DBI::dbExecute(conn, "
    UPDATE main.cricsheet_ball_raa AS r SET tsa = s.tsa
    FROM tsa_stage s WHERE s.delivery_id = r.delivery_id")
  total <- total + n
  cat(sprintf("  %-4s %-6s %s balls  mean tsa %+.4f  dot %+.3f  wicket %+.3f\n",
              FMT, GEN, format(n, big.mark=","), mean(d$tsa, na.rm=TRUE),
              d[actual_runs==0 & is_wicket==0, mean(tsa, na.rm=TRUE)],
              d[is_wicket==1, mean(tsa, na.rm=TRUE)]))
}
cat(sprintf("\ntotal rows given a TSA: %s\n", format(total, big.mark=",")))

cat("\n=== coverage: what share of each bucket has TSA? ===\n")
print(DBI::dbGetQuery(conn, "
  SELECT format, gender, COUNT(*) AS balls,
         SUM(CASE WHEN tsa IS NOT NULL THEN 1 ELSE 0 END) AS with_tsa,
         ROUND(100.0*SUM(CASE WHEN tsa IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),1) AS pct
  FROM main.cricsheet_ball_raa GROUP BY 1,2 ORDER BY 1,2"))
cat("  innings 1 only, so ~50% in limited overs and 0% in Test is expected\n")
